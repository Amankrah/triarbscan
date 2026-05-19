"""WebSocket-driven triangular arbitrage scanner — Binance (Phase 2).

Event-driven recompute over a streaming top-of-book mirror, with two cadences:

* **Fast loop** (up to ~20 Hz, awakened by `BinanceBookFeed.update_event`):
  evaluates every triangle on every wakeup, detects per-cycle gate-crossings
  against the previous-scan state, and writes each crossing — up or down — to
  a daily-rotated arrival-event log. This is the resolution at which a real
  cross-rate dislocation actually moves; REST polling at 3–15 s aliases over
  the dislocation lifetime and cannot characterise it.

* **Slow tick** (~1 Hz, gated by elapsed time): updates the SurfaceTracker
  with current eligible-cycle surface rates, refreshes the aggregate
  (mean/dispersion + rising/flat trend) for the venue-wide trajectory,
  detects precursor enter/exit transitions and logs them, and writes the
  aggregate row to its daily-rotated CSV. The 1 Hz cadence keeps the moving
  averages comparable to the REST-baseline analysis.

A separate heartbeat (~5 s) prints a console summary — cumulative events,
last-scan diagnostics, compute timing distribution, feed health — instead of
spamming output at 20 Hz. The arrival-event log and the aggregate log live in
separate daily-rotated files so the two cadences can be analysed independently.

Depth check is the inside-quote single level from bookTicker (L1). A notional
that does not fit at the inside book is flagged via the fill-fraction signal
from `calculate_acquired_coin`; full L2 streaming on a precursor watchlist is
the next phase.
"""
import asyncio
import csv
import os
import statistics
import sys
import time
from collections import Counter, deque
from datetime import datetime

from binance_adapter import BinanceAdapter
from fee_calculator import calculate_fee_impact
from multi_exchange_manager import (
    filter_by_usd_volume,
    PRECURSOR_WINDOW, PRECURSOR_GATE_FRACTION, MAX_TICK_RATIO,
    AGGREGATE_TREND_WINDOW, _series_rising,
)
from surface_tracker import SurfaceTracker
from ws_binance import BinanceBookFeed, BinanceDepthFeed
import func_arbitrage

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# ----- configuration -----
SLIPPAGE_BUFFER = 0.10          # % subtracted on top of fees (L1 fills are optimistic)
MIN_NET_RATE = 0.0              # opportunity must clear fees + slippage by at least this %
MIN_SURFACE_RATE = 0.0          # manual floor (the auto gate is normally tighter)
FEE_TYPE = 'taker'              # triangular legs cross the spread
NOTIONAL_USD = 5000             # assumed capital, for the net-profit dollar estimate
AUTO_DEPTH_GATE = True          # derive the surface-gate from the venue's fee hurdle
DEPTH_GATE_MARGIN = 0.10        # gate sits this far below the strict hurdle
MIN_USD_VOLUME = 50000
MIN_USD_VOLUME_BY_QUOTE = {
    'EUR': 250000, 'GBP': 250000, 'JPY': 250000,
    'CAD': 250000, 'AUD': 250000, 'CHF': 250000,
}
STARTING_AMOUNTS = {"USDT": 100, "USDC": 100, "BTC": 0.05, "ETH": 0.1}

# ----- Phase 2 cadence -----
EVENT_SCAN_HZ = 20              # max scans per second (50 ms floor between scans)
AGGREGATE_LOG_SECONDS = 1.0     # slow-tick interval: tracker, aggregate, CSV
HEARTBEAT_SECONDS = 5.0         # console summary cadence
ENABLE_L2_WATCHLIST = True      # Phase 3: stream L2 depth for precursor cycles

LOG_AGGREGATE_CSV = True
LOG_ARRIVAL_EVENTS = True
AGGREGATE_CSV_PREFIX = 'surface_aggregate_log_ws'
EVENT_CSV_PREFIX = 'arrival_events_ws'


# ============================================================
# helpers
# ============================================================

def _fmt_age(age):
    """Format a quote-age (seconds) compactly: sub-second in ms, else seconds."""
    if age is None:
        return "n/a"
    if age < 1.0:
        return f"{age * 1000:.0f}ms"
    return f"{age:.1f}s"


def _cycle_smooth(t_pair, prices, adapter):
    """True only if every leg's price tick is small relative to mid. Coarse-
    tick legs produce surface staircase artifacts (manuscript §3.4.2)."""
    for leg in ('a', 'b', 'c'):
        tick = adapter.get_tick_size(t_pair[f"pair_{leg}"])
        if not tick:
            return False
        mid = (prices[f"pair_{leg}_ask"] + prices[f"pair_{leg}_bid"]) / 2
        if mid <= 0 or tick / mid >= MAX_TICK_RATIO:
            return False
    return True


def log_aggregate_csv(stats):
    """Append the slow-tick aggregate row to a daily-rotated WS-specific CSV."""
    agg = stats.get('aggregate')
    if not agg:
        return
    now = datetime.now()
    path = f"{AGGREGATE_CSV_PREFIX}_{now:%Y-%m-%d}.csv"
    new_file = not os.path.exists(path)
    with open(path, 'a', newline='') as fp:
        writer = csv.writer(fp)
        if new_file:
            writer.writerow(['timestamp', 'exchange', 'mean_ma',
                             'dispersion', 'route_count'])
        writer.writerow([now.isoformat(timespec='seconds'), 'binance',
                         f"{agg['mean_ma']:.8f}",
                         f"{agg['dispersion']:.8f}",
                         agg['route_count']])


def log_event(event):
    """Append one arrival-process event to a daily-rotated event log.

    `event` is a dict with 'type', 'route', and optional 'surface_perc',
    'real_perc', 'net_perc', 'filled_fraction', and 'timestamp' (datetime).
    Timestamp is recorded to millisecond precision so inter-event times can
    be reconstructed even at high cadence."""
    if not LOG_ARRIVAL_EVENTS:
        return
    now = event.get('timestamp') or datetime.now()
    path = f"{EVENT_CSV_PREFIX}_{now:%Y-%m-%d}.csv"
    new_file = not os.path.exists(path)
    with open(path, 'a', newline='') as fp:
        writer = csv.writer(fp)
        if new_file:
            writer.writerow(['timestamp', 'exchange', 'event_type', 'route',
                             'surface_perc', 'real_perc', 'net_perc',
                             'filled_fraction'])

        def fmt(v, places=8):
            return f"{v:.{places}f}" if v is not None else ''

        writer.writerow([
            now.isoformat(timespec='milliseconds'),
            'binance', event['type'], event.get('route', ''),
            fmt(event.get('surface_perc')),
            fmt(event.get('real_perc')),
            fmt(event.get('net_perc')),
            fmt(event.get('filled_fraction'), 4),
        ])


def bootstrap():
    """One-time REST work: load tickers (also populates tick sizes via
    exchangeInfo), apply the volume + fiat filter, enumerate triangles, seed
    the L1 book mirror from /ticker/bookTicker."""
    adapter = BinanceAdapter()
    print("📥 Bootstrapping symbols + triangles from Binance REST...")
    tickers = adapter.get_all_tickers()
    pairs_raw = adapter.get_tradeable_pairs(tickers)
    pairs, dropped = filter_by_usd_volume(
        tickers, pairs_raw, MIN_USD_VOLUME, MIN_USD_VOLUME_BY_QUOTE
    )
    triangles = func_arbitrage.structure_triangular_pairs(pairs)
    symbols = set()
    for t in triangles:
        symbols.update((t['pair_a'], t['pair_b'], t['pair_c']))
    snapshot = _snapshot_books(adapter)
    print(f"  ✓ {len(pairs_raw)} tradeable -> {len(pairs)} after volume filter "
          f"(dropped {dropped}) | {len(triangles)} triangles | "
          f"{len(symbols)} symbols | seeded {len(snapshot)} books")
    return adapter, triangles, symbols, snapshot


def _snapshot_books(adapter):
    data = adapter.fetch_with_retry(f"{adapter.base_url}/ticker/bookTicker")
    books = {}
    if isinstance(data, list):
        for d in data:
            try:
                base, quote = adapter.parse_symbol(d['symbol'])
                if base and quote:
                    books[f"{base}_{quote}"] = {
                        'bid': float(d['bidPrice']),
                        'ask': float(d['askPrice']),
                        'bid_qty': float(d['bidQty']),
                        'ask_qty': float(d['askQty']),
                    }
            except (KeyError, ValueError, TypeError):
                continue
    return books


def _prices(t_pair, books):
    a = books.get(t_pair['pair_a'])
    b = books.get(t_pair['pair_b'])
    c = books.get(t_pair['pair_c'])
    if not (a and b and c):
        return None
    return {
        'pair_a_ask': a['ask'], 'pair_a_bid': a['bid'],
        'pair_b_ask': b['ask'], 'pair_b_bid': b['bid'],
        'pair_c_ask': c['ask'], 'pair_c_bid': c['bid'],
    }


def _l1_orderbook(sym, books):
    bk = books.get(sym)
    if not bk:
        return {'bids': [], 'asks': []}
    return {'bids': [[bk['bid'], bk['bid_qty']]],
            'asks': [[bk['ask'], bk['ask_qty']]]}


def _select_book(sym, books_l1, books_l2):
    """Prefer the L2 partial-book snapshot if a fresh one is available for
    this symbol; otherwise fall back to the L1 inside-quote mirror. Returns
    (book, used_l2: bool)."""
    if books_l2 is not None:
        bk = books_l2.get(sym)
        if bk and bk.get('bids') and bk.get('asks'):
            return bk, True
    return _l1_orderbook(sym, books_l1), False


def _real_rate(surface_arb, books_l1, books_l2):
    """Reordered, fill-fraction-aware depth walk that prefers L2 partial-book
    snapshots where the watchlist has them and falls back to the L1 inside
    quote otherwise.

    The surface calc records correct legs but not in executable order, and
    its `swap_1` is mislabeled (manuscript §3.2). We reorder via the
    produce/consume chain and recover the true start currency before walking.
    Returns a dict with `legs_l2` (count of legs that used L2). On a clean
    fill the dict carries `profit` and `real_rate_perc`; on a partial fill
    it carries `exhausted=True` and `filled_fraction`; None when the legs
    do not close."""
    raw_legs = [
        (surface_arb['contract_1'], surface_arb['direction_trade_1']),
        (surface_arb['contract_2'], surface_arb['direction_trade_2']),
        (surface_arb['contract_3'], surface_arb['direction_trade_3']),
    ]
    legs, swap_1 = func_arbitrage.order_legs_for_execution(raw_legs)
    if legs is None:
        return None
    contracts = [c for c, _ in legs]
    directions = [d for _, d in legs]

    legs_l2 = 0
    depths = []
    for contract, direction in zip(contracts, directions):
        book, used_l2 = _select_book(contract, books_l1, books_l2)
        if used_l2:
            legs_l2 += 1
        depths.append(func_arbitrage.reformated_orderbook(book, direction))

    start = STARTING_AMOUNTS.get(swap_1, 100)
    amount = start
    for leg_idx, depth in enumerate(depths, start=1):
        amount, filled = func_arbitrage.calculate_acquired_coin(amount, depth)
        if filled < 1.0:
            return {'exhausted': True, 'filled_fraction': filled,
                    'exhausted_leg': leg_idx,
                    'contracts': contracts, 'swap_1': swap_1,
                    'legs_l2': legs_l2}
    profit = amount - start
    real_perc = (profit / start * 100) if start else 0
    return {'exhausted': False, 'profit': profit, 'real_rate_perc': real_perc,
            'contracts': contracts, 'swap_1': swap_1,
            'legs_l2': legs_l2}


# ============================================================
# scan
# ============================================================

def scan(triangles, books_l1, books_l2, adapter, depth_gate, *,
         tracker=None, prev_above_gate=None):
    """One pass over the cached triangle set against the live book mirror.

    `books_l1` is the inside-quote mirror (bookTicker). `books_l2` is the
    partial-book snapshots for the precursor watchlist (or None to disable).
    The depth walk uses L2 where available and falls back to L1 otherwise.

    Returns `(opportunities, stats, events)`. `events` carries one entry per
    gate-crossing detected against `prev_above_gate` (a mutable route->bool
    dict the routine updates in place). With `tracker`, records eligible
    cycles to the SurfaceTracker (slow tick)."""
    opportunities = []
    events = []
    stats = {
        'pairs_checked': len(triangles),
        'missing_prices': 0, 'paths_evaluated': 0,
        'positive_surface': 0, 'depth_checked': 0,
        'book_thin': 0, 'book_thin_fill_sum': 0.0,
        'book_thin_worst_fill': None,
        'best_surface_perc': None, 'best_surface_route': None,
        'best_real_perc': None, 'best_net_perc': None,
        'depth_gate': depth_gate,
        'l2_legs_used': 0, 'l2_full_cycles': 0,
    }

    for t_pair in triangles:
        combined = t_pair.get('combined', '')
        prices = _prices(t_pair, books_l1)
        if prices is None:
            stats['missing_prices'] += 1
            continue

        surface_arb, best_rate = func_arbitrage.calc_triangular_arb_surface_rate(
            t_pair, prices)

        if best_rate is not None:
            stats['paths_evaluated'] += 1
            if (stats['best_surface_perc'] is None
                    or best_rate > stats['best_surface_perc']):
                stats['best_surface_perc'] = best_rate
                stats['best_surface_route'] = combined
            if tracker is not None and _cycle_smooth(t_pair, prices, adapter):
                tracker.record(combined, best_rate)

        surface_rate = (surface_arb.get('profit_loss_perc', 0)
                        if surface_arb else 0)
        if surface_arb and surface_rate > 0:
            stats['positive_surface'] += 1

        gate_crossed = surface_arb is not None and surface_rate >= depth_gate
        depth_result = None
        net_value = None

        if gate_crossed:
            stats['depth_checked'] += 1
            result = _real_rate(surface_arb, books_l1, books_l2)
            if result is None:
                pass
            else:
                legs_l2 = result.get('legs_l2', 0)
                stats['l2_legs_used'] += legs_l2
                if legs_l2 == 3:
                    stats['l2_full_cycles'] += 1
            if result is None:
                pass
            elif result.get('exhausted'):
                frac = result.get('filled_fraction', 0.0)
                if frac > 0:
                    stats['book_thin'] += 1
                    stats['book_thin_fill_sum'] += frac
                    if (stats['book_thin_worst_fill'] is None
                            or frac < stats['book_thin_worst_fill']):
                        stats['book_thin_worst_fill'] = frac
                depth_result = result
            else:
                real = result['real_rate_perc']
                if (stats['best_real_perc'] is None
                        or real > stats['best_real_perc']):
                    stats['best_real_perc'] = real
                fee_3 = sum(adapter.get_taker_fee(c) or 0.0
                            for c in result['contracts'])
                fee = calculate_fee_impact('binance', real, FEE_TYPE,
                                           total_fee_perc=fee_3)
                net = (fee['net_profit_perc'] or 0) - SLIPPAGE_BUFFER
                net_value = net
                if (stats['best_net_perc'] is None
                        or net > stats['best_net_perc']):
                    stats['best_net_perc'] = net
                depth_result = {**result, 'net_perc': net,
                                'fee_perc': fee['total_fee_3_trades']}
                if net >= MIN_NET_RATE:
                    opportunities.append({
                        'exchange': 'binance',
                        'contract_1': result['contracts'][0],
                        'contract_2': result['contracts'][1],
                        'contract_3': result['contracts'][2],
                        'real_rate_perc': real,
                        'profit_loss': result['profit'],
                        'fee_perc': fee['total_fee_3_trades'],
                        'slippage_buffer_perc': SLIPPAGE_BUFFER,
                        'net_rate_perc': net,
                        'surface_arb': surface_arb,
                    })

        if prev_above_gate is not None:
            was_above = prev_above_gate.get(combined, False)
            if gate_crossed != was_above:
                evt = {
                    'type': 'gate_cross_up' if gate_crossed else 'gate_cross_down',
                    'route': combined,
                    'surface_perc': surface_rate,
                    'real_perc': None, 'net_perc': None,
                    'filled_fraction': None,
                }
                if gate_crossed and depth_result is not None:
                    if depth_result.get('exhausted'):
                        evt['filled_fraction'] = depth_result.get('filled_fraction')
                    else:
                        evt['real_perc'] = depth_result.get('real_rate_perc')
                        evt['net_perc'] = net_value
                events.append(evt)
                prev_above_gate[combined] = gate_crossed

    return opportunities, stats, events


# ============================================================
# heartbeat console output
# ============================================================

def _print_heartbeat(scan_count, events_logged, compute_samples,
                     stats, agg, feed, depth_feed, symbols, gate, opportunities):
    print(f"\n>>> HEARTBEAT scan #{scan_count} at {time.strftime('%H:%M:%S')}")
    print("=" * 60)

    e = events_logged
    print(f"  events: gate↑={e.get('gate_cross_up', 0)} "
          f"gate↓={e.get('gate_cross_down', 0)}  "
          f"precursor+={e.get('precursor_enter', 0)} "
          f"precursor-={e.get('precursor_exit', 0)}")

    if stats:
        best_s = stats.get('best_surface_perc')
        best_s_str = (f"{best_s:.4f}% (raw {best_s:.8f}%)"
                      if best_s is not None else "n/a")
        best_r = stats.get('best_real_perc')
        best_n = stats.get('best_net_perc')
        best_r_str = f"{best_r:.4f}%" if best_r is not None else "n/a"
        best_n_str = f"{best_n:.4f}%" if best_n is not None else "n/a"
        gate_str = f"≥{gate:.2f}%" if gate else "off"
        print(f"  last scan: evaluated={stats['paths_evaluated']} | "
              f"+surface={stats['positive_surface']} | "
              f"depth_checks={stats['depth_checked']} (gate {gate_str}) | "
              f"thin={stats.get('book_thin', 0)}")
        print(f"             best surface={best_s_str} | "
              f"best real={best_r_str} | best net={best_n_str}")
        if stats.get('best_surface_route'):
            route = stats['best_surface_route']
            print(f"  best-surface route: {route}")
            # Per-leg quote staleness — a leg whose inside price hasn't
            # moved for many seconds while the others tick smoothly is the
            # signature of quote-refresh sparsity pinning the surface.
            legs = route.split(',')
            if len(legs) == 3:
                parts = [f"{leg}={_fmt_age(feed.quote_age(leg))}" for leg in legs]
                print(f"    leg quote ages: {' | '.join(parts)}")

    if agg:
        a_trend = "↑ rising" if agg.get('rising') else "— flat"
        print(f"  aggregate (1Hz): mean MA={agg['mean_ma']:.4f}% "
              f"σ={agg['dispersion']:.4f}% routes={agg['route_count']}  {a_trend}")

    if compute_samples:
        samples = list(compute_samples)
        p50 = statistics.median(samples)
        p99 = sorted(samples)[int(len(samples) * 0.99)] if len(samples) >= 100 else max(samples)
        print(f"  compute: median {p50:.2f}ms | p99 {p99:.2f}ms | "
              f"samples n={len(samples)}")

    conn = "connected" if feed.connected else "DISCONNECTED"
    print(f"  feed L1: {feed.coverage(symbols)}/{len(symbols)} symbols | "
          f"ws msgs={feed.messages} | {conn}")
    if depth_feed is not None:
        d_conn = "connected" if depth_feed.connected else "DISCONNECTED"
        l2_used = (stats.get('l2_legs_used', 0) if stats else 0)
        l2_full = (stats.get('l2_full_cycles', 0) if stats else 0)
        print(f"  feed L2: watchlist={len(depth_feed._target)} symbols, "
              f"{len(depth_feed.depth_books)} snapshots | "
              f"ws msgs={depth_feed.messages} | {d_conn} | "
              f"last scan: {l2_full} cycles fully on L2, {l2_used} legs")

    if opportunities:
        for i, opp in enumerate(opportunities, 1):
            surface = opp['surface_arb']
            net_usd = opp['net_rate_perc'] / 100 * NOTIONAL_USD
            print(f"\n  💰 OPPORTUNITY #{i} — BINANCE")
            print(f"  Route: {opp['contract_1']} -> {opp['contract_2']} -> "
                  f"{opp['contract_3']}")
            print(f"  Surface: {surface['profit_loss_perc']:.4f}% | "
                  f"Real: {opp['real_rate_perc']:.4f}% | "
                  f"NET: {opp['net_rate_perc']:.4f}%  "
                  f"(${net_usd:.2f} on ${NOTIONAL_USD:,})")

    print("=" * 60)


# ============================================================
# event-driven main loop
# ============================================================

async def run():
    print("=" * 60)
    print("WEBSOCKET TRIANGULAR ARBITRAGE SCANNER — BINANCE  (Phase 3)")
    print("=" * 60)
    print(f"Min NET: {MIN_NET_RATE}% (after {FEE_TYPE} fees + {SLIPPAGE_BUFFER}% slippage)")
    print(f"Event-driven recompute, up to {EVENT_SCAN_HZ} Hz. Aggregate slow-tick: "
          f"{AGGREGATE_LOG_SECONDS:g}s. Heartbeat: {HEARTBEAT_SECONDS:g}s.")
    print("=" * 60)

    adapter, triangles, symbols, snapshot = bootstrap()
    if not triangles:
        print("❌ No triangular pairs found after filtering.")
        return

    depth_gate = MIN_SURFACE_RATE
    if AUTO_DEPTH_GATE:
        hurdle = adapter.min_taker_fee() * 3 + SLIPPAGE_BUFFER + MIN_NET_RATE
        depth_gate = max(MIN_SURFACE_RATE, hurdle - DEPTH_GATE_MARGIN)
    print(f"  ↳ depth gate: {depth_gate:.2f}% "
          f"(3·{adapter.min_taker_fee():.2f}% fee + {SLIPPAGE_BUFFER:.2f}% slip "
          f"- {DEPTH_GATE_MARGIN:.2f}% margin)")

    tracker = SurfaceTracker(PRECURSOR_WINDOW)
    aggregate_history = deque(maxlen=AGGREGATE_TREND_WINDOW)
    prev_above_gate: dict = {}
    prev_precursors: set = set()
    events_logged: Counter = Counter()
    compute_samples = deque(maxlen=500)
    # Maps the route key used by precursor tracking back to the triangle dict,
    # so the L2 watchlist can be derived from the precursor set.
    combined_to_triangle = {t['combined']: t for t in triangles}

    feed = BinanceBookFeed(symbols, initial_books=snapshot)
    depth_feed = BinanceDepthFeed() if ENABLE_L2_WATCHLIST else None

    await feed.start()
    print("\n📡 L1 feed started — subscribing to per-symbol bookTicker streams...")
    for _ in range(50):
        await asyncio.sleep(0.2)
        if feed.connected and feed.messages > 0:
            break
    print(f"  ✓ L1 connected={feed.connected} | "
          f"{feed.coverage(symbols)}/{len(symbols)} symbols covered")

    if depth_feed is not None:
        await depth_feed.start()
        print(f"📡 L2 depth feed started — partial-book depth{20}@100ms; "
              f"watchlist initially empty (filled by precursor tracking).")

    print(f"\nEvent-driven scanning (Ctrl+C to stop)")
    print(f"  arrival events -> {EVENT_CSV_PREFIX}_*.csv | "
          f"aggregate -> {AGGREGATE_CSV_PREFIX}_*.csv")

    scan_min_interval = 1.0 / EVENT_SCAN_HZ
    last_scan_t = 0.0
    last_slow_tick = time.perf_counter()
    last_heartbeat = time.perf_counter()
    last_stats = None
    last_agg = None
    last_opportunities: list = []
    scan_count = 0

    try:
        while True:
            # Rate cap: enforce at least scan_min_interval between scans.
            now = time.perf_counter()
            wait = scan_min_interval - (now - last_scan_t)
            if wait > 0:
                await asyncio.sleep(wait)

            # If nothing changed during the cap window, block on the next update.
            if not feed.has_pending:
                await feed.update_event.wait()

            feed.consume_dirty()
            last_scan_t = time.perf_counter()
            slow_this = (last_scan_t - last_slow_tick) >= AGGREGATE_LOG_SECONDS

            t0 = time.perf_counter()
            opportunities, stats, events = scan(
                triangles, feed.books,
                depth_feed.depth_books if depth_feed is not None else None,
                adapter, depth_gate,
                tracker=tracker if slow_this else None,
                prev_above_gate=prev_above_gate,
            )
            compute_ms = (time.perf_counter() - t0) * 1000
            compute_samples.append(compute_ms)
            scan_count += 1
            last_stats = stats
            last_opportunities = opportunities

            # Fast-path: write each gate-crossing event with ms-precision timestamp.
            ts = datetime.now()
            for ev in events:
                ev['timestamp'] = ts
                log_event(ev)
                events_logged[ev['type']] += 1

            # Slow tick: aggregate, precursor transitions, aggregate CSV.
            if slow_this:
                last_slow_tick = last_scan_t
                agg = tracker.aggregate()
                if agg:
                    aggregate_history.append(agg['mean_ma'])
                    agg['rising'] = _series_rising(aggregate_history)
                    last_agg = agg

                if depth_gate > 0:
                    precursors = tracker.precursors(
                        PRECURSOR_GATE_FRACTION * depth_gate)
                    new_routes = {p[0] for p in precursors}
                    entered = new_routes - prev_precursors
                    exited = prev_precursors - new_routes
                    if entered or exited:
                        slow_ts = datetime.now()
                        ma_by_route = {p[0]: p[1] for p in precursors}
                        for route in entered:
                            log_event({
                                'timestamp': slow_ts,
                                'type': 'precursor_enter', 'route': route,
                                'surface_perc': ma_by_route.get(route),
                                'real_perc': None, 'net_perc': None,
                                'filled_fraction': None,
                            })
                            events_logged['precursor_enter'] += 1
                        for route in exited:
                            log_event({
                                'timestamp': slow_ts,
                                'type': 'precursor_exit', 'route': route,
                                'surface_perc': None, 'real_perc': None,
                                'net_perc': None, 'filled_fraction': None,
                            })
                            events_logged['precursor_exit'] += 1
                    prev_precursors = new_routes

                    # Drive the L2 watchlist from the current precursor set:
                    # every symbol that appears in any precursor route gets
                    # streamed at L2. set_watchlist is idempotent and diffs
                    # internally, so calling every slow tick is cheap.
                    if depth_feed is not None:
                        watchlist = set()
                        for route in new_routes:
                            tr = combined_to_triangle.get(route)
                            if tr:
                                watchlist.update((tr['pair_a'], tr['pair_b'], tr['pair_c']))
                        await depth_feed.set_watchlist(watchlist)

                if LOG_AGGREGATE_CSV and last_agg:
                    log_aggregate_csv({'aggregate': last_agg})

            # Console heartbeat.
            if last_scan_t - last_heartbeat >= HEARTBEAT_SECONDS:
                _print_heartbeat(scan_count, events_logged, compute_samples,
                                 last_stats, last_agg, feed, depth_feed,
                                 symbols, depth_gate, last_opportunities)
                last_heartbeat = last_scan_t

    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"\nStopped after {scan_count} scans. "
              f"Events: {dict(events_logged)}")
    finally:
        await feed.stop()
        if depth_feed is not None:
            await depth_feed.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nExiting...")
