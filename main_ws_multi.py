"""Multi-venue WebSocket triangular arbitrage scanner (Binance + Kraken + KuCoin).

Three parallel event-driven scan loops, one per venue, sharing the same
methodology as `main_ws.py` (Phase 2/3) — volume + tick + fee filters, auto
depth gate, precursor tracker, aggregate trajectory, per-leg quote-age
diagnostics. Each venue logs to its own daily-rotated CSV pair
(`surface_aggregate_log_ws_<venue>_*.csv` and
`arrival_events_ws_<venue>_*.csv`) so the venues can be analysed
independently without joining on `exchange` column.

L2 partial-book streaming (Phase 3) runs on all three venues, each via the
venue's depth channel: Binance `depth20@100ms` (snapshot+diff), Kraken v2
`book` channel (snapshot+diff), KuCoin `/spotMarket/level2Depth50` (full
snapshot push every ~100 ms).

A combined heartbeat (every 5 s) prints a per-venue stanza covering events,
last-scan diagnostics, the best-surface route with leg quote-ages, the
aggregate trajectory, and feed health.
"""
import asyncio
import json
import statistics
import sys
import time
from collections import Counter, deque
from datetime import datetime

from binance_adapter import BinanceAdapter
from kraken_adapter import KrakenAdapter
from kucoin_adapter import KuCoinAdapter
from ws_binance import BinanceBookFeed, BinanceDepthFeed
from ws_kraken import KrakenBookFeed, KrakenDepthFeed
from ws_kucoin import KuCoinBookFeed, KuCoinDepthFeed
from surface_tracker import SurfaceTracker
from multi_exchange_manager import (
    filter_by_usd_volume,
    PRECURSOR_WINDOW, PRECURSOR_GATE_FRACTION, MAX_TICK_RATIO,
    AGGREGATE_TREND_WINDOW, _series_rising,
)
import main_ws as core            # scan(), log_event(), log_aggregate_csv(), _fmt_age(), _snapshot_books()
import func_arbitrage

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# ----- configuration (mirrors main_ws.py) -----
SLIPPAGE_BUFFER = 0.10
MIN_NET_RATE = 0.0
MIN_SURFACE_RATE = 0.0
FEE_TYPE = 'taker'
NOTIONAL_USD = 5000
AUTO_DEPTH_GATE = True
DEPTH_GATE_MARGIN = 0.10
MIN_USD_VOLUME = 50000
MIN_USD_VOLUME_BY_QUOTE = {
    'EUR': 250000, 'GBP': 250000, 'JPY': 250000,
    'CAD': 250000, 'AUD': 250000, 'CHF': 250000,
}

EVENT_SCAN_HZ = 20
AGGREGATE_LOG_SECONDS = 1.0
HEARTBEAT_SECONDS = 5.0
ENABLE_L2_WATCHLIST = True       # all three venues

LOG_AGGREGATE_CSV = True


# ============================================================
# per-venue bootstrap
# ============================================================

def bootstrap_binance():
    a = BinanceAdapter()
    tickers = a.get_all_tickers()
    raw = a.get_tradeable_pairs(tickers)
    pairs, dropped = filter_by_usd_volume(
        tickers, raw, MIN_USD_VOLUME, MIN_USD_VOLUME_BY_QUOTE)
    triangles = func_arbitrage.structure_triangular_pairs(pairs)
    symbols = {s for t in triangles
                  for s in (t['pair_a'], t['pair_b'], t['pair_c'])}
    snapshot = core._snapshot_books(a)
    return a, triangles, symbols, snapshot, len(raw), len(pairs), dropped


def bootstrap_kraken():
    a = KrakenAdapter()
    tickers = a.get_all_tickers()             # populates _symbol_to_wsname + tick sizes
    raw = a.get_tradeable_pairs(tickers)
    pairs, dropped = filter_by_usd_volume(
        tickers, raw, MIN_USD_VOLUME, MIN_USD_VOLUME_BY_QUOTE)
    triangles = func_arbitrage.structure_triangular_pairs(pairs)
    symbols = {s for t in triangles
                  for s in (t['pair_a'], t['pair_b'], t['pair_c'])}
    # Kraken v2 ticker channel pushes a snapshot on subscribe — no REST seed.
    return a, triangles, symbols, {}, len(raw), len(pairs), dropped


def bootstrap_kucoin():
    a = KuCoinAdapter()
    tickers = a.get_all_tickers()             # populates taker_fee_by_symbol + tick sizes
    raw = a.get_tradeable_pairs(tickers)
    pairs, dropped = filter_by_usd_volume(
        tickers, raw, MIN_USD_VOLUME, MIN_USD_VOLUME_BY_QUOTE)
    triangles = func_arbitrage.structure_triangular_pairs(pairs)
    symbols = {s for t in triangles
                  for s in (t['pair_a'], t['pair_b'], t['pair_c'])}
    return a, triangles, symbols, {}, len(raw), len(pairs), dropped


def _depth_gate(adapter):
    if not AUTO_DEPTH_GATE:
        return MIN_SURFACE_RATE
    hurdle = adapter.min_taker_fee() * 3 + SLIPPAGE_BUFFER + MIN_NET_RATE
    return max(MIN_SURFACE_RATE, hurdle - DEPTH_GATE_MARGIN)


# ============================================================
# per-venue scan loop
# ============================================================

async def venue_scan_loop(venue, ctx):
    """One venue's event-driven loop. `ctx` carries the venue's adapter, feed,
    optional L2 depth feed, triangles, tracker, gate, state-mutating
    containers, and the combined->triangle map for L2 watchlist promotion."""
    feed = ctx['feed']
    depth_feed = ctx['depth_feed']
    adapter = ctx['adapter']
    triangles = ctx['triangles']
    tracker = ctx['tracker']
    depth_gate = ctx['depth_gate']
    prev_above_gate = ctx['prev_above_gate']
    prev_precursors = ctx['prev_precursors']
    aggregate_history = ctx['aggregate_history']
    events_logged = ctx['events_logged']
    compute_samples = ctx['compute_samples']
    combined_to_triangle = ctx['combined_to_triangle']
    state = ctx['state']

    agg_csv_prefix = f"surface_aggregate_log_ws_{venue}"
    evt_csv_prefix = f"arrival_events_ws_{venue}"

    scan_min_interval = 1.0 / EVENT_SCAN_HZ
    last_scan_t = 0.0
    last_slow_tick = time.perf_counter()

    while True:
        now = time.perf_counter()
        wait = scan_min_interval - (now - last_scan_t)
        if wait > 0:
            await asyncio.sleep(wait)
        if not feed.has_pending:
            await feed.update_event.wait()
        feed.consume_dirty()
        last_scan_t = time.perf_counter()
        slow_this = (last_scan_t - last_slow_tick) >= AGGREGATE_LOG_SECONDS

        t0 = time.perf_counter()
        opportunities, stats, events = core.scan(
            triangles, feed.books,
            depth_feed.depth_books if depth_feed is not None else None,
            adapter, depth_gate,
            tracker=tracker if slow_this else None,
            prev_above_gate=prev_above_gate,
            exchange=venue,
        )
        compute_ms = (time.perf_counter() - t0) * 1000
        compute_samples.append(compute_ms)
        state['scan_count'] += 1
        state['last_stats'] = stats
        state['last_opportunities'] = opportunities

        ts = datetime.now()
        for ev in events:
            ev['timestamp'] = ts
            core.log_event(ev, exchange=venue, csv_prefix=evt_csv_prefix)
            events_logged[ev['type']] += 1

        if slow_this:
            last_slow_tick = last_scan_t
            agg = tracker.aggregate()
            if agg:
                aggregate_history.append(agg['mean_ma'])
                agg['rising'] = _series_rising(aggregate_history)
                state['last_agg'] = agg

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
                        core.log_event({
                            'timestamp': slow_ts, 'type': 'precursor_enter',
                            'route': route, 'surface_perc': ma_by_route.get(route),
                            'real_perc': None, 'net_perc': None,
                            'filled_fraction': None,
                        }, exchange=venue, csv_prefix=evt_csv_prefix)
                        events_logged['precursor_enter'] += 1
                    for route in exited:
                        core.log_event({
                            'timestamp': slow_ts, 'type': 'precursor_exit',
                            'route': route, 'surface_perc': None,
                            'real_perc': None, 'net_perc': None,
                            'filled_fraction': None,
                        }, exchange=venue, csv_prefix=evt_csv_prefix)
                        events_logged['precursor_exit'] += 1
                prev_precursors.clear()
                prev_precursors.update(new_routes)

                # L2 watchlist — all three venues now expose a depth_feed
                # when ENABLE_L2_WATCHLIST is on; otherwise depth_feed is None.
                if depth_feed is not None:
                    watchlist = set()
                    for route in new_routes:
                        tr = combined_to_triangle.get(route)
                        if tr:
                            watchlist.update((tr['pair_a'], tr['pair_b'], tr['pair_c']))
                    await depth_feed.set_watchlist(watchlist)

            if LOG_AGGREGATE_CSV and state['last_agg']:
                core.log_aggregate_csv({'aggregate': state['last_agg']},
                                       exchange=venue,
                                       csv_prefix=agg_csv_prefix)


# ============================================================
# combined heartbeat
# ============================================================

async def heartbeat_task(venue_ctxs):
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        _print_combined_heartbeat(venue_ctxs)


def _print_combined_heartbeat(venue_ctxs):
    print(f"\n>>> MULTI-VENUE HEARTBEAT at {time.strftime('%H:%M:%S')}")
    print("=" * 70)
    for venue, ctx in venue_ctxs.items():
        feed = ctx['feed']
        depth_feed = ctx['depth_feed']
        state = ctx['state']
        events_logged = ctx['events_logged']
        compute_samples = ctx['compute_samples']
        gate = ctx['depth_gate']
        symbols = ctx['symbols']
        stats = state.get('last_stats')
        agg = state.get('last_agg')

        e = events_logged
        print(f"  [{venue}] scans={state['scan_count']} | "
              f"events gate↑={e.get('gate_cross_up',0)} ↓={e.get('gate_cross_down',0)}  "
              f"pre+={e.get('precursor_enter',0)} -={e.get('precursor_exit',0)}")

        if stats:
            best_s = stats.get('best_surface_perc')
            best_r = stats.get('best_real_perc')
            best_n = stats.get('best_net_perc')
            bs = f"{best_s:.4f}%" if best_s is not None else "n/a"
            br = f"{best_r:.4f}%" if best_r is not None else "n/a"
            bn = f"{best_n:.4f}%" if best_n is not None else "n/a"
            gate_s = f"≥{gate:.2f}%" if gate else "off"
            print(f"          eval={stats['paths_evaluated']} +s={stats['positive_surface']} "
                  f"depth={stats['depth_checked']} (gate {gate_s}) "
                  f"thin={stats.get('book_thin',0)} | "
                  f"best surface={bs} real={br} net={bn}")
            route = stats.get('best_surface_route')
            if route:
                legs = route.split(',')
                if len(legs) == 3:
                    ages = ' | '.join(f"{leg}={core._fmt_age(feed.quote_age(leg))}"
                                       for leg in legs)
                    print(f"          best route: {route}")
                    print(f"            ages: {ages}")

        if agg:
            trend = "↑ rising" if agg.get('rising') else "— flat"
            print(f"          aggregate: mean MA={agg['mean_ma']:.4f}% "
                  f"σ={agg['dispersion']:.4f}% routes={agg['route_count']} {trend}")

        if compute_samples:
            samples = list(compute_samples)
            p50 = statistics.median(samples)
            p99 = (sorted(samples)[int(len(samples)*0.99)]
                   if len(samples) >= 100 else max(samples))
            print(f"          compute: median {p50:.2f}ms | p99 {p99:.2f}ms | n={len(samples)}")

        l1c = "connected" if feed.connected else "DISCONNECTED"
        l1_line = (f"          feed L1: {feed.coverage(symbols)}/{len(symbols)} symbols "
                   f"| msgs={feed.messages} | {l1c}")
        print(l1_line)
        if depth_feed is not None:
            l2c = "connected" if depth_feed.connected else "DISCONNECTED"
            l2_used = (stats.get('l2_legs_used', 0) if stats else 0)
            l2_full = (stats.get('l2_full_cycles', 0) if stats else 0)
            print(f"          feed L2: watchlist={len(depth_feed._target)} "
                  f"snapshots={len(depth_feed.depth_books)} | msgs={depth_feed.messages} | "
                  f"{l2c} | last scan: {l2_full} cycles full L2, {l2_used} legs")
    print("=" * 70)


# ============================================================
# orchestration
# ============================================================

async def run():
    print("=" * 70)
    print("MULTI-VENUE WEBSOCKET SCANNER  (Binance + Kraken + KuCoin)")
    print("=" * 70)
    print(f"Min NET: {MIN_NET_RATE}% (after {FEE_TYPE} fees + {SLIPPAGE_BUFFER}% slippage)")
    print(f"Event-driven recompute, up to {EVENT_SCAN_HZ} Hz per venue. "
          f"Slow tick: {AGGREGATE_LOG_SECONDS:g}s. Heartbeat: {HEARTBEAT_SECONDS:g}s.")
    print("=" * 70)

    print("\n📥 Bootstrapping all three venues via REST...")
    boots = {
        'binance': bootstrap_binance(),
        'kraken':  bootstrap_kraken(),
        'kucoin':  bootstrap_kucoin(),
    }
    for v, (_, tri, syms, snap, raw, kept, dropped) in boots.items():
        print(f"  [{v}] {raw} tradeable -> {kept} after volume filter (dropped {dropped}) | "
              f"{len(tri)} triangles | {len(syms)} symbols | seeded {len(snap)} L1 books")
    # Persist each venue's triangle set, matching the REST scanner's artifact.
    for v, (_, tri, _, _, _, _, _) in boots.items():
        fname = f"triangular_pairs_{v}.json"
        with open(fname, "w") as fp:
            json.dump(tri, fp, indent=2)
        print(f"  💾 saved {len(tri)} {v} triangles -> {fname}")

    # Build per-venue context
    venue_ctxs = {}
    for venue in ('binance', 'kraken', 'kucoin'):
        adapter, triangles, symbols, snapshot, _, _, _ = boots[venue]
        if not triangles:
            print(f"  ⚠ [{venue}] no triangles, skipping")
            continue
        gate = _depth_gate(adapter)
        print(f"  [{venue}] depth gate: {gate:.2f}% "
              f"(3·{adapter.min_taker_fee():.2f}% fee + {SLIPPAGE_BUFFER:.2f}% slip "
              f"− {DEPTH_GATE_MARGIN:.2f}% margin)")
        if venue == 'binance':
            feed = BinanceBookFeed(symbols, initial_books=snapshot)
            depth_feed = BinanceDepthFeed() if ENABLE_L2_WATCHLIST else None
        elif venue == 'kraken':
            feed = KrakenBookFeed(symbols, adapter._symbol_to_wsname)
            depth_feed = (KrakenDepthFeed(adapter._symbol_to_wsname)
                          if ENABLE_L2_WATCHLIST else None)
        elif venue == 'kucoin':
            feed = KuCoinBookFeed(symbols)
            depth_feed = (KuCoinDepthFeed(symbols)
                          if ENABLE_L2_WATCHLIST else None)
        venue_ctxs[venue] = {
            'adapter': adapter,
            'triangles': triangles,
            'symbols': symbols,
            'feed': feed,
            'depth_feed': depth_feed,
            'tracker': SurfaceTracker(PRECURSOR_WINDOW),
            'depth_gate': gate,
            'prev_above_gate': {},
            'prev_precursors': set(),
            'aggregate_history': deque(maxlen=AGGREGATE_TREND_WINDOW),
            'events_logged': Counter(),
            'compute_samples': deque(maxlen=500),
            'combined_to_triangle': {t['combined']: t for t in triangles},
            'state': {'scan_count': 0, 'last_stats': None,
                      'last_agg': None, 'last_opportunities': []},
        }

    # Start every feed in parallel
    print("\n📡 Starting feeds...")
    starts = []
    for venue, ctx in venue_ctxs.items():
        starts.append(ctx['feed'].start())
        if ctx['depth_feed'] is not None:
            starts.append(ctx['depth_feed'].start())
    await asyncio.gather(*starts)

    # Warmup: wait until each venue's L1 feed shows >= 1 message
    print("  awaiting first messages from each feed...")
    for _ in range(60):
        await asyncio.sleep(0.25)
        if all(ctx['feed'].connected and ctx['feed'].messages > 0
               for ctx in venue_ctxs.values()):
            break
    for venue, ctx in venue_ctxs.items():
        feed = ctx['feed']
        print(f"  [{venue}] L1 connected={feed.connected} | "
              f"{feed.coverage(ctx['symbols'])}/{len(ctx['symbols'])} symbols covered")

    print("\nEvent-driven scanning across all venues (Ctrl+C to stop)")
    print(f"  per-venue logs: surface_aggregate_log_ws_<venue>_*.csv | "
          f"arrival_events_ws_<venue>_*.csv")

    # Launch one scan task per venue + the combined heartbeat
    tasks = [asyncio.create_task(venue_scan_loop(v, ctx))
             for v, ctx in venue_ctxs.items()]
    tasks.append(asyncio.create_task(heartbeat_task(venue_ctxs)))

    try:
        # Run until any task crashes or external cancellation
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nStopping...")
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        for venue, ctx in venue_ctxs.items():
            stops = [ctx['feed'].stop()]
            if ctx['depth_feed'] is not None:
                stops.append(ctx['depth_feed'].stop())
            await asyncio.gather(*stops, return_exceptions=True)
        summary = ", ".join(
            f"{v}: scans={c['state']['scan_count']} events={dict(c['events_logged'])}"
            for v, c in venue_ctxs.items()
        )
        print(f"\nStopped. {summary}")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nExiting...")
