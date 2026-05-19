"""WebSocket-driven triangular arbitrage scanner (Binance).

Replaces the REST poll loop in main_multi.py. Order books are kept live in
memory by ws_binance.BinanceBookFeed; each "scan" is a pure in-memory
recompute (sub-millisecond) over the triangle set.

Bootstrap still uses one REST call to discover symbols/triangles and to seed
the book mirror — that is a one-time cost, not part of the scan loop.

Depth note: this MVP uses the top-of-book size from !bookTicker as a 1-level
depth check. A trade that does not fit at L1 is rejected by
calculate_acquired_coin (returns 0). Full L2 depth would need per-symbol
@depth streams — that is the next iteration.
"""
import asyncio
import sys
import time

from binance_adapter import BinanceAdapter
from fee_calculator import calculate_fee_impact
from ws_binance import BinanceBookFeed
import func_arbitrage

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

SLIPPAGE_BUFFER = 0.10   # % subtracted on top of fees (L1 fills are optimistic)
MIN_NET_RATE = 0.0       # opportunity must clear fees + slippage by at least this %
FEE_TYPE = 'taker'       # triangular legs cross the spread
NOTIONAL_USD = 5000      # assumed capital, for the net-profit dollar estimate
SCAN_INTERVAL = 2.0      # seconds between prints; the compute itself is sub-ms
STARTING_AMOUNTS = {"USDT": 100, "USDC": 100, "BTC": 0.05, "ETH": 0.1}


def bootstrap():
    """One-time REST: discover triangles and seed the live book mirror."""
    adapter = BinanceAdapter()
    print("📥 Bootstrapping symbols + triangles from Binance REST...")
    tickers = adapter.get_all_tickers()
    pairs = adapter.get_tradeable_pairs(tickers)
    triangles = func_arbitrage.structure_triangular_pairs(pairs)
    symbols = set()
    for t in triangles:
        symbols.update((t['pair_a'], t['pair_b'], t['pair_c']))
    snapshot = _snapshot_books(adapter)
    print(f"  ✓ {len(pairs)} tradeable pairs | {len(triangles)} triangles | "
          f"{len(symbols)} symbols | seeded {len(snapshot)} books")
    return adapter, triangles, symbols, snapshot


def _snapshot_books(adapter: BinanceAdapter) -> dict:
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


def _real_rate(surface_arb, books):
    """Walk the L1 book mirror, mirroring the REST scanner's depth math."""
    dirs = (surface_arb['direction_trade_1'],
            surface_arb['direction_trade_2'],
            surface_arb['direction_trade_3'])
    contracts = (surface_arb['contract_1'],
                 surface_arb['contract_2'],
                 surface_arb['contract_3'])
    depths = [func_arbitrage.reformated_orderbook(_l1_orderbook(c, books), d)
              for c, d in zip(contracts, dirs)]
    start = STARTING_AMOUNTS.get(surface_arb['swap_1'], 100)
    t1 = func_arbitrage.calculate_acquired_coin(start, depths[0])
    t2 = func_arbitrage.calculate_acquired_coin(t1, depths[1])
    t3 = func_arbitrage.calculate_acquired_coin(t2, depths[2])
    profit = t3 - start
    return profit, (profit / start * 100 if start else 0)


def scan(triangles, books):
    """Pure in-memory recompute. Synchronous — atomic w.r.t. the WS task."""
    opportunities = []
    stats = {
        'paths_evaluated': 0, 'positive_surface': 0, 'missing_prices': 0,
        'depth_checked': 0, 'best_surface_perc': None,
        'best_real_perc': None, 'best_net_perc': None,
    }
    for t_pair in triangles:
        prices = _prices(t_pair, books)
        if prices is None:
            stats['missing_prices'] += 1
            continue
        surface_arb, best_rate = func_arbitrage.calc_triangular_arb_surface_rate(
            t_pair, prices)
        if best_rate is not None:
            stats['paths_evaluated'] += 1
            if stats['best_surface_perc'] is None or best_rate > stats['best_surface_perc']:
                stats['best_surface_perc'] = best_rate
        if not surface_arb:
            continue
        stats['positive_surface'] += 1
        stats['depth_checked'] += 1

        profit, real = _real_rate(surface_arb, books)
        if stats['best_real_perc'] is None or real > stats['best_real_perc']:
            stats['best_real_perc'] = real

        fee = calculate_fee_impact('binance', real, FEE_TYPE)
        net = (fee['net_profit_perc'] or 0) - SLIPPAGE_BUFFER
        if stats['best_net_perc'] is None or net > stats['best_net_perc']:
            stats['best_net_perc'] = net

        if net >= MIN_NET_RATE:
            opportunities.append({
                'exchange': 'binance',
                'contract_1': surface_arb['contract_1'],
                'contract_2': surface_arb['contract_2'],
                'contract_3': surface_arb['contract_3'],
                'real_rate_perc': real,
                'profit_loss': profit,
                'fee_perc': fee['total_fee_3_trades'],
                'slippage_buffer_perc': SLIPPAGE_BUFFER,
                'net_rate_perc': net,
                'surface_arb': surface_arb,
            })
    return opportunities, stats


def _print_scan(scan_count, opportunities, stats, feed, symbols, compute_ms):
    print(f"\n>>> SCAN #{scan_count} at {time.strftime('%H:%M:%S')}  (binance, websocket)")
    print("=" * 60)

    def pct(v):
        return f"{v:.4f}%" if v is not None else "n/a"

    print(f"  ✓ binance: {len(opportunities)} opportunities "
          f"(net ≥ {MIN_NET_RATE}% after fees)")
    print(f"      diag: evaluated={stats['paths_evaluated']} | "
          f"+surface={stats['positive_surface']} | "
          f"depth_checks={stats['depth_checked']} | "
          f"best surface={pct(stats['best_surface_perc'])} | "
          f"best real={pct(stats['best_real_perc'])} | "
          f"best net={pct(stats['best_net_perc'])}")
    if stats['missing_prices']:
        print(f"      ⚠ {stats['missing_prices']} triangles skipped (symbol not in feed yet)")
    conn = "connected" if feed.connected else "DISCONNECTED"
    print(f"      live: {feed.coverage(symbols)}/{len(symbols)} symbols | "
          f"ws msgs={feed.messages} | compute={compute_ms:.2f}ms | feed {conn}")

    for i, opp in enumerate(opportunities, 1):
        surface = opp['surface_arb']
        net_usd = opp['net_rate_perc'] / 100 * NOTIONAL_USD
        print(f"\n{'=' * 60}")
        print(f"💰 OPPORTUNITY #{i} — BINANCE")
        print(f"Route: {opp['contract_1']} -> {opp['contract_2']} -> {opp['contract_3']}")
        print(f"Surface: {surface['profit_loss_perc']:.4f}% | "
              f"Real: {opp['real_rate_perc']:.4f}%")
        print(f"Fees (3 legs): -{opp['fee_perc']:.4f}% | "
              f"Slippage buffer: -{opp['slippage_buffer_perc']:.4f}%")
        print(f"NET (after fees): {opp['net_rate_perc']:.4f}% | "
              f"Direction: {surface['direction']}")
        print(f"Est. Net Profit on ${NOTIONAL_USD:,}: ${net_usd:.2f}")
        print(f"{'=' * 60}")

    print(f"\nTotal: {len(opportunities)} above threshold")
    print("=" * 60)


async def run():
    print("=" * 60)
    print("WEBSOCKET TRIANGULAR ARBITRAGE SCANNER — BINANCE")
    print("=" * 60)
    print(f"Min NET: {MIN_NET_RATE}% (after {FEE_TYPE} fees + {SLIPPAGE_BUFFER}% slippage)")
    print("Order books are live (WS push); each scan is an in-memory recompute.")
    print("=" * 60)

    adapter, triangles, symbols, snapshot = bootstrap()

    feed = BinanceBookFeed(symbols, initial_books=snapshot)
    await feed.start()
    print("\n📡 WebSocket feed started — subscribing to per-symbol streams...")
    for _ in range(50):  # up to ~10s for all connections to subscribe
        await asyncio.sleep(0.2)
        if feed.connected and feed.messages > 0:
            break
    print(f"  ✓ feed connected={feed.connected} | "
          f"{feed.coverage(symbols)}/{len(symbols)} symbols covered")

    print("\nContinuous scanning (Ctrl+C to stop)")
    scan_count = 0
    try:
        while True:
            scan_count += 1
            t0 = time.perf_counter()
            opportunities, stats = scan(triangles, feed.books)
            compute_ms = (time.perf_counter() - t0) * 1000
            _print_scan(scan_count, opportunities, stats, feed, symbols, compute_ms)
            await asyncio.sleep(SCAN_INTERVAL)
    except KeyboardInterrupt:
        print(f"\nStopped after {scan_count} scans.")
    finally:
        await feed.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nExiting...")
