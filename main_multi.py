import asyncio
import json
import time

from multi_exchange_manager import MultiExchangeManager

ENABLED_EXCHANGES = ['poloniex', 'binance', 'kraken', 'kucoin']
MIN_SURFACE_RATE = 0.0   # only fetch order books when surface profit >= this
SLIPPAGE_BUFFER = 0.10   # % subtracted on top of fees — L20 book walks overstate fills
MIN_NET_RATE = 0.0       # opportunity must clear fees + slippage by at least this %
FEE_TYPE = 'taker'       # triangular legs cross the spread, so taker fees apply
NOTIONAL_USD = 5000      # assumed capital, for the net-profit dollar estimate
SHOW_SCAN_SUMMARY = True
SHOW_SCAN_DIAGNOSTICS = True  # best surface/real/net % per exchange each scan


def step_0_multi():
    print("\n[Step 0] Retrieving tradeable coins from all exchanges...")
    print("=" * 60)
    manager = MultiExchangeManager(ENABLED_EXCHANGES)
    print(manager.get_summary())
    print("=" * 60)

    print("\n📥 Fetching market data...")
    exchange_tickers = manager.get_all_tickers_sync()

    print("\n🔍 Filtering tradeable pairs...")
    tradeable_pairs = manager.get_tradeable_pairs(exchange_tickers)
    total_pairs = sum(len(pairs) for pairs in tradeable_pairs.values())
    print(f"\n✓ Total tradeable pairs: {total_pairs}")
    return manager, tradeable_pairs, exchange_tickers


def step_1_multi(manager, tradeable_pairs):
    print("\n[Step 1] Structuring triangular pairs...")
    print("=" * 60)
    start_time = time.time()
    triangular_pairs = manager.structure_triangular_pairs(tradeable_pairs)
    elapsed = time.time() - start_time
    total_triangles = sum(len(pairs) for pairs in triangular_pairs.values())
    print(f"\n✓ Total triangular pairs: {total_triangles} ({elapsed:.2f}s)")
    print("=" * 60)

    for exchange, pairs in triangular_pairs.items():
        filename = f"triangular_pairs_{exchange}.json"
        with open(filename, "w") as fp:
            json.dump(pairs, fp, indent=2)
        print(f"💾 Saved {exchange} -> {filename}")
    return triangular_pairs


async def step_2_multi_async(manager, triangular_pairs):
    print(f"\n[Step 2] Scanning {len(triangular_pairs)} exchanges...")
    print("=" * 60)

    tasks = [
        manager.scan_exchange_async(exchange, t_pairs, MIN_SURFACE_RATE,
                                    MIN_NET_RATE, SLIPPAGE_BUFFER, FEE_TYPE)
        for exchange, t_pairs in triangular_pairs.items()
    ]
    exchange_names = list(triangular_pairs.keys())
    results = await asyncio.gather(*tasks)

    opportunities_by_exchange = {}
    stats_by_exchange = {}
    for exchange, result in zip(exchange_names, results):
        opportunities_by_exchange[exchange] = result['opportunities']
        stats_by_exchange[exchange] = result['stats']

    total_above_threshold = sum(len(opps) for opps in opportunities_by_exchange.values())

    for exchange, opportunities in opportunities_by_exchange.items():
        print(f"  ✓ {exchange}: {len(opportunities)} opportunities (net ≥ {MIN_NET_RATE}% after fees)")
        if SHOW_SCAN_DIAGNOSTICS:
            s = stats_by_exchange[exchange]
            best_s = s['best_surface_perc']
            best_r = s['best_real_perc']
            best_n = s.get('best_net_perc')
            best_s_str = f"{best_s:.4f}%" if best_s is not None else "n/a"
            best_r_str = f"{best_r:.4f}%" if best_r is not None else "n/a"
            best_n_str = f"{best_n:.4f}%" if best_n is not None else "n/a"
            print(
                f"      diag: evaluated={s['paths_evaluated']} | "
                f"+surface={s['positive_surface']} | "
                f"depth_checks={s['depth_checked']} | "
                f"best surface={best_s_str} | best real={best_r_str} | best net={best_n_str}"
            )
            if s['missing_prices']:
                print(f"      ⚠ {s['missing_prices']} pairs skipped (missing bid/ask)")
            if s.get('errors'):
                print(f"      ❌ {s['errors']} pairs raised errors — last: {s.get('last_error')}")
            if s['no_path'] and s['paths_evaluated'] == 0:
                print(f"      ⚠ {s['no_path']} pairs: triangle path logic did not match")
            elif s['no_path']:
                print(f"      ⚠ {s['no_path']} pairs: no routable path (of {s['paths_evaluated']} evaluated)")

    if total_above_threshold:
        opp_num = 1
        for exchange, opportunities in opportunities_by_exchange.items():
            for opp in opportunities:
                surface = opp['surface_arb']
                net_rate = opp['net_rate_perc']
                net_usd = (net_rate / 100) * NOTIONAL_USD
                print(f"\n{'=' * 60}")
                print(f"💰 OPPORTUNITY #{opp_num} — {exchange.upper()}")
                print(f"Route: {opp['contract_1']} -> {opp['contract_2']} -> {opp['contract_3']}")
                print(f"Surface: {surface['profit_loss_perc']:.4f}% | Real: {opp['real_rate_perc']:.4f}%")
                print(f"Fees (3 legs): -{opp['fee_perc']:.4f}% | Slippage buffer: -{opp['slippage_buffer_perc']:.4f}%")
                print(f"NET (after fees): {net_rate:.4f}% | Direction: {surface['direction']}")
                print(f"Est. Net Profit on ${NOTIONAL_USD:,}: ${net_usd:.2f}")
                print(f"{'=' * 60}")
                opp_num += 1

    if SHOW_SCAN_SUMMARY:
        total_pairs = sum(len(pairs) for pairs in triangular_pairs.values())
        print(f"\n{'=' * 60}")
        print("SCAN SUMMARY")
        for exchange in triangular_pairs:
            n_pairs = len(triangular_pairs[exchange])
            n_opps = len(opportunities_by_exchange.get(exchange, []))
            print(f"  {exchange}: {n_pairs} pairs, {n_opps} opportunities")
        print(f"  Total: {total_pairs} pairs | {total_above_threshold} above threshold")
        print(f"{'=' * 60}\n")

    return opportunities_by_exchange


async def main_multi_async():
    print("=" * 60)
    print("MULTI-EXCHANGE TRIANGULAR ARBITRAGE SCANNER")
    print("=" * 60)
    print(f"Exchanges: {', '.join(e.capitalize() for e in ENABLED_EXCHANGES)}")
    print(f"Min surface: {MIN_SURFACE_RATE}% | Min NET: {MIN_NET_RATE}% "
          f"(after {FEE_TYPE} fees + {SLIPPAGE_BUFFER}% slippage)")
    print("(An opportunity must be profitable AFTER 3 legs of fees — raw 'real rate' is not enough.)")
    print("=" * 60)

    manager, tradeable_pairs, _ = step_0_multi()
    if not tradeable_pairs:
        print("❌ No tradeable pairs found.")
        return

    triangular_pairs = step_1_multi(manager, tradeable_pairs)
    if not triangular_pairs:
        print("❌ No triangular pairs found.")
        return

    print("\nContinuous scanning (Ctrl+C to stop)")
    scan_count = 0
    try:
        while True:
            scan_count += 1
            print(f"\n>>> SCAN #{scan_count} at {time.strftime('%H:%M:%S')}")
            start_time = time.time()
            await step_2_multi_async(manager, triangular_pairs)
            print(f"⏱  Scan completed in {time.time() - start_time:.2f}s\n")
            await asyncio.sleep(3)
    except KeyboardInterrupt:
        print(f"\nStopped after {scan_count} scans.")


if __name__ == "__main__":
    try:
        asyncio.run(main_multi_async())
    except KeyboardInterrupt:
        print("\nExiting...")
