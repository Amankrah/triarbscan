import asyncio
import json
import time

from multi_exchange_manager import MultiExchangeManager

ENABLED_EXCHANGES = ['poloniex', 'binance', 'kraken', 'kucoin']
MIN_SURFACE_RATE = 0.0
MIN_REAL_RATE = 0.1
SHOW_SCAN_SUMMARY = True


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
        manager.scan_exchange_async(exchange, t_pairs, MIN_SURFACE_RATE, MIN_REAL_RATE)
        for exchange, t_pairs in triangular_pairs.items()
    ]
    exchange_names = list(triangular_pairs.keys())
    results = await asyncio.gather(*tasks)

    opportunities_by_exchange = dict(zip(exchange_names, results))
    total_above_threshold = sum(len(opps) for opps in results)

    for exchange, opportunities in opportunities_by_exchange.items():
        print(f"  ✓ {exchange}: {len(opportunities)} opportunities ≥ {MIN_REAL_RATE}%")

    if total_above_threshold:
        opp_num = 1
        for exchange, opportunities in opportunities_by_exchange.items():
            for opp in opportunities:
                surface = opp['surface_arb']
                print(f"\n{'=' * 60}")
                print(f"💰 OPPORTUNITY #{opp_num} — {exchange.upper()}")
                print(f"Route: {opp['contract_1']} -> {opp['contract_2']} -> {opp['contract_3']}")
                print(f"Surface: {surface['profit_loss_perc']:.4f}% | Real: {opp['real_rate_perc']:.4f}%")
                print(f"Profit: {opp['profit_loss']:.6f} | Direction: {surface['direction']}")
                if 'BTC' in surface['swap_1']:
                    print(f"Est. Profit (USD): ${opp['profit_loss'] * 76800:.2f}")
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
    print(f"Min surface: {MIN_SURFACE_RATE}% | Min real: {MIN_REAL_RATE}%")
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
