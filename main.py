import asyncio
import json
import time

import func_arbitrage

COIN_PRICE_URL = "https://api.poloniex.com/markets/ticker24h"
MIN_SURFACE_RATE = 0.0
MIN_REAL_RATE = 0.1
SHOW_SCAN_SUMMARY = True
SHOW_LOW_PROFIT_WARNINGS = False
PAIRS_CACHE = "structured_triangular_pairs.json"


def step_0():
    print("Fetching tradeable coins from Poloniex...")
    coin_json = func_arbitrage.get_coin_tickers(COIN_PRICE_URL)
    if not coin_json:
        print("ERROR: Failed to fetch coin data")
        return []
    coin_list = func_arbitrage.collect_tradeables(coin_json)
    print(f"Found {len(coin_list)} tradeable pairs")
    return coin_list


def step_1(coin_list):
    print(f"Structuring {len(coin_list)} pairs into triangular combinations...")
    start_time = time.time()
    structured_list = func_arbitrage.structure_triangular_pairs(coin_list)
    elapsed = time.time() - start_time
    print(f"Found {len(structured_list)} triangular pairs in {elapsed:.2f}s")
    with open(PAIRS_CACHE, "w") as fp:
        json.dump(structured_list, fp, indent=2)
    return structured_list


async def step_2_async():
    try:
        with open(PAIRS_CACHE) as json_file:
            structured_pairs = json.load(json_file)
    except FileNotFoundError:
        print(f"ERROR: {PAIRS_CACHE} not found. Run steps 0 and 1 first.")
        return

    prices_json = func_arbitrage.get_coin_tickers(COIN_PRICE_URL)
    if not prices_json:
        print("ERROR: Failed to fetch price data")
        return

    opportunities_found = 0
    opportunities_above_threshold = 0
    opportunities_below_threshold = 0
    pairs_checked = 0

    for t_pair in structured_pairs:
        pairs_checked += 1
        try:
            prices_dict = func_arbitrage.get_price_for_t_pair(t_pair, prices_json)
            surface_arb, _ = func_arbitrage.calc_triangular_arb_surface_rate(t_pair, prices_dict)

            if surface_arb and surface_arb.get('profit_loss_perc', 0) >= MIN_SURFACE_RATE:
                real_rate_arb = await func_arbitrage.get_depth_from_orderbook_async(surface_arb)
                if real_rate_arb:
                    real_rate = real_rate_arb.get("real_rate_perc", 0)
                    if real_rate > 0:
                        opportunities_found += 1
                        if real_rate >= MIN_REAL_RATE:
                            opportunities_above_threshold += 1
                            _print_opportunity(opportunities_above_threshold, surface_arb, real_rate_arb, real_rate)
                        else:
                            opportunities_below_threshold += 1
                            if SHOW_LOW_PROFIT_WARNINGS:
                                print(f"⚠ Low profit: {surface_arb['contract_1']}->... {real_rate:.4f}%")
        except Exception as e:
            print(f"Error processing pair: {e}")

    if SHOW_SCAN_SUMMARY:
        print(f"✓ Checked {pairs_checked} pairs | Found {opportunities_found} total opportunities")
        if opportunities_above_threshold:
            print(f"  ├─ 💰 {opportunities_above_threshold} above {MIN_REAL_RATE}% threshold")
        if opportunities_below_threshold:
            print(f"  └─ ⚠ {opportunities_below_threshold} below threshold (not shown)")


def _print_opportunity(num, surface_arb, real_rate_arb, real_rate):
    print(f"\n{'=' * 60}")
    print(f"💰 OPPORTUNITY #{num} (Real: {real_rate:.4f}%)")
    print(f"{'=' * 60}")
    print(f"Route: {surface_arb['contract_1']} -> {surface_arb['contract_2']} -> {surface_arb['contract_3']}")
    print(f"Surface Rate: {surface_arb['profit_loss_perc']:.4f}%")
    print(f"Real Rate: {real_rate:.4f}%")
    print(f"Profit: {real_rate_arb['profit_loss']:.6f}")
    print(f"Direction: {surface_arb['direction']}")
    if 'BTC' in surface_arb['swap_1']:
        print(f"Est. Profit (USD): ${real_rate_arb['profit_loss'] * 76800:.2f}")
    print(f"{'=' * 60}\n")


async def main_async():
    print("=" * 60)
    print("TRIANGULAR ARBITRAGE SCANNER — POLONIEX")
    print("=" * 60)
    print(f"Min surface rate: {MIN_SURFACE_RATE}% | Min real rate: {MIN_REAL_RATE}%")
    print("=" * 60)

    coin_list = step_0()
    if not coin_list:
        return

    structured_pairs = step_1(coin_list)
    if not structured_pairs:
        return

    print("\n[Step 2] Scanning (Ctrl+C to stop)...")
    scan_count = 0
    try:
        while True:
            scan_count += 1
            print(f"\n>>> Scan #{scan_count} at {time.strftime('%H:%M:%S')}")
            start = time.time()
            await step_2_async()
            print(f"Scan completed in {time.time() - start:.2f}s\n")
            await asyncio.sleep(3)
    except KeyboardInterrupt:
        print(f"\nStopped after {scan_count} scans.")


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nExiting...")
