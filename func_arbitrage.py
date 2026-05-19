import asyncio
import time
import aiohttp
import requests

POLONIEX_ORDERBOOK = "https://api.poloniex.com/markets/{symbol}/orderBook?limit=20"
STARTING_AMOUNTS = {"USDT": 100, "USDC": 100, "BTC": 0.05, "ETH": 0.1}


def get_coin_tickers(url, max_retries=3, retry_delay=1):
    for attempt in range(max_retries):
        try:
            req = requests.get(url, timeout=10)
            req.raise_for_status()
            return req.json()
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠ Attempt {attempt + 1}/{max_retries} failed: {type(e).__name__}")
                time.sleep(retry_delay)
            else:
                print(f"❌ All {max_retries} attempts failed for {url}: {e}")
                return {}
    return {}


async def get_coin_tickers_async(session, url, max_retries=3, retry_delay=1):
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠ Async attempt {attempt + 1}/{max_retries} failed: {type(e).__name__}")
                await asyncio.sleep(retry_delay)
            else:
                print(f"❌ All async {max_retries} attempts failed for {url}: {e}")
                return {}
    return {}


def collect_tradeables(json_obj):
    if isinstance(json_obj, list):
        return [
            item['symbol'] for item in json_obj
            if item.get('symbol') and item.get('close')
            and float(item.get('close', 0)) > 0 and float(item.get('quantity', 0)) > 0
        ]
    return [
        coin for coin, data in json_obj.items()
        if data.get("isFrozen") == "0" and data.get("postOnly") == "0"
    ]


def structure_triangular_pairs(coin_list):
    """Find unique 3-leg cycles; O(n²) via currency index."""
    triangular_pairs_list = []
    seen = set()
    currency_pairs_map = {}

    for pair in coin_list:
        base, quote = pair.split("_")
        currency_pairs_map.setdefault(base, []).append((pair, base, quote, 'base'))
        currency_pairs_map.setdefault(quote, []).append((pair, base, quote, 'quote'))

    for pair_a in coin_list:
        a_base, a_quote = pair_a.split("_")
        for pair_b, b_base, b_quote, _ in currency_pairs_map.get(a_quote, []):
            if pair_b == pair_a:
                continue
            intermediate = b_base if b_quote == a_quote else b_quote
            for pair_c, c_base, c_quote, _ in currency_pairs_map.get(intermediate, []):
                if pair_c in (pair_a, pair_b):
                    continue
                if (c_base == intermediate and c_quote == a_base) or \
                   (c_quote == intermediate and c_base == a_base):
                    key = ''.join(sorted([pair_a, pair_b, pair_c]))
                    if key not in seen:
                        seen.add(key)
                        triangular_pairs_list.append({
                            "a_base": a_base, "b_base": b_base, "c_base": c_base,
                            "a_quote": a_quote, "b_quote": b_quote, "c_quote": c_quote,
                            "pair_a": pair_a, "pair_b": pair_b, "pair_c": pair_c,
                            "combined": f"{pair_a},{pair_b},{pair_c}",
                        })
    return triangular_pairs_list


def get_price_for_t_pair(t_pair, prices_json):
    pair_a, pair_b, pair_c = t_pair["pair_a"], t_pair["pair_b"], t_pair["pair_c"]
    try:
        prices_dict = (
            {item['symbol']: item for item in prices_json}
            if isinstance(prices_json, list) else prices_json
        )
        pair_a_data = prices_dict.get(pair_a, {})
        pair_b_data = prices_dict.get(pair_b, {})
        pair_c_data = prices_dict.get(pair_c, {})
        return {
            "pair_a_ask": float(pair_a_data.get("ask") or pair_a_data.get("lowestAsk") or 0),
            "pair_a_bid": float(pair_a_data.get("bid") or pair_a_data.get("highestBid") or 0),
            "pair_b_ask": float(pair_b_data.get("ask") or pair_b_data.get("lowestAsk") or 0),
            "pair_b_bid": float(pair_b_data.get("bid") or pair_b_data.get("highestBid") or 0),
            "pair_c_ask": float(pair_c_data.get("ask") or pair_c_data.get("lowestAsk") or 0),
            "pair_c_bid": float(pair_c_data.get("bid") or pair_c_data.get("highestBid") or 0),
        }
    except (KeyError, ValueError, TypeError) as e:
        print(f"Error extracting prices for {pair_a},{pair_b},{pair_c}: {e}")
        return {k: 0 for k in (
            "pair_a_ask", "pair_a_bid", "pair_b_ask", "pair_b_bid", "pair_c_ask", "pair_c_bid"
        )}


def calc_triangular_arb_surface_rate(t_pair, prices_dict):
    """Return (profitable_opportunity, best_surface_perc).

    profitable_opportunity is non-empty only when the best path has profit > 0.
    best_surface_perc is the best rate found across forward/reverse (may be negative).
    """
    starting_amount = 1
    min_profitable_rate = 0
    surface_dict = {}
    best_surface_perc = None

    a_base, a_quote = t_pair["a_base"], t_pair["a_quote"]
    b_base, b_quote = t_pair["b_base"], t_pair["b_quote"]
    c_base, c_quote = t_pair["c_base"], t_pair["c_quote"]
    pair_a, pair_b, pair_c = t_pair["pair_a"], t_pair["pair_b"], t_pair["pair_c"]

    a_ask, a_bid = prices_dict["pair_a_ask"], prices_dict["pair_a_bid"]
    b_ask, b_bid = prices_dict["pair_b_ask"], prices_dict["pair_b_bid"]
    c_ask, c_bid = prices_dict["pair_c_ask"], prices_dict["pair_c_bid"]

    if not all([a_ask, a_bid, b_ask, b_bid, c_ask, c_bid]):
        return {}, None

    for direction in ("forward", "reverse"):
        calculated = False
        contract_2, contract_3 = "", ""
        direction_trade_2, direction_trade_3 = "", ""
        acquired_coin_t2, acquired_coin_t3 = 0, 0

        if direction == "forward":
            swap_1, swap_2 = a_base, a_quote
            swap_1_rate = 1 / a_ask
            direction_trade_1 = "base_to_quote"
        else:
            swap_1, swap_2 = a_quote, a_base
            swap_1_rate = a_bid
            direction_trade_1 = "quote_to_base"

        contract_1 = pair_a
        acquired_coin_t1 = starting_amount * swap_1_rate

        if direction == "forward" and not calculated:
            if a_quote == b_quote:
                swap_2_rate = b_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "quote_to_base", pair_b, b_base
                if b_base == c_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / c_ask, "base_to_quote", pair_c
                elif b_base == c_quote:
                    swap_3_rate, direction_trade_3, contract_3 = c_bid, "quote_to_base", pair_c
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True
            elif a_quote == b_base and not calculated:
                swap_2_rate = 1 / b_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "base_to_quote", pair_b, b_quote
                if b_quote == c_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / c_ask, "base_to_quote", pair_c
                elif b_quote == c_quote:
                    swap_3_rate, direction_trade_3, contract_3 = c_bid, "quote_to_base", pair_c
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True
            elif a_quote == c_quote and not calculated:
                swap_2_rate = c_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "quote_to_base", pair_c, c_base
                if c_base == b_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / b_ask, "base_to_quote", pair_b
                elif c_base == b_quote:
                    swap_3_rate, direction_trade_3, contract_3 = b_bid, "quote_to_base", pair_b
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True
            elif a_quote == c_base and not calculated:
                swap_2_rate = 1 / c_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "base_to_quote", pair_c, c_quote
                if c_quote == b_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / b_ask, "base_to_quote", pair_b
                elif c_quote == b_quote:
                    swap_3_rate, direction_trade_3, contract_3 = b_bid, "quote_to_base", pair_b
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True

        if direction == "reverse" and not calculated:
            if a_base == b_quote:
                swap_2_rate = b_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "quote_to_base", pair_b, b_base
                if b_base == c_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / c_ask, "base_to_quote", pair_c
                elif b_base == c_quote:
                    swap_3_rate, direction_trade_3, contract_3 = c_bid, "quote_to_base", pair_c
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True
            elif a_base == b_base and not calculated:
                swap_2_rate = 1 / b_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "base_to_quote", pair_b, b_quote
                if b_quote == c_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / c_ask, "base_to_quote", pair_c
                elif b_quote == c_quote:
                    swap_3_rate, direction_trade_3, contract_3 = c_bid, "quote_to_base", pair_c
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True
            elif a_base == c_quote and not calculated:
                swap_2_rate = c_bid
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "quote_to_base", pair_c, c_base
                if c_base == b_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / b_ask, "base_to_quote", pair_b
                elif c_base == b_quote:
                    swap_3_rate, direction_trade_3, contract_3 = b_bid, "quote_to_base", pair_b
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True
            elif a_base == c_base and not calculated:
                swap_2_rate = 1 / c_ask
                acquired_coin_t2 = acquired_coin_t1 * swap_2_rate
                direction_trade_2, contract_2, swap_3 = "base_to_quote", pair_c, c_quote
                if c_quote == b_base:
                    swap_3_rate, direction_trade_3, contract_3 = 1 / b_ask, "base_to_quote", pair_b
                elif c_quote == b_quote:
                    swap_3_rate, direction_trade_3, contract_3 = b_bid, "quote_to_base", pair_b
                if contract_3:
                    acquired_coin_t3 = acquired_coin_t2 * swap_3_rate
                    calculated = True

        if not calculated:
            continue

        profit_loss = acquired_coin_t3 - starting_amount
        profit_loss_perc = (profit_loss / starting_amount) * 100 if starting_amount else 0

        if best_surface_perc is None or profit_loss_perc > best_surface_perc:
            best_surface_perc = profit_loss_perc

        if profit_loss_perc > min_profitable_rate:
            surface_dict = {
                "swap_1": swap_1, "swap_2": swap_2, "swap_3": swap_3,
                "contract_1": contract_1, "contract_2": contract_2, "contract_3": contract_3,
                "direction_trade_1": direction_trade_1,
                "direction_trade_2": direction_trade_2,
                "direction_trade_3": direction_trade_3,
                "starting_amount": starting_amount,
                "acquired_coin_t1": acquired_coin_t1,
                "acquired_coin_t2": acquired_coin_t2,
                "acquired_coin_t3": acquired_coin_t3,
                "swap_1_rate": swap_1_rate, "swap_2_rate": swap_2_rate, "swap_3_rate": swap_3_rate,
                "profit_loss": profit_loss, "profit_loss_perc": profit_loss_perc,
                "direction": direction,
            }
            min_profitable_rate = profit_loss_perc

    return surface_dict, best_surface_perc


def reformated_orderbook(prices, c_direction):
    if not prices:
        return []
    if "asks" in prices and isinstance(prices.get("asks"), list):
        asks, bids = prices.get("asks", []), prices.get("bids", [])
        if asks and isinstance(asks[0], str):
            asks_nested = [[asks[i], asks[i + 1]] for i in range(0, len(asks), 2)]
            bids_nested = [[bids[i], bids[i + 1]] for i in range(0, len(bids), 2)]
        else:
            asks_nested, bids_nested = asks, bids
        if c_direction == "base_to_quote":
            return [
                [1 / float(p[0]) if float(p[0]) else 0, float(p[1]) * float(p[0])]
                for p in asks_nested if len(p) >= 2
            ]
        if c_direction == "quote_to_base":
            return [[float(p[0]), float(p[1])] for p in bids_nested if len(p) >= 2]
    return []


def calculate_acquired_coin(amount_in, orderbook):
    trading_balance = amount_in
    acquired_coin = 0
    for idx, level in enumerate(orderbook):
        level_price, level_qty = level[0], level[1]
        if trading_balance <= level_qty:
            quantity_bought = trading_balance
            trading_balance = 0
        else:
            quantity_bought = level_qty
            trading_balance -= quantity_bought
        acquired_coin += quantity_bought * level_price
        if trading_balance == 0:
            return acquired_coin
        if idx == len(orderbook) - 1:
            return 0
    return acquired_coin


async def get_depth_from_orderbook_async(surface_arb):
    swap_1 = surface_arb["swap_1"]
    starting_amount = STARTING_AMOUNTS.get(swap_1, 100)
    contracts = surface_arb["contract_1"], surface_arb["contract_2"], surface_arb["contract_3"]
    directions = (
        surface_arb["direction_trade_1"],
        surface_arb["direction_trade_2"],
        surface_arb["direction_trade_3"],
    )
    urls = [POLONIEX_ORDERBOOK.format(symbol=c) for c in contracts]

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[get_coin_tickers_async(session, u) for u in urls])

    depths = [reformated_orderbook(r, d) for r, d in zip(results, directions)]
    acquired_coin_t1 = calculate_acquired_coin(starting_amount, depths[0])
    acquired_coin_t2 = calculate_acquired_coin(acquired_coin_t1, depths[1])
    acquired_coin_t3 = calculate_acquired_coin(acquired_coin_t2, depths[2])

    profit_loss = acquired_coin_t3 - starting_amount
    real_rate_perc = (profit_loss / starting_amount) * 100 if starting_amount else 0

    if real_rate_perc > -1:
        return {
            "profit_loss": profit_loss,
            "real_rate_perc": real_rate_perc,
            "contract_1": contracts[0],
            "contract_2": contracts[1],
            "contract_3": contracts[2],
            "contract_1_direction": directions[0],
            "contract_2_direction": directions[1],
            "contract_3_direction": directions[2],
        }
    return {}
