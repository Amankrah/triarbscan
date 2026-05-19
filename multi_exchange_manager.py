import asyncio
import aiohttp
from typing import List, Dict
from exchange_adapter import ExchangeAdapter
from poloniex_adapter import PoloniexAdapter
from binance_adapter import BinanceAdapter
from kraken_adapter import KrakenAdapter
from kucoin_adapter import KuCoinAdapter
from fee_calculator import calculate_fee_impact
import func_arbitrage

STARTING_AMOUNTS = {"USDT": 100, "USDC": 100, "BTC": 0.05, "ETH": 0.1}

# Orderbook levels to fetch per leg. 20 is too thin for BTC-denominated
# notional on altcoin books — the walk runs off the end and the leg gets
# silently scored as catastrophic. All four exchanges accept 100.
ORDERBOOK_DEPTH = 100

# Currencies treated as 1:1 with USD when converting 24h volume.
STABLE_USD = {
    'USD', 'USDT', 'USDC', 'USDP', 'TUSD', 'BUSD', 'DAI', 'PAX', 'USDS', 'USD1',
}


def _build_usd_rates(tickers: List[Dict]) -> Dict[str, float]:
    """USD value of one unit of each currency, read off <currency>_<stable>
    tickers. Stablecoins map to 1.0; anything without a stable-quoted ticker
    is simply absent (callers leave such pairs unfiltered)."""
    rates: Dict[str, float] = {s: 1.0 for s in STABLE_USD}
    for t in tickers:
        symbol = t.get('symbol', '')
        if '_' not in symbol:
            continue
        base, quote = symbol.split('_', 1)
        try:
            last = float(t.get('last') or 0)
        except (ValueError, TypeError):
            continue
        if quote in STABLE_USD and base not in rates and last > 0:
            rates[base] = last
    return rates


def filter_by_usd_volume(tickers: List[Dict], tradeable: List[str],
                         min_usd_volume: float):
    """Drop symbols whose 24h volume, converted to USD, is below the threshold.

    `volume` is base-asset volume on all four adapters, so quote volume is
    volume * last, then * the quote currency's USD rate. Pairs whose quote
    cannot be priced in USD are kept (can't assess -> don't drop).

    Returns (kept_symbols, dropped_count).
    """
    if min_usd_volume <= 0:
        return tradeable, 0
    rates = _build_usd_rates(tickers)
    by_symbol = {t.get('symbol'): t for t in tickers}
    kept, dropped = [], 0
    for symbol in tradeable:
        ticker = by_symbol.get(symbol)
        if not ticker or '_' not in symbol:
            kept.append(symbol)
            continue
        rate = rates.get(symbol.split('_', 1)[1])
        if rate is None:
            kept.append(symbol)               # quote not priceable in USD
            continue
        try:
            usd_vol = float(ticker.get('volume') or 0) * float(ticker.get('last') or 0) * rate
        except (ValueError, TypeError):
            usd_vol = 0.0
        if usd_vol >= min_usd_volume:
            kept.append(symbol)
        else:
            dropped += 1
    return kept, dropped


class MultiExchangeManager:

    def __init__(self, exchanges: List[str] = None):
        self.adapters: Dict[str, ExchangeAdapter] = {}
        if exchanges is None:
            exchanges = ['poloniex', 'binance', 'kraken', 'kucoin']

        for exchange in exchanges:
            key = exchange.lower()
            if key == 'poloniex':
                self.adapters['poloniex'] = PoloniexAdapter()
            elif key == 'binance':
                self.adapters['binance'] = BinanceAdapter()
            elif key == 'kraken':
                self.adapters['kraken'] = KrakenAdapter()
            elif key == 'kucoin':
                self.adapters['kucoin'] = KuCoinAdapter()

        print(f"📊 Initialized {len(self.adapters)} exchange(s): {', '.join(self.adapters.keys())}")

    def get_all_tickers_sync(self, exchange: str = None) -> Dict[str, List[Dict]]:
        results = {}
        if exchange:
            adapter = self.adapters.get(exchange.lower())
            if adapter:
                print(f"📥 Fetching tickers from {exchange}...")
                results[exchange] = adapter.get_all_tickers()
        else:
            for name, adapter in self.adapters.items():
                print(f"📥 Fetching tickers from {name}...")
                results[name] = adapter.get_all_tickers()
        return results

    async def get_all_tickers_async(self, exchange: str = None) -> Dict[str, List[Dict]]:
        async with aiohttp.ClientSession() as session:
            if exchange:
                adapter = self.adapters.get(exchange.lower())
                if adapter:
                    tickers = await adapter.get_all_tickers_async(session)
                    return {exchange: tickers}
                return {}
            tasks = [a.get_all_tickers_async(session) for a in self.adapters.values()]
            names = list(self.adapters.keys())
            results_list = await asyncio.gather(*tasks)
            return dict(zip(names, results_list))

    def get_tradeable_pairs(self, exchange_tickers: Dict[str, List[Dict]],
                            min_usd_volume: float = 0.0) -> Dict[str, List[str]]:
        results = {}
        for exchange, tickers in exchange_tickers.items():
            adapter = self.adapters.get(exchange)
            if adapter and tickers:
                tradeable = adapter.get_tradeable_pairs(tickers)
                kept, dropped = filter_by_usd_volume(tickers, tradeable, min_usd_volume)
                results[exchange] = kept
                if dropped:
                    print(f"  ✓ {exchange}: {len(kept)} tradeable pairs "
                          f"({dropped} dropped: 24h volume < ${min_usd_volume:,.0f})")
                else:
                    print(f"  ✓ {exchange}: {len(kept)} tradeable pairs")
        return results

    def structure_triangular_pairs(self, tradeable_pairs: Dict[str, List[str]]) -> Dict[str, List[Dict]]:
        results = {}
        for exchange, pairs in tradeable_pairs.items():
            print(f"🔺 Finding triangular pairs for {exchange} ({len(pairs)} pairs)...")
            triangular_pairs = func_arbitrage.structure_triangular_pairs(pairs)
            results[exchange] = triangular_pairs
            print(f"  ✓ {exchange}: {len(triangular_pairs)} triangular pairs found")
        return results

    async def scan_exchange_async(self, exchange: str, triangular_pairs: List[Dict],
                                   min_surface_rate: float = 0.0,
                                   min_net_rate: float = 0.0,
                                   slippage_buffer: float = 0.0,
                                   fee_type: str = 'taker') -> Dict:
        """Scan one exchange; returns opportunities and per-scan diagnostics.

        An opportunity must clear exchange fees (3 taker legs) and a slippage
        buffer by at least `min_net_rate` percent — raw `real_rate` is not enough.
        """
        empty_stats = {
            'pairs_checked': 0,
            'missing_prices': 0,
            'paths_evaluated': 0,
            'no_path': 0,
            'positive_surface': 0,
            'depth_checked': 0,
            'book_thin': 0,
            'book_thin_fill_sum': 0.0,
            'book_thin_worst_fill': None,
            'book_empty': 0,
            'errors': 0,
            'last_error': None,
            'best_surface_perc': None,
            'best_surface_route': None,
            'best_real_perc': None,
            'best_net_perc': None,
        }
        adapter = self.adapters.get(exchange.lower())
        if not adapter:
            return {'opportunities': [], 'stats': empty_stats}

        opportunities = []
        stats = dict(empty_stats)
        stats['pairs_checked'] = len(triangular_pairs)

        async with aiohttp.ClientSession() as session:
            tickers = await adapter.get_all_tickers_async(session)
            if not tickers:
                return {'opportunities': [], 'stats': stats}

            ticker_dict = {t['symbol']: t for t in tickers}

            for t_pair in triangular_pairs:
                try:
                    prices_dict = self._get_prices_for_pair(t_pair, ticker_dict)
                    if not all(prices_dict.values()):
                        stats['missing_prices'] += 1
                        continue

                    surface_arb, best_rate = func_arbitrage.calc_triangular_arb_surface_rate(
                        t_pair, prices_dict
                    )
                    if best_rate is not None:
                        stats['paths_evaluated'] += 1
                        if (stats['best_surface_perc'] is None
                                or best_rate > stats['best_surface_perc']):
                            stats['best_surface_perc'] = best_rate
                            stats['best_surface_route'] = t_pair.get('combined')
                    else:
                        stats['no_path'] += 1

                    if not surface_arb:
                        continue

                    surface_rate = surface_arb.get('profit_loss_perc', 0)
                    if surface_rate > 0:
                        stats['positive_surface'] += 1

                    if surface_rate >= min_surface_rate:
                        stats['depth_checked'] += 1
                        real_rate_arb = await self._get_depth_async(session, adapter, surface_arb)
                        if real_rate_arb.get('book_exhausted'):
                            frac = real_rate_arb.get('filled_fraction', 0.0)
                            if frac <= 0.0:
                                # No level consumed at all -> empty book,
                                # i.e. a failed / unrecognised orderbook fetch.
                                stats['book_empty'] += 1
                            else:
                                stats['book_thin'] += 1
                                stats['book_thin_fill_sum'] += frac
                                if (stats['book_thin_worst_fill'] is None
                                        or frac < stats['book_thin_worst_fill']):
                                    stats['book_thin_worst_fill'] = frac
                            continue
                        if real_rate_arb:
                            real_rate = real_rate_arb.get('real_rate_perc', 0)
                            if stats['best_real_perc'] is None or real_rate > stats['best_real_perc']:
                                stats['best_real_perc'] = real_rate

                            fee = calculate_fee_impact(exchange, real_rate, fee_type)
                            net_rate = (fee['net_profit_perc'] or 0) - slippage_buffer
                            real_rate_arb['fee_perc'] = fee['total_fee_3_trades']
                            real_rate_arb['slippage_buffer_perc'] = slippage_buffer
                            real_rate_arb['net_rate_perc'] = net_rate
                            if stats['best_net_perc'] is None or net_rate > stats['best_net_perc']:
                                stats['best_net_perc'] = net_rate

                            if net_rate >= min_net_rate:
                                real_rate_arb['exchange'] = exchange
                                real_rate_arb['surface_arb'] = surface_arb
                                opportunities.append(real_rate_arb)
                except Exception as e:
                    stats['errors'] += 1
                    stats['last_error'] = f"{type(e).__name__}: {e}"
                    continue

        return {'opportunities': opportunities, 'stats': stats}

    def _get_prices_for_pair(self, t_pair: Dict, ticker_dict: Dict) -> Dict:
        pair_a_data = ticker_dict.get(t_pair["pair_a"], {})
        pair_b_data = ticker_dict.get(t_pair["pair_b"], {})
        pair_c_data = ticker_dict.get(t_pair["pair_c"], {})
        return {
            "pair_a_ask": float(pair_a_data.get("ask", 0)),
            "pair_a_bid": float(pair_a_data.get("bid", 0)),
            "pair_b_ask": float(pair_b_data.get("ask", 0)),
            "pair_b_bid": float(pair_b_data.get("bid", 0)),
            "pair_c_ask": float(pair_c_data.get("ask", 0)),
            "pair_c_bid": float(pair_c_data.get("bid", 0))
        }

    async def _get_depth_async(self, session: aiohttp.ClientSession,
                               adapter: ExchangeAdapter,
                               surface_arb: Dict) -> Dict:
        try:
            raw_legs = [
                (surface_arb["contract_1"], surface_arb["direction_trade_1"]),
                (surface_arb["contract_2"], surface_arb["direction_trade_2"]),
                (surface_arb["contract_3"], surface_arb["direction_trade_3"]),
            ]
            # The surface calc records correct legs but not in execution order
            # (and a mislabeled swap_1). Reorder into a runnable chain and
            # recover the true start currency before walking depth.
            legs, swap_1 = func_arbitrage.order_legs_for_execution(raw_legs)
            if legs is None:
                return {}
            contracts = [c for c, _ in legs]
            directions = [d for _, d in legs]

            orderbooks = await asyncio.gather(
                adapter.get_orderbook_async(session, contracts[0], ORDERBOOK_DEPTH),
                adapter.get_orderbook_async(session, contracts[1], ORDERBOOK_DEPTH),
                adapter.get_orderbook_async(session, contracts[2], ORDERBOOK_DEPTH),
            )

            depths = (
                func_arbitrage.reformated_orderbook(orderbooks[0], directions[0]),
                func_arbitrage.reformated_orderbook(orderbooks[1], directions[1]),
                func_arbitrage.reformated_orderbook(orderbooks[2], directions[2]),
            )

            starting_amount = STARTING_AMOUNTS.get(swap_1, 100)
            amount = starting_amount
            for leg, depth in enumerate(depths, start=1):
                amount, filled_fraction = func_arbitrage.calculate_acquired_coin(amount, depth)
                # filled_fraction < 1.0 means the book was too thin to fill the
                # size. Report it distinctly — with how far short the walk fell —
                # so it isn't lumped in with genuine "no opportunity" results.
                if filled_fraction < 1.0:
                    return {
                        'book_exhausted': True,
                        'filled_fraction': filled_fraction,
                        'exhausted_leg': leg,
                    }
            acquired_coin_t3 = amount

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
        except Exception:
            return {}

    def get_summary(self) -> str:
        lines = ["Multi-Exchange Configuration:"]
        for name, adapter in self.adapters.items():
            lines.append(f"  • {name.capitalize()}: {adapter.base_url}")
        return "\n".join(lines)
