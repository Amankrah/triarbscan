import asyncio
import aiohttp
from typing import List, Dict
from exchange_adapter import ExchangeAdapter
from poloniex_adapter import PoloniexAdapter
from binance_adapter import BinanceAdapter
from kraken_adapter import KrakenAdapter
from kucoin_adapter import KuCoinAdapter
import func_arbitrage

STARTING_AMOUNTS = {"USDT": 100, "USDC": 100, "BTC": 0.05, "ETH": 0.1}


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

    def get_tradeable_pairs(self, exchange_tickers: Dict[str, List[Dict]]) -> Dict[str, List[str]]:
        results = {}
        for exchange, tickers in exchange_tickers.items():
            adapter = self.adapters.get(exchange)
            if adapter and tickers:
                tradeable = adapter.get_tradeable_pairs(tickers)
                results[exchange] = tradeable
                print(f"  ✓ {exchange}: {len(tradeable)} tradeable pairs")
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
                                   min_real_rate: float = 0.1) -> Dict:
        """Scan one exchange; returns opportunities and per-scan diagnostics."""
        empty_stats = {
            'pairs_checked': 0,
            'missing_prices': 0,
            'paths_evaluated': 0,
            'no_path': 0,
            'positive_surface': 0,
            'depth_checked': 0,
            'errors': 0,
            'last_error': None,
            'best_surface_perc': None,
            'best_real_perc': None,
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
                        if real_rate_arb:
                            real_rate = real_rate_arb.get('real_rate_perc', 0)
                            if stats['best_real_perc'] is None or real_rate > stats['best_real_perc']:
                                stats['best_real_perc'] = real_rate
                            if real_rate >= min_real_rate:
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
            contract_1 = surface_arb["contract_1"]
            contract_2 = surface_arb["contract_2"]
            contract_3 = surface_arb["contract_3"]
            directions = (
                surface_arb["direction_trade_1"],
                surface_arb["direction_trade_2"],
                surface_arb["direction_trade_3"],
            )

            orderbooks = await asyncio.gather(
                adapter.get_orderbook_async(session, contract_1, 20),
                adapter.get_orderbook_async(session, contract_2, 20),
                adapter.get_orderbook_async(session, contract_3, 20),
            )

            depth_1 = func_arbitrage.reformated_orderbook(orderbooks[0], directions[0])
            depth_2 = func_arbitrage.reformated_orderbook(orderbooks[1], directions[1])
            depth_3 = func_arbitrage.reformated_orderbook(orderbooks[2], directions[2])

            starting_amount = STARTING_AMOUNTS.get(surface_arb["swap_1"], 100)
            acquired_coin_t1 = func_arbitrage.calculate_acquired_coin(starting_amount, depth_1)
            acquired_coin_t2 = func_arbitrage.calculate_acquired_coin(acquired_coin_t1, depth_2)
            acquired_coin_t3 = func_arbitrage.calculate_acquired_coin(acquired_coin_t2, depth_3)

            profit_loss = acquired_coin_t3 - starting_amount
            real_rate_perc = (profit_loss / starting_amount) * 100 if starting_amount else 0

            if real_rate_perc > -1:
                return {
                    "profit_loss": profit_loss,
                    "real_rate_perc": real_rate_perc,
                    "contract_1": contract_1,
                    "contract_2": contract_2,
                    "contract_3": contract_3,
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
