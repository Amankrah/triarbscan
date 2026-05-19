from exchange_adapter import ExchangeAdapter
from typing import Dict, List, Tuple
import aiohttp


class KuCoinAdapter(ExchangeAdapter):

    def __init__(self):
        super().__init__(name="KuCoin", base_url="https://api.kucoin.com/api/v1")

    def get_all_tickers(self) -> List[Dict]:
        data = self.fetch_with_retry(f"{self.base_url}/market/allTickers")
        return self._extract_tickers(data)

    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        data = await self.fetch_with_retry_async(session, f"{self.base_url}/market/allTickers")
        return self._extract_tickers(data)

    def _extract_tickers(self, data: Dict) -> List[Dict]:
        if not data or data.get('code') != '200000':
            return []
        ticker_list = data.get('data', {}).get('ticker', [])
        normalized = []
        for ticker in ticker_list:
            try:
                base, quote = self.parse_symbol(ticker['symbol'])
                if not base or not quote:
                    continue
                normalized.append({
                    'symbol': f"{base}_{quote}",
                    'bid': float(ticker.get('buy', 0)),
                    'ask': float(ticker.get('sell', 0)),
                    'last': float(ticker.get('last', 0)),
                    'volume': float(ticker.get('vol', 0))
                })
            except (KeyError, ValueError, TypeError):
                continue
        return normalized

    @staticmethod
    def _depth_endpoint(limit: int) -> str:
        """KuCoin only exposes fixed-depth public books: level2_20 and
        level2_100 (deeper books need an authenticated request). Pick the
        100-level endpoint whenever more than 20 levels are asked for."""
        return 'level2_100' if limit > 20 else 'level2_20'

    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        kucoin_symbol = symbol.replace('_', '-')
        endpoint = self._depth_endpoint(limit)
        data = self.fetch_with_retry(
            f"{self.base_url}/market/orderbook/{endpoint}?symbol={kucoin_symbol}"
        )
        if not data or data.get('code') != '200000':
            return {'bids': [], 'asks': []}
        return self._normalize_orderbook(data.get('data', {}))

    async def get_orderbook_async(self, session: aiohttp.ClientSession,
                                   symbol: str, limit: int = 20) -> Dict:
        kucoin_symbol = symbol.replace('_', '-')
        endpoint = self._depth_endpoint(limit)
        data = await self.fetch_with_retry_async(
            session, f"{self.base_url}/market/orderbook/{endpoint}?symbol={kucoin_symbol}"
        )
        if not data or data.get('code') != '200000':
            return {'bids': [], 'asks': []}
        return self._normalize_orderbook(data.get('data', {}))

    def _normalize_orderbook(self, data: Dict) -> Dict:
        try:
            bids = [[float(p), float(q)] for p, q in data.get('bids', [])]
            asks = [[float(p), float(q)] for p, q in data.get('asks', [])]
            return {'bids': bids, 'asks': asks}
        except (ValueError, TypeError):
            return {'bids': [], 'asks': []}

    def normalize_symbol(self, base: str, quote: str) -> str:
        return f"{base}-{quote}"

    def parse_symbol(self, symbol: str) -> Tuple[str, str]:
        parts = symbol.split('-')
        return (parts[0], parts[1]) if len(parts) == 2 else ('', '')
