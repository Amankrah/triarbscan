from exchange_adapter import ExchangeAdapter
from typing import Dict, List, Tuple
import aiohttp


class PoloniexAdapter(ExchangeAdapter):

    def __init__(self):
        super().__init__(name="Poloniex", base_url="https://api.poloniex.com")
        self.taker_fee = 0.155   # verified fallback (Poloniex is disabled by default)

    def get_all_tickers(self) -> List[Dict]:
        data = self.fetch_with_retry(f"{self.base_url}/markets/ticker24h")
        if not data or not isinstance(data, list):
            return []
        return self._normalize_tickers(data)

    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        data = await self.fetch_with_retry_async(session, f"{self.base_url}/markets/ticker24h")
        if not data or not isinstance(data, list):
            return []
        return self._normalize_tickers(data)

    def _normalize_tickers(self, data: list) -> List[Dict]:
        normalized = []
        for ticker in data:
            try:
                normalized.append({
                    'symbol': ticker['symbol'],
                    'bid': float(ticker.get('bid', 0)),
                    'ask': float(ticker.get('ask', 0)),
                    'last': float(ticker.get('close', 0)),
                    'volume': float(ticker.get('quantity', 0))
                })
            except (KeyError, ValueError, TypeError):
                continue
        return normalized

    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        data = self.fetch_with_retry(
            f"{self.base_url}/markets/{symbol}/orderBook?limit={limit}"
        )
        return self._normalize_orderbook(data) if data else {'bids': [], 'asks': []}

    async def get_orderbook_async(self, session: aiohttp.ClientSession,
                                   symbol: str, limit: int = 20) -> Dict:
        data = await self.fetch_with_retry_async(
            session, f"{self.base_url}/markets/{symbol}/orderBook?limit={limit}"
        )
        return self._normalize_orderbook(data) if data else {'bids': [], 'asks': []}

    def _normalize_orderbook(self, data: Dict) -> Dict:
        asks = data.get('asks', [])
        bids = data.get('bids', [])
        if asks and isinstance(asks[0], str):
            asks_nested = [[asks[i], asks[i + 1]] for i in range(0, len(asks), 2)]
            bids_nested = [[bids[i], bids[i + 1]] for i in range(0, len(bids), 2)]
        else:
            asks_nested, bids_nested = asks, bids
        return {
            'bids': [[float(p), float(q)] for p, q in bids_nested if len([p, q]) == 2],
            'asks': [[float(p), float(q)] for p, q in asks_nested if len([p, q]) == 2]
        }

    def normalize_symbol(self, base: str, quote: str) -> str:
        return f"{base}_{quote}"

    def parse_symbol(self, symbol: str) -> Tuple[str, str]:
        parts = symbol.split('_')
        return (parts[0], parts[1]) if len(parts) == 2 else ('', '')
