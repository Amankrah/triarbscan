from exchange_adapter import ExchangeAdapter
from typing import Dict, List, Tuple
import aiohttp

QUOTE_CURRENCIES = [
    'USDT', 'USDC', 'BUSD', 'TUSD', 'USDP',
    'BTC', 'ETH', 'BNB', 'XRP', 'DOGE',
    'EUR', 'GBP', 'AUD', 'TRY', 'BRL',
    'DAI', 'PAX', 'USDS',
]


class BinanceAdapter(ExchangeAdapter):

    def __init__(self):
        super().__init__(name="Binance", base_url="https://api.binance.com/api/v3")
        # Binance exposes no public no-auth spot-fee endpoint; 0.10% is the
        # standard regular-user rate (verified May 2026, binance.com/en/fee).
        self.taker_fee = 0.10

    def get_all_tickers(self) -> List[Dict]:
        data = self.fetch_with_retry(f"{self.base_url}/ticker/24hr")
        if not data or not isinstance(data, list):
            return []
        return self._normalize_tickers(data)

    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        data = await self.fetch_with_retry_async(session, f"{self.base_url}/ticker/24hr")
        if not data or not isinstance(data, list):
            return []
        return self._normalize_tickers(data)

    def _normalize_tickers(self, data: list) -> List[Dict]:
        normalized = []
        for ticker in data:
            try:
                base, quote = self.parse_symbol(ticker['symbol'])
                if not base or not quote:
                    continue
                normalized.append({
                    'symbol': f"{base}_{quote}",
                    'bid': float(ticker.get('bidPrice', 0)),
                    'ask': float(ticker.get('askPrice', 0)),
                    'last': float(ticker.get('lastPrice', 0)),
                    'volume': float(ticker.get('volume', 0))
                })
            except (KeyError, ValueError, TypeError):
                continue
        return normalized

    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        binance_symbol = symbol.replace('_', '')
        data = self.fetch_with_retry(
            f"{self.base_url}/depth?symbol={binance_symbol}&limit={limit}"
        )
        return self._normalize_orderbook(data) if data else {'bids': [], 'asks': []}

    async def get_orderbook_async(self, session: aiohttp.ClientSession,
                                   symbol: str, limit: int = 20) -> Dict:
        binance_symbol = symbol.replace('_', '')
        data = await self.fetch_with_retry_async(
            session, f"{self.base_url}/depth?symbol={binance_symbol}&limit={limit}"
        )
        return self._normalize_orderbook(data) if data else {'bids': [], 'asks': []}

    def _normalize_orderbook(self, data: Dict) -> Dict:
        try:
            bids = [[float(p), float(q)] for p, q in data.get('bids', [])]
            asks = [[float(p), float(q)] for p, q in data.get('asks', [])]
            return {'bids': bids, 'asks': asks}
        except (ValueError, TypeError):
            return {'bids': [], 'asks': []}

    def normalize_symbol(self, base: str, quote: str) -> str:
        return f"{base}{quote}"

    def parse_symbol(self, symbol: str) -> Tuple[str, str]:
        symbol = symbol.upper()
        for quote in QUOTE_CURRENCIES:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                if base:
                    return base, quote
        return '', ''
