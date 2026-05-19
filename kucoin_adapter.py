from exchange_adapter import ExchangeAdapter
from typing import Dict, List, Tuple
import aiohttp

# KuCoin Lv0 spot taker fee by feeCategory — Class A / B / C
# (verified May 2026; kucoin.com fee schedule).
KUCOIN_FEE_BY_CATEGORY = {1: 0.10, 2: 0.16, 3: 0.24}


class KuCoinAdapter(ExchangeAdapter):

    def __init__(self):
        super().__init__(name="KuCoin", base_url="https://api.kucoin.com/api/v1")
        self.taker_fee = 0.10   # Class A default; per-pair rates loaded live

    def _symbols_url(self) -> str:
        return self.base_url.replace('/api/v1', '/api/v2') + '/symbols'

    def get_all_tickers(self) -> List[Dict]:
        data = self.fetch_with_retry(f"{self.base_url}/market/allTickers")
        self._load_fee_map(self.fetch_with_retry(self._symbols_url()))
        return self._extract_tickers(data)

    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        data = await self.fetch_with_retry_async(session, f"{self.base_url}/market/allTickers")
        symbols = await self.fetch_with_retry_async(session, self._symbols_url())
        self._load_fee_map(symbols)
        return self._extract_tickers(data)

    def _load_fee_map(self, symbols_data: Dict) -> None:
        """Build per-pair taker fees from /api/v2/symbols. Each symbol carries
        a feeCategory (1/2/3 -> Class A/B/C) and a taker fee coefficient (a
        promotional multiplier, normally 1.0). On a failed fetch the previous
        map is kept rather than wiped."""
        if not symbols_data or symbols_data.get('code') != '200000':
            return
        fees = {}
        for s in symbols_data.get('data', []):
            base, quote = s.get('baseCurrency'), s.get('quoteCurrency')
            if not base or not quote:
                continue
            rate = KUCOIN_FEE_BY_CATEGORY.get(s.get('feeCategory'), self.taker_fee)
            try:
                coef = float(s.get('takerFeeCoefficient') or 1.0)
            except (ValueError, TypeError):
                coef = 1.0
            fees[f"{base}_{quote}"] = rate * coef
        if fees:
            self.taker_fee_by_symbol = fees

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
