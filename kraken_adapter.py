from exchange_adapter import ExchangeAdapter
from typing import Dict, List, Optional, Tuple
import aiohttp

QUOTE_CURRENCIES = [
    'USDT', 'USDC', 'USD', 'EUR', 'GBP',
    'BTC', 'ETH', 'XBT',
    'DAI', 'ZUSD', 'ZEUR',
]


class KrakenAdapter(ExchangeAdapter):

    def __init__(self):
        super().__init__(name="Kraken", base_url="https://api.kraken.com/0/public")

    def get_all_tickers(self) -> List[Dict]:
        return self._fetch_all_tickers_sync()

    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        pairs_data = await self.fetch_with_retry_async(session, f"{self.base_url}/AssetPairs")
        if not pairs_data or 'result' not in pairs_data:
            return []
        pair_names = self._online_pairs(pairs_data['result'])
        return await self._fetch_tickers_batched(session, pair_names)

    def _fetch_all_tickers_sync(self) -> List[Dict]:
        pairs_data = self.fetch_with_retry(f"{self.base_url}/AssetPairs")
        if not pairs_data or 'result' not in pairs_data:
            return []
        pair_names = self._online_pairs(pairs_data['result'])
        all_tickers = []
        batch_size = 500
        for i in range(0, len(pair_names), batch_size):
            batch = ','.join(pair_names[i:i + batch_size])
            ticker_data = self.fetch_with_retry(f"{self.base_url}/Ticker?pair={batch}")
            if ticker_data and 'result' in ticker_data:
                all_tickers.extend(self._normalize_tickers(ticker_data['result']))
        return all_tickers

    async def _fetch_tickers_batched(self, session, pair_names: List[str]) -> List[Dict]:
        all_tickers = []
        batch_size = 500
        for i in range(0, len(pair_names), batch_size):
            batch = ','.join(pair_names[i:i + batch_size])
            ticker_data = await self.fetch_with_retry_async(
                session, f"{self.base_url}/Ticker?pair={batch}"
            )
            if ticker_data and 'result' in ticker_data:
                all_tickers.extend(self._normalize_tickers(ticker_data['result']))
        return all_tickers

    def _online_pairs(self, pairs_result: Dict) -> List[str]:
        names = []
        for pair_name, pair_info in pairs_result.items():
            if pair_name.endswith('.d'):
                continue
            if pair_info.get('status') == 'online':
                names.append(pair_name)
        return names

    def _normalize_tickers(self, result: Dict) -> List[Dict]:
        normalized = []
        for pair_name, ticker in result.items():
            try:
                base, quote = self.parse_symbol(pair_name)
                if not base or not quote:
                    continue
                ask = float(ticker['a'][0]) if ticker.get('a') else 0
                bid = float(ticker['b'][0]) if ticker.get('b') else 0
                last = float(ticker['c'][0]) if ticker.get('c') else 0
                volume = float(ticker['v'][1]) if ticker.get('v') and len(ticker['v']) > 1 else 0
                normalized.append({
                    'symbol': f"{base}_{quote}",
                    'bid': bid,
                    'ask': ask,
                    'last': last,
                    'volume': volume
                })
            except (KeyError, ValueError, TypeError, IndexError):
                continue
        return normalized

    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        data = self.fetch_with_retry(
            f"{self.base_url}/Depth?pair={self._to_kraken_symbol(symbol)}&count={limit}"
        )
        return self._orderbook_from_response(data)

    async def get_orderbook_async(self, session: aiohttp.ClientSession,
                                   symbol: str, limit: int = 20) -> Dict:
        data = await self.fetch_with_retry_async(
            session,
            f"{self.base_url}/Depth?pair={self._to_kraken_symbol(symbol)}&count={limit}"
        )
        return self._orderbook_from_response(data)

    def _orderbook_from_response(self, data: Optional[Dict]) -> Dict:
        if not data or 'result' not in data or not data['result']:
            return {'bids': [], 'asks': []}
        pair_data = list(data['result'].values())[0]
        return self._normalize_orderbook(pair_data)

    def _normalize_orderbook(self, data: Dict) -> Dict:
        try:
            bids = [[float(p), float(q)] for p, q, *_ in data.get('bids', [])]
            asks = [[float(p), float(q)] for p, q, *_ in data.get('asks', [])]
            return {'bids': bids, 'asks': asks}
        except (ValueError, TypeError):
            return {'bids': [], 'asks': []}

    def _to_kraken_symbol(self, symbol: str) -> str:
        base, quote = symbol.split('_')
        if base == 'BTC':
            base = 'XBT'
        return f"{base}{quote}"

    def normalize_symbol(self, base: str, quote: str) -> str:
        if base == 'XBT':
            base = 'BTC'
        return f"{base}{quote}"

    def parse_symbol(self, symbol: str) -> Tuple[str, str]:
        symbol = symbol.upper()
        if symbol.startswith('X') and len(symbol) > 6:
            symbol = symbol[1:]
        for quote in QUOTE_CURRENCIES:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                if base:
                    if base == 'XBT':
                        base = 'BTC'
                    if quote == 'XBT':
                        quote = 'BTC'
                    if quote.startswith('Z') and len(quote) == 4:
                        quote = quote[1:]
                    return base, quote
        return '', ''
