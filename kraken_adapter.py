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
        # Populated from AssetPairs during a ticker fetch. Kraken pair names
        # cannot be reliably reconstructed from a normalized BASE_QUOTE symbol
        # (legacy X/Z asset prefixes are ambiguous), so we keep the exact
        # names the API gave us rather than guessing them.
        self._pair_meta: Dict[str, Dict[str, str]] = {}   # kraken name -> {base, quote, altname}
        self._symbol_to_kraken: Dict[str, str] = {}        # BASE_QUOTE -> kraken depth name
        # Verified fallback; overwritten each ticker fetch with the live
        # entry-tier rate from AssetPairs (Kraken's schedule is account-wide).
        self.taker_fee = 0.40

    def get_all_tickers(self) -> List[Dict]:
        return self._fetch_all_tickers_sync()

    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        pairs_data = await self.fetch_with_retry_async(session, f"{self.base_url}/AssetPairs")
        if not pairs_data or 'result' not in pairs_data:
            return []
        pair_names = self._build_pair_meta(pairs_data['result'])
        return await self._fetch_tickers_batched(session, pair_names)

    def _fetch_all_tickers_sync(self) -> List[Dict]:
        pairs_data = self.fetch_with_retry(f"{self.base_url}/AssetPairs")
        if not pairs_data or 'result' not in pairs_data:
            return []
        pair_names = self._build_pair_meta(pairs_data['result'])
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

    def _build_pair_meta(self, pairs_result: Dict) -> List[str]:
        """Index online pairs from AssetPairs, recording clean base/quote and
        the exact Kraken name needed for the Depth endpoint. Returns the list
        of online pair names to query for tickers."""
        self._pair_meta = {}
        self._symbol_to_kraken = {}
        self.price_increment = {}
        names = []
        live_taker_fee = None
        for kraken_name, info in pairs_result.items():
            if kraken_name.endswith('.d'):          # dark-pool variant
                continue
            if info.get('status') != 'online':
                continue
            base, quote = self._clean_pair(info)
            if not base or not quote:
                continue
            if live_taker_fee is None:
                live_taker_fee = self._entry_tier_fee(info.get('fees'))
            tick = self._tick_size(info)
            if tick:
                self.price_increment[f"{base}_{quote}"] = tick
            self._pair_meta[kraken_name] = {
                'base': base,
                'quote': quote,
                'altname': info.get('altname') or kraken_name,
            }
            names.append(kraken_name)
        if live_taker_fee is not None:
            self.taker_fee = live_taker_fee
        return names

    @staticmethod
    def _entry_tier_fee(fee_tiers):
        """Entry-tier (0-volume) taker fee from a Kraken `fees` array
        [[volume, pct], ...]. The schedule is account-wide, so any online
        pair's array carries the same tiers."""
        try:
            return float(fee_tiers[0][1])
        except (TypeError, IndexError, ValueError):
            return None

    @staticmethod
    def _tick_size(info):
        """Price tick from an AssetPairs entry — the explicit `tick_size`
        field, falling back to 10**-pair_decimals."""
        ts = info.get('tick_size')
        if ts is not None:
            try:
                return float(ts)
            except (TypeError, ValueError):
                pass
        pd = info.get('pair_decimals')
        if pd is not None:
            try:
                return 10.0 ** -int(pd)
            except (TypeError, ValueError):
                pass
        return None

    def _clean_pair(self, info: Dict) -> Tuple[str, str]:
        """Derive human-readable (base, quote) from an AssetPairs entry,
        preferring `wsname` ('XBT/USD') over the raw asset codes."""
        wsname = info.get('wsname')
        if wsname and '/' in wsname:
            base, quote = wsname.split('/', 1)
        else:
            base = self._clean_asset(info.get('base', ''))
            quote = self._clean_asset(info.get('quote', ''))
        return self._canonical(base), self._canonical(quote)

    @staticmethod
    def _clean_asset(code: str) -> str:
        """Strip Kraken's legacy 4-char X/Z asset prefix (XXBT -> XBT,
        ZUSD -> USD); leave modern 3-char-or-shorter codes alone."""
        code = code.upper()
        if len(code) == 4 and code[0] in ('X', 'Z'):
            return code[1:]
        return code

    @staticmethod
    def _canonical(code: str) -> str:
        """Map Kraken-specific tickers onto the cross-exchange convention."""
        code = code.upper()
        return 'BTC' if code in ('XBT', 'XXBT') else code

    def _normalize_tickers(self, result: Dict) -> List[Dict]:
        normalized = []
        for pair_name, ticker in result.items():
            meta = self._pair_meta.get(pair_name)
            if not meta:
                continue
            try:
                ask = float(ticker['a'][0]) if ticker.get('a') else 0
                bid = float(ticker['b'][0]) if ticker.get('b') else 0
                last = float(ticker['c'][0]) if ticker.get('c') else 0
                volume = float(ticker['v'][1]) if ticker.get('v') and len(ticker['v']) > 1 else 0
            except (KeyError, ValueError, TypeError, IndexError):
                continue
            symbol = f"{meta['base']}_{meta['quote']}"
            self._symbol_to_kraken[symbol] = meta['altname']
            normalized.append({
                'symbol': symbol,
                'bid': bid,
                'ask': ask,
                'last': last,
                'volume': volume,
            })
        return normalized

    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        data = self.fetch_with_retry(
            f"{self.base_url}/Depth?pair={self._depth_pair(symbol)}&count={limit}"
        )
        return self._orderbook_from_response(data)

    async def get_orderbook_async(self, session: aiohttp.ClientSession,
                                   symbol: str, limit: int = 20) -> Dict:
        data = await self.fetch_with_retry_async(
            session,
            f"{self.base_url}/Depth?pair={self._depth_pair(symbol)}&count={limit}"
        )
        return self._orderbook_from_response(data)

    def _depth_pair(self, symbol: str) -> str:
        """Exact Kraken pair name for a normalized BASE_QUOTE symbol. Uses the
        AssetPairs map; falls back to best-effort reconstruction if unknown."""
        return self._symbol_to_kraken.get(symbol) or self._to_kraken_symbol(symbol)

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
        """Best-effort fallback used only when a symbol is not in the
        AssetPairs map (it normally is)."""
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
