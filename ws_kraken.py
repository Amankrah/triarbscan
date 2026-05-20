"""Live Kraken top-of-book feed (WebSocket v2, ticker channel).

Kraken's v2 ticker channel pushes top-of-book bid/ask/qty per pair on every
inside-quote change. A single connection handles all subscribed pairs (no
per-symbol fan-out required, unlike Binance).

The feed exposes the same interface as `BinanceBookFeed` — `books`, dirty-set
+ `update_event`, `quote_age`, `connected`, `coverage` — so the multi-venue
scanner can drive it identically.
"""
import asyncio
import json
import time
import aiohttp

KRAKEN_WS_V2 = "wss://ws.kraken.com/v2"


class KrakenBookFeed:
    """Maintains `books[BASE_QUOTE] = {bid, ask, bid_qty, ask_qty}` from the
    Kraken v2 ticker stream.

    `symbol_to_wsname` translates normalized BASE_QUOTE symbols (the scanner's
    universal form) into Kraken's WS subscription identifier (e.g. "XBT/USD").
    Pass it from `KrakenAdapter._symbol_to_wsname` after a ticker fetch.
    """

    def __init__(self, symbols, symbol_to_wsname, initial_books=None):
        self._symbols = sorted(set(symbols))
        # Filter to only those we can actually subscribe to, then translate
        # the legacy `XBT` token to `BTC` — Kraken's v2 WS rejects `XBT/USD`
        # ("Currency pair not supported") even though AssetPairs still
        # returns `XBT/...` as wsname. Responses also come back in the
        # translated form, so the reverse map keys on the same string.
        self._sym_to_ws = {
            s: self._translate(symbol_to_wsname[s])
            for s in self._symbols if s in symbol_to_wsname
        }
        self._ws_to_sym = {v: k for k, v in self._sym_to_ws.items()}
        self.books = dict(initial_books or {})
        self.messages = 0
        self._connected = False
        self._task = None
        self._session = None
        self._ws = None
        self._req_id = 0
        # Dirty-set + event: same protocol as BinanceBookFeed.
        self._dirty: set = set()
        self.update_event = asyncio.Event()
        # Quote-age tracking, seeded from any bootstrap snapshot.
        _now = time.perf_counter()
        self.last_price_change_t = {sym: _now for sym in self.books}

    @staticmethod
    def _translate(wsname: str) -> str:
        """Map AssetPairs `wsname` to the v2 WS-accepted form. Currently only
        XBT (legacy code for Bitcoin) needs to become BTC; other Kraken legacy
        codes appearing in wsname are already modernised by AssetPairs."""
        return wsname.replace('XBT', 'BTC')

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None and not self._ws.closed

    @property
    def has_pending(self) -> bool:
        return bool(self._dirty)

    def coverage(self, symbols) -> int:
        return sum(1 for s in symbols if s in self.books)

    def consume_dirty(self) -> set:
        dirty = self._dirty
        self._dirty = set()
        self.update_event.clear()
        return dirty

    def quote_age(self, symbol):
        ts = self.last_price_change_t.get(symbol)
        if ts is None:
            return None
        return time.perf_counter() - ts

    async def start(self):
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with self._session.ws_connect(KRAKEN_WS_V2, heartbeat=30) as ws:
                    self._ws = ws
                    self._req_id += 1
                    wsnames = list(self._ws_to_sym.keys())
                    # Kraken v2 accepts the full array in one subscribe; for
                    # very large universes we'd batch, but the filtered Kraken
                    # universe is hundreds of pairs, fine for one message.
                    await ws.send_json({
                        "method": "subscribe",
                        "params": {
                            "channel": "ticker",
                            "symbol": wsnames,
                            "snapshot": True,   # push initial book on subscribe
                        },
                        "req_id": self._req_id,
                    })
                    self._connected = True
                    backoff = 1
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        self._connected = False
                        self._ws = None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠ Kraken WS error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        if msg.get('channel') != 'ticker':
            return                         # ignore heartbeat, status, ack, etc.
        data = msg.get('data') or []
        if not isinstance(data, list):
            return
        for entry in data:
            wsname = entry.get('symbol')
            norm = self._ws_to_sym.get(wsname)
            if not norm:
                continue
            try:
                new_bid = float(entry['bid'])
                new_ask = float(entry['ask'])
                new_bid_qty = float(entry.get('bid_qty', 0))
                new_ask_qty = float(entry.get('ask_qty', 0))
            except (KeyError, ValueError, TypeError):
                continue
            prev = self.books.get(norm)
            prices_changed = (prev is None
                              or prev['bid'] != new_bid
                              or prev['ask'] != new_ask)
            if norm not in self.last_price_change_t or prices_changed:
                self.last_price_change_t[norm] = time.perf_counter()
            self.books[norm] = {
                'bid': new_bid, 'ask': new_ask,
                'bid_qty': new_bid_qty, 'ask_qty': new_ask_qty,
            }
            self.messages += 1
            self._dirty.add(norm)
            self.update_event.set()
