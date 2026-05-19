"""Live Binance top-of-book feed.

Binance deprecated the all-market `!bookTicker` / `!ticker@arr` streams — they
accept the connection but deliver no frames. Per-symbol streams still work, so
this feed subscribes to one `<symbol>@bookTicker` stream per symbol, fanned
across N connections (Binance caps a connection at 1024 streams).

It keeps `books[BASE_QUOTE] = {bid, ask, bid_qty, ask_qty}` continuously
updated; the scan loop reads that dict directly with no network I/O.
"""
import asyncio
import json
import time
import aiohttp

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"
BINANCE_WS_COMBINED = "wss://stream.binance.com:9443/stream"
MAX_STREAMS_PER_CONN = 900   # Binance hard limit is 1024 — leave margin
SUBSCRIBE_BATCH = 100        # stream names per SUBSCRIBE message
SUBSCRIBE_PACING = 0.3       # seconds between SUBSCRIBE messages (limit: 5/s)
DEPTH_LEVELS = 20            # Binance partial-book depth (5, 10, 20)
DEPTH_SPEED = "100ms"        # partial-book update cadence


class BinanceBookFeed:
    """Maintains `books[normalized_symbol] = {bid, ask, bid_qty, ask_qty}`.

    `normalized_symbol` is BASE_QUOTE (e.g. 'BTC_USDT'), matching the triangle
    data. Connections reconnect automatically with exponential backoff.
    """

    def __init__(self, symbols, initial_books=None):
        # symbols: iterable of normalized 'BASE_QUOTE'
        self._symbols = sorted(set(symbols))
        self._rev = {s.replace('_', ''): s for s in self._symbols}  # 'BTCUSDT'->'BTC_USDT'
        self.books = dict(initial_books or {})
        self.messages = 0
        self._conns = 0
        self._conns_up = 0
        self._tasks = []
        self._session = None
        # Symbols whose book changed since the last consume_dirty() call.
        # Single-threaded asyncio means no lock is needed — `_handle` and the
        # scan loop never run concurrently. The event signals "at least one
        # symbol moved" so the scanner can block instead of polling.
        self._dirty: set = set()
        self.update_event = asyncio.Event()
        # Wall-clock (perf_counter) when each symbol's inside PRICE last
        # changed. Quantity-only refreshes do not bump this — the surface
        # rate only sees price moves. Exposes per-leg quote-refresh
        # sparsity for diagnosing pinned-surface cycles.
        # Seeded from the bootstrap snapshot so a symbol whose REST-seeded
        # price has not yet been moved by a WS message still reports a
        # meaningful age ("stable since startup"), not n/a.
        _now = time.perf_counter()
        self.last_price_change_t = {sym: _now for sym in self.books}

    @property
    def connected(self) -> bool:
        return self._conns > 0 and self._conns_up == self._conns

    @property
    def has_pending(self) -> bool:
        return bool(self._dirty)

    def consume_dirty(self) -> set:
        """Return a snapshot of dirty symbols and clear the set + event."""
        dirty = self._dirty
        self._dirty = set()
        self.update_event.clear()
        return dirty

    def quote_age(self, symbol):
        """Seconds since `symbol`'s inside price last changed, or None if no
        message has been received for it yet."""
        ts = self.last_price_change_t.get(symbol)
        if ts is None:
            return None
        return time.perf_counter() - ts

    async def start(self):
        self._session = aiohttp.ClientSession()
        chunks = [self._symbols[i:i + MAX_STREAMS_PER_CONN]
                  for i in range(0, len(self._symbols), MAX_STREAMS_PER_CONN)]
        self._conns = len(chunks)
        for conn_id, chunk in enumerate(chunks):
            self._tasks.append(asyncio.create_task(self._run(conn_id, chunk)))

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()

    async def _run(self, conn_id, syms):
        streams = [f"{s.replace('_', '').lower()}@bookTicker" for s in syms]
        backoff = 1
        while True:
            try:
                async with self._session.ws_connect(BINANCE_WS_BASE, heartbeat=30) as ws:
                    for i in range(0, len(streams), SUBSCRIBE_BATCH):
                        await ws.send_json({
                            "method": "SUBSCRIBE",
                            "params": streams[i:i + SUBSCRIBE_BATCH],
                            "id": conn_id * 100000 + i,
                        })
                        await asyncio.sleep(SUBSCRIBE_PACING)
                    self._conns_up += 1
                    backoff = 1
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        self._conns_up -= 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠ Binance WS conn {conn_id} error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _handle(self, data):
        try:
            t = json.loads(data)
        except json.JSONDecodeError:
            return
        # bookTicker payload: {u, s, b, B, a, A}. Subscription acks lack 's'/'b'.
        if not isinstance(t, dict) or 's' not in t or 'b' not in t:
            return
        norm = self._rev.get(t['s'])
        if not norm:
            return
        try:
            new_bid = float(t['b'])
            new_ask = float(t['a'])
            new_bid_qty = float(t['B'])
            new_ask_qty = float(t['A'])
        except (KeyError, ValueError, TypeError):
            return
        prev = self.books.get(norm)
        # Bump the price-change clock only when the inside price actually
        # moved; size-only refreshes don't affect the cycle's surface rate.
        # First-time-seen symbols also get a timestamp so quote_age never
        # silently returns n/a once we've heard from a pair.
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

    def coverage(self, symbols) -> int:
        return sum(1 for s in symbols if s in self.books)


class BinanceDepthFeed:
    """Streams Binance partial-book depth (top-N) snapshots for a dynamic
    watchlist of symbols, used by the scanner to depth-validate cycles that
    precursor-tracking has flagged as approaching the gate.

    Maintains `depth_books[BASE_QUOTE] = {'bids': [[p,q],...], 'asks': [...]}`
    with up to `DEPTH_LEVELS` levels per side, refreshed every `DEPTH_SPEED`.
    The watchlist can be changed at runtime via `set_watchlist(symbols)`; the
    feed sends SUBSCRIBE / UNSUBSCRIBE messages for the delta.

    Uses the combined-stream endpoint because partial-book payloads do not
    carry the symbol — the `{stream, data}` wrapper is needed to route
    updates to the right book.
    """

    def __init__(self):
        self.depth_books = {}             # 'BTC_USDT' -> {'bids': [...], 'asks': [...]}
        self._subscribed: set = set()      # currently-subscribed normalized symbols
        self._target: set = set()          # last set_watchlist target
        self._rev = {}                     # 'BTCUSDT' (upper) -> 'BTC_USDT'
        self.messages = 0
        self._ws = None
        self._task = None
        self._session = None
        self._req_id = 0

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

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

    async def set_watchlist(self, symbols):
        """Replace the watchlist. Diffs against the current subscription set
        and sends SUBSCRIBE / UNSUBSCRIBE for additions and removals.
        Idempotent — safe to call every slow tick."""
        target = set(symbols)
        self._target = target
        # Keep the reverse-map in sync so any in-flight messages still resolve.
        self._rev = {s.replace('_', '').upper(): s for s in target}
        if not self.connected:
            return  # _run reconciles on (re)connect
        to_add = target - self._subscribed
        to_remove = self._subscribed - target
        if to_add:
            await self._send_subs('SUBSCRIBE', to_add)
            self._subscribed |= to_add
        if to_remove:
            await self._send_subs('UNSUBSCRIBE', to_remove)
            self._subscribed -= to_remove
            for s in to_remove:
                self.depth_books.pop(s, None)

    @staticmethod
    def _stream_name(symbol):
        return f"{symbol.replace('_', '').lower()}@depth{DEPTH_LEVELS}@{DEPTH_SPEED}"

    async def _send_subs(self, method, symbols):
        if not self._ws:
            return
        params = [self._stream_name(s) for s in symbols]
        for i in range(0, len(params), SUBSCRIBE_BATCH):
            self._req_id += 1
            await self._ws.send_json({
                'method': method,
                'params': params[i:i + SUBSCRIBE_BATCH],
                'id': self._req_id,
            })
            await asyncio.sleep(SUBSCRIBE_PACING)

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with self._session.ws_connect(BINANCE_WS_COMBINED, heartbeat=30) as ws:
                    self._ws = ws
                    self._subscribed = set()
                    self.depth_books = {}          # stale on reconnect
                    if self._target:
                        await self._send_subs('SUBSCRIBE', self._target)
                        self._subscribed = set(self._target)
                    backoff = 1
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        self._ws = None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠ Binance DepthFeed error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        # Combined-stream payloads: {"stream": "btcusdt@depth20@100ms", "data": {...}}
        # Subscription acks lack the wrapper.
        stream = msg.get('stream')
        data = msg.get('data')
        if not stream or not isinstance(data, dict):
            return
        sym_upper = stream.split('@', 1)[0].upper()
        norm = self._rev.get(sym_upper)
        if not norm:
            return
        try:
            bids = [[float(p), float(q)] for p, q in data.get('bids', [])]
            asks = [[float(p), float(q)] for p, q in data.get('asks', [])]
        except (ValueError, TypeError):
            return
        if not bids or not asks:
            return
        self.depth_books[norm] = {'bids': bids, 'asks': asks}
        self.messages += 1
