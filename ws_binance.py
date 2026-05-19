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
import aiohttp

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"
MAX_STREAMS_PER_CONN = 900   # Binance hard limit is 1024 — leave margin
SUBSCRIBE_BATCH = 100        # stream names per SUBSCRIBE message
SUBSCRIBE_PACING = 0.3       # seconds between SUBSCRIBE messages (limit: 5/s)


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

    @property
    def connected(self) -> bool:
        return self._conns > 0 and self._conns_up == self._conns

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
            self.books[norm] = {
                'bid': float(t['b']),
                'ask': float(t['a']),
                'bid_qty': float(t['B']),
                'ask_qty': float(t['A']),
            }
            self.messages += 1
        except (KeyError, ValueError, TypeError):
            return

    def coverage(self, symbols) -> int:
        return sum(1 for s in symbols if s in self.books)
