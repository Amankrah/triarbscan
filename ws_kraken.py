"""Live Kraken feeds (WebSocket v2): top-of-book ticker and L2 partial book.

Kraken's v2 ticker channel pushes top-of-book bid/ask/qty per pair on every
inside-quote change. The v2 book channel sends a snapshot on subscribe plus
incremental updates — each update modifies a price level (qty=0 removes it).

Both feeds expose the same interface as their Binance counterparts so the
multi-venue scanner can drive them identically.
"""
import asyncio
import json
import time
import aiohttp

KRAKEN_WS_V2 = "wss://ws.kraken.com/v2"
KRAKEN_DEPTH_LEVELS = 25     # v2 book channel supports 10, 25, 100, 500, 1000


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


class KrakenDepthFeed:
    """Live Kraken L2 partial-book feed for a dynamic watchlist.

    Subscribes to the v2 `book` channel for symbols promoted by precursor
    tracking; maintains `depth_books[BASE_QUOTE] = {'bids': [[p,q],...],
    'asks': [[p,q],...]}` with up to `KRAKEN_DEPTH_LEVELS` levels per side.

    Kraken's book channel uses a snapshot+diff protocol: a snapshot on
    subscribe, then per-level updates where `qty == 0` removes the price
    level and any other qty sets it. We mirror the book as a `{price: qty}`
    dict per side and materialise sorted lists on every change so the scan
    layer sees the same `{bids, asks}` shape that BinanceDepthFeed produces.

    Same interface as BinanceDepthFeed so the orchestrator can drive it
    uniformly. The same XBT→BTC translation as KrakenBookFeed applies on the
    subscribe path."""

    def __init__(self, symbol_to_wsname):
        # Full mapping from the adapter; subset gets selected by set_watchlist.
        self._sym_to_ws_full = dict(symbol_to_wsname)
        self._sym_to_ws = {}      # normalized BASE_QUOTE -> translated wsname
        self._ws_to_sym = {}      # translated wsname -> normalized BASE_QUOTE
        self.depth_books = {}     # BASE_QUOTE -> {'bids': [[p,q],...], 'asks': [...]}
        # Internal price-keyed maps; we re-sort to materialise depth_books.
        self._bids_by_sym = {}
        self._asks_by_sym = {}
        self._target = set()      # last set_watchlist target
        self._subscribed = set()  # currently-subscribed translated wsnames
        self.messages = 0
        self._ws = None
        self._task = None
        self._session = None
        self._req_id = 0

    @staticmethod
    def _translate(wsname):
        """Match KrakenBookFeed: v2 WS rejects legacy XBT, accepts BTC."""
        return wsname.replace('XBT', 'BTC')

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
        and sends subscribe / unsubscribe for additions and removals.
        Idempotent — safe to call every slow tick."""
        target = set(symbols)
        self._target = target
        # Build the new (translated) wsname pair maps but keep the OLD
        # reverse map around so we can resolve unsubscribed wsnames back to
        # their BASE_QUOTE symbols and drop their books.
        old_ws_to_sym = dict(self._ws_to_sym)
        new_sym_to_ws = {}
        for sym in target:
            full_ws = self._sym_to_ws_full.get(sym)
            if full_ws:
                new_sym_to_ws[sym] = self._translate(full_ws)
        self._sym_to_ws = new_sym_to_ws
        self._ws_to_sym = {v: k for k, v in new_sym_to_ws.items()}

        if not self.connected:
            return                          # _run reconciles on (re)connect

        target_wsnames = set(new_sym_to_ws.values())
        to_add = target_wsnames - self._subscribed
        to_remove = self._subscribed - target_wsnames
        if to_add:
            await self._send_sub('subscribe', to_add)
            self._subscribed |= to_add
        if to_remove:
            await self._send_sub('unsubscribe', to_remove)
            self._subscribed -= to_remove
            for wsname in to_remove:
                sym = old_ws_to_sym.get(wsname)
                if sym:
                    self.depth_books.pop(sym, None)
                    self._bids_by_sym.pop(sym, None)
                    self._asks_by_sym.pop(sym, None)

    async def _send_sub(self, method, wsnames):
        if not self._ws:
            return
        self._req_id += 1
        params = {
            "channel": "book",
            "symbol": list(wsnames),
            "depth": KRAKEN_DEPTH_LEVELS,
        }
        if method == 'subscribe':
            params['snapshot'] = True
        await self._ws.send_json({
            "method": method,
            "params": params,
            "req_id": self._req_id,
        })

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with self._session.ws_connect(KRAKEN_WS_V2, heartbeat=30) as ws:
                    self._ws = ws
                    self._subscribed = set()
                    self.depth_books = {}            # stale on reconnect
                    self._bids_by_sym = {}
                    self._asks_by_sym = {}
                    target_wsnames = set(self._sym_to_ws.values())
                    if target_wsnames:
                        await self._send_sub('subscribe', target_wsnames)
                        self._subscribed = set(target_wsnames)
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
                print(f"⚠ Kraken DepthFeed error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        if msg.get('channel') != 'book':
            return                                   # status, ack, heartbeat
        msg_type = msg.get('type')
        data = msg.get('data') or []
        if not isinstance(data, list):
            return
        for entry in data:
            wsname = entry.get('symbol')
            norm = self._ws_to_sym.get(wsname)
            if not norm:
                continue
            bids = entry.get('bids', [])
            asks = entry.get('asks', [])
            if msg_type == 'snapshot':
                self._apply_snapshot(norm, bids, asks)
            elif msg_type == 'update':
                self._apply_update(norm, bids, asks)
            else:
                continue
            self.messages += 1

    def _apply_snapshot(self, sym, bids, asks):
        try:
            self._bids_by_sym[sym] = {
                float(b['price']): float(b['qty'])
                for b in bids if float(b.get('qty', 0)) > 0
            }
            self._asks_by_sym[sym] = {
                float(a['price']): float(a['qty'])
                for a in asks if float(a.get('qty', 0)) > 0
            }
        except (KeyError, ValueError, TypeError):
            return
        self._rebuild(sym)

    def _apply_update(self, sym, bids, asks):
        bb = self._bids_by_sym.get(sym)
        aa = self._asks_by_sym.get(sym)
        if bb is None or aa is None:
            return                                   # snapshot not yet received
        try:
            for b in bids:
                price = float(b['price'])
                qty = float(b['qty'])
                if qty == 0.0:
                    bb.pop(price, None)
                else:
                    bb[price] = qty
            for a in asks:
                price = float(a['price'])
                qty = float(a['qty'])
                if qty == 0.0:
                    aa.pop(price, None)
                else:
                    aa[price] = qty
        except (KeyError, ValueError, TypeError):
            return
        self._rebuild(sym)

    def _rebuild(self, sym):
        bb = self._bids_by_sym.get(sym, {})
        aa = self._asks_by_sym.get(sym, {})
        # Trim to the partial-book depth — book updates beyond N levels can
        # arrive and would inflate the local map otherwise.
        bids = sorted(bb.items(), key=lambda x: -x[0])[:KRAKEN_DEPTH_LEVELS]
        asks = sorted(aa.items(), key=lambda x: x[0])[:KRAKEN_DEPTH_LEVELS]
        self.depth_books[sym] = {
            'bids': [[p, q] for p, q in bids],
            'asks': [[p, q] for p, q in asks],
        }
