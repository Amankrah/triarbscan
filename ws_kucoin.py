"""Live KuCoin feeds: top-of-book ticker and L2 partial book.

KuCoin requires a token handshake before WS: POST `/api/v1/bullet-public`
returns a token, an endpoint to connect to, and the server's ping cadence.
The token is valid for 24 hours, so we re-fetch on every reconnect — this
sidesteps token expiry entirely without needing a separate timer.

Subscribe to `/market/ticker:SYM1,SYM2,...` in batches (KuCoin recommends
splitting topics for large universes). Ticker payloads carry `bestBid`,
`bestAsk`, and sizes; the symbol is encoded in the topic string.

The L2 feed subscribes to `/spotMarket/level2Depth50:SYMBOL` per symbol on
the watchlist; KuCoin pushes full 50-level snapshots every ~100 ms so no
diff state is required (unlike Binance/Kraken, where we'd need snapshot +
update reconciliation).

Both feeds expose the same interface as their Binance counterparts so the
multi-venue scanner can drive them identically.
"""
import asyncio
import json
import time
import uuid
import aiohttp

KUCOIN_BULLET_URL = "https://api.kucoin.com/api/v1/bullet-public"
KUCOIN_SUB_BATCH = 50            # symbols per SUBSCRIBE topic string
KUCOIN_SUB_PACING = 0.15         # seconds between subscribes (cap ~10/s)
KUCOIN_DEPTH_LEVELS = 50         # /spotMarket/level2Depth50 — full snapshot push


class KuCoinBookFeed:
    """Maintains `books[BASE_QUOTE] = {bid, ask, bid_qty, ask_qty}` from the
    KuCoin `/market/ticker` stream."""

    def __init__(self, symbols, initial_books=None):
        self._symbols = sorted(set(symbols))
        # KuCoin uses BASE-QUOTE (dash); normalized form is BASE_QUOTE.
        self._kucoin_to_norm = {s.replace('_', '-'): s for s in self._symbols}
        self.books = dict(initial_books or {})
        self.messages = 0
        self._connected = False
        self._task = None
        self._ping_task = None
        self._session = None
        self._ws = None
        self._req_id = 0
        self._dirty: set = set()
        self.update_event = asyncio.Event()
        _now = time.perf_counter()
        self.last_price_change_t = {sym: _now for sym in self.books}

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None and not self._ws.closed

    @property
    def has_pending(self) -> bool:
        return bool(self._dirty)

    def coverage(self, symbols) -> int:
        return sum(1 for s in symbols if s in self.books)

    def consume_dirty(self) -> set:
        d = self._dirty
        self._dirty = set()
        self.update_event.clear()
        return d

    def quote_age(self, symbol):
        ts = self.last_price_change_t.get(symbol)
        return None if ts is None else time.perf_counter() - ts

    async def start(self):
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        for t in (self._ping_task, self._task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        if self._session:
            await self._session.close()

    async def _fetch_token(self):
        """POST /api/v1/bullet-public → (token, ws_endpoint, ping_interval_s)."""
        async with self._session.post(
            KUCOIN_BULLET_URL, timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            r.raise_for_status()
            payload = await r.json()
        if payload.get('code') != '200000':
            raise RuntimeError(f"bullet-public error: {payload}")
        data = payload['data']
        token = data['token']
        instance = data['instanceServers'][0]
        endpoint = instance['endpoint']
        ping_interval_ms = instance.get('pingInterval', 18000)
        return token, endpoint, ping_interval_ms / 1000.0

    async def _run(self):
        backoff = 1
        while True:
            try:
                token, endpoint, ping_interval = await self._fetch_token()
                connect_id = str(uuid.uuid4())
                url = f"{endpoint}?token={token}&connectId={connect_id}"
                async with self._session.ws_connect(
                    url, heartbeat=ping_interval + 5
                ) as ws:
                    self._ws = ws
                    # Wait for welcome before subscribing.
                    welcome = await asyncio.wait_for(ws.receive(), timeout=10)
                    if welcome.type != aiohttp.WSMsgType.TEXT:
                        raise RuntimeError(f"welcome msg type {welcome.type}")
                    wmsg = json.loads(welcome.data)
                    if wmsg.get('type') != 'welcome':
                        raise RuntimeError(f"unexpected welcome payload: {wmsg}")

                    # Subscribe in batches.
                    kucoin_syms = [s.replace('_', '-') for s in self._symbols]
                    for i in range(0, len(kucoin_syms), KUCOIN_SUB_BATCH):
                        batch = kucoin_syms[i:i + KUCOIN_SUB_BATCH]
                        self._req_id += 1
                        await ws.send_json({
                            'id': str(self._req_id),
                            'type': 'subscribe',
                            'topic': f'/market/ticker:{",".join(batch)}',
                            'privateChannel': False,
                            'response': True,
                        })
                        await asyncio.sleep(KUCOIN_SUB_PACING)

                    self._connected = True
                    self._ping_task = asyncio.create_task(
                        self._ping_loop(ws, ping_interval)
                    )
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
                        if self._ping_task:
                            self._ping_task.cancel()
                            try:
                                await self._ping_task
                            except asyncio.CancelledError:
                                pass
                            self._ping_task = None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠ KuCoin WS error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _ping_loop(self, ws, interval):
        try:
            while True:
                await asyncio.sleep(interval)
                self._req_id += 1
                try:
                    await ws.send_json({'id': str(self._req_id), 'type': 'ping'})
                except Exception:
                    return
        except asyncio.CancelledError:
            pass

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        if msg.get('type') != 'message':
            return        # ignore welcome / ack / pong / error
        topic = msg.get('topic', '')
        if not topic.startswith('/market/ticker:'):
            return
        symbol_part = topic.split(':', 1)[1]
        # KuCoin pushes one symbol per message even when many are subscribed.
        norm = self._kucoin_to_norm.get(symbol_part)
        if not norm:
            return
        data = msg.get('data') or {}
        try:
            new_bid = float(data['bestBid'])
            new_ask = float(data['bestAsk'])
            new_bid_qty = float(data.get('bestBidSize', 0))
            new_ask_qty = float(data.get('bestAskSize', 0))
        except (KeyError, ValueError, TypeError):
            return
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


class KuCoinDepthFeed:
    """Live KuCoin L2 partial-book feed for a dynamic watchlist.

    Subscribes to `/spotMarket/level2Depth50:SYMBOL` per watched symbol; each
    push carries the full top-50 levels per side (asks ascending, bids
    descending) so we just overwrite `depth_books[BASE_QUOTE]` on each
    message — no snapshot+diff reconciliation needed.

    Same interface as `BinanceDepthFeed` / `KrakenDepthFeed`: `start()`,
    `stop()`, `set_watchlist()`, `connected`, `messages`, `depth_books`,
    `_target`. The orchestrator can drive all three uniformly."""

    def __init__(self, symbols):
        # We accept the full normalized universe upfront so `_norm_to_kucoin`
        # is complete; the actual subscribe set is whatever set_watchlist
        # filters down to.
        self._all_symbols = sorted(set(symbols))
        self._norm_to_kucoin = {s: s.replace('_', '-') for s in self._all_symbols}
        self._kucoin_to_norm = {v: k for k, v in self._norm_to_kucoin.items()}
        self.depth_books = {}                # BASE_QUOTE -> {'bids':[[p,q],...], 'asks':[...]}
        self._target = set()                 # last set_watchlist normalized syms
        self._subscribed = set()             # currently-subscribed normalized syms
        self.messages = 0
        self._ws = None
        self._task = None
        self._ping_task = None
        self._session = None
        self._req_id = 0

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    async def start(self):
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        for t in (self._ping_task, self._task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        if self._session:
            await self._session.close()

    async def set_watchlist(self, symbols):
        """Replace the watchlist. Diffs against `_subscribed` and sends per-
        symbol subscribe/unsubscribe messages for additions/removals.
        Idempotent — safe to call every slow tick."""
        target = {s for s in symbols if s in self._norm_to_kucoin}
        self._target = target

        if not self.connected:
            return                          # _run reconciles on (re)connect

        to_add = target - self._subscribed
        to_remove = self._subscribed - target
        if to_add:
            await self._send_sub('subscribe', to_add)
            self._subscribed |= to_add
        if to_remove:
            await self._send_sub('unsubscribe', to_remove)
            self._subscribed -= to_remove
            for sym in to_remove:
                self.depth_books.pop(sym, None)

    async def _send_sub(self, method, syms):
        """One WS frame per symbol. KuCoin allows comma-joined topics but
        per-symbol messages keep the diff bookkeeping trivial and the
        request rate stays well under the cap (we throttle below)."""
        if not self._ws:
            return
        for sym in syms:
            kucoin_sym = self._norm_to_kucoin.get(sym)
            if not kucoin_sym:
                continue
            self._req_id += 1
            try:
                await self._ws.send_json({
                    'id': str(self._req_id),
                    'type': method,
                    'topic': f'/spotMarket/level2Depth50:{kucoin_sym}',
                    'response': False,
                })
            except Exception:
                return
            await asyncio.sleep(KUCOIN_SUB_PACING)

    async def _fetch_token(self):
        """POST /api/v1/bullet-public → (token, ws_endpoint, ping_interval_s)."""
        async with self._session.post(
            KUCOIN_BULLET_URL, timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            r.raise_for_status()
            payload = await r.json()
        if payload.get('code') != '200000':
            raise RuntimeError(f"bullet-public error: {payload}")
        data = payload['data']
        inst = data['instanceServers'][0]
        return data['token'], inst['endpoint'], inst.get('pingInterval', 18000) / 1000.0

    async def _run(self):
        backoff = 1
        while True:
            try:
                token, endpoint, ping_interval = await self._fetch_token()
                connect_id = str(uuid.uuid4())
                url = f"{endpoint}?token={token}&connectId={connect_id}"
                async with self._session.ws_connect(
                    url, heartbeat=ping_interval + 5
                ) as ws:
                    self._ws = ws
                    welcome = await asyncio.wait_for(ws.receive(), timeout=10)
                    if welcome.type != aiohttp.WSMsgType.TEXT:
                        raise RuntimeError(f"welcome msg type {welcome.type}")
                    wmsg = json.loads(welcome.data)
                    if wmsg.get('type') != 'welcome':
                        raise RuntimeError(f"unexpected welcome payload: {wmsg}")

                    # Reconnect: drop stale books, resubscribe to current target.
                    self.depth_books = {}
                    self._subscribed = set()
                    if self._target:
                        await self._send_sub('subscribe', set(self._target))
                        self._subscribed = set(self._target)

                    self._ping_task = asyncio.create_task(
                        self._ping_loop(ws, ping_interval)
                    )
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
                        if self._ping_task:
                            self._ping_task.cancel()
                            try:
                                await self._ping_task
                            except asyncio.CancelledError:
                                pass
                            self._ping_task = None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"⚠ KuCoin DepthFeed error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _ping_loop(self, ws, interval):
        try:
            while True:
                await asyncio.sleep(interval)
                self._req_id += 1
                try:
                    await ws.send_json({'id': str(self._req_id), 'type': 'ping'})
                except Exception:
                    return
        except asyncio.CancelledError:
            pass

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        if msg.get('type') != 'message':
            return                                  # welcome / ack / pong
        topic = msg.get('topic', '')
        if not topic.startswith('/spotMarket/level2Depth50:'):
            return
        kucoin_sym = topic.split(':', 1)[1]
        norm = self._kucoin_to_norm.get(kucoin_sym)
        if not norm:
            return
        # Late pushes can arrive after we've unsubscribed but before KuCoin's
        # server-side processes it. Filter on the live target set so dropped
        # symbols don't get their books re-populated by these stragglers.
        if norm not in self._target:
            return
        data = msg.get('data') or {}
        bids_raw = data.get('bids') or []
        asks_raw = data.get('asks') or []
        try:
            # KuCoin already returns asks ascending and bids descending; we
            # rebuild as nested float lists matching Binance/Kraken shape.
            bids = [[float(p), float(q)] for p, q in bids_raw]
            asks = [[float(p), float(q)] for p, q in asks_raw]
        except (ValueError, TypeError):
            return
        self.depth_books[norm] = {'bids': bids, 'asks': asks}
        self.messages += 1
