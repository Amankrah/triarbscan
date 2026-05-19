import asyncio
import sys
import aiohttp
from poloniex_adapter import PoloniexAdapter
from binance_adapter import BinanceAdapter
from kraken_adapter import KrakenAdapter
from kucoin_adapter import KuCoinAdapter

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass


async def test_exchange(adapter, name):
    print(f"\n{'=' * 60}\nTesting {name}\n{'=' * 60}")

    try:
        async with aiohttp.ClientSession() as session:
            tickers = await adapter.get_all_tickers_async(session)

        if not tickers:
            print("✗ Failed to fetch tickers")
            return False

        print(f"✓ {len(tickers)} tickers")
        for ticker in tickers[:3]:
            print(f"  {ticker['symbol']}: bid={ticker['bid']} ask={ticker['ask']}")

        btc_pair = None
        for t in tickers:
            base, quote = adapter.parse_symbol(t['symbol'])
            if base == 'BTC' and quote in ('USDT', 'USD', 'USDC'):
                btc_pair = t['symbol']
                break

        if btc_pair:
            async with aiohttp.ClientSession() as session:
                ob = await adapter.get_orderbook_async(session, btc_pair, 5)
            if ob.get('bids') and ob.get('asks'):
                print(f"✓ Order book for {btc_pair}: {len(ob['bids'])} bids, {len(ob['asks'])} asks")
            else:
                print("✗ Empty order book")
                return False

        tradeable = adapter.get_tradeable_pairs(tickers)
        print(f"✓ {len(tradeable)} tradeable pairs")
        return True

    except Exception as e:
        print(f"✗ {name} failed: {e}")
        return False


async def main():
    adapters = [
        (PoloniexAdapter(), "Poloniex"),
        (BinanceAdapter(), "Binance"),
        (KrakenAdapter(), "Kraken"),
        (KuCoinAdapter(), "KuCoin"),
    ]
    results = {}
    for adapter, name in adapters:
        results[name] = await test_exchange(adapter, name)

    print(f"\n{'=' * 60}\nSUMMARY\n{'=' * 60}")
    for name, ok in results.items():
        print(f"  {name}: {'PASS' if ok else 'FAIL'}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
