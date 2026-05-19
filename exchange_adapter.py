from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
import requests
import asyncio
import aiohttp
import time


class ExchangeAdapter(ABC):
    """Normalized interface for exchange REST APIs."""

    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url
        self.max_retries = 3
        self.retry_delay = 1

    @abstractmethod
    def get_all_tickers(self) -> List[Dict]:
        pass

    @abstractmethod
    async def get_all_tickers_async(self, session: aiohttp.ClientSession) -> List[Dict]:
        pass

    @abstractmethod
    def get_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        pass

    @abstractmethod
    async def get_orderbook_async(self, session: aiohttp.ClientSession,
                                   symbol: str, limit: int = 20) -> Dict:
        pass

    @abstractmethod
    def normalize_symbol(self, base: str, quote: str) -> str:
        pass

    @abstractmethod
    def parse_symbol(self, symbol: str) -> Tuple[str, str]:
        pass

    def get_tradeable_pairs(self, tickers: List[Dict]) -> List[str]:
        tradeable = []
        for ticker in tickers:
            try:
                if (ticker.get('symbol') and
                    ticker.get('bid') and ticker.get('ask') and
                    float(ticker['bid']) > 0 and float(ticker['ask']) > 0):
                    tradeable.append(ticker['symbol'])
            except (ValueError, TypeError):
                continue
        return tradeable

    def fetch_with_retry(self, url: str) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    print(f"⚠ {self.name} attempt {attempt + 1}/{self.max_retries} failed: {type(e).__name__}")
                    time.sleep(self.retry_delay)
                else:
                    print(f"❌ {self.name} all attempts failed: {e}")
                    return None
        return None

    async def fetch_with_retry_async(self, session: aiohttp.ClientSession,
                                     url: str) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    print(f"⚠ {self.name} async attempt {attempt + 1}/{self.max_retries} failed: {type(e).__name__}")
                    await asyncio.sleep(self.retry_delay)
                else:
                    print(f"❌ {self.name} async all attempts failed: {e}")
                    return None
        return None

    def __repr__(self):
        return f"<{self.__class__.__name__} exchange='{self.name}'>"
