import asyncio
from datetime import datetime
import json
import logging
from pathlib import Path
import aiohttp
import orjson




logger = logging.getLogger(__name__)


class ApiManager:
    def __init__(self,  on_add_symbol, on_remove_symbol, orderbook_limit: int = 100):
        self.session: aiohttp.ClientSession

        self.futures_symbols = []
        self.spot_symbols = []

        self.on_add_symbol = on_add_symbol
        self.on_remove_symbol = on_remove_symbol

        self.futures_minutes_limit = 0
        self.spot_minutes_limit = 0

        self.futures_used_weight = 0
        self.spot_used_weight = 0

        self.orderbook_limit = orderbook_limit
        
        self.futures_url = 'https://fapi.binance.com/fapi/v1'
        self.spot_url = 'https://api.binance.com/api/v3'

        self.ready = asyncio.Event()

        self.blacklist: dict[str, set] = {
            'futures': set(),
            'spot': set(),
        }
        self._blacklist_file = Path("../blacklist.json")

    async def request_symbols(self, market_type: str):
        async with self.session.get(url=f'{self.futures_url if market_type == 'futures' else self.spot_url}/exchangeInfo') as resp:
            if resp.status == 200:
                data = orjson.loads(await resp.read())
                
                for i in data['rateLimits']:
                    if i['rateLimitType'] == 'REQUEST_WEIGHT' and i['interval'] == 'MINUTE':
                        if market_type == 'futures':
                            self.futures_minutes_limit = i['limit']
                        elif market_type == 'spot':
                            self.spot_minutes_limit = i['limit']
            
                           # парсим символы
                symbols = []
                for item in data.get("symbols", []):
                    if market_type == "futures":
                        if (item.get("contractType") == "PERPETUAL"
                            and item.get("status") == "TRADING"
                            and item.get("quoteAsset") == "USDT"):
                            symbols.append(item["symbol"].lower())
                    else:
                        if (item.get("status") == "TRADING"
                            and item.get("quoteAsset") in ("BTC", "ETH", "USDT", "BNB", "USDC")):
                            symbols.append(item["symbol"].lower())
                
                symbols = self._filter_blacklisted(symbols, market_type)

                if market_type == "futures":
                    self.futures_symbols = symbols
                else:
                    self.spot_symbols = symbols

                logger.info(f"Загружено {len(symbols)} {market_type} символов")
                return symbols

            elif resp.status == 429:
                logger.error('Rate limit на запрос символов')
                await asyncio.sleep(5)
    
    async def _poll_snapshots(self, market_type: str, interval: int = 3600):
        """Опрашивает все символы по кругу с равномерной задержкой."""
        while True:
            symbols = self.futures_symbols if market_type == 'futures' else self.spot_symbols
            max_limit = self.futures_minutes_limit if market_type == 'futures' else self.spot_minutes_limit

            if not symbols:
                await asyncio.sleep(5)
                continue

            interval = 3600

            for symbol in list(symbols):  # list() — копия, если symbols обновится в процессе
                used_weight = self.futures_used_weight if market_type == 'futures' else self.spot_used_weight

                if used_weight > max_limit * 0.8:  # если использовано больше 80% лимита, ждем до следующей минуты
                    wait = 60 - datetime.now().second + 1  # ждем до начала следующей минуты
                    logger.warning(f"[{market_type}] Лимит {used_weight}/{max_limit}, пауза {wait}с")
                    await asyncio.sleep(wait)

                for attempt in range(3):
                    success = await self.request_orderbook(symbol, market_type)
                    if success:
                        break
                    logger.warning(f"[{market_type}] {symbol} попытка {attempt + 1}/3 неудачна")
                    await asyncio.sleep(2 ** attempt)  # 1с, 2с, 4с
                else:
                    logger.error(f"[{market_type}] {symbol} не удалось получить стакан после 3 попыток")

            await asyncio.sleep(interval)

    def _load_blacklist(self):
            if not self._blacklist_file.exists():
                return
            try:
                with open(self._blacklist_file) as f:
                    data = json.loads(f.read())
                    self.blacklist = {
                        'futures': set(data.get('futures', [])),
                        'spot': set(data.get('spot', [])),
                    }
            except Exception as e:
                logger.warning(f"Ошибка чтения blacklist: {e}")

    def _filter_blacklisted(self, symbols: list[str], market_type: str) -> list[str]:
        return [s for s in symbols if s not in self.blacklist[market_type]]

    async def add_to_blacklist(self, symbol: str, market_type: str):
        self.blacklist[market_type].add(symbol)
        # убираем из списков снапшотов
        if market_type == 'futures' and symbol in self.futures_symbols:
            self.futures_symbols.remove(symbol)
        elif market_type == 'spot' and symbol in self.spot_symbols:
            self.spot_symbols.remove(symbol)

    async def remove_from_blacklist(self, symbol: str, market_type: str):
        self.blacklist[market_type].discard(symbol)
        # символ появится сам при следующем _check_symbols

    async def _check_symbols(self, market_type: str, on_add, on_remove, interval: int = 300):
        while True:
            await asyncio.sleep(interval)
            try:
                old = set(self.futures_symbols if market_type == 'futures' else self.spot_symbols)
                new = set(await self.request_symbols(market_type))

                for symbol in new - old:
                    logger.info(f"[{market_type}] Новый символ: {symbol}")
                    await on_add(symbol, market_type)

                for symbol in old - new:
                    logger.info(f"[{market_type}] Удалён символ: {symbol}")
                    await on_remove(symbol, market_type)

            except Exception as e:
                logger.error(f"Ошибка проверки символов [{market_type}]: {e}")

    async def run(self):
        self.session = aiohttp.ClientSession()
        self._load_blacklist()
        
        await self.request_symbols('futures')
        await self.request_symbols('spot')
        
        logger.info(f"API Manager готов. futures_symbols={len(self.futures_symbols)}, spot_symbols={len(self.spot_symbols)}")

        self.ready.set()

        await asyncio.gather(
            # self._poll_snapshots('futures'),
            # self._poll_snapshots('spot'),
            self._check_symbols('futures', self.on_add_symbol, self.on_remove_symbol),
            self._check_symbols('spot', self.on_add_symbol, self.on_remove_symbol),
        )


    async def stop(self):
        await self.session.close()