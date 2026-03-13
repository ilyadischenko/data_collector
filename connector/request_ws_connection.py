import asyncio
from datetime import datetime
import json
import logging
import random
import time

import orjson
from request_ws_data_manager import SnapshotWriter
from ws_client import WSClient
from data_manager import DataManager
from schemas import SCHEMAS
import uuid


logger = logging.getLogger(__name__)



class RequestWsConnection:
    def __init__(self, conn_id: int, symbols: list, market_type: str, ob_limit: int = 100, interval: int = 300):
        self.conn_id = conn_id
        self.symbols = symbols
        self.market_type = market_type
        self.ob_limit = ob_limit
        self.interval = interval
        self.snapshot_writer = SnapshotWriter(market_type=self.market_type, flush_interval=30.0)

        self.callbacks = {}
        
        self.is_waiting_for_rate_limit = False
        self.rate_limit_minute = -1

        self.ws = WSClient(
            conn_id=self.conn_id,
            url="wss://ws-fapi.binance.com/ws-fapi/v1" if self.market_type == 'futures' else 'wss://ws-api.binance.com:443/ws-api/v3',
            on_message=self._on_message,
            on_connect=self._on_connect
        )

    async def _on_message(self, raw: str):
        """Async обёртка для data_manager.add"""
        data = orjson.loads(raw)

        status = data.get('status')
        request_id = data.get('id')
        if status == 200 and request_id in self.callbacks:
            self.snapshot_writer.add(symbol=self.callbacks[request_id]['s'], data=data['result'])
            self.callbacks.pop(request_id, None)
        elif status and status != 200:
            logger.warning(f"Ошибка ответа [{status}]: {data.get('error')}, id={request_id}")
            self.callbacks.pop(request_id, None)
        elif status is None:
            logger.debug(f"Служебное сообщение без статуса: {raw}")
            return  # пинг или другое служебное — дальше не обрабатываем

        rl = data.get('rateLimits', [])

        for i in rl:
            if i['rateLimitType'] == 'REQUEST_WEIGHT' and i['limit'] * 0.7 < i['count']:
                logger.debug(f'Приближаемся к лимиту {i["rateLimitType"]} для {self.market_type} ({i["count"]}/{i["limit"]}), ставим флаг ожидания')
                now = datetime.now()
        
                # если флаг уже стоит и минута уже сменилась — игнорируем
                if self.is_waiting_for_rate_limit and now.minute != self.rate_limit_minute:
                    self.is_waiting_for_rate_limit = False
                    continue
                
                if not self.is_waiting_for_rate_limit:
                    self.rate_limit_minute = now.minute
                    self.is_waiting_for_rate_limit = True
                    asyncio.create_task(self._wait_rate_limit(now))


    async def _wait_rate_limit(self, triggered_at: datetime):
        wait = 60 - triggered_at.second - triggered_at.microsecond / 1_000_000
        logger.debug(f"[{self.market_type}] Пауза {wait:.1f}с до сброса минуты")
        await asyncio.sleep(max(wait, 1))
        # проверяем что минута действительно сменилась
        if datetime.now().minute != triggered_at.minute:
            self.is_waiting_for_rate_limit = False
            logger.debug(f"[{self.market_type}] Rate limit сброшен")
        else:
            # ещё не сменилась — ждём ещё секунду
            await asyncio.sleep(1)
            self.is_waiting_for_rate_limit = False
            logger.debug(f"[{self.market_type}] Rate limit сброшен (доп. ожидание)")

    async def _on_connect(self):
        """Вызывается после каждого (пере)подключения."""
        logger.info(f"Реквест коннектор [{self.market_type} {self.conn_id}] подключен")

    async def add_symbol(self, symbol):
        if symbol in self.symbols:
            return
        
        self.symbols.append(symbol)
    
    async def remove_symbol(self, symbol):
        if symbol in self.symbols:
            self.symbols.remove(symbol)
            return 
        return 
    
    async def fetch_symbol_orderbook(self, symbol: str, limit: int = 1000):
        # logger.info(f"Запрашиваю стакан для {symbol} в реквест коннекторе {self.market_type}...")
        params = {
            "symbol": symbol.upper(),
            "limit": limit
        }
        id = str(uuid.uuid4())
        msg = json.dumps({
            "id": id,
            "method": "depth",
            "params": params,
        })

        await self.ws._send_message(msg)

        self.callbacks[id] = {'s': symbol.lower(), 'ts': datetime.now().timestamp()}

    async def update_symbols_list(self, symbols: list):
        self.symbols = symbols
    
    async def _cleanup_callbacks(self, ttl: int = 60):
        while True:
            await asyncio.sleep(ttl)
            now = time.time()
            stale = [k for k, v in self.callbacks.items() if now - v['ts'] > ttl]
            for k in stale:
                logger.warning(f"Коллбэк {k} для {self.callbacks[k]['s']} завис, удаляю")
                del self.callbacks[k]

    async def start_pooling(self, interval: int):
        """Запускает цикл, который каждые interval секунд запрашивает стаканы для всех символов"""
        logger.info(f"Запускаю пуллинг стаканов для {self.market_type} каждые {interval} секунд")
        while True:
            c = 0
            for symbol in self.symbols:
                while self.is_waiting_for_rate_limit:
                    logger.debug(f"Ожидаем снятия флага ожидания для {self.market_type}...")
                    await asyncio.sleep(5)
                await self.fetch_symbol_orderbook(symbol, limit=self.ob_limit)
                await asyncio.sleep(0.1)  # небольшой интервал между запросами, чтобы не спамить слишком быстро
                c += 1
            logger.info(f"Собрал все стаканы для {self.market_type}, получилось {c} стаканов")
            await asyncio.sleep(interval)



    async def run(self):
        """Запускает WS и подписывается"""
        sw_task = asyncio.create_task(self.snapshot_writer.run())

        # запускаем WS в фоне
        ws_task = asyncio.create_task(self.ws.run())

        cleanup_task = asyncio.create_task(self._cleanup_callbacks())

        # ждём подключения
        while not self.ws.is_connected:
            await asyncio.sleep(0.1)

        pool_task = asyncio.create_task(self.start_pooling(self.interval))
        # ждём пока WS работает
        try:
            await ws_task
        finally:
            sw_task.cancel()
            pool_task.cancel()
            cleanup_task.cancel()

