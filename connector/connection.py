import asyncio
import json
import logging
import random

from connector.ws_client import WSClient
from connector.data_manager import DataManager
from connector.schemas import SCHEMAS

logger = logging.getLogger(__name__)



class Connection:
    def __init__(self, conn_id: int, symbols: list, market_type: str, data_dir, max_symbols: int = 150, check_interval: int = 30):
        self.conn_id = conn_id
        self.symbols = symbols
        self.market_type = market_type
        self.max_symbols = max_symbols
        self.msg_count = 0
        self._check_interval = check_interval

        self.data_manager = DataManager(
            market_type='futures' if self.market_type == 'futures' else 'spot',
            conn_id=self.conn_id,
            schemas=SCHEMAS,
            flush_interval=30.0,
            data_dir=data_dir
        )

        for sym in self.symbols:
            self.data_manager.add_symbol(sym)

        self.ws = WSClient(
            conn_id=self.conn_id,
            url="wss://fstream.binance.com/ws" if self.market_type == 'futures' else 'wss://stream.binance.com:9443/ws',
            on_message=self._on_message,
            on_connect=self._on_connect
        )

    async def _on_message(self, raw: str):
        """Async обёртка для data_manager.add"""
        self.msg_count += 1
        self.data_manager.add(raw)

    async def _on_connect(self):
        """Вызывается после каждого (пере)подключения."""
        # logger.info(f"Коннектор [{self.market_type} {self.conn_id}] подключен, подписываемся на {len(self.symbols)} символов")
        await self._subscribe()
    
    async def add_symbol(self, symbol) -> bool:
        if len(self.symbols) >= self.max_symbols:
            return False

        self.data_manager.add_symbol(symbol)
        params = [f"{symbol}@trade", f"{symbol}@depth@100ms"]
        msg = json.dumps({
            "method": "SUBSCRIBE",
            "params": params,
            "id": random.randint(1, 10000),
        })

        await self.ws._send_message(msg)
        self.symbols.append(symbol)
    
    async def remove_symbol(self, symbol):
        params = [f"{symbol}@trade", f"{symbol}@depth@100ms"]
        msg = json.dumps({
            "method": "UNSUBSCRIBE",
            "params": params,
            "id": random.randint(1, 10000),
        })
        await self.ws._send_message(msg)
        self.data_manager._flush_symbol(symbol)
        self.data_manager.remove_symbol(symbol)
        self.symbols.remove(symbol)

    async def _subscribe(self):
        """Подписка на стримы после подключения"""

        stream_types = ["trade", "depth@100ms"]
        all_streams = [f"{sym}@{st}" for sym in self.symbols for st in stream_types]

        msg = json.dumps({
            "method": "SUBSCRIBE",
            "params": all_streams,
            "id": random.randint(1, 10000),
        })

        await self.ws._send_message(msg)

    async def _watchdog(self):
        """Проверяет что сообщения приходят, иначе переподключает"""
        while True:
            
            await asyncio.sleep(self._check_interval)

            if self.msg_count == 0 and self.symbols:
                logger.warning(
                    f"[{self.conn_id}] 0 сообщений за {self._check_interval}с, "
                    f"переподключаю..."
                )
                if self.ws._ws:
                    await self.ws._ws.close()
                # ws.run() сам переподключится и вызовет on_connect
            else:
                logger.debug(
                    f"[{self.market_type} {self.conn_id}] {self.msg_count} сообщений за {self._check_interval} с., "
                    f"symbols={len(self.symbols)}"
                )
            self.msg_count = 0

    async def run(self):
        """Запускает WS и подписывается"""
        dm_task = asyncio.create_task(self.data_manager.run())

        # запускаем WS в фоне
        ws_task = asyncio.create_task(self.ws.run())

        wd_task = asyncio.create_task(self._watchdog())
        # ждём подключения
        while not self.ws.is_connected:
            await asyncio.sleep(0.1)

        # ждём пока WS работает
        try:
            await ws_task
        finally:
            wd_task.cancel()
            dm_task.cancel()
            self.data_manager.stop()





logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)


async def main():
    symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
    connector = Connection(conn_id=1, symbols=symbols)
    
    run_task = asyncio.create_task(connector.run())  

    await asyncio.sleep(10)
    await connector.add_symbol("riverusdt")
    await asyncio.sleep(30)
    await connector.remove_symbol("riverusdt")

    await run_task


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        logger.info("uvloop installed")
    except ImportError:
        pass

    asyncio.run(main())