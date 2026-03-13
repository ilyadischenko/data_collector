import asyncio
import json
import logging
from pathlib import Path


from connection import Connection
from api_manager import ApiManager
from request_ws_connection import RequestWsConnection
from monitor import Monitor



logger = logging.getLogger(__name__)

BLACKLIST_FILE = Path("../blacklist.json")

class ConnectorsManager:
    def __init__(self):
        self.tasks = []

        self.connections = {
            "futures": [],
            "spot": [],
        }
        
        self.futures_symbols: list[str] = []
        self.spot_symbols: list[str] = []

        """Загружает blacklist из файла при старте."""
        if not BLACKLIST_FILE.exists():
            self._save_blacklist()
            return

        try:
            with open(BLACKLIST_FILE, "r") as f:
                data = f.read().strip()
                if data:
                    self.blacklist = json.loads(data)
                    logger.info(
                        f"Blacklist загружен: "
                        f"futures={len(self.blacklist['futures'])}, "
                        f"spot={len(self.blacklist['spot'])}"
                    )
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Ошибка чтения blacklist: {e}, создаю новый")
            self.blacklist = {"futures": [], "spot": []}
            self._save_blacklist()

    def _save_blacklist(self):
        """Сохраняет blacklist в файл."""
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(self.blacklist, f, indent=2)
        logger.debug(f"Blacklist сохранён: {BLACKLIST_FILE}")

    async def add_to_blacklist(self, symbol: str, market_type: str):
        symbol = symbol.lower()
        if symbol in self.blacklist[market_type]:
            return

        self.blacklist[market_type].append(symbol)
        self._save_blacklist()

        # говорим api_manager не собирать снапшоты
        await self.api_manager.add_to_blacklist(symbol, market_type)

        # отписываем WS
        for conn in self.connections[market_type]:
            if symbol in conn.symbols:
                await conn.remove_symbol(symbol)
                break

    async def remove_from_blacklist(self, symbol: str, market_type: str):
        symbol = symbol.lower()
        if symbol not in self.blacklist[market_type]:
            return

        self.blacklist[market_type].remove(symbol)
        self._save_blacklist()

        await self.api_manager.remove_from_blacklist(symbol, market_type)
        # WS подписка появится сама через _check_symbols в ApiManager


    def _batch_symbols(self, symbols: list[str], batch_size: int = 100) -> list[list[str]]:
        return [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    def create_connection(self, type: str, symbols: list[str]):
        conn_id = len(self.connections[type]) + 1
        conn = Connection(conn_id=conn_id, symbols=symbols, market_type=type)
        self.connections[type].append(conn)
        return conn

    async def _on_symbol_added(self, symbol: str, market_type: str):
        """Коллбэк — новый символ появился на бирже."""
        added = 0
        for conn in self.connections[market_type]:
            if len(conn.symbols) < conn.max_symbols and symbol not in conn.symbols:
                await conn.add_symbol(symbol)
                added += 1
                if added == 2:
                    break

        # если не нашли два коннекта с местом — создаём новые
        while added < 2:
            conn = self.create_connection(market_type, [symbol])
            self.tasks.append(asyncio.create_task(conn.run()))
            added += 1

        if market_type == 'futures':
            self.futures_request_ws.add_symbol(symbol)
        else:
            self.spot_request_ws.add_symbol(symbol)
    
    async def _on_symbol_removed(self, symbol: str, market_type: str):
        """Коллбэк — символ исчез с биржи."""
        removed = 0
        for conn in self.connections[market_type]:
            if symbol in conn.symbols:
                await conn.remove_symbol(symbol)
                removed += 1
                if removed == 2:
                    break
        if market_type == 'futures':
            self.futures_request_ws.remove_symbol(symbol)
        else:
            self.spot_request_ws.remov_symbol(symbol)

    async def run(self):
        self.api_manager = ApiManager(
            on_add_symbol=self._on_symbol_added,
            on_remove_symbol=self._on_symbol_removed,
        )

        self.tasks.append(asyncio.create_task(self.api_manager.run()))
        
        await self.api_manager.ready.wait()

        self.futures_request_ws = RequestWsConnection(conn_id=1, symbols=self.api_manager.futures_symbols, market_type='futures')
        self.tasks.append(asyncio.create_task(self.futures_request_ws.run()))
        self.spot_request_ws = RequestWsConnection(conn_id=1, symbols=self.api_manager.spot_symbols, market_type='spot')
        self.tasks.append(asyncio.create_task(self.spot_request_ws.run()))

        futures_batches = self._batch_symbols(self.api_manager.futures_symbols)
        spot_batches = self._batch_symbols(self.api_manager.spot_symbols)

        for _ in range(2):
            for i in futures_batches:
                self.create_connection(type='futures', symbols=i)
            
            for i in spot_batches:
                self.create_connection(type='spot', symbols=i)

        logger.info(f"Создано {len(self.connections['futures'])} futures коннектов и {len(self.connections['spot'])} спотовых коннектов")

        for i in self.connections['futures']:
            self.tasks.append(asyncio.create_task(i.run()))
        
        for i in self.connections['spot']:
            self.tasks.append(asyncio.create_task(i.run()))

        results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Таск {i} упал: {result}")



    async def stop(self):
        """Останавливает все коннекты."""
        for connector in self.connections["futures"]:
            await connector.stop()
        for connector in self.connections["spot"]:
            await connector.stop()
        logger.info("Все коннекты остановлены")










# ── запуск ──

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    monitor = Monitor()
    asyncio.create_task(monitor.run())
    manager = ConnectorsManager()
    

    try:
        await manager.run()
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
        await manager.stop()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    asyncio.run(main())