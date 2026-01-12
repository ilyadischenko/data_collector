import asyncio
from collections import deque
import websockets
import json
import gzip
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Set, Dict

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


class GateCollector:
    def __init__(self, settle: str = "usdt"):
        """
        Args:
            settle: Тип расчёта - "usdt" или "btc"
        """
        self.ws_url = f"wss://fx-ws.gateio.ws/v4/ws/{settle}"
        self.exchange = "gate"
        self.settle = settle
        self.active_symbols: Set[str] = set()
        
        # Разделяем буферы для trades и orderbook
        self.symbol_buffers: Dict[str, Dict[str, deque]] = {}
        
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        self.is_running = False
        self.logger = logging.getLogger("Gate")
        self.subscription_lock = asyncio.Lock()
        self.ws = None
        self.loop = None

    async def run(self):
        self.loop = asyncio.get_running_loop()
        self.is_running = True
        self.logger.info("🚀 Started Gate Collector")
        await asyncio.gather(
            self._ws_listener(), 
            self._periodic_disk_flush(),
            self._ping_loop()
        )

    async def stop(self):
        self.is_running = False
        await self.flush_memory()
        if self.ws: 
            await self.ws.close()
        self.thread_pool.shutdown(wait=True)

    def _ws_is_connected(self) -> bool:
        """Проверка что WebSocket подключён."""
        if self.ws is None:
            return False
        try:
            return self.ws.close_code is None
        except AttributeError:
            try:
                return self.ws.open
            except AttributeError:
                return False

    async def _ping_loop(self):
        """Gate.io требует периодический ping для поддержания соединения."""
        while self.is_running:
            await asyncio.sleep(15)
            if self._ws_is_connected():
                try:
                    await self.ws.send(json.dumps({
                        "time": int(time.time()),
                        "channel": "futures.ping"
                    }))
                except Exception as e:
                    self.logger.debug(f"Ping error: {e}")
    
    async def _periodic_disk_flush(self):
        while self.is_running:
            await asyncio.sleep(5)
            await self.flush_memory()

    async def flush_memory(self):
        """Сохраняет данные из памяти на диск в разные файлы для trades и orderbook."""
        now_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        flush_tasks = []
        
        for symbol, buffers in list(self.symbol_buffers.items()):
            # Нормализуем имя символа для файла (BTC_USDT -> btc_usdt)
            symbol_file = symbol.lower().replace("_", "")
            
            # Сохраняем trades
            if buffers["trades"]:
                messages = list(buffers["trades"])
                buffers["trades"].clear()
                
                filename = f'{self.exchange}_{symbol_file}_{now_hour_key}_trades.csv.gz'
                filepath = Path('collected_data') / filename
                
                flush_tasks.append(
                    self.loop.run_in_executor(
                        self.thread_pool, 
                        self._write_gz, 
                        filepath, 
                        messages
                    )
                )
            
            # Сохраняем orderbook
            if buffers["orderbook"]:
                messages = list(buffers["orderbook"])
                buffers["orderbook"].clear()
                
                filename = f'{self.exchange}_{symbol_file}_{now_hour_key}_orderbook.csv.gz'
                filepath = Path('collected_data') / filename
                
                flush_tasks.append(
                    self.loop.run_in_executor(
                        self.thread_pool, 
                        self._write_gz, 
                        filepath, 
                        messages
                    )
                )
        
        if flush_tasks:
            await asyncio.gather(*flush_tasks)
            self.logger.info(f"💾 Flushed {len(flush_tasks)} files to disk")

    def _write_gz(self, filepath: Path, messages: list):
        """Записывает данные в gzip файл."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(str(filepath), 'at', compresslevel=3) as f:
            for msg in messages:
                f.write(msg + "\n")

    async def _ws_listener(self):
        """Слушает WebSocket и обрабатывает сообщения."""
        while self.is_running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None  # Используем свой ping
                ) as ws:
                    self.ws = ws
                    self.logger.info("✅ WebSocket connected")
                    await self._subscribe()
                    
                    async for msg in ws:
                        if not self.is_running:
                            break
                        self._parse(msg)
                        
            except Exception as e:
                self.logger.error(f"❌ WebSocket error: {e}")
                await asyncio.sleep(5)

    async def _subscribe(self):
        """Подписывается на стримы для всех активных символов."""
        if not self.active_symbols:
            return
        
        symbols_list = list(self.active_symbols)
        current_time = int(time.time())
        
        # Подписка на trades
        await self.ws.send(json.dumps({
            "time": current_time,
            "channel": "futures.trades",
            "event": "subscribe",
            "payload": symbols_list
        }))
        
        # Подписка на book_ticker
        await self.ws.send(json.dumps({
            "time": current_time,
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": symbols_list
        }))
        
        self.logger.info(f"📡 Subscribed to {len(self.active_symbols)} symbols")


    def _parse(self, raw):
        """Парсит сообщение и добавляет в соответствующий буфер."""
        try:
            data = json.loads(raw)
            
            channel = data.get("channel", "")
            event = data.get("event", "")
            
            # Пропускаем подтверждения подписки, pong и другие служебные сообщения
            if event != "update":
                return
            
            result = data.get("result")
            if not result:
                return
            
            # Trades - result это список сделок
            if channel == "futures.trades":
                trades = result if isinstance(result, list) else [result]
                for trade in trades:
                    sym = trade.get("contract", "")
                    if sym not in self.symbol_buffers:
                        continue
                    
                    # Формат: CreateTimeMs, TradeId, Price, Size (со знаком: + buy, - sell)
                    line = f'{trade.get("create_time_ms", "")},{trade.get("id", "")},{trade.get("price", "")},{trade.get("size", "")}'
                    self.symbol_buffers[sym]["trades"].append(line)
            
            # BookTicker - result это один объект
            elif channel == "futures.book_ticker":
                sym = result.get("s", "")
                if sym not in self.symbol_buffers:
                    return
                
                # Формат: Time, UpdateId, BidPr, BidQty, AskPr, AskQty
                line = f'{result.get("t", int(time.time()*1000))},{result.get("u", "")},{result.get("b", "")},{result.get("B", "")},{result.get("a", "")},{result.get("A", "")}'
                self.symbol_buffers[sym]["orderbook"].append(line)
                
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")

    async def add_symbol(self, symbol: str):
        """
        Добавляет символ для сбора данных.
        Gate.io использует формат: BTC_USDT (uppercase с подчёркиванием)
        """
        s = symbol.upper()
        
        async with self.subscription_lock:
            if s in self.active_symbols:
                self.logger.warning(f"Symbol {s} already active")
                return
            
            self.active_symbols.add(s)
            
            # Создаем буферы для trades и orderbook
            self.symbol_buffers[s] = {
                "trades": deque(),
                "orderbook": deque()
            }
            
            # Подписываемся если WebSocket подключен
            if self._ws_is_connected():
                current_time = int(time.time())
                
                await self.ws.send(json.dumps({
                    "time": current_time,
                    "channel": "futures.trades",
                    "event": "subscribe",
                    "payload": [s]
                }))
                
                await self.ws.send(json.dumps({
                    "time": current_time,
                    "channel": "futures.book_ticker",
                    "event": "subscribe",
                    "payload": [s]
                }))
                
                self.logger.info(f"➕ Added symbol: {s}")

    async def remove_symbol(self, symbol: str):
        """Удаляет символ из сбора данных."""
        s = symbol.upper()
        
        async with self.subscription_lock:
            if s not in self.active_symbols:
                return
            
            # Сбрасываем только буфер этого символа
            if s in self.symbol_buffers:
                await self._flush_symbol(s)
            
            self.active_symbols.discard(s)
            self.symbol_buffers.pop(s, None)
            
            # Отписываемся если WebSocket подключен
            if self._ws_is_connected():
                current_time = int(time.time())
                
                await self.ws.send(json.dumps({
                    "time": current_time,
                    "channel": "futures.trades",
                    "event": "unsubscribe",
                    "payload": [s]
                }))
                
                await self.ws.send(json.dumps({
                    "time": current_time,
                    "channel": "futures.book_ticker",
                    "event": "unsubscribe",
                    "payload": [s]
                }))
                
                self.logger.info(f"➖ Removed symbol: {s}")

    async def _flush_symbol(self, symbol: str):
        """Сбрасывает буфер конкретного символа на диск."""
        if symbol not in self.symbol_buffers:
            return
        
        buffers = self.symbol_buffers[symbol]
        now_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        symbol_file = symbol.lower().replace("_", "")  # BTC_USDT -> btcusdt
        flush_tasks = []
        
        if buffers["trades"]:
            messages = list(buffers["trades"])
            buffers["trades"].clear()
            filepath = Path('collected_data') / f'{self.exchange}_{symbol_file}_{now_hour_key}_trades.csv.gz'
            flush_tasks.append(
                self.loop.run_in_executor(self.thread_pool, self._write_gz, filepath, messages)
            )
        
        if buffers["orderbook"]:
            messages = list(buffers["orderbook"])
            buffers["orderbook"].clear()
            filepath = Path('collected_data') / f'{self.exchange}_{symbol_file}_{now_hour_key}_orderbook.csv.gz'
            flush_tasks.append(
                self.loop.run_in_executor(self.thread_pool, self._write_gz, filepath, messages)
            )
        
        if flush_tasks:
            await asyncio.gather(*flush_tasks)

    def get_buffer_stats(self) -> dict:
        """Получить статистику буферов."""
        stats = {}
        for symbol, buffers in self.symbol_buffers.items():
            stats[symbol] = {
                "trades": len(buffers["trades"]),
                "orderbook": len(buffers["orderbook"])
            }
        return stats


# Пример использования
async def main():
    collector = GateCollector(settle="usdt")
    
    # Добавляем символы ДО запуска
    await collector.add_symbol("BTC_USDT")
    await collector.add_symbol("ETH_USDT")
    
    # Запускаем
    try:
        await collector.run()
    except KeyboardInterrupt:
        await collector.stop()


if __name__ == "__main__":
    asyncio.run(main())
