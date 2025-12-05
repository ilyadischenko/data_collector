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

class BinanceCollector:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/ws"
        self.exchange = "binance"
        self.active_symbols: Set[str] = set()
        
        # Разделяем буферы для trades и orderbook
        self.symbol_buffers: Dict[str, Dict[str, deque]] = {}
        
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        self.is_running = False
        self.logger = logging.getLogger("Binance")
        self.subscription_lock = asyncio.Lock()
        self.ws = None
        self.loop = None

    async def run(self):
        self.loop = asyncio.get_running_loop()
        self.is_running = True
        self.logger.info("🚀 Started Binance Collector")
        await asyncio.gather(self._ws_listener(), self._periodic_disk_flush())

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
    
    async def _periodic_disk_flush(self):
        while self.is_running:
            await asyncio.sleep(5)
            await self.flush_memory()

    async def flush_memory(self):
        """Сохраняет данные из памяти на диск в разные файлы для trades и orderbook."""
        now_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        flush_tasks = []
        
        for symbol, buffers in list(self.symbol_buffers.items()):
            # Сохраняем trades
            if buffers["trades"]:
                messages = list(buffers["trades"])
                buffers["trades"].clear()
                
                filename = f'{self.exchange}_{symbol}_{now_hour_key}_trades.csv.gz'
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
                
                filename = f'{self.exchange}_{symbol}_{now_hour_key}_orderbook.csv.gz'
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
                async with websockets.connect(self.ws_url) as ws:
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
        
        streams = []
        for s in self.active_symbols:
            streams.extend([f"{s}@bookTicker", f"{s}@aggTrade"])
        
        if streams:
            await self.ws.send(json.dumps({
                'method': 'SUBSCRIBE',
                'params': streams,
                'id': 1
            }))
            self.logger.info(f"📡 Subscribed to {len(self.active_symbols)} symbols")

    def _parse(self, raw):
        """Парсит сообщение и добавляет в соответствующий буфер."""
        try:
            data = json.loads(raw)
            sym = data.get("s", "").lower()
            
            if not sym or sym not in self.symbol_buffers:
                return
            
            # Trades
            if data.get("e") == "aggTrade":
                # EventTime, TradeId, Price, Qty, TradeTime, IsMaker
                mk = "1" if data.get("m") else "0"
                line = f'{data["E"]},{data["a"]},{data["p"]},{data["q"]},{data["T"]},{mk}'
                self.symbol_buffers[sym]["trades"].append(line)
            
            # BookTicker
            elif "u" in data:
                # EventTime, UpdateId, BidPr, BidQty, AskPr, AskQty
                line = f'{data.get("E", int(time.time()*1000))},{data["u"]},{data["b"]},{data["B"]},{data["a"]},{data["A"]}'
                self.symbol_buffers[sym]["orderbook"].append(line)
                
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")

    async def add_symbol(self, symbol: str):
        """Добавляет символ для сбора данных."""
        s = symbol.lower()
        
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
                await self.ws.send(json.dumps({
                    'method': 'SUBSCRIBE',
                    'params': [f"{s}@bookTicker", f"{s}@aggTrade"],
                    'id': 1
                }))
                self.logger.info(f"➕ Added symbol: {s}")

    async def remove_symbol(self, symbol: str):
        """Удаляет символ из сбора данных."""
        s = symbol.lower()
        
        async with self.subscription_lock:
            if s not in self.active_symbols:
                return
            
            # Сбрасываем данные перед удалением
            if s in self.symbol_buffers:
                await self.flush_memory()
            
            self.active_symbols.discard(s)
            self.symbol_buffers.pop(s, None)
            
            # Отписываемся если WebSocket подключен
            if self._ws_is_connected():
                await self.ws.send(json.dumps({
                    'method': 'UNSUBSCRIBE',
                    'params': [f"{s}@bookTicker", f"{s}@aggTrade"],
                    'id': 1
                }))
                self.logger.info(f"➖ Removed symbol: {s}")

    def get_buffer_stats(self) -> dict:
        """Получить статистику буферов."""
        stats = {}
        for symbol, buffers in self.symbol_buffers.items():
            stats[symbol] = {
                "trades": len(buffers["trades"]),
                "orderbook": len(buffers["orderbook"])
            }
        return stats