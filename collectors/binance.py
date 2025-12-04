import asyncio
from collections import deque
import threading
import websockets
import json
import gzip
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Set, Deque, Dict, Optional

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

class BinanceCollector:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/ws"
        self.exchange = "binance"
        self.active_symbols: Set[str] = set()
        self.symbol_buffers: Dict[str, Deque[str]] = {}
        
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        self.disk_flush_interval = 5
        
        self.is_running = False
        self.logger = logging.getLogger("BinanceCollector")
        self.subscription_lock = asyncio.Lock()
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
        self.message_count = 0

    def _ws_is_connected(self) -> bool:
        if self.ws is None: return False
        try: return self.ws.close_code is None
        except AttributeError:
            try: return self.ws.open
            except AttributeError: return False

    async def run(self):
        # Получаем loop ТОГО потока, в котором запустился этот метод
        self.loop = asyncio.get_running_loop()
        self.is_running = True
        self.logger.info(f"Starting Collector in thread: {threading.current_thread().name}")
        
        await asyncio.gather(
            self._ws_listener(),
            self._periodic_disk_flush()
        )

    async def stop(self):
        self.is_running = False
        await self.flush_memory() # Финальный сброс
        await self._close_ws_connection()
        self.thread_pool.shutdown(wait=True)
        self.logger.info("Collector stopped.")

    async def _periodic_disk_flush(self):
        while self.is_running:
            await asyncio.sleep(self.disk_flush_interval)
            await self.flush_memory()

    async def flush_memory(self):
        """Сбрасывает текущий буфер RAM на диск."""
        now_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        
        flush_tasks = []
        for symbol, buffer in list(self.symbol_buffers.items()):
            if buffer:
                messages = list(buffer)
                buffer.clear()
                
                # Имя файла: binance_btcusdt_20250115_14.csv.gz
                filename = f'{self.exchange}_{symbol}_{now_hour_key}.csv.gz'
                filepath = Path('collected_data') / filename
                
                flush_tasks.append(
                    self.loop.run_in_executor(
                        self.thread_pool,
                        self._write_gz_file,
                        filepath,
                        messages
                    )
                )
        
        if flush_tasks:
            await asyncio.gather(*flush_tasks)

    def _write_gz_file(self, filepath: Path, messages: list):
        filepath.parent.mkdir(parents=True, exist_ok=True)
        # 'at' = append text. Дописываем в конец файла.
        with gzip.open(str(filepath), 'at', compresslevel=3) as f:
            for msg in messages:
                f.write(msg + "\n")

    # === WS Logic ===
    async def _ws_listener(self):
        while self.is_running:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws = ws
                    await self._initial_subscribe()
                    async for message in ws:
                        if not self.is_running: break
                        self.message_count += 1
                        if isinstance(message, bytes): message = message.decode('utf-8')
                        self._parse_message(message)
            except Exception as e:
                self.logger.error(f"WS Error: {e}")
                await asyncio.sleep(5)
            finally:
                self.ws = None

    async def _initial_subscribe(self):
        if not self.active_symbols: return
        streams = []
        for sym in self.active_symbols:
            streams.extend([f"{sym}@bookTicker", f"{sym}@aggTrade"])
        msg = {'method': 'SUBSCRIBE', 'params': streams, 'id': int(time.time()*1000)}
        await self.ws.send(json.dumps(msg))

    async def _close_ws_connection(self):
        if self.ws: await self.ws.close()

    def _parse_message(self, raw: str):
        try:
            data = json.loads(raw)
            if "s" not in data: return
            sym = data["s"].lower()
            if sym not in self.active_symbols: return
            if sym not in self.symbol_buffers: self.symbol_buffers[sym] = deque()

            csv_line = None
            if data.get("e") == "aggTrade":
                csv_line = f'T,{data["E"]},{data["a"]},{data["p"]},{data["q"]},{data["T"]},{1 if data.get("m") else 0}'
            elif "u" in data:
                csv_line = f'B,{data.get("E",0)},{data["u"]},{data["b"]},{data["B"]},{data["a"]},{data["A"]},{data.get("T",0)}'
            
            if csv_line: self.symbol_buffers[sym].append(csv_line)
        except Exception: pass

    async def add_symbol(self, symbol: str):
        symbol = symbol.lower()
        async with self.subscription_lock:
            if symbol in self.active_symbols: return
            self.active_symbols.add(symbol)
            self.symbol_buffers[symbol] = deque()
            if self._ws_is_connected():
                await self.ws.send(json.dumps({
                    'method': 'SUBSCRIBE',
                    'params': [f"{symbol}@bookTicker", f"{symbol}@aggTrade"],
                    'id': int(time.time()*1000)
                }))

    async def remove_symbol(self, symbol: str):
        symbol = symbol.lower()
        async with self.subscription_lock:
            if symbol not in self.active_symbols: return
            self.active_symbols.discard(symbol)
            self.symbol_buffers.pop(symbol, None)