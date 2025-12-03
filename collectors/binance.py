import asyncio
from collections import deque
import websockets
import json
import gzip
import time
import logging
from datetime import datetime, timezone  # Исправляем deprecation warning
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Set, Deque, Dict, Optional

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)


class BinanceCollector:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/ws"
        self.active_symbols: Set[str] = set()
        self.symbol_buffers: Dict[str, Deque[str]] = {}
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        self.flush_interval = 5
        self.is_running = False
        self.logger = logging.getLogger(__name__)
        self.subscription_lock = asyncio.Lock()
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
        self.message_count = 0
        self.parsed_count = 0

    def _ws_is_connected(self) -> bool:
        """Проверка что WebSocket подключён."""
        if self.ws is None:
            return False
        # Для websockets 12+ проверяем state или close_code
        try:
            # Вариант 1: проверяем close_code (None = соединение активно)
            return self.ws.close_code is None
        except AttributeError:
            # Вариант 2: старые версии websockets
            try:
                return self.ws.open
            except AttributeError:
                return False

    async def run(self):
        self.is_running = True
        self.loop = asyncio.get_running_loop()
        self.logger.info("Starting Binance collector...")
        
        await asyncio.gather(
            self._ws_listener(),
            self._periodic_flusher(),
            self._stats_printer()
        )

    async def _stats_printer(self):
        while self.is_running:
            await asyncio.sleep(10)
            self.logger.info(
                f"📈 Stats: symbols={list(self.active_symbols)}, "
                f"messages={self.message_count}, parsed={self.parsed_count}, "
                f"buffers={[(s, len(b)) for s, b in self.symbol_buffers.items()]}"
            )

    async def stop(self):
        self.is_running = False
        await self._flush_all_buffers()
        await self._close_ws_connection()
        self.thread_pool.shutdown(wait=True)
        self.logger.info("Stopped Binance collector.")

    async def _ws_listener(self):
        while self.is_running:
            try:
                self.logger.info(f"🔌 Connecting to {self.ws_url}...")
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10
                ) as ws:
                    self.ws = ws
                    self.logger.info(f"✅ WebSocket connected! close_code={ws.close_code}")
                    await self._initial_subscribe()
                    
                    async for message in ws:
                        if not self.is_running:
                            break
                        self.message_count += 1
                        if isinstance(message, bytes):
                            message = message.decode('utf-8')
                        self._parse_message(message)
                        
            except websockets.ConnectionClosed as e:
                self.logger.warning(f"Connection closed: {e}. Reconnecting...")
            except Exception as e:
                self.logger.warning(f"WebSocket error: {e}. Reconnecting...")
            finally:
                self.ws = None
            
            if self.is_running:
                await asyncio.sleep(5)

    async def _initial_subscribe(self):
        if not self.active_symbols:
            self.logger.info("No symbols to subscribe to initially")
            return
        
        streams = []
        for sym in self.active_symbols:
            streams.append(f"{sym}@bookTicker")
            streams.append(f"{sym}@aggTrade")
        
        msg = {
            'method': 'SUBSCRIBE',
            'params': streams,
            'id': int(time.time() * 1000)
        }
        await self.ws.send(json.dumps(msg))
        self.logger.info(f"📡 Subscribed to {len(streams)} streams: {streams}")

    async def _close_ws_connection(self):
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def _periodic_flusher(self):
        next_flush = time.time() + self.flush_interval
        while self.is_running:
            now = time.time()
            if now >= next_flush:
                next_flush = now + self.flush_interval
                await self._flush_all_buffers()
            await asyncio.sleep(1)

    async def _flush_all_buffers(self):
        now = datetime.now(timezone.utc)  # Исправлено: без deprecation warning
        hour_key = now.strftime("%Y%m%d_%H")
        
        flush_tasks = []
        for symbol, buffer in list(self.symbol_buffers.items()):
            if buffer:
                messages = list(buffer)
                buffer.clear()
                self.logger.info(f"💾 Flushing {len(messages)} messages for {symbol}")
                flush_tasks.append(
                    self._flush_to_file(symbol, hour_key, messages)
                )
        
        if flush_tasks:
            await asyncio.gather(*flush_tasks)

    async def _flush_to_file(self, symbol: str, hour_key: str, messages: list):
        filename = f'{symbol}_{hour_key}.csv.gz'
        filepath = Path('collected_data') / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        await self.loop.run_in_executor(
            self.thread_pool,
            self._write_gz_file,
            filepath,
            messages
        )
        self.logger.info(f"✅ Wrote {len(messages)} messages to {filepath}")

    def _write_gz_file(self, filepath: Path, messages: list):
        mode = 'at' if filepath.exists() else 'wt'
        with gzip.open(str(filepath), mode, compresslevel=3) as f:
            for msg in messages:
                f.write(msg + "\n")

    def _parse_message(self, raw: str):
        try:
            data = json.loads(raw)
            
            if "result" in data:
                self.logger.info(f"📨 Binance response: {data}")
                return
            
            if "s" not in data:
                return
            
            sym = data["s"].lower()
            
            if sym not in self.active_symbols:
                return
            
            if sym not in self.symbol_buffers:
                self.symbol_buffers[sym] = deque()

            csv_line = None
            event_type = data.get("e")
            
            if event_type == "aggTrade":
                is_maker = "1" if data.get("m") else "0"
                csv_line = (
                    f'T,{data["E"]},{data["a"]},{data["p"]},'
                    f'{data["q"]},{data["T"]},{is_maker}'
                )
            elif "u" in data:
                csv_line = (
                    f'B,{data.get("E", 0)},{data["u"]},{data["b"]},'
                    f'{data["B"]},{data["a"]},{data["A"]},{data.get("T", 0)}'
                )
            
            if csv_line:
                self.symbol_buffers[sym].append(csv_line)
                self.parsed_count += 1
                
        except Exception as e:
            self.logger.error(f"Parse error: {e}")

    async def add_symbol(self, symbol: str):
        symbol = symbol.lower()
        
        async with self.subscription_lock:
            if symbol in self.active_symbols:
                self.logger.info(f"Symbol {symbol} already active")
                return
            
            self.active_symbols.add(symbol)
            self.symbol_buffers[symbol] = deque()
            
            # ✅ Используем правильную проверку
            is_connected = self._ws_is_connected()
            self.logger.info(f"🔍 WS connected: {is_connected}")
            
            if is_connected:
                try:
                    msg = {
                        'method': 'SUBSCRIBE',
                        'params': [f"{symbol}@bookTicker", f"{symbol}@aggTrade"],
                        'id': int(time.time() * 1000)
                    }
                    await self.ws.send(json.dumps(msg))
                    self.logger.info(f"✅ Sent subscription: {msg['params']}")
                except Exception as e:
                    self.logger.error(f"❌ Subscription failed: {e}")
            else:
                self.logger.warning(f"⚠️ WS not ready! {symbol} queued for reconnect")

    async def remove_symbol(self, symbol: str):
        symbol = symbol.lower()
        
        async with self.subscription_lock:
            if symbol not in self.active_symbols:
                return
            
            self.active_symbols.discard(symbol)
            
            if self._ws_is_connected():
                try:
                    msg = {
                        'method': 'UNSUBSCRIBE',
                        'params': [f"{symbol}@bookTicker", f"{symbol}@aggTrade"],
                        'id': int(time.time() * 1000)
                    }
                    await self.ws.send(json.dumps(msg))
                except Exception as e:
                    self.logger.error(f"Unsubscribe failed: {e}")
            
            if symbol in self.symbol_buffers and self.symbol_buffers[symbol]:
                hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
                messages = list(self.symbol_buffers[symbol])
                await self._flush_to_file(symbol, hour_key, messages)
            
            self.symbol_buffers.pop(symbol, None)
            self.logger.info(f"Removed {symbol}")


async def main():
    collector = BinanceCollector()
    
    # Символы ДО запуска
    await collector.add_symbol('BTCUSDT')
    await collector.add_symbol('ETHUSDT')
    
    collector_task = asyncio.create_task(collector.run())
    
    # Ждём пока WS подключится
    await asyncio.sleep(3)
    
    # Динамическое добавление
    print("\n" + "="*50)
    print("Adding SOLUSDT dynamically...")
    print("="*50 + "\n")
    await collector.add_symbol('SOLUSDT')
    
    await asyncio.sleep(30)
    
    await collector.stop()
    collector_task.cancel()
    
    try:
        await collector_task
    except asyncio.CancelledError:
        pass
    
    print("\n✅ Done! Check collected_data/ folder")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")