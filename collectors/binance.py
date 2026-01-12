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
from typing import Set, Dict, Optional
import threading

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)


class BinanceCollector:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/ws"
        self.exchange = "binance"
        self.active_symbols: Set[str] = set()
        self.symbol_buffers: Dict[str, Dict[str, deque]] = {}
        
        self._buffer_lock = threading.Lock()
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        self.is_running = False
        self.logger = logging.getLogger("Binance")
        self.subscription_lock = asyncio.Lock()
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
        self.data_dir = Path('collected_data')
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def run(self):
        self.loop = asyncio.get_running_loop()
        self.is_running = True
        self.logger.info("🚀 Starting Binance Collector")
        
        await asyncio.gather(
            self._ws_listener(),
            self._periodic_flush(),
            return_exceptions=True
        )

    async def stop(self):
        self.logger.info("🛑 Stopping...")
        self.is_running = False
        await self.flush_memory()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        self.thread_pool.shutdown(wait=True)
        self.logger.info("✅ Stopped, all data saved")

    def _ws_connected(self) -> bool:
        if self.ws is None:
            return False
        try:
            return self.ws.close_code is None
        except AttributeError:
            return getattr(self.ws, 'open', False)

    def get_status(self) -> dict:
        """Статус для API endpoint."""
        buffer_stats = {}
        total_in_memory = 0
        
        with self._buffer_lock:
            for symbol, buffers in self.symbol_buffers.items():
                buffer_stats[symbol] = {
                    data_type: len(buffers[data_type])
                    for data_type in buffers
                }
                total_in_memory += sum(len(buffers[t]) for t in buffers)
        
        return {
            "is_running": self.is_running,
            "ws_connected": self._ws_connected(),
            "active_symbols": list(self.active_symbols),
            "symbols_count": len(self.active_symbols),
            "buffers": buffer_stats,
            "total_in_memory": total_in_memory,
        }

    async def _periodic_flush(self):
        while self.is_running:
            await asyncio.sleep(5)
            try:
                await self.flush_memory()
            except Exception as e:
                self.logger.error(f"Flush error: {e}")

    async def flush_memory(self):
        """Атомарный swap буферов и запись на диск."""
        flush_data = []
        
        with self._buffer_lock:
            for symbol, buffers in self.symbol_buffers.items():
                for data_type, buffer in buffers.items():
                    if buffer:
                        old_buffer = buffer
                        buffers[data_type] = deque()
                        flush_data.append((symbol, data_type, old_buffer))
        
        if not flush_data:
            return
        
        tasks = [
            self.loop.run_in_executor(
                self.thread_pool,
                self._write_gz,
                symbol, dtype, list(messages)
            )
            for symbol, dtype, messages in flush_data
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        total = sum(len(m) for _, _, m in flush_data)
        self.logger.info(f"💾 Saved {total} messages")

    def _write_gz(self, symbol: str, data_type: str, messages: list):
        """Записывает в файл, группируя по часам."""
        if not messages:
            return
        
        # Группируем по часам из EventTime
        hour_groups: Dict[str, list] = {}
        for msg in messages:
            try:
                ts_ms = int(msg.split(',')[0])
                hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d_%H")
            except:
                hour = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
            
            if hour not in hour_groups:
                hour_groups[hour] = []
            hour_groups[hour].append(msg)
        
        # Пишем
        for hour, msgs in hour_groups.items():
            path = self.data_dir / f'{self.exchange}_{symbol}_{hour}_{data_type}.csv.gz'
            with gzip.open(str(path), 'at', compresslevel=3) as f:
                for m in msgs:
                    f.write(m + "\n")

    async def _ws_listener(self):
        reconnect_delay = 1
        
        while self.is_running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 1
                    self.logger.info("✅ Connected")
                    
                    await self._subscribe_all()
                    
                    async for msg in ws:
                        if not self.is_running:
                            break
                        self._parse(msg)
                        
            except websockets.ConnectionClosed as e:
                self.logger.warning(f"🔌 Disconnected: {e.code}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"WS error: {e}")
            
            if self.is_running:
                self.logger.info(f"🔄 Reconnect in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    async def _subscribe_all(self):
        if not self.active_symbols or not self._ws_connected():
            return
        
        streams = []
        for s in self.active_symbols:
            # Подписываемся на trades, bookTicker и depth20@100ms
            streams.extend([
                f"{s}@bookTicker", 
                f"{s}@aggTrade",
                f"{s}@depth20@100ms"  # depth20 с максимальной частотой
            ])
        
        await self.ws.send(json.dumps({
            'method': 'SUBSCRIBE',
            'params': streams,
            'id': 1
        }))
        self.logger.info(f"📡 Subscribed: {len(self.active_symbols)} symbols x 3 streams")

    def _parse(self, raw: str):
        try:
            data = json.loads(raw)
            
            # Системные ответы — пропускаем
            if "result" in data:
                return
            
            sym = data.get("s", "").lower()
            if not sym or sym not in self.symbol_buffers:
                return
            
            event_type = data.get("e")
            
            # aggTrade
            if event_type == "aggTrade":
                mk = "1" if data.get("m") else "0"
                line = f'{data["E"]},{data["a"]},{data["p"]},{data["q"]},{data["T"]},{mk}'
                self.symbol_buffers[sym]["trades"].append(line)
            
            # bookTicker (BBO)
            elif event_type == "bookTicker":
                # Правильное условие для bookTicker
                et = data.get("E", int(time.time() * 1000))
                line = f'{et},{data["u"]},{data["b"]},{data["B"]},{data["a"]},{data["A"]}'
                self.symbol_buffers[sym]["orderbook"].append(line)
            
            # depthUpdate (Partial Book Depth)
            elif event_type == "depthUpdate":
                bids = "|".join(f"{p}:{q}" for p, q in data.get("b", []))
                asks = "|".join(f"{p}:{q}" for p, q in data.get("a", []))
                line = f'{data["E"]},{data["T"]},{data["u"]},{bids},{asks}'
                self.symbol_buffers[sym]["depth"].append(line)
                
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")
    async def add_symbol(self, symbol: str):
        s = symbol.lower()
        
        async with self.subscription_lock:
            if s in self.active_symbols:
                return
            
            self.active_symbols.add(s)
            
            with self._buffer_lock:
                self.symbol_buffers[s] = {
                    "trades": deque(),       # aggTrades
                    "orderbook": deque(),    # bookTicker (BBO)
                    "depth": deque()         # depth20 (стакан)
                }
            
            # Подписываемся на все потоки
            if self._ws_connected():
                await self.ws.send(json.dumps({
                    'method': 'SUBSCRIBE',
                    'params': [
                        f"{s}@bookTicker",
                        f"{s}@aggTrade",
                        f"{s}@depth20@100ms"  # 20 уровней, 100ms
                    ],
                    'id': 1
                }))
            
            self.logger.info(f"➕ Added: {s}")

    async def remove_symbol(self, symbol: str):
        s = symbol.lower()
        
        async with self.subscription_lock:
            if s not in self.active_symbols:
                return
            
            if self._ws_connected():
                await self.ws.send(json.dumps({
                    'method': 'UNSUBSCRIBE',
                    'params': [
                        f"{s}@bookTicker",
                        f"{s}@aggTrade",
                        f"{s}@depth20@100ms"
                    ],
                    'id': 1
                }))
            
            await self.flush_memory()
            
            self.active_symbols.discard(s)
            with self._buffer_lock:
                self.symbol_buffers.pop(s, None)
            
            self.logger.info(f"➖ Removed: {s}")


# Код для чтения depth20 данных
def parse_depth(line: str) -> dict:
    """Парсит строку depth20 в словарь с данными."""
    parts = line.strip().split(',')
    event_time = int(parts[0])
    trans_time = int(parts[1])
    update_id = int(parts[2])
    
    # Парсим bids (цена:количество)
    bids = []
    if parts[3]:
        for level in parts[3].split('|'):
            price, qty = level.split(':')
            bids.append([float(price), float(qty)])
    
    # Парсим asks (цена:количество)
    asks = []
    if parts[4]:
        for level in parts[4].split('|'):
            price, qty = level.split(':')
            asks.append([float(price), float(qty)])
    
    return {
        'event_time': event_time,
        'trans_time': trans_time,
        'update_id': update_id,
        'bids': bids,
        'asks': asks
    }


# async def main():
#     collector = BinanceCollector()
#     await collector.add_symbol("btcusdt")
#     await collector.add_symbol("ethusdt")
    
#     try:
#         await collector.run()
#     except KeyboardInterrupt:
#         pass
#     finally:
#         await collector.stop()


# if __name__ == "__main__":
#     asyncio.run(main())