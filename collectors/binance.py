import asyncio
from collections import deque
import websockets
import json
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Set, Dict, Optional, List
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class BinanceConnection:
    """Одно WebSocket соединение к Binance."""
    
    def __init__(self, conn_id: str, parent: 'BinanceCollector'):
        self.conn_id = conn_id
        self.parent = parent
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False
        self.logger = logging.getLogger(f"Binance.{conn_id}")
        
        # Простые буферы: {symbol: {data_type: deque}}
        # БЕЗ привязки к часам - просто накапливаем ВСЁ
        self.buffers: Dict[str, Dict[str, deque]] = {}
        self._buffer_lock = asyncio.Lock()
    
    async def _ensure_symbol_buffer(self, symbol: str):
        """Создает буферы для символа если их нет."""
        async with self._buffer_lock:
            if symbol not in self.buffers:
                self.buffers[symbol] = {
                    "trades": deque(),
                    "bookticker": deque(),
                    "depth": deque()
                }
    
    async def run(self):
        """Основной цикл соединения с автореконнектом."""
        self.is_running = True
        reconnect_delay = 1
        
        while self.is_running:
            try:
                async with websockets.connect(
                    self.parent.ws_url,
                    ping_interval=None,
                    max_size=10_000_000,
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 1
                    self.logger.info(f"✅ Binance {self.conn_id} Connected")
                    
                    await self._subscribe_all()
                    
                    async for msg in ws:
                        if not self.is_running:
                            break
                        await self._parse(msg)
                        
            except websockets.ConnectionClosed as e:
                self.logger.error(
                    f"🔌 Binance {self.conn_id} Disconnected: {e.code} - {e.reason or 'No reason'}"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Binance {self.conn_id} WS error: {e}", exc_info=True)
            
            if self.is_running:
                self.logger.info(f"🔄 Reconnect in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
    
    async def _subscribe_all(self):
        """Подписывается на все активные символы."""
        if not self.parent.active_symbols or not self._ws_connected():
            return
        
        streams = []
        for s in self.parent.active_symbols:
            streams.extend([
                f"{s}@bookTicker",
                f"{s}@trade",
                f"{s}@depth20@100ms"
            ])
        
        await self.ws.send(json.dumps({
            'method': 'SUBSCRIBE',
            'params': streams,
            'id': 1
        }))
        self.logger.info(f"📡 Subscribed: {len(self.parent.active_symbols)} symbols")
    
    def _ws_connected(self) -> bool:
        if self.ws is None:
            return False
        try:
            return self.ws.close_code is None
        except AttributeError:
            return getattr(self.ws, 'open', False)
    
    async def _parse(self, raw: str):
        """Парсит сообщения от биржи и складывает в буферы."""
        try:
            data = json.loads(raw)
            
            if "result" in data:
                return
            
            sym = data.get("s", "").lower()
            if not sym:
                return
            
            event_type = data.get("e")
            
            # Создаем буфер для символа если нужно
            await self._ensure_symbol_buffer(sym)
            
            async with self._buffer_lock:
                if sym not in self.buffers:
                    return
                buffers = self.buffers[sym]
            
            if event_type == "trade":
                trade = {
                    'timestamp_ms': data["E"],
                    'connection_id': self.conn_id,
                    'trade_id': data["t"],
                    'price': float(data["p"]),
                    'qty': float(data["q"]),
                    'trade_time_ms': data["T"],
                    'is_buyer_maker': data["m"],
                }
                buffers["trades"].append(trade)
            
            elif event_type == "bookTicker":
                bbo = {
                    'timestamp_ms': data.get("E", int(time.time_ns() // 1_000_000)),
                    'connection_id': self.conn_id,
                    'update_id': data["u"],
                    'best_bid_price': float(data["b"]),
                    'best_bid_qty': float(data["B"]),
                    'best_ask_price': float(data["a"]),
                    'best_ask_qty': float(data["A"])
                }
                buffers["bookticker"].append(bbo)
            
            elif event_type == "depthUpdate":
                bids = [[float(p), float(q)] for p, q in data.get("b", [])]
                asks = [[float(p), float(q)] for p, q in data.get("a", [])]
                
                depth = {
                    'timestamp_ms': data["E"],
                    'connection_id': self.conn_id,
                    'update_id': data["u"],
                    'bids': bids,
                    'asks': asks
                }
                buffers["depth"].append(depth)
                    
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")
    
    async def get_snapshot_and_clear(self) -> Dict[str, Dict[str, List[dict]]]:
        """
        Создает снимок ВСЕХ буферов и очищает их.
        Возвращает: {symbol: {data_type: [data]}}
        """
        result = {}
        
        async with self._buffer_lock:
            for symbol, buffers in self.buffers.items():
                result[symbol] = {}
                for data_type, buffer in buffers.items():
                    if buffer:
                        result[symbol][data_type] = list(buffer)
                        buffer.clear()
        
        return result
    
    async def stop(self):
        """Останавливает соединение."""
        self.is_running = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
    
    def get_buffer_stats(self) -> dict:
        """Возвращает статистику буферов."""
        stats = {}
        for symbol, buffers in self.buffers.items():
            stats[symbol] = {
                dt: len(buf) for dt, buf in buffers.items()
            }
        return stats


class BinanceCollector:
    """Коллектор с множественными соединениями."""
    
    def __init__(self, num_connections: int = 2, max_workers: int = 8):
        self.ws_url = "wss://fstream.binance.com/ws"
        self.exchange = "binance"
        self.num_connections = num_connections
        self.connections: List[BinanceConnection] = []
        
        self.active_symbols: Set[str] = set()
        self.symbol_lock = asyncio.Lock()
        
        self.is_running = False
        self.logger = logging.getLogger("BinanceCollector")
        
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        
        self.data_dir = Path('collected_data') / self.exchange
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self._write_semaphore = asyncio.Semaphore(max_workers)
        
        for i in range(num_connections):
            conn = BinanceConnection(f"conn_{i+1}", self)
            self.connections.append(conn)
    
    async def run(self):
        """Запускает все соединения и фоновые задачи."""
        self.is_running = True
        self.logger.info(f"🚀 Starting {self.num_connections} connections")
        
        tasks = [
            *[conn.run() for conn in self.connections],
            self._writer_task()
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop(self):
        """Останавливает все соединения."""
        self.logger.info("🛑 Stopping...")
        self.is_running = False
        
        await asyncio.gather(*[conn.stop() for conn in self.connections])
        
        # Финальный сброс всех буферов
        await self._flush_all()
        
        self.thread_pool.shutdown(wait=True)
        self.logger.info("✅ Stopped")
    
    async def _writer_task(self):
        """Задача записи: каждые 5 сек берет буферы и пишет в файлы."""
        while self.is_running:
            await asyncio.sleep(5)
            
            try:
                await self._flush_all()
            except Exception as e:
                self.logger.error(f"Writer task error: {e}", exc_info=True)
    
    async def _flush_all(self):
        """Берет snapshot всех буферов и распределяет по файлам."""
        flush_tasks = []
        
        for conn in self.connections:
            # Получаем снимок и очищаем буферы
            snapshot = await conn.get_snapshot_and_clear()
            
            # Распределяем данные по файлам согласно timestamp
            for symbol, data_types in snapshot.items():
                for data_type, data_list in data_types.items():
                    if data_list:
                        # Группируем по часам
                        hourly_data = self._group_by_hour(data_list)
                        
                        # Пишем каждый час в отдельный файл
                        for hour_key, hour_data in hourly_data.items():
                            task = self._write_with_semaphore(
                                symbol, data_type, conn.conn_id, hour_data, hour_key
                            )
                            flush_tasks.append(task)
        
        if flush_tasks:
            results = await asyncio.gather(*flush_tasks, return_exceptions=True)
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                self.logger.error(f"Flush errors: {len(errors)}/{len(flush_tasks)}")
    
    def _group_by_hour(self, data_list: List[dict]) -> Dict[str, List[dict]]:
        """
        Группирует данные по часам на основе timestamp_ms.
        Возвращает: {hour_key: [data]}
        """
        hourly_data = {}
        
        for item in data_list:
            timestamp_ms = item.get('timestamp_ms')
            if not timestamp_ms:
                continue
            
            # Определяем hour_key из timestamp
            dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            hour_key = dt.strftime("%Y%m%d_%H")
            
            if hour_key not in hourly_data:
                hourly_data[hour_key] = []
            
            hourly_data[hour_key].append(item)
        
        return hourly_data
    
    async def _write_with_semaphore(self, symbol: str, data_type: str, 
                                    conn_id: str, data: List[dict], hour_key: str):
        """Запись с ограничением параллелизма."""
        async with self._write_semaphore:
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self._write_parquet,
                symbol, data_type, conn_id, data, hour_key
            )
    
    def _write_parquet(self, symbol: str, data_type: str, conn_id: str, 
                      data: List[dict], hour_key: str):
        """Атомарная запись с .tmp файлом."""
        if not data:
            return
        
        symbol_dir = self.data_dir / symbol / hour_key
        symbol_dir.mkdir(parents=True, exist_ok=True)
        
        final_filepath = symbol_dir / f"{conn_id}_{data_type}.parquet"
        temp_filepath = symbol_dir / f"{conn_id}_{data_type}.parquet.tmp"
        
        schema = self._get_schema(data_type)
        if not schema:
            return
        
        try:
            new_table = pa.Table.from_pylist(data, schema=schema)
            
            if final_filepath.exists():
                existing = pq.read_table(final_filepath)
                combined = pa.concat_tables([existing, new_table])
                
                pq.write_table(
                    combined,
                    temp_filepath,
                    compression='zstd',
                    compression_level=3
                )
            else:
                pq.write_table(
                    new_table,
                    temp_filepath,
                    compression='zstd',
                    compression_level=3
                )
            
            temp_filepath.replace(final_filepath)
            
        except Exception as e:
            self.logger.error(f"Parquet write error for {final_filepath}: {e}")
            if temp_filepath.exists():
                temp_filepath.unlink()
    
    def _get_schema(self, data_type: str) -> Optional[pa.Schema]:
        """Возвращает схему по типу данных."""
        schemas = {
            'depth': self._get_depth_schema(),
            'trades': self._get_trades_schema(),
            'bookticker': self._get_bookticker_schema()
        }
        return schemas.get(data_type)
    
    @staticmethod
    def _get_depth_schema() -> pa.Schema:
        return pa.schema([
            ('timestamp_ms', pa.int64()),
            ('connection_id', pa.string()),
            ('update_id', pa.int64()),
            ('bids', pa.list_(pa.list_(pa.float64(), 2))),
            ('asks', pa.list_(pa.list_(pa.float64(), 2)))
        ])
    
    @staticmethod
    def _get_trades_schema() -> pa.Schema:
        return pa.schema([
            ('timestamp_ms', pa.int64()),
            ('connection_id', pa.string()),
            ('trade_id', pa.int64()),
            ('price', pa.float64()),
            ('qty', pa.float64()),
            ('trade_time_ms', pa.int64()),
            ('is_buyer_maker', pa.bool_()),
        ])
    
    @staticmethod
    def _get_bookticker_schema() -> pa.Schema:
        return pa.schema([
            ('timestamp_ms', pa.int64()),
            ('connection_id', pa.string()),
            ('update_id', pa.int64()),
            ('best_bid_price', pa.float64()),
            ('best_bid_qty', pa.float64()),
            ('best_ask_price', pa.float64()),
            ('best_ask_qty', pa.float64())
        ])
    
    async def add_symbol(self, symbol: str):
        """Добавляет символ к подписке."""
        s = symbol.lower()
        
        async with self.symbol_lock:
            if s in self.active_symbols:
                return
            
            self.active_symbols.add(s)
            
            # Создаем буферы для символа в каждом соединении
            for conn in self.connections:
                await conn._ensure_symbol_buffer(s)
            
            for conn in self.connections:
                if conn._ws_connected():
                    await conn.ws.send(json.dumps({
                        'method': 'SUBSCRIBE',
                        'params': [
                            f"{s}@bookTicker",
                            f"{s}@trade",
                            f"{s}@depth20@100ms"
                        ],
                        'id': 1
                    }))
            
            self.logger.info(f"➕ Added: {s}")
    
    async def remove_symbol(self, symbol: str):
        """Удаляет символ из подписки."""
        s = symbol.lower()
        
        async with self.symbol_lock:
            if s not in self.active_symbols:
                return
            
            for conn in self.connections:
                if conn._ws_connected():
                    await conn.ws.send(json.dumps({
                        'method': 'UNSUBSCRIBE',
                        'params': [
                            f"{s}@bookTicker",
                            f"{s}@trade",
                            f"{s}@depth20@100ms"
                        ],
                        'id': 1
                    }))
            
            # Сбрасываем текущие буферы
            await self._flush_all()
            
            self.active_symbols.discard(s)
            
            self.logger.info(f"➖ Removed: {s}")
    
    async def get_status(self) -> dict:
        """Возвращает статус коллектора."""
        connections_status = []
        for conn in self.connections:
            connections_status.append({
                'id': conn.conn_id,
                'connected': conn._ws_connected(),
                'buffers': conn.get_buffer_stats()
            })
        
        return {
            'is_running': self.is_running,
            'active_symbols': list(self.active_symbols),
            'connections': connections_status
        }


async def main():
    collector = BinanceCollector(num_connections=2, max_workers=8)
    
    await collector.add_symbol("btcusdt")
    await collector.add_symbol("ethusdt")
    
    try:
        await collector.run()
    except KeyboardInterrupt:
        pass
    finally:
        await collector.stop()


if __name__ == "__main__":
    asyncio.run(main())