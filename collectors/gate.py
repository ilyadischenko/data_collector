import asyncio
from collections import deque
import websockets
import json
import time
import gc
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Set, Dict, Optional, List
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class GateConnection:
    """Одно WebSocket соединение к Gate.io Futures."""
    
    def __init__(self, conn_id: str, parent: 'GateCollector'):
        self.conn_id = conn_id
        self.parent = parent
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False
        self.logger = logging.getLogger(f"Gate.{conn_id}")
        
        # Уменьшенные буферы для снижения памяти
        self.buffers: Dict[str, Dict[str, deque]] = {}
        self._buffer_lock = asyncio.Lock()
        
        # Пороги для мониторинга
        self.MAX_BUFFER_SIZE = 50_000
        self.EMERGENCY_THRESHOLD = 250_000
        
        # Для пинга на уровне приложения
        self._ping_task: Optional[asyncio.Task] = None
        self._last_pong_time = time.time()
    
    def _normalize_symbol(self, gate_symbol: str) -> str:
        """Gate.io: BTC_USDT -> btcusdt"""
        return gate_symbol.replace('_', '').lower()
    
    def _to_gate_symbol(self, normalized_symbol: str) -> str:
        """btcusdt -> BTC_USDT"""
        if normalized_symbol.endswith('usdt'):
            base = normalized_symbol[:-4].upper()
            return f"{base}_USDT"
        return normalized_symbol.upper()
    
    async def _ensure_symbol_buffer(self, symbol: str):
        """Создает буферы для символа если их нет."""
        async with self._buffer_lock:
            if symbol not in self.buffers:
                self.buffers[symbol] = {
                    # УМЕНЬШЕНЫ для экономии памяти
                    "trades": deque(maxlen=50_000),
                    "bookticker": deque(maxlen=25_000),
                    "depth": deque(maxlen=25_000)
                }
    
    async def _app_ping_loop(self):
        """Отправляет пинг на уровне приложения каждые 30 секунд."""
        while self.is_running and self._ws_connected():
            try:
                await asyncio.sleep(30)
                
                if not self._ws_connected():
                    break
                
                ping_msg = {
                    "time": int(time.time()),
                    "channel": "futures.ping"
                }
                
                await self.ws.send(json.dumps(ping_msg))
                self.logger.debug(f"📡 Sent app-level ping")
                
                if time.time() - self._last_pong_time > 60:
                    self.logger.warning(f"⚠️ No pong received for 60s, reconnecting...")
                    if self.ws:
                        await self.ws.close()
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Ping loop error: {e}")
                break
    
    async def run(self):
        """Основной цикл соединения с автореконнектом."""
        self.is_running = True
        reconnect_delay = 1
        
        while self.is_running:
            try:
                async with websockets.connect(
                    self.parent.ws_url,
                    ping_interval=20,
                    ping_timeout=30,
                    max_size=10_000_000,
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 1
                    self._last_pong_time = time.time()
                    self.logger.info(f"✅ Gate {self.conn_id} Connected")
                    
                    await self._subscribe_all()
                    
                    self._ping_task = asyncio.create_task(self._app_ping_loop())
                    
                    async for msg in ws:
                        if not self.is_running:
                            break
                        await self._parse(msg)
                        
            except websockets.ConnectionClosed as e:
                self.logger.error(
                    f"🔌 Gate {self.conn_id} Disconnected: {e.code} - {e.reason or 'No reason'}"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Gate {self.conn_id} WS error: {e}", exc_info=True)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
            
            if self.is_running:
                self.logger.info(f"🔄 Reconnect in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
    
    async def _subscribe_all(self):
        """Подписывается на все активные символы."""
        if not self.parent.active_symbols or not self._ws_connected():
            return
        
        for s in self.parent.active_symbols:
            gate_symbol = self._to_gate_symbol(s)
            
            await self.ws.send(json.dumps({
                'time': int(time.time()),
                'channel': 'futures.trades',
                'event': 'subscribe',
                'payload': [gate_symbol]
            }))
            
            await self.ws.send(json.dumps({
                'time': int(time.time()),
                'channel': 'futures.book_ticker',
                'event': 'subscribe',
                'payload': [gate_symbol]
            }))
            
            await self.ws.send(json.dumps({
                'time': int(time.time()),
                'channel': 'futures.order_book',
                'event': 'subscribe',
                'payload': [gate_symbol, "20", "0"]
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
            
            channel = data.get('channel', '')
            if channel == 'futures.pong':
                self._last_pong_time = time.time()
                self.logger.debug(f"🏓 Received pong")
                return
            
            if 'event' in data and data['event'] == 'subscribe':
                return
            
            event = data.get('event', '')
            
            if channel == 'futures.order_book':
                if event != 'all':
                    return
            else:
                if event != 'update':
                    return
            
            result = data.get('result')
            if not result:
                return
            
            if not isinstance(result, list):
                result = [result]
            
            timestamp_ms = data.get('time_ms', int(time.time() * 1000))
            
            for item in result:
                await self._process_item(channel, item, timestamp_ms)
                    
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")
    
    async def _process_item(self, channel: str, item: dict, timestamp_ms: int):
        """Обрабатывает один элемент данных."""
        try:
            if channel == 'futures.trades':
                gate_symbol = item.get('contract')
            elif channel == 'futures.book_ticker':
                gate_symbol = item.get('s')
            elif channel == 'futures.order_book':
                gate_symbol = item.get('contract')
            else:
                return
            
            if not gate_symbol:
                return
            
            normalized_symbol = self._normalize_symbol(gate_symbol)
            
            await self._ensure_symbol_buffer(normalized_symbol)
            
            async with self._buffer_lock:
                if normalized_symbol not in self.buffers:
                    return
                buffers = self.buffers[normalized_symbol]
            
            if channel == 'futures.trades':
                trade = {
                    'timestamp_ms': item.get('create_time_ms', timestamp_ms),
                    'connection_id': self.conn_id,
                    'trade_id': item['id'],
                    'price': float(item['price']),
                    'qty': float(item['size']),
                }
                buffers["trades"].append(trade)
            
            elif channel == 'futures.book_ticker':
                bbo = {
                    'timestamp_ms': timestamp_ms,
                    'connection_id': self.conn_id,
                    'update_id': item.get('u', 0),
                    'best_bid_price': float(item['b']),
                    'best_bid_qty': float(item['B']),
                    'best_ask_price': float(item['a']),
                    'best_ask_qty': float(item['A'])
                }
                buffers["bookticker"].append(bbo)
            
            elif channel == 'futures.order_book':
                asks_raw = item.get('asks', [])
                bids_raw = item.get('bids', [])
                
                asks = [[float(level['p']), abs(float(level['s']))] for level in asks_raw]
                bids = [[float(level['p']), abs(float(level['s']))] for level in bids_raw]
                
                depth = {
                    'timestamp_ms': item.get('t', timestamp_ms),
                    'connection_id': self.conn_id,
                    'update_id': item.get('id', 0),
                    'bids': bids,
                    'asks': asks
                }
                buffers["depth"].append(depth)
                    
        except Exception as e:
            self.logger.debug(f"Process item error for {channel}: {e}")
    
    async def flush_symbol(self, symbol: str) -> Dict[str, List[dict]]:
        """
        STREAMING: Забирает и очищает буферы ТОЛЬКО одного символа.
        Возвращает: {data_type: [data]}
        """
        result = {}
        
        async with self._buffer_lock:
            if symbol not in self.buffers:
                return result
            
            buffers = self.buffers[symbol]
            for data_type, buffer in buffers.items():
                if buffer:
                    result[data_type] = list(buffer)
                    buffer.clear()  # ← СРАЗУ очищаем
        
        return result
    
    async def stop(self):
        """Останавливает соединение."""
        self.is_running = False
        
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
    
    def get_buffer_stats(self) -> dict:
        """Возвращает статистику буферов."""
        stats = {}
        total = 0
        for symbol, buffers in self.buffers.items():
            symbol_stats = {dt: len(buf) for dt, buf in buffers.items()}
            stats[symbol] = symbol_stats
            total += sum(symbol_stats.values())
        
        stats['_total'] = total
        stats['_warning'] = total > self.EMERGENCY_THRESHOLD
        return stats


class GateCollector:
    """Коллектор для Gate.io Futures с множественными соединениями."""
    
    def __init__(self, num_connections: int = 2, max_workers: int = 8):
        self.ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
        self.exchange = "gate"
        self.num_connections = num_connections
        self.connections: List[GateConnection] = []
        
        self.active_symbols: Set[str] = set()
        self.symbol_lock = asyncio.Lock()
        
        self.is_running = False
        self.logger = logging.getLogger("GateCollector")
        
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        
        self.data_dir = Path('collected_data') / self.exchange
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self._write_semaphore = asyncio.Semaphore(max_workers)
        
        # Очистка старых .tmp файлов при старте
        self._cleanup_temp_files()
        
        for i in range(num_connections):
            conn = GateConnection(f"conn_{i+1}", self)
            self.connections.append(conn)
    
    def _cleanup_temp_files(self):
        """Удаляет оставшиеся .tmp файлы при старте."""
        count = 0
        for tmp_file in self.data_dir.rglob("*.tmp"):
            try:
                tmp_file.unlink()
                count += 1
            except Exception as e:
                self.logger.error(f"Failed to cleanup {tmp_file}: {e}")
        if count > 0:
            self.logger.info(f"🗑️ Cleaned up {count} .tmp files")
    
    async def run(self):
        """Запускает все соединения и фоновые задачи."""
        self.is_running = True
        self.logger.info(f"🚀 Starting {self.num_connections} Gate.io connections")
        
        tasks = [
            *[conn.run() for conn in self.connections],
            self._writer_task(),
            self._memory_monitor()
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
    
    async def _memory_monitor(self):
        """Мониторинг памяти каждые 30 секунд."""
        if not PSUTIL_AVAILABLE:
            self.logger.warning("⚠️ psutil not available, memory monitoring disabled")
            return
        
        process = psutil.Process()
        
        while self.is_running:
            await asyncio.sleep(30)
            
            try:
                mem_info = process.memory_info()
                mem_percent = process.memory_percent()
                
                # Подсчёт буферов
                total_buffered = sum(
                    sum(
                        sum(len(buf) for buf in buffers.values())
                        for buffers in conn.buffers.values()
                    )
                    for conn in self.connections
                )
                
                self.logger.info(
                    f"📊 Memory: {mem_info.rss / 1024 / 1024:.1f} MB ({mem_percent:.1f}%), "
                    f"Buffers: {total_buffered:,} items"
                )
                
                # КРИТИЧЕСКИЕ ПОРОГИ
                if mem_percent > 80:
                    self.logger.error(
                        f"🚨 CRITICAL: Memory usage {mem_percent:.1f}%! "
                        f"Forcing garbage collection and flush..."
                    )
                    
                    gc.collect()
                    await self._flush_all()
                    
                    await asyncio.sleep(5)
                    new_mem = process.memory_percent()
                    self.logger.info(
                        f"📉 After cleanup: {new_mem:.1f}% "
                        f"(freed {mem_percent - new_mem:.1f}%)"
                    )
                
                elif mem_percent > 60:
                    self.logger.warning(
                        f"⚠️ High memory usage: {mem_percent:.1f}%"
                    )
                
                if total_buffered > 500_000:
                    self.logger.error(
                        f"🚨 CRITICAL: {total_buffered:,} items buffered! "
                        f"Forcing flush..."
                    )
                    await self._flush_all()
                
            except Exception as e:
                self.logger.error(f"Memory monitor error: {e}")
    
    async def _writer_task(self):
        """Задача записи с адаптивной частотой."""
        min_interval = 2
        max_interval = 10
        current_interval = 5
        
        while self.is_running:
            await asyncio.sleep(current_interval)
            
            try:
                start_time = time.time()
                
                # Подсчёт буферов перед сбросом
                total_before = sum(
                    sum(
                        sum(len(buf) for buf in buffers.values())
                        for buffers in conn.buffers.values()
                    )
                    for conn in self.connections
                )
                
                await self._flush_all()
                
                flush_duration = time.time() - start_time
                
                # Адаптивная частота
                if total_before > 250_000:
                    current_interval = max(min_interval, current_interval - 0.5)
                    self.logger.warning(
                        f"⚡ Increasing flush frequency to every {current_interval}s "
                        f"(buffer size: {total_before:,})"
                    )
                elif total_before < 50_000 and current_interval < max_interval:
                    current_interval = min(max_interval, current_interval + 0.5)
                
                if flush_duration > 10:
                    self.logger.warning(
                        f"⏱️ Slow flush: {flush_duration:.1f}s for {total_before:,} items"
                    )
                
            except Exception as e:
                self.logger.error(f"Writer task error: {e}", exc_info=True)
    
    async def _flush_all(self):
        """
        STREAMING FLUSH: Обрабатываем символы по одному.
        Минимизирует пиковое потребление памяти.
        """
        # Собираем список активных символов
        all_symbols = set()
        for conn in self.connections:
            async with conn._buffer_lock:
                all_symbols.update(conn.buffers.keys())
        
        if not all_symbols:
            return
        
        # Обрабатываем каждый символ последовательно
        for symbol in all_symbols:
            try:
                await self._flush_symbol(symbol)
            except Exception as e:
                self.logger.error(f"Error flushing {symbol}: {e}", exc_info=True)
    
    async def _flush_symbol(self, symbol: str):
        """
        Сбрасывает данные ОДНОГО символа со ВСЕХ соединений.
        Данные в памяти только для одного символа в каждый момент.
        """
        flush_tasks = []
        
        # Забираем данные со всех соединений для этого символа
        for conn in self.connections:
            symbol_data = await conn.flush_symbol(symbol)
            
            if not symbol_data:
                continue
            
            # Обрабатываем каждый тип данных
            for data_type, data_list in symbol_data.items():
                if not data_list:
                    continue
                
                # Группируем по часам
                hourly_data = self._group_by_hour(data_list)
                
                # Создаём задачи записи
                for hour_key, hour_data in hourly_data.items():
                    task = self._write_with_semaphore(
                        symbol, data_type, conn.conn_id, hour_data, hour_key
                    )
                    flush_tasks.append(task)
                
                # Явно освобождаем память
                del data_list
            
            del symbol_data
        
        # Выполняем запись параллельно (но только для одного символа)
        if flush_tasks:
            results = await asyncio.gather(*flush_tasks, return_exceptions=True)
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                self.logger.error(
                    f"Flush errors for {symbol}: {len(errors)}/{len(flush_tasks)}"
                )
    
    def _group_by_hour(self, data_list: List[dict]) -> Dict[str, List[dict]]:
        """Группирует данные по часам на основе timestamp_ms."""
        hourly_data = {}
        
        for item in data_list:
            timestamp_ms = item.get('timestamp_ms')
            if not timestamp_ms:
                continue
            
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
                self._write_parquet_rotation,
                symbol, data_type, conn_id, data, hour_key
            )
    
    def _write_parquet_rotation(self, symbol: str, data_type: str, conn_id: str, 
                               data: List[dict], hour_key: str):
        """
        ROTATION: Каждый flush создаёт НОВЫЙ файл с timestamp.
        CloudManager потом соберёт все conn_*.parquet файлы.
        """
        if not data:
            return
        
        symbol_dir = self.data_dir / symbol / hour_key
        symbol_dir.mkdir(parents=True, exist_ok=True)
        
        # ROTATION: timestamp в имени файла
        timestamp_ms = int(time.time() * 1000)
        
        # ВАЖНО: сохраняем паттерн conn_*_{data_type}.parquet
        # чтобы CloudManager мог найти файлы через glob("conn_*_{data_type}.parquet")
        filename = f"{conn_id}_{data_type}_{timestamp_ms}.parquet"
        filepath = symbol_dir / filename
        
        schema = self._get_schema(data_type)
        if not schema:
            return
        
        try:
            # Просто создаём и пишем - БЕЗ чтения старых файлов!
            table = pa.Table.from_pylist(data, schema=schema)
            
            pq.write_table(
                table,
                filepath,
                compression='zstd',
                compression_level=3  # быстрая запись
            )
            
            # Явно освобождаем память
            del table
            
        except Exception as e:
            self.logger.error(f"Parquet write error for {filepath}: {e}")
    
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
            
            for conn in self.connections:
                await conn._ensure_symbol_buffer(s)
            
            gate_symbol = self.connections[0]._to_gate_symbol(s)
            
            for conn in self.connections:
                if conn._ws_connected():
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.trades',
                        'event': 'subscribe',
                        'payload': [gate_symbol]
                    }))
                    
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.book_ticker',
                        'event': 'subscribe',
                        'payload': [gate_symbol]
                    }))
                    
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.order_book',
                        'event': 'subscribe',
                        'payload': [gate_symbol, "20", "0"]
                    }))
            
            self.logger.info(f"➕ Added: {s} (Gate: {gate_symbol})")
    
    async def remove_symbol(self, symbol: str):
        """Удаляет символ из подписки."""
        s = symbol.lower()
        
        async with self.symbol_lock:
            if s not in self.active_symbols:
                return
            
            gate_symbol = self.connections[0]._to_gate_symbol(s)
            
            for conn in self.connections:
                if conn._ws_connected():
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.trades',
                        'event': 'unsubscribe',
                        'payload': [gate_symbol]
                    }))
                    
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.book_ticker',
                        'event': 'unsubscribe',
                        'payload': [gate_symbol]
                    }))
                    
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.order_book',
                        'event': 'unsubscribe',
                        'payload': [gate_symbol, "20", "0"]
                    }))
            
            # Сбрасываем только этот символ
            await self._flush_symbol(s)
            
            self.active_symbols.discard(s)
            
            self.logger.info(f"➖ Removed: {s} (Gate: {gate_symbol})")
    
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
    collector = GateCollector(num_connections=2, max_workers=8)
    
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