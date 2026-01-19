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


class GateConnection:
    """Одно WebSocket соединение к Gate.io Futures."""
    
    def __init__(self, conn_id: str, parent: 'GateCollector'):
        self.conn_id = conn_id
        self.parent = parent
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False
        self.logger = logging.getLogger(f"Gate.{conn_id}")
        
        # Буферы для этого соединения
        self.buffers: Dict[str, Dict[str, deque]] = {}
        self._buffer_lock = asyncio.Lock()
    
    async def _init_symbol_buffers(self, symbol: str):
        """Инициализирует буферы для символа."""
        async with self._buffer_lock:
            if symbol not in self.buffers:
                self.buffers[symbol] = {
                    "trades": deque(maxlen=50000),
                    "bookticker": deque(maxlen=50000),
                    "depth": deque(maxlen=25000)
                }
    
    def _normalize_symbol(self, gate_symbol: str) -> str:
        """
        Преобразует символ Gate.io в нормализованный формат.
        
        Gate.io: BTC_USDT -> btcusdt
        Для обратной совместимости с Binance данными.
        """
        return gate_symbol.replace('_', '').lower()
    
    def _to_gate_symbol(self, normalized_symbol: str) -> str:
        """
        Преобразует нормализованный символ в формат Gate.io.
        
        btcusdt -> BTC_USDT
        ethusdt -> ETH_USDT
        """
        # Убираем 'usdt' в конце и добавляем подчеркивание
        if normalized_symbol.endswith('usdt'):
            base = normalized_symbol[:-4].upper()
            return f"{base}_USDT"
        # Для других пар можно добавить логику
        return normalized_symbol.upper()
    
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
                    self.logger.info(f"✅ Gate {self.conn_id} Connected")
                    
                    await self._subscribe_all()
                    
                    async for msg in ws:
                        if not self.is_running:
                            break
                        await self._parse(msg)
                        
            except websockets.ConnectionClosed as e:
                self.logger.error(
                    f"🔌 Gate {self.conn_id} Disconnected\n"
                    f"  Code: {e.code}\n"
                    f"  Reason: {e.reason or 'No reason provided'}",
                    exc_info=True
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    f"Gate {self.conn_id} WS error: {type(e).__name__}: {e}",
                    exc_info=True
                )
            
            if self.is_running:
                self.logger.info(f"🔄 Reconnect in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
    
    async def _subscribe_all(self):
        """Подписывается на все активные символы."""
        if not self.parent.active_symbols or not self._ws_connected():
            return
        
        # Gate.io использует отдельные подписки для каждого канала
        for s in self.parent.active_symbols:
            gate_symbol = self._to_gate_symbol(s)
            
            # Подписка на trades
            await self.ws.send(json.dumps({
                'time': int(time.time()),
                'channel': 'futures.trades',
                'event': 'subscribe',
                'payload': [gate_symbol]
            }))
            
            # Подписка на book ticker (BBO)
            await self.ws.send(json.dumps({
                'time': int(time.time()),
                'channel': 'futures.book_ticker',
                'event': 'subscribe',
                'payload': [gate_symbol]
            }))
            
            # ИСПРАВЛЕНО: подписка на полные снимки ордербука (не инкрементальные апдейты)
            # interval="0" означает максимальную частоту обновлений
            await self.ws.send(json.dumps({
                'time': int(time.time()),
                'channel': 'futures.order_book',
                'event': 'subscribe',
                'payload': [gate_symbol, "20", "0"]  # 20 уровней, "0" = максимальная частота
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
            
            # Пропускаем служебные сообщения
            if 'event' in data and data['event'] == 'subscribe':
                return
            
            channel = data.get('channel', '')
            event = data.get('event', '')
            
            # ИСПРАВЛЕНО: order_book использует event="all", остальные каналы "update"
            if channel == 'futures.order_book':
                if event != 'all':
                    return
            else:
                if event != 'update':
                    return
            
            result = data.get('result')
            if not result:
                return
            
            # Gate.io может отдавать список результатов (для trades, bookticker)
            # Для order_book result - это один объект
            if not isinstance(result, list):
                result = [result]
            
            for item in result:
                await self._process_item(channel, item, data.get('time_ms', int(time.time() * 1000)))
                    
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")
    
    async def _process_item(self, channel: str, item: dict, timestamp_ms: int):
        """Обрабатывает один элемент данных."""
        try:
            # ИСПРАВЛЕНО: получаем символ в зависимости от канала
            if channel == 'futures.trades':
                gate_symbol = item.get('contract')
            elif channel == 'futures.book_ticker':
                gate_symbol = item.get('s')
            elif channel == 'futures.order_book':
                gate_symbol = item.get('contract')  # ИСПРАВЛЕНО: для order_book используется 'contract'
            else:
                return
            
            if not gate_symbol:
                self.logger.warning(f"Missing symbol in {channel}: {item.keys()}")
                return
            
            # Нормализуем символ для хранения
            normalized_symbol = self._normalize_symbol(gate_symbol)
            
            async with self._buffer_lock:
                if normalized_symbol not in self.buffers:
                    return
                buffers = self.buffers[normalized_symbol]
            
            if channel == 'futures.trades':
                # Gate.io уже отдаёт signed size:
                # Positive size = taker is buyer (покупка)
                # Negative size = taker is seller (продажа)
                trade = {
                    'timestamp_ms': item.get('create_time_ms', timestamp_ms),
                    'connection_id': self.conn_id,
                    'trade_id': item['id'],
                    'price': float(item['price']),
                    'qty': float(item['size']),  # УЖЕ со знаком!
                    'trade_time_ms': item.get('create_time_ms', timestamp_ms),
                }
                buffers["trades"].append(trade)
            
            elif channel == 'futures.book_ticker':
                # Best bid/ask
                bbo = {
                    'timestamp_ms': timestamp_ms,
                    'connection_id': self.conn_id,
                    'update_id': item.get('u', 0),  # Gate может не отдавать update_id для BBO
                    'best_bid_price': float(item['b']),
                    'best_bid_qty': float(item['B']),
                    'best_ask_price': float(item['a']),
                    'best_ask_qty': float(item['A'])
                }
                buffers["bookticker"].append(bbo)
            
            elif channel == 'futures.order_book':
                # ИСПРАВЛЕНО: обработка полных снимков ордербука
                # Gate.io формат: asks/bids как [{p: price, s: size}, ...]
                asks_raw = item.get('asks', [])
                bids_raw = item.get('bids', [])
                
                # ИСПРАВЛЕНО: преобразуем из {p, s} в [[price, size], ...]
                asks = [[float(level['p']), abs(float(level['s']))] for level in asks_raw]
                bids = [[float(level['p']), abs(float(level['s']))] for level in bids_raw]
                
                depth = {
                    'timestamp_ms': item.get('t', timestamp_ms),  # Gate отдаёт 't' для timestamp
                    'connection_id': self.conn_id,
                    'update_id': item.get('id', 0),  # ИСПРАВЛЕНО: orderbook id
                    'bids': bids,
                    'asks': asks
                }
                buffers["depth"].append(depth)
                    
        except Exception as e:
            self.logger.debug(f"Process item error for {channel}: {e}")
    
    async def stop(self):
        """Останавливает соединение."""
        self.is_running = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


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
        
        for i in range(num_connections):
            conn = GateConnection(f"conn_{i+1}", self)
            self.connections.append(conn)
    
    async def run(self):
        """Запускает все соединения и фоновые задачи."""
        self.is_running = True
        self.logger.info(f"🚀 Starting {self.num_connections} Gate.io connections")
        
        tasks = [
            *[conn.run() for conn in self.connections],
            self._periodic_flush()
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop(self):
        """Останавливает все соединения."""
        self.logger.info("🛑 Stopping...")
        self.is_running = False
        
        await asyncio.gather(*[conn.stop() for conn in self.connections])
        
        await self.flush_all()
        
        self.thread_pool.shutdown(wait=True)
        self.logger.info("✅ Stopped")
    
    async def _periodic_flush(self):
        """Периодически сбрасывает буферы на диск."""
        while self.is_running:
            await asyncio.sleep(5)
            try:
                await self.flush_all()
            except Exception as e:
                self.logger.error(f"Flush error: {e}")
    
    async def flush_all(self):
        """Сбрасывает буферы БЕЗ длительной блокировки."""
        flush_tasks = []
        
        for conn in self.connections:
            batches = {}
            async with conn._buffer_lock:
                for symbol, buffers in conn.buffers.items():
                    batches[symbol] = {}
                    for data_type, buffer in buffers.items():
                        if buffer:
                            batches[symbol][data_type] = list(buffer)
                            buffer.clear()
            
            for symbol, data_types in batches.items():
                for data_type, data_list in data_types.items():
                    if data_list:
                        task = self._write_with_semaphore(
                            symbol, data_type, conn.conn_id, data_list
                        )
                        flush_tasks.append(task)
        
        if flush_tasks:
            results = await asyncio.gather(*flush_tasks, return_exceptions=True)
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                self.logger.error(f"Flush errors: {len(errors)}")
            else:
                self.logger.info(f"💾 Flushed {len(flush_tasks)} buffers")
    
    async def _write_with_semaphore(self, symbol: str, data_type: str, 
                                    conn_id: str, data: List[dict]):
        """Запись с ограничением параллелизма."""
        async with self._write_semaphore:
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self._write_parquet,
                symbol, data_type, conn_id, data
            )
    
    def _write_parquet(self, symbol: str, data_type: str, conn_id: str, data: List[dict]):
        """Атомарная запись с .tmp файлом."""
        if not data:
            return
        
        now = datetime.now(timezone.utc)
        symbol_dir = self.data_dir / symbol / now.strftime("%Y%m%d_%H")
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
        """Схема для depth - двумерные массивы [[price, qty], ...]."""
        return pa.schema([
            ('timestamp_ms', pa.int64()),
            ('connection_id', pa.string()),
            ('update_id', pa.int64()),
            ('bids', pa.list_(pa.list_(pa.float64(), 2))),
            ('asks', pa.list_(pa.list_(pa.float64(), 2)))
        ])
    
    @staticmethod
    def _get_trades_schema() -> pa.Schema:
        """
        Схема для trades.
        
        ВАЖНО: qty уже со знаком (positive = buy, negative = sell).
        Поле is_buyer_maker не хранится, как и в Binance после трансформации.
        """
        return pa.schema([
            ('timestamp_ms', pa.int64()),
            ('connection_id', pa.string()),
            ('trade_id', pa.int64()),
            ('price', pa.float64()),
            ('qty', pa.float64()),  # Signed: + купля, - продажа
            ('trade_time_ms', pa.int64()),
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
        """
        Добавляет символ к подписке.
        
        Принимает нормализованный формат (btcusdt, ethusdt).
        """
        s = symbol.lower()
        
        async with self.symbol_lock:
            if s in self.active_symbols:
                return
            
            self.active_symbols.add(s)
            
            # Инициализируем буферы
            for conn in self.connections:
                await conn._init_symbol_buffers(s)
            
            # Подписываемся на каналы
            gate_symbol = self.connections[0]._to_gate_symbol(s)
            
            for conn in self.connections:
                if conn._ws_connected():
                    # Trades
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.trades',
                        'event': 'subscribe',
                        'payload': [gate_symbol]
                    }))
                    
                    # Book ticker
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.book_ticker',
                        'event': 'subscribe',
                        'payload': [gate_symbol]
                    }))
                    
                    # ИСПРАВЛЕНО: подписка на полные снимки ордербука
                    # interval="0" означает максимальную частоту обновлений
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
                    # Отписываемся от всех каналов
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
                    
                    # ИСПРАВЛЕНО: отписка от полных снимков ордербука
                    await conn.ws.send(json.dumps({
                        'time': int(time.time()),
                        'channel': 'futures.order_book',
                        'event': 'unsubscribe',
                        'payload': [gate_symbol, "20", "0"]
                    }))
            
            await self.flush_all()
            self.active_symbols.discard(s)
            
            self.logger.info(f"➖ Removed: {s} (Gate: {gate_symbol})")
    
    async def get_status(self) -> dict:
        """Возвращает статус коллектора."""
        connections_status = []
        for conn in self.connections:
            buffer_counts = {}
            async with conn._buffer_lock:
                for symbol, buffers in conn.buffers.items():
                    buffer_counts[symbol] = {
                        dt: len(buf) for dt, buf in buffers.items()
                    }
            
            connections_status.append({
                'id': conn.conn_id,
                'connected': conn._ws_connected(),
                'buffers': buffer_counts
            })
        
        return {
            'is_running': self.is_running,
            'active_symbols': list(self.active_symbols),
            'connections': connections_status
        }


async def main():
    collector = GateCollector(num_connections=2, max_workers=8)
    
    # Используем нормализованный формат символов
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