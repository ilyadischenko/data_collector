"""
WebSocket соединение к Binance через CCXT Pro
"""

import asyncio
import time
import logging
import gc
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Set, Optional, Deque

try:
    import ccxt.pro as ccxtpro
except ImportError:
    raise ImportError("Install ccxt pro: pip install ccxt")

import pyarrow as pa
import pyarrow.parquet as pq

from config import CONFIG


class WSConnection:
    """
    Одно WebSocket соединение к Binance через CCXT Pro.
    Собирает trades и depth для множества символов.
    """
    
    def __init__(
        self,
        market_type: str,  # "spot" или "futures"
        pool_id: str,      # "poolA" или "poolB"
        connection_id: str # "conn_1", "conn_2", etc.
    ):
        self.market_type = market_type
        self.pool_id = pool_id
        self.connection_id = connection_id
        
        # Создаём CCXT exchange
        self.exchange = self._create_exchange()
        
        # Состояние
        self.is_running = False
        self.subscribed_symbols: Set[str] = set()
        
        # Буферы для каждого символа
        self.buffers: Dict[str, Dict[str, Deque]] = {}
        self._buffer_lock = asyncio.Lock()
        
        # Метрики
        self.last_message_time = time.time()
        self.messages_received = 0
        
        # Rate limiting для depth
        self._last_depth_updates: Dict[str, float] = {}
        
        # Логирование
        self.logger = logging.getLogger(
            f"WSConnection.{market_type}.{pool_id}.{connection_id}"
        )
        
        # Задачи
        self._watch_tasks: Dict[str, list] = {}  # symbol -> [task_trades, task_depth]
    
    def _create_exchange(self):
        """Создаёт CCXT exchange instance"""
        options = {
            'enableRateLimit': True,
            'newUpdates': True
        }
        
        # Настройка типа рынка
        if self.market_type == 'futures':
            options['options'] = {'defaultType': 'future'}
        
        exchange = ccxtpro.binance(options)
        return exchange
    
    async def initialize(self) -> None:
        """Инициализация exchange"""
        try:
            await self.exchange.load_markets()
            self.logger.info(
                f"✅ Initialized Binance {self.market_type} "
                f"- {len(self.exchange.markets)} markets available"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize: {e}")
            raise
    
    async def subscribe_symbol(self, symbol: str) -> bool:
        """
        Подписывается на символ (trades + depth)
        
        Args:
            symbol: Символ в формате CCXT (BTC/USDT)
        """
        if symbol in self.subscribed_symbols:
            self.logger.warning(f"Already subscribed to {symbol}")
            return True
        
        # Проверяем существование символа
        if symbol not in self.exchange.markets:
            self.logger.error(f"Symbol {symbol} not found in markets")
            return False
        
        try:
            # Создаём буферы
            await self._ensure_symbol_buffer(symbol)
            
            # Добавляем в подписанные
            self.subscribed_symbols.add(symbol)
            
            # Запускаем watch циклы
            task_trades = asyncio.create_task(self._watch_trades_loop(symbol))
            task_depth = asyncio.create_task(self._watch_depth_loop(symbol))
            
            self._watch_tasks[symbol] = [task_trades, task_depth]
            
            self.logger.info(
                f"➕ Subscribed to {symbol} "
                f"({len(self.subscribed_symbols)} symbols)"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to subscribe to {symbol}: {e}")
            self.subscribed_symbols.discard(symbol)
            return False
    
    async def unsubscribe_symbol(self, symbol: str) -> bool:
        """Отписывается от символа"""
        if symbol not in self.subscribed_symbols:
            return True
        
        try:
            # Удаляем из подписанных (watch циклы сами остановятся)
            self.subscribed_symbols.discard(symbol)
            
            # Отменяем задачи
            if symbol in self._watch_tasks:
                for task in self._watch_tasks[symbol]:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                del self._watch_tasks[symbol]
            
            self.logger.info(f"➖ Unsubscribed from {symbol}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unsubscribe from {symbol}: {e}")
            return False
    
    async def _ensure_symbol_buffer(self, symbol: str) -> None:
        """Создаёт буферы для символа"""
        async with self._buffer_lock:
            if symbol not in self.buffers:
                self.buffers[symbol] = {
                    "trades": deque(maxlen=CONFIG.binance.TRADES_BUFFER_SIZE),
                    "depth": deque(maxlen=CONFIG.binance.DEPTH_BUFFER_SIZE)
                }
    
    async def _watch_trades_loop(self, symbol: str) -> None:
        """Цикл получения трейдов для символа"""
        self.logger.info(f"📊 Starting trades stream for {symbol}")
        
        while self.is_running and symbol in self.subscribed_symbols:
            try:
                trades = await self.exchange.watch_trades(symbol)
                
                if not trades:
                    continue
                
                await self._ensure_symbol_buffer(symbol)
                
                async with self._buffer_lock:
                    if symbol not in self.buffers:
                        continue
                    
                    buffer = self.buffers[symbol]["trades"]
                    
                    for trade in trades:
                        # ВАЖНО: Конвертируем в signed qty
                        # Положительное = покупка, отрицательное = продажа
                        amount = float(trade['amount'])
                        if trade.get('side') == 'sell':
                            amount = -amount
                        
                        trade_data = {
                            'timestamp_ms': int(trade['timestamp']),
                            'pool_id': self.pool_id,
                            'connection_id': self.connection_id,
                            'trade_id': int(trade.get('id', 0)) if trade.get('id') else 0,
                            'price': float(trade['price']),
                            'qty': amount  # Signed quantity
                        }
                        buffer.append(trade_data)
                
                self.messages_received += len(trades)
                self.last_message_time = time.time()
                
            except ccxtpro.NetworkError as e:
                self.logger.warning(f"Network error for {symbol} trades: {e}")
                await asyncio.sleep(1)
            except ccxtpro.ExchangeError as e:
                self.logger.error(f"Exchange error for {symbol} trades: {e}")
                await asyncio.sleep(5)
            except Exception as e:
                if symbol not in self.subscribed_symbols:
                    break
                self.logger.error(f"Unexpected error for {symbol} trades: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def _watch_depth_loop(self, symbol: str) -> None:
        """Цикл получения orderbook для символа"""
        self.logger.info(f"📖 Starting depth stream for {symbol} (100ms)")
        
        limit = CONFIG.binance.DEPTH_LIMIT
        update_interval = CONFIG.binance.DEPTH_UPDATE_INTERVAL
        
        while self.is_running and symbol in self.subscribed_symbols:
            try:
                orderbook = await self.exchange.watch_order_book(symbol, limit)
                
                # Защита от неполных данных при инициализации
                if not orderbook:
                    self.logger.debug(f"Empty orderbook for {symbol}, waiting for data...")
                    await asyncio.sleep(0.1)
                    continue
                
                # Проверяем обязательные поля
                if not isinstance(orderbook, dict):
                    self.logger.warning(f"Invalid orderbook type for {symbol}: {type(orderbook)}")
                    await asyncio.sleep(0.1)
                    continue
                
                # Проверяем timestamp
                timestamp = orderbook.get('timestamp')
                if timestamp is None:
                    self.logger.debug(f"No timestamp yet for {symbol}, waiting for snapshot...")
                    await asyncio.sleep(0.1)
                    continue
                
                # Проверяем наличие данных
                bids = orderbook.get('bids', [])
                asks = orderbook.get('asks', [])
                
                if not bids or not asks:
                    self.logger.debug(f"Empty bids/asks for {symbol}, waiting for data...")
                    await asyncio.sleep(0.1)
                    continue
                
                # Rate limiting - не чаще 100ms
                current_time = time.time()
                last_update = self._last_depth_updates.get(symbol, 0)
                
                if current_time - last_update < update_interval:
                    await asyncio.sleep(0.01)
                    continue
                
                self._last_depth_updates[symbol] = current_time
                
                await self._ensure_symbol_buffer(symbol)
                
                async with self._buffer_lock:
                    if symbol not in self.buffers:
                        continue
                    
                    # Теперь безопасно создаём depth_data
                    depth_data = {
                        'timestamp_ms': int(timestamp),
                        'pool_id': self.pool_id,
                        'connection_id': self.connection_id,
                        'update_id': int(orderbook.get('nonce', 0)),
                        'bids': [[float(p), float(q)] for p, q in bids[:limit]],
                        'asks': [[float(p), float(q)] for p, q in asks[:limit]]
                    }
                    
                    self.buffers[symbol]["depth"].append(depth_data)
                
                self.messages_received += 1
                self.last_message_time = time.time()
                
            except ccxtpro.NetworkError as e:
                self.logger.warning(f"Network error for {symbol} depth: {e}")
                await asyncio.sleep(1)
            except ccxtpro.ExchangeError as e:
                self.logger.error(f"Exchange error for {symbol} depth: {e}")
                await asyncio.sleep(5)
            except Exception as e:
                if symbol not in self.subscribed_symbols:
                    break
                self.logger.error(f"Unexpected error for {symbol} depth: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    def _normalize_symbol_for_path(self, symbol: str) -> str:
        """
        Нормализует символ для файловой системы.
        BTC/USDT → btcusdt
        """
        return symbol.replace('/', '').replace(':', '').lower()
    
    async def flush_symbol(self, symbol: str) -> Dict[str, list]:
        """
        Забирает и очищает буферы для символа.
        Возвращает данные для записи.
        """
        result = {}
        
        async with self._buffer_lock:
            if symbol not in self.buffers:
                return result
            
            buffers = self.buffers[symbol]
            for data_type, buffer in buffers.items():
                if buffer:
                    result[data_type] = list(buffer)
                    buffer.clear()
        
        return result
    
    def write_parquet(
        self,
        symbol: str,
        data_type: str,
        data: list,
        output_dir: Path
    ) -> Optional[Path]:
        """
        Записывает данные в parquet файл.
        
        Args:
            symbol: Символ (BTC/USDT)
            data_type: "trades" или "depth"
            data: Список словарей с данными
            output_dir: Директория для записи
            
        Returns:
            Path к созданному файлу или None
        """
        if not data:
            return None
        
        try:
            # Создаём директорию
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Имя файла
            timestamp_ms = int(time.time() * 1000)
            filename = f"{self.pool_id}_{self.connection_id}_{data_type}_{timestamp_ms}.parquet"
            filepath = output_dir / filename
            
            # Схема
            schema = self._get_schema(data_type)
            if not schema:
                self.logger.error(f"Unknown data type: {data_type}")
                return None
            
            # Записываем
            table = pa.Table.from_pylist(data, schema=schema)
            
            pq.write_table(
                table,
                filepath,
                compression=CONFIG.storage.PARQUET_COMPRESSION,
                compression_level=CONFIG.storage.PARQUET_COMPRESSION_LEVEL
            )
            
            del table
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"Parquet write error for {symbol}/{data_type}: {e}")
            return None
    
    def _get_schema(self, data_type: str) -> Optional[pa.Schema]:
        """Возвращает схему для типа данных"""
        schemas = {
            'trades': pa.schema([
                ('timestamp_ms', pa.int64()),
                ('pool_id', pa.string()),
                ('connection_id', pa.string()),
                ('trade_id', pa.int64()),
                ('price', pa.float64()),
                ('qty', pa.float64()),  # Signed: positive = buy, negative = sell
            ]),
            'depth': pa.schema([
                ('timestamp_ms', pa.int64()),
                ('pool_id', pa.string()),
                ('connection_id', pa.string()),
                ('update_id', pa.int64()),
                ('bids', pa.list_(pa.list_(pa.float64(), 2))),
                ('asks', pa.list_(pa.list_(pa.float64(), 2)))
            ])
        }
        return schemas.get(data_type)
    
    def get_buffer_stats(self) -> dict:
        """Возвращает статистику буферов"""
        stats = {}
        total = 0
        
        for symbol, buffers in self.buffers.items():
            symbol_stats = {dt: len(buf) for dt, buf in buffers.items()}
            stats[symbol] = symbol_stats
            total += sum(symbol_stats.values())
        
        stats['_total'] = total
        stats['_symbols'] = len(self.buffers)
        
        return stats
    
    async def close(self) -> None:
        """Закрывает соединение"""
        self.is_running = False
        
        # Отменяем все задачи
        for symbol, tasks in self._watch_tasks.items():
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._watch_tasks.clear()
        
        # Закрываем exchange
        try:
            await self.exchange.close()
        except Exception as e:
            self.logger.error(f"Error closing exchange: {e}")
        
        self.logger.info("Connection closed")