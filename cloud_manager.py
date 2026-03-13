"""
Cloud Manager - обработка и загрузка данных в облако
ВАЖНО: Обрабатывает символы ПОСЛЕДОВАТЕЛЬНО для экономии памяти
"""

import asyncio
import logging
import gzip
import shutil
import gc
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

from config import CONFIG

try:
    from cloud import CloudStorage
except ImportError:
    logging.warning("CloudStorage not found, upload will be disabled")
    CloudStorage = None


class CloudManager:
    """
    Менеджер облачного хранилища.
    
    КЛЮЧЕВЫЕ ОСОБЕННОСТИ:
    1. Обрабатывает символы ПОСЛЕДОВАТЕЛЬНО (не параллельно)
    2. Trades отправляются с signed qty (БЕЗ is_buyer_maker)
    3. Принудительная очистка памяти после каждого символа
    """
    
    def __init__(self, market_type: str):
        """
        Args:
            market_type: "spot" или "futures"
        """
        self.market_type = market_type
        self.is_running = False
        
        # Облачное хранилище
        if CloudStorage:
            self.cloud = CloudStorage()
        else:
            self.cloud = None
            logging.warning("Cloud storage disabled")
        
        # Директории
        self.data_dir = Path(CONFIG.storage.LOCAL_DIR) / "binance" / market_type
        
        # Настройки
        self.compress_before_upload = CONFIG.storage.GZIP_ENABLED
        self.parquet_compression_level = CONFIG.storage.PARQUET_COMPRESSION_LEVEL
        self.hour_change_delay = CONFIG.storage.HOUR_CHANGE_DELAY
        
        # Отслеживание обработанных часов
        self.last_processed_hour = self._get_previous_hour()
        
        # Thread pool для I/O
        self._executor = ThreadPoolExecutor(max_workers=CONFIG.storage.MAX_WORKERS)
        
        # Логирование
        self.logger = logging.getLogger(f"CloudManager.{market_type}")
    
    def _get_previous_hour(self) -> str:
        """Возвращает предыдущий час в формате YYYYMMDD_HH"""
        prev = datetime.now(timezone.utc) - timedelta(hours=1)
        return prev.strftime("%Y%m%d_%H")
    
    def _get_current_hour(self) -> str:
        """Возвращает текущий час в формате YYYYMMDD_HH"""
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    
    async def run(self) -> None:
        """Фоновая задача для отслеживания смены часа"""
        self.is_running = True
        self.logger.info(
            f"☁️ Cloud Manager started for {self.market_type}. "
            f"Compression: Parquet ZSTD-{self.parquet_compression_level}"
            f"{', + GZIP' if self.compress_before_upload else ''}, "
            f"Hour change delay: {self.hour_change_delay}s"
        )
        
        await asyncio.sleep(5)
        
        while self.is_running:
            try:
                await asyncio.sleep(30)
                
                current_hour = self._get_current_hour()
                
                if current_hour != self.last_processed_hour:
                    self.logger.info(
                        f"⏰ Hour changed: {self.last_processed_hour} -> {current_hour}. "
                        f"Waiting {self.hour_change_delay}s before processing..."
                    )
                    
                    await asyncio.sleep(self.hour_change_delay)
                    
                    prev_hour = self.last_processed_hour
                    await self.process_hour(prev_hour)
                    
                    # Принудительная очистка памяти после обработки часа
                    gc.collect()
                    self.logger.info("🧹 Memory cleanup after hour processing")
                    
                    self.last_processed_hour = current_hour
                
            except Exception as e:
                self.logger.error(f"❌ Manager loop error: {e}", exc_info=True)
    
    def stop(self) -> None:
        """Останавливает менеджер"""
        self.is_running = False
        self._executor.shutdown(wait=True)
        self.logger.info("🛑 Cloud Manager stopped")
    
    async def process_hour(self, hour_key: str) -> None:
        """
        Обрабатывает час ПОСЛЕДОВАТЕЛЬНО по символам.
        
        Args:
            hour_key: Час в формате YYYYMMDD_HH
        """
        if not self.data_dir.exists():
            return
        
        date, hour = hour_key.split('_')
        
        # Получаем список всех символов в этом часе
        symbols_to_process = []
        
        for symbol_dir in self.data_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            
            hour_dir = symbol_dir / hour_key
            if hour_dir.exists():
                symbols_to_process.append((symbol_dir.name, hour_dir))
        
        if not symbols_to_process:
            self.logger.info(f"No data to process for {hour_key}")
            return
        
        self.logger.info(
            f"📦 Processing {len(symbols_to_process)} symbols for {hour_key}"
        )
        
        # ВАЖНО: Обрабатываем ПОСЛЕДОВАТЕЛЬНО
        success_count = 0
        
        for symbol, hour_dir in symbols_to_process:
            try:
                self.logger.info(f"Processing {symbol}/{hour_key}...")
                
                # Обрабатываем trades, потом depth
                for data_type in ["trades", "depth"]:
                    result = await self._merge_compress_and_upload(
                        symbol, date, hour, data_type, hour_dir
                    )
                    
                    if result:
                        success_count += 1
                    
                    # Очистка памяти после каждого типа
                    gc.collect()
                
                # Удаляем директорию после успешной обработки
                try:
                    for file in hour_dir.glob("*"):
                        file.unlink()
                    hour_dir.rmdir()
                    self.logger.info(f"🗑️ Deleted: {hour_dir}")
                except Exception as e:
                    self.logger.error(f"Failed to delete {hour_dir}: {e}")
                
                # Очистка памяти после символа
                gc.collect()
                
            except Exception as e:
                self.logger.error(f"Error processing {symbol}/{hour_key}: {e}")
        
        self.logger.info(
            f"✅ Hour {hour_key} processed: {success_count} files uploaded"
        )
    
    async def _merge_compress_and_upload(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        hour_dir: Path
    ) -> bool:
        """
        Объединяет, сжимает и загружает файлы для одного типа данных.
        
        ВАЖНО: Trades отправляются с signed qty БЕЗ is_buyer_maker
        """
        merged_file = None
        compressed_file = None
        upload_file = None
        
        try:
            # 1. Объединяем файлы от обоих пулов
            merged_file = await self.merge_parquet_files(
                symbol, date, hour, data_type, hour_dir
            )
            
            if not merged_file:
                return False
            
            upload_file = merged_file
            
            # 2. Дополнительное сжатие GZIP
            if self.compress_before_upload:
                compressed_file = await self.compress_file_gzip(merged_file)
                
                if compressed_file:
                    upload_file = compressed_file
                    # Удаляем несжатый файл
                    try:
                        merged_file.unlink()
                        self.logger.debug(f"Deleted uncompressed: {merged_file.name}")
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {merged_file.name}: {e}")
                else:
                    self.logger.warning(
                        f"GZIP failed, uploading uncompressed: {merged_file.name}"
                    )
            
            # 3. Загружаем в облако
            if self.cloud:
                success = await self.cloud.async_upload_file(
                    local_path=upload_file,
                    exchange="binance",
                    market=self.market_type,
                    symbol=symbol,
                    date=date,
                    hour=hour,
                    data_type=data_type,
                    is_compressed=self.compress_before_upload and upload_file.suffix == '.gz'
                )
                
                return success
            else:
                self.logger.warning("Cloud storage disabled, file not uploaded")
                return True
            
        except Exception as e:
            self.logger.error(
                f"Error in merge_compress_upload for {symbol}/{data_type}: {e}"
            )
            return False
        
        finally:
            # Гарантированная очистка
            del merged_file, compressed_file, upload_file
            gc.collect()
    
    async def merge_parquet_files(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        hour_dir: Path
    ) -> Optional[Path]:
        """
        Объединяет parquet файлы от обоих пулов.
        """
        # Находим все файлы для этого типа данных
        # poolA_conn1_trades_*.parquet, poolB_conn2_trades_*.parquet, etc.
        pattern = f"*_{data_type}_*.parquet"
        source_files = list(hour_dir.glob(pattern))
        
        if not source_files:
            self.logger.debug(f"No files for {symbol}/{data_type}")
            return None
        
        self.logger.info(
            f"🔗 Merging {len(source_files)} files: {symbol}/{data_type}"
        )
        
        # Выполняем в executor
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._merge_parquet_sync,
            source_files,
            hour_dir,
            data_type,
            symbol
        )
    
    def _merge_parquet_sync(
        self,
        source_files: List[Path],
        hour_dir: Path,
        data_type: str,
        symbol: str
    ) -> Optional[Path]:
        """
        Синхронное объединение parquet файлов.
        """
        tables = []
        merged_table = None
        df = None
        
        try:
            # 1. Читаем все файлы
            total_rows = 0
            for file in source_files:
                try:
                    table = pq.read_table(file)
                    tables.append(table)
                    total_rows += len(table)
                except Exception as e:
                    self.logger.error(f"Failed to read {file.name}: {e}")
            
            if not tables:
                return None
            
            # 2. Объединяем
            merged_table = pa.concat_tables(tables)
            
            # Освобождаем tables (= None вместо del)
            tables = None
            gc.collect()
            
            # 3. Конвертируем в pandas
            df = merged_table.to_pandas()
            original_count = len(df)
            
            # Освобождаем merged_table
            merged_table = None
            gc.collect()
            
            # 4. Дедупликация
            if data_type == "trades":
                df.sort_values('timestamp_ms', inplace=True)
                df.drop_duplicates(subset=['trade_id'], keep='last', inplace=True)
            elif data_type == "depth":
                df.sort_values(['update_id', 'timestamp_ms'], inplace=True)
                df.drop_duplicates(subset=['update_id'], keep='last', inplace=True)
            
            duplicates_removed = original_count - len(df)
            
            if duplicates_removed > 0:
                self.logger.info(
                    f"   Deduplicated {data_type}: {duplicates_removed:,} removed "
                    f"({duplicates_removed/original_count*100:.1f}%), "
                    f"{len(df):,} unique records kept"
                )
            
            df.reset_index(drop=True, inplace=True)
            
            # 5. Удаляем служебные поля
            if 'pool_id' in df.columns:
                df.drop(columns=['pool_id'], inplace=True)
            if 'connection_id' in df.columns:
                df.drop(columns=['connection_id'], inplace=True)
            
            # 6. Для trades убираем is_buyer_maker если есть
            if data_type == "trades":
                if 'is_buyer_maker' in df.columns:
                    df.drop(columns=['is_buyer_maker'], inplace=True)
                    self.logger.debug("Removed is_buyer_maker from trades")
            
            # 7. Конвертируем обратно в Arrow
            merged_table = pa.Table.from_pandas(df, preserve_index=False)
            
            # Освобождаем df
            df = None
            gc.collect()
            
            # 8. Приводим к финальной схеме
            final_schema = self._get_final_schema(data_type)
            merged_table = merged_table.cast(final_schema)
            
            # 9. Запись с атомарной заменой
            merged_path = hour_dir / f"merged_{data_type}.parquet"
            temp_path = hour_dir / f"merged_{data_type}.parquet.tmp"
            
            pq.write_table(
                merged_table,
                temp_path,
                compression=CONFIG.storage.PARQUET_COMPRESSION,
                compression_level=self.parquet_compression_level,
                use_dictionary=True,
                data_page_size=1024*1024,
                write_statistics=True,
            )
            
            temp_path.replace(merged_path)
            
            file_size = merged_path.stat().st_size
            final_rows = len(merged_table)
            compression_ratio = (file_size / final_rows) if final_rows > 0 else 0
            
            self.logger.info(
                f"✅ Merged {symbol}/{data_type}: {len(source_files)} files → "
                f"{final_rows:,} unique rows (from {total_rows:,}), "
                f"{file_size / 1024:.1f} KB ({compression_ratio:.2f} bytes/row)"
            )
            
            # Освобождаем merged_table
            merged_table = None
            gc.collect()
            
            return merged_path
            
        except Exception as e:
            self.logger.error(f"❌ Merge failed for {symbol}/{data_type}: {e}")
            temp_path = hour_dir / f"merged_{data_type}.parquet.tmp"
            if temp_path.exists():
                temp_path.unlink()
            return None
        
        finally:
            # Гарантированная очистка (= None безопасно даже если уже None)
            tables = None
            merged_table = None
            df = None
            gc.collect()  

    async def flush_current_hour(self) -> dict:
        """
        Принудительный flush ТЕКУЩЕГО часа.
        
        Логика:
        1. Берём все файлы за текущий час
        2. Собираем по одному символу
        3. Отправляем в облако (как snapshot)
        4. НЕ удаляем файлы — они нужны для финальной сборки в конце часа
        """
        if self._processing:
            return {
                'status': 'busy',
                'message': 'Processing already in progress'
            }
        
        self._processing = True
        current_hour = self._get_current_hour()
        
        self.logger.info(f"🔄 Force flush started for current hour: {current_hour}")
        
        try:
            if not self.data_dir.exists():
                return {'status': 'success', 'symbols_processed': 0, 'hour': current_hour}
            
            date, hour = current_hour.split('_')
            
            # ============================================================
            # ЭТАП 1: Собираем информацию (только пути!)
            # ============================================================
            
            symbols_to_process = []
            
            for symbol_dir in self.data_dir.iterdir():
                if not symbol_dir.is_dir():
                    continue
                
                hour_dir = symbol_dir / current_hour
                if not hour_dir.exists():
                    continue
                
                # Запоминаем конкретные файлы на момент вызова
                trades_files = list(hour_dir.glob("*_trades_*.parquet"))
                depth_files = list(hour_dir.glob("*_depth_*.parquet"))
                
                if trades_files or depth_files:
                    symbols_to_process.append({
                        'symbol': symbol_dir.name,
                        'hour_dir': hour_dir,
                        'date': date,
                        'hour': hour,
                        'trades_files': trades_files,
                        'depth_files': depth_files
                    })
            
            if not symbols_to_process:
                self.logger.info(f"No data to flush for {current_hour}")
                return {'status': 'success', 'symbols_processed': 0, 'hour': current_hour}
            
            self.logger.info(f"📦 Flushing {len(symbols_to_process)} symbols for {current_hour}")
            
            # ============================================================
            # ЭТАП 2: Обрабатываем ПОСЛЕДОВАТЕЛЬНО
            # ============================================================
            
            success_count = 0
            results = []
            
            for symbol_info in symbols_to_process:
                symbol = symbol_info['symbol']
                
                try:
                    self.logger.info(f"🔄 Flushing {symbol}...")
                    
                    symbol_result = {
                        'symbol': symbol,
                        'trades': False,
                        'depth': False
                    }
                    
                    # Trades
                    if symbol_info['trades_files']:
                        success = await self._flush_data_type(
                            symbol=symbol,
                            date=symbol_info['date'],
                            hour=symbol_info['hour'],
                            data_type="trades",
                            source_files=symbol_info['trades_files'],
                            hour_dir=symbol_info['hour_dir']
                        )
                        symbol_result['trades'] = success
                        gc.collect()
                    
                    # Depth
                    if symbol_info['depth_files']:
                        success = await self._flush_data_type(
                            symbol=symbol,
                            date=symbol_info['date'],
                            hour=symbol_info['hour'],
                            data_type="depth",
                            source_files=symbol_info['depth_files'],
                            hour_dir=symbol_info['hour_dir']
                        )
                        symbol_result['depth'] = success
                        gc.collect()
                    
                    if symbol_result['trades'] or symbol_result['depth']:
                        success_count += 1
                    
                    results.append(symbol_result)
                    
                    # Даём event loop обработать записи
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error flushing {symbol}: {e}")
                    results.append({
                        'symbol': symbol,
                        'error': str(e)
                    })
                
                finally:
                    gc.collect()
            
            self.logger.info(
                f"✅ Flush completed: {success_count}/{len(symbols_to_process)} symbols"
            )
            
            return {
                'status': 'success',
                'hour': current_hour,
                'symbols_processed': success_count,
                'total_symbols': len(symbols_to_process),
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"❌ Flush failed: {e}", exc_info=True)
            return {
                'status': 'error',
                'hour': current_hour,
                'error': str(e)
            }
        
        finally:
            self._processing = False
            gc.collect()
            self.logger.info("🧹 Memory cleanup after flush")


    async def _flush_data_type(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        source_files: List[Path],
        hour_dir: Path
    ) -> bool:
        """
        Flush одного типа данных (snapshot).
        
        В отличие от _process_data_type:
        - НЕ удаляет source файлы
        - Удаляет только временный merged файл после upload
        """
        merged_file = None
        upload_file = None
        
        try:
            # ============================================================
            # 1. MERGE
            # ============================================================
            
            loop = asyncio.get_event_loop()
            merged_file = await loop.run_in_executor(
                self._executor,
                self._merge_files_sync,
                source_files,
                hour_dir,
                data_type,
                symbol
            )
            
            if not merged_file:
                self.logger.warning(f"No data merged for {symbol}/{data_type}")
                return False
            
            upload_file = merged_file
            
            # ============================================================
            # 2. COMPRESS (опционально)
            # ============================================================
            
            if self.compress_before_upload:
                compressed_file = await loop.run_in_executor(
                    self._executor,
                    self._compress_gzip_sync,
                    merged_file
                )
                
                if compressed_file:
                    upload_file = compressed_file
                    # Удаляем несжатый merged
                    try:
                        merged_file.unlink()
                    except Exception:
                        pass
            
            # ============================================================
            # 3. UPLOAD
            # ============================================================
            
            if self.cloud:
                success = await self.cloud.async_upload_file(
                    local_path=upload_file,
                    exchange="binance",
                    market=self.market_type,
                    symbol=symbol,
                    date=date,
                    hour=hour,
                    data_type=data_type,
                    is_compressed=upload_file.suffix == '.gz'
                )
                
                if not success:
                    self.logger.error(f"❌ Upload failed for {symbol}/{data_type}")
                    return False
            
            # ============================================================
            # 4. Удаляем ТОЛЬКО merged/upload файл
            #    Source файлы ОСТАВЛЯЕМ для финальной сборки!
            # ============================================================
            
            try:
                if upload_file.exists():
                    upload_file.unlink()
            except Exception:
                pass
            
            self.logger.info(f"✅ Flushed {symbol}/{data_type} (source files kept)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error flushing {symbol}/{data_type}: {e}", exc_info=True)
            return False
        
        finally:
            # Очистка временных файлов при ошибке
            for f in [merged_file, upload_file]:
                if f and f.exists() and 'merged_' in f.name:
                    try:
                        f.unlink()
                    except Exception:
                        pass
            
            merged_file = None
            upload_file = None
    def _get_final_schema(self, data_type: str) -> pa.Schema:
        """
        Возвращает ФИНАЛЬНУЮ схему для merged файлов.
        БЕЗ pool_id, connection_id, is_buyer_maker
        """
        if data_type == 'trades':
            return pa.schema([
                ('timestamp_ms', pa.int64()),
                ('trade_id', pa.int64()),
                ('price', pa.float64()),
                ('qty', pa.float64()),  # Signed: + buy, - sell
            ])
        
        elif data_type == 'depth':
            return pa.schema([
                ('timestamp_ms', pa.int64()),
                ('update_id', pa.int64()),
                ('bids', pa.list_(pa.list_(pa.float64(), 2))),
                ('asks', pa.list_(pa.list_(pa.float64(), 2)))
            ])
        
        else:
            raise ValueError(f"Unknown data_type: {data_type}")
    
    async def compress_file_gzip(self, file_path: Path) -> Optional[Path]:
        """Асинхронное GZIP сжатие"""
        if not file_path.exists():
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._compress_file_gzip_sync,
            file_path
        )
    
    def _compress_file_gzip_sync(self, file_path: Path) -> Optional[Path]:
        """Синхронное GZIP сжатие"""
        gz_path = file_path.with_suffix('.parquet.gz')
        temp_gz_path = file_path.with_suffix('.parquet.gz.tmp')
        
        try:
            original_size = file_path.stat().st_size
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(temp_gz_path, 'wb', compresslevel=CONFIG.storage.GZIP_LEVEL) as f_out:
                    shutil.copyfileobj(f_in, f_out, length=1024*1024)
            
            temp_gz_path.replace(gz_path)
            
            compressed_size = gz_path.stat().st_size
            ratio = (1 - compressed_size / original_size) * 100
            
            self.logger.info(
                f"🗜️ GZIP: {file_path.name} → {compressed_size / 1024:.1f} KB "
                f"(saved {ratio:.1f}%)"
            )
            
            return gz_path
            
        except Exception as e:
            self.logger.error(f"❌ GZIP compression failed: {e}")
            if temp_gz_path.exists():
                temp_gz_path.unlink()
            if gz_path.exists():
                gz_path.unlink()
            return None
    
    def get_local_files_stats(self) -> dict:
        """Возвращает статистику локальных файлов"""
        if not self.data_dir.exists():
            return {}
        
        current_hour_key = self._get_current_hour()
        
        stats = {
            'total_directories': 0,
            'total_files': 0,
            'total_size_mb': 0,
            'by_symbol': {},
            'current_hour_dirs': 0,
            'past_hour_dirs': 0
        }
        
        for symbol_dir in self.data_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            
            symbol = symbol_dir.name
            stats['by_symbol'][symbol] = {
                'hour_directories': 0,
                'total_files': 0,
                'size_mb': 0
            }
            
            for hour_dir in symbol_dir.iterdir():
                if not hour_dir.is_dir():
                    continue
                
                stats['total_directories'] += 1
                stats['by_symbol'][symbol]['hour_directories'] += 1
                
                if current_hour_key in hour_dir.name:
                    stats['current_hour_dirs'] += 1
                else:
                    stats['past_hour_dirs'] += 1
                
                for file in hour_dir.glob("*.parquet*"):
                    stats['total_files'] += 1
                    stats['by_symbol'][symbol]['total_files'] += 1
                    
                    size_mb = file.stat().st_size / (1024 * 1024)
                    stats['total_size_mb'] += size_mb
                    stats['by_symbol'][symbol]['size_mb'] += size_mb
        
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        
        for symbol in stats['by_symbol']:
            stats['by_symbol'][symbol]['size_mb'] = round(
                stats['by_symbol'][symbol]['size_mb'], 2
            )
        
        return stats