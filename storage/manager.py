import asyncio
import logging
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

try:
    from storage.cloud import CloudStorage
except ImportError:
    print("❌ Ошибка: файл cloud.py не найден.")
    exit(1)


class CloudManager:
    def __init__(
        self, 
        data_dir: str = "collected_data", 
        exchange: str = "binance",
        compress_before_upload: bool = True,
        parquet_compression_level: int = 9,
        hour_change_delay: int = 90  # Задержка в секундах перед обработкой часа
    ):
        self.cloud = CloudStorage()
        self.data_dir = Path(data_dir)
        self.exchange = exchange
        self.is_running = False
        self.logger = logging.getLogger(f"CloudManager.{exchange}")
        
        self.compress_before_upload = compress_before_upload
        self.parquet_compression_level = parquet_compression_level
        self.hour_change_delay = hour_change_delay
        
        self.last_processed_hour = self._get_previous_hour()
        
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=4)

    def _get_previous_hour(self) -> str:
        """Возвращает предыдущий час в формате YYYYMMDD_HH."""
        prev = datetime.now(timezone.utc) - timedelta(hours=1)
        return prev.strftime("%Y%m%d_%H")

    def _get_current_hour(self) -> str:
        """Возвращает текущий час в формате YYYYMMDD_HH."""
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    async def run(self):
        """Фоновая задача для отслеживания смены часа."""
        self.is_running = True
        self.logger.info(
            f"☁️ Cloud Manager started. Exchange: {self.exchange}, "
            f"Compression: Parquet ZSTD-{self.parquet_compression_level}"
            f"{', + GZIP' if self.compress_before_upload else ''}, "
            f"Hour change delay: {self.hour_change_delay}s"
        )
        
        await asyncio.sleep(5)
        
        while self.is_running:
            try:
                await asyncio.sleep(30)
                
                current_hour = self._get_current_hour()
                
                # Если час сменился
                if current_hour != self.last_processed_hour:
                    self.logger.info(
                        f"⏰ Hour changed: {self.last_processed_hour} -> {current_hour}. "
                        f"Waiting {self.hour_change_delay}s before processing..."
                    )
                    
                    # ЖДЕМ чтобы Writer успел дописать все файлы
                    await asyncio.sleep(self.hour_change_delay)
                    
                    # Обрабатываем ПРЕДЫДУЩИЙ час
                    prev_hour = self.last_processed_hour
                    await self.process_hour(prev_hour)
                    
                    self.last_processed_hour = current_hour
                    
            except Exception as e:
                self.logger.error(f"❌ Manager loop error: {e}")

    def stop(self):
        """Остановка менеджера."""
        self.is_running = False
        self._executor.shutdown(wait=True)
        self.logger.info("🛑 Cloud Manager stopped")

    async def process_hour(self, hour_key: str):
        """
        Обрабатывает конкретный час:
        1. Объединяет файлы conn_*.parquet
        2. Сжимает (опционально)
        3. Загружает в облако
        4. Удаляет локальные файлы включая директорию часа
        """
        if not self.data_dir.exists():
            return
        
        exchange_dir = self.data_dir / self.exchange
        if not exchange_dir.exists():
            return
        
        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            
            symbol = symbol_dir.name
            hour_dir = symbol_dir / hour_key
            
            if not hour_dir.exists():
                continue
            
            try:
                date, hour = hour_key.split('_')
                
                self.logger.info(f"📦 Processing: {symbol}/{hour_key}")
                
                await self._process_hour_directory(symbol, date, hour, hour_dir)
                
            except Exception as e:
                self.logger.error(f"Error processing {symbol}/{hour_key}: {e}")

    async def _process_hour_directory(
        self,
        symbol: str,
        date: str,
        hour: str,
        hour_dir: Path
    ):
        """Обрабатывает одну часовую директорию."""
        tasks = []
        
        for data_type in ["trades", "bookticker", "depth"]:
            tasks.append(
                self._merge_compress_and_upload(symbol, date, hour, data_type, hour_dir)
            )
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        
        if success_count > 0:
            self.logger.info(f"✅ Completed {symbol}/{date}_{hour}: {success_count}/3 uploaded")
            
            # Удаляем директорию после успешной загрузки
            try:
                for file in hour_dir.glob("*"):
                    file.unlink()
                hour_dir.rmdir()
                self.logger.info(f"🗑️ Deleted: {hour_dir}")
            except Exception as e:
                self.logger.error(f"Failed to delete {hour_dir}: {e}")

    async def merge_parquet_files(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str
    ) -> Optional[Path]:
        """Асинхронное объединение файлов через executor."""
        hour_dir = self.data_dir / self.exchange / symbol / f"{date}_{hour}"
        
        if not hour_dir.exists():
            return None
        
        source_files = list(hour_dir.glob(f"conn_*_{data_type}_*.parquet"))
        
        if not source_files:
            self.logger.debug(f"No files for {symbol}/{date}_{hour}/{data_type}")
            return None
        
        self.logger.info(f"🔗 Merging {len(source_files)} files: {symbol}/{data_type}")
        
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
        """Синхронное объединение с дедупликацией и нормализацией."""
        try:
            tables = []
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
            
            merged_table = pa.concat_tables(tables)
            df = merged_table.to_pandas()
            original_count = len(df)
            
            # Дедупликация
            if data_type == "trades":
                df = df.sort_values('timestamp_ms')
                df = df.drop_duplicates(subset=['trade_id'], keep='last')
                
            elif data_type == "bookticker":
                df = df.sort_values(['update_id', 'timestamp_ms'])
                df = df.drop_duplicates(subset=['update_id'], keep='last')
                
            elif data_type == "depth":
                df = df.sort_values(['update_id', 'timestamp_ms'])
                df = df.drop_duplicates(subset=['update_id'], keep='last')
            
            duplicates_removed = original_count - len(df)
            
            if duplicates_removed > 0:
                self.logger.info(
                    f"   Deduplicated {data_type}: {duplicates_removed:,} duplicates removed "
                    f"({duplicates_removed/original_count*100:.1f}%), "
                    f"{len(df):,} unique records kept"
                )
            
            df = df.sort_values('timestamp_ms').reset_index(drop=True)
            
            # УДАЛЯЕМ connection_id из всех типов данных
            if 'connection_id' in df.columns:
                df = df.drop(columns=['connection_id'])
                self.logger.debug(f"Removed connection_id from {data_type}")
            
            # Трансформация для приведения к единому формату
            if data_type == "trades":
                df = self._normalize_trades(df, symbol)
            
            merged_table = pa.Table.from_pandas(df, preserve_index=False)
            
            # Получаем финальную схему (без connection_id)
            final_schema = self._get_final_schema(data_type)
            
            # Приводим таблицу к финальной схеме
            merged_table = merged_table.cast(final_schema)
            
            # Запись с атомарной заменой
            merged_path = hour_dir / f"merged_{data_type}.parquet"
            temp_path = hour_dir / f"merged_{data_type}.parquet.tmp"
            
            pq.write_table(
                merged_table,
                temp_path,
                compression='zstd',
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
                f"✅ Merged {symbol}/{data_type}: {len(tables)} files → "
                f"{final_rows:,} unique rows (from {total_rows:,}), "
                f"{file_size / 1024:.1f} KB ({compression_ratio:.2f} bytes/row)"
            )
            
            return merged_path
            
        except Exception as e:
            self.logger.error(f"❌ Merge failed for {symbol}/{data_type}: {e}")
            temp_path = hour_dir / f"merged_{data_type}.parquet.tmp"
            if temp_path.exists():
                temp_path.unlink()
            return None

    def _normalize_trades(self, df, symbol: str):
        """
        Нормализует trades к единому формату:
        - qty: знаковый float (+ buy, - sell)
        - удаляет is_buyer_maker для Binance
        """
        if self.exchange == "binance":
            if 'is_buyer_maker' in df.columns:
                # is_buyer_maker=True означает taker купил → qty отрицательный
                df.loc[df['is_buyer_maker'] == True, 'qty'] = -df.loc[df['is_buyer_maker'] == True, 'qty']
                df = df.drop(columns=['is_buyer_maker'])
                
                sell_count = (df['qty'] < 0).sum()
                buy_count = (df['qty'] > 0).sum()
                
                self.logger.info(
                    f"   Normalized Binance trades: {len(df)} rows "
                    f"(buys: {buy_count}, sells: {sell_count})"
                )
        
        # Gate.io уже приходит со знаковым qty - ничего не делаем
        
        return df

    def _get_final_schema(self, data_type: str) -> pa.Schema:
        """
        Возвращает ФИНАЛЬНУЮ схему для merged файлов (БЕЗ connection_id).
        Единая для всех бирж.
        """
        if data_type == 'trades':
            return pa.schema([
                ('timestamp_ms', pa.int64()),
                ('trade_id', pa.int64()),
                ('price', pa.float64()),
                ('qty', pa.float64()),  # Знаковый: + buy, - sell
            ])
        
        elif data_type == 'bookticker':
            return pa.schema([
                ('timestamp_ms', pa.int64()),
                ('update_id', pa.int64()),
                ('best_bid_price', pa.float64()),
                ('best_bid_qty', pa.float64()),
                ('best_ask_price', pa.float64()),
                ('best_ask_qty', pa.float64())
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
        """Асинхронное GZIP сжатие через executor."""
        if not file_path.exists():
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._compress_file_gzip_sync,
            file_path
        )
    
    def _compress_file_gzip_sync(self, file_path: Path) -> Optional[Path]:
        """Синхронное GZIP сжатие."""
        gz_path = file_path.with_suffix('.parquet.gz')
        temp_gz_path = file_path.with_suffix('.parquet.gz.tmp')
        
        try:
            original_size = file_path.stat().st_size
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(temp_gz_path, 'wb', compresslevel=9) as f_out:
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

    async def _merge_compress_and_upload(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        hour_dir: Path
    ) -> bool:
        """Объединяет, сжимает и загружает файлы."""
        try:
            # 1. Объединяем файлы conn_*.parquet
            merged_file = await self.merge_parquet_files(symbol, date, hour, data_type)
            
            if not merged_file:
                return False
            
            upload_file = merged_file
            
            # 2. Дополнительное сжатие GZIP
            if self.compress_before_upload:
                compressed_file = await self.compress_file_gzip(merged_file)
                
                if compressed_file:
                    upload_file = compressed_file
                    try:
                        merged_file.unlink()
                        self.logger.debug(f"Deleted uncompressed: {merged_file.name}")
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {merged_file.name}: {e}")
                else:
                    self.logger.warning(f"GZIP failed, uploading uncompressed: {merged_file.name}")
            
            # 3. Загружаем в облако
            success = await self.cloud.async_upload_file(
                local_path=upload_file,
                exchange=self.exchange,
                symbol=symbol,
                date=date,
                hour=hour,
                data_type=data_type,
                is_compressed=self.compress_before_upload and upload_file.suffix == '.gz'
            )
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in merge_compress_upload for {symbol}/{data_type}: {e}")
            return False

    async def force_upload_current(self):
        """
        Принудительная загрузка текущего часа.
        НЕ удаляет исходные файлы conn_*.parquet
        """
        current_hour_key = self._get_current_hour()
        date, hour = current_hour_key.split('_')
        
        exchange_dir = self.data_dir / self.exchange
        if not exchange_dir.exists():
            return 0
        
        success_count = 0
        
        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            
            symbol = symbol_dir.name
            hour_dir = symbol_dir / current_hour_key
            
            if not hour_dir.exists():
                continue
            
            self.logger.info(f"⚡ Force upload: {symbol}/{current_hour_key}")
            
            for data_type in ["trades", "bookticker", "depth"]:
                merged_file = await self.merge_parquet_files(symbol, date, hour, data_type)
                
                if not merged_file:
                    continue
                
                upload_file = merged_file
                
                if self.compress_before_upload:
                    compressed_file = await self.compress_file_gzip(merged_file)
                    if compressed_file:
                        upload_file = compressed_file
                
                success = await self.cloud.async_upload_file(
                    local_path=upload_file,
                    exchange=self.exchange,
                    symbol=symbol,
                    date=date,
                    hour=hour,
                    data_type=data_type,
                    is_compressed=self.compress_before_upload and upload_file.suffix == '.gz'
                )
                
                if success:
                    success_count += 1
                    # Удаляем только merged/compressed файлы
                    try:
                        upload_file.unlink()
                        if upload_file.suffix == '.gz' and merged_file.exists():
                            merged_file.unlink()
                    except Exception as e:
                        self.logger.warning(f"Failed to cleanup {upload_file.name}: {e}")
        
        self.logger.info(f"📊 Force upload complete: {success_count} files")
        return success_count

    async def force_upload_symbol(
        self,
        symbol: str,
        delete_after: bool = False
    ) -> int:
        """Загрузка всех файлов конкретного символа."""
        symbol_dir = self.data_dir / self.exchange / symbol
        
        if not symbol_dir.exists():
            return 0
        
        success_count = 0
        current_hour_key = self._get_current_hour()
        
        for hour_dir in symbol_dir.iterdir():
            if not hour_dir.is_dir():
                continue
            
            try:
                date_hour = hour_dir.name
                date, hour = date_hour.split('_')
                
                for data_type in ["trades", "bookticker", "depth"]:
                    merged_file = await self.merge_parquet_files(
                        symbol, date, hour, data_type
                    )
                    
                    if not merged_file:
                        continue
                    
                    upload_file = merged_file
                    
                    if self.compress_before_upload:
                        compressed_file = await self.compress_file_gzip(merged_file)
                        if compressed_file:
                            upload_file = compressed_file
                    
                    success = await self.cloud.async_upload_file(
                        local_path=upload_file,
                        exchange=self.exchange,
                        symbol=symbol,
                        date=date,
                        hour=hour,
                        data_type=data_type,
                        is_compressed=self.compress_before_upload and upload_file.suffix == '.gz'
                    )
                    
                    if success:
                        success_count += 1
                
                # Удаляем директорию если требуется И это не текущий час
                if delete_after and date_hour != current_hour_key:
                    try:
                        for file in hour_dir.glob("*"):
                            file.unlink()
                        hour_dir.rmdir()
                        self.logger.info(f"🗑️ Deleted: {hour_dir}")
                    except Exception as e:
                        self.logger.error(f"Failed to delete {hour_dir}: {e}")
                    
            except Exception as e:
                self.logger.error(f"Error processing {hour_dir.name}: {e}")
        
        return success_count

    def get_local_files_stats(self) -> dict:
        """Статистика по локальным файлам."""
        exchange_dir = self.data_dir / self.exchange
        
        if not exchange_dir.exists():
            return {}
        
        current_hour_key = self._get_current_hour()
        
        stats = {
            'total_directories': 0,
            'total_files': 0,
            'total_size_mb': 0,
            'by_symbol': {},
            'current_hour_dirs': 0,
            'past_hour_dirs': 0,
            'compression_stats': {
                'parquet_files': 0,
                'gzip_files': 0,
                'total_parquet_size_mb': 0,
                'total_gzip_size_mb': 0
            }
        }
        
        for symbol_dir in exchange_dir.iterdir():
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
                    
                    if file.suffix == '.gz':
                        stats['compression_stats']['gzip_files'] += 1
                        stats['compression_stats']['total_gzip_size_mb'] += size_mb
                    elif file.suffix == '.parquet':
                        stats['compression_stats']['parquet_files'] += 1
                        stats['compression_stats']['total_parquet_size_mb'] += size_mb
        
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        stats['compression_stats']['total_parquet_size_mb'] = round(
            stats['compression_stats']['total_parquet_size_mb'], 2
        )
        stats['compression_stats']['total_gzip_size_mb'] = round(
            stats['compression_stats']['total_gzip_size_mb'], 2
        )
        
        for symbol in stats['by_symbol']:
            stats['by_symbol'][symbol]['size_mb'] = round(
                stats['by_symbol'][symbol]['size_mb'], 2
            )
        
        return stats