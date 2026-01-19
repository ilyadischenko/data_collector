import asyncio
import logging
import os
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timezone
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
        parquet_compression_level: int = 9
    ):
        self.cloud = CloudStorage()
        self.data_dir = Path(data_dir)
        self.exchange = exchange
        self.is_running = False
        self.logger = logging.getLogger(f"CloudManager.{exchange}")
        
        self.compress_before_upload = compress_before_upload
        self.parquet_compression_level = parquet_compression_level
        
        self.last_checked_hour = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        
        # ✅ Lock для предотвращения race condition при смене часа
        self._hour_processing_lock = asyncio.Lock()
        
        # ✅ Thread pool для CPU-интенсивных операций
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=4)

    async def run(self):
        """
        Фоновая задача. При смене часа:
        1. Объединяет Parquet файлы от разных соединений
        2. Сжимает объединенные файлы (опционально)
        3. Загружает в облако
        4. Удаляет исходные файлы
        """
        self.is_running = True
        self.logger.info(
            f"☁️ Cloud Manager started. Exchange: {self.exchange}, "
            f"Compression: Parquet ZSTD-{self.parquet_compression_level}"
            f"{', + GZIP' if self.compress_before_upload else ''}"
        )
        
        await asyncio.sleep(5)
        
        while self.is_running:
            try:
                await asyncio.sleep(10)
                
                current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
                
                if current_hour_key != self.last_checked_hour:
                    self.logger.info(f"⏰ Hour changed: {self.last_checked_hour} -> {current_hour_key}")
                    
                    # ✅ Защита от race condition
                    async with self._hour_processing_lock:
                        await self.process_past_hour_directories(current_hour_key)
                    
                    self.last_checked_hour = current_hour_key
                    
            except Exception as e:
                self.logger.error(f"❌ Manager loop error: {e}")

    def stop(self):
        """Остановка менеджера."""
        self.is_running = False
        self._executor.shutdown(wait=True)
        self.logger.info("🛑 Cloud Manager stopped")

    async def merge_parquet_files(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str
    ) -> Optional[Path]:
        """
        ✅ ИСПРАВЛЕНО: Асинхронное объединение файлов через executor.
        """
        hour_dir = self.data_dir / self.exchange / symbol / f"{date}_{hour}"
        
        if not hour_dir.exists():
            return None
        
        source_files = list(hour_dir.glob(f"conn_*_{data_type}.parquet"))
        
        if not source_files:
            self.logger.debug(f"No files for {symbol}/{date}_{hour}/{data_type}")
            return None
        
        self.logger.info(f"🔗 Merging {len(source_files)} files: {symbol}/{data_type}")
        
        # ✅ Выполняем в executor (не блокирует event loop)
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
        ✅ УЛУЧШЕНО: Синхронное объединение с умной дедупликацией.
        
        Логика дедупликации:
        - Для trades: по trade_id (уникальный ID сделки)
        - Для bookticker: по update_id + timestamp_ms (на случай переподключения)
        - Для depth: по update_id + timestamp_ms
        
        keep='last' используется для сохранения самой свежей версии данных
        при возможных переподключениях соединений.
        """
        try:
            tables = []
            total_rows = 0
            
            # Читаем все файлы
            for file in source_files:
                try:
                    table = pq.read_table(file)
                    tables.append(table)
                    total_rows += len(table)
                except Exception as e:
                    self.logger.error(f"Failed to read {file.name}: {e}")
            
            if not tables:
                return None
            
            # Объединяем все таблицы
            merged_table = pa.concat_tables(tables)
            df = merged_table.to_pandas()
            original_count = len(df)
            
            # ✅ ДЕДУПЛИКАЦИЯ в зависимости от типа данных
            if data_type == "trades":
                # Сортируем по timestamp для гарантии правильного порядка
                df = df.sort_values('timestamp_ms')
                
                # Дедупликация по trade_id
                # keep='last' сохраняет последнюю версию на случай повторной передачи
                df = df.drop_duplicates(subset=['trade_id'], keep='last')
                
            elif data_type == "bookticker":
                # Для BBO важен порядок обновлений
                df = df.sort_values(['update_id', 'timestamp_ms'])
                
                # Дедупликация по update_id
                # Если update_id одинаковый, берем запись с более поздним timestamp
                df = df.drop_duplicates(subset=['update_id'], keep='last')
                
            elif data_type == "depth":
                # Для стакана также важен порядок
                df = df.sort_values(['update_id', 'timestamp_ms'])
                
                # Дедупликация по update_id
                df = df.drop_duplicates(subset=['update_id'], keep='last')
            
            duplicates_removed = original_count - len(df)
            
            if duplicates_removed > 0:
                self.logger.info(
                    f"   Deduplicated {data_type}: {duplicates_removed:,} duplicates removed "
                    f"({duplicates_removed/original_count*100:.1f}%), "
                    f"{len(df):,} unique records kept"
                )
            
            # Финальная сортировка перед сохранением
            df = df.sort_values('timestamp_ms').reset_index(drop=True)
            
            # Конвертируем обратно в Arrow Table
            merged_table = pa.Table.from_pandas(df, preserve_index=False)
            
            # ✅ ТРАНСФОРМАЦИЯ TRADES ТОЛЬКО ДЛЯ BINANCE (после дедупликации)
            if data_type == "trades" and self.exchange == "binance":
                merged_table = self._transform_binance_trades(merged_table)
            
            # ✅ ЗАПИСЬ С АТОМАРНОЙ ЗАМЕНОЙ
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
            
            # Атомарная замена
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


    def _validate_merged_data(self, df, data_type: str) -> bool:
        """
        ✅ ДОПОЛНИТЕЛЬНО: Валидация объединенных данных.
        
        Проверяет:
        - Отсутствие дубликатов по ключевым полям
        - Монотонность timestamp_ms
        - Корректность диапазонов значений
        """
        try:
            if data_type == "trades":
                # Проверка уникальности trade_id
                if df['trade_id'].duplicated().any():
                    duplicates = df['trade_id'].duplicated().sum()
                    self.logger.error(f"❌ Found {duplicates} duplicate trade_ids!")
                    return False
                    
            elif data_type in ["bookticker", "depth"]:
                # Проверка уникальности update_id
                if df['update_id'].duplicated().any():
                    duplicates = df['update_id'].duplicated().sum()
                    self.logger.error(f"❌ Found {duplicates} duplicate update_ids!")
                    return False
            
            # Проверка монотонности timestamp
            if not df['timestamp_ms'].is_monotonic_increasing:
                self.logger.warning(f"⚠️ Timestamps are not monotonic for {data_type}")
                # Это warning, не error, т.к. разные соединения могут получать данные с небольшими задержками
            
            # Проверка на пустые значения в ключевых полях
            if df[['timestamp_ms']].isnull().any().any():
                self.logger.error(f"❌ Found null timestamps in {data_type}!")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False
    def _transform_binance_trades(self, table: pa.Table) -> pa.Table:
        """
        ✅ НОВОЕ: Трансформирует таблицу trades для Binance:
        - Удаляет поле is_buyer_maker
        - Делает qty отрицательным для продаж (is_buyer_maker=True)
        
        Binance логика:
        - is_buyer_maker=True → sell order (маркет продажа) → qty < 0
        - is_buyer_maker=False → buy order (маркет покупка) → qty > 0
        
        Gate.io и другие биржи уже присылают signed qty, поэтому для них
        эта трансформация не нужна.
        """
        try:
            # Проверяем наличие поля is_buyer_maker
            if 'is_buyer_maker' not in table.column_names:
                self.logger.warning("Field 'is_buyer_maker' not found, skipping transformation")
                return table
            
            # Получаем колонки
            qty = table['qty']
            is_buyer_maker = table['is_buyer_maker']
            
            # ✅ Создаем signed_qty: если продажа (True), то -qty, иначе +qty
            signed_qty = pc.if_else(
                is_buyer_maker,
                pc.negate(qty),  # Продажа → отрицательный объем
                qty              # Покупка → положительный объем
            )
            
            # ✅ Создаем новую схему БЕЗ is_buyer_maker
            new_fields = []
            for field in table.schema:
                if field.name != 'is_buyer_maker':
                    new_fields.append(field)
            new_schema = pa.schema(new_fields)
            
            # ✅ Собираем колонки для новой таблицы
            columns = []
            for field in new_schema:
                if field.name == 'qty':
                    columns.append(signed_qty)
                else:
                    columns.append(table[field.name])
            
            transformed_table = pa.Table.from_arrays(columns, schema=new_schema)
            
            # Статистика
            sell_count = pc.sum(is_buyer_maker).as_py()
            buy_count = len(table) - sell_count
            
            self.logger.info(
                f"Transformed Binance trades: {len(table)} rows "
                f"(buys: {buy_count}, sells: {sell_count})"
            )
            
            return transformed_table
            
        except Exception as e:
            self.logger.error(f"❌ Binance trade transformation failed: {e}")
            # В случае ошибки возвращаем оригинал
            return table

    async def compress_file_gzip(self, file_path: Path) -> Optional[Path]:
        """
        ✅ ИСПРАВЛЕНО: Асинхронное GZIP сжатие через executor.
        """
        if not file_path.exists():
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._compress_file_gzip_sync,
            file_path
        )
    
    def _compress_file_gzip_sync(self, file_path: Path) -> Optional[Path]:
        """Синхронное GZIP сжатие (выполняется в отдельном потоке)."""
        gz_path = file_path.with_suffix('.parquet.gz')
        temp_gz_path = file_path.with_suffix('.parquet.gz.tmp')
        
        try:
            original_size = file_path.stat().st_size
            
            # ✅ Сжимаем во временный файл
            with open(file_path, 'rb') as f_in:
                with gzip.open(temp_gz_path, 'wb', compresslevel=9) as f_out:
                    shutil.copyfileobj(f_in, f_out, length=1024*1024)
            
            # ✅ Атомарная замена
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
            # ✅ Удаляем поврежденные файлы
            if temp_gz_path.exists():
                temp_gz_path.unlink()
            if gz_path.exists():
                gz_path.unlink()
            return None

    async def process_past_hour_directories(self, current_hour_key: str):
        """
        Обрабатывает все директории прошлых часов:
        1. Объединяет файлы от разных соединений
        2. Сжимает (опционально)
        3. Загружает в облако
        4. Удаляет исходные файлы
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
            
            for hour_dir in symbol_dir.iterdir():
                if not hour_dir.is_dir():
                    continue
                
                # ✅ Пропускаем текущий час
                if current_hour_key in hour_dir.name:
                    continue
                
                try:
                    date_hour = hour_dir.name
                    date, hour = date_hour.split('_')
                    
                    self.logger.info(f"📦 Processing: {symbol}/{date_hour}")
                    
                    await self._process_hour_directory(symbol, date, hour, hour_dir)
                    
                except Exception as e:
                    self.logger.error(f"Error processing {hour_dir.name}: {e}")

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
            
            # ✅ Удаляем директорию после успешной загрузки
            try:
                for file in hour_dir.glob("*"):
                    file.unlink()
                hour_dir.rmdir()
                self.logger.info(f"🗑️ Deleted: {hour_dir}")
            except Exception as e:
                self.logger.error(f"Failed to delete {hour_dir}: {e}")

    async def _merge_compress_and_upload(
        self,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        hour_dir: Path
    ) -> bool:
        """✅ ИСПРАВЛЕНО: Правильная обработка файлов при сжатии."""
        try:
            # 1. Объединяем файлы (с трансформацией для Binance trades)
            merged_file = await self.merge_parquet_files(symbol, date, hour, data_type)
            
            if not merged_file:
                return False
            
            # 2. Определяем файл для загрузки
            upload_file = merged_file
            
            # 3. Дополнительное сжатие GZIP (опционально)
            if self.compress_before_upload:
                compressed_file = await self.compress_file_gzip(merged_file)
                
                if compressed_file:
                    upload_file = compressed_file
                    # ✅ Удаляем несжатый merged файл, т.к. загружаем .gz
                    try:
                        merged_file.unlink()
                        self.logger.debug(f"Deleted uncompressed: {merged_file.name}")
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {merged_file.name}: {e}")
                else:
                    # Сжатие не удалось - загружаем несжатый файл
                    self.logger.warning(f"GZIP failed, uploading uncompressed: {merged_file.name}")
            
            # 4. Загружаем в облако
            success = await self.cloud.async_upload_file(
                local_path=upload_file,
                exchange=self.exchange,
                symbol=symbol,
                date=date,
                hour=hour,
                data_type=data_type,
                is_compressed=self.compress_before_upload and upload_file.suffix == '.gz'
            )
            
            # ✅ Если загрузка успешна - файл будет удален вместе с директорией
            # Если нет - файл останется для повторной попытки
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in merge_compress_upload for {symbol}/{data_type}: {e}")
            return False

    async def force_upload_current(self):
        """
        Принудительная загрузка текущего часа.
        Объединяет, сжимает и загружает БЕЗ удаления исходников.
        """
        current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
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
                # Объединяем файлы (с трансформацией для Binance trades)
                merged_file = await self.merge_parquet_files(symbol, date, hour, data_type)
                
                if not merged_file:
                    continue
                
                upload_file = merged_file
                
                # Сжимаем (опционально)
                if self.compress_before_upload:
                    compressed_file = await self.compress_file_gzip(merged_file)
                    if compressed_file:
                        upload_file = compressed_file
                
                # Загружаем (НЕ удаляем исходники)
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
                    # ✅ Удаляем merged/compressed файлы после успешной загрузки
                    try:
                        upload_file.unlink()
                        # Если загружали .gz, удаляем и .parquet
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
        """
        Загрузка всех файлов конкретного символа.
        """
        symbol_dir = self.data_dir / self.exchange / symbol
        
        if not symbol_dir.exists():
            return 0
        
        success_count = 0
        current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        
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
                    
                    # Сжимаем
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
                
                # ✅ Удаляем директорию если требуется И это не текущий час
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
        """
        Статистика по локальным файлам.
        """
        exchange_dir = self.data_dir / self.exchange
        
        if not exchange_dir.exists():
            return {}
        
        current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        
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