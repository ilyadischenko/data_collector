import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import List

# Подключаем наш драйвер
try:
    from storage.cloud import CloudStorage
except ImportError:
    print("❌ Ошибка: файл cloud_storage.py не найден.")
    exit(1)

class CloudManager:
    def __init__(self, data_dir: str = "collected_data"):
        self.cloud = CloudStorage()
        self.data_dir = Path(data_dir)
        self.is_running = False
        self.logger = logging.getLogger("CloudManager")
        
        # Храним "текущий" час (например, "20250115_14"), чтобы заметить его смену
        self.last_checked_hour = datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    async def run(self):
        """
        Фоновая задача. Просыпается раз в 10 секунд и проверяет время.
        """
        self.is_running = True
        self.logger.info(f"☁️ Cloud Manager started. Watching dir: {self.data_dir.absolute()}")
        
        # Даем системе немного времени на старт
        await asyncio.sleep(5)
        
        while self.is_running:
            try:
                await asyncio.sleep(10)
                
                # Получаем текущий час
                current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
                
                # Если час изменился (было 14:59 -> стало 15:00)
                if current_hour_key != self.last_checked_hour:
                    self.logger.info(f"⏰ Hour changed: {self.last_checked_hour} -> {current_hour_key}")
                    
                    # Запускаем обработку файлов ПРОШЛЫХ часов (загрузить и удалить)
                    await self.process_past_files(current_hour_key)
                    
                    self.last_checked_hour = current_hour_key
                    
            except Exception as e:
                self.logger.error(f"Manager loop error: {e}")

    def stop(self):
        self.is_running = False

    async def process_past_files(self, current_hour_key: str):
        """
        Ищет все файлы, которые НЕ относятся к текущему часу.
        Действие: Загрузить в облако -> Удалить с диска.
        """
        if not self.data_dir.exists(): return

        files = list(self.data_dir.glob("*.gz"))
        tasks = []

        for filepath in files:
            filename = filepath.name
            # Если имя файла НЕ содержит ключ текущего часа, значит это старый файл
            if current_hour_key not in filename:
                self.logger.info(f"📦 Found archived file: {filename}")
                # delete_after=True
                tasks.append(self._upload_file_logic(filepath, delete_after=True))
        
        if tasks:
            await asyncio.gather(*tasks)

    async def force_upload_current(self):
        """
        Ищет все файлы ТЕКУЩЕГО часа.
        Действие: Загрузить в облако -> ОСТАВИТЬ на диске (чтобы коллектор мог писать дальше).
        """
        if not self.data_dir.exists(): return 0

        current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        # Ищем файлы, в названии которых есть текущий час
        files = list(self.data_dir.glob(f"*{current_hour_key}*.gz"))
        
        tasks = []
        for filepath in files:
            self.logger.info(f"⚡ Force uploading: {filepath.name}")
            # delete_after=False
            tasks.append(self._upload_file_logic(filepath, delete_after=False))
        
        if tasks:
            results = await asyncio.gather(*tasks)
            return sum(results) # Возвращает количество успешных загрузок
        return 0

    async def _upload_file_logic(self, filepath: Path, delete_after: bool) -> bool:
        """
        Универсальная функция: парсит имя, грузит, опционально удаляет.
        """
        try:
            # Парсинг имени файла: binance_btcusdt_20250115_14.csv.gz
            parts = filepath.name.split('_')
            
            if len(parts) < 4:
                self.logger.warning(f"⚠️ Skipping unknown file format: {filepath.name}")
                return False

            exchange = parts[0]           # binance
            symbol = parts[1]             # btcusdt
            date = parts[2]               # 20250115
            hour = parts[3].split('.')[0] # 14

            # Вызов метода драйвера
            success = await self.cloud.async_upload_hour_file(
                str(filepath), exchange, symbol, date, hour
            )

            if success:
                if delete_after:
                    try:
                        os.remove(filepath)
                        self.logger.info(f"🗑️ Deleted local file: {filepath.name}")
                    except OSError as e:
                        self.logger.error(f"❌ Failed to delete {filepath.name}: {e}")
                else:
                    self.logger.info(f"✅ Uploaded (kept local): {filepath.name}")
                return True
            else:
                self.logger.error(f"❌ Upload failed: {filepath.name}")
                return False

        except Exception as e:
            self.logger.error(f"⚠️ Error processing {filepath.name}: {e}")
            return False