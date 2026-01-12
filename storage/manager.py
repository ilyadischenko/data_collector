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
        
        # Храним текущий час для отслеживания смены
        self.last_checked_hour = datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    async def run(self):
        """
        Фоновая задача. Проверяет каждые 10 секунд, не сменился ли час.
        При смене часа загружает файлы прошлого часа и удаляет их.
        """
        self.is_running = True
        self.logger.info(f"☁️ Cloud Manager started. Watching: {self.data_dir.absolute()}")
        
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
                    
                    # Обрабатываем файлы прошлых часов (загрузить и удалить)
                    await self.process_past_files(current_hour_key)
                    
                    self.last_checked_hour = current_hour_key
                    
            except Exception as e:
                self.logger.error(f"❌ Manager loop error: {e}")

    def stop(self):
        """Остановка менеджера."""
        self.is_running = False
        self.logger.info("🛑 Cloud Manager stopped")

    async def process_past_files(self, current_hour_key: str):
        """
        Ищет все файлы, которые НЕ относятся к текущему часу.
        Действие: Загрузить в облако -> Удалить с диска.
        
        Args:
            current_hour_key: текущий час в формате YYYYMMDD_HH
        """
        if not self.data_dir.exists():
            self.logger.warning(f"⚠️ Data directory not found: {self.data_dir}")
            return

        files = list(self.data_dir.glob("*.gz"))
        tasks = []

        for filepath in files:
            filename = filepath.name
            
            # Если имя файла НЕ содержит ключ текущего часа, значит это старый файл
            if current_hour_key not in filename:
                self.logger.info(f"📦 Found past hour file: {filename}")
                # Загружаем и удаляем
                tasks.append(self._upload_file_logic(filepath, delete_after=True))
        
        if tasks:
            results = await asyncio.gather(*tasks)
            success_count = sum(results)
            self.logger.info(
                f"📊 Processed {len(tasks)} past files: "
                f"{success_count} uploaded, {len(tasks) - success_count} failed"
            )

    async def force_upload_current(self):
        """
        Принудительная загрузка файлов ТЕКУЩЕГО часа.
        Действие: Загрузить в облако -> ОСТАВИТЬ на диске.
        
        Используется для синхронизации данных текущего часа без остановки коллектора.
        
        Returns:
            Количество успешно загруженных файлов
        """
        if not self.data_dir.exists():
            self.logger.warning(f"⚠️ Data directory not found: {self.data_dir}")
            return 0

        current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        
        # Ищем файлы текущего часа
        files = list(self.data_dir.glob(f"*{current_hour_key}*.gz"))
        
        if not files:
            self.logger.info(f"ℹ️ No files found for current hour: {current_hour_key}")
            return 0
        
        tasks = []
        for filepath in files:
            self.logger.info(f"⚡ Force uploading: {filepath.name}")
            # Загружаем, но НЕ удаляем
            tasks.append(self._upload_file_logic(filepath, delete_after=False))
        
        results = await asyncio.gather(*tasks)
        success_count = sum(results)
        
        self.logger.info(
            f"📊 Force upload complete: {success_count}/{len(tasks)} successful"
        )
        
        return success_count

    async def upload_all_files(self, delete_after: bool = False):
        """
        Загрузить ВСЕ файлы из директории (включая текущий час).
        
        Args:
            delete_after: удалять ли файлы после загрузки
            
        Returns:
            Количество успешно загруженных файлов
        """
        if not self.data_dir.exists():
            self.logger.warning(f"⚠️ Data directory not found: {self.data_dir}")
            return 0

        files = list(self.data_dir.glob("*.gz"))
        
        if not files:
            self.logger.info("ℹ️ No files to upload")
            return 0
        
        self.logger.info(f"📦 Found {len(files)} files to upload")
        
        tasks = []
        for filepath in files:
            tasks.append(self._upload_file_logic(filepath, delete_after=delete_after))
        
        results = await asyncio.gather(*tasks)
        success_count = sum(results)
        
        self.logger.info(
            f"📊 Upload complete: {success_count}/{len(tasks)} successful"
        )
        
        return success_count

    async def _upload_file_logic(self, filepath: Path, delete_after: bool) -> bool:
        """
        Универсальная функция загрузки файла.
        
        Парсит имя файла, загружает в облако, опционально удаляет.
        
        Args:
            filepath: путь к файлу
            delete_after: удалять ли файл после успешной загрузки
            
        Returns:
            True если загрузка успешна, False иначе
        """
        try:
            # Парсинг имени файла: binance_btcusdt_20250115_14_trades.csv.gz
            # Убираем расширения .csv.gz
            name_without_ext = filepath.name.replace('.csv.gz', '').replace('.gz', '')
            parts = name_without_ext.split('_')
            
            # Ожидаем минимум 5 частей: exchange_symbol_date_hour_datatype
            if len(parts) < 5:
                self.logger.warning(f"⚠️ Skipping unknown file format: {filepath.name}")
                self.logger.warning(f"   Expected format: exchange_symbol_YYYYMMDD_HH_datatype.csv.gz")
                return False

            exchange = parts[0]      # binance
            symbol = parts[1]        # btcusdt
            date = parts[2]          # 20250115
            hour = parts[3]          # 14
            data_type = parts[4]     # trades или orderbook

            # Проверяем корректность data_type
            if data_type not in ["trades", "orderbook", "depth"]:
                self.logger.warning(
                    f"⚠️ Unknown data type '{data_type}' in {filepath.name}"
                )
                return False

            # Загружаем файл через обновленный драйвер
            success = await self.cloud.async_upload_file(
                local_path=filepath,
                exchange=exchange,
                symbol=symbol,
                date=date,
                hour=hour,
                data_type=data_type
            )

            if success:
                if delete_after:
                    try:
                        os.remove(filepath)
                        self.logger.info(f"🗑️ Deleted: {filepath.name}")
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

    async def force_upload_symbol(
        self,
        exchange: str,
        symbol: str,
        delete_after: bool = False
    ) -> int:
        """
        Принудительная загрузка всех файлов конкретного символа.
        
        Args:
            exchange: биржа (binance, bybit)
            symbol: символ (btcusdt)
            delete_after: удалять ли файлы после загрузки
            
        Returns:
            Количество успешно загруженных файлов
        """
        if not self.data_dir.exists():
            return 0

        # Паттерн для поиска: binance_btcusdt_*.gz
        pattern = f"{exchange}_{symbol}_*.gz"
        files = list(self.data_dir.glob(pattern))
        
        if not files:
            self.logger.info(f"ℹ️ No files found for {exchange}/{symbol}")
            return 0
        
        self.logger.info(f"📦 Found {len(files)} files for {exchange}/{symbol}")
        
        tasks = []
        for filepath in files:
            tasks.append(self._upload_file_logic(filepath, delete_after=delete_after))
        
        results = await asyncio.gather(*tasks)
        success_count = sum(results)
        
        self.logger.info(
            f"📊 Uploaded {exchange}/{symbol}: {success_count}/{len(tasks)} successful"
        )
        
        return success_count

    def get_local_files_stats(self) -> dict:
        """
        Получить статистику по локальным файлам.
        
        Returns:
            {
                'total_files': 10,
                'total_size_mb': 125.5,
                'by_symbol': {
                    'binance_btcusdt': {'trades': 5, 'orderbook': 5, 'depth': 5},
                    'bybit_ethusdt': {'trades': 3, 'orderbook': 2, 'depth': 0}
                },
                'current_hour_files': 4,
                'past_hour_files': 6
            }
        """
        if not self.data_dir.exists():
            return {}

        current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        
        stats = {
            'total_files': 0,
            'total_size_mb': 0,
            'by_symbol': {},
            'current_hour_files': 0,
            'past_hour_files': 0
        }
        
        for filepath in self.data_dir.glob("*.gz"):
            stats['total_files'] += 1
            stats['total_size_mb'] += filepath.stat().st_size / (1024 * 1024)
            
            # Текущий или прошлый час?
            if current_hour_key in filepath.name:
                stats['current_hour_files'] += 1
            else:
                stats['past_hour_files'] += 1
            
            # Парсим для статистики по символам
            try:
                name = filepath.name.replace('.csv.gz', '').replace('.gz', '')
                parts = name.split('_')
                
                if len(parts) >= 5:
                    exchange = parts[0]
                    symbol = parts[1]
                    data_type = parts[4]
                    
                    key = f"{exchange}_{symbol}"
                    if key not in stats['by_symbol']:
                        stats['by_symbol'][key] = {'trades': 0, 'orderbook': 0, 'depth': 0}
                    
                    if data_type in ['trades', 'orderbook', 'depth']:
                        stats['by_symbol'][key][data_type] += 1
            except:
                pass
        
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        
        return stats


# ==================== Пример использования ====================

# async def main():
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
    
#     manager = CloudManager(data_dir="collected_data")
    
#     # Запустить менеджер в фоне
#     manager_task = asyncio.create_task(manager.run())
    
#     # Подождать немного
#     await asyncio.sleep(30)
    
#     # Принудительная загрузка текущего часа
#     await manager.force_upload_current()
    
#     # Статистика
#     stats = manager.get_local_files_stats()
#     print(f"📊 Local files stats: {stats}")
    
#     # Загрузить все файлы конкретного символа
#     await manager.force_upload_symbol("binance", "btcusdt", delete_after=False)
    
#     # Загрузить все файлы
#     await manager.upload_all_files(delete_after=False)
    
#     # Остановка
#     manager.stop()
#     await manager_task


# if __name__ == "__main__":
#     asyncio.run(main())