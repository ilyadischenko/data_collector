from typing import Optional, Literal
import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
import logging
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor

class CloudStorage:
    def __init__(
        self,
        endpoint_url: str = "https://storage.yandexcloud.net",
        region: str = "ru-central1"
    ):
        """
        Инициализация клиента Yandex Cloud Object Storage.
        """
        self.bucket_name = "data-collector-hft"
        
        # Настройка сессии S3
        # В продакшене ключи лучше брать из os.getenv()
        session = Session()
        self.s3 = session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id="YCAJEVJeIO1bNwwm7wm9o9by1",
            aws_secret_access_key="YCNJ_6Frr8TWLFZxASFW47ZeYGFTtEawQaE0gwXa",
        )
        
        self.log = logging.getLogger("YandexS3")
        
        # Пул потоков для асинхронного выполнения блокирующих операций Boto3
        self._executor = ThreadPoolExecutor(max_workers=4)

    def get_cloud_key(
        self,
        exchange: str,
        symbol: str,
        date: str,
        hour: str,
        data_type: Literal["trades", "orderbook"]
    ) -> str:
        """
        Формирует путь файла в бакете.
        
        Args:
            exchange: биржа (binance, bybit)
            symbol: символ (btcusdt)
            date: дата в формате YYYYMMDD
            hour: час (0-23)
            data_type: тип данных (trades или orderbook)
        
        Returns:
            Путь в S3, например: futures/binance/btcusdt/20250115/14_trades.gz
        """
        return f"futures/{exchange.lower()}/{symbol.lower()}/{date}/{hour}_{data_type}.gz"

    def upload_file(
        self,
        local_path: str | Path,
        exchange: str,
        symbol: str,
        date: str,
        hour: str,
        data_type: Literal["trades", "orderbook"]
    ) -> bool:
        """
        Синхронная загрузка файла.
        ВАЖНО: S3 автоматически перезаписывает файл, если ключ совпадает.
        Это позволяет обновлять файл текущего часа при Force Upload.
        
        Args:
            local_path: путь к локальному файлу
            exchange: биржа
            symbol: символ
            date: дата (YYYYMMDD)
            hour: час (0-23)
            data_type: trades или orderbook
        """
        key = self.get_cloud_key(exchange, symbol, date, hour, data_type)
        local_path = Path(local_path)
        
        if not local_path.exists():
            self.log.error(f"Файл не найден: {local_path}")
            return False

        try:
            with open(local_path, "rb") as f:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=f.read(),
                    ContentType="application/gzip"
                )
            
            file_size = local_path.stat().st_size
            self.log.info(f"✅ Uploaded: {key} ({file_size / 1024:.1f} KB)")
            return True
            
        except Exception as e:
            self.log.error(f"❌ Upload failed {key}: {e}")
            return False

    async def async_upload_file(
        self,
        local_path: str | Path,
        exchange: str,
        symbol: str,
        date: str,
        hour: str,
        data_type: Literal["trades", "orderbook"]
    ) -> bool:
        """
        Асинхронная обертка для загрузки файла.
        Запускает в отдельном потоке, не блокируя Event Loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.upload_file,
            local_path,
            exchange,
            symbol,
            date,
            hour,
            data_type
        )

    def upload_hour_files(
        self,
        exchange: str,
        symbol: str,
        date: str,
        hour: str,
        data_dir: Path | str = Path("collected_data")
    ) -> dict[str, bool]:
        """
        Загружает оба файла (trades и orderbook) за определенный час.
        
        Returns:
            {"trades": True/False, "orderbook": True/False}
        """
        data_dir = Path(data_dir)
        results = {}
        
        for data_type in ["trades", "orderbook"]:
            # Формат локального файла: binance_btcusdt_20251204_13_trades.csv.gz
            filename = f"{exchange}_{symbol}_{date}_{hour}_{data_type}.csv.gz"
            local_path = data_dir / filename
            
            results[data_type] = self.upload_file(
                local_path, exchange, symbol, date, hour, data_type
            )
        
        return results

    async def async_upload_hour_files(
        self,
        exchange: str,
        symbol: str,
        date: str,
        hour: str,
        data_dir: Path | str = Path("collected_data")
    ) -> dict[str, bool]:
        """
        Асинхронная загрузка обоих файлов за час.
        Загружает trades и orderbook параллельно.
        """
        data_dir = Path(data_dir)
        
        # Запускаем загрузку параллельно
        tasks = []
        for data_type in ["trades", "orderbook"]:
            filename = f"{exchange}_{symbol}_{date}_{hour}_{data_type}.csv.gz"
            local_path = data_dir / filename
            
            tasks.append(
                self.async_upload_file(
                    local_path, exchange, symbol, date, hour, data_type
                )
            )
        
        results = await asyncio.gather(*tasks)
        
        return {
            "trades": results[0],
            "orderbook": results[1]
        }

    def force_upload_current_hour(
        self,
        exchange: str,
        symbol: str,
        data_dir: Path | str = Path("collected_data")
    ) -> dict[str, bool]:
        """
        Принудительная загрузка файлов текущего часа.
        Используется для обновления данных в облаке до завершения часа.
        
        Example:
            >>> cloud.force_upload_current_hour("binance", "btcusdt")
            {'trades': True, 'orderbook': True}
        """
        from datetime import datetime, timezone
        
        now = datetime.now(timezone.utc)
        date = now.strftime("%Y%m%d")
        hour = str(now.hour)
        
        self.log.info(f"🔄 Force upload for {exchange}/{symbol} - {date}/{hour}")
        
        return self.upload_hour_files(exchange, symbol, date, hour, data_dir)

    async def async_force_upload_current_hour(
        self,
        exchange: str,
        symbol: str,
        data_dir: Path | str = Path("collected_data")
    ) -> dict[str, bool]:
        """
        Асинхронная принудительная загрузка текущего часа.
        """
        from datetime import datetime, timezone
        
        now = datetime.now(timezone.utc)
        date = now.strftime("%Y%m%d")
        hour = str(now.hour)
        
        self.log.info(f"🔄 Force upload for {exchange}/{symbol} - {date}/{hour}")
        
        return await self.async_upload_hour_files(
            exchange, symbol, date, hour, data_dir
        )

    def upload_all_files(
        self,
        data_dir: Path | str = Path("collected_data"),
        pattern: str = "*.csv.gz"
    ) -> dict[str, int]:
        """
        Загружает все файлы из директории.
        
        Returns:
            {"uploaded": 10, "failed": 2, "skipped": 0}
        """
        data_dir = Path(data_dir)
        stats = {"uploaded": 0, "failed": 0, "skipped": 0}
        
        for file_path in data_dir.glob(pattern):
            # Парсим имя файла: binance_btcusdt_20251204_13_trades.csv.gz
            try:
                parts = file_path.stem.replace(".csv", "").split("_")
                
                if len(parts) < 5:
                    self.log.warning(f"⚠️  Неизвестный формат: {file_path.name}")
                    stats["skipped"] += 1
                    continue
                
                exchange = parts[0]
                symbol = parts[1]
                date = parts[2]
                hour = parts[3]
                data_type = parts[4]  # trades или orderbook
                
                success = self.upload_file(
                    file_path, exchange, symbol, date, hour, data_type
                )
                
                if success:
                    stats["uploaded"] += 1
                else:
                    stats["failed"] += 1
                    
            except Exception as e:
                self.log.error(f"❌ Error processing {file_path.name}: {e}")
                stats["failed"] += 1
        
        self.log.info(
            f"📊 Upload complete: {stats['uploaded']} uploaded, "
            f"{stats['failed']} failed, {stats['skipped']} skipped"
        )
        
        return stats

    async def async_upload_all_files(
        self,
        data_dir: Path | str = Path("collected_data"),
        pattern: str = "*.csv.gz",
        max_concurrent: int = 5
    ) -> dict[str, int]:
        """
        Асинхронная загрузка всех файлов с ограничением параллелизма.
        """
        data_dir = Path(data_dir)
        stats = {"uploaded": 0, "failed": 0, "skipped": 0}
        
        files = list(data_dir.glob(pattern))
        
        # Семафор для ограничения одновременных загрузок
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def upload_with_semaphore(file_path):
            async with semaphore:
                try:
                    parts = file_path.stem.replace(".csv", "").split("_")
                    
                    if len(parts) < 5:
                        return "skipped"
                    
                    exchange = parts[0]
                    symbol = parts[1]
                    date = parts[2]
                    hour = parts[3]
                    data_type = parts[4]
                    
                    success = await self.async_upload_file(
                        file_path, exchange, symbol, date, hour, data_type
                    )
                    
                    return "uploaded" if success else "failed"
                    
                except Exception as e:
                    self.log.error(f"❌ Error: {e}")
                    return "failed"
        
        # Загружаем все файлы параллельно
        results = await asyncio.gather(*[upload_with_semaphore(f) for f in files])
        
        # Подсчитываем статистику
        for result in results:
            stats[result] += 1
        
        self.log.info(
            f"📊 Upload complete: {stats['uploaded']} uploaded, "
            f"{stats['failed']} failed, {stats['skipped']} skipped"
        )
        
        return stats
    
    def download_bytes(self, key: str) -> Optional[bytes]:
        """Синхронное скачивание файла в память."""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except ClientError:
            # Файла нет - это нормально
            return None
        except Exception as e:
            self.log.error(f"Download failed {key}: {e}")
            return None

    async def async_download_bytes(self, key: str) -> Optional[bytes]:
        """Асинхронное скачивание файла."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.download_bytes,
            key
        )

    def file_exists(self, key: str) -> bool:
        """Проверить существование файла в S3."""
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def get_file_info(self, key: str) -> Optional[dict]:
        """Получить информацию о файле."""
        try:
            response = self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return {
                "size": response["ContentLength"],
                "last_modified": response["LastModified"],
                "content_type": response.get("ContentType", ""),
            }
        except ClientError:
            return None