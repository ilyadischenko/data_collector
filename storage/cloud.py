from typing import Optional
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

    def get_cloud_key(self, exchange: str, symbol: str, date: str, hour: str) -> str:
        """
        Формирует путь файла в бакете.
        Пример: futures/binance/btcusdt/20250115/14.gz
        """
        return f"futures/{exchange.lower()}/{symbol.lower()}/{date}/{hour}.gz"

    def upload_hour_file(self, local_path: str, exchange: str, symbol: str, date: str, hour: str) -> bool:
        """
        Синхронная загрузка файла.
        ВАЖНО: S3 автоматически перезаписывает файл, если ключ совпадает.
        Это позволяет нам обновлять файл текущего часа при Force Upload.
        """
        key = self.get_cloud_key(exchange, symbol, date, hour)
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
                    ContentType="application/gzip"  # Важно для правильного скачивания
                )
            self.log.info(f"✅ Uploaded to Cloud: {key}")
            return True
        except Exception as e:
            self.log.error(f"❌ Cloud upload failed {key}: {e}")
            return False

    async def async_upload_hour_file(
        self,
        local_path: str,
        exchange: str,
        symbol: str,
        date: str,
        hour: str
    ) -> bool:
        """
        Асинхронная обертка. Запускает загрузку в отдельном потоке,
        чтобы не блокировать основной цикл (Event Loop).
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.upload_hour_file,
            local_path,
            exchange,
            symbol,
            date,
            hour
        )
    
    def download_bytes(self, key: str) -> Optional[bytes]:
        """Синхронное скачивание файла в память."""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except ClientError:
            # Файла нет - это нормально, просто возвращаем None
            return None
        except Exception as e:
            self.log.error(f"Download failed {key}: {e}")
            return None