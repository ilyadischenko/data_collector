"""
Cloud Storage - Yandex Cloud Object Storage
"""

from typing import Optional, Literal
import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
import logging
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor


class CloudStorage:
    """
    Клиент для Yandex Cloud Object Storage (S3-совместимый).
    """
    
    def __init__(
        self,
        endpoint_url: str = "https://storage.yandexcloud.net",
        region: str = "ru-central1"
    ):
        """
        Инициализация клиента Yandex Cloud Object Storage.
        """
        self.bucket_name = "data-collector-hft"
        
        session = Session()
        self.s3 = session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id="YCAJEVJeIO1bNwwm7wm9o9by1",
            aws_secret_access_key="YCNJ_6Frr8TWLFZxASFW47ZeYGFTtEawQaE0gwXa",
        )
        
        self.logger = logging.getLogger("YandexS3")
        self._executor = ThreadPoolExecutor(max_workers=4)
        
        self.logger.info("✅ Yandex Cloud Storage initialized")

    def get_cloud_key(
        self,
        exchange: str,
        market: str,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        is_compressed: bool = False
    ) -> str:
        """
        Формирует путь файла в бакете.
        
        Args:
            exchange: "binance"
            market: "spot" или "futures"
            symbol: "btcusdt"
            date: "20260209"
            hour: "14"
            data_type: "trades" или "depth"
            is_compressed: если True, добавляет .gz к расширению
        
        Returns:
            futures/binance/btcusdt/20260209/14_trades.parquet.gz
            или
            spot/binance/btcusdt/20260209/14_trades.parquet
        """
        extension = "parquet.gz" if is_compressed else "parquet"
        # Формат: {market}/{exchange}/{symbol}/{date}/{hour}_{data_type}.{extension}
        return f"{market}/{exchange.lower()}/{symbol.lower()}/{date}/{hour}_{data_type}.{extension}"

    def upload_file(
        self,
        local_path: str | Path,
        exchange: str,
        market: str,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        is_compressed: bool = False
    ) -> bool:
        """
        Синхронная загрузка Parquet файла (с опциональным GZIP).
        """
        key = self.get_cloud_key(exchange, market, symbol, date, hour, data_type, is_compressed)
        local_path = Path(local_path)
        
        if not local_path.exists():
            self.logger.error(f"Файл не найден: {local_path}")
            return False

        try:
            # Определяем Content-Type
            if is_compressed:
                content_type = "application/gzip"
            else:
                content_type = "application/octet-stream"
            
            with open(local_path, "rb") as f:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=f.read(),
                    ContentType=content_type,
                    # Метаданные для удобства
                    Metadata={
                        'exchange': exchange,
                        'market': market,
                        'symbol': symbol,
                        'data_type': data_type,
                        'compressed': str(is_compressed)
                    }
                )
            
            file_size = local_path.stat().st_size
            self.logger.info(f"✅ Uploaded: {key} ({file_size / 1024:.1f} KB)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Upload failed {key}: {e}")
            return False

    async def async_upload_file(
        self,
        local_path: str | Path,
        exchange: str,
        market: str,
        symbol: str,
        date: str,
        hour: str,
        data_type: str,
        is_compressed: bool = False
    ) -> bool:
        """
        Асинхронная загрузка файла.
        
        Args:
            local_path: Путь к локальному файлу
            exchange: "binance"
            market: "spot" или "futures"
            symbol: "btcusdt"
            date: "20260209"
            hour: "14"
            data_type: "trades" или "depth"
            is_compressed: Сжат ли файл GZIP
            
        Returns:
            True если успешно загружено
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.upload_file,
            local_path,
            exchange,
            market,
            symbol,
            date,
            hour,
            data_type,
            is_compressed
        )

    def download_bytes(self, key: str) -> Optional[bytes]:
        """Синхронное скачивание файла в память."""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except ClientError:
            return None
        except Exception as e:
            self.logger.error(f"Download failed {key}: {e}")
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
                "metadata": response.get("Metadata", {})
            }
        except ClientError:
            return None

    def list_files(
        self,
        market: Optional[str] = None,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> list[dict]:
        """
        Список файлов в бакете с фильтрацией.
        
        Returns:
            [{"key": "...", "size": ..., "last_modified": ...}, ...]
        """
        try:
            # Формируем префикс для поиска
            if prefix:
                search_prefix = prefix
            else:
                parts = []
                if market:
                    parts.append(market)
                if exchange:
                    parts.append(exchange.lower())
                if symbol:
                    parts.append(symbol.lower())
                search_prefix = "/".join(parts) + "/" if parts else ""
            
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=search_prefix
            )
            
            if 'Contents' not in response:
                return []
            
            files = []
            for obj in response['Contents']:
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'size_mb': round(obj['Size'] / (1024 * 1024), 2)
                })
            
            return files
            
        except Exception as e:
            self.logger.error(f"List files failed: {e}")
            return []

    async def async_list_files(
        self,
        market: Optional[str] = None,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> list[dict]:
        """Асинхронный список файлов."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.list_files,
            market,
            exchange,
            symbol,
            prefix
        )