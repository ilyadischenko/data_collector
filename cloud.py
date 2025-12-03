


# key = "YCAJEVJeIO1bNwwm7wm9o9by1"
# secret_key = "YCNJ_6Frr8TWLFZxASFW47ZeYGFTtEawQaE0gwXa"




# class DataCloud:
#     def __init__(self):
#         self.url = "https://storage.yandexcloud.net/data-collector-hft"
#         self.key = key
#         self.secret_key = secret_key

    

#     def get_all_objects(self):
#         pass


import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
from typing import List, Optional
import logging
from pathlib import Path

class CloudStorage:
    def __init__(
        self,
        # access_key: str,
        # secret_key: str,
        # bucket_name: ,
        endpoint_url: str = "https://storage.yandexcloud.net",
        region: str = "ru-central1"
    ):
        """
        Инициализация клиента Yandex Cloud Object Storage
        
        access_key, secret_key — статические ключи из сервисного аккаунта
        bucket_name — имя вашего бакета
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
        
        logging.basicConfig(level=logging.INFO)
        self.log = logging.getLogger("YandexS3")

    def upload_file(
        self,
        file_path: str | Path,
        object_key: str,
        extra_args: Optional[dict] = None
    ) -> bool:
        """
        Загружает файл в бакет
        
        object_key — полный путь в бакете, например: trades/binance/2025/01/10/data_123456.gz
        """
        file_path = Path(file_path)
        if not file_path.exists():
            self.log.error(f"Файл не найден: {file_path}")
            return False
            
        try:
            args = extra_args or {}
            self.s3.upload_file(
                Filename=str(file_path),
                Bucket=self.bucket_name,
                Key=object_key,
                ExtraArgs=args
            )
            self.log.info(f"Успешно загружен: {object_key}")
            return True
        except ClientError as e:
            self.log.error(f"Ошибка загрузки {object_key}: {e}")
            return False

    def upload_bytes(
        self,
        data: bytes,
        object_key: str,
        extra_args: Optional[dict] = None
    ) -> bool:
        """
        Загружает байты напрямую (удобно для сжатых данных в памяти)
        """
        try:
            args = extra_args or {}
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=data,
                **args
            )
            self.log.info(f"Успешно загружено из памяти: {object_key}")
            return True
        except ClientError as e:
            self.log.error(f"Ошибка загрузки из памяти {object_key}: {e}")
            return False

    def download_file(self, object_key: str, local_path: str | Path) -> bool:
        """
        Скачивает файл из бакета
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.s3.download_file(
                Bucket=self.bucket_name,
                Key=object_key,
                Filename=str(local_path)
            )
            return True
        except ClientError as e:
            self.log.error(f"Ошибка скачивания {object_key}: {e}")
            return False

    def download_bytes(self, object_key: str) -> Optional[bytes]:
        """
        Возвращает содержимое объекта как bytes
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=object_key)
            return response['Body'].read()
        except ClientError as e:
            self.log.error(f"Ошибка чтения {object_key}: {e}")
            return None

    def list_folders(self, prefix: str = "", delimiter: str = "/") -> List[str]:
        """
        Возвращает список "папок" (CommonPrefixes) по префиксу
        
        Например: prefix="trades/binance/" → вернёт ['trades/binance/2025/', 'trades/binance/2024/']
        """
        paginator = self.s3.get_paginator('list_objects_v2')
        folders = set()
        
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter):
            for common_prefix in page.get('CommonPrefixes', []):
                folders.add(common_prefix['Prefix'])
                
        return sorted(folders)

    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> List[dict]:
        """
        Возвращает список объектов (файлов) по префиксу
        """
        objects = []
        paginator = self.s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                })
                
            if len(objects) >= max_keys:
                break
                
        return objects

    def exists(self, object_key: str) -> bool:
        """Проверяет, существует ли объект"""
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=object_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def delete_object(self, object_key: str) -> bool:
        """Удаляет объект"""
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=object_key)
            return True
        except ClientError as e:
            self.log.error(f"Ошибка удаления {object_key}: {e}")
            return False
        
    # cloud_storage.py — добавь этот метод в существующий класс

    def get_cloud_key(self, exchange: str, symbol: str, date: str, hour: str) -> str:
        """
        futures/binance/btcusdt/20250405/14.gz
        """
        return f"futures/{exchange.lower()}/{symbol.lower()}/{date}/{hour}.gz"

    def upload_hour_file(self, local_path: str, exchange: str, symbol: str, date: str, hour: str) -> bool:
        key = self.get_cloud_key(exchange, symbol, date, hour)
        try:
            with open(local_path, "rb") as f:
                self.s3.put_object(
                    Bucket=self.bucket_name,  # ✅ Исправлено
                    Key=key,
                    Body=f.read(),
                    ContentType="application/gzip"
                )
            self.log.info(f"↑ Cloud: {key}")  # ✅ Исправлено
            return True
        except Exception as e:
            self.log.error(f"Cloud upload failed {key}: {e}")  # ✅ Исправлено
            return False

    def hour_exists_in_cloud(self, exchange: str, symbol: str, date: str, hour: str) -> bool:
        key = self.get_cloud_key(exchange, symbol, date, hour)
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)  # ✅ Исправлено
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise