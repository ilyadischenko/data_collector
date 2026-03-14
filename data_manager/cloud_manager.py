import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from boto3.session import Session
from botocore.exceptions import ClientError

from config import cloud_url, region, bucket_name, s3_access_key_id, s3_secret_access_key

logger = logging.getLogger(__name__)

class CloudManager:
    def __init__(self):
        self._bucket = bucket_name
        self._executor = ThreadPoolExecutor(max_workers=4)

        session = Session()
        self._s3 = session.client(
            service_name='s3',
            endpoint_url=cloud_url,
            region_name=region,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
        )
        logger.info(f"CloudStorage инициализирован: {bucket_name} в {region}")

    def _make_key(self, market: str, symbol: str, date: str, hour: str, data_type: str, exchange: str = 'binance') -> str:
        """futures/btcusdt/20260209/14_trades.parquet"""
        date_compact = date.replace("-", "")
        return f"{exchange}/{market}/{symbol}/{date_compact}/{hour}_{data_type}.parquet"

    def upload(self, local_path: Path, market: str, symbol: str, date: str, hour: str, data_type: str) -> bool:
        key = self._make_key(market, symbol, date, hour, data_type)
        try:
            with open(local_path, "rb") as f:
                self._s3.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=f.read(),
                    ContentType="application/octet-stream",
                )
            # size_kb = local_path.stat().st_size / 1024
            # logger.info(f"[s3] ✅ {key} ({size_kb:.1f} KB)")
            return True
        except Exception as e:
            logger.error(f"[s3] ❌ {key}: {e}")
            return False

    async def async_upload(self, local_path: Path, market: str, symbol: str, date: str, hour: str, data_type: str) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.upload,
            local_path, market, symbol, date, hour, data_type,
        )

    def file_exists(self, market: str, symbol: str, date: str, hour: str, data_type: str) -> bool:
        key = self._make_key(market, symbol, date, hour, data_type)
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str = "") -> list[dict]:
        try:
            response = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
            if 'Contents' not in response:
                return []
            return [
                {
                    "key":           obj["Key"],
                    "size_kb":       round(obj["Size"] / 1024, 1),
                    "last_modified": obj["LastModified"],
                }
                for obj in response["Contents"]
            ]
        except Exception as e:
            logger.error(f"[s3] list_files failed: {e}")
            return []