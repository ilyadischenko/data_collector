from cloud import CloudStorage
import gzip
from datetime import datetime

s3 = CloudStorage(
    # access_key="YCAJExxx...",
    # secret_key="YCPsxxx...",
    # bucket_name="my-trading-data"
)

# Собираешь данные → сжимаешь → сразу кидаешь в облако
raw_data = b'{"price": 65000, "amount": 0.15}...' * 10
compressed = gzip.compress(raw_data)

today = datetime.now().strftime("%Y/%m/%d")
object_key = f"trades/binance/{today}/data_{int(datetime.now().timestamp())}.gz"

s3.upload_bytes(
    data=compressed,
    object_key=object_key,
    extra_args={'ContentType': 'application/gzip'}
)

# Получить список всех дат (папок) за январь
dates = s3.list_folders("trades/binance/2025/01/")
print(dates)