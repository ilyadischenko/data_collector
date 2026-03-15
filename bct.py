# delete_hour.py
from data_manager.cloud_manager import CloudManager


class ExCloud(CloudManager):
    def list_hour(self, date: str, hour: str, market: str = None, exchange: str = 'binance') -> list[dict]:
        date_compact = date.replace("-", "")
        prefix = f"{exchange}/{market}/" if market else f"{exchange}/"
        files = self.list_files(prefix=prefix)
        print(len(files))

        return [
            f for f in files
            if f"/{date_compact}/{hour}_" in f["key"]
        ]
    
    def check_hour_completeness(self, date: str, hour: str, market: str = None, exchange: str = 'binance') -> dict:
        files = self.list_hour(date=date, hour=hour, market=market, exchange=exchange)
        
        # группируем по символу
        by_symbol: dict[str, set] = {}
        for f in files:
            # key: binance/futures/btcusdt/20260314/15_trades.parquet
            parts = f["key"].split("/")
            symbol    = parts[2]
            data_type = parts[4].split("_", 1)[1].replace(".parquet", "")
            by_symbol.setdefault(symbol, set()).add(data_type)

        expected = {"trades", "depth", "ob_snapshot"}
        
        complete   = {s: types for s, types in by_symbol.items() if types == expected}
        incomplete = {s: expected - types for s, types in by_symbol.items() if types != expected}

        print(f"\nДата: {date} | Час: {hour} | Market: {market or 'all'}")
        print(f"Всего символов: {len(by_symbol)}")
        print(f"Полные ({len(complete)}): все 3 файла есть")
        
        if incomplete:
            print(f"\nНеполные ({len(incomplete)}):")
            for symbol, missing in sorted(incomplete.items()):
                print(f"  {symbol}: нет {', '.join(missing)}")
        else:
            print("Неполных нет ✅")

        return {"complete": complete, "incomplete": incomplete}


    def delete(self, key: str) -> bool:
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=key)
            return True
        except Exception as e:
            print(f"[s3] delete failed {key}: {e}")
            return False

    def delete_hour(self, date: str, hour: str, market: str = None, exchange: str = 'binance') -> int:
        # формируем префикс для поиска
        # binance/futures/btcusdt/20260209/14_
        date_compact = date.replace("-", "")
        if market:
            prefix = f"{exchange}/{market}/"
        else:
            prefix = f"{exchange}/"

        files = self.list_files(prefix=prefix)
        
        # фильтруем по дате и часу
        to_delete = [
            f for f in files
            if f"/{date_compact}/{hour}_" in f["key"]
        ]

        if not to_delete:
            print(f"[s3] Нет файлов за {date}/{hour}")
            return 0

        print(f"[s3] Удаляю {len(to_delete)} файлов за {date}/{hour}...")
        deleted = 0
        for f in to_delete:
            if self.delete(f["key"]):
                print(f"[s3] Удалён {f['key']}")
                deleted += 1

        print(f"[s3] Удалено {deleted}/{len(to_delete)}")
        return deleted
    
    def download_bytes(self, key: str) -> bytes | None:
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()
        except Exception as e:
            print(f"[s3] download failed {key}: {e}")
            return None
    
cloud = ExCloud()

# files = cloud.list_hour(date="2026-03-15", hour="04", market="futures")
# for f in files[:10]:
#     print(f["key"])

# btc = [f for f in files if "btcusdt" in f["key"]]
# print(f"btcusdt файлов: {len(btc)}")
# for f in btc:
#     print(f["key"])

# удалить всё за час
# cloud.delete_hour(date="2026-03-15", hour="11")

# только futures
# cloud.delete_hour(date="2026-03-15", hour="11", market="future")

# # только spot
# cloud.delete_hour(date="2026-03-14", hour="09", market="spot")

# for i in range(0, 10):
#     cloud.check_hour_completeness(date="2026-03-15", hour=f"{i:02}", market="spot")

# files = cloud.list_hour(date="2026-03-15", hour="10")
# for f in files:
#     print(f"  {f['key']}  {f['size_kb']} KB  {f['last_modified']:%Y-%m-%d %H:%M}")
# print(f"Файлов: {len(files)}")
