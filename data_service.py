import asyncio
import logging
import gzip
import io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

# Импорт CloudStorage
try:
    from storage.cloud import CloudStorage
except ImportError:
    pass

logger = logging.getLogger("DataService")

class DataQueryService:
    def __init__(self):
        self.cloud = CloudStorage()
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def fetch_data(self, exchange: str, symbol: str, 
                         date_from: str, hour_from: int,
                         date_to: str, hour_to: int) -> List[Dict[str, Any]]:
        """
        Асинхронная точка входа.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor,
            self._fetch_sync,
            exchange, symbol, date_from, hour_from, date_to, hour_to
        )

    def _fetch_sync(self, exchange: str, symbol: str, 
                    date_from: str, hour_from: int,
                    date_to: str, hour_to: int) -> List[Dict[str, Any]]:
        """
        Синхронная логика:
        Строит временной ряд с шагом 1 час от (date_from + hour_from) до (date_to + hour_to).
        """
        results = []
        try:
            # Собираем полные Timestamp начала и конца
            # Формат строки: YYYYMMDD HH
            start_dt = datetime.strptime(f"{date_from} {hour_from}", "%Y%m%d %H")
            end_dt = datetime.strptime(f"{date_to} {hour_to}", "%Y%m%d %H")
        except ValueError as e:
            return [{"error": f"Date/Time parsing error: {e}"}]

        if start_dt > end_dt:
            return [{"error": "Start time must be before end time"}]

        current_dt = start_dt

        # Цикл пока текущее время <= конечному
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y%m%d")
            hour_str = current_dt.strftime("%H") # вернет "09", "14" и т.д.
            
            # Формируем ключ S3
            key = self.cloud.get_cloud_key(exchange, symbol, date_str, hour_str)
            
            # Скачиваем (синхронно, внутри потока)
            file_bytes = self.cloud.download_bytes(key)
            
            if file_bytes:
                # Парсим этот конкретный час
                chunk_data = self._parse_gzip(file_bytes)
                results.extend(chunk_data)
            
            # Шагаем на час вперед
            current_dt += timedelta(hours=1)

        return results

    def _parse_gzip(self, content: bytes) -> List[Dict[str, Any]]:
        data = []
        try:
            with gzip.open(io.BytesIO(content), 'rt') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if not parts: continue
                    
                    rtype = parts[0]
                    try:
                        if rtype == 'T':
                            # T, Time, TradeId, Price, Qty, TradeTime, IsMaker
                            data.append({
                                "type": "trade",
                                "ts": int(parts[1]),
                                "p": float(parts[3]),
                                "q": float(parts[4]),
                                "m": parts[6] == "1"
                            })
                        elif rtype == 'B':
                            # B, Time, UpdateId, BidPr, BidQty, AskPr, AskQty
                            data.append({
                                "type": "book",
                                "ts": int(parts[1]),
                                "bp": float(parts[3]), # Bid Price
                                "bq": float(parts[4]), # Bid Qty
                                "ap": float(parts[5]), # Ask Price
                                "aq": float(parts[6])  # Ask Qty
                            })
                    except: continue
        except Exception: pass
        return data