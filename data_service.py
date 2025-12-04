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
        # Отдельный пул для тяжелых операций (I/O + Unzip)
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def fetch_data(self, exchange: str, symbol: str, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Главный метод для вызова из API.
        """
        loop = asyncio.get_running_loop()
        # Запускаем синхронную функцию в отдельном потоке
        return await loop.run_in_executor(
            self.executor,
            self._fetch_sync,
            exchange, symbol, date_from, date_to
        )

    def _fetch_sync(self, exchange: str, symbol: str, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Синхронная логика:
        1. Сгенерировать список часов.
        2. Скачать байты из S3.
        3. Распаковать и распарсить CSV.
        """
        results = []
        try:
            start_dt = datetime.strptime(date_from, "%Y%m%d")
            end_dt = datetime.strptime(date_to, "%Y%m%d")
        except ValueError:
            return [{"error": "Invalid date format. Use YYYYMMDD"}]

        # Проходим по часам
        # Ограничитель: если запросили год, это убьет память.
        # В реале тут нужен лимит или стриминг. Допустим, просто собираем всё в список.
        
        current_dt = start_dt
        # Конец - конец дня date_to (23:00)
        limit_dt = end_dt + timedelta(hours=23)

        while current_dt <= limit_dt:
            date_str = current_dt.strftime("%Y%m%d")
            hour_str = current_dt.strftime("%H")
            
            # Скачиваем
            key = self.cloud.get_cloud_key(exchange, symbol, date_str, hour_str)
            file_bytes = self.cloud.download_bytes(key)
            
            if file_bytes:
                parsed = self._parse_gzip(file_bytes)
                results.extend(parsed)
            
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
                            # T, EventTime, TradeId, Price, Qty, TradeTime, IsMaker
                            data.append({
                                "type": "trade",
                                "ts": int(parts[1]),
                                "price": float(parts[3]),
                                "qty": float(parts[4]),
                                "maker": parts[6] == "1"
                            })
                        elif rtype == 'B':
                            # B, EventTime, UpdateId, BidPr, BidQty, AskPr, AskQty
                            data.append({
                                "type": "book",
                                "ts": int(parts[1]),
                                "bid": float(parts[3]),
                                "bid_q": float(parts[4]),
                                "ask": float(parts[5]),
                                "ask_q": float(parts[6])
                            })
                    except: continue
        except Exception: pass
        return data