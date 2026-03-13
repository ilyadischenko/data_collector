import asyncio
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

SNAPSHOT_SCHEMA = pa.schema([
    pa.field("ts", pa.int64()),
    pa.field("lastUpdateId", pa.int64()),
    pa.field("bids", pa.string()),
    pa.field("asks", pa.string()),
])


class SnapshotWriter:
    def __init__(
        self,
        data_dir: str = './data',
        flush_interval: float = 60.0,
    ):
        self._base_dir = Path(data_dir)
        self._flush_interval = flush_interval

        self._buffers: dict[str, list[dict]] = defaultdict(list)
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._running = False

    def add(self, symbol: str, market_type: str, data: dict):
        row = {
            "ts": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            "lastUpdateId": data["lastUpdateId"],
            "bids": orjson.dumps(data["bids"]).decode(),
            "asks": orjson.dumps(data["asks"]).decode(),
        }
        key = f"{market_type}/{symbol}"
        with self._lock:
            self._buffers[key].append(row)

    def _flush_all(self):
        with self._lock:
            snapshot = {k: rows for k, rows in self._buffers.items() if rows}
            for k in snapshot:
                self._buffers[k] = []

        if not snapshot:
            return

        self._executor.submit(self._write_parquet, snapshot)

    def _write_parquet(self, snapshot: dict[str, list[dict]]):
        now = datetime.now(tz=timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        ts = int(time.time())

        for key, rows in snapshot.items():
            market_type, symbol = key.split("/", 1)
            try:
                table = pa.table({
                    "ts":           [r["ts"] for r in rows],
                    "lastUpdateId": [r["lastUpdateId"] for r in rows],
                    "bids":         [r["bids"] for r in rows],
                    "asks":         [r["asks"] for r in rows],
                }, schema=SNAPSHOT_SCHEMA)

                out_dir = self._base_dir / market_type / symbol / date_str
                out_dir.mkdir(parents=True, exist_ok=True)

                filepath = out_dir / f"{hour_str}-ob_snapshot-{ts}.parquet"
                pq.write_table(table, filepath)

            except Exception as e:
                logger.error(f"Ошибка записи снапшота [{symbol}]: {e}")

        logger.info(f"Снапшоты записаны: {len(snapshot)} символов")

    async def run(self):
        self._running = True
        logger.info("SnapshotWriter запущен")
        while self._running:
            await asyncio.sleep(self._flush_interval)
            self._flush_all()

    def stop(self):
        self._running = False
        self._flush_all()
        self._executor.shutdown(wait=True)
        logger.info("SnapshotWriter остановлен")