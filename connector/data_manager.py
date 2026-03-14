import asyncio
import logging
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import time

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class DataManager:

    def __init__(
        self,
        market_type: str,       # "futures" или "spot"
        conn_id: int,           # 1 или 2
        schemas,
        data_dir: str = '../data',
        flush_count: int = 10_000,
        flush_interval: float = 60.0,
    ):
        self._base_dir = Path(data_dir) / market_type
        self._market_type = market_type
        self._conn_id = conn_id
        self.schemas = schemas
        self._flush_count = flush_count
        self._flush_interval = flush_interval

        self._buffers: dict[str, deque[str]] = {}
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._writer_lock = threading.Lock()

        self._current_hour: str = ""
        self._running = False

        self.total_flushed: int = 0

    # ── извлечение полей ──────────────────────────────────

    @staticmethod
    def extract_symbol(raw: str) -> str | None:
        idx = raw.find('"s":"')
        if idx == -1:
            return None
        start = idx + 5
        end = raw.index('"', start)
        return raw[start:end].lower()

    @staticmethod
    def extract_type(raw: str) -> str | None:
        idx = raw.find('"e":"')
        if idx == -1:
            return None
        start = idx + 5
        end = raw.index('"', start)
        return raw[start:end]

    # ── символы ───────────────────────────────────────────

    def add_symbol(self, symbol: str):
        if symbol not in self._buffers:
            self._buffers[symbol] = deque()

    def remove_symbol(self, symbol: str):
        if symbol in self._buffers:
            self._flush_symbol(symbol)
            del self._buffers[symbol]

    # ── добавление ────────────────────────────────────────

    def add(self, raw: str) -> bool:
        symbol = self.extract_symbol(raw)
        if symbol is None:
            return False

        buf = self._buffers.get(symbol)
        if buf is None:
            return False

        buf.append(raw)

        if len(buf) >= self._flush_count:
            self._flush_symbol(symbol)

        return True


    # ── сброс ─────────────────────────────────────────────

    def _flush_symbol(self, symbol: str) -> int:
        buf = self._buffers.get(symbol)
        if not buf:
            return 0

        snapshot = list(buf)
        buf.clear()

        self._executor.submit(
            self._write_parquet, symbol, snapshot
        )
        return len(snapshot)

    def _write_parquet(self, symbol: str, rows_raw: list[str]):
        try:
            by_type: dict[str, list[dict]] = defaultdict(list)
            for raw in rows_raw:
                d = orjson.loads(raw)
                msg_type = d.get("e")
                if not msg_type or msg_type not in self.schemas:
                    continue

                if msg_type == "trade":
                    if d.get("p") == "0" and d.get("q") == "0":
                        continue

                by_type[msg_type].append(d)

            for msg_type, rows in by_type.items():
                config = self.schemas[msg_type]

                if msg_type == "depthUpdate":
                    for r in rows:
                        r["b"] = orjson.dumps(r["b"]).decode()
                        r["a"] = orjson.dumps(r["a"]).decode()

                
                # группируем по часу из timestamp сообщения
                by_hour: dict[tuple[str, str], list[dict]] = defaultdict(list)
                for r in rows:
                    ts_ms = r.get("T") or r.get("E")
                    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    by_hour[(dt.strftime("%Y-%m-%d"), dt.strftime("%H"))].append(r)

                # пишем каждый час в свой файл
                for (msg_date, msg_hour), hour_rows in by_hour.items():
                    columns = {f: [r[f] for r in hour_rows] for f in config["fields"]}
                    table = pa.table(columns, schema=config["schema"])

                    symbol_dir = self._base_dir / symbol / msg_date
                    symbol_dir.mkdir(parents=True, exist_ok=True)

                    ts = int(time.time())
                    filename = f"{msg_hour}-{config['name']}-{self._conn_id}-{ts}.parquet"
                    filepath = symbol_dir / filename

                    pq.write_table(table, filepath)

                    # print(f'Сбросил {msg_type} для {symbol}. Lenght% [{len(by_hour.items())}]', flush=True)


        except Exception as e:
            logger.error(f"Ошибка записи [{symbol}]: {e}")


    def flush_all(self):
        flushed = 0
        for symbol in list(self._buffers.keys()):
            flushed += self._flush_symbol(symbol)
        
        # in_memory = sum(len(b) for b in self._buffers.values())
    
        # # память процесса
        # process = psutil.Process(os.getpid())
        # ram_mb = process.memory_info().rss / 1024 ** 2
        
        # logger.info(
        #     f'Коннектор [{self._market_type} {self._conn_id}] '
        #     f'сбросил {flushed} | в буферах осталось {in_memory} | '
        #     f'RAM процесса {ram_mb:.1f} MB'
        # )
        # logger.info(f'Коннектор [{self._market_type} {self._conn_id}] сбросил {flushed} сообщений на диск')



    # ── фоновая задача ────────────────────────────────────

    async def run(self):
        self._running = True

        while self._running:
            await asyncio.sleep(self._flush_interval)
            await asyncio.get_event_loop().run_in_executor(
                self._executor, self.flush_all
            )


    def stop(self):
        self._running = False
        self.flush_all()
        self._executor.shutdown(wait=True)
        logger.info(f"DataManager [{self._market_type}:{self._conn_id}] остановлен"
                   f"Total: {self.total_flushed:,}")