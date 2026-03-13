# bench_realistic.py

import gc
import time
import random
import shutil
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq


# ══════════════════════════════════════════════════════════
# DataManager
# ══════════════════════════════════════════════════════════

SCHEMAS = {
    "trade": {
        "schema": pa.schema([
            ("E", pa.int64()),
            ("T", pa.int64()),
            ("s", pa.string()),
            ("t", pa.int64()),
            ("p", pa.string()),
            ("q", pa.string()),
            ("X", pa.string()),
            ("m", pa.bool_()),
        ]),
        "fields": ("E", "T", "s", "t", "p", "q", "X", "m"),
        "name": "trades",
    },
    "depthUpdate": {
        "schema": pa.schema([
            ("E", pa.int64()),
            ("T", pa.int64()),
            ("s", pa.string()),
            ("U", pa.int64()),
            ("u", pa.int64()),
            ("pu", pa.int64()),
            ("b", pa.string()),
            ("a", pa.string()),
        ]),
        "fields": ("E", "T", "s", "U", "u", "pu", "b", "a"),
        "name": "depth",
    },
}


class DataManager:

    def __init__(
        self,
        data_dir: str,
        market_type: str,
        conn_id: int,
        flush_count: int = 5_000,
        flush_interval: float = 60.0,
    ):
        self._base_dir = Path(data_dir) / market_type
        self._market_type = market_type
        self._conn_id = conn_id
        self._flush_count = flush_count
        self._flush_interval = flush_interval

        self._buffers: dict[str, deque[str]] = {}
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._writer_lock = threading.Lock()

        self._current_hour: str = ""
        self._running = False

        self.total_received: int = 0
        self.total_flushed: int = 0
        self.total_write_time: float = 0.0
        self.total_write_ops: int = 0
        self.write_errors: int = 0

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

    def add_symbol(self, symbol: str):
        if symbol not in self._buffers:
            self._buffers[symbol] = deque()

    def add(self, raw: str) -> bool:
        symbol = self.extract_symbol(raw)
        if symbol is None:
            return False

        buf = self._buffers.get(symbol)
        if buf is None:
            return False

        buf.append(raw)
        self.total_received += 1

        if len(buf) >= self._flush_count:
            self._flush_symbol(symbol)

        return True

    def _now(self) -> tuple[str, str]:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.strftime("%H")

    def _check_hour_rotation(self):
        date, hour = self._now()
        current = f"{date}/{hour}"
        if current != self._current_hour:
            if self._current_hour:
                self.flush_all()
            self._current_hour = current

    def _flush_symbol(self, symbol: str):
        buf = self._buffers.get(symbol)
        if not buf:
            return

        self._check_hour_rotation()

        snapshot = list(buf)
        buf.clear()
        self.total_flushed += len(snapshot)

        date, hour = self._now()
        self._executor.submit(
            self._write_parquet, symbol, snapshot, date, hour
        )

    def _write_parquet(self, symbol: str, rows_raw: list[str], date: str, hour: str):
        try:
            by_type: dict[str, list[dict]] = defaultdict(list)
            for raw in rows_raw:
                d = orjson.loads(raw)
                msg_type = d.get("e")
                if msg_type and msg_type in SCHEMAS:
                    by_type[msg_type].append(d)

            for msg_type, rows in by_type.items():
                config = SCHEMAS[msg_type]

                if msg_type == "depthUpdate":
                    for r in rows:
                        r["b"] = orjson.dumps(r["b"]).decode()
                        r["a"] = orjson.dumps(r["a"]).decode()

                columns = {f: [r[f] for r in rows] for f in config["fields"]}
                table = pa.table(columns, schema=config["schema"])

                symbol_dir = self._base_dir / symbol / date
                symbol_dir.mkdir(parents=True, exist_ok=True)

                filename = f"{hour}-{config['name']}_{self._conn_id}.parquet"
                filepath = symbol_dir / filename

                with self._writer_lock:
                    if filepath.exists():
                        # старый способ: читаем + конкатенируем + перезаписываем
                        existing = pq.read_table(filepath)
                        table = pa.concat_tables([existing, table])
                        pq.write_table(table, filepath)
                    else:
                        pq.write_table(table, filepath)

        except Exception as e:
            logger.error(f"Write error [{symbol}]: {e}")
    def flush_all(self):
        for symbol in list(self._buffers.keys()):
            self._flush_symbol(symbol)

    def wait_complete(self):
        self._executor.shutdown(wait=True)

    def get_file_stats(self) -> dict:
        files = list(self._base_dir.rglob("*.parquet"))
        total_size = sum(f.stat().st_size for f in files)
        return {
            "files": len(files),
            "size_mb": total_size / 1024 / 1024,
        }

    def get_buffer_memory(self) -> int:
        return sum(sum(len(s) for s in b) for b in self._buffers.values())


# ══════════════════════════════════════════════════════════
# Генерация данных
# ══════════════════════════════════════════════════════════

def gen_symbols(count: int) -> list[str]:
    bases = ["btc", "eth", "sol", "bnb", "xrp", "ada", "doge", "dot", "avax", "link",
             "ltc", "uni", "atom", "etc", "xlm", "fil", "trx", "near", "algo", "vet",
             "sand", "mana", "axs", "grt", "aave", "mkr", "comp", "snx", "crv", "yfi"]
    symbols = []
    for i in range(count):
        base = bases[i % len(bases)]
        suffix = f"{i // len(bases)}" if i >= len(bases) else ""
        symbols.append(f"{base}{suffix}usdt")
    return symbols


def gen_trade(symbol: str, seq: int) -> str:
    return orjson.dumps({
        "e": "trade",
        "E": 1772620232139 + seq,
        "T": 1772620232139 + seq,
        "s": symbol.upper(),
        "t": 7381676580 + seq,
        "p": f"{random.uniform(0.001, 70000):.4f}",
        "q": f"{random.uniform(0.001, 100):.3f}",
        "X": random.choice(["MARKET", "LIMIT"]),
        "m": random.choice([True, False]),
    }).decode()


def gen_depth(symbol: str, seq: int) -> str:
    def levels(n):
        return [
            [f"{random.uniform(0.001, 70000):.4f}", str(random.randint(1, 5000))]
            for _ in range(n)
        ]
    return orjson.dumps({
        "e": "depthUpdate",
        "E": 1772620475640 + seq,
        "T": 1772620475557 + seq,
        "s": symbol.upper(),
        "U": 10037133442374 + seq,
        "u": 10037133455683 + seq,
        "pu": 10037133427224 + seq,
        "b": levels(random.randint(3, 15)),
        "a": levels(random.randint(3, 15)),
    }).decode()


# ══════════════════════════════════════════════════════════
# Бенчмарк
# ══════════════════════════════════════════════════════════

def run_benchmark():
    # ── параметры ──
    NUM_SYMBOLS = 300
    MSG_PER_SEC = 10_000
    DURATION_SEC = 30
    FLUSH_COUNT = 300        # ~300 msg на символ → flush каждые ~9 сек
    FLUSH_INTERVAL = 10.0    # таймерный flush каждые 10 сек
    TRADE_RATIO = 0.3      # 60% trades, 40% depth

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)   # создаём если нет, не удаляем


    print("=" * 70)
    print("БЕНЧМАРК DataManager — реалистичный поток")
    print("=" * 70)
    print(f"Символов:               {NUM_SYMBOLS}")
    print(f"msg/sec суммарно:       {MSG_PER_SEC:,}")
    print(f"msg/sec на символ:      {MSG_PER_SEC/NUM_SYMBOLS:.1f}")
    print(f"Длительность:           {DURATION_SEC} сек")
    print(f"Всего сообщений:        {MSG_PER_SEC * DURATION_SEC:,}")
    print(f"flush_count:            {FLUSH_COUNT}")
    print(f"flush_interval:         {FLUSH_INTERVAL}s")
    print(f"Trade/Depth:            {TRADE_RATIO:.0%}/{1-TRADE_RATIO:.0%}")
    print()

    symbols = gen_symbols(NUM_SYMBOLS)

    dm = DataManager(
        data_dir=str(data_dir),
        market_type="futures",
        conn_id=1,
        flush_count=FLUSH_COUNT,
        flush_interval=FLUSH_INTERVAL,
    )
    for sym in symbols:
        dm.add_symbol(sym)

    # ── прогрев ──
    print("Прогрев...")
    for i, sym in enumerate(symbols[:20]):
        dm.add(gen_trade(sym, i))
        dm.add(gen_depth(sym, i))
    print("Готово.")
    print()

    # ── основной цикл ──
    print("-" * 70)
    print(
        f"{'Сек':>4} | "
        f"{'add ms':>8} | "
        f"{'msg/sec':>8} | "
        f"{'in_mem':>8} | "
        f"{'flushed':>10} | "
        f"{'auto_fl':>8} | "
        f"{'RAM MB':>8}"
    )
    print("-" * 70)

    gc.collect()

    add_times = []
    prev_flushed = 0
    seq = 0
    total_start = time.perf_counter()

    for sec in range(DURATION_SEC):
        sec_start = time.perf_counter()

        # генерируем и добавляем batch за эту секунду
        for _ in range(MSG_PER_SEC):
            sym = symbols[seq % NUM_SYMBOLS]
            if random.random() < TRADE_RATIO:
                raw = gen_trade(sym, seq)
            else:
                raw = gen_depth(sym, seq)
            dm.add(raw)
            seq += 1

        add_elapsed = time.perf_counter() - sec_start
        add_times.append(add_elapsed)

        # таймерный flush (каждые FLUSH_INTERVAL секунд)
        if (sec + 1) % int(FLUSH_INTERVAL) == 0:
            dm.flush_all()

        in_memory = sum(len(b) for b in dm._buffers.values())
        auto_flush = dm.total_flushed - prev_flushed
        ram_mb = dm.get_buffer_memory() / 1024 / 1024
        prev_flushed = dm.total_flushed

        print(
            f"{sec+1:>4} | "
            f"{add_elapsed*1000:>8.1f} | "
            f"{MSG_PER_SEC:>8,} | "
            f"{in_memory:>8,} | "
            f"{dm.total_flushed:>10,} | "
            f"{auto_flush:>8,} | "
            f"{ram_mb:>8.1f}"
        )

    print("-" * 70)
    print()

    # ── финальный flush ──
    print("Финальный flush...")
    flush_start = time.perf_counter()
    dm.flush_all()
    dm.wait_complete()
    flush_wait = time.perf_counter() - flush_start

    total_elapsed = time.perf_counter() - total_start
    file_stats = dm.get_file_stats()

    # ── итоги ──
    print()
    print("=" * 70)
    print("ИТОГИ")
    print("=" * 70)
    print()
    print(f"{'Общее время:':<30} {total_elapsed:.2f}s")
    print(f"{'Финальный flush:':<30} {flush_wait*1000:.1f}ms")
    print()
    print(f"{'Получено:':<30} {dm.total_received:,}")
    print(f"{'Записано:':<30} {dm.total_flushed:,}")
    print(f"{'Потеряно:':<30} {dm.total_received - dm.total_flushed:,}")
    print(f"{'Ошибок записи:':<30} {dm.write_errors}")
    print(f"{'Операций write_parquet:':<30} {dm.total_write_ops:,}")
    print()
    print(f"{'Файлов создано:':<30} {file_stats['files']:,}")
    print(f"{'Размер на диске:':<30} {file_stats['size_mb']:.1f} MB")
    print()

    avg_add = sum(add_times) / len(add_times)
    max_add = max(add_times)
    avg_write = dm.total_write_time / max(dm.total_write_ops, 1)

    print("-" * 70)
    print("ПРОИЗВОДИТЕЛЬНОСТЬ")
    print("-" * 70)
    print()
    print(f"{'add() avg/sec:':<30} {avg_add*1000:.2f}ms")
    print(f"{'add() max/sec:':<30} {max_add*1000:.2f}ms")
    print(f"{'add() на 1 msg:':<30} {avg_add/MSG_PER_SEC*1e6:.2f} μs")
    print(f"{'write_parquet avg:':<30} {avg_write*1000:.1f}ms")
    print(f"{'Event loop занят:':<30} {avg_add/1.0*100:.2f}%")
    print()

    # проверка записанных данных
    print("-" * 70)
    print("ПРОВЕРКА ДАННЫХ")
    print("-" * 70)
    print()
    sample_files = list((data_dir / "futures").rglob("*.parquet"))[:5]
    for f in sample_files:
        t = pq.read_table(f)
        print(f"  {f.relative_to(data_dir)}")
        print(f"    строк: {t.num_rows:,} | колонок: {len(t.schema)}")
    print()

    # shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    run_benchmark()