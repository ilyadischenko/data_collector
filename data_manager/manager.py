

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ASSEMBLE_CONFIGS = {
    "trades": {"sort_by": "E", "dedup_col": "t"},        # по trade id
    "depth":  {"sort_by": "E", "dedup_col": ["U", "u"]}, # по диапазону обновлений
    "ob_snapshot": {"sort_by": "ts", "dedup_col": None},  # без дедупа
}

class DataManager:
    def __init__(self, data_dir: str = '../data', interval: float = 60.0):
        self.data_dir = Path(data_dir)
        self.interval = interval
        self._executor = ThreadPoolExecutor(max_workers=4)

        self.compression = "zstd"
        self.compression_level = 9


    def _get_target_hour(self) -> tuple[str, str]:
        """Возвращает дату и час который уже закрыт (прошлый час)."""
        dt = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H")
    
    def _find_dirs(self, date: str, hour: str) -> list[Path]:
        """
        Возвращает список date-папок где есть чанки за указанный час.
        """
        result = []

        for market_dir in self.data_dir.iterdir():
            if not market_dir.is_dir():
                continue

            for symbol_dir in market_dir.iterdir():
                if not symbol_dir.is_dir():
                    continue

                date_dir = symbol_dir / date
                if not date_dir.exists():
                    continue

                if any(date_dir.glob(f"{hour}-*-*-*.parquet")):
                    result.append(date_dir)

        logger.info(f"Найдено {len(result)} папок с чанками за {date}/{hour}")
        print(f"Найдено {len(result)} папок с чанками за {date}/{hour}")

        return result
    
    async def assembling_file(self, date_dir: Path, hour: str):
        chunk_groups: dict[str, list[Path]] = {}
        for chunk in date_dir.glob(f"{hour}-*-*.parquet"):
            parts = chunk.stem.split("-")
            if len(parts) <= 2:
                continue
            name = parts[1]
            chunk_groups.setdefault(name, []).append(chunk)

        if not chunk_groups:
            logger.info(f"1111.  Нет чанков за {date_dir}/{hour}")
            return

        assemblers = {
            "trades": self.assemble_trades,
            "depth": self.assemble_depth,
            "ob_snapshot": self.assemble_ob_snapshot,
        }

        loop = asyncio.get_event_loop()
        tasks = []
        for name, assembler in assemblers.items():
            if name in chunk_groups:
                tasks.append(
                    loop.run_in_executor(self._executor, assembler, date_dir, hour)
                )

        if tasks:
            await asyncio.gather(*tasks)

    async def assembling_loop(self):
        while True:
            date, hour = self._get_target_hour()

            await self.assemble_hour(date, hour)

            # ждём до начала следующего часа
            now = datetime.now(tz=timezone.utc)
            next_hour = (now + timedelta(hours=1)).replace(minute=1, second=0, microsecond=0)
            wait = (next_hour - now).total_seconds()
            logger.info(f"Следующая сборка через {wait:.0f}с")
            await asyncio.sleep(wait)

    async def assemble_hour(self, date: str, hour: str):
        date_dirs = await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self._find_dirs, date, hour
        )
        tasks = [self.assembling_file(d, hour) for d in date_dirs]
        if tasks:
            await asyncio.gather(*tasks)
            logger.info(f"Сборка завершена: {len(tasks)} папок за {date}/{hour}")
        else:
            logger.info(f"Нет папок для сборки за {date}/{hour}")

    def assemble_trades(self, date_dir: Path, hour: str):
        chunks = list(date_dir.glob(f"{hour}-trades-*-*.parquet"))

        if not chunks:
            # logger.info(f"[trades] Нет чанков в {date_dir} за час {hour}")
            return

        # logger.info(f"[trades] Найдено {len(chunks)} чанков:")
        tables = []
        for c in chunks:
            t = pq.read_table(c)
            # logger.info(f"  {c.name}: {len(t)} строк")
            tables.append(t)

        combined = pa.concat_tables(tables).to_pandas()
        # logger.info(f"[trades] Всего после объединения: {len(combined)} строк")

        before = len(combined)
        combined = combined.drop_duplicates(subset="t")
        removed = before - len(combined)
        # logger.info(f"[trades] После дедупа по t: {len(combined)} строк, удалено: {removed}")

        combined['q'] = combined.apply(
            lambda row: '-' + row['q'] if row['m'] else row['q'], axis=1
        )
        combined = combined.drop(columns=['m'])

        combined = combined.sort_values("t").reset_index(drop=True)
        # logger.info(f"[trades] Отсортировано по t, монотонно: {combined['t'].is_monotonic_increasing}")

        out_path = date_dir / f"{hour}-trades.parquet"
        pq.write_table(pa.Table.from_pandas(combined, preserve_index=False), out_path, compression=self.compression, compression_level=self.compression_level)
        # logger.info(f"[trades] Сохранён {out_path}")

        for chunk in chunks:
            chunk.unlink()
        # logger.info(f"[trades] Удалено {len(chunks)} чанков")

    def assemble_depth(self, date_dir: Path, hour: str):
        chunks = list(date_dir.glob(f"{hour}-depth-*-*.parquet"))
        if not chunks:
            # logger.info(f"[depth] Нет чанков в {date_dir} за час {hour}")
            return

        # logger.info(f"[depth] Найдено {len(chunks)} чанков:")
        tables = []
        for c in chunks:
            t = pq.read_table(c)
            # logger.info(f"  {c.name}: {len(t)} строк")
            tables.append(t)

        combined = pa.concat_tables(tables).to_pandas()
        # logger.info(f"[depth] Всего после объединения: {len(combined)} строк")

        before = len(combined)
        combined = combined.drop_duplicates(subset=["E", "U", "u"])
        removed = before - len(combined)
        # logger.info(f"[depth] После дедупа: {len(combined)} строк, удалено: {removed}")

        combined = combined.sort_values("E").reset_index(drop=True)

        b_parsed = combined['b'].map(orjson.loads)
        a_parsed = combined['a'].map(orjson.loads)

        b_p = [[float(p) for p, q in levels] for levels in b_parsed]
        b_q = [[float(q) for p, q in levels] for levels in b_parsed]
        a_p = [[float(p) for p, q in levels] for levels in a_parsed]
        a_q = [[float(q) for p, q in levels] for levels in a_parsed]

        schema = pa.schema([
            pa.field("E",   pa.int64()),
            pa.field("U",   pa.int64()),
            pa.field("u",   pa.int64()),
            pa.field("b_p", pa.list_(pa.float64())),
            pa.field("b_q", pa.list_(pa.float64())),
            pa.field("a_p", pa.list_(pa.float64())),
            pa.field("a_q", pa.list_(pa.float64())),
        ])

        table = pa.table({
            "E":   pa.array(combined["E"].tolist(), type=pa.int64()),
            "U":   pa.array(combined["U"].tolist(), type=pa.int64()),
            "u":   pa.array(combined["u"].tolist(), type=pa.int64()),
            "b_p": pa.array(b_p, type=pa.list_(pa.float64())),
            "b_q": pa.array(b_q, type=pa.list_(pa.float64())),
            "a_p": pa.array(a_p, type=pa.list_(pa.float64())),
            "a_q": pa.array(a_q, type=pa.list_(pa.float64())),
        }, schema=schema)

        out_path = date_dir / f"{hour}-depth.parquet"
        pq.write_table(table, out_path, compression=self.compression, compression_level=self.compression_level)

        for chunk in chunks:
            chunk.unlink()
        # logger.info(f"[depth] Собран {out_path.name}: {len(combined)} строк из {len(chunks)} чанков")

    def assemble_ob_snapshot(self, date_dir: Path, hour: str):
        chunks = list(date_dir.glob(f"{hour}-ob_snapshot-*.parquet"))
        if not chunks:
            # logger.info(f"[ob_snapshot] Нет чанков в {date_dir} за час {hour}")
            return

        # logger.info(f"[ob_snapshot] Найдено {len(chunks)} чанков:")
        tables = []
        for c in chunks:
            t = pq.read_table(c)
            # logger.info(f"  {c.name}: {len(t)} строк")
            tables.append(t)

        combined = pa.concat_tables(tables).to_pandas()
        # logger.info(f"[ob_snapshot] Всего после объединения: {len(combined)} строк")

        combined = combined.sort_values("ts").reset_index(drop=True)

        bids_parsed = combined['bids'].map(orjson.loads)
        asks_parsed = combined['asks'].map(orjson.loads)

        b_p = [[float(p) for p, q in levels] for levels in bids_parsed]
        b_q = [[float(q) for p, q in levels] for levels in bids_parsed]
        a_p = [[float(p) for p, q in levels] for levels in asks_parsed]
        a_q = [[float(q) for p, q in levels] for levels in asks_parsed]

        schema = pa.schema([
            pa.field("ts",           pa.int64()),
            pa.field("lastUpdateId", pa.int64()),
            pa.field("b_p", pa.list_(pa.float64())),
            pa.field("b_q", pa.list_(pa.float64())),
            pa.field("a_p", pa.list_(pa.float64())),
            pa.field("a_q", pa.list_(pa.float64())),
        ])

        table = pa.table({
            "ts":           pa.array(combined["ts"].tolist(),           type=pa.int64()),
            "lastUpdateId": pa.array(combined["lastUpdateId"].tolist(), type=pa.int64()),
            "b_p": pa.array(b_p, type=pa.list_(pa.float64())),
            "b_q": pa.array(b_q, type=pa.list_(pa.float64())),
            "a_p": pa.array(a_p, type=pa.list_(pa.float64())),
            "a_q": pa.array(a_q, type=pa.list_(pa.float64())),
        }, schema=schema)

        out_path = date_dir / f"{hour}-ob_snapshot.parquet"
        pq.write_table(table, out_path, compression="zstd", compression_level=9)

        for chunk in chunks:
            chunk.unlink()
        # logger.info(f"[ob_snapshot] Собран {out_path.name}: {len(combined)} строк из {len(chunks)} чанков")

    async def run(self):
        while True:
            date, hour = self._get_target_hour()

            await self.assemble_hour(date, hour)

            # ждём до начала следующего часа
            now = datetime.now(tz=timezone.utc)
            next_hour = (now + timedelta(hours=1)).replace(minute=1, second=0, microsecond=0)
            wait = (next_hour - now).total_seconds()
            logger.info(f"Следующая сборка через {wait:.0f}с")
            await asyncio.sleep(wait)
        

symb = 'zrousdt'
market_type = 'spot'
hour = '10'

async def main():
    import datetime
    dm = DataManager(data_dir='../data')
    
    # date_dir = Path(f'../data/{market_type}/{symb}/2026-03-13')
    # await dm.assembling_file(date_dir, hour)
    start = datetime.datetime.now()
    await dm.assemble_hour('2026-03-13', hour)
    print(f"Сборка за {hour} завершена за {(datetime.datetime.now() - start).total_seconds():.2f} секунд")

# asyncio.run(main())

# import pandas as pd

# async def main():
#     dm = DataManager(data_dir='../data')
#     date_dir = Path(f'../data/{market_type}/{symb}/2026-03-13')
#     await dm.assembling_file(date_dir, hour)

# asyncio.run(main())

# # просмотр и сохранение в CSV
# df = pq.read_table(f'../data/{market_type}/{symb}/2026-03-13/{hour}-trades.parquet').to_pandas()

# print(df.shape)
# print(df.head(20).to_string())
# # print(f"Дубли по t: {df['t'].duplicated().sum()}")
# # print(f"Отсортировано по E: {df['E'].is_monotonic_increasing}")

# df.to_csv('./trades_view.csv', index=False)
# # print("Сохранено -> trades_view.csv")

# df = pq.read_table(f'../data/{market_type}/{symb}/2026-03-13/{hour}-depth.parquet').to_pandas()
# df.to_csv('./depth_view.csv', index=False)

# df = pq.read_table(f'../data/{market_type}/{symb}/2026-03-13/{hour}-ob_snapshot.parquet').to_pandas()
# df.to_csv('./ob_snapshot_view.csv', index=False)