from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

def view_depth(
    symbol: str,
    market_type: str,
    date: str,
    hour: str | None = None,
    data_dir: str = '../data',
    out: str = './depth_view.csv',
):
    """
    Собирает все depth чанки для символа за дату (и опционально час),
    сортирует по E и сохраняет в CSV.
    """
    base = Path(data_dir) / market_type / symbol / date

    if not base.exists():
        print(f"Папка не найдена: {base}")
        return

    # паттерн: если час указан — только он, иначе все часы
    pattern = f"{hour}-depth-*-*.parquet" if hour else "??-depth-*-*.parquet"
    chunks = sorted(base.glob(pattern))

    if not chunks:
        # может уже собранный файл
        pattern2 = f"{hour}-depth.parquet" if hour else "??-depth.parquet"
        chunks = sorted(base.glob(pattern2))

    if not chunks:
        print(f"Файлы не найдены в {base} по паттерну {pattern}")
        return

    print(f"Найдено {len(chunks)} файлов:")
    for c in chunks:
        print(f"  {c.name}")

    tables = [pq.read_table(c) for c in chunks]
    df = pa.concat_tables(tables).to_pandas()

    df = df.sort_values("E").reset_index(drop=True)

    # читаемое время
    df["time"] = pd.to_datetime(df["E"], unit="ms", utc=True).dt.strftime("%H:%M:%S.%f")

    # переставляем колонки для удобства
    cols = ["time", "E", "s", "U", "u", "b", "a"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    df.to_csv(out, index=False)
    print(f"\nСохранено {len(df)} строк -> {out}")
    print(df.head(10).to_string())


if __name__ == "__main__":
    view_depth(
        symbol="btcusdt",
        market_type="futures",
        date="2026-03-12",
        hour="14",          # None = все часы за дату
        data_dir="./data",
        out="./depth_view.csv",
    )