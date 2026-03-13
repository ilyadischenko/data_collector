#!/usr/bin/env python3
"""
python view_table.py ./connector/data/futures/btcusdt trades -d 2026-03-05 -H 09
python view_table.py ./connector/data/futures/btcusdt trades -d 2026-03-05 -H 09 --head 50
python view_table.py ./connector/data/futures/btcusdt depth -d 2026-03-05 -H 09
python view_table.py ./connector/data/futures/btcusdt trades -d 2026-03-05 --csv output.csv
python view_table.py ./connector/data/futures/btcusdt stats -d 2026-03-05 -H 09
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)


FILE_PATTERN = re.compile(r'^(\d{2})-(trades|depth)[-_].*\.parquet$')


def parse_filename(name: str):
    m = FILE_PATTERN.match(name)
    return (m.group(1), m.group(2)) if m else None


def find_files(symbol_dir: Path, file_type: str, date=None, hour=None):
    if date:
        search_dir = symbol_dir / date
        if not search_dir.exists():
            return []
        all_parquet = sorted(search_dir.glob("*.parquet"))
    else:
        all_parquet = sorted(symbol_dir.rglob("*.parquet"))

    result = []
    hour_padded = hour.zfill(2) if hour else None

    for f in all_parquet:
        parsed = parse_filename(f.name)
        if parsed is None:
            continue
        f_hour, f_type = parsed
        if f_type != file_type:
            continue
        if hour_padded and f_hour != hour_padded:
            continue
        result.append(f)

    return result


def load_df(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pq.read_table(f).to_pandas())
        except Exception as e:
            print(f"  ⚠ {f.name}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_trades(symbol_dir, date=None, hour=None):
    files = find_files(symbol_dir, "trades", date, hour)
    if not files:
        return pd.DataFrame()
    df = load_df(files)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["t"], keep="first")
    df["price"] = df["p"].astype(float)
    df["qty"] = df["q"].astype(float)
    df["time"] = pd.to_datetime(df["E"], unit="ms", utc=True)
    df["side"] = df["m"].map({True: "SELL", False: "BUY"})
    df = df.sort_values("time").reset_index(drop=True)
    return df


def load_depth(symbol_dir, date=None, hour=None):
    files = find_files(symbol_dir, "depth", date, hour)
    if not files:
        return pd.DataFrame()
    df = load_df(files)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["U", "u"], keep="first")
    df["time"] = pd.to_datetime(df["E"], unit="ms", utc=True)
    df = df.sort_values("time").reset_index(drop=True)
    return df


def show_trades(df: pd.DataFrame, head: int, tail: int):
    cols = ["time", "side", "price", "qty", "t"]
    display_df = df[cols].copy()
    display_df["price"] = display_df["price"].map(lambda x: f"{x:.4f}")
    display_df["qty"] = display_df["qty"].map(lambda x: f"{x:.6f}")

    print(f"\n{'='*70}")
    print(f"  TRADES: {len(df):,} rows")
    print(f"{'='*70}")

    if head:
        print(f"\n  First {head} rows:")
        print(display_df.head(head).to_string(index=False))
    if tail:
        print(f"\n  Last {tail} rows:")
        print(display_df.tail(tail).to_string(index=False))
    if not head and not tail:
        print(display_df.to_string(index=False))


def show_depth(df: pd.DataFrame, head: int, tail: int):
    cols = ["time", "U", "u"]
    display_df = df[cols].copy()

    print(f"\n{'='*70}")
    print(f"  DEPTH UPDATES: {len(df):,} rows")
    print(f"{'='*70}")

    if head:
        print(f"\n  First {head} rows:")
        print(display_df.head(head).to_string(index=False))
    if tail:
        print(f"\n  Last {tail} rows:")
        print(display_df.tail(tail).to_string(index=False))
    if not head and not tail:
        print(display_df.to_string(index=False))


def show_stats(symbol_dir: Path, date: str, hour: str):
    print(f"\n{'='*70}")
    print(f"  STATS: {symbol_dir.name.upper()}")
    print(f"{'='*70}")

    # trades
    trades_df = load_trades(symbol_dir, date, hour)
    if not trades_df.empty:
        print(f"\n  TRADES:")
        print(f"    Rows:        {len(trades_df):,}")
        print(f"    Time:        {trades_df['time'].min()} → {trades_df['time'].max()}")
        print(f"    Price:       {trades_df['price'].min():.4f} → {trades_df['price'].max():.4f}")
        print(f"    Total vol:   {trades_df['qty'].sum():,.4f}")
        print(f"    Buy vol:     {trades_df[trades_df['side']=='BUY']['qty'].sum():,.4f}")
        print(f"    Sell vol:    {trades_df[trades_df['side']=='SELL']['qty'].sum():,.4f}")
        print(f"    Avg price:   {trades_df['price'].mean():.4f}")
        print(f"    Avg qty:     {trades_df['qty'].mean():.6f}")

        # группировка по минутам
        trades_df["minute"] = trades_df["time"].dt.floor("1min")
        per_min = trades_df.groupby("minute").agg(
            trades=("t", "count"),
            volume=("qty", "sum"),
            vwap=("price", lambda x: (x * trades_df.loc[x.index, "qty"]).sum() / trades_df.loc[x.index, "qty"].sum())
        )
        print(f"\n    Per-minute stats:")
        print(f"      Avg trades/min:  {per_min['trades'].mean():.1f}")
        print(f"      Max trades/min:  {per_min['trades'].max()}")
        print(f"      Avg volume/min:  {per_min['volume'].mean():.4f}")

    # depth
    depth_df = load_depth(symbol_dir, date, hour)
    if not depth_df.empty:
        print(f"\n  DEPTH:")
        print(f"    Rows:        {len(depth_df):,}")
        print(f"    Time:        {depth_df['time'].min()} → {depth_df['time'].max()}")
        
        # updates per second
        depth_df["second"] = depth_df["time"].dt.floor("1s")
        per_sec = depth_df.groupby("second").size()
        print(f"    Avg updates/sec: {per_sec.mean():.1f}")
        print(f"    Max updates/sec: {per_sec.max()}")


def main():
    parser = argparse.ArgumentParser(description="Табличный просмотр parquet")
    parser.add_argument("path", help="Папка символа")
    parser.add_argument("mode", choices=["trades", "depth", "stats"])
    parser.add_argument("--date", "-d", default=None)
    parser.add_argument("--hour", "-H", default=None)
    parser.add_argument("--head", type=int, default=20, help="Первые N строк")
    parser.add_argument("--tail", type=int, default=20, help="Последние N строк")
    parser.add_argument("--all", action="store_true", help="Все строки")
    parser.add_argument("--csv", type=str, default=None, help="Экспорт в CSV")
    parser.add_argument("--parquet", type=str, default=None, help="Экспорт в parquet")

    args = parser.parse_args()

    symbol_dir = Path(args.path)
    if not symbol_dir.exists():
        print(f"Не найдено: {symbol_dir}")
        sys.exit(1)

    if args.mode == "stats":
        show_stats(symbol_dir, args.date, args.hour)
        return

    # загрузка
    if args.mode == "trades":
        df = load_trades(symbol_dir, args.date, args.hour)
    else:
        df = load_depth(symbol_dir, args.date, args.hour)

    if df.empty:
        print(f"Нет данных (date={args.date}, hour={args.hour})")
        sys.exit(1)

    # экспорт
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"Сохранено в {args.csv} ({len(df):,} rows)")
        return

    if args.parquet:
        df.to_parquet(args.parquet, index=False)
        print(f"Сохранено в {args.parquet} ({len(df):,} rows)")
        return

    # вывод
    head = 0 if args.all else args.head
    tail = 0 if args.all else args.tail

    if args.mode == "trades":
        if args.all:
            show_trades(df, head=0, tail=0)
        else:
            show_trades(df, head=head, tail=tail)
    else:
        if args.all:
            show_depth(df, head=0, tail=0)
        else:
            show_depth(df, head=head, tail=tail)


if __name__ == "__main__":
    main()