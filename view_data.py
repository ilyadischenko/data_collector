#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path

import orjson
import pandas as pd
import pyarrow.parquet as pq

FILE_PATTERN = re.compile(r'^(\d{2})-(trades|depth)[-_].*\.parquet$')


def parse_filename(name: str):
    m = FILE_PATTERN.match(name)
    if m:
        return m.group(1), m.group(2)
    return None


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


def read_and_concat(files):
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
        print(f"  Нет trade-файлов (date={date}, hour={hour})")
        return pd.DataFrame()
    print(f"  Найдено {len(files)} trade-файлов")
    df = read_and_concat(files)
    if df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=["t"], keep="first")
    after = len(df)
    if before != after:
        print(f"  Дедупликация: {before:,} → {after:,}")
    df["price"] = df["p"].astype(float)
    df["qty"] = df["q"].astype(float)
    df["time"] = pd.to_datetime(df["E"], unit="ms", utc=True)
    df = df.sort_values("time").reset_index(drop=True)
    return df


def load_depth(symbol_dir, date=None, hour=None):
    files = find_files(symbol_dir, "depth", date, hour)
    if not files:
        print(f"  Нет depth-файлов (date={date}, hour={hour})")
        return pd.DataFrame()
    print(f"  Найдено {len(files)} depth-файлов")
    df = read_and_concat(files)
    if df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=["U", "u"], keep="first")
    after = len(df)
    if before != after:
        print(f"  Дедупликация: {before:,} → {after:,}")
    df["time"] = pd.to_datetime(df["E"], unit="ms", utc=True)
    df = df.sort_values("time").reset_index(drop=True)
    return df


def make_candles(trades_df, interval="1min"):
    df = trades_df.set_index("time")
    ohlcv = df["price"].resample(interval).ohlc()
    ohlcv["volume"] = df["qty"].resample(interval).sum()
    ohlcv["buy_vol"] = df.loc[~df["m"], "qty"].resample(interval).sum()
    ohlcv["sell_vol"] = df.loc[df["m"], "qty"].resample(interval).sum()
    ohlcv = ohlcv.dropna(subset=["open"]).fillna(0)
    return ohlcv


def plot_candles(ohlcv, symbol, interval, label):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, row_heights=[0.75, 0.25])
    fig.add_trace(go.Candlestick(
        x=ohlcv.index, open=ohlcv["open"], high=ohlcv["high"],
        low=ohlcv["low"], close=ohlcv["close"], name="Price",
    ), row=1, col=1)
    fig.add_trace(go.Bar(x=ohlcv.index, y=ohlcv["buy_vol"],
                         name="Buy", marker_color="rgba(38,166,91,0.6)"), row=2, col=1)
    fig.add_trace(go.Bar(x=ohlcv.index, y=ohlcv["sell_vol"],
                         name="Sell", marker_color="rgba(239,83,80,0.6)"), row=2, col=1)
    fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False,
                      barmode="stack", height=800,
                      title=f"{symbol.upper()} {interval} | {label}")
    fig.show()

def plot_depth_heatmap(depth_df, symbol, label, max_events=300, levels=25):
    """
    Единая ось цен — mid price двигается по графику.
    """
    import numpy as np
    import plotly.graph_objects as go
    import orjson

    if len(depth_df) > max_events:
        print(f"  Берём последние {max_events} из {len(depth_df):,}")
        depth_df = depth_df.tail(max_events).reset_index(drop=True)

    full_bids = {}
    full_asks = {}

    snapshots = []
    mids = []
    times = []

    for _, row in depth_df.iterrows():
        bids = orjson.loads(row["b"]) if isinstance(row["b"], str) else row["b"]
        asks = orjson.loads(row["a"]) if isinstance(row["a"], str) else row["a"]

        for b in bids:
            price, qty = float(b[0]), float(b[1])
            if qty == 0:
                full_bids.pop(price, None)
            else:
                full_bids[price] = qty

        for a in asks:
            price, qty = float(a[0]), float(a[1])
            if qty == 0:
                full_asks.pop(price, None)
            else:
                full_asks[price] = qty

        best_bid = max(full_bids.keys()) if full_bids else 0
        best_ask = min(full_asks.keys()) if full_asks else 0
        mid = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask

        snap = {}
        for p, v in full_bids.items():
            snap[p] = (v, 'bid')
        for p, v in full_asks.items():
            snap[p] = (v, 'ask')

        snapshots.append(snap)
        mids.append(mid)
        times.append(row["time"])

    if not times:
        print("  Нет данных")
        return

    all_prices = set()
    for snap in snapshots:
        all_prices.update(snap.keys())

    min_mid, max_mid = min(mids), max(mids)

    sorted_prices = sorted(all_prices)
    if len(sorted_prices) > 1:
        diffs = [sorted_prices[i+1] - sorted_prices[i] for i in range(min(100, len(sorted_prices)-1))]
        tick = np.median(diffs)
    else:
        tick = 0.0001

    # определяем нужную точность по tick size
    if tick >= 1:
        decimals = 0
    elif tick >= 0.1:
        decimals = 1
    elif tick >= 0.01:
        decimals = 2
    elif tick >= 0.001:
        decimals = 3
    elif tick >= 0.0001:
        decimals = 4
    elif tick >= 0.00001:
        decimals = 5
    else:
        decimals = 6

    def fmt_price(p):
        return f"{p:.{decimals}f}"

    price_range = levels * tick
    price_min = min_mid - price_range
    price_max = max_mid + price_range

    price_axis = sorted([p for p in all_prices if price_min <= p <= price_max])

    if len(price_axis) < 3:
        price_axis = sorted_prices[max(0, len(sorted_prices)//2 - levels): len(sorted_prices)//2 + levels]

    print(f"  Tick size: {tick}, decimals: {decimals}")
    print(f"  Price axis: {len(price_axis)} levels, {fmt_price(price_axis[0])} — {fmt_price(price_axis[-1])}")
    print(f"  Mid range: {fmt_price(min_mid)} — {fmt_price(max_mid)}")

    vol_matrix = np.zeros((len(price_axis), len(times)))
    side_matrix = np.zeros((len(price_axis), len(times)))

    price_to_idx = {p: i for i, p in enumerate(price_axis)}

    for t_idx, snap in enumerate(snapshots):
        for price, (vol, side) in snap.items():
            if price in price_to_idx:
                p_idx = price_to_idx[price]
                vol_matrix[p_idx, t_idx] = vol
                side_matrix[p_idx, t_idx] = 1 if side == 'bid' else -1

    vol_log = np.log1p(vol_matrix)
    max_vol_log = np.percentile(vol_log[vol_log > 0], 98) if np.any(vol_log > 0) else 1

    display_matrix = (vol_log / max_vol_log) * side_matrix

    time_strs = [t.strftime("%H:%M:%S.%f")[:-3] for t in times]
    price_labels = [fmt_price(p) for p in price_axis]

    fig = go.Figure()

    fig.add_trace(go.Heatmap(
        z=display_matrix,
        x=time_strs,
        y=price_labels,
        colorscale=[
            [0.0, "rgb(255,100,100)"],
            [0.25, "rgb(200,60,60)"],
            [0.4, "rgb(120,40,40)"],
            [0.5, "rgb(30,30,30)"],
            [0.6, "rgb(40,120,50)"],
            [0.75, "rgb(60,200,80)"],
            [1.0, "rgb(100,255,120)"],
        ],
        zmid=0,
        showscale=True,
        colorbar=dict(title="Volume", tickvals=[-1, 0, 1], ticktext=["Ask", "0", "Bid"]),
        hovertemplate="Price: %{y}<br>Time: %{x}<br>Val: %{z:.3f}<extra></extra>",
    ))

    fig.add_trace(go.Scatter(
        x=time_strs,
        y=[fmt_price(m) for m in mids],
        mode="lines",
        name="Mid Price",
        line=dict(color="yellow", width=2, dash="dot"),
    ))

    fig.update_layout(
        template="plotly_dark",
        height=800,
        title=(
            f"{symbol.upper()} Depth Heatmap | {label}<br>"
            f"{len(times)} events | mid {fmt_price(min_mid)}→{fmt_price(max_mid)}"
        ),
        yaxis_title="Price",
        xaxis_title="Time",
        yaxis=dict(type="category"),
    )

    fig.show()
    
def plot_trades(trades_df, symbol, label):
    import plotly.graph_objects as go

    df = trades_df.tail(50_000) if len(trades_df) > 50_000 else trades_df
    buys = df[~df["m"]]
    sells = df[df["m"]]

    fig = go.Figure()
    fig.add_trace(go.Scattergl(x=buys["time"], y=buys["price"], mode="markers",
                               name=f"Buy ({len(buys):,})",
                               marker=dict(color="green", size=3, opacity=0.5)))
    fig.add_trace(go.Scattergl(x=sells["time"], y=sells["price"], mode="markers",
                               name=f"Sell ({len(sells):,})",
                               marker=dict(color="red", size=3, opacity=0.5)))
    fig.update_layout(title=f"{symbol.upper()} Trades | {label}",
                      template="plotly_dark", height=600)
    fig.show()


def show_info(symbol_dir, date, hour):
    symbol = symbol_dir.name
    label = make_label(date, hour)

    print(f"\n{'='*60}")
    print(f"  Символ:  {symbol.upper()}")
    print(f"  Путь:    {symbol_dir.resolve()}")
    print(f"  Фильтр:  {label}")
    print(f"{'='*60}")

    all_parquet = list(symbol_dir.rglob("*.parquet"))
    print(f"\n  Всего parquet: {len(all_parquet)}")
    for f in all_parquet[:5]:
        p = parse_filename(f.name)
        print(f"    {f.relative_to(symbol_dir)}  → {p}")
    if len(all_parquet) > 5:
        print(f"    ... ещё {len(all_parquet) - 5}")

    # даты
    date_dirs = sorted(d for d in symbol_dir.iterdir() if d.is_dir())
    for dd in date_dirs:
        dd_files = list(dd.glob("*.parquet"))
        t_hours = sorted(set(parse_filename(f.name)[0] for f in dd_files
                             if parse_filename(f.name) and parse_filename(f.name)[1] == "trades"))
        d_hours = sorted(set(parse_filename(f.name)[0] for f in dd_files
                             if parse_filename(f.name) and parse_filename(f.name)[1] == "depth"))
        print(f"\n    📅 {dd.name}  ({len(dd_files)} файлов)")
        if t_hours: print(f"       trades: часы {', '.join(t_hours)}")
        if d_hours: print(f"       depth:  часы {', '.join(d_hours)}")

    # статистика
    trade_files = find_files(symbol_dir, "trades", date, hour)
    depth_files = find_files(symbol_dir, "depth", date, hour)

    print(f"\n  Trade файлов: {len(trade_files)}")
    if trade_files:
        df = load_trades(symbol_dir, date, hour)
        if not df.empty:
            print(f"  Строк (dedup): {len(df):,}")
            print(f"  Время: {df['time'].min()} → {df['time'].max()}")
            print(f"  Цена:  {df['price'].min():.2f} → {df['price'].max():.2f}")
            print(f"  Объём: {df['qty'].sum():,.4f}")

    print(f"\n  Depth файлов: {len(depth_files)}")
    if depth_files:
        df = load_depth(symbol_dir, date, hour)
        if not df.empty:
            print(f"  Строк (dedup): {len(df):,}")
            print(f"  Время: {df['time'].min()} → {df['time'].max()}")

    total_size = sum(f.stat().st_size for f in all_parquet)
    print(f"\n  Размер: {total_size / 1024 / 1024:.2f} MB" if total_size > 1048576
          else f"\n  Размер: {total_size / 1024:.1f} KB")
    print()


def make_label(date, hour):
    if date and hour: return f"{date} {hour.zfill(2)}:00 UTC"
    if date: return f"{date} (все часы)"
    return "все данные"


def main():
    parser = argparse.ArgumentParser(description="Просмотр parquet")
    parser.add_argument("path", help="Папка символа")
    parser.add_argument("mode", choices=["candles", "depth", "trades", "info"])
    parser.add_argument("--date", "-d", default=None)
    parser.add_argument("--hour", "-H", default=None)
    parser.add_argument("--interval", "-i", default="1min")
    args = parser.parse_args()

    symbol_dir = Path(args.path)
    if not symbol_dir.exists():
        print(f"Не найдено: {symbol_dir}")
        sys.exit(1)

    symbol = symbol_dir.name
    label = make_label(args.date, args.hour)

    if args.mode == "info":
        show_info(symbol_dir, args.date, args.hour)
    elif args.mode == "candles":
        df = load_trades(symbol_dir, args.date, args.hour)
        if df.empty: sys.exit(1)
        ohlcv = make_candles(df, args.interval)
        print(f"  {len(df):,} трейдов → {len(ohlcv):,} свечей ({args.interval})")
        plot_candles(ohlcv, symbol, args.interval, label)
    elif args.mode == "depth":
        df = load_depth(symbol_dir, args.date, args.hour)
        if df.empty: sys.exit(1)
        print(f"  {len(df):,} depth обновлений")
        plot_depth_heatmap(df, symbol, label, levels=50, max_events=1000)
    elif args.mode == "trades":
        df = load_trades(symbol_dir, args.date, args.hour)
        if df.empty: sys.exit(1)
        print(f"  {len(df):,} трейдов")
        plot_trades(df, symbol, label)


if __name__ == "__main__":
    main()