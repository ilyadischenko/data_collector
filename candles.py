"""
candles.py — свечной график из файла трейдов

Использование:
    python candles.py trades.parquet
    python candles.py trades.parquet --interval 1m
    python candles.py trades.parquet --interval 5m
    python candles.py trades.parquet --interval 1h

Интервалы: 1m, 3m, 5m, 15m, 30m, 1h, 4h
"""

import sys
from pathlib import Path
import argparse

import pandas as pd
import pyarrow.parquet as pq
import plotly.graph_objects as go


INTERVALS = {
    "1m":  "1min",
    "3m":  "3min",
    "5m":  "5min",
    "15m": "15min",
    "30m": "30min",
    "1h":  "1h",
    "4h":  "4h",
}


def load_trades(path: Path) -> pd.DataFrame:
    df = pq.read_table(path).to_pandas()

    # E — event time в миллисекундах
    df["time"] = pd.to_datetime(df["E"], unit="ms", utc=True)

    # q может быть строкой со знаком минус (продажи)
    df["price"] = df["p"].astype(float)
    df["qty"]   = df["q"].astype(str).str.replace("-", "").astype(float)
    df["side"]  = df["q"].astype(str).str.startswith("-").map({True: "sell", False: "buy"})

    return df.sort_values("time")


def build_candles(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    freq = INTERVALS[interval]
    df = df.set_index("time")

    ohlcv = df["price"].resample(freq).ohlc()
    volume = df["qty"].resample(freq).sum()
    buy_vol  = df[df["side"] == "buy"]["qty"].resample(freq).sum()
    sell_vol = df[df["side"] == "sell"]["qty"].resample(freq).sum()

    candles = ohlcv.copy()
    candles["volume"]   = volume
    candles["buy_vol"]  = buy_vol.fillna(0)
    candles["sell_vol"] = sell_vol.fillna(0)

    return candles.dropna(subset=["open"])


def plot(candles: pd.DataFrame, title: str, interval: str):
    colors = {
        "bg":       "#0d0d0d",
        "grid":     "#1a1a1a",
        "up":       "#00d4aa",
        "down":     "#ff4d6d",
        "vol_buy":  "rgba(0, 212, 170, 0.5)",
        "vol_sell": "rgba(255, 77, 109, 0.5)",
        "text":     "#888888",
    }

    is_up = candles["close"] >= candles["open"]

    fig = go.Figure()

    # ── свечи ─────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=candles.index,
        open=candles["open"],
        high=candles["high"],
        low=candles["low"],
        close=candles["close"],
        increasing=dict(line=dict(color=colors["up"],   width=1), fillcolor=colors["up"]),
        decreasing=dict(line=dict(color=colors["down"], width=1), fillcolor=colors["down"]),
        name="Price",
        yaxis="y",
    ))

    # ── объём покупок ──────────────────────────────────────
    fig.add_trace(go.Bar(
        x=candles.index,
        y=candles["buy_vol"],
        marker_color=colors["vol_buy"],
        name="Buy vol",
        yaxis="y2",
        showlegend=True,
    ))

    # ── объём продаж ───────────────────────────────────────
    fig.add_trace(go.Bar(
        x=candles.index,
        y=candles["sell_vol"],
        marker_color=colors["vol_sell"],
        name="Sell vol",
        yaxis="y2",
        showlegend=True,
    ))

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family="'Courier New', monospace", size=14, color="#666"),
            x=0.01,
        ),
        paper_bgcolor=colors["bg"],
        plot_bgcolor=colors["bg"],
        font=dict(family="'Courier New', monospace", color=colors["text"]),

        xaxis=dict(
            rangeslider=dict(visible=False),
            gridcolor=colors["grid"],
            showgrid=True,
            zeroline=False,
            color=colors["text"],
        ),
        yaxis=dict(
            domain=[0.3, 1.0],
            gridcolor=colors["grid"],
            showgrid=True,
            zeroline=False,
            color=colors["text"],
            side="right",
        ),
        yaxis2=dict(
            domain=[0.0, 0.25],
            gridcolor=colors["grid"],
            showgrid=False,
            zeroline=False,
            color=colors["text"],
            side="right",
            title=dict(text="Volume", font=dict(size=10)),
        ),

        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11),
            x=0.01, y=0.99,
        ),
        margin=dict(l=20, r=60, t=40, b=20),
        barmode="overlay",
        hovermode="x unified",
    )

    out_path = Path("candles.html")
    fig.write_html(str(out_path))
    print(f"Сохранено → {out_path.resolve()}")
    fig.show()


def main():
    parser = argparse.ArgumentParser(description="Свечной график из трейдов")
    parser.add_argument("file", nargs="?", help="Путь к parquet файлу (по умолчанию ищет *trades*.parquet рядом)")
    parser.add_argument("--interval", "-i", default="1m", choices=INTERVALS.keys(), help="Интервал свечи (default: 1m)")
    args = parser.parse_args()

    # ищем файл
    if args.file:
        path = Path(args.file)
    else:
        candidates = list(Path(".").glob("*trades*.parquet"))
        if not candidates:
            candidates = list(Path(".").glob("*.parquet"))
        if not candidates:
            print("Файл не найден. Укажи путь: python candles.py path/to/trades.parquet")
            sys.exit(1)
        path = candidates[0]
        print(f"Файл: {path}")

    if not path.exists():
        print(f"Файл не найден: {path}")
        sys.exit(1)

    print(f"Загружаю {path.name}...")
    df = load_trades(path)
    print(f"Трейдов: {len(df):,} | с {df['time'].min()} по {df['time'].max()}")

    print(f"Строю свечи {args.interval}...")
    candles = build_candles(df, args.interval)
    print(f"Свечей: {len(candles):,}")

    symbol = path.stem.split("-")[0] if "-" in path.stem else path.stem
    title  = f"{symbol.upper()} · {args.interval}"
    plot(candles, title, args.interval)


# import pyarrow.parquet as pq
# df = pq.read_table("14_trades.parquet").to_pandas()
# print(df.dtypes)
# print(df.head(3).to_string())

if __name__ == "__main__":
    main()
