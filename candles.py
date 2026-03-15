"""
cloud_candles.py — свечной график из трейдов в облаке + сравнение с Binance API

Использование:
    python cloud_candles.py --symbol btcusdt --market futures --date 2026-03-14 --from 6 --to 10
    python cloud_candles.py --symbol btcusdt --market futures --date 2026-03-14 --from 6 --to 10 --interval 5m
    python cloud_candles.py --symbol btcusdt --market futures --date 2026-03-14 --from 6 --to 10 --no-binance
"""

import argparse
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests

sys.path.append(str(Path(__file__).parent))
from bct import ExCloud
from data_manager.cloud_manager import CloudManager


INTERVALS = {
    "1m":  ("1min",  "1m"),
    "3m":  ("3min",  "3m"),
    "5m":  ("5min",  "5m"),
    "15m": ("15min", "15m"),
    "30m": ("30min", "30m"),
    "1h":  ("1h",    "1h"),
    "4h":  ("4h",    "4h"),
}

BINANCE_FUTURES_URL = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_SPOT_URL    = "https://api.binance.com/api/v3/klines"


# ── загрузка из облака ────────────────────────────────────────────────────────

def download_trades(cloud: CloudManager, symbol: str, market: str, date: str, hour_from: int, hour_to: int) -> pd.DataFrame:
    frames = []
    for hour in range(hour_from, hour_to + 1):
        hour_str = str(hour).zfill(2)
        key = cloud._make_key(market=market, symbol=symbol, date=date, hour=hour_str, data_type="trades")
        print(f"  Скачиваю {key}...")
        data = cloud.download_bytes(key)
        if data is None:
            print(f"  ⚠️  Не найден: {key}")
            continue
        df = pq.read_table(io.BytesIO(data)).to_pandas()
        frames.append(df)
        print(f"  ✅ {len(df):,} трейдов")

    if not frames:
        print("Нет данных в облаке")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    print(f"Всего трейдов из облака: {len(combined):,}\n")
    return combined


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df["time"]  = pd.to_datetime(df["E"], unit="ms", utc=True)
    df["price"] = df["p"].astype(float)
    df["qty"]   = df["q"].astype(str).str.lstrip("-").astype(float)
    df["side"]  = df["q"].astype(str).str.startswith("-").map({True: "sell", False: "buy"})
    return df.sort_values("time")


def build_candles(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    freq = INTERVALS[interval][0]
    df = df.set_index("time")

    candles             = df["price"].resample(freq).ohlc()
    candles["volume"]   = df["qty"].resample(freq).sum()
    candles["buy_vol"]  = df[df["side"] == "buy"]["qty"].resample(freq).sum().fillna(0)
    candles["sell_vol"] = df[df["side"] == "sell"]["qty"].resample(freq).sum().fillna(0)

    return candles.dropna(subset=["open"])


# ── загрузка с Binance API ────────────────────────────────────────────────────

def fetch_binance_candles(symbol: str, market: str, date: str, hour_from: int, hour_to: int, interval: str) -> pd.DataFrame:
    binance_interval = INTERVALS[interval][1]
    url = BINANCE_FUTURES_URL if market == "futures" else BINANCE_SPOT_URL

    dt_from  = datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), hour_from, 0, 0, tzinfo=timezone.utc)
    dt_to    = datetime(int(date[:4]), int(date[5:7]), int(date[8:10]), hour_to,   59, 59, tzinfo=timezone.utc)
    start_ms = int(dt_from.timestamp() * 1000)
    end_ms   = int(dt_to.timestamp()   * 1000)

    print(f"Загружаю свечи с Binance API ({market}, {binance_interval})...")

    frames  = []
    current = start_ms
    while current < end_ms:
        resp = requests.get(url, params={
            "symbol":    symbol.upper(),
            "interval":  binance_interval,
            "startTime": current,
            "endTime":   end_ms,
            "limit":     1000,
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        frames.extend(data)
        current = data[-1][0] + 1

        if len(data) < 1000:
            break

        time.sleep(0.1)

    if not frames:
        print("⚠️  Binance API не вернул данных")
        return pd.DataFrame()

    df = pd.DataFrame(frames, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_buy_vol", "taker_buy_quote_vol", "ignore"
    ])
    df["time"]     = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["open"]     = df["open"].astype(float)
    df["high"]     = df["high"].astype(float)
    df["low"]      = df["low"].astype(float)
    df["close"]    = df["close"].astype(float)
    df["volume"]   = df["volume"].astype(float)
    df["buy_vol"]  = df["taker_buy_vol"].astype(float)
    df["sell_vol"] = df["volume"] - df["buy_vol"]
    df = df.set_index("time")

    print(f"✅ {len(df):,} свечей с Binance\n")
    return df


# ── график ────────────────────────────────────────────────────────────────────

def plot(our: pd.DataFrame, binance: pd.DataFrame, title: str):
    colors = {
        "bg":       "#0d0d0d",
        "grid":     "#1a1a1a",
        "up":       "#00d4aa",
        "down":     "#ff4d6d",
        "vol_buy":  "rgba(0, 212, 170, 0.4)",
        "vol_sell": "rgba(255, 77, 109, 0.4)",
        "text":     "#888888",
    }

    has_binance = not binance.empty
    cols = 2 if has_binance else 1

    fig = make_subplots(
        rows=2, cols=cols,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
        horizontal_spacing=0.05,
        subplot_titles=(
            ["Наши данные (облако)", "Binance API"] if has_binance
            else ["Наши данные (облако)"]
        ),
    )

    def add_candles(col: int, candles: pd.DataFrame):
        fig.add_trace(go.Candlestick(
            x=candles.index,
            open=candles["open"],
            high=candles["high"],
            low=candles["low"],
            close=candles["close"],
            increasing=dict(line=dict(color=colors["up"],   width=1), fillcolor=colors["up"]),
            decreasing=dict(line=dict(color=colors["down"], width=1), fillcolor=colors["down"]),
            showlegend=False,
        ), row=1, col=col)

        fig.add_trace(go.Bar(
            x=candles.index, y=candles["buy_vol"],
            marker_color=colors["vol_buy"],
            name="Buy", showlegend=False,
        ), row=2, col=col)

        fig.add_trace(go.Bar(
            x=candles.index, y=candles["sell_vol"],
            marker_color=colors["vol_sell"],
            name="Sell", showlegend=False,
        ), row=2, col=col)

    add_candles(1, our)
    if has_binance:
        add_candles(2, binance)

    # стиль осей
    axis_style = dict(gridcolor=colors["grid"], zeroline=False, color=colors["text"])
    for i in ["", "2", "3", "4"]:
        fig.update_layout(**{
            f"xaxis{i}": dict(**axis_style, rangeslider=dict(visible=False)),
            f"yaxis{i}": dict(**axis_style, side="right"),
        })

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family="'Courier New', monospace", size=13, color="#666"),
            x=0.01,
        ),
        paper_bgcolor=colors["bg"],
        plot_bgcolor=colors["bg"],
        font=dict(family="'Courier New', monospace", color=colors["text"]),
        barmode="overlay",
        hovermode="x",
        margin=dict(l=20, r=60, t=60, b=20),
    )

    out_path = Path("candles.html")
    fig.write_html(str(out_path))
    print(f"Сохранено → {out_path.resolve()}")
    fig.show()


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",     required=True, help="btcusdt")
    parser.add_argument("--market",     required=True, help="futures / spot")
    parser.add_argument("--date",       required=True, help="2026-03-14")
    parser.add_argument("--from",       dest="hour_from", required=True, type=int)
    parser.add_argument("--to",         dest="hour_to",   required=True, type=int)
    parser.add_argument("--interval",   default="1m", choices=INTERVALS.keys())
    parser.add_argument("--no-binance", action="store_true", help="Только наши данные, без Binance API")
    args = parser.parse_args()

    print(f"\n{args.symbol.upper()} · {args.market} · {args.date} {args.hour_from:02d}:00–{args.hour_to:02d}:59 · {args.interval}\n")

    cloud       = ExCloud()
    df          = download_trades(cloud, args.symbol, args.market, args.date, args.hour_from, args.hour_to)
    df          = prepare(df)
    our_candles = build_candles(df, args.interval)
    print(f"Наших свечей: {len(our_candles):,}")

    binance_candles = pd.DataFrame()
    if not args.no_binance:
        binance_candles = fetch_binance_candles(
            args.symbol, args.market, args.date,
            args.hour_from, args.hour_to, args.interval
        )

    title = f"{args.symbol.upper()} · {args.market} · {args.date} {args.hour_from:02d}:00–{args.hour_to:02d}:59 · {args.interval}"
    plot(our_candles, binance_candles, title)


if __name__ == "__main__":
    main()