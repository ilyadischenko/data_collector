"""
memory_chart.py — график потребления памяти по модулям

Использование:
    python memory_chart.py                        # берёт memory_log.csv рядом
    python memory_chart.py path/to/memory_log.csv
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("memory_log.csv")
    if not path.exists():
        print(f"Файл не найден: {path}")
        sys.exit(1)

    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["ts"])

    # суммируем по модулю и времени
    by_module = df.groupby(["ts", "module"])["size_mb"].sum().reset_index()

    # топ модулей по максимальному размеру
    top_modules = (
        by_module.groupby("module")["size_mb"].max()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )

    colors = [
        "#00d4aa", "#ff4d6d", "#7c83fd", "#ffd166",
        "#06d6a0", "#ef476f", "#118ab2", "#ffa552",
        "#a8dadc", "#e63946",
    ]

    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.08,
        subplot_titles=["Память по модулям (MB)", "Суммарная память (MB)"],
    )

    # график по каждому модулю
    for i, module in enumerate(top_modules):
        mdf = by_module[by_module["module"] == module].sort_values("ts")
        fig.add_trace(go.Scatter(
            x=mdf["ts"],
            y=mdf["size_mb"],
            name=module,
            mode="lines",
            line=dict(color=colors[i % len(colors)], width=1.5),
        ), row=1, col=1)

    # суммарная память
    total = by_module.groupby("ts")["size_mb"].sum().reset_index()
    fig.add_trace(go.Scatter(
        x=total["ts"],
        y=total["size_mb"],
        name="Total",
        mode="lines",
        line=dict(color="#ffffff", width=2),
        fill="tozeroy",
        fillcolor="rgba(255,255,255,0.05)",
    ), row=2, col=1)

    fig.update_layout(
        paper_bgcolor="#0d0d0d",
        plot_bgcolor="#0d0d0d",
        font=dict(family="'Courier New', monospace", color="#888"),
        title=dict(
            text=f"Memory profile — {path.name}",
            font=dict(size=13, color="#666"),
            x=0.01,
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=10),
            x=1.01, y=1,
        ),
        hovermode="x unified",
        margin=dict(l=20, r=200, t=50, b=20),
    )

    axis_style = dict(gridcolor="#1a1a1a", zeroline=False, color="#555")
    for ax in ["xaxis", "xaxis2", "yaxis", "yaxis2"]:
        fig.update_layout(**{ax: axis_style})

    out = Path("memory_chart.html")
    fig.write_html(str(out))
    print(f"Сохранено → {out.resolve()}")
    fig.show()


if __name__ == "__main__":
    main()