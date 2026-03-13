# test_live.py

import asyncio
import json
import logging
import os
import signal
import time

import orjson
import psutil
import requests
import websockets
from websockets.asyncio.client import connect

from connector.data_manager import DataManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_futures_symbols() -> list[str]:
    resp = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    symbols = []
    for s in resp.json()["symbols"]:
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING":
            symbols.append(s["symbol"].lower())
    logger.info(f"Futures symbols: {len(symbols)}")
    return symbols


def chunk_list(lst: list, size: int) -> list[list]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


class ConnectionStats:
    """Статистика по соединению"""
    def __init__(self, conn_id: int):
        self.conn_id = conn_id
        self.total_messages: int = 0
        self.trade_messages: int = 0
        self.depth_messages: int = 0
        self.subscribe_responses: int = 0
        self.unknown_messages: int = 0
        self.errors: int = 0
        self.reconnects: int = 0
        self.is_connected: bool = False


async def ws_connection(
    conn_id: int,
    url: str,
    streams: list[str],
    dm: DataManager,
    stats: ConnectionStats,
):
    """Одно WS соединение"""
    while True:
        try:
            async with connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                max_size=1 << 20,
            ) as ws:
                stats.is_connected = True

                # подписка
                sub_msg = json.dumps({
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": conn_id,
                })
                await ws.send(sub_msg)
                logger.info(f"[WS:{conn_id}] Connected, {len(streams)} streams")

                async for raw in ws:
                    stats.total_messages += 1

                    # определяем тип
                    msg_type = DataManager.extract_type(raw)

                    if msg_type == "trade":
                        tr = orjson.loads(raw)
                        # if tr['s'] == 'BIGTIMEUSDT':
                        #     print(tr)
                        stats.trade_messages += 1
                        dm.add(raw)
                    elif msg_type == "depthUpdate":
                        stats.depth_messages += 1
                        dm.add(raw)
                    elif msg_type is None:
                        # служебное сообщение {"result":null,"id":1}
                        stats.subscribe_responses += 1
                    else:
                        stats.unknown_messages += 1
                        dm.add(raw)

        except websockets.ConnectionClosed as e:
            stats.is_connected = False
            stats.reconnects += 1
            logger.warning(f"[WS:{conn_id}] Closed: {e}, reconnecting...")
            await asyncio.sleep(2)

        except Exception as e:
            stats.is_connected = False
            stats.errors += 1
            stats.reconnects += 1
            logger.error(f"[WS:{conn_id}] Error: {e}, reconnecting...")
            await asyncio.sleep(5)


def get_process_stats() -> dict:
    """Статистика процесса"""
    process = psutil.Process(os.getpid())
    mem = process.memory_info()
    cpu = process.cpu_percent()

    return {
        "rss_mb": mem.rss / 1024 / 1024,
        "vms_mb": mem.vms / 1024 / 1024,
        "cpu_percent": cpu,
        "threads": process.num_threads(),
    }


async def print_stats(
    data_managers: list[DataManager],
    conn_stats: list[ConnectionStats],
    start_time: float,
    interval: float = 5.0,
):
    """Статистика каждые N секунд"""
    prev_totals = {i: 0 for i in range(len(conn_stats))}
    prev_dm_received = {i: 0 for i in range(len(data_managers))}

    while True:
        await asyncio.sleep(interval)

        elapsed = time.time() - start_time
        proc = get_process_stats()

        print()
        print("=" * 110)
        print(f"UPTIME: {elapsed:.0f}s | CPU: {proc['cpu_percent']:.1f}% | "
              f"RAM: {proc['rss_mb']:.1f} MB | Threads: {proc['threads']}")
        print("=" * 110)

        # ── WS Connections ──
        print()
        print("WS CONNECTIONS:")
        print(f"{'ID':>4} | {'Status':>8} | {'msg/sec':>8} | {'total':>12} | "
              f"{'trades':>10} | {'depth':>10} | {'sub_resp':>8} | {'reconn':>6}")
        print("-" * 95)

        total_msg_sec = 0
        total_all = 0
        total_trades = 0
        total_depth = 0
        connected_count = 0

        for i, stats in enumerate(conn_stats):
            msg_sec = (stats.total_messages - prev_totals[i]) / interval
            prev_totals[i] = stats.total_messages

            total_msg_sec += msg_sec
            total_all += stats.total_messages
            total_trades += stats.trade_messages
            total_depth += stats.depth_messages

            if stats.is_connected:
                connected_count += 1

            status = "✓ OK" if stats.is_connected else "✗ DOWN"

            print(
                f"{stats.conn_id:>4} | "
                f"{status:>8} | "
                f"{msg_sec:>8,.0f} | "
                f"{stats.total_messages:>12,} | "
                f"{stats.trade_messages:>10,} | "
                f"{stats.depth_messages:>10,} | "
                f"{stats.subscribe_responses:>8} | "
                f"{stats.reconnects:>6}"
            )

        print("-" * 95)
        print(
            f"{'ALL':>4} | "
            f"{connected_count}/{len(conn_stats):>7} | "
            f"{total_msg_sec:>8,.0f} | "
            f"{total_all:>12,} | "
            f"{total_trades:>10,} | "
            f"{total_depth:>10,} | "
        )

        # ── Data Managers ──
        print()
        print("DATA MANAGERS:")
        print(f"{'ID':>4} | {'msg/sec':>8} | {'received':>12} | {'flushed':>12} | "
              f"{'in_memory':>10} | {'buf_mb':>8} | {'writes':>8} | {'errors':>6}")
        print("-" * 95)

        dm_total_received = 0
        dm_total_flushed = 0
        dm_total_memory = 0
        dm_total_buf_bytes = 0

        for i, dm in enumerate(data_managers):
            in_memory = sum(len(b) for b in dm._buffers.values())
            buf_bytes = sum(sum(len(s) for s in b) for b in dm._buffers.values())

            msg_sec = (dm.total_received - prev_dm_received[i]) / interval
            prev_dm_received[i] = dm.total_received

            dm_total_received += dm.total_received
            dm_total_flushed += dm.total_flushed
            dm_total_memory += in_memory
            dm_total_buf_bytes += buf_bytes

            print(
                f"{dm._conn_id:>4} | "
                f"{msg_sec:>8,.0f} | "
                f"{dm.total_received:>12,} | "
                f"{dm.total_flushed:>12,} | "
                f"{in_memory:>10,} | "
                f"{buf_bytes/1024/1024:>7.2f} | "
                f"{dm.total_write_ops:>8} | "
                f"{dm.total_write_errors:>6}"
            )

        print("-" * 95)
        print(
            f"{'ALL':>4} | "
            f"{'':>8} | "
            f"{dm_total_received:>12,} | "
            f"{dm_total_flushed:>12,} | "
            f"{dm_total_memory:>10,} | "
            f"{dm_total_buf_bytes/1024/1024:>7.2f} | "
        )

        # ── Summary ──
        print()
        avg_msg_sec = total_all / elapsed if elapsed > 0 else 0
        pending = dm_total_received - dm_total_flushed

        print(f"SUMMARY: avg {avg_msg_sec:,.0f} msg/sec | "
              f"pending write: {pending:,} | "
              f"trade/depth ratio: {total_trades}/{total_depth}")
        print("=" * 110)
        print()


async def main():
    STREAMS_PER_CONNECTION = 200

    # ── получаем символы ──
    symbols = get_futures_symbols()

    # ── разбиваем стримы ──
    stream_types = ["trade", "depth@100ms"]
    all_streams = [f"{sym}@{st}" for sym in symbols for st in stream_types]
    batches = chunk_list(all_streams, STREAMS_PER_CONNECTION)

    # ── символы для каждого соединения ──
    symbols_per_conn = STREAMS_PER_CONNECTION // len(stream_types)
    symbols_batches = chunk_list(symbols, symbols_per_conn)

    logger.info(f"Total streams: {len(all_streams)}")
    logger.info(f"Connections: {len(batches)} × {STREAMS_PER_CONNECTION} streams")
    logger.info(f"DataManagers: {len(symbols_batches)}")

    # ── создаём DataManager для каждого batch символов ──
    data_managers: list[DataManager] = []
    conn_stats: list[ConnectionStats] = []

    for i, sym_batch in enumerate(symbols_batches):
        dm = DataManager(
            data_dir="./data",
            market_type="futures",
            conn_id=i,
            flush_count=5_000,
            flush_interval=15.0,
        )
        for sym in sym_batch:
            dm.add_symbol(sym)

        data_managers.append(dm)
        logger.info(f"  DM[{i}]: {len(sym_batch)} symbols")

    # ── запускаем DataManagers ──
    dm_tasks = [asyncio.create_task(dm.run()) for dm in data_managers]

    # ── создаём WS соединения ──
    ws_tasks = []
    for i, batch in enumerate(batches):
        dm_index = min(i, len(data_managers) - 1)

        stats = ConnectionStats(i)
        conn_stats.append(stats)

        task = asyncio.create_task(
            ws_connection(
                conn_id=i,
                url="wss://fstream.binance.com/ws",
                streams=batch,
                dm=data_managers[dm_index],
                stats=stats,
            )
        )
        ws_tasks.append(task)
        logger.info(f"  WS[{i}]: {len(batch)} streams → DM[{dm_index}]")

    # ── статистика ──
    start_time = time.time()
    stats_task = asyncio.create_task(
        print_stats(data_managers, conn_stats, start_time, interval=5.0)
    )

    # ── graceful shutdown ──
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    logger.info("Running... Press Ctrl+C to stop")
    await stop.wait()

    # ── остановка ──
    logger.info("Stopping...")

    stats_task.cancel()
    for t in ws_tasks:
        t.cancel()
    for t in dm_tasks:
        t.cancel()

    for dm in data_managers:
        dm.stop()

    # ── финальная статистика ──
    total_received = sum(dm.total_received for dm in data_managers)
    total_flushed = sum(dm.total_flushed for dm in data_managers)
    total_errors = sum(dm.total_write_errors for dm in data_managers)
    elapsed = time.time() - start_time

    print()
    print("=" * 60)
    print("FINAL STATS")
    print("=" * 60)
    print(f"Duration:          {elapsed:.1f}s")
    print(f"Total received:    {total_received:,}")
    print(f"Total flushed:     {total_flushed:,}")
    print(f"Write errors:      {total_errors}")
    print(f"Avg msg/sec:       {total_received/elapsed:,.0f}")
    print("=" * 60)

    logger.info("Done")


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        logger.info("uvloop installed")
    except ImportError:
        pass

    asyncio.run(main())