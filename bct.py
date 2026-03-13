# bench_exchange_throughput.py

import asyncio
import json
import time
import logging

import requests
import websockets
from websockets.asyncio.client import connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════
# Получение символов
# ══════════════════════════════════════════════════════════

def get_futures_symbols() -> list[str]:
    """Все бессрочные фьючерсы"""
    resp = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    symbols = []
    for s in resp.json()["symbols"]:
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING":
            symbols.append(s["symbol"].lower())
    logger.info(f"Futures symbols: {len(symbols)}")
    return symbols


def get_spot_symbols() -> list[str]:
    """Все спотовые USDT пары"""
    resp = requests.get("https://api.binance.com/api/v3/exchangeInfo")
    symbols = []
    for s in resp.json()["symbols"]:
        if s["status"] == "TRADING" and s["quoteAsset"] == "USDT":
            symbols.append(s["symbol"].lower())
    logger.info(f"Spot symbols: {len(symbols)}")
    return symbols


# ══════════════════════════════════════════════════════════
# WS подключение и подсчёт
# ══════════════════════════════════════════════════════════

class MessageCounter:
    """Потокобезопасный счётчик"""
    def __init__(self):
        self.total: int = 0
        self.per_type: dict[str, int] = {
            "futures_trade": 0,
            "futures_depth": 0,
            "spot_trade": 0,
            "spot_depth": 0,
        }
        self.per_second: list[int] = []
        self._second_count: int = 0
        self._last_second: float = time.monotonic()

    def add(self, msg_type: str):
        self.total += 1
        self._second_count += 1
        self.per_type[msg_type] = self.per_type.get(msg_type, 0) + 1

    def tick(self) -> int:
        """Вызывать каждую секунду, возвращает count за прошлую секунду"""
        count = self._second_count
        self.per_second.append(count)
        self._second_count = 0
        return count


async def ws_listener(
    name: str,
    url: str,
    symbols: list[str],
    stream_types: list[str],
    counter: MessageCounter,
    msg_type_prefix: str,
    max_streams_per_conn: int = 200,
):
    """
    Подключается к WS, подписывается батчами, считает сообщения.
    """
    # Разбиваем на батчи
    all_streams = [f"{s}@{st}" for s in symbols for st in stream_types]
    batches = [
        all_streams[i:i + max_streams_per_conn]
        for i in range(0, len(all_streams), max_streams_per_conn)
    ]

    logger.info(
        f"[{name}] {len(symbols)} symbols × {len(stream_types)} types "
        f"= {len(all_streams)} streams → {len(batches)} connections"
    )

    async def run_connection(batch_id: int, streams: list[str]):
        """Одно WS соединение"""
        while True:
            try:
                async with connect(
                    url,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=1 << 20,
                ) as ws:
                    # Подписка
                    sub = json.dumps({
                        "method": "SUBSCRIBE",
                        "params": streams,
                        "id": batch_id + 1,
                    })
                    await ws.send(sub)

                    async for raw in ws:
                        # Определяем тип по содержимому
                        if '"result"' in raw:
                            continue

                        if "@depth" in raw or '"depthUpdate"' in raw or '"e":"depthUpdate"' in raw:
                            counter.add(f"{msg_type_prefix}_depth")
                        else:
                            counter.add(f"{msg_type_prefix}_trade")

            except Exception as e:
                logger.warning(f"[{name}:{batch_id}] {type(e).__name__}: {e}")
                await asyncio.sleep(2)

    # Запускаем все соединения параллельно
    tasks = [
        asyncio.create_task(run_connection(i, batch))
        for i, batch in enumerate(batches)
    ]

    await asyncio.gather(*tasks)


async def print_stats(counter: MessageCounter, duration: int):
    """Выводит статистику каждую секунду"""
    start = time.monotonic()

    # Заголовок
    print()
    print("=" * 90)
    print(
        f"{'Сек':>4} | "
        f"{'msg/sec':>10} | "
        f"{'fut_trade':>10} | "
        f"{'fut_depth':>10} | "
        f"{'spt_trade':>10} | "
        f"{'spt_depth':>10} | "
        f"{'total':>12}"
    )
    print("-" * 90)

    prev_total = 0
    prev_types = dict(counter.per_type)

    for sec in range(1, duration + 1):
        await asyncio.sleep(1.0)

        current_total = counter.total
        per_sec = current_total - prev_total

        # Дельта по типам
        deltas = {}
        for key in counter.per_type:
            deltas[key] = counter.per_type[key] - prev_types.get(key, 0)

        counter.tick()

        print(
            f"{sec:>4} | "
            f"{per_sec:>10,} | "
            f"{deltas.get('futures_trade', 0):>10,} | "
            f"{deltas.get('futures_depth', 0):>10,} | "
            f"{deltas.get('spot_trade', 0):>10,} | "
            f"{deltas.get('spot_depth', 0):>10,} | "
            f"{current_total:>12,}"
        )

        prev_total = current_total
        prev_types = dict(counter.per_type)

    print("-" * 90)

    # Итоги
    elapsed = time.monotonic() - start
    avg = counter.total / elapsed if elapsed > 0 else 0

    print()
    print("=" * 90)
    print("ИТОГИ")
    print("=" * 90)
    print()
    print(f"{'Время замера:':<30} {elapsed:.1f} sec")
    print(f"{'Всего сообщений:':<30} {counter.total:,}")
    print(f"{'Среднее msg/sec:':<30} {avg:,.0f}")
    print()

    for key, val in counter.per_type.items():
        pct = val / counter.total * 100 if counter.total > 0 else 0
        per_sec = val / elapsed if elapsed > 0 else 0
        print(f"  {key:<20} {val:>12,}  ({pct:5.1f}%)  ~{per_sec:>8,.0f} msg/sec")

    print()

    if counter.per_second:
        valid = [x for x in counter.per_second if x > 0]
        if valid:
            print(f"{'Min msg/sec:':<30} {min(valid):,}")
            print(f"{'Max msg/sec:':<30} {max(valid):,}")
            print(f"{'Avg msg/sec:':<30} {sum(valid) // len(valid):,}")
    print()


async def main():
    DURATION = 60  # секунд замера

    # Получаем символы
    logger.info("Загрузка символов...")
    futures_symbols = get_futures_symbols()
    spot_symbols = get_spot_symbols()

    logger.info(
        f"Всего: {len(futures_symbols)} futures + {len(spot_symbols)} spot "
        f"= {len(futures_symbols) + len(spot_symbols)} символов"
    )

    counter = MessageCounter()

    # Стримы для подписки
    stream_types = ["trade", "depth@100ms"]

    total_streams = (len(futures_symbols) + len(spot_symbols)) * len(stream_types)
    logger.info(f"Всего стримов: {total_streams}")
    logger.info(f"Типы: {stream_types}")
    logger.info(f"Замер: {DURATION} секунд")
    logger.info("Подключение...")

    # Запускаем всё
    tasks = [
        asyncio.create_task(ws_listener(
            name="futures",
            url="wss://fstream.binance.com/ws",
            symbols=futures_symbols,
            stream_types=stream_types,
            counter=counter,
            msg_type_prefix="futures",
        )),
        asyncio.create_task(ws_listener(
            name="spot",
            url="wss://stream.binance.com:9443/ws",
            symbols=spot_symbols,
            stream_types=stream_types,
            counter=counter,
            msg_type_prefix="spot",
        )),
        asyncio.create_task(print_stats(counter, DURATION)),
    ]

    # print_stats завершится через DURATION секунд
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # Останавливаем остальные
    for t in pending:
        t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено")