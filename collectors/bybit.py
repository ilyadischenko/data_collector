import asyncio
from collections import deque
import websockets
import json
import gzip
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Set, Deque, Dict

class BybitCollector:
    def __init__(self):
        # Bybit V5 Public Linear Stream
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.exchange = "bybit"
        self.active_symbols: Set[str] = set()
        self.symbol_buffers: Dict[str, Deque[str]] = {}
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        self.is_running = False
        self.logger = logging.getLogger("Bybit")
        self.subscription_lock = asyncio.Lock()
        self.ws = None
        self.loop = None

    async def run(self):
        self.loop = asyncio.get_running_loop()
        self.is_running = True
        self.logger.info("🚀 Started Bybit Collector")
        await asyncio.gather(self._ws_listener(), self._periodic_disk_flush())

    async def stop(self):
        self.is_running = False
        await self.flush_memory()
        if self.ws: await self.ws.close()
        self.thread_pool.shutdown(wait=True)

    def _ws_is_connected(self) -> bool:
        """Проверка что WebSocket подключён."""
        if self.ws is None:
            return False
        # Для websockets 12+ проверяем state или close_code
        try:
            # Вариант 1: проверяем close_code (None = соединение активно)
            return self.ws.close_code is None
        except AttributeError:
            # Вариант 2: старые версии websockets
            try:
                return self.ws.open
            except AttributeError:
                return False

    async def _periodic_disk_flush(self):
        while self.is_running:
            await asyncio.sleep(5)
            await self.flush_memory()

    async def flush_memory(self):
        now_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        flush_tasks = []
        
        for symbol, buffer in list(self.symbol_buffers.items()):
            if buffer:
                messages = list(buffer)
                buffer.clear()
                filename = f'{self.exchange}_{symbol}_{now_hour_key}.csv.gz'
                filepath = Path('collected_data') / filename
                flush_tasks.append(
                    self.loop.run_in_executor(self.thread_pool, self._write_gz, filepath, messages)
                )
        if flush_tasks: await asyncio.gather(*flush_tasks)

    def _write_gz(self, filepath: Path, messages: list):
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(str(filepath), 'at', compresslevel=3) as f:
            for msg in messages: f.write(msg + "\n")

    async def _ws_listener(self):
        while self.is_running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    await self._subscribe()
                    
                    # Пинг каждые 20 сек (Bybit требует)
                    async def pinger():
                        while self.is_running and self.ws:
                            await asyncio.sleep(20)
                            try: await self.ws.send(json.dumps({"op": "ping"}))
                            except: break
                    asyncio.create_task(pinger())

                    async for msg in ws:
                        if not self.is_running: break
                        self._parse(msg)
            except Exception as e:
                self.logger.error(f"WS Error: {e}")
                await asyncio.sleep(5)

    async def _subscribe(self):
        if not self.active_symbols: return
        args = []
        for s in self.active_symbols:
            args.append(f"publicTrade.{s.upper()}")
            args.append(f"orderbook.1.{s.upper()}") # Level 1 Orderbook = BookTicker
        
        # Bybit позволяет подписываться батчами (max 10 args)
        # Тут упрощенно шлем все сразу, но для продакшена надо делить на чанки по 10
        if args:
            req = {"op": "subscribe", "args": args}
            await self.ws.send(json.dumps(req))

    def _parse(self, raw):
        try:
            data = json.loads(raw)
            if "topic" not in data: return
            
            topic = data["topic"] # e.g. "publicTrade.BTCUSDT"
            
            # Извлекаем символ из топика
            parts = topic.split('.')
            sym = parts[-1].lower()
            
            if sym not in self.symbol_buffers: return
            
            payload = data["data"]
            line = None

            # 1. Trades
            if topic.startswith("publicTrade"):
                # Payload is a LIST of trades
                for t in payload:
                    # T, Time, TradeId, Price, Qty, TradeTime(ts), Side(Buy/Sell -> Maker?)
                    # Bybit: S="Buy" means Taker bought (Maker sold). S="Sell" means Taker sold.
                    # is_maker logic: If side is Buy, Maker is Sell side.
                    # Simple CSV: T, ts, trade_id, price, size, side
                    
                    # Приведем к формату бинанса насколько возможно
                    # T, EventTime, TradeId, Price, Qty, TradeTime, IsMaker
                    # is_maker сложно определить точно без контекста, запишем Side
                    is_buyer_maker = "0" if t["S"] == "Buy" else "1" 
                    
                    line = f'T,{t["T"]},{t["i"]},{t["p"]},{t["v"]},{t["T"]},{is_buyer_maker}'
                    self.symbol_buffers[sym].append(line)

            # 2. BookTicker (Orderbook Level 1)
            elif topic.startswith("orderbook.1"):
                # Payload: { "b": [["20000", "0.1"]], "a": [["20001", "0.2"]], "u": 123, "ts": ... }
                ts = data.get("ts", int(time.time()*1000))
                u_id = payload.get("u", 0)
                
                bid_p, bid_q = payload["b"][0] if payload.get("b") else ("0", "0")
                ask_p, ask_q = payload["a"][0] if payload.get("a") else ("0", "0")
                
                # B, EventTime, UpdateId, BidPr, BidQty, AskPr, AskQty
                line = f'B,{ts},{u_id},{bid_p},{bid_q},{ask_p},{ask_q}'
                self.symbol_buffers[sym].append(line)

        except: pass

    async def add_symbol(self, symbol: str):
        s = symbol.lower()
        async with self.subscription_lock:
            if s in self.active_symbols: return
            self.active_symbols.add(s)
            self.symbol_buffers[s] = deque()

            is_connected = self._ws_is_connected()
            if is_connected:
                # Bybit требует UPPERCASE в подписке
                req = {"op": "subscribe", "args": [f"publicTrade.{s.upper()}", f"orderbook.1.{s.upper()}"]}
                await self.ws.send(json.dumps(req))

    async def remove_symbol(self, symbol: str):
        s = symbol.lower()
        async with self.subscription_lock:
            self.active_symbols.discard(s)
            self.symbol_buffers.pop(s, None)

            is_connected = self._ws_is_connected()
            if is_connected:
                req = {"op": "unsubscribe", "args": [f"publicTrade.{s.upper()}", f"orderbook.1.{s.upper()}"]}
                await self.ws.send(json.dumps(req))

