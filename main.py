import asyncio
import os
import gzip
import time
import logging
from datetime import datetime, timedelta
from typing import Set, Dict, List, Optional, Literal
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from collections import deque

import aiohttp
import orjson
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import FSInputFile

# --- КОНФИГУРАЦИЯ ---
TELEGRAM_TOKEN = "7706834120:AAFRZ77Oh8mTNgKHXfacwYLr2AOckoNk1Mo" 
DATA_DIR = "data_futures"
BINANCE_WS_URL = "wss://fstream.binance.com/ws"
FLUSH_INTERVAL = 5   
RECONNECT_DELAY = 2 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ADMIN_CHAT_ID: Optional[int] = None

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- УТИЛИТЫ ПАРСИНГА ---

def parse_csv_line(line: str):
    """Парсит строку CSV в словарь"""
    parts = line.strip().split(',')
    if not parts: return None
    row_type = parts[0]
    
    # T, EventTime, TradeID, Price, Qty, TransactTime, IsMaker
    if row_type == 'T':
        try:
            ts = int(parts[1])
            return {
                "type": "trade",
                "event_time": ts,
                "trade_id": int(parts[2]),
                "price": float(parts[3]),
                "qty": float(parts[4]),
                "transact_time": int(parts[5]),
                "is_maker": bool(int(parts[6]))
            }
        except: return None

    # B, UpdateID, BidPr, BidQty, AskPr, AskQty
    elif row_type == 'B':
        try:
            return {
                "type": "book",
                # У bookTicker нет времени в потоке, используем approximate time приема, 
                # но так как его нет в CSV B-строке, это поле будет отсутствовать или null
                "update_id": int(parts[1]),
                "bid_p": float(parts[2]),
                "bid_q": float(parts[3]),
                "ask_p": float(parts[4]),
                "ask_q": float(parts[5])
            }
        except: return None
    return None

def get_files_in_range(symbol: str, start_ts: int, end_ts: int) -> List[str]:
    """
    Возвращает список файлов, которые ПЕРЕСЕКАЮТСЯ с заданным диапазоном времени.
    Фильтрация идет по имени файла (YYYYMMDD_HH).
    """
    symbol_path = os.path.join(DATA_DIR, symbol.lower())
    if not os.path.exists(symbol_path): return []
    
    all_files = glob(os.path.join(symbol_path, "*.csv.gz"))
    relevant_files = []
    
    # Конвертируем запрос в datetime (без учета таймзон, так как файлы в UTC)
    start_dt = datetime.utcfromtimestamp(start_ts / 1000)
    end_dt = datetime.utcfromtimestamp(end_ts / 1000)
    
    for f_path in all_files:
        try:
            # Имя файла: btcusdt_20231027_14.csv.gz
            basename = os.path.basename(f_path)
            # Вырезаем дату и час: 20231027_14
            date_part = basename.split('_', 1)[1].split('.')[0] 
            file_dt = datetime.strptime(date_part, "%Y%m%d_%H")
            
            # Файл содержит данные за 1 час. 
            file_end_dt = file_dt + timedelta(hours=1)
            
            # Проверка пересечения интервалов
            # (StartA <= EndB) and (EndA >= StartB)
            if start_dt < file_end_dt and end_dt >= file_dt:
                relevant_files.append(f_path)
        except Exception:
            continue # Если файл назван криво, пропускаем
            
    # Сортируем файлы по времени
    return sorted(relevant_files)

def extract_data_from_files(files: List[str], start_ts: int, end_ts: int, limit: int, data_type: str):
    """Читает файлы и фильтрует строки по timestamp"""
    results = []
    count = 0
    
    for filepath in files:
        if count >= limit: break
        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                for line in f:
                    if count >= limit: break
                    
                    # Предварительная фильтрация по типу строки (быстрее чем парсинг)
                    if data_type == "trade" and not line.startswith("T"): continue
                    if data_type == "book" and not line.startswith("B"): continue
                    
                    parsed = parse_csv_line(line)
                    if not parsed: continue
                    
                    # Фильтрация по времени
                    # У трейдов есть event_time. У bookTicker в нашем CSV времени нет,
                    # поэтому bookTicker фильтруем только по попаданию в файл (грубая фильтрация)
                    # или пропускаем, если нужна строгая выборка по времени.
                    
                    if parsed['type'] == 'trade':
                        if start_ts <= parsed['event_time'] <= end_ts:
                            results.append(parsed)
                            count += 1
                    elif parsed['type'] == 'book':
                        # Для стакана берем всё, что попало в выбранные часовые файлы,
                        # так как точного времени в строке CSV нет
                        results.append(parsed)
                        count += 1
                        
        except Exception: pass
        
    return results

# --- COLLECTOR CLASS ---
class BinanceFuturesCollector:
    def __init__(self):
        self.active_symbols: Set[str] = set()
        self.ws = None
        self.session = None
        self.buffer: Dict[str, List[bytes]] = {}
        self.running = False
        self.lock = asyncio.Lock()
        self.thread_pool = ThreadPoolExecutor(max_workers=4)

    async def start(self):
        self.running = True
        self.session = aiohttp.ClientSession()
        asyncio.create_task(self._connect_loop())
        asyncio.create_task(self._flush_loop())

    async def stop(self):
        self.running = False
        if self.ws: await self.ws.close()
        if self.session: await self.session.close()
        self.thread_pool.shutdown(wait=False)

    async def add_symbol(self, symbol: str):
        symbol = symbol.lower()
        async with self.lock:
            if symbol not in self.active_symbols:
                self.active_symbols.add(symbol)
                self.buffer[symbol] = []
                path = os.path.join(DATA_DIR, symbol)
                if not os.path.exists(path): os.makedirs(path)
                await self._subscribe([symbol])
                logger.info(f"Added {symbol}")
                return True
            return False

    async def remove_symbol(self, symbol: str):
        symbol = symbol.lower()
        async with self.lock:
            if symbol in self.active_symbols:
                self.active_symbols.remove(symbol)
                await self._unsubscribe([symbol])
                logger.info(f"Removed {symbol}")
                return True
            return False

    async def _connect_loop(self):
        while self.running:
            try:
                logger.info(f"Connecting to {BINANCE_WS_URL}...")
                async with self.session.ws_connect(BINANCE_WS_URL) as ws:
                    self.ws = ws
                    logger.info("Connected.")
                    if self.active_symbols:
                        await self._subscribe(list(self.active_symbols))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._parse_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception as e:
                logger.error(f"WS Error: {e}")
            await asyncio.sleep(RECONNECT_DELAY)

    async def _parse_message(self, raw):
        try:
            data = orjson.loads(raw)
            if "s" not in data: return
            
            sym = data["s"].lower()
            if sym not in self.active_symbols: return
            if sym not in self.buffer: self.buffer[sym] = []

            csv = None
            if data.get("e") == "trade":
                # FUTURES: T, E, t, p, q, T, m
                is_m = "1" if data.get("m") else "0"
                csv = f"T,{data.get('E')},{data.get('t')},{data.get('p')},{data.get('q')},{data.get('T')},{is_m}"
            elif "u" in data:
                # BOOK: B, u, b, B, a, A
                csv = f"B,{data.get('u')},{data.get('b')},{data.get('B')},{data.get('a')},{data.get('A')}"
            
            if csv: self.buffer[sym].append(csv.encode('utf-8'))
        except: pass

    async def _subscribe(self, syms):
        if not self.ws: return
        params = [f"{s}@bookTicker" for s in syms] + [f"{s}@trade" for s in syms]
        await self.ws.send_json({"method": "SUBSCRIBE", "params": params, "id": int(time.time())})

    async def _unsubscribe(self, syms):
        if not self.ws: return
        params = [f"{s}@bookTicker" for s in syms] + [f"{s}@trade" for s in syms]
        await self.ws.send_json({"method": "UNSUBSCRIBE", "params": params, "id": int(time.time())})

    async def _flush_loop(self):
        while self.running:
            await asyncio.sleep(FLUSH_INTERVAL)
            suffix = datetime.utcnow().strftime("%Y%m%d_%H")
            async with self.lock:
                tasks = []
                for s, recs in self.buffer.items():
                    if not recs: continue
                    fname = os.path.join(DATA_DIR, s, f"{s}_{suffix}.csv.gz")
                    data = recs[:]
                    self.buffer[s] = []
                    tasks.append(asyncio.get_event_loop().run_in_executor(
                        self.thread_pool, self._write, fname, data))
                if tasks: await asyncio.gather(*tasks)

    def _write(self, name, data):
        with gzip.open(name, "ab", compresslevel=3) as f:
            for d in data: f.write(d + b"\n")

# --- APP ---
collector = BinanceFuturesCollector()
app = FastAPI(title="Futures Data Service")
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
router = Router()

# ==============================
# 🕹️ API: CONTROL (УПРАВЛЕНИЕ)
# ==============================

@app.post("/api/control/start/{symbol}")
async def start_recording(symbol: str):
    """Добавить монету в сбор"""
    res = await collector.add_symbol(symbol)
    return {"status": "ok", "symbol": symbol, "started": res}

@app.post("/api/control/stop/{symbol}")
async def stop_recording(symbol: str):
    """Остановить запись монеты"""
    res = await collector.remove_symbol(symbol)
    return {"status": "ok", "symbol": symbol, "stopped": res}

@app.get("/api/control/list")
async def list_active():
    """Список активных монет"""
    return {"active_count": len(collector.active_symbols), "symbols": list(collector.active_symbols)}

# ==============================
# 💾 API: DATA ACCESS (ВЫГРУЗКА)
# ==============================

@app.get("/api/history/{symbol}")
async def get_history(
    symbol: str, 
    from_ts: int = Query(..., description="Start Timestamp (ms)"),
    to_ts: int = Query(..., description="End Timestamp (ms)"),
    limit: int = Query(1000, le=10000, description="Max records to return"),
    type: Literal["all", "trade", "book"] = "all"
):
    """
    Получить исторические данные за период.
    Умный фильтр выбирает нужные архивные файлы по дате.
    """
    symbol = symbol.lower()
    
    # 1. Находим файлы, которые затрагивают этот период
    files = get_files_in_range(symbol, from_ts, to_ts)
    
    if not files:
        return {"status": "ok", "count": 0, "data": [], "msg": "No files found for this time range"}

    # 2. Выполняем чтение и фильтрацию в пуле потоков (CPU intensive)
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(
        None, 
        extract_data_from_files, 
        files, from_ts, to_ts, limit, type
    )
    
    return {
        "symbol": symbol,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "files_scanned": len(files),
        "count": len(data),
        "data": data
    }

# --- TELEGRAM ---
@router.message(Command("start"))
async def cmd_start(m: types.Message):
    global ADMIN_CHAT_ID
    ADMIN_CHAT_ID = m.chat.id
    await m.answer("Bot active. Admin ID saved.")

@router.message(Command("add"))
async def cmd_add(m: types.Message):
    args = m.text.split()
    if len(args) > 1:
        if await collector.add_symbol(args[1]): await m.answer(f"✅ {args[1]}")

@router.message(Command("stop"))
async def cmd_stop(m: types.Message):
    args = m.text.split()
    if len(args) > 1:
        if await collector.remove_symbol(args[1]): await m.answer(f"🛑 {args[1]}")

@router.message(Command("list"))
async def cmd_list(m: types.Message):
    await m.answer(f"Active: {list(collector.active_symbols)}")

dp.include_router(router)

@app.on_event("startup")
async def start():
    await collector.start()
    asyncio.create_task(dp.start_polling(bot))

@app.on_event("shutdown")
async def stop():
    await collector.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)