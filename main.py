import asyncio
import os
import gzip
import time
import logging
from datetime import datetime
from typing import Set, Dict, List
from concurrent.futures import ThreadPoolExecutor
from glob import glob

import aiohttp
import orjson
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import FSInputFile

# --- КОНФИГУРАЦИЯ ---
TELEGRAM_TOKEN = "7706834120:AAFRZ77Oh8mTNgKHXfacwYLr2AOckoNk1Mo" 
DATA_DIR = "data"
BINANCE_WS_URL = "wss://fstream.binance.com/ws"
FLUSH_INTERVAL = 5   # Сброс на диск каждые 5 сек
RECONNECT_DELAY = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- CSV INTEGRITY CHECKER ---
def analyze_file_integrity(filepath: str):
    """
    Читает CSV.GZ файл и проверяет последовательность ID.
    Форматы:
    Trade: T, EventTime, TradeID, ... (TradeID index = 2)
    Book:  B, UpdateID, ... (UpdateID index = 1)
    """
    trade_gaps = 0
    trade_duplicates = 0
    book_jumps = 0
    
    last_trade_id = None
    last_book_u = None
    
    total_lines = 0
    trades_count = 0
    books_count = 0
    
    try:
        # mode="rt" - читаем как текст
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            for line in f:
                total_lines += 1
                # Быстро разбиваем строку
                parts = line.strip().split(',')
                if not parts: continue

                row_type = parts[0]
                
                # --- ПРОВЕРКА ТРЕЙДОВ ---
                if row_type == 'T':
                    try:
                        # T, E, t, p, q, b, a, T, m
                        # ID находится под индексом 2
                        tid = int(parts[2])
                        trades_count += 1
                        
                        if last_trade_id is not None:
                            diff = tid - last_trade_id
                            if diff > 1:
                                trade_gaps += (diff - 1)
                            elif diff == 0:
                                trade_duplicates += 1
                        last_trade_id = tid
                    except (ValueError, IndexError):
                        pass # Битая строка

                # --- ПРОВЕРКА СТАКАНА ---
                elif row_type == 'B':
                    try:
                        # B, u, b, B, a, A
                        # ID находится под индексом 1
                        uid = int(parts[1])
                        books_count += 1
                        
                        if last_book_u is not None:
                            diff = uid - last_book_u
                            if diff > 1:
                                book_jumps += 1
                        last_book_u = uid
                    except (ValueError, IndexError):
                        pass

        return {
            "status": "ok",
            "filename": os.path.basename(filepath),
            "stats": {
                "lines": total_lines,
                "trades": trades_count,
                "books": books_count,
            },
            "integrity": {
                "trade_loss": trade_gaps,       # Должно быть 0
                "trade_dupes": trade_duplicates,
                "book_jumps": book_jumps
            }
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- COLLECTOR CLASS ---
class BinanceCollector:
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
                logger.info(f"Added: {symbol}")
                return True
            return False

    async def remove_symbol(self, symbol: str):
        symbol = symbol.lower()
        async with self.lock:
            if symbol in self.active_symbols:
                self.active_symbols.remove(symbol)
                await self._unsubscribe([symbol])
                logger.info(f"Removed: {symbol}")
                return True
            return False

    async def _connect_loop(self):
        while self.running:
            try:
                logger.info("Connecting WS...")
                async with self.session.ws_connect(BINANCE_WS_URL) as ws:
                    self.ws = ws
                    logger.info("Connected.")
                    if self.active_symbols:
                        await self._subscribe(list(self.active_symbols))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = orjson.loads(msg.data)
                                if "result" in data or "id" in data: continue
                                
                                s = data.get("s")
                                if not s: continue
                                symbol = s.lower()

                                if symbol in self.active_symbols:
                                    if symbol not in self.buffer:
                                        self.buffer[symbol] = []
                                    
                                    csv_str = None
                                    
                                    # --- ФОРМИРОВАНИЕ CSV СТРОКИ ---
                                    
                                    # 1. TRADE
                                    if data.get("e") == "trade":
                                        # Структура: T, E, t, p, q, b, a, T, m
                                        # m (IsMaker) превращаем в 1 или 0
                                        m_val = "1" if data.get("m") else "0"
                                        
                                        # Используем f-string, это очень быстро в Python
                                        csv_str = (
                                            f"T,{data.get('E')},{data.get('t')},"
                                            f"{data.get('p')},{data.get('q')},"
                                            f"{data.get('b')},{data.get('a')},"
                                            f"{data.get('T')},{m_val}"
                                        )

                                    # 2. BOOKTICKER
                                    # У bookTicker нет поля "e" в raw stream, но есть "u" (UpdateID)
                                    elif "u" in data:
                                        # Структура: B, u, b, B, a, A
                                        csv_str = (
                                            f"B,{data.get('u')},"
                                            f"{data.get('b')},{data.get('B')},"
                                            f"{data.get('a')},{data.get('A')}"
                                        )

                                    if csv_str:
                                        # Кодируем в байты и добавляем в буфер
                                        self.buffer[symbol].append(csv_str.encode('utf-8'))

                            except Exception:
                                pass
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception as e:
                logger.error(f"WS Error: {e}")
            await asyncio.sleep(RECONNECT_DELAY)

    async def _subscribe(self, symbols: List[str]):
        if not self.ws: return
        params = [f"{s}@bookTicker" for s in symbols] + [f"{s}@trade" for s in symbols]
        payload = {"method": "SUBSCRIBE", "params": params, "id": int(time.time())}
        await self.ws.send_json(payload)

    async def _unsubscribe(self, symbols: List[str]):
        if not self.ws: return
        params = [f"{s}@bookTicker" for s in symbols] + [f"{s}@trade" for s in symbols]
        payload = {"method": "UNSUBSCRIBE", "params": params, "id": int(time.time())}
        await self.ws.send_json(payload)

    async def _flush_loop(self):
        while self.running:
            await asyncio.sleep(FLUSH_INTERVAL)
            # Ротация по часам
            suffix = datetime.utcnow().strftime("%Y%m%d_%H")
            
            async with self.lock:
                tasks = []
                for symbol, records in self.buffer.items():
                    if not records: continue
                    
                    # Имя файла теперь заканчивается на .csv.gz
                    filename = os.path.join(DATA_DIR, symbol, f"{symbol}_{suffix}.csv.gz")
                    
                    # Копируем и очищаем
                    data_to_write = records[:]
                    self.buffer[symbol] = []

                    # В пул потоков
                    tasks.append(
                        asyncio.get_event_loop().run_in_executor(
                            self.thread_pool,
                            self._write_bytes,
                            filename,
                            data_to_write
                        )
                    )
                if tasks: await asyncio.gather(*tasks)

    def _write_bytes(self, filename, records):
        # ab = append binary
        with gzip.open(filename, "ab", compresslevel=3) as f:
            for r in records:
                f.write(r)
                f.write(b"\n")

# --- APP SETUP ---
collector = BinanceCollector()
app = FastAPI()
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
router = Router()

# --- API ---
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_view():
    html = """<html><head><title>Data Monitor</title><style>
    body{font-family:sans-serif;padding:20px;} table{width:100%;border-collapse:collapse;}
    th,td{padding:8px;border-bottom:1px solid #ddd;text-align:left} th{background:#007bff;color:white}
    .btn{padding:4px 8px;text-decoration:none;color:white;border-radius:4px;margin-right:5px}
    .dl{background:#28a745} .chk{background:#17a2b8}
    </style></head><body><h1>Data Files (CSV Compressed)</h1><table>
    <tr><th>Symbol</th><th>File</th><th>Size</th><th>Actions</th></tr>"""
    
    rows = ""
    if os.path.exists(DATA_DIR):
        for s in sorted(os.listdir(DATA_DIR)):
            spath = os.path.join(DATA_DIR, s)
            if not os.path.isdir(spath): continue
            # Ищем .csv.gz
            files = sorted(glob(os.path.join(spath, "*.csv.gz")), reverse=True)[:5]
            for f in files:
                fn = os.path.basename(f)
                sz = os.path.getsize(f) / 1024 / 1024
                rows += f"<tr><td>{s.upper()}</td><td>{fn}</td><td>{sz:.2f} MB</td>"
                rows += f"<td><a href='/dl/{s}/{fn}' class='btn dl'>DL</a><a href='/chk/{s}/{fn}' class='btn chk'>Check</a></td></tr>"
    
    return html + rows + "</table></body></html>"

@app.get("/dl/{symbol}/{filename}")
async def download(symbol: str, filename: str):
    path = os.path.join(DATA_DIR, symbol.lower(), filename)
    if not os.path.exists(path) or ".." in filename: raise HTTPException(404)
    return FileResponse(path, media_type='application/gzip', filename=filename)

@app.get("/chk/{symbol}/{filename}")
async def check(symbol: str, filename: str):
    path = os.path.join(DATA_DIR, symbol.lower(), filename)
    if not os.path.exists(path) or ".." in filename: raise HTTPException(404)
    return await asyncio.get_event_loop().run_in_executor(None, analyze_file_integrity, path)

@app.post("/add/{symbol}")
async def api_add(symbol: str): return {"res": await collector.add_symbol(symbol)}

@app.post("/stop/{symbol}")
async def api_stop(symbol: str): return {"res": await collector.remove_symbol(symbol)}

# --- TELEGRAM ---
@router.message(Command("start"))
async def start_cmd(m: types.Message): await m.answer("/add <sym>, /stop <sym>, /get <sym>")

@router.message(Command("add"))
async def add_cmd(m: types.Message):
    args = m.text.split()
    if len(args)>1 and await collector.add_symbol(args[1]): await m.answer("Started")

@router.message(Command("stop"))
async def stop_cmd(m: types.Message):
    args = m.text.split()
    if len(args)>1 and await collector.remove_symbol(args[1]): await m.answer("Stopped")

@router.message(Command("get"))
async def get_cmd(m: types.Message):
    args = m.text.split()
    if len(args) < 2: return
    files = glob(os.path.join(DATA_DIR, args[1].lower(), "*.csv.gz"))
    if files:
        latest = max(files, key=os.path.getctime)
        await m.answer_document(FSInputFile(latest))
    else: await m.answer("No files")

dp.include_router(router)

# --- STARTUP ---
@app.on_event("startup")
async def on_startup():
    await collector.start()
    asyncio.create_task(dp.start_polling(bot))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)