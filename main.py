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
    
    try:
        # FUTURES TRADE: T, EventTime, TradeID, Price, Qty, TransactTime, IsMaker
        if row_type == 'T':
            ts = int(parts[1])
            return {
                "type": "trade",
                "timestamp": ts,
                "time_str": datetime.fromtimestamp(ts/1000).strftime('%H:%M:%S.%f')[:-3],
                "trade_id": int(parts[2]),
                "price": float(parts[3]),
                "qty": float(parts[4]),
                "transact_time": int(parts[5]),
                "is_maker": bool(int(parts[6]))
            }

        # BOOK: B, EventTime, UpdateID, BidPr, BidQty, AskPr, AskQty, TransactTime
        elif row_type == 'B':
            ts = int(parts[1]) # Теперь это Биржевое Event Time (E)
            return {
                "type": "book",
                "timestamp": ts, 
                "time_str": datetime.fromtimestamp(ts/1000).strftime('%H:%M:%S.%f')[:-3],
                "update_id": int(parts[2]),
                "bid_p": float(parts[3]),
                "bid_q": float(parts[4]),
                "ask_p": float(parts[5]),
                "ask_q": float(parts[6]),
                "transact_time": int(parts[7]) # Добавили TransactTime (T)
            }
    except (ValueError, IndexError):
        return None
    return None

def get_files_in_range(symbol: str, start_ts: int, end_ts: int) -> List[str]:
    symbol_path = os.path.join(DATA_DIR, symbol.lower())
    if not os.path.exists(symbol_path): return []
    
    all_files = glob(os.path.join(symbol_path, "*.csv.gz"))
    relevant_files = []
    
    start_dt = datetime.utcfromtimestamp(start_ts / 1000)
    end_dt = datetime.utcfromtimestamp(end_ts / 1000)
    
    for f_path in all_files:
        try:
            basename = os.path.basename(f_path)
            date_part = basename.split('_', 1)[1].split('.')[0] 
            file_dt = datetime.strptime(date_part, "%Y%m%d_%H")
            file_end_dt = file_dt + timedelta(hours=1)
            
            if start_dt < file_end_dt and end_dt >= file_dt:
                relevant_files.append(f_path)
        except: continue
            
    return sorted(relevant_files)

def extract_data_from_files(files: List[str], start_ts: int, end_ts: int, limit: int, data_type: str):
    results = []
    count = 0
    for filepath in files:
        if count >= limit: break
        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                for line in f:
                    if count >= limit: break
                    
                    if data_type == "trade" and not line.startswith("T"): continue
                    if data_type == "book" and not line.startswith("B"): continue
                    
                    parsed = parse_csv_line(line)
                    if not parsed: continue
                    
                    # Теперь фильтрация работает честно по биржевому времени для обоих типов
                    if start_ts <= parsed['timestamp'] <= end_ts:
                        results.append(parsed)
                        count += 1
        except: pass
    return results

# --- COLLECTOR ---
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
            # Trade: T, E, t, p, q, T, m
            if data.get("e") == "trade":
                is_m = "1" if data.get("m") else "0"
                csv = f"T,{data.get('E')},{data.get('t')},{data.get('p')},{data.get('q')},{data.get('T')},{is_m}"
            
            # BookTicker: B, E, u, b, B, a, A, T
            # Теперь используем E и T из payload
            elif "u" in data:
                # E = Event Time, T = Transaction Time
                csv = f"B,{data.get('E')},{data.get('u')},{data.get('b')},{data.get('B')},{data.get('a')},{data.get('A')},{data.get('T')}"
            
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

# --- APP INIT ---
collector = BinanceFuturesCollector()
app = FastAPI(title="Futures Data Service")
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
router = Router()

# ==============================
# 🖥️ DASHBOARD & VIEWER
# ==============================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    html = """<html><head><title>Futures Dashboard</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body{font-family:'Segoe UI', sans-serif;background:#f4f4f9;padding:20px}
        table{width:100%;background:white;border-collapse:collapse;box-shadow:0 1px 3px rgba(0,0,0,0.1); border-radius:8px; overflow:hidden}
        th,td{padding:12px;border-bottom:1px solid #ddd;text-align:left} 
        th{background:#6f42c1;color:white}
        tr:hover{background:#f1f1f1}
        .btn{text-decoration:none;padding:6px 12px;color:white;border-radius:4px;margin-right:5px;font-size:13px;font-weight:600}
        .badge{background:#e9ecef;padding:4px 8px;border-radius:4px;font-weight:bold;color:#495057}
    </style></head><body><h1>🚀 Futures Data Collector</h1><table>
    <tr><th>Symbol</th><th>File</th><th>Size</th><th>Actions</th></tr>"""
    
    rows = ""
    if os.path.exists(DATA_DIR):
        for s in sorted(os.listdir(DATA_DIR)):
            spath = os.path.join(DATA_DIR, s)
            if not os.path.isdir(spath): continue
            
            files = sorted(glob(os.path.join(spath, "*.csv.gz")), reverse=True)[:5]
            for f in files:
                fn = os.path.basename(f)
                sz = os.path.getsize(f) / (1024*1024)
                
                rows += f"""<tr>
                    <td><span class="badge">{s.upper()}</span></td>
                    <td>{fn}</td>
                    <td>{sz:.2f} MB</td>
                    <td>
                        <a href='/view/{s}/{fn}' class='btn' style='background:#6610f2'>👁 View</a>
                        <a href='/api/view/{s}/{fn}' class='btn' style='background:#fd7e14' target='_blank'>JSON</a>
                        <a href='/download/{s}/{fn}' class='btn' style='background:#28a745'>⬇ DL</a>
                    </td></tr>"""
    return html + rows + "</table></body></html>"

@app.get("/view/{symbol}/{filename}", response_class=HTMLResponse)
async def view_html(symbol: str, filename: str):
    path = os.path.join(DATA_DIR, symbol.lower(), filename)
    if not os.path.exists(path): return HTMLResponse("<h1>File Not Found</h1>", 404)
    
    trades, books = [], []
    try:
        loop = asyncio.get_event_loop()
        # Читаем последние 200 строк
        lines = await loop.run_in_executor(None, lambda: list(deque(gzip.open(path, "rt"), maxlen=200)))
        
        for line in reversed(lines):
            d = parse_csv_line(line)
            if not d: continue
            if d['type'] == 'trade': trades.append(d)
            elif d['type'] == 'book': books.append(d)
    except Exception as e: return HTMLResponse(f"Error: {e}")

    html = f"""<html><head><title>View {symbol}</title>
    <style>
        body{{font-family:'Segoe UI', sans-serif;padding:20px;display:flex;gap:20px;background:#f8f9fa}} 
        .box{{flex:1;background:white;padding:15px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.1)}} 
        table{{width:100%;font-size:12px;border-collapse:collapse}} 
        td,th{{border-bottom:1px solid #eee;padding:6px;text-align:left}} 
        th{{color:#555}} 
        .buy{{color:#28a745;font-weight:bold}} .sell{{color:#dc3545;font-weight:bold}}
        .price{{font-family:monospace}}
        .back{{display:inline-block;margin-bottom:15px;text-decoration:none;color:#666}}
    </style></head><body>
    <div style="width:100%;position:absolute;top:0;left:0;padding:10px;"><a href="/dashboard" class="back">← Back to Dashboard</a></div>
    <div style="margin-top:30px; display:flex; width:100%; gap:20px;">
    """
    
    html += f"<div class='box'><h3>🛒 Trades</h3><table><tr><th>Time</th><th>Price</th><th>Qty</th><th>Side</th></tr>"
    for t in trades:
        side = "<span class='sell'>SELL</span>" if t['is_maker'] else "<span class='buy'>BUY</span>"
        html += f"<tr><td>{t['time_str']}</td><td class='price'>{t['price']}</td><td>{t['qty']}</td><td>{side}</td></tr>"
    html += "</table></div>"
    
    html += f"<div class='box'><h3>📚 Order Book</h3><table><tr><th>Time</th><th>Bid P</th><th>Bid Q</th><th>Ask P</th><th>Ask Q</th></tr>"
    for b in books:
        html += f"<tr><td>{b['time_str']}</td><td class='buy price'>{b['bid_p']}</td><td>{b['bid_q']}</td><td class='sell price'>{b['ask_p']}</td><td>{b['ask_q']}</td></tr>"
    html += "</table></div></div></body></html>"
    return html

# ==============================
# 🕹️ API CONTROL
# ==============================

@app.post("/api/control/start/{symbol}")
async def start_recording(symbol: str):
    res = await collector.add_symbol(symbol)
    return {"status": "ok", "symbol": symbol, "started": res}

@app.post("/api/control/stop/{symbol}")
async def stop_recording(symbol: str):
    res = await collector.remove_symbol(symbol)
    return {"status": "ok", "symbol": symbol, "stopped": res}

@app.get("/api/control/list")
async def list_active():
    return {"active_count": len(collector.active_symbols), "symbols": list(collector.active_symbols)}

# ==============================
# 💾 API DATA & HISTORY
# ==============================

@app.get("/api/view/{symbol}/{filename}")
async def api_view_json(symbol: str, filename: str, limit: int = 100):
    path = os.path.join(DATA_DIR, symbol.lower(), filename)
    if not os.path.exists(path): return {"error": "Not found"}
    data = []
    try:
        lines = list(deque(gzip.open(path, "rt"), maxlen=limit))
        for line in reversed(lines):
            parsed = parse_csv_line(line)
            if parsed: data.append(parsed)
    except: pass
    return {"file": filename, "count": len(data), "data": data}

@app.get("/api/history/{symbol}")
async def get_history(
    symbol: str, 
    from_ts: int = Query(..., description="Start Time (ms)"),
    to_ts: int = Query(..., description="End Time (ms)"),
    limit: int = Query(1000, le=10000),
    type: Literal["all", "trade", "book"] = "all"
):
    symbol = symbol.lower()
    files = get_files_in_range(symbol, from_ts, to_ts)
    if not files: return {"status": "ok", "count": 0, "data": [], "msg": "No files"}

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(
        None, extract_data_from_files, files, from_ts, to_ts, limit, type
    )
    return {"symbol": symbol, "from": from_ts, "to": to_ts, "count": len(data), "data": data}

@app.get("/download/{symbol}/{filename}")
async def download_file(symbol: str, filename: str):
    path = os.path.join(DATA_DIR, symbol.lower(), filename)
    if not os.path.exists(path): raise HTTPException(404)
    return FileResponse(path, media_type='application/gzip', filename=filename)

# --- TELEGRAM ---
@router.message(Command("start"))
async def cmd_start(m: types.Message):
    global ADMIN_CHAT_ID
    ADMIN_CHAT_ID = m.chat.id
    await m.answer("Bot Ready.")

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