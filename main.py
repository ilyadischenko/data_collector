import asyncio
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import List

# Импорты наших модулей
try:
    from collectors.binance import BinanceCollector
    from collectors.bybit import BybitCollector
    from storage.manager import CloudManager
    from routes import router as data_router
except ImportError as e:
    print(f"❌ Import Error: {e}")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MainAPI")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 System Starting...")
    
    # 1. Binance
    binance = BinanceCollector()
    app.state.binance = binance
    t1 = asyncio.create_task(binance.run())
    
    # 2. Bybit
    bybit = BybitCollector()
    app.state.bybit = bybit
    t2 = asyncio.create_task(bybit.run())
    
    # 3. Cloud Manager (сброс файлов)
    manager = CloudManager(data_dir="collected_data")
    app.state.manager = manager
    t3 = asyncio.create_task(manager.run())
    
    yield
    
    logger.info("🛑 Shutting down...")
    await binance.stop()
    await bybit.stop()
    manager.stop()
    
    for t in [t1, t2, t3]:
        t.cancel()
        try: await t
        except: pass

app = FastAPI(title="HFT Data Collector", lifespan=lifespan)

# Подключаем роуты для выгрузки данных
app.include_router(data_router)

class SymbolRequest(BaseModel):
    exchange: str # 'binance' or 'bybit'
    symbol: str

@app.get("/health")
async def health(request: Request):
    b = request.app.state.binance
    by = request.app.state.bybit
    m = request.app.state.manager
    return {
        "binance": {"running": b.is_running, "symbols": list(b.active_symbols)},
        "bybit": {"running": by.is_running, "symbols": list(by.active_symbols)},
        "manager": {"running": m.is_running}
    }

@app.post("/symbols")
async def add_symbol(request: Request, body: SymbolRequest):
    exch = body.exchange.lower()
    if exch == 'binance':
        await request.app.state.binance.add_symbol(body.symbol)
    elif exch == 'bybit':
        await request.app.state.bybit.add_symbol(body.symbol)
    else:
        raise HTTPException(400, "Unknown exchange")
    return {"status": "added", "exchange": exch, "symbol": body.symbol}

@app.delete("/symbols/{exchange}/{symbol}")
async def remove_symbol(request: Request, exchange: str, symbol: str):
    exch = exchange.lower()
    if exch == 'binance':
        await request.app.state.binance.remove_symbol(symbol)
    elif exch == 'bybit':
        await request.app.state.bybit.remove_symbol(symbol)
    else:
        raise HTTPException(400, "Unknown exchange")
    return {"status": "removed"}

@app.post("/upload/force")
async def force_upload(request: Request):
    # Сброс памяти обоих коллекторов
    await request.app.state.binance.flush_memory()
    await request.app.state.bybit.flush_memory()
    
    # Менеджер грузит файлы
    count = await request.app.state.manager.force_upload_current()
    return {"uploaded": count}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)