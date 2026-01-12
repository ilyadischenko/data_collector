import asyncio
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from collectors.gate import GateCollector

# Импорты наших модулей
try:
    from collectors.binance import BinanceCollector
    from collectors.bybit import BybitCollector
    from storage.manager import CloudManager
    from routes import router as data_router
except ImportError as e:
    print(f"❌ Import Error: {e}")
    exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MainAPI")




# ==================== Lifecycle ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 System Starting...")
    
    # 1. Binance Collector
    binance = BinanceCollector()
    app.state.binance = binance
    t1 = asyncio.create_task(binance.run())
    
    # 2. Bybit Collector
    bybit = BybitCollector()
    app.state.bybit = bybit
    t2 = asyncio.create_task(bybit.run())
    
        # 2. Bybit Collector
    gate = GateCollector()
    app.state.gate = gate
    t3 = asyncio.create_task(gate.run())

    # 3. Cloud Manager
    manager = CloudManager(data_dir="collected_data")
    app.state.manager = manager
    t4 = asyncio.create_task(manager.run())
    
    
    logger.info("✅ All services started")
    
    yield
    
    logger.info("🛑 Shutting down...")
    
    # Останавливаем коллекторы
    await binance.stop()
    await bybit.stop()
    manager.stop()
    
    # Отменяем задачи
    for t in [t1, t2, t3, t4]:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    
    logger.info("✅ Shutdown complete")


app = FastAPI(title="HFT Data Collector", lifespan=lifespan)

# Подключаем роуты для выгрузки данных
app.include_router(data_router)


# ==================== Models ====================

class SymbolRequest(BaseModel):
    exchange: str  # 'binance' or 'bybit'
    symbol: str


class UploadRequest(BaseModel):
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    delete_after: bool = False


# ==================== Health & Status ====================

@app.get("/health")
async def health(request: Request):
    """Статус системы и всех компонентов."""
    b = request.app.state.binance
    by = request.app.state.bybit
    g = request.app.state.gate
    m = request.app.state.manager
    
    return {
        "status": "running",
        "binance": {
            "running": b.is_running,
            "symbols": list(b.active_symbols),
            "buffer_stats": b.get_buffer_stats()
        },
        "bybit": {
            "running": by.is_running,
            "symbols": list(by.active_symbols),
            "buffer_stats": by.get_buffer_stats()
        },
        "gate": {
            "running": g.is_running,
            "symbols": list(g.active_symbols),
            "buffer_stats": g.get_buffer_stats()
        },
        "cloud_manager": {
            "running": m.is_running,
            "local_files": m.get_local_files_stats()
        }
    }


@app.get("/stats")
async def stats(request: Request):
    """Детальная статистика системы."""
    b = request.app.state.binance
    by = request.app.state.bybit
    g = request.app.state.gate
    m = request.app.state.manager
    
    local_stats = m.get_local_files_stats()
    
    return {
        "collectors": {
            "binance": {
                "active_symbols": len(b.active_symbols),
                "symbols": list(b.active_symbols),
                "buffers": b.get_buffer_stats()
            },
            "bybit": {
                "active_symbols": len(by.active_symbols),
                "symbols": list(by.active_symbols),
                "buffers": by.get_buffer_stats()
            },
            "gate": {
                "active_symbols": len(g.active_symbols),
                "symbols": list(g.active_symbols),
                "buffers": g.get_buffer_stats()
            }
        },
        "storage": {
            "total_files": local_stats.get('total_files', 0),
            "total_size_mb": local_stats.get('total_size_mb', 0),
            "current_hour_files": local_stats.get('current_hour_files', 0),
            "past_hour_files": local_stats.get('past_hour_files', 0),
            "by_symbol": local_stats.get('by_symbol', {})
        }
    }


# ==================== Symbol Management ====================

@app.post("/symbols")
async def add_symbol(request: Request, body: SymbolRequest):
    """Добавить символ для сбора данных."""
    exch = body.exchange.lower()
    
    if exch == 'binance':
        await request.app.state.binance.add_symbol(body.symbol)
    elif exch == 'bybit':
        await request.app.state.bybit.add_symbol(body.symbol)
    elif exch == 'gate':
        await request.app.state.gate.add_symbol(body.symbol)
    else:
        raise HTTPException(400, "Unknown exchange. Use 'binance', 'bybit' or 'gate'")
    
    logger.info(f"➕ Added symbol: {exch}/{body.symbol}")
    
    return {
        "status": "added",
        "exchange": exch,
        "symbol": body.symbol
    }


@app.delete("/symbols/{exchange}/{symbol}")
async def remove_symbol(request: Request, exchange: str, symbol: str):
    """Удалить символ из сбора данных."""
    exch = exchange.lower()
    
    if exch == 'binance':
        await request.app.state.binance.remove_symbol(symbol)
    elif exch == 'bybit':
        await request.app.state.bybit.remove_symbol(symbol)
    elif exch == 'gate':
        await request.app.state.gate.remove_symbol(symbol)
    else:
        raise HTTPException(400, "Unknown exchange")
    
    logger.info(f"➖ Removed symbol: {exch}/{symbol}")
    
    return {
        "status": "removed",
        "exchange": exch,
        "symbol": symbol
    }


@app.get("/symbols")
async def list_symbols(request: Request):
    """Список всех активных символов."""
    b = request.app.state.binance
    by = request.app.state.bybit
    g = request.app.state.gate
    return {
        "binance": list(b.active_symbols),
        "bybit": list(by.active_symbols),
        "gate": list(g.active_symbols)
    }


# ==================== Upload Management ====================

@app.post("/upload/force")
async def force_upload(request: Request):
    """
    Принудительная загрузка файлов текущего часа.
    Сбрасывает буферы коллекторов и загружает файлы в облако.
    """
    # Сбрасываем буферы в файлы
    await request.app.state.binance.flush_memory()
    await request.app.state.bybit.flush_memory()
    await request.app.state.gate.flush_memory()

    # Загружаем файлы текущего часа в облако
    count = await request.app.state.manager.force_upload_current()
    
    logger.info(f"⚡ Force upload: {count} files")
    
    return {
        "status": "success",
        "uploaded_files": count
    }


@app.post("/upload/symbol")
async def upload_symbol(request: Request, body: UploadRequest):
    """
    Загрузить все файлы конкретного символа.
    
    Body:
        {
            "exchange": "binance",
            "symbol": "btcusdt",
            "delete_after": false
        }
    """
    if not body.exchange or not body.symbol:
        raise HTTPException(400, "Both 'exchange' and 'symbol' are required")
    
    # Сначала сбрасываем буферы
    if body.exchange.lower() == 'binance':
        await request.app.state.binance.flush_memory()
    elif body.exchange.lower() == 'bybit':
        await request.app.state.bybit.flush_memory()
    elif body.exchange.lower() == 'gate':
        await request.app.state.gate.flush_memory()

    # Загружаем файлы символа
    count = await request.app.state.manager.force_upload_symbol(
        exchange=body.exchange,
        symbol=body.symbol,
        delete_after=body.delete_after
    )
    
    logger.info(
        f"⚡ Symbol upload: {body.exchange}/{body.symbol} - "
        f"{count} files (delete_after={body.delete_after})"
    )
    
    return {
        "status": "success",
        "exchange": body.exchange,
        "symbol": body.symbol,
        "uploaded_files": count,
        "deleted": body.delete_after
    }


@app.post("/upload/all")
async def upload_all(request: Request, delete_after: bool = False):
    """
    Загрузить ВСЕ файлы в облако.
    
    Query params:
        delete_after: удалить файлы после загрузки (default: false)
    """
    # Сбрасываем все буферы
    await request.app.state.binance.flush_memory()
    await request.app.state.bybit.flush_memory()
    await request.app.state.gate.flush_memory()

    # Загружаем все файлы
    count = await request.app.state.manager.upload_all_files(
        delete_after=delete_after
    )
    
    logger.info(f"⚡ Upload all: {count} files (delete_after={delete_after})")
    
    return {
        "status": "success",
        "uploaded_files": count,
        "deleted": delete_after
    }


# ==================== Storage Info ====================

@app.get("/storage/local")
async def local_storage_info(request: Request):
    """Информация о локальных файлах."""
    stats = request.app.state.manager.get_local_files_stats()
    return stats


# ==================== Root ====================

@app.get("/")
async def root():
    """API информация."""
    return {
        "service": "HFT Data Collector",
        "version": "2.0",
        "endpoints": {
            "health": "GET /health - System health check",
            "stats": "GET /stats - Detailed statistics",
            "symbols": {
                "list": "GET /symbols - List all symbols",
                "add": "POST /symbols - Add symbol",
                "remove": "DELETE /symbols/{exchange}/{symbol} - Remove symbol"
            },
            "upload": {
                "force": "POST /upload/force - Force upload current hour",
                "symbol": "POST /upload/symbol - Upload specific symbol",
                "all": "POST /upload/all - Upload all files"
            },
            "storage": {
                "local": "GET /storage/local - Local storage info"
            }
        }
    }


# ==================== Main ====================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )