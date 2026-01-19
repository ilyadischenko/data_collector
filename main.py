import asyncio
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional


# Импорты наших модулей
try:
    from collectors.binance import BinanceCollector
    # from collectors.bybit import BybitCollector
    from collectors.gate import GateCollector

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
    binance = BinanceCollector(num_connections=2)
    app.state.binance = binance
    t1 = asyncio.create_task(binance.run())
    
    # 3. Gate Collector
    gate = GateCollector(num_connections=2)
    app.state.gate = gate
    t3 = asyncio.create_task(gate.run())

    # 4. Cloud Manager для Binance
    # compress_before_upload=True - дополнительное GZIP сжатие
    # parquet_compression_level=9 - уровень ZSTD в Parquet (1-22, рекомендуется 9-15)
    manager_binance = CloudManager(
        data_dir="collected_data", 
        exchange="binance",
        compress_before_upload=True,  # Дополнительное GZIP сжатие
        parquet_compression_level=12   # Высокое сжатие ZSTD
    )
    app.state.manager_binance = manager_binance
    t2 = asyncio.create_task(manager_binance.run())
    
    # 6. Cloud Manager для Gate
    manager_gate = CloudManager(
        data_dir="collected_data", 
        exchange="gate",
        compress_before_upload=True,
        parquet_compression_level=12
    )
    app.state.manager_gate = manager_gate
    t4 = asyncio.create_task(manager_gate.run())
    
    logger.info("✅ All services started")
        
    yield

    logger.info("🛑 Shutting down...")
    
    # Останавливаем коллекторы
    await binance.stop()
    await gate.stop()
    
    # Останавливаем менеджеры
    manager_binance.stop()
    manager_gate.stop()
    
    # Отменяем задачи
    # for t in [t1, t2, t3, t4, t5, t6]:
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
    exchange: str  # 'binance', 'bybit', or 'gate'
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
    g = request.app.state.gate
    mb = request.app.state.manager_binance
    mg = request.app.state.manager_gate
    
    # Получаем статусы асинхронно
    binance_status = await b.get_status()
    gate_status = await g.get_status()
    
    return {
        "status": "running",
        "binance": {
            "running": binance_status['is_running'],
            "symbols": binance_status['active_symbols'],
            "connections": binance_status['connections']
        },
        "gate": {
            "running": gate_status['is_running'],
            "symbols": gate_status['active_symbols'],
            "connections": gate_status['connections']
        },
        "cloud_managers": {
            "binance": {
                "running": mb.is_running,
                "local_files": mb.get_local_files_stats()
            },
            "gate": {
                "running": mg.is_running,
                "local_files": mg.get_local_files_stats()
            }
        }
    }


@app.get("/stats")
async def stats(request: Request):
    """Детальная статистика системы."""
    b = request.app.state.binance
    g = request.app.state.gate
    
    mb = request.app.state.manager_binance
    mg = request.app.state.manager_gate
    
    binance_status = await b.get_status()
    gate_status = await g.get_status()
    
    return {
        "collectors": {
            "binance": {
                "active_symbols": len(binance_status['active_symbols']),
                "symbols": binance_status['active_symbols'],
                "connections": binance_status['connections']
            },
            "gate": {
                "active_symbols": len(gate_status['active_symbols']),
                "symbols": gate_status['active_symbols'],
                "connections": gate_status['connections']
            }
        },
        "storage": {
            "binance": mb.get_local_files_stats(),
            "gate": mg.get_local_files_stats()
        }
    }


# ==================== Symbol Management ====================

@app.post("/symbols")
async def add_symbol(request: Request, body: SymbolRequest):
    """Добавить символ для сбора данных."""
    exch = body.exchange.lower()
    
    if exch == 'binance':
        await request.app.state.binance.add_symbol(body.symbol)
    elif exch == 'gate':
        await request.app.state.gate.add_symbol(body.symbol)
    else:
        raise HTTPException(400, "Unknown exchange. Use 'binance' or 'gate'")

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
    binance_status = await request.app.state.binance.get_status()
    gate_status = await request.app.state.gate.get_status()
    
    return {
        "binance": binance_status['active_symbols'],
        "gate": gate_status['active_symbols']
    }


# ==================== Upload Management ====================

@app.post("/upload/force")
async def force_upload(request: Request, exchange: Optional[str] = None):
    """
    Принудительная загрузка файлов текущего часа.
    Сбрасывает буферы коллекторов и загружает файлы в облако.
    
    Query params:
        exchange: конкретная биржа (binance/bybit/gate) или все если не указано
    """
    total_uploaded = 0
    
    if exchange:
        exch = exchange.lower()
        if exch == 'binance':
            await request.app.state.binance.flush_all()
            count = await request.app.state.manager_binance.force_upload_current()
            total_uploaded += count
        elif exch == 'gate':
            await request.app.state.gate.flush_all()
            count = await request.app.state.manager_gate.force_upload_current()
            total_uploaded += count
        else:
            raise HTTPException(400, "Unknown exchange")
    else:
        # Сбрасываем буферы всех коллекторов
        await request.app.state.binance.flush_all()
        await request.app.state.gate.flush_all()
        
        # Загружаем файлы всех бирж
        count_b = await request.app.state.manager_binance.force_upload_current()
        count_g = await request.app.state.manager_gate.force_upload_current()

        total_uploaded = count_b + count_g

    logger.info(f"⚡ Force upload: {total_uploaded} files")
    
    return {
        "status": "success",
        "uploaded_files": total_uploaded,
        "exchange": exchange or "all"
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
    
    exch = body.exchange.lower()
    
    # Сначала сбрасываем буферы
    if exch == 'binance':
        await request.app.state.binance.flush_all()
        manager = request.app.state.manager_binance
    elif exch == 'gate':
        await request.app.state.gate.flush_all()
        manager = request.app.state.manager_gate
    else:
        raise HTTPException(400, "Unknown exchange")

    # Загружаем файлы символа
    count = await manager.force_upload_symbol(
        symbol=body.symbol,
        delete_after=body.delete_after
    )
    
    logger.info(
        f"⚡ Symbol upload: {exch}/{body.symbol} - "
        f"{count} files (delete_after={body.delete_after})"
    )
    
    return {
        "status": "success",
        "exchange": exch,
        "symbol": body.symbol,
        "uploaded_files": count,
        "deleted": body.delete_after
    }


@app.post("/upload/all")
async def upload_all(request: Request, exchange: Optional[str] = None, delete_after: bool = False):
    """
    Загрузить ВСЕ файлы в облако.
    
    Query params:
        exchange: конкретная биржа или все
        delete_after: удалить файлы после загрузки (default: false)
    """
    total_uploaded = 0
    
    if exchange:
        exch = exchange.lower()
        if exch == 'binance':
            await request.app.state.binance.flush_all()
            # Note: нужно добавить метод upload_all_files в CloudManager
            logger.warning("upload_all_files not implemented yet")
        elif exch == 'gate':
            await request.app.state.gate.flush_all()
            logger.warning("upload_all_files not implemented yet")
        else:
            raise HTTPException(400, "Unknown exchange")
    else:
        # Сбрасываем все буферы
        await request.app.state.binance.flush_all()
        await request.app.state.gate.flush_all()
        
        logger.warning("upload_all_files not implemented yet for all exchanges")
    
    logger.info(f"⚡ Upload all: {total_uploaded} files (delete_after={delete_after})")
    
    return {
        "status": "success",
        "uploaded_files": total_uploaded,
        "deleted": delete_after
    }


# ==================== Storage Info ====================

@app.get("/storage/local")
async def local_storage_info(request: Request, exchange: Optional[str] = None):
    """
    Информация о локальных файлах.
    
    Query params:
        exchange: конкретная биржа или все
    """
    if exchange:
        exch = exchange.lower()
        if exch == 'binance':
            return request.app.state.manager_binance.get_local_files_stats()
        elif exch == 'gate':
            return request.app.state.manager_gate.get_local_files_stats()
        else:
            raise HTTPException(400, "Unknown exchange")
    else:
        return {
            "binance": request.app.state.manager_binance.get_local_files_stats(),
            "gate": request.app.state.manager_gate.get_local_files_stats()
        }


# ==================== Root ====================

@app.get("/")
async def root():
    """API информация."""
    return {
        "service": "HFT Data Collector",
        "version": "2.0",
        "exchanges": ["binance", "bybit", "gate"],
        "endpoints": {
            "health": "GET /health - System health check",
            "stats": "GET /stats - Detailed statistics",
            "symbols": {
                "list": "GET /symbols - List all symbols",
                "add": "POST /symbols - Add symbol",
                "remove": "DELETE /symbols/{exchange}/{symbol} - Remove symbol"
            },
            "upload": {
                "force": "POST /upload/force?exchange={exchange} - Force upload current hour",
                "symbol": "POST /upload/symbol - Upload specific symbol",
                "all": "POST /upload/all?exchange={exchange} - Upload all files"
            },
            "storage": {
                "local": "GET /storage/local?exchange={exchange} - Local storage info"
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