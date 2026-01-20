import asyncio
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request, HTTPException

from pydantic import BaseModel
from typing import List, Optional

from routes import router as data_router


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

# После создания app

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
app.include_router(data_router, prefix="/api", tags=["Data Access"])

# Подключаем роуты для выгрузки данных
# app.include_router(data_router)


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
# ==================== Upload Management ====================

@app.post("/upload/force")
async def force_upload(request: Request, exchange: Optional[str] = None):
    """
    Принудительная загрузка файлов текущего часа.
    Загружает файлы в облако БЕЗ удаления исходных conn_*.parquet файлов.
    
    Query params:
        exchange: конкретная биржа (binance/gate) или все если не указано
    
    Как это работает:
    1. CloudManager читает все conn_*.parquet файлы текущего часа
    2. Объединяет их в merged_*.parquet
    3. Сжимает (если включено)
    4. Загружает в облако
    5. Удаляет только merged файлы, НЕ трогая исходные conn_* файлы
    """
    # Ждем немного, чтобы writer успел дописать свежие данные
    await asyncio.sleep(2)
    
    total_uploaded = 0
    
    if exchange:
        exch = exchange.lower()
        if exch == 'binance':
            count = await request.app.state.manager_binance.force_upload_current()
            total_uploaded += count
        elif exch == 'gate':
            count = await request.app.state.manager_gate.force_upload_current()
            total_uploaded += count
        else:
            raise HTTPException(400, "Unknown exchange. Use 'binance' or 'gate'")
    else:
        # Загружаем файлы всех бирж
        count_b = await request.app.state.manager_binance.force_upload_current()
        count_g = await request.app.state.manager_gate.force_upload_current()
        total_uploaded = count_b + count_g

    logger.info(f"⚡ Force upload: {total_uploaded} files")
    
    return {
        "status": "success",
        "uploaded_files": total_uploaded,
        "exchange": exchange or "all",
        "note": "Original conn_* files preserved, only merged files removed"
    }

@app.post("/upload/symbol")
async def upload_symbol(request: Request, body: UploadRequest):
    """
    Загрузить все файлы конкретного символа за ВСЕ часы.
    
    Body:
        {
            "exchange": "binance",
            "symbol": "btcusdt",
            "delete_after": false  # если true - удалит директории после загрузки
        }
    
    Как это работает:
    1. Находит все директории symbol/{date}_{hour}/
    2. Для каждого часа: объединяет conn_*.parquet → merged_*.parquet
    3. Загружает в облако
    4. Если delete_after=true: удаляет всю директорию часа (кроме текущего)
    """
    if not body.exchange or not body.symbol:
        raise HTTPException(400, "Both 'exchange' and 'symbol' are required")
    
    # Ждем немного для свежести данных
    await asyncio.sleep(2)
    
    exch = body.exchange.lower()
    
    if exch == 'binance':
        manager = request.app.state.manager_binance
    elif exch == 'gate':
        manager = request.app.state.manager_gate
    else:
        raise HTTPException(400, "Unknown exchange. Use 'binance' or 'gate'")

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
        "deleted": body.delete_after,
        "note": f"Current hour preserved, {count} files uploaded"
    }


@app.post("/upload/cleanup")
async def cleanup_old_hours(request: Request, exchange: Optional[str] = None):
    """
    Удалить старые директории часов (которые уже были загружены в облако).
    
    Query params:
        exchange: конкретная биржа или все
    
    ВНИМАНИЕ: Это удалит все директории кроме текущего часа!
    Убедитесь, что данные загружены в облако перед использованием.
    """
    deleted_dirs = []
    
    from pathlib import Path
    from datetime import datetime, timezone
    
    data_dir = Path("collected_data")
    current_hour_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    
    exchanges = [exchange.lower()] if exchange else ["binance", "gate"]
    
    for exch in exchanges:
        exchange_dir = data_dir / exch
        if not exchange_dir.exists():
            continue
        
        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            
            for hour_dir in symbol_dir.iterdir():
                if not hour_dir.is_dir():
                    continue
                
                # Не трогаем текущий час
                if hour_dir.name == current_hour_key:
                    continue
                
                try:
                    # Удаляем все файлы
                    for file in hour_dir.glob("*"):
                        file.unlink()
                    
                    # Удаляем директорию
                    hour_dir.rmdir()
                    deleted_dirs.append(f"{exch}/{symbol_dir.name}/{hour_dir.name}")
                    
                except Exception as e:
                    logger.error(f"Failed to delete {hour_dir}: {e}")
    
    logger.info(f"🗑️ Cleanup: deleted {len(deleted_dirs)} directories")
    
    return {
        "status": "success",
        "deleted_directories": len(deleted_dirs),
        "directories": deleted_dirs[:20],  # первые 20 для примера
        "total": len(deleted_dirs)
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