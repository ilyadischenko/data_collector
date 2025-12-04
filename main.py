import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from typing import List
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

try:
    from collectors.binance import BinanceCollector
    from storage.manager import CloudManager
except ImportError:
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MainAPI")

# Глобальные переменные для доступа к воркеру
collector_instance: BinanceCollector = None
collector_loop: asyncio.AbstractEventLoop = None
collector_thread: threading.Thread = None

def start_collector_worker():
    """Функция, которая будет работать в ОТДЕЛЬНОМ ПОТОКЕ."""
    global collector_instance, collector_loop

    # 1. Создаем новый Event Loop для этого потока
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    collector_loop = loop

    # 2. Создаем экземпляр коллектора
    collector_instance = BinanceCollector()

    # 3. Запускаем его (это блокирующая операция для потока, пока не вызовем stop)
    try:
        loop.run_until_complete(collector_instance.run())
    except Exception as e:
        logger.error(f"Collector thread crashed: {e}")
    finally:
        loop.close()
        logger.info("Collector loop closed.")

async def bridge_to_collector(coroutine):
    """
    МОСТИК: Позволяет из Главного потока (API) выполнить асинхронную функцию
    в Потоке Коллектора (Thread-2).
    """
    if not collector_loop or not collector_loop.is_running():
        raise HTTPException(503, "Collector is not running")
    
    # threadsafe передает задачу в другой цикл событий и возвращает Future
    future = asyncio.run_coroutine_threadsafe(coroutine, collector_loop)
    
    # Оборачиваем concurrent.futures.Future в asyncio.Future, чтобы можно было сделать await в API
    return await asyncio.wrap_future(future)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global collector_thread, collector_instance

    logger.info("🚀 System Starting...")

    # 1. ЗАПУСК КОЛЛЕКТОРА В ОТДЕЛЬНОМ ПОТОКЕ
    # Daemon=True значит, что поток умрет, если умрет основная программа
    collector_thread = threading.Thread(target=start_collector_worker, name="CollectorThread", daemon=True)
    collector_thread.start()
    
    # Ждем, пока коллектор инициализируется (получит loop)
    while collector_loop is None or not collector_loop.is_running():
        await asyncio.sleep(0.1)
    
    logger.info("✅ Collector Thread started independently")

    # 2. ЗАПУСК МЕНЕДЖЕРА (Это оставляем в главном потоке, он легкий и должен быть рядом с API)
    manager = CloudManager(data_dir="collected_data")
    app.state.manager = manager
    manager_task = asyncio.create_task(manager.run())

    yield

    # SHUTDOWN
    logger.info("🛑 Shutting down...")
    
    # Останавливаем менеджер
    manager.stop()
    if not manager_task.done():
        manager_task.cancel()

    # Останавливаем коллектор через мостик
    if collector_instance:
        # Вызываем .stop() внутри потока коллектора
        await bridge_to_collector(collector_instance.stop())
        # Ждем завершения потока
        collector_thread.join(timeout=5)

app = FastAPI(title="HFT Data Collector", lifespan=lifespan)

# Pydantic models
class SymbolRequest(BaseModel):
    symbol: str

class SymbolsRequest(BaseModel):
    symbols: List[str]

# === ENDPOINTS ===

@app.get("/health")
async def health(request: Request):
    m: CloudManager = request.app.state.manager
    
    # Получаем данные из потока коллектора.
    # Просто читать переменные (is_running, message_count) можно напрямую, 
    # так как чтение атомарных типов в Python потокобезопасно (обычно),
    # но для полной гарантии лучше не злоупотреблять.
    if collector_instance:
        coll_status = {
            "running": collector_instance.is_running,
            "ws_connected": collector_instance._ws_is_connected(),
            "messages": collector_instance.message_count
        }
    else:
        coll_status = "Starting..."

    return {
        "collector_thread": coll_status,
        "manager_task": {"running": m.is_running}
    }

@app.post("/symbols")
async def add_symbol(request: Request, body: SymbolRequest):
    # Передаем задачу в поток коллектора
    await bridge_to_collector(collector_instance.add_symbol(body.symbol))
    return {"status": "added", "symbol": body.symbol}

@app.delete("/symbols/{symbol}")
async def remove_symbol(request: Request, symbol: str):
    await bridge_to_collector(collector_instance.remove_symbol(symbol))
    return {"status": "removed", "symbol": symbol}

@app.post("/upload/force")
async def force_upload(request: Request):
    m: CloudManager = request.app.state.manager
    
    logger.info("⚡ Force upload requested")
    
    # 1. Команда в поток коллектора: Сбрось RAM на диск!
    await bridge_to_collector(collector_instance.flush_memory())
    
    # 2. Команда в главном потоке (Менеджеру): Грузи файлы!
    count = await m.force_upload_current()
    
    return {"status": "ok", "files_uploaded": count}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)