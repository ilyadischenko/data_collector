import asyncio
import logging
from pathlib import Path
import uvloop

from connector.connectors_manager import ConnectorsManager
from connector.monitor import Monitor

from data_manager.manager import DataManager


uvloop.install()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / 'data'

async def main():

    monitor = Monitor()
    asyncio.create_task(monitor.run())
    manager = ConnectorsManager(data_dir = DATA_DIR)
    
    # data_manager = DataManager(data_dir='./data')
    # asyncio.create_task(data_manager.assembling_loop())

    try:
        await manager.run()
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
        await manager.stop()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    asyncio.run(main())



