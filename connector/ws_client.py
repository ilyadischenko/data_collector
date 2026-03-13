import logging
import asyncio
from typing import Awaitable, Callable
import websockets
from websockets.asyncio.client import connect, ClientConnection



logger = logging.getLogger(__name__)

OnMessage = Callable[[str], Awaitable[None]]
OnConnect = Callable[[], Awaitable[None]]

class WSClient:
    def __init__(
            self,
            conn_id: int,
            url: str,
            
            on_message: OnMessage,
            on_connect: OnConnect | None = None
        ):
        self.conn_id = conn_id
        self.url = url
        self._ws : ClientConnection | None = None
        self.is_connected = False
        self.is_running = False
        self._on_message = on_message
        self._on_connect = on_connect

    async def _send_message(self, message) -> None:
        if self._ws and self.is_connected:
            await self._ws.send(message=message)
        else:
            logger.error(f"Попытка отправить сообщение в закрытое соединение {self.conn_id}")

    async def listen(self):
        async with connect(
            self.url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5
        ) as ws:
            self._ws = ws

            self.is_connected = True
            logger.info(f"Соединение {self.conn_id} установленно")

            # вызываем колбэк после подключения
            if self._on_connect:
                logger.info("Вызываю он коннект")
                await self._on_connect()
            
            
            async for raw in ws:
                if not self.is_running:
                    return
                await self._on_message(raw)

    async def run(self):
        self.is_running = True

        while self.is_running:
            try:
                logger.info(f"Подключение к {self.url} с ID: {self.conn_id}")
                await self.listen()

            except websockets.ConnectionClosedOK:
                logger.info(f"Соединение было закрыто со статусом ОК. ID: {self.conn_id}")
                await asyncio.sleep(2)

            except websockets.ConnectionClosedError as e:
                logger.error(f"Соединение было закрыто с ошибкой. ID: {self.conn_id}, Код: {e.code}, Причина: {e.reason}")
                await asyncio.sleep(2)

            except websockets.ConnectionClosed:
                logger.info(f"Соединение было закрыто просто так. ID: {self.conn_id}")
                await asyncio.sleep(2)
            
            except (ConnectionResetError, ConnectionRefusedError, OSError) as e:
                logger.error(f"Сетевая ошибка при подключении. ID: {self.conn_id}: {e}")
       
            finally:
                self._ws = None
                self.is_connected = False
            
            if self.is_running:
                logger.info(f"Попытка переподключиться к бирже у соединения {self.conn_id} через 2 секунды")




    async def stop(self):
        self.is_running = False
        if self._ws:
            await self._ws.close()