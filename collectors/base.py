import websockets 
from typing import List


class BaseConnector:

    def __init__(self, exchange: str, ws_url: str, conn_id: str, protocol_lvl_ping: bool) -> None:
        self.exchange = exchange
        self.ws_url = ws_url
        self.conn_id = conn_id
        self.protocol_lvl_ping = protocol_lvl_ping

    async def run(self):
        async with websockets.connect(self.ws_url) as websocket:
            while True:
                try:
                    message = await websocket.recv()
                    self.handle_message(message)
                except websockets.ConnectionClosed:
                    break

    def handle_message(self, message: str) -> None:
        pass

