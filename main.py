import asyncio
from contextlib import asynccontextmanager
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from polar_worker import PolarBleWorker


class ConnectionManager:
    """Gerencia as conexões WebSocket ativas e distribui mensagens em broadcast."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        disconnected = set()
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.add(connection)
        for dead_conn in disconnected:
            self.active_connections.discard(dead_conn)


manager = ConnectionManager()
worker = PolarBleWorker(broadcast_callback=manager.broadcast)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicialização: inicia a busca e conexão Bluetooth em background
    await worker.start()
    yield
    # Encerramento: desconecta da fita e limpa recursos
    await worker.stop()


app = FastAPI(
    title="Polar HR Receiver API",
    description="API FastAPI para captura e streaming em tempo real de frequência cardíaca via BLE e WebSocket.",
    version="1.0.0",
    lifespan=lifespan,
)

# Permite acesso cruzado do frontend Next.js (ex: localhost:3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "Polar HR Receiver API",
        "endpoints": {
            "health": "/health",
            "websocket": "/ws/hr",
        },
        "device": worker.device_name,
        "is_connected": worker.client.is_connected if worker.client else False,
        "mock_mode": worker.mock_mode,
    }


@app.get("/health")
async def health():
    return {
        "status": "online",
        "active_ws_clients": len(manager.active_connections),
        "device_name": worker.device_name,
        "device_address": worker.device_address,
        "battery_level": worker.battery_level,
        "is_connected": worker.client.is_connected if worker.client else False,
        "mock_mode": worker.mock_mode,
    }


@app.websocket("/ws/hr")
async def websocket_heart_rate(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Envia o status atual assim que o cliente conecta
        current_status = "connected" if (
            (worker.client and worker.client.is_connected) or worker.mock_mode
        ) else "scanning"

        await websocket.send_json({
            "type": "status",
            "status": current_status,
            "device": worker.device_name,
            "battery": worker.battery_level,
            "mock": worker.mock_mode,
            "message": "Conectado ao WebSocket da API Polar.",
        })

        while True:
            # Mantém a conexão aberta escutando pings/mensagens do cliente
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

