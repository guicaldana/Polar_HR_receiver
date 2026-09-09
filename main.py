import asyncio
import os
from contextlib import asynccontextmanager
from typing import Optional, Set
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from polar_worker import PolarBleWorker


class ConnectRequest(BaseModel):
    address: Optional[str] = None


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
    # Conecta automaticamente apenas se AUTO_CONNECT=true for especificado
    auto_connect = os.getenv("AUTO_CONNECT", "false").lower() in ("true", "1", "yes")
    if auto_connect:
        target_address = os.getenv("POLAR_ADDRESS", None)
        print(f"⚡ AUTO_CONNECT ativo. Conectando a {target_address or 'fita Polar disponível'}...")
        asyncio.create_task(worker.connect_to_device(address=target_address))
    else:
        print("ℹ️ Servidor iniciado com Bluetooth livre. Use POST /devices/connect ou faça um scan.")

    yield

    # Encerramento seguro: desconecta a fita e limpa recursos
    await worker.disconnect()


app = FastAPI(
    title="Polar HR Receiver API",
    description="API FastAPI para busca, seleção, conexão sob demanda e streaming em tempo real da fita Polar H10 via BLE e WebSocket.",
    version="1.1.0",
    lifespan=lifespan,
)

# Permite acesso cruzado do frontend Next.js (ex: localhost:3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "Polar HR Receiver API",
        "version": "1.1.0",
        "endpoints": {
            "scan_devices": "GET /devices/scan",
            "connect_device": "POST /devices/connect",
            "disconnect_device": "POST /devices/disconnect",
            "device_status": "GET /devices/status",
            "health": "GET /health",
            "websocket": "WS /ws/hr",
            "docs": "GET /docs",
        },
        "device_status": worker.get_status(),
    }


@app.get("/devices/scan")
async def scan_devices(timeout: float = 4.0):
    """
    Escaneia dispositivos Bluetooth Low Energy nas proximidades.
    Retorna lista ordenada com dispositivos Polar no topo.
    """
    try:
        devices = await worker.scan_devices(timeout=timeout)
        return {
            "devices": devices,
            "total": len(devices),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/devices/connect")
async def connect_device(request: Optional[ConnectRequest] = None):
    """
    Inicia conexão sob demanda com um dispositivo BLE específico pelo endereço MAC.
    Se nenhum endereço for enviado, conecta ao primeiro Polar encontrado.
    """
    addr = request.address if request else None
    try:
        result = await worker.connect_to_device(address=addr)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/devices/disconnect")
async def disconnect_device():
    """
    Desconecta da fita ativa e libera o adaptador Bluetooth do Linux.
    """
    result = await worker.disconnect()
    return result


@app.get("/devices/status")
async def device_status():
    """
    Retorna o status atual do dispositivo e da conexão BLE.
    """
    return worker.get_status()


@app.get("/health")
async def health():
    status = worker.get_status()
    status["active_ws_clients"] = len(manager.active_connections)
    return status


@app.websocket("/ws/hr")
async def websocket_heart_rate(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Envia o estado atual assim que o cliente conecta
        current_status = worker.get_status()
        await websocket.send_json({
            "type": "status",
            "status": current_status["status"],
            "device": current_status["device_name"],
            "address": current_status.get("device_address", current_status.get("device")),
            "battery": current_status["battery_level"],
            "mock": current_status["mock_mode"],
            "message": "Conectado ao canal WebSocket da fita Polar.",
        })

        while True:
            # Mantém a conexão aberta escutando pings/mensagens do cliente
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
