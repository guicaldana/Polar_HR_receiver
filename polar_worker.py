import asyncio
import os
import random
import struct
import time
from typing import Any, Callable, Optional

try:
    from bleak import BleakScanner, BleakClient
    HAS_BLEAK = True
except ImportError:
    BleakScanner = None
    BleakClient = None
    HAS_BLEAK = False

# UUIDs GATT oficiais do Bluetooth SIG
HR_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"
BATTERY_LEVEL_UUID = "00002a19-0000-1000-8000-00805f9b34fb"


def parse_heart_rate_data(data: bytearray) -> dict:
    """
    Decodifica o pacote binário da característica GATT 0x2A37 (Heart Rate Measurement).
    - Bit 0: Formato de FC (0 = UINT8, 1 = UINT16)
    - Bit 4: Presença de intervalos RR
    """
    flags = data[0]
    hr_format = (flags & 0x01) == 0x01
    has_rr = (flags & 0x10) == 0x10

    if hr_format:
        hr_value = struct.unpack("<H", data[1:3])[0]
        offset = 3
    else:
        hr_value = data[1]
        offset = 2

    rr_intervals = []
    if has_rr:
        # Cada intervalo RR ocupa 2 bytes (em unidades de 1/1024 s)
        num_rr = (len(data) - offset) // 2
        for i in range(num_rr):
            raw_rr = struct.unpack("<H", data[offset + i * 2 : offset + i * 2 + 2])[0]
            rr_ms = round((raw_rr / 1024.0) * 1000.0, 2)
            rr_intervals.append(rr_ms)

    return {
        "type": "data",
        "heart_rate": hr_value,
        "rr_intervals": rr_intervals,
        "timestamp": time.time(),
    }


class PolarBleWorker:
    """
    Gerencia a busca, conexão e assinatura de notificações BLE da fita Polar H10.
    Suporta modo de simulação (mock) via variável de ambiente MOCK_POLAR=true.
    """

    def __init__(self, broadcast_callback: Callable[[dict], asyncio.Future]):
        self.broadcast_callback = broadcast_callback
        self.client: Optional[Any] = None
        self.is_running = False
        self.device_name: Optional[str] = None
        self.device_address: Optional[str] = None
        self.battery_level: Optional[int] = None
        self.mock_mode = os.getenv("MOCK_POLAR", "false").lower() in ("true", "1", "yes")

    async def start(self):
        self.is_running = True
        if self.mock_mode:
            print("⚠️ MOCK_POLAR ativado: simulando dados de frequência cardíaca...")
            asyncio.create_task(self._mock_loop())
        else:
            asyncio.create_task(self._connection_loop())

    async def stop(self):
        self.is_running = False
        if self.client and self.client.is_connected:
            try:
                await self.client.disconnect()
            except Exception as e:
                print(f"Erro ao desconectar cliente BLE: {e}")

    async def _mock_loop(self):
        """Simulador de dados para desenvolvimento do frontend sem fita ligada."""
        base_hr = 72
        while self.is_running:
            base_hr += random.choice([-1, 0, 1])
            base_hr = max(55, min(140, base_hr))
            sim_rr = round((60.0 / base_hr) * 1000.0 + random.uniform(-15, 15), 2)

            payload = {
                "type": "data",
                "heart_rate": base_hr,
                "rr_intervals": [sim_rr],
                "timestamp": time.time(),
                "mock": True,
            }
            await self.broadcast_callback(payload)
            await asyncio.sleep(1.0)

    async def _read_battery(self, client: Any):
        """Tenta ler a porcentagem de bateria do dispositivo."""
        try:
            battery_data = await client.read_gatt_char(BATTERY_LEVEL_UUID)
            if battery_data:
                self.battery_level = int(battery_data[0])
                print(f"🔋 Bateria Polar H10: {self.battery_level}%")
        except Exception as e:
            print(f"Não foi possível ler nível de bateria: {e}")
            self.battery_level = None

    async def _connection_loop(self):
        """Loop principal de busca, conexão e reconexão automática."""
        if not HAS_BLEAK:
            error_msg = (
                "A biblioteca 'bleak' não está instalada no ambiente Python atual. "
                "Ative o ambiente virtual ('source venv/bin/activate') ou instale com 'pip install bleak'. "
                "Para testar sem Bluetooth/bleak, execute com a variável de ambiente MOCK_POLAR=true."
            )
            print(f"❌ {error_msg}")
            await self.broadcast_callback({
                "type": "status",
                "status": "error",
                "message": error_msg,
            })
            return

        while self.is_running:
            try:
                await self.broadcast_callback({
                    "type": "status",
                    "status": "scanning",
                    "message": "Buscando fita Polar H10 via Bluetooth...",
                })

                # Escaneia procurando dispositivo com 'Polar' no nome
                device = await BleakScanner.find_device_by_filter(
                    lambda d, adv: d.name and ("Polar" in d.name or "polar" in d.name),
                    timeout=5.0,
                )

                if not device:
                    await self.broadcast_callback({
                        "type": "status",
                        "status": "not_found",
                        "message": "Nenhum dispositivo Polar encontrado. Tentando novamente em 3s...",
                    })
                    await asyncio.sleep(3.0)
                    continue

                self.device_name = device.name
                self.device_address = device.address

                await self.broadcast_callback({
                    "type": "status",
                    "status": "connecting",
                    "device": self.device_name,
                    "address": self.device_address,
                    "message": f"Conectando a {self.device_name} ({self.device_address})...",
                })

                def on_disconnect(client):
                    print(f"Polar {self.device_name} desconectado.")
                    asyncio.create_task(self.broadcast_callback({
                        "type": "status",
                        "status": "disconnected",
                        "message": "Fita desconectada. Reconectando...",
                    }))

                async with BleakClient(device.address, disconnected_callback=on_disconnect) as client:
                    self.client = client
                    await self._read_battery(client)

                    await self.broadcast_callback({
                        "type": "status",
                        "status": "connected",
                        "device": self.device_name,
                        "address": self.device_address,
                        "battery": self.battery_level,
                        "message": "Polar conectado com sucesso! Transmitindo dados.",
                    })

                    def on_notification(sender, data: bytearray):
                        payload = parse_heart_rate_data(data)
                        if self.battery_level is not None:
                            payload["battery"] = self.battery_level
                        asyncio.create_task(self.broadcast_callback(payload))

                    await client.start_notify(HR_MEASUREMENT_UUID, on_notification)

                    # Mantém a conexão aberta enquanto o cliente responder
                    while client.is_connected and self.is_running:
                        await asyncio.sleep(1.0)

            except Exception as e:
                print(f"Erro no loop BLE: {e}")
                await self.broadcast_callback({
                    "type": "status",
                    "status": "error",
                    "message": f"Falha na conexão BLE: {str(e)}. Nova tentativa em 3s...",
                })
                await asyncio.sleep(3.0)

