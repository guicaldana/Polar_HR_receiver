import asyncio
import os
import random
import struct
import time
from typing import Any, Callable, List, Optional

try:
    from bleak import BleakScanner, BleakClient
    HAS_BLEAK = True
except ImportError as e:
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
    Gerencia o ciclo de vida BLE da fita Polar H10:
    - Escaneamento sob demanda (descoberta de dispositivos BLE).
    - Conexão e desconexão explícitas via API REST / WebSocket.
    - Streaming de dados e leitura de bateria.
    - Modo de simulação (mock) para desenvolvimento sem hardware.
    """

    def __init__(self, broadcast_callback: Callable[[dict], asyncio.Future]):
        self.broadcast_callback = broadcast_callback
        self.reconnect_attempts = 0
        self.client: Optional[Any] = None
        self.is_running = False
        self.user_requested_disconnect = False
        self.status = "disconnected"  # "disconnected" | "scanning" | "connecting" | "connected" | "error"
        self.device_name: Optional[str] = None
        self.device: Optional[str] = None
        self.battery_level: Optional[int] = None

        self.mock_mode = os.getenv("MOCK_POLAR", "false").lower() in ("true", "1", "yes")
        self.auto_reconnect = os.getenv("AUTO_RECONNECT", "true").lower() in ("true", "1", "yes")

        self._mock_task: Optional[asyncio.Task] = None
        self._connection_task: Optional[asyncio.Task] = None

    def get_status(self) -> dict:
        """Retorna o estado completo da conexão e do dispositivo."""
        is_conn = False
        if self.mock_mode:
            is_conn = (self.status == "connected")
        elif self.client:
            is_conn = getattr(self.client, "is_connected", False)

        return {
            "status": self.status,
            "is_connected": is_conn,
            "device_name": self.device_name,
            "device": self.device,
            "battery_level": self.battery_level,
            "mock_mode": self.mock_mode,
            "auto_reconnect": self.auto_reconnect,
        }

    async def scan_devices(self, timeout: float = 4.0) -> List[dict]:
        """
        Escaneia dispositivos BLE próximos.
        Retorna uma lista ordenada com os dispositivos Polar no topo.
        """
        if self.mock_mode:
            await asyncio.sleep(1.0)
            return [
                {"name": "Polar H10 16680834 (Mock)", "address": "24:AC:AC:16:68:08", "rssi": -48, "is_polar": True},
                {"name": "Polar Verity Sense (Mock)", "address": "A0:9E:1A:12:34:56", "rssi": -65, "is_polar": True},
                {"name": "Dispositivo BLE Genérico", "address": "F1:22:33:44:55:66", "rssi": -82, "is_polar": False},
            ]

        if not HAS_BLEAK:
            raise RuntimeError("Biblioteca 'bleak' não instalada. Ative o venv ou execute com MOCK_POLAR=true.")

        previous_status = self.status
        self.status = "scanning"
        await self.broadcast_callback({
            "type": "status",
            "status": "scanning",
            "message": "Escaneando dispositivos Bluetooth Low Energy...",
        })

        try:
            discovered = await BleakScanner.discover(timeout=timeout, return_adv=True)
            results = []
            for d, adv in discovered.values():
                name = d.name or adv.local_name or "Desconhecido"
                is_polar = "polar" in name.lower()
                results.append({
                    "name": name,
                    "address": d.address,
                    "rssi": adv.rssi if adv else -99,
                    "is_polar": is_polar,
                })

            # Ordena: fita Polar primeiro, depois por sinal mais forte
            results.sort(key=lambda x: (not x["is_polar"], -x["rssi"]))
            return results

        finally:
            self.status = previous_status

    async def connect_to_device(self, address: Optional[str] = None) -> dict:
        """
        Conecta a um dispositivo específico pelo endereço MAC (ou busca Polar automaticamente).
        """
        self.user_requested_disconnect = False

        # Se já estiver conectado ao mesmo dispositivo, apenas retorna sucesso
        if self.client and getattr(self.client, "is_connected", False) and self.device == address:
            return {"status": "connected", "message": f"Já conectado a {self.device_name}.", "device": self.device_name}

        # Desconecta de conexão anterior se houver
        if self.status == "connected":
            await self.disconnect()

        if self.mock_mode:
            self.is_running = True
            self.status = "connected"
            self.device_name = "Polar H10 16680834 (Mock)"
            self.device = address or "24:AC:AC:16:68:08"
            self.battery_level = 100

            if self._mock_task and not self._mock_task.done():
                self._mock_task.cancel()
            self._mock_task = asyncio.create_task(self._mock_loop())

            await self.broadcast_callback({
                "type": "status",
                "status": "connected",
                "device": self.device_name,
                "address": self.device,
                "battery": self.battery_level,
                "mock": True,
                "message": "Polar conectado com sucesso (Modo Mock)! Transmitindo dados.",
            })
            return {"status": "connected", "device": self.device_name, "address": self.device}

        if not HAS_BLEAK:
            raise RuntimeError("Biblioteca 'bleak' não instalada.")

        self.status = "connecting"
        await self.broadcast_callback({
            "type": "status",
            "status": "connecting",
            "address": address,
            "message": f"Conectando ao dispositivo {address or 'Polar H10'}...",
        })

        device = address
        if not address:
            d = await BleakScanner.find_device_by_filter(
                lambda d, adv: d.name and ("polar" in d.name.lower()),
                timeout=6.0,
            )
            if not d:
                raise ValueError("Nenhum dispositivo Polar encontrado no scan.")
            device = d.address
            self.device_name = d.name or "Polar H10"
            self.device = d.address
        else:
            self.device_name = "Polar H10"
            self.device = address


        # Cancela task de conexão anterior se existir
        if self._connection_task and not self._connection_task.done():
            self._connection_task.cancel()

        # Inicia a sessão de conexão
        self.is_running = True
        connected_event = asyncio.Event()
        connection_error = []

        self._connection_task = asyncio.create_task(
            self._manage_connection(device, connected_event, connection_error)
        )

        try:
            # Aguarda até 10 segundos para a conexão ser estabelecida
            await asyncio.wait_for(connected_event.wait(), timeout=25.0)
            return {
                "status": "connected",
                "device": self.device_name,
                "address": self.device,
                "battery": self.battery_level,
            }
        except asyncio.TimeoutError:
            if connection_error:
                raise connection_error[0]
            raise TimeoutError("Tempo esgotado ao tentar conectar com a fita Polar.")


    async def _auto_pair_with_agent(self, mac: str):
        if not mac: return
        
        def run_pexpect():
            try:
                import pexpect
                import subprocess
                
                # Check se já está pareado
                info = subprocess.getoutput(f"bluetoothctl info {mac}")
                if "Paired: yes" in info:
                    return True
                    
                print(f"🛡️ Iniciando Agente de Pareamento BLE no Servidor para {mac}...")
                child = pexpect.spawn('bluetoothctl', encoding='utf-8', timeout=10)
                child.expect(['#', '>'])
                child.sendline('agent on')
                child.expect(['#', '>'])
                child.sendline('default-agent')
                child.expect(['#', '>'])
                
                child.sendline(f'pair {mac}')
                
                success = False
                while True:
                    index = child.expect(['Accept pairing', 'Confirm passkey', 'Pairing successful', 'Failed to pair', pexpect.TIMEOUT, pexpect.EOF])
                    if index == 0 or index == 1:
                        child.sendline('yes')
                    elif index == 2:
                        success = True
                        break
                    elif index >= 3:
                        break
                        
                if success:
                    print(f"🛡️ Pareamento aceito com sucesso pelo Servidor!")
                    child.sendline(f'trust {mac}')
                    child.expect(['#', '>'], timeout=5)
                
                child.sendline('quit')
                child.close()
                return success
            except ImportError as e:
                print("⚠️ Pacote 'pexpect' não instalado. Execute: pip install pexpect")
                print(f"ERRO NO AGENTE: {e}"); return False
            except Exception as e:
                print(f"⚠️ Falha no agente de pareamento: {e}")
                print(f"ERRO NO AGENTE: {e}"); return False

        await asyncio.to_thread(run_pexpect)


    async def _force_system_disconnect(self, mac: str):
        if not mac: return
        try:
            import subprocess
            subprocess.run(["bluetoothctl", "disconnect", mac], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            await asyncio.sleep(1.5)
        except Exception:
            pass

    async def _manage_connection(self, device: Any, connected_event: asyncio.Event, error_holder: list):
        """Gerencia o ciclo de vida da conexão BleakClient ativa."""
        def on_disconnect(client):
            print(f"🔌 Dispositivo {self.device_name} ({self.device}) desconectado.")
            self.status = "disconnected"
            self.client = None
            asyncio.create_task(self.broadcast_callback({
                "type": "status",
                "status": "disconnected",
                "device": self.device_name,
                "message": "Fita desconectada.",
            }))

            # Reconecta automaticamente apenas se a queda foi inesperada e auto_reconnect estiver ativo
            if not self.user_requested_disconnect and self.auto_reconnect and self.is_running:
                print("Tentando reconectar automaticamente em 3 segundos...")
                asyncio.create_task(self._auto_reconnect(self.device))

        try:
            if isinstance(device, str):
                await self._force_system_disconnect(device)
                await self._auto_pair_with_agent(device)
            elif hasattr(device, 'address'):
                await self._force_system_disconnect(device.address)
                await self._auto_pair_with_agent(device.address)
                
            async with BleakClient(device, disconnected_callback=on_disconnect, timeout=12.0) as client:
                self.client = client
                self.status = "connected"

                # Aguarda brevemente para o BlueZ resolver a tabela GATT
                # await asyncio.sleep(0.5)
                # await self._read_battery(client)

                self.reconnect_attempts = 0
                connected_event.set()

                await self.broadcast_callback({
                    "type": "status",
                    "status": "connected",
                    "device": self.device_name,
                    "address": self.device,
                    "battery": self.battery_level,
                    "message": "Polar conectado com sucesso! Transmitindo dados.",
                })

                def on_notification(sender, data: bytearray):
                    payload = parse_heart_rate_data(data)
                    if self.battery_level is not None:
                        payload["battery"] = self.battery_level
                    asyncio.create_task(self.broadcast_callback(payload))

                await client.start_notify(HR_MEASUREMENT_UUID, on_notification)

                # Mantém o loop enquanto estiver conectado e rodando
                while client.is_connected and self.is_running and not self.user_requested_disconnect:
                    await asyncio.sleep(1.0)

                # Desassina notificações ao encerrar normalmente
                try:
                    await client.stop_notify(HR_MEASUREMENT_UUID)
                except Exception:
                    pass

        except Exception as e:
            print(f"Erro na conexão com Polar: {e}")
            self.status = "error"
            self.client = None
            error_holder.append(e)
            connected_event.set()
            await self.broadcast_callback({
                "type": "status",
                "status": "error",
                "message": f"Erro de conexão Bluetooth: {str(e)}",
            })

    async def _auto_reconnect(self, device: Any):
        """Tenta restabelecer a conexão após queda involuntária com backoff exponencial."""
        self.reconnect_attempts += 1
        
        # Limita o atraso máximo a 60 segundos
        delay = min(60.0, 3.0 * (1.5 ** (self.reconnect_attempts - 1)))
        print(f"Tentando reconectar automaticamente em {delay:.1f} segundos (Tentativa {self.reconnect_attempts})...")
        
        await asyncio.sleep(delay)
        
        if not self.user_requested_disconnect and self.is_running:
            try:
                print(f"Reconectando a {self.device_name}...")
                await self.connect_to_device(self.device)
                # Se conectou com sucesso, _manage_connection reseta reconnect_attempts
            except Exception as e:
                print(f"Falha na reconexão automática: {e}")
                # Agenda nova tentativa se falhou
                asyncio.create_task(self._auto_reconnect(self.device))

    async def disconnect(self) -> dict:
        """Desconecta a fita e libera o adaptador Bluetooth do sistema."""
        self.user_requested_disconnect = True
        self.is_running = False

        if self._mock_task and not self._mock_task.done():
            self._mock_task.cancel()

        if self.client and getattr(self.client, "is_connected", False):
            try:
                print(f"Desconectando e liberando fita {self.device_name}...")
                await self.client.disconnect()
            except Exception as e:
                print(f"Erro ao desconectar fita: {e}")

        self.client = None
        self.status = "disconnected"
        self.battery_level = None

        await self.broadcast_callback({
            "type": "status",
            "status": "disconnected",
            "message": "Fita desconectada. Adaptador Bluetooth liberado.",
        })

        return {"status": "disconnected", "message": "Dispositivo desconectado e Bluetooth liberado com sucesso."}

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

    async def _mock_loop(self):
        """Simulador de dados para desenvolvimento do frontend sem fita ligada."""
        base_hr = 72
        while self.is_running and self.status == "connected":
            base_hr += random.choice([-1, 0, 1])
            base_hr = max(55, min(140, base_hr))
            sim_rr = round((60.0 / base_hr) * 1000.0 + random.uniform(-15, 15), 2)

            payload = {
                "type": "data",
                "heart_rate": base_hr,
                "rr_intervals": [sim_rr],
                "timestamp": time.time(),
                "battery": 100,
                "mock": True,
            }
            await self.broadcast_callback(payload)
            await asyncio.sleep(1.0)
