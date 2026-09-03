import asyncio
import time
from bleak import BleakScanner, BleakClient

HR_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"

async def main():
    print("Escaneando fita Polar...")
    devices = await BleakScanner.discover(timeout=5.0)
    polar_device = next((d for d in devices if d.name and "polar" in d.name.lower()), None)
    if not polar_device:
        print("Polar nao encontrado.")
        return
        
    def on_hr(sender, data):
        hr_format = data[0] & 0x01
        bpm = data[1] if hr_format == 0 else int.from_bytes(data[1:3], byteorder='little')
        print(f"[{time.strftime('%H:%M:%S')}] BPM Recebido: {bpm}")
        
    def on_disconnect(client):
        print(">>> Callback de desconexao do sistema ativado!")
        
    print(f"Conectando a {polar_device.address}...")
    async with BleakClient(polar_device, disconnected_callback=on_disconnect) as client:
        print("Conectado! Iniciando notify direto...")
        await client.start_notify(HR_MEASUREMENT_UUID, on_hr)
        
        print("Aguardando 60 segundos para validar estabilidade...")
        for i in range(60):
            await asyncio.sleep(1.0)
            if not client.is_connected:
                print("Conexão caiu prematuramente!")
                break
        
        if client.is_connected:
            await client.stop_notify(HR_MEASUREMENT_UUID)

if __name__ == "__main__":
    asyncio.run(main())
