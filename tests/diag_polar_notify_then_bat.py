import asyncio
from bleak import BleakScanner, BleakClient

HR_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"
BATTERY_LEVEL_UUID = "00002a19-0000-1000-8000-00805f9b34fb"

async def main():
    devices = await BleakScanner.discover(timeout=5.0)
    polar_device = next((d for d in devices if d.name and "polar" in d.name.lower()), None)
    if not polar_device:
        print("Polar nao encontrado.")
        return
        
    def on_hr(sender, data):
        print(f"BPM: {data.hex()}")
        
    def on_disconnect(client):
        print(">>> Disconectado!")
        
    async with BleakClient(polar_device, disconnected_callback=on_disconnect) as client:
        print("Conectado! Iniciando notify...")
        await client.start_notify(HR_MEASUREMENT_UUID, on_hr)
        
        await asyncio.sleep(2.0)
        
        print("Tentando ler bateria agora...")
        try:
            bat = await client.read_gatt_char(BATTERY_LEVEL_UUID)
            print(f"Bateria: {bat[0]}%")
        except Exception as e:
            print(f"Erro bateria: {e}")
            
        print("Aguardando 10 segundos...")
        await asyncio.sleep(10.0)

if __name__ == "__main__":
    asyncio.run(main())
