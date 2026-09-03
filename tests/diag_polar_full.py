import asyncio
from bleak import BleakScanner, BleakClient

HR_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"
BATTERY_LEVEL_UUID = "00002a19-0000-1000-8000-00805f9b34fb"

async def main():
    print("Escaneando...")
    devices = await BleakScanner.discover(timeout=5.0)
    polar_device = next((d for d in devices if d.name and "polar" in d.name.lower()), None)
            
    if not polar_device:
        print("Polar nao encontrado.")
        return
        
    print(f"Encontrado: {polar_device.address}")
    
    def on_disconnect(client):
        print(">>> Callback de desconexao ativado!")
        
    async with BleakClient(polar_device, disconnected_callback=on_disconnect) as client:
        print("Conectado. Aguardando 1 seg...")
        await asyncio.sleep(1.0)
        
        try:
            print("Tentando ler bateria...")
            bat = await client.read_gatt_char(BATTERY_LEVEL_UUID)
            print(f"Bateria: {int(bat[0])}%")
        except Exception as e:
            print(f"Erro bateria: {e}")
            
        await asyncio.sleep(1.0)
        
        try:
            print("Iniciando notificacao HR...")
            def on_hr(sender, data):
                print(f"HR Data recebida: {data.hex()}")
            await client.start_notify(HR_MEASUREMENT_UUID, on_hr)
            print("Notificacao iniciada. Aguardando 5 segs...")
            await asyncio.sleep(5.0)
            await client.stop_notify(HR_MEASUREMENT_UUID)
        except Exception as e:
            print(f"Erro HR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
