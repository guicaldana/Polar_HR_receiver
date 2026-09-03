import asyncio
from bleak import BleakScanner, BleakClient

async def main():
    print("Escaneando...")
    devices = await BleakScanner.discover(timeout=5.0)
    polar_device = None
    for d in devices:
        if d.name and "polar" in d.name.lower():
            polar_device = d
            break
            
    if not polar_device:
        print("Polar nao encontrado.")
        return
        
    print(f"Encontrado: {polar_device.name} - {polar_device.address}")
    
    def on_disconnect(client):
        print("Desconectado pelo callback!")
        
    try:
        async with BleakClient(polar_device, disconnected_callback=on_disconnect) as client:
            print("Conectado! Aguardando 10 segundos...")
            await asyncio.sleep(10.0)
            print("Sobreviveu 10 segundos!")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    asyncio.run(main())
