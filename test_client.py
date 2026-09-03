"""
Script simples para testar o recebimento dos dados via WebSocket no terminal.
Uso: python test_client.py
"""

import asyncio
import json
import websockets

WS_URL = "ws://localhost:8000/ws/hr"


async def listen():
    print(f"Tentando conectar a {WS_URL}...")
    try:
        async with websockets.connect(WS_URL) as ws:
            print("✓ Conectado ao WebSocket com sucesso!\n")
            while True:
                message = await ws.recv()
                data = json.loads(message)

                if data.get("type") == "data":
                    hr = data.get("heart_rate")
                    rr = data.get("rr_intervals", [])
                    battery = data.get("battery")
                    battery_str = f" | Bateria: {battery}%" if battery is not None else ""
                    mock_str = " [MOCK]" if data.get("mock") else ""
                    print(f"❤️ FC: {hr:3d} BPM | RR: {rr}{battery_str}{mock_str}")

                elif data.get("type") == "status":
                    print(f"ℹ️ Status: [{data.get('status')}] - {data.get('message')}")

    except ConnectionRefusedError:
        print("❌ Erro: Não foi possível conectar ao servidor. Certifique-se de que a API está rodando:")
        print("   uvicorn main:app --reload --port 8000")
    except Exception as e:
        print(f"Erro no cliente: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(listen())
    except KeyboardInterrupt:
        print("\nCliente encerrado.")

