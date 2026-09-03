# Polar HR Receiver (FastAPI + WebSockets)

Serviço de backend em Python para captura de frequência cardíaca da fita **Polar H10** via **Bluetooth Low Energy (BLE)** e transmissão em tempo real via **WebSocket** para aplicações web (Next.js / React).

---

## 📁 Estrutura do Projeto

```
Polar_HR_receiver/
├── main.py              # API FastAPI e endpoint WebSocket (/ws/hr)
├── polar_worker.py      # Worker BLE com Bleak, decodificador GATT e reconexão
├── test_client.py       # Script CLI para testar o WebSocket no terminal
├── requirements.txt     # Dependências Python
└── README.md            # Documentação de uso
```

---

## 🚀 Instalação e Execução

### 1. Criar ambiente virtual e instalar dependências
```bash
cd /home/guicaldana/UFES/HCS/Polar_HR_receiver
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Iniciar a API com a fita Polar H10 real
Certifique-se de que a fita esteja com os eletrodos umedecidos e posicionada no peito:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Modo Simulação (Mock) — Sem precisar da fita física
Ideal para desenvolver a interface no Next.js quando a fita estiver desligada ou descarregada:
```bash
MOCK_POLAR=true uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

---

## 🧪 Testando o WebSocket no Terminal

Em outro terminal (com o ambiente virtual ativo):
```bash
python test_client.py
```
Você verá os dados de frequência cardíaca (BPM) e intervalos RR chegando ao vivo no console.

---

## 🌐 Endpoints da API

- **`GET /`**: Informações gerais da API e estado do dispositivo.
- **`GET /health`**: Status detalhado da conexão, nível de bateria e número de clientes conectados.
- **`WS /ws/hr`**: Canal WebSocket para streaming em tempo real.

---

## 📦 Formato dos Dados (JSON) via WebSocket

### Pacote de Dados (`type: "data"`):
```json
{
  "type": "data",
  "heart_rate": 74,
  "rr_intervals": [812.5, 808.2],
  "timestamp": 1725331200.45,
  "battery": 95,
  "mock": false
}
```

### Pacote de Status (`type: "status"`):
```json
{
  "type": "status",
  "status": "connected",
  "device": "Polar H10 12345678",
  "battery": 95,
  "message": "Polar conectado com sucesso! Transmitindo dados."
}
```

---

## ⚛️ Conectando com o Next.js

No seu frontend Next.js, conecte-se ao WebSocket:
```typescript
const ws = new WebSocket('ws://localhost:8000/ws/hr');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'data') {
    console.log('BPM:', data.heart_rate);
    console.log('Intervalos RR (ms):', data.rr_intervals);
  }
};
```
Consulte o relatório completo de integração com componentes React e hooks em:  
`~/.gemini/antigravity/brain/3d63c2e2-5dab-4086-9001-e7fd576ef9e4/relatorio_implementacao_fastapi_websockets.md`

