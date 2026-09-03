# Polar HR Receiver (FastAPI + WebSockets)

Serviço de backend em Python para captura de frequência cardíaca da fita **Polar H10** via **Bluetooth Low Energy (BLE)** e transmissão em tempo real via **WebSocket** para aplicações web (Next.js / React).

---

## 📁 Estrutura do Projeto

```
Polar_HR_receiver/
├── main.py                     # API FastAPI e endpoint WebSocket (/ws/hr)
├── polar_worker.py             # Worker BLE com Bleak, decodificador GATT e reconexão
├── test_client.py              # Script CLI para testar o WebSocket no terminal
├── requirements.txt            # Dependências Python
├── RELATORIO_IMPLEMENTACAO.md  # Relatório técnico completo de arquitetura e código
├── README.md                   # Documentação do projeto
└── .gitignore                  # Regras de exclusão Git
```

---

## 🚀 Instalação e Execução

### 1. Clonar o repositório e criar o ambiente virtual
```bash
git clone git@github.com:guicaldana/Polar_HR_receiver.git
cd Polar_HR_receiver

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Iniciar a API com a fita Polar H10 real
Certifique-se de que a fita esteja com os eletrodos umedecidos e posicionada no peito:
```bash
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Modo Simulação (Mock) — Sem precisar da fita física
Ideal para desenvolver a interface no Next.js quando a fita estiver desligada, sem bateria ou sem receptor Bluetooth no computador de desenvolvimento:
```bash
source venv/bin/activate
MOCK_POLAR=true uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
> **Nota:** Em modo mock (`MOCK_POLAR=true`), o servidor roda mesmo sem a biblioteca `bleak` ou adaptadores Bluetooth disponíveis.

---

## 🧪 Testando o WebSocket no Terminal

Em outro terminal (com o ambiente virtual ativo):
```bash
source venv/bin/activate
python test_client.py
```
Você verá os dados de frequência cardíaca (BPM), nível de bateria e intervalos RR chegando ao vivo no console.

---

## 🌐 Endpoints da API

- **`GET /devices/scan`**: Escaneia dispositivos BLE ao redor (retorna nome, endereço MAC e intensidade RSSI).
- **`POST /devices/connect`**: Conecta a um dispositivo BLE específico pelo endereço MAC (ex: `{"address": "24:AC:AC:16:68:08"}`).
- **`POST /devices/disconnect`**: Desconecta a fita ativa e **libera o adaptador Bluetooth do Linux**.
- **`GET /devices/status`**: Estado atual detalhado da conexão BLE e do dispositivo.
- **`GET /health`**: Verificação de saúde da API, status da fita e clientes WebSocket ativos.
- **`WS /ws/hr`**: Canal WebSocket para streaming em tempo real (dados de FC e status).
- **`GET /docs`**: Documentação interativa Swagger UI da FastAPI.

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

Para exemplos completos de integração com hooks customizados (`usePolarHeartRate`), gráficos temporais e componentes React, consulte o documento:
- [`RELATORIO_IMPLEMENTACAO.md`](RELATORIO_IMPLEMENTACAO.md)

---

## 🛠 Troubleshooting: Lidando com Instabilidades do Bluetooth (Linux / BlueZ)

A conexão BLE no Linux pode sofrer com travamentos e loops de desconexão. Abaixo estão as resoluções para os cenários mais comuns (Especialmente com cintas Polar H10):

### 1. Desconexões constantes com menos de 20 segundos
**Causa:** A cinta Polar H10 desliga o próprio hardware de transmissão Bluetooth automaticamente para economizar bateria se os sensores não estiverem captando batimentos cardíacos.\n**Solução:**
- Vista a fita no peito.
- **Umedeça bem os eletrodos de borracha** para garantir a condutividade.
- Sem estar no peito, ela nunca ficará estável!

### 2. Erro: `failed to discover services, device disconnected`
**Causa:** Conexão "Fantasma" (Ghost Connection) no BlueZ. Ao reiniciar a API abruptamente (CTRL+C), o Linux (BlueZ) não envia o encerramento da conexão, mantendo a fita conectada em background. Como a Polar só aceita 1 conexão, a nova tentativa de conexão é bloqueada. Outra causa pode ser a corrupção do cache GATT no Linux.
**Solução:**
- Desligue e ligue o Bluetooth nas configurações do seu SO.
- **Ou** pelo terminal: `bluetoothctl disconnect <MAC_ADDRESS>` (ex: `bluetoothctl disconnect 24:AC:AC:16:68:08`).
- Sempre prefira desconectar através do endpoint `/devices/disconnect` antes de derrubar o servidor.

### 3. Fita conecta, envia apenas 1 dado (BPM) e desconecta
**Causa:** Restrição de Segurança do firmware da Polar. A fita se recusa a transmitir dados contínuos para um host não pareado e derruba a conexão caso não receba a chave de criptografia de *Bonding*.
**Solução:**
Você precisa parear (Trust & Pair) o dispositivo de forma definitiva pelo nível do sistema:
1. Abra o terminal e limpe o cache antigo: `bluetoothctl remove 24:AC:AC:16:68:08`
2. Pareie com confiança profunda: `bluetoothctl pair 24:AC:AC:16:68:08`
3. Após receber a mensagem `Pairing successful`, sua leitura na API será 100% estável.
