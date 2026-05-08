# Crypto Price Dashboard — GCP Streaming Pipeline

Pipeline de datos en tiempo real que ingesta precios de criptomonedas desde la API de CoinCap, los procesa con Apache Beam en Google Cloud Dataflow y los almacena en BigQuery para su visualización en Looker Studio.

## Arquitectura

```
CoinCap REST API
      │
      │  polling cada 5s (Python)
      ▼
Cloud Pub/Sub (topic: crypto-prices)
      │
      │  Apache Beam / Dataflow
      ▼
BigQuery (crypto_dashboard.prices_raw)
      │
      │  vistas agregadas
      ▼
Looker Studio Dashboard
```

**Servicios GCP utilizados:** Pub/Sub · Dataflow · BigQuery · Cloud Storage (staging)

---

## Requisitos previos

- Python 3.11 (no 3.12 — apache-beam no tiene wheels para 3.12 en Windows aún)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) instalado y autenticado
- [Terraform](https://developer.hashicorp.com/terraform/install) instalado
- Cuenta GCP con créditos activos
- API key gratuita de [CoinCap](https://coincap.io)

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/gcp_dataflow.git
cd gcp_dataflow
```

### 2. Crear y activar el entorno virtual

```powershell
# Windows
python -m venv venv
.\venv\Scripts\Activate.ps1
```

```bash
# macOS / Linux
python3.11 -m venv venv
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
# Instalar grpcio primero de forma aislada (evita conflictos en Windows)
pip install --only-binary=:all: grpcio==1.59.3 grpcio-status==1.59.3

# Instalar el resto
pip install -r requirements.txt
```

---

## Configuración

### 1. Configurar GCP

Autentícate y configura el proyecto:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project crypto-dashboard-gcp
gcloud auth application-default set-quota-project crypto-dashboard-gcp
```

### 2. Crear el archivo `.env`

Copia el archivo de ejemplo y rellena los valores:

```bash
cp .env.example .env
```

Edita `.env` con tus valores:

```
GOOGLE_APPLICATION_CREDENTIALS=credentials.json
GCP_PROJECT_ID=crypto-dashboard-gcp
PUBSUB_TOPIC_ID=crypto-prices
COINCAP_API_KEY=tu_api_key_de_coincap
```

### 3. Colocar las credenciales de la Service Account

Descarga el JSON de la Service Account desde GCP y colócalo en la raíz del proyecto como `credentials.json`.

> ⚠️ `credentials.json` y `.env` están en `.gitignore` — nunca los subas al repositorio.

---

## Infraestructura con Terraform

Crea todos los recursos GCP necesarios (Pub/Sub, BigQuery, GCS, APIs):

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

Escribe `yes` cuando lo solicite. Al terminar verás los outputs:

```
bigquery_table           = "crypto_dashboard.prices_raw"
dataflow_staging_bucket  = "crypto-dashboard-gcp-dataflow-staging"
pubsub_subscription      = "projects/crypto-dashboard-gcp/subscriptions/crypto-prices-sub"
pubsub_topic             = "projects/crypto-dashboard-gcp/topics/crypto-prices"
```

```bash
cd ..
```

---

## Ejecución

Necesitas **dos terminales** abiertas en la raíz del proyecto con el venv activo.

### Terminal 1 — Publisher (CoinCap → Pub/Sub)

```bash
python publisher.py
```

Verás los precios publicándose cada 5 segundos:

```
2025-05-07 10:23:03 [INFO] Publicado: Bitcoin (BTC) → $62483.21
2025-05-07 10:23:03 [INFO] Publicado: Ethereum (ETH) → $3012.88
2025-05-07 10:23:03 [INFO] Publicado: Solana (SOL) → $148.32
```

### Terminal 2 — Pipeline Dataflow (Pub/Sub → BigQuery)

```bash
python dataflow_pipeline.py \
  --runner=DataflowRunner \
  --project=crypto-dashboard-gcp \
  --region=europe-west1
```

El job tarda **3-5 minutos** en arrancar. Puedes seguir el progreso en:
👉 https://console.cloud.google.com/dataflow/jobs?project=crypto-dashboard-gcp

Cuando veas `All workers have finished the startup processes`, el pipeline está activo.

---

## Verificación

### Comprobar mensajes en Pub/Sub

```bash
gcloud pubsub subscriptions pull crypto-prices-sub \
  --limit=5 \
  --auto-ack \
  --project=crypto-dashboard-gcp
```

### Comprobar datos en BigQuery

```bash
bq query \
  --use_legacy_sql=false \
  --project=crypto-dashboard-gcp \
  "SELECT symbol, price_usd, event_timestamp
   FROM crypto_dashboard.prices_raw
   ORDER BY event_timestamp DESC
   LIMIT 10"
```

---

## Vistas para el dashboard

Ejecuta estas queries en BigQuery para crear las vistas optimizadas para Looker Studio:

```sql
-- Último precio por cripto (para tarjetas y tabla resumen)
CREATE OR REPLACE VIEW crypto_dashboard.prices_latest AS
SELECT
  symbol, name, price_usd,
  change_percent_24h, volume_usd_24h, market_cap_usd,
  event_timestamp
FROM crypto_dashboard.prices_raw
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_timestamp DESC) = 1;

-- Precio promedio por minuto (para gráfico de líneas)
CREATE OR REPLACE VIEW crypto_dashboard.prices_by_minute AS
SELECT
  symbol,
  TIMESTAMP_TRUNC(event_timestamp, MINUTE) AS minute,
  AVG(price_usd)  AS avg_price_usd,
  MAX(price_usd)  AS max_price_usd,
  MIN(price_usd)  AS min_price_usd
FROM crypto_dashboard.prices_raw
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY symbol, minute;
```

---

## Dashboard en Looker Studio

1. Ve a https://lookerstudio.google.com
2. Crea un informe nuevo → conector **BigQuery**
3. Conecta `prices_latest` para tarjetas y tabla resumen
4. Conecta `prices_by_minute` para el gráfico de líneas (dimensión: `minute`, métrica: `avg_price_usd`, desglose: `symbol`)

---

## Parar el pipeline de forma controlada

### Publisher
`Ctrl+C` en el terminal.

### Dataflow
Nunca uses `Ctrl+C` para el pipeline — el job seguiría corriendo en GCP consumiendo créditos.

Para pararlo correctamente:
1. Ve a https://console.cloud.google.com/dataflow/jobs?project=crypto-dashboard-gcp
2. Selecciona el job activo
3. Haz clic en **Detener** → **Drenar** → confirmar

O desde la CLI:

```bash
gcloud dataflow jobs drain JOB_ID --region=europe-west1
```

---

## Destruir la infraestructura

Para eliminar todos los recursos GCP y dejar de incurrir en costes:

```bash
cd terraform
terraform destroy
```

Escribe `yes` cuando lo solicite.

---

## Estructura del proyecto

```
gcp_dataflow/
├── terraform/
│   ├── main.tf           # Recursos GCP (Pub/Sub, BQ, GCS, APIs)
│   ├── variables.tf      # Definición de variables
│   ├── outputs.tf        # Outputs tras el apply
│   └── terraform.tfvars  # Valores de las variables
├── publisher.py          # Polling CoinCap → Pub/Sub
├── dataflow_pipeline.py  # Pipeline Beam: Pub/Sub → BigQuery
├── requirements.txt      # Dependencias Python
├── .env.example          # Plantilla de variables de entorno
└── .gitignore
```

---

## Criptomonedas monitorizadas

| ID | Símbolo | Nombre |
|----|---------|--------|
| bitcoin | BTC | Bitcoin |
| ethereum | ETH | Ethereum |
| solana | SOL | Solana |
| binance-coin | BNB | BNB |
| cardano | ADA | Cardano |

Para añadir más, edita el set `ASSETS` en `publisher.py`.