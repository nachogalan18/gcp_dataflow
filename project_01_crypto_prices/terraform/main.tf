terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# ── APIs ──────────────────────────────────────────────────────────────────────

resource "google_project_service" "pubsub" {
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "dataflow" {
  service            = "dataflow.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

# ── Pub/Sub ───────────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "crypto_prices" {
  name    = "crypto-prices"
  project = var.project_id

  depends_on = [google_project_service.pubsub]
}

resource "google_pubsub_subscription" "crypto_prices_sub" {
  name    = "crypto-prices-sub"
  topic   = google_pubsub_topic.crypto_prices.name
  project = var.project_id

  ack_deadline_seconds = 60
  message_retention_duration = "600s"
}

# ── GCS bucket (staging para Dataflow) ───────────────────────────────────────

resource "google_storage_bucket" "dataflow_staging" {
  name          = "${var.project_id}-dataflow-staging"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  depends_on = [google_project_service.storage]
}

# ── BigQuery ──────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "crypto_dashboard" {
  dataset_id  = "crypto_dashboard"
  description = "Dataset para el dashboard de criptomonedas"
  location    = "EU"

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_table" "prices_raw" {
  dataset_id          = google_bigquery_dataset.crypto_dashboard.dataset_id
  table_id            = "prices_raw"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    {
      name = "symbol"
      type = "STRING"
      mode = "REQUIRED"
      description = "Símbolo de la cripto (BTC, ETH...)"
    },
    {
      name = "name"
      type = "STRING"
      mode = "NULLABLE"
      description = "Nombre completo"
    },
    {
      name = "price_usd"
      type = "FLOAT64"
      mode = "REQUIRED"
      description = "Precio en USD"
    },
    {
      name = "change_percent_24h"
      type = "FLOAT64"
      mode = "NULLABLE"
      description = "Variación porcentual en 24h"
    },
    {
      name = "volume_usd_24h"
      type = "FLOAT64"
      mode = "NULLABLE"
      description = "Volumen en USD en 24h"
    },
    {
      name = "market_cap_usd"
      type = "FLOAT64"
      mode = "NULLABLE"
      description = "Market cap en USD"
    },
    {
      name = "event_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Timestamp del evento recibido"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "Timestamp de inserción en BigQuery"
    }
  ])
}