output "pubsub_topic" {
  value = google_pubsub_topic.crypto_prices.id
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.crypto_prices_sub.id
}

output "bigquery_table" {
  value = "${google_bigquery_dataset.crypto_dashboard.dataset_id}.${google_bigquery_table.prices_raw.table_id}"
}

output "dataflow_staging_bucket" {
  value = google_storage_bucket.dataflow_staging.name
}