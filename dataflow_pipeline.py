import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PROJECT_ID    = "crypto-dashboard-gcp"
SUBSCRIPTION  = "projects/crypto-dashboard-gcp/subscriptions/crypto-prices-sub"
BQ_TABLE      = "crypto-dashboard-gcp:crypto_dashboard.prices_raw"
STAGING_BUCKET = "gs://crypto-dashboard-gcp-dataflow-staging"


class ParseAndEnrich(beam.DoFn):

    def process(self, element):
        import json
        import logging
        from datetime import datetime, timezone

        log = logging.getLogger(__name__)

        try:
            data = json.loads(element.decode("utf-8"))

            row = {
                "symbol":              data.get("symbol", "").upper(),
                "name":                data.get("name", ""),
                "price_usd":           float(data.get("price_usd", 0)),
                "change_percent_24h":  float(data.get("change_percent_24h", 0)),
                "volume_usd_24h":      float(data.get("volume_usd_24h", 0)),
                "market_cap_usd":      float(data.get("market_cap_usd", 0)),
                "event_timestamp":     data.get("event_timestamp"),
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            yield row

        except Exception as e:
            import logging
            logging.getLogger(__name__).error(
                "Error procesando mensaje: %s — %s", element, e
            )

def run():
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project        = PROJECT_ID
    google_cloud_options.region         = "europe-west1"
    google_cloud_options.staging_location = f"{STAGING_BUCKET}/staging"
    google_cloud_options.temp_location   = f"{STAGING_BUCKET}/temp"
    google_cloud_options.job_name        = "crypto-prices-pipeline"

    options.view_as(StandardOptions).streaming = True

    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type       = "e2-medium"
    worker_options.max_num_workers    = 2

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Leer de Pub/Sub"    >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parsear y enriquecer" >> beam.ParDo(ParseAndEnrich())
            | "Escribir en BigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()