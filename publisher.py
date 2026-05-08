import asyncio
import json
import os
import logging
import aiohttp
from datetime import datetime, timezone

from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID   = os.getenv("PUBSUB_TOPIC_ID")
API_KEY    = os.getenv("COINCAP_API_KEY")
COINCAP_URL = f"https://rest.coincap.io/v3/assets"

ASSETS     = ["bitcoin", "ethereum", "solana", "binance-coin", "cardano"]
INTERVAL   = 5  # segundos entre cada llamada

publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def publish(payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data)
    future.result()


async def fetch_prices(session: aiohttp.ClientSession) -> list[dict]:
    params  = {"apiKey": API_KEY}
    async with session.get(COINCAP_URL, params=params) as response:
        body = await response.json()
        return body.get("data", [])


async def stream() -> None:
    log.info("Iniciando polling a CoinCap REST API cada %ss...", INTERVAL)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                all_assets = await fetch_prices(session)
                event_ts   = datetime.now(timezone.utc).isoformat()

                for asset in all_assets:
                    if asset["id"] not in ASSETS:
                        continue
                    payload = {
                        "asset_id":           asset["id"],
                        "symbol":             asset["symbol"],
                        "name":               asset["name"],
                        "price_usd":          float(asset["priceUsd"] or 0),
                        "change_percent_24h": float(asset["changePercent24Hr"] or 0),
                        "volume_usd_24h":     float(asset["volumeUsd24Hr"] or 0),
                        "market_cap_usd":     float(asset["marketCapUsd"] or 0),
                        "event_timestamp":    event_ts,
                    }
                    publish(payload)
                    log.info("Publicado: %s (%s) → $%.2f", asset["name"], asset["symbol"], payload["price_usd"])

            except Exception as e:
                log.error("Error en el ciclo: %s", e)

            await asyncio.sleep(INTERVAL)


if __name__ == "__main__":
    asyncio.run(stream())