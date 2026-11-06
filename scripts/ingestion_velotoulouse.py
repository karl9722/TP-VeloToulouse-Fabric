
import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from azure.eventhub import (
    EventHubProducerClient,
    EventData,
    TransportType
)

# ==========================================
# CONFIGURATION
# ==========================================

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

CONNECTION_STRING = os.getenv(
    "FABRIC_EVENTHUB_CONNECTION_STRING"
)

API_URL = (
    "https://data.toulouse-metropole.fr/api/explore/v2.1/"
    "catalog/datasets/"
    "velotoulouse-disponibilite-temps-reel/records"
)

INTERVAL = 60
PAGE_SIZE = 100
MAX_STATIONS = 0  # 0 = toutes les stations

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            LOG_DIR / "ingestion.log",
            encoding="utf-8"
        )
    ]
)

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("uamqp").setLevel(logging.WARNING)

if not CONNECTION_STRING:
    raise ValueError(
        "FABRIC_EVENTHUB_CONNECTION_STRING absente du .env"
    )


# ==========================================
# 1. SESSION HTTP AVEC RETRIES
# ==========================================

def create_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    return session


# ==========================================
# 2. RECUPERATION COMPLETE DE L'API
# ==========================================

def fetch_stations():
    records = []
    offset = 0

    with create_session() as session:
        while True:
            response = session.get(
                API_URL,
                params={
                    "limit": PAGE_SIZE,
                    "offset": offset
                },
                timeout=30
            )

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise RuntimeError(
                    "Format de reponse API invalide"
                )

            page = payload.get("results")
            total = payload.get("total_count")

            if not isinstance(page, list):
                raise RuntimeError(
                    "Champ results absent ou invalide"
                )

            if not isinstance(total, int):
                raise RuntimeError(
                    "Champ total_count absent ou invalide"
                )

            if total == 0:
                raise RuntimeError(
                    "L'API ne retourne aucun enregistrement"
                )

            if not page:
                raise RuntimeError(
                    f"Page vide a l'offset {offset}"
                )

            records.extend(page)
            offset += len(page)

            logging.info(
                "API : %s / %s enregistrements",
                len(records),
                total
            )

            if len(records) >= total:
                break

            if offset >= 10000:
                raise RuntimeError(
                    "Limite de pagination de 10000 atteinte"
                )

    return records


# ==========================================
# 3. OUTILS DE NETTOYAGE
# ==========================================

def to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_bool(value):
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False

    return None


# ==========================================
# 4. TRANSFORMATION ET DEDUPLICATION
# ==========================================

def transform_records(records):
    stations = {}

    collected_at = datetime.now(
        timezone.utc
    ).isoformat()

    for record in records:
        if not isinstance(record, dict):
            continue

        raw_data = record.get("data")
        details = {}

        if isinstance(raw_data, str):
            try:
                details = json.loads(raw_data)
            except json.JSONDecodeError:
                logging.warning("JSON interne invalide")
                continue

        elif isinstance(raw_data, dict):
            details = raw_data

        if not isinstance(details, dict):
            continue

        station_id = details.get(
            "station_id",
            record.get("station_id")
        )

        if station_id is None:
            continue

        types = details.get(
            "vehicle_types_available", []
        )

        if not isinstance(types, list):
            types = []

        mechanical = 0
        electrical = 0

        for item in types:
            if not isinstance(item, dict):
                continue

            bike_type = item.get("vehicle_type_id")
            count = to_int(item.get("count"))

            if bike_type == "mechanical":
                mechanical += count

            elif bike_type == "electrical":
                electrical += count

        bikes = to_int(
            details.get(
                "num_bikes_available",
                record.get("num_bikes_available")
            )
        )

        docks = to_int(
            details.get(
                "num_docks_available",
                record.get("num_docks_available")
            )
        )

        last_reported = to_int(
            details.get(
                "last_reported",
                record.get("last_reported")
            )
        )

        event = {
            "source": "VeloToulouse",
            "station_id": str(station_id),
            "num_bikes_available": bikes,
            "num_docks_available": docks,
            "num_bikes_disabled": to_int(
                details.get("num_bikes_disabled")
            ),
            "mechanical_bikes": mechanical,
            "electrical_bikes": electrical,
            "is_renting": to_bool(
                details.get("is_renting")
            ),
            "is_returning": to_bool(
                details.get("is_returning")
            ),
            "last_reported": last_reported,
            "collected_at": collected_at
        }

        key = str(station_id)
        previous = stations.get(key)

        if (
            previous is None
            or last_reported > previous["last_reported"]
        ):
            stations[key] = event

    return sorted(
        stations.values(),
        key=lambda x: (
            0 if x["station_id"].isdigit() else 1,
            int(x["station_id"])
            if x["station_id"].isdigit()
            else x["station_id"]
        )
    )


# ==========================================
# 5. ENVOI DES EVENEMENTS VERS FABRIC
# ==========================================

def send_events(producer, events):
    batch = producer.create_batch()
    sent = 0

    for event in events:
        message = EventData(
            json.dumps(event, ensure_ascii=False)
        )

        try:
            batch.add(message)

        except ValueError:
            if len(batch) == 0:
                raise

            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(message)

        sent += 1

    if len(batch) > 0:
        producer.send_batch(batch)

    return sent


# ==========================================
# 6. BOUCLE D'INGESTION
# ==========================================

def main():
    logging.info(
        "Demarrage ingestion VeloToulouse"
    )

    with EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        transport_type=TransportType.AmqpOverWebsocket,
        retry_total=5,
        logging_enable=False
    ) as producer:

        while True:
            started = time.monotonic()

            try:
                records = fetch_stations()
                events = transform_records(records)

                if MAX_STATIONS > 0:
                    events = events[:MAX_STATIONS]

                if not events:
                    raise RuntimeError(
                        "Aucune station valide recue"
                    )

                logging.info(
                    "Enregistrements API : %s",
                    len(records)
                )

                logging.info(
                    "Stations distinctes : %s",
                    len(events)
                )

                sent = send_events(
                    producer,
                    events
                )

                logging.info(
                    "SUCCES : %s evenements envoyes vers Fabric",
                    sent
                )

            except Exception:
                logging.exception(
                    "Erreur pendant l'ingestion"
                )

            elapsed = time.monotonic() - started
            wait = max(0, INTERVAL - elapsed)

            logging.info(
                "Prochaine collecte dans %.0f secondes",
                wait
            )

            time.sleep(wait)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info(
            "Arret du streaming VeloToulouse"
        )
