
import os
import json
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Charger le fichier .env depuis la racine du projet
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

connection_string = os.getenv(
    "FABRIC_EVENTHUB_CONNECTION_STRING"
)

if not connection_string:
    raise ValueError("Connection string Fabric manquante dans .env")

print("Connexion a Microsoft Fabric...")

event = {
    "source": "VeloToulouse",
    "station_id": "TEST001",
    "velos_disponibles": 10,
    "places_disponibles": 5,
    "timestamp": datetime.now(timezone.utc).isoformat()
}

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_string
)

with producer:
    batch = producer.create_batch()
    batch.add(EventData(json.dumps(event)))
    producer.send_batch(batch)

print("SUCCES : evenement envoye vers Microsoft Fabric !")
