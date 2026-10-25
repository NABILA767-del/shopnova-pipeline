"""
Consommateur Kafka → MinIO (Couche Bronze)
------------------------------------------
Écoute les topics CDC Debezium (shopnova.public.*) et écrit chaque
événement brut (before/after/op) dans MinIO, partitionné par
ingestion_date/heure.

Exigence couverte :
  - Bronze : événements de changement bruts dans MinIO (avant/après)

Grain : un fichier = un événement CDC (une modification d'une ligne).

Usage :
    python ingestion/bronze_writer.py
"""

import os
import io
import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

# ============================================================
# Configuration
# ============================================================
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = "bronze-writer-group"
TOPICS = ["shopnova.public.customers", "shopnova.public.products", "shopnova.public.orders"]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "ShopNova2026!")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bronze_writer")


# ============================================================
# Client MinIO (S3 compatible)
# ============================================================
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
        log.info("✅ Bucket '%s' existe déjà", bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        log.info("✅ Bucket '%s' créé", bucket)


# ============================================================
# Écriture d'un événement dans MinIO
# ============================================================
def write_event(s3, table, event):
    """
    Écrit un événement CDC dans MinIO.
    Partitionnement : bronze/<table>/ingestion_date=YYYY-MM-DD/hour=HH/<offset>.json
    """
    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")

    # Extraire l'offset et la clé
    offset = event.get("offset", "0")
    table_name = table.split(".")[-1]

    key = f"{table_name}/ingestion_date={ingestion_date}/hour={hour}/{offset}.json"

    body = json.dumps(event, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key


# ============================================================
# Boucle principale : consommation Kafka
# ============================================================
def main():
    log.info("Démarrage du consumer Kafka → MinIO")
    log.info("Kafka : %s", KAFKA_BOOTSTRAP)
    log.info("MinIO : %s / bucket=%s", MINIO_ENDPOINT, MINIO_BUCKET)

    # 1. Client MinIO
    s3 = get_s3_client()
    ensure_bucket(s3, MINIO_BUCKET)

    # 2. Consumer Kafka
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    log.info("Abonné aux topics : %s", TOPICS)

    nb_events = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            topic = msg.topic()
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.warning("Message non-JSON ignoré : %s", e)
                continue

            # Construire l'événement avec métadonnées
            event = {
                "topic": topic,
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                "payload": payload,
            }

            key = write_event(s3, topic, event)
            nb_events += 1

            if nb_events % 10 == 0:
                log.info("📥 %d événements écrits (dernier : %s)", nb_events, key)

    except KeyboardInterrupt:
        log.info("⏹️  Interruption manuelle")
    finally:
        consumer.close()
        log.info("📊 Bilan : %d événements écrits dans MinIO", nb_events)


if __name__ == "__main__":
    main()