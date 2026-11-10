# Projet ShopNova - Pipeline Data E-commerce

**Membres :** Nabila Imouzaz, Jihane Bouazza, Asmaa Aandadi (Master Data & IA - INPT)

## Architecture
PostgreSQL (Source) -> Debezium -> Kafka -> MinIO (Bronze) -> Spark/Iceberg (Silver) -> dbt/DuckDB (Gold) -> Metabase (Dashboard).

## 🚀 Lancement rapide (5 commandes)

```bash
# 1. Démarrer toute l'infrastructure
docker compose up -d

# 2. Lancer le simulateur (génère les données dans PostgreSQL)
python source/simulateur.py

# 3. Lire Kafka et écrire dans le Bucket Bronze
python ingestion/bronze_writer.py

# 4. Traiter la couche Silver (Spark + Iceberg)
docker exec -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD="ShopNova2026!" -e MINIO_ENDPOINT="http://minio:9000" -it shopnova-spark /opt/spark/bin/spark-submit --master "local[*]" /opt/silver/silver_job.py --full

# 5. Construire la couche Gold et préparer Metabase
cd shopnova_gold && dbt build --full-refresh && cd ..
python create_metabase_db.py