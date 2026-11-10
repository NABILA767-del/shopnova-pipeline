#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${SPARK_CONTAINER:-shopnova-spark}"

if [ -f .env ]; then
  set -a; source .env; set +a
fi

echo "▶ Spark-submit Silver…"
docker exec -e MINIO_ROOT_USER="${MINIO_ROOT_USER}" \
            -e MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
            -e MINIO_ENDPOINT="http://minio:9000" \
            -it "$CONTAINER" \
  /opt/spark/bin/spark-submit \
    --master "local[*]" \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    /opt/silver/silver_job.py "$@"