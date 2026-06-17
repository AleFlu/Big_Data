#!/bin/bash
set -e

case "$SPARK_ROLE" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 --port 7077 --webui-port 8082
    ;;
  worker)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        spark://spark-master:7077 \
        --cores "${SPARK_WORKER_CORES:-1}" \
        --memory "${SPARK_WORKER_MEMORY:-512m}"
    ;;
  driver)
    exec /spark/app/submit.sh
    ;;
  batch)
    # BATCH LAYER STORICO: lancia il loop (o un one-shot se BATCH_LOOP=false).
    # Job FISICAMENTE separato dallo streaming, gira sullo stesso cluster Spark.
    exec /spark/app/batch_loop.sh
    ;;
  *)
    echo "[ERROR] SPARK_ROLE='${SPARK_ROLE}' non riconosciuto. Valori validi: master | worker | driver | batch" >&2
    exit 1
    ;;
esac
