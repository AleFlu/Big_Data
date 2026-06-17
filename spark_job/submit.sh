#!/bin/bash
set -e

mkdir -p /spark/checkpoints/iot_stream
# Checkpoint dedicato alla query a finestra (window_stats): stream indipendente
# dalla pipeline principale, deve avere offset Kafka e stato tracciati a parte.
mkdir -p /spark/checkpoints/iot_window

# Il driver gira come root, i worker come uid 185 (spark). L'aggregazione a
# finestra mantiene uno state store scritto DAGLI EXECUTOR sui worker: senza
# permessi di scrittura il mkdir di .../iot_window/state/* fallisce e la query
# crasha. La pipeline principale (iot_stream) non ha questo problema perché il
# suo checkpoint lo scrive solo il driver. chmod 777 sul volume condiviso
# allinea i permessi così i worker possono materializzare lo state store.
chmod -R 777 /spark/checkpoints

# --total-executor-cores 2 (non 3): il cluster ha 3 core totali e ne lasciamo 1
# SEMPRE libero per il batch layer on-demand (submit_batch.sh chiede 1 core). Così
# `make batch-now` prende subito il core libero e gira in parallelo senza coda,
# senza mai interrompere lo streaming. Con 4 nodi e micro-batch piccoli 2 core sono
# abbondanti (i batch processano ~30-40 record ciascuno senza ritardo).
exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-512m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-512m}" \
  --total-executor-cores 2 \
  --conf spark.driver.host=spark-job \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /spark/app/spark_stream_job.py
