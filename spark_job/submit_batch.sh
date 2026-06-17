#!/bin/bash
set -e

# spark-submit del BATCH LAYER verso lo stesso cluster standalone dello streaming.
# Speculare a submit.sh ma con due differenze importanti:
#   - lancia batch_analytics.py (job batch, non lo streaming);
#   - spark.driver.host = spark-batch (il NOME del NUOVO service nel compose).
#     Deve combaciare con `hostname: spark-batch` nel docker-compose: in
#     deploy-mode client il driver gira QUI e gli executor sul cluster devono
#     poterlo raggiungere per nome. Se questo host fosse sbagliato, gli executor
#     non si connetterebbero al driver e il job resterebbe appeso.
#
# --total-executor-cores 1: lo streaming gira stabilmente a 2 core (submit.sh) e
# lascia 1 core libero. Il batch chiede esattamente quell'1 core, così parte SUBITO
# senza coda e gira in parallelo allo streaming. Con 3 core totali nel cluster
# (2 streaming + 1 batch) i due layer Lambda coesistono senza conflitti.
exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-512m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-512m}" \
  --total-executor-cores 1 \
  --conf spark.driver.host=spark-batch \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=16 \
  /spark/app/batch_analytics.py
