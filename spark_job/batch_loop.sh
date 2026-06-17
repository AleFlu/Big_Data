#!/bin/bash
set -e

# Runner del BATCH LAYER STORICO.
# Il batch gira in LOOP come service che parte con lo stack: lo streaming usa 2
# core fissi e lascia libero il 3° core, che il batch occupa a ogni ciclo (ogni
# 10 min) e poi rilascia. Per questo il default è LOOP.
# Modalità controllata da BATCH_LOOP (env):
#   - BATCH_LOOP=true  (DEFAULT): loop infinito → submit → sleep 600s (10 min).
#   - BATCH_LOOP=false (one-shot): un solo submit ed esce — `make batch-now`.
# Intervallo configurabile via BATCH_INTERVAL_SECONDS (default 600 = 10 minuti).

BATCH_LOOP="${BATCH_LOOP:-true}"
BATCH_INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-600}"

if [ "$BATCH_LOOP" = "true" ]; then
  echo "[BATCH] Modalità LOOP: un run ogni ${BATCH_INTERVAL_SECONDS}s."
  while true; do
    # `|| true`: un singolo run fallito (es. Mongo momentaneamente irraggiungibile)
    # NON deve uccidere il loop. Il prossimo ciclo riproverà tra 10 minuti.
    /spark/app/submit_batch.sh || echo "[BATCH][WARN] run fallito, riprovo al prossimo ciclo."
    echo "[BATCH] Attendo ${BATCH_INTERVAL_SECONDS}s prima del prossimo run..."
    sleep "$BATCH_INTERVAL_SECONDS"
  done
else
  echo "[BATCH] Modalità ONE-SHOT: un solo run ed esco."
  exec /spark/app/submit_batch.sh
fi
