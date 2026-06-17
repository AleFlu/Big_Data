# Makefile — scorciatoie operative per la pipeline IoT Big Data
# Uso: `make <target>`. Lancia `make` o `make help` per l'elenco.

COMPOSE      := docker compose
SPARK_SVCS   := spark-job spark-master spark-worker-1 spark-worker-2 spark-worker-3
ES_URL       := http://localhost:9200
GRAFANA_URL  := http://localhost:3000

.DEFAULT_GOAL := help

.PHONY: help build up down reset rebuild-spark ps logs logs-all health open \
        check check-es check-mongo compile validate-json batch-now

help: ## Mostra questo elenco di comandi
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ── Ciclo di vita dello stack ────────────────────────────────────────────────

build: ## Builda tutte le immagini Docker
	$(COMPOSE) build

up: ## Avvia tutti i servizi in background
	$(COMPOSE) up -d

down: ## Ferma e rimuove i container (mantiene i volumi/dati)
	$(COMPOSE) down

reset: ## Reset COMPLETO: cancella i volumi (DATI PERSI), ribuilda e riavvia
	$(COMPOSE) down -v
	$(COMPOSE) up -d --build

rebuild-spark: ## Ribuilda e ricrea le immagini Spark (job+master+3 worker) + img batch
	# I container Spark condividono lo stesso Dockerfile: vanno ribuildati e RICREATI
	# insieme, altrimenti un container già in esecuzione resta sull'immagine vecchia
	# e i task falliscono a runtime. Il batch è profile-gated (on-demand): ne
	# ribuildiamo solo l'immagine, viene poi ricreato da `make batch-now`.
	$(COMPOSE) build $(SPARK_SVCS)
	$(COMPOSE) up -d --force-recreate $(SPARK_SVCS)
	$(COMPOSE) --profile batch build spark-batch

# ── Batch layer storico (Lambda) ──────────────────────────────────────────────
# Il batch è ON-DEMAND (profile "batch"): NON parte con `make up`. Si lancia a
# comando con `make batch-now` — un run e termina. Scelta per stabilità RAM su
# 8 GB: nessun executor batch sempre attivo, il 3° core resta libero.

batch-now: ## Esegue il batch layer storico ON-DEMAND (snapshot node_baseline) e termina
	# `run --rm`: container effimero, rimosso a fine run. Rilegge tutto
	# raw_readings, ricalcola baseline/validazione/correlazioni, riscrive
	# node_baseline (+ mirror ES) ed esce. Usa il 3° core libero del cluster.
	$(COMPOSE) --profile batch run --rm -e BATCH_LOOP=false spark-batch

# ── Osservabilità ─────────────────────────────────────────────────────────────

ps: ## Stato dei container
	$(COMPOSE) ps

logs: ## Segue i log dello Spark job (i batch dello streaming)
	$(COMPOSE) logs -f spark-job

logs-all: ## Segue i log di tutti i servizi
	$(COMPOSE) logs -f

health: ## Riepilogo rapido: worker Spark attivi + conteggi Elasticsearch
	@echo "── Spark workers ──"
	@curl -s "http://localhost:8082/json/" 2>/dev/null \
		| python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  workers attivi: {d.get('aliveworkers')} | cores usati: {d.get('coresused')}/{d.get('cores')}\")" \
		2>/dev/null || echo "  (Spark master non raggiungibile — stack avviato?)"
	@echo "── Elasticsearch ──"
	@$(MAKE) --no-print-directory check-es

open: ## Apre Grafana nel browser (macOS)
	open $(GRAFANA_URL)

# ── Verifica dati ─────────────────────────────────────────────────────────────

check: check-es check-mongo ## Esegue tutte le verifiche dati (ES + MongoDB)

check-es: ## Conteggio documenti negli indici Elasticsearch
	@for idx in sensors_live_index node_status_index window_stats node_baseline_index sensor_correlation_index; do \
		curl -s "$(ES_URL)/$$idx/_count" 2>/dev/null \
			| python3 -c "import sys,json; print(f'  $$idx:', json.load(sys.stdin).get('count'))" \
			2>/dev/null || echo "  $$idx: (non raggiungibile)"; \
	done

check-mongo: ## Conteggio documenti nelle collezioni MongoDB
	@docker exec mongodb mongosh sensor_data --quiet --eval \
		'["raw_readings","processed_readings","node_stats","agg_per_nodo","fire_events","node_baseline"].forEach(c => print("  " + c + ": " + db[c].countDocuments()))' \
		2>/dev/null || echo "  (MongoDB non raggiungibile — stack avviato?)"

# ── Sviluppo ──────────────────────────────────────────────────────────────────

compile: ## Verifica sintassi dei sorgenti Python (Spark job + batch + producer)
	python3 -m py_compile spark_job/spark_stream_job.py spark_job/batch_analytics.py csv_producer/csv_producer.py
	@echo "OK: sintassi Python valida"

validate-json: ## Valida i dashboard JSON di Grafana
	@for f in grafana/provisioning/dashboards/iot_dashboard.json \
	          grafana/provisioning/dashboards/node_detail_dashboard.json \
	          grafana/provisioning/dashboards/fire_map_dashboard.json \
	          grafana/provisioning/dashboards/historical_dashboard.json; do \
		python3 -c "import json,sys; json.load(open('$$f')); print('OK:', '$$f')" || exit 1; \
	done
