# IoT Streaming Pipeline — Real-time Fire & Anomaly Detection

> 4 sensor nodes · Kafka cluster · Spark Structured Streaming + Batch · Elasticsearch · MongoDB · Grafana

<p>
  <img alt="Kafka" src="https://img.shields.io/badge/Kafka-7.6.1-231F20?logo=apachekafka&logoColor=white">
  <img alt="Spark" src="https://img.shields.io/badge/Spark-3.5.3-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Elasticsearch" src="https://img.shields.io/badge/Elasticsearch-7.17-005571?logo=elasticsearch&logoColor=white">
  <img alt="MongoDB" src="https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb&logoColor=white">
  <img alt="Grafana" src="https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker%20Compose-orchestrated-2496ED?logo=docker&logoColor=white">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white">
</p>

<p align="center">
  <img alt="Grafana dashboard demo" src="docs/grafana_dashboard.gif" width="720">
  <br>
  <em>Grafana dashboard — real-time sensor metrics, anomaly and fire detection</em>
</p>

---

## Quick start

### 1. Clone

```bash
git clone https://github.com/AleFlu/Big_Data_iot.git
cd Big_Data_iot
```

### 2. Create `.env`

```bash
cp .env.example .env
```

Generate a Kafka KRaft cluster ID and paste it in:

```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```

```
# .env
CLUSTER_ID=<generated-uuid>
PRODUCER_DELAY_MS=500
ES_HEAP_SIZE=512m
MONGO_INITDB_DATABASE=sensor_data
ALERT_WEBHOOK_URL=          # optional — leave empty to disable fire alerts
```

### 3. Place the sensor CSV files

```
acquisizioni/
├── Nodo_1/prima_acquisizione/nodo1_csv.csv   # ~1 733 rows · includes Fire column
├── Nodo_2/prima_acquisizione/nodo2.csv       # ~1 732 rows · includes Fire column
├── Nodo_3/prima_acq/nodo3_csv.csv            # ~1 669 rows · includes Fire column
└── Nodo_4/nodo4_csv.csv                      # ~1 611 rows · no Fire column
```

Columns: `Temperature (C)` · `Humidity (%)` · `Pressure (hPA)` · `Gas (Ohm)` · `Visible Light` · `IR` · `UV index` · `CO` · `NO2` · `Smoke (ppm)` · `Fire` (optional)

### 4. Start everything

```bash
make up        # or: docker compose up -d
```

Wait ~60–90 seconds for all health checks to pass, then open Grafana:

```
http://localhost:3000   (admin / admin)
```

> The streaming pipeline (`make up`) is the **speed layer** and runs continuously.
> The **batch layer** (historical analytics) is on-demand — run `make batch-now`
> once raw data has accumulated. See [Lambda Architecture](#lambda-architecture).

<details>
<summary>Step-by-step startup (first time or after issues)</summary>

```bash
# Build custom images
docker compose build

# Core infrastructure (3-broker Kafka, Mongo, Elasticsearch)
docker compose up -d kafka kafka2 kafka3 mongodb elasticsearch

# Create the Kafka topic (wait ~30–60s for the brokers to be healthy)
docker compose up kafka-init

# Spark cluster
docker compose up -d spark-master
# Check: http://localhost:8082 → Spark Master UI
docker compose up -d spark-worker-1 spark-worker-2 spark-worker-3
# Check: http://localhost:8082 → 3 Workers

# Spark streaming job (driver)
docker compose up -d spark-job
docker compose logs -f spark-job
# Wait for: "Streaming query avviata" and "Window query avviata"

# CSV producers (one per node)
docker compose up -d csv-producer-1 csv-producer-2 csv-producer-3 csv-producer-4

# Monitoring tools
docker compose up -d grafana mongo-express kafka-ui
```

</details>

---

## Services & ports

| Service | URL | Credentials |
|---|---|---|
| **Grafana** | http://localhost:3000 | admin / admin |
| Spark Master UI | http://localhost:8082 | — |
| Kafka UI | http://localhost:8080 | — |
| Mongo Express | http://localhost:8081 | — |
| Elasticsearch | http://localhost:9200 | — |
| MongoDB | localhost:27017 | — |
| Kafka | localhost:9092 | — |

---

## Makefile shortcuts

A `Makefile` wraps the most common commands. Run `make` (or `make help`) for the full list:

```bash
make up              # start the whole streaming stack
make logs            # follow the Spark streaming job logs
make health          # active Spark workers + Elasticsearch doc counts
make check           # document counts across Elasticsearch + MongoDB
make batch-now       # run the historical batch layer once (on-demand)
make rebuild-spark   # rebuild & recreate all Spark images together (+ batch)
make compile         # py_compile all Python sources
make validate-json   # validate the Grafana dashboard JSON
make reset           # full reset (drops volumes, rebuilds, restarts)
```

> `make rebuild-spark` rebuilds and recreates the driver, master and all three
> workers in one shot — they share a single Dockerfile, so they must always be
> rebuilt together (rebuilding only the driver leaves the workers on a stale
> image and tasks fail at runtime). It also rebuilds the on-demand batch image.

---

## What this project does

A fully containerised streaming pipeline that ingests environmental sensor data from 4 independent IoT nodes, processes it in real-time with anomaly detection and fire-transition logic, and visualises everything on live Grafana dashboards. It implements a **Lambda Architecture**: a continuous *speed layer* (Spark Structured Streaming) plus an on-demand *batch layer* (Spark batch analytics over the full history).

Built as a university project for the Big Data course (Master's degree), the system intentionally simulates a **multi-machine deployment**: each sensor node runs in its own container, Spark runs as a Standalone cluster (1 master + 3 workers), and every service communicates via explicit hostnames as it would on separate physical hosts.

### Architecture

```
[machine-1]  csv-producer-1 (nodo_1) ──┐
[machine-2]  csv-producer-2 (nodo_2) ──┤──► Kafka cluster ──► Spark Standalone Cluster
[machine-3]  csv-producer-3 (nodo_3) ──┤   3 brokers·4 parts   (master + 3 workers)
[machine-4]  csv-producer-4 (nodo_4) ──┘   replication-factor 3        │
                                                          ┌────────────┴────────────┐
                                                       MongoDB                Elasticsearch
                                                     6 collections             5 indices
                                                          └────────────┬────────────┘
                                                                       ▼
                                                                    Grafana
                                                              (4 dashboards)
```

A Mermaid version lives in [`architettura.mmd`](architettura.mmd).

### Tech Stack

| Component | Technology | Version |
|---|---|---|
| Message broker | Confluent Kafka cluster — 3 brokers, KRaft, no ZooKeeper | 7.6.1 |
| Stream processing | Apache Spark Structured Streaming + Batch | 3.5.3 |
| Batch analytics | Spark SQL + MLlib (`Correlation`) | 3.5.3 |
| Time-series store | Elasticsearch | 7.17.28 |
| Document store | MongoDB | 7.0 |
| Dashboards | Grafana | 10.4.2 |
| Producer | Python + kafka-python-ng | 3.11 |
| Platform | Docker Compose · ARM64 (Apple Silicon) | — |

### Pipeline (speed layer)

Each sensor node continuously streams CSV rows to a dedicated Kafka partition. Spark runs **two independent streaming queries** over the same topic, each with its own checkpoint:

**Query 1 — `foreachBatch` (every 5s, ≤ 200 offsets/partition):**

```
CSV Producer ×4
    │  JSON messages (key = node_id, explicit partition 0–3)
    ▼
Kafka cluster (3 brokers)  iot.sensor.data  (4 partitions · replication-factor 3 · min.insync.replicas 2 · 24h retention)
    │  micro-batch every 5s
    ▼
Spark Structured Streaming — process_batch (driver)
    │
    ├─ 1. Write raw JSON → MongoDB  raw_readings              (immutable history, native Spark write)
    │
    │  ── cleaning ──────────────────────────────────────────────────────────────
    │  Filter outliers: CO > 1000 ppm or Gas > 1 MΩ  →  discarded  (cached for reuse)
    │
    │  ── per-node enrichment ON THE WORKERS (repartition by node_id → foreachPartition)
    │  Welford online signed z-score (Temperature, CO, Smoke, Gas)
    │  Absolute thresholds: CO > 50 ppm · Smoke > 0.08 · Temp > 35 °C · Gas < 5 kΩ
    │  Fire transition: flag raised only when Fire crosses 0 → ≥1
    │     ├─ Write enriched rows → MongoDB  processed_readings
    │     ├─ Bulk index          → Elasticsearch  sensors_live_index   (time-series)
    │     ├─ Persist Welford state→ MongoDB  node_stats                (survives restarts)
    │     ├─ Write fire events    → MongoDB  fire_events               (transitions only)
    │     └─ Upsert node status   → Elasticsearch  node_status_index   (1 doc/node + map coords)
    │
    └─ 4. Rolling cumulative stats (groupBy on the driver) → MongoDB  agg_per_nodo
```

**Query 2 — windowed aggregation (`F.window`, tumbling 1 min, watermark 2 min, `outputMode("update")`):**
reads the same topic with a separate checkpoint and writes avg/max/min per `(window, node_id)` → Elasticsearch `window_stats` (idempotent upsert by `node_id_window_start`).

**Distributed compute** — the stateful per-node logic (step 3) runs *on the executors*: `repartition(node_id)` puts one node per partition, so the four nodes are enriched in parallel via `foreachPartition` with no `collect()` back to the driver. Mongo/ES clients are opened lazily inside each worker process to keep the closure cloudpickle-safe.

**Anomaly detection** — two complementary mechanisms run in parallel:

- **Welford online signed z-score** — mean and variance updated incrementally without storing history. State `{count, mean, m2}` is persisted in MongoDB `node_stats` per node and survives container restarts. The z-score is **signed** (positive for spikes above the mean, negative for drops below — e.g. a failing sensor). An anomaly is flagged when `|z| > 2σ` on any of the four sensors.
- **Absolute thresholds** — CO > 50 ppm, Smoke > 0.08 ppm, Temperature > 35 °C, Gas < 5 kΩ. These cover **all four sensors** during the Welford warm-up (when `n=1` the std is 0 and the z-score is forced to 0) and nodes with a structurally elevated baseline.

**Fire transition logic** — a `fire_event` is recorded only when a node transitions from no-fire (Fire = 0) to fire (Fire ≥ 1), not on every row where Fire ≥ 1. The previous fire value (`last_fire_value`) is persisted in MongoDB `node_stats` per node. On each transition an optional webhook alert is fired **asynchronously** (daemon thread) so a slow endpoint never stalls the micro-batch — set `ALERT_WEBHOOK_URL` to enable.

### Lambda Architecture

| Layer | Component | Content |
|---|---|---|
| **Speed — raw** | MongoDB `raw_readings` | Every message before any filtering |
| **Speed — processed** | MongoDB `processed_readings` | Post-filter with z-scores and anomaly flags |
| **Speed — serving (time-series)** | ES `sensors_live_index` | Grafana time-series queries |
| **Speed — serving (live status)** | ES `node_status_index` | Current snapshot per node (Fire / NO FIRE cards + map coords) |
| **Speed — serving (windows)** | ES `window_stats` | Tumbling 1-min aggregates per node |
| **Batch — historical** | MongoDB `node_baseline` (+ ES mirror) | Percentiles, baselines, hourly trends, detection validation, correlations |

The **batch layer** (`spark_job/batch_analytics.py`) is a separate Spark **batch** job — not part of the stream. It re-reads the *entire* `raw_readings` collection and computes heavy, full-history analytics that streaming cannot:

- **Percentiles & baseline** — p50/p95/p99 + avg/min/max per sensor (`percentile_approx`).
- **Hourly trend** — average temperature per hour-of-day, per node.
- **Detection validation vs ground-truth `Fire`** — confusion matrix (TP/FP/TN/FN) and precision / recall / F1 / accuracy per node, validating the physical *fire-oriented* threshold rule (not the generic `is_anomaly`, which also catches non-fire statistical outliers). Node 4 (no `Fire` label) is excluded.
- **Sensor correlations** — Pearson correlation matrix (Spark MLlib `Correlation`), global and per node.

It writes a snapshot to MongoDB `node_baseline` (source of truth, overwrite) and mirrors it to Elasticsearch (`node_baseline_index`, `sensor_correlation_index`) so Grafana — which has no native Mongo datasource — can read it. Run it on demand:

```bash
make batch-now    # one-shot run, ephemeral container, then exits
```

The batch service is **profile-gated** (`--profile batch`): it does *not* start with `make up`. This keeps RAM headroom on an 8 GB machine — no permanently-running batch executor.

### Storage layout

**MongoDB — 6 collections** (`mongo_init/init.js` creates them with indexes & TTLs):

| Collection | Role | TTL |
|---|---|---|
| `raw_readings` | Immutable raw history | 7 days |
| `processed_readings` | Enriched (z-scores, flags) | 3 days |
| `node_stats` | Welford state per node | — |
| `agg_per_nodo` | Rolling cumulative stats | — |
| `fire_events` | Fire-transition events | 30 days |
| `node_baseline` | Batch-layer snapshot | — |

**Elasticsearch — 5 indices** (created lazily with explicit mappings): `sensors_live_index`, `node_status_index`, `window_stats`, `node_baseline_index`, `sensor_correlation_index`.

### Grafana dashboards

Four dashboards and five datasources are provisioned automatically from files:

| Dashboard | Content | Datasource |
|---|---|---|
| **IoT Sensor Dashboard** (home) | Global view: Fire / NO FIRE live cards, Temp/CO/Smoke/Gas per node, anomaly count, fire activity | `sensors_live_index` + `node_status_index` |
| **IoT Node Detail** | Single-node drill-down: all sensors overlaid, z-score trend, anomaly timeline, rolling stats | `node_status_index` |
| **Mappa Incendi** | Geomap of the 4 nodes (fictional coords near the *Sette Fratelli* massif), coloured by status (normal / anomaly / fire) | `node_status_index` |
| **IoT Storico (Batch Layer)** | Historical view from the batch snapshot: percentiles, hourly trends, detection precision/recall/F1, sensor correlations | `node_baseline_index` + `sensor_correlation_index` |

### Spark cluster roles

The same Docker image handles all Spark roles, selected via `SPARK_ROLE`:

| Container | SPARK_ROLE | Role | Profile |
|---|---|---|---|
| `spark-master` | master | Coordinator — accepts jobs, assigns tasks | default |
| `spark-worker-1/2/3` | worker | Executor — 1 core · 512 MB Spark mem (640 MB container limit) each | default |
| `spark-job` | driver | Streaming job — submits & runs `foreachBatch` logic | default |
| `spark-batch` | batch | On-demand historical batch analytics | `batch` |

The driver runs in **client mode**: `foreachBatch`/`foreachPartition` logic executes in the `spark-job` container; workers handle the distributed DataFrame parsing, filtering and per-node enrichment tasks assigned by the master.

### Repository structure

```
.
├── docker-compose.yml          # Full orchestration (19 services, batch profile-gated)
├── Makefile                    # Operational shortcuts (make help)
├── .env.example                # Environment variable template
├── architettura.mmd            # Mermaid architecture diagram
├── csv_producer/
│   ├── csv_producer.py         # Parametric producer (NODE_ID + CSV_PATH + KAFKA_PARTITION)
│   ├── Dockerfile
│   └── requirements.txt
├── spark_job/
│   ├── spark_stream_job.py     # Speed layer — 2 streaming queries (~870 lines)
│   ├── batch_analytics.py      # Batch layer — percentiles, validation, correlations
│   ├── entrypoint.sh           # Dispatches SPARK_ROLE: master | worker | driver | batch
│   ├── submit.sh               # spark-submit (streaming) to the standalone cluster
│   ├── submit_batch.sh         # spark-submit (batch job)
│   ├── batch_loop.sh           # Optional periodic batch loop
│   ├── Dockerfile
│   └── requirements.txt
├── mongo_init/
│   └── init.js                 # Creates 6 collections, indexes and TTLs on first run
└── grafana/
    └── provisioning/           # 5 datasources + 4 auto-provisioned dashboards
```

> **Not included in the repo** (must be provided manually):
> - `.env` — generate from `.env.example`
> - `acquisizioni/` — CSV files from physical sensors

### Memory requirements

| Service | mem_limit |
|---|---|
| Elasticsearch | 1 024 MB |
| Spark workers × 3 | 640 MB × 3 |
| Spark driver | 768 MB |
| Spark batch (on-demand) | 768 MB |
| Spark master | 384 MB |
| Kafka brokers × 3 | 512 MB × 3 |
| MongoDB | 512 MB |
| CSV producers × 4 | 128 MB × 4 |
| Grafana | 256 MB |
| Mongo Express / Kafka UI | 128 / 256 MB |
| **Total (streaming)** | **~7.4 GB** |

Set at least **8 GB** in Docker Desktop → Settings → Resources → Memory (the 3-broker Kafka cluster needs the headroom). Actual runtime usage is lower since `mem_limit` values are ceilings. The batch job is on-demand precisely so its 768 MB don't have to coexist with the full streaming stack permanently.

---

## Full reset

```bash
make reset
# equivalent to:
docker compose down -v
docker compose up -d --build
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `spark-job` exits / restarts | Kafka or ES not healthy yet at startup | `make logs`; the job has `restart: on-failure` and recovers once dependencies are healthy |
| Grafana panels empty | Producers not started, or time range outside ingested window | Confirm `csv-producer-*` are running; set the Grafana time range to **Last 15 minutes** |
| Historical dashboard empty | Batch layer never ran | Run `make batch-now` once raw data has accumulated |
| `Topic iot.sensor.data not present` | `kafka-init` ran before Kafka was healthy | Re-run `docker compose up kafka-init` |
| Two nodes on the same Kafka partition | `KAFKA_PARTITION` not set on a producer | Each producer pins an explicit partition (0–3); a missing value logs a `[WARN]` and falls back to key hashing |
| Stale data after long downtime | Kafka retention is 24 h; older offsets are gone | `failOnDataLoss=false` skips missing offsets silently — for a clean restart use `make reset` |

---

## Demo: Kafka cluster fault tolerance

The topic uses `replication-factor 3` + `min.insync.replicas 2`, so the cluster survives losing one broker. To see it live:

```bash
# inspect replica placement (Replicas / Isr per partition)
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic iot.sensor.data

# kill one broker — leaders are re-elected, the pipeline keeps running
docker compose stop kafka3
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic iot.sensor.data   # Isr drops to 2

# bring it back — replicas re-sync, Isr returns to 3
docker compose start kafka3
```

Elasticsearch document counts keep increasing throughout (`make check`) — no data is lost while a broker is down.

---

## Authors

Repository: <https://github.com/AleFlu/Big_Data>
