import os
import threading
from datetime import datetime, timezone

import requests
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError
from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Configurazione da variabili d'ambiente ────────────────────────────────────
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "iot.sensor.data")
MONGO_URI        = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/sensor_data")
ES_HOST          = os.environ.get("ES_HOST", "elasticsearch")
ES_PORT          = int(os.environ.get("ES_PORT", "9200"))
ES_INDEX         = os.environ.get("ES_INDEX", "sensors_live_index")
ES_STATUS_INDEX   = "node_status_index"
# Index dedicato alle aggregazioni a finestra (query streaming separata e additiva).
# Costante e non sovrascrivibile da env: il pannello Grafana e il mapping ES lo
# referenziano per nome fisso, quindi non deve poter "driftare" via ambiente.
ES_WINDOW_INDEX   = "window_stats"
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")  # vuoto = alerting disabilitato

# Soglie di pulizia identiche al notebook (Cell 19)
CO_MAX  = 1000
GAS_MAX = 1_000_000

# Soglie assolute per anomaly detection (dai dati reali dei CSV):
# nodo_3 fire=1/2 raggiunge CO fino a 993 ppm; baseline normale ~1-5 ppm.
# Smoke baseline ~0.01-0.04; durante eventi fire nodo_2 arriva a 0.18.
# Temperatura: range normale 20-35°C, sopra 40°C segnale di calore anomalo.
# Gas (resistenza Ohm): cala in presenza di gas combusti; baseline ~10k-200k Ohm.
# Una caduta sotto 5k Ohm indica concentrazione anomala di volatili (fumo/gas).
CO_ANOMALY_THRESHOLD    = 50.0     # ppm — >50 indica combustione (baseline <10)
SMOKE_ANOMALY_THRESHOLD = 0.08     # ppm — >0.08 è sopra il doppio del max baseline
TEMP_ANOMALY_THRESHOLD  = 35.0     # °C  — >35 allineato alla soglia orange Grafana
GAS_ANOMALY_THRESHOLD   = 5000.0   # Ohm — <5000 segnala alta concentrazione di volatili

# Soglia z-score oltre la quale un sensore è considerato anomalo (in valore assoluto)
ZSCORE_THRESHOLD = 2.0

# Campi usati per z-score (identici al notebook)
ANOMALY_SENSORS = [
    ("Temperature (C)", "Temperature"),
    ("CO",              "CO"),
    ("Smoke (ppm)",     "Smoke"),
    ("Gas (Ohm)",       "Gas"),
]

# Coordinate geografiche FITTIZIE dei nodi, assegnate a mano nella zona del
# massiccio dei Sette Fratelli (a est di Cagliari). Servono solo a collocare i
# nodi sulla mappa Grafana (pannello Geomap): i sensori reali non hanno GPS.
NODE_COORDS = {
    "nodo_1": (39.290, 9.400),
    "nodo_2": (39.275, 9.420),
    "nodo_3": (39.300, 9.435),
    "nodo_4": (39.265, 9.395),
}

# Rinomina campi per ES (snake_case, compatibili con Kibana KQL / Lens).
# I nomi originali restano invariati in MongoDB raw_readings e nei messaggi Kafka.
FIELD_RENAME_FOR_ES = {
    "Temperature (C)": "temperature_c",
    "Humidity (%)":    "humidity_pct",
    "Pressure (hPA)":  "pressure_hpa",
    "Gas (Ohm)":       "gas_ohm",
    "Visible Light":   "visible_light",
    "IR":              "ir",
    "UV index":        "uv_index",
    "CO":              "co",
    "NO2":             "no2",
    "Smoke (ppm)":     "smoke_ppm",
    "Fire":            "fire",
    "node_id":         "node_id",
    "reading_index":   "reading_index",
    "ingest_ts":       "ingest_ts",
}

# ── Schema Spark (replica esatta delle colonne CSV + campi aggiunti dal producer)
SENSOR_SCHEMA = StructType([
    StructField("Temperature (C)", DoubleType(),  True),
    StructField("Humidity (%)",    DoubleType(),  True),
    StructField("Pressure (hPA)",  DoubleType(),  True),
    StructField("Gas (Ohm)",       DoubleType(),  True),
    StructField("Visible Light",   DoubleType(),  True),
    StructField("IR",              DoubleType(),  True),
    StructField("UV index",        DoubleType(),  True),
    StructField("CO",              DoubleType(),  True),
    StructField("NO2",             DoubleType(),  True),
    StructField("Smoke (ppm)",     DoubleType(),  True),
    StructField("Fire",            IntegerType(), True),   # null per nodo_4
    StructField("node_id",         StringType(),  False),
    StructField("reading_index",   IntegerType(), True),
    StructField("ingest_ts",       StringType(),  True),   # ISO8601 UTC dal producer
])

# ── Connessioni per-processo ──────────────────────────────────────────────────
# Cache dei client in un dict (non variabili globali "nude"): un MongoClient o
# un Elasticsearch contiene un threading.Lock NON serializzabile. Se questi
# oggetti finissero nei globali del modulo, cloudpickle proverebbe a serializzarli
# quando spedisce la closure di foreachPartition ai worker → PicklingError.
# Tenendoli in un dict popolato lazy DENTRO ogni processo (driver o executor),
# ogni JVM/Python worker apre le proprie connessioni e la closure resta pulita.
_CONN: dict = {}


def get_es_client() -> Elasticsearch:
    """Client ES per il processo corrente; crea gli indici al primo accesso."""
    es = _CONN.get("es")
    if es is None:
        es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
        ensure_es_index(es)
        ensure_node_status_index(es)
        _CONN["es"] = es
    return es


def get_mongo_db():
    """MongoClient per il processo corrente; restituisce il database sensor_data."""
    client = _CONN.get("mongo")
    if client is None:
        client = MongoClient(MONGO_URI)
        _CONN["mongo"] = client
    return client["sensor_data"]


def _safe_create_index(es_client: Elasticsearch, index: str, body: dict) -> None:
    """Crea un indice ES; ignora l'errore se esiste già (TOCTOU-safe)."""
    try:
        es_client.indices.create(index=index, body=body)
        print(f"Indice ES '{index}' creato.")
    except RequestError as e:
        if e.error != "resource_already_exists_exception":
            raise


def ensure_es_index(es_client: Elasticsearch) -> None:
    """Crea sensors_live_index con mapping esplicito."""
    if es_client.indices.exists(index=ES_INDEX):
        return
    mapping = {
        "settings": {"index": {"number_of_replicas": 0, "refresh_interval": "1s"}},
        "mappings": {
            "properties": {
                "node_id":            {"type": "keyword"},
                "reading_index":      {"type": "integer"},
                "ingest_ts":          {"type": "date", "format": "strict_date_optional_time"},
                "temperature_c":      {"type": "float"},
                "humidity_pct":       {"type": "float"},
                "pressure_hpa":       {"type": "float"},
                "gas_ohm":            {"type": "float"},
                "visible_light":      {"type": "float"},
                "ir":                 {"type": "float"},
                "uv_index":           {"type": "float"},
                "co":                 {"type": "float"},
                "no2":                {"type": "float"},
                "smoke_ppm":          {"type": "float"},
                "fire":               {"type": "integer"},
                "is_fire":            {"type": "boolean"},
                "is_fire_transition": {"type": "boolean"},
                "fire_state_label":   {"type": "keyword"},
                "zscore_Temperature": {"type": "float"},
                "zscore_CO":          {"type": "float"},
                "zscore_Smoke":       {"type": "float"},
                "zscore_Gas":         {"type": "float"},
                "is_anomaly":         {"type": "boolean"},
                "anomaly_sensors":    {"type": "keyword"},
            }
        },
    }
    _safe_create_index(es_client, ES_INDEX, mapping)


def ensure_node_status_index(es_client: Elasticsearch) -> None:
    """Crea node_status_index — un documento per nodo, aggiornato ogni batch."""
    if es_client.indices.exists(index=ES_STATUS_INDEX):
        return
    mapping = {
        "settings": {"index": {"number_of_replicas": 0, "refresh_interval": "5s"}},
        "mappings": {
            "properties": {
                "node_id":               {"type": "keyword"},
                "last_ingest_ts":        {"type": "date", "format": "strict_date_optional_time"},
                "temperature_c":         {"type": "float"},
                "humidity_pct":          {"type": "float"},
                "pressure_hpa":          {"type": "float"},
                "gas_ohm":               {"type": "float"},
                "co":                    {"type": "float"},
                "smoke_ppm":             {"type": "float"},
                "fire":                  {"type": "integer"},
                "is_fire":               {"type": "boolean"},
                "fire_state_label":      {"type": "keyword"},
                "is_anomaly_current":    {"type": "boolean"},
                "anomaly_sensors":       {"type": "keyword"},
                "zscore_Temperature":    {"type": "float"},
                "zscore_CO":             {"type": "float"},
                "zscore_Smoke":          {"type": "float"},
                "zscore_Gas":            {"type": "float"},
                "running_min_temp":      {"type": "float"},
                "running_max_temp":      {"type": "float"},
                "running_min_co":        {"type": "float"},
                "running_max_co":        {"type": "float"},
                "running_min_smoke":     {"type": "float"},
                "running_max_smoke":     {"type": "float"},
                "running_min_gas":       {"type": "float"},
                "running_max_gas":       {"type": "float"},
                "total_processed":       {"type": "long"},
                "last_update_ts":        {"type": "date", "format": "strict_date_optional_time"},
                "lat":                   {"type": "float"},
                "lon":                   {"type": "float"},
                "map_status":            {"type": "integer"},
            }
        },
    }
    _safe_create_index(es_client, ES_STATUS_INDEX, mapping)


def ensure_window_stats_index(es_client: Elasticsearch) -> None:
    """
    Crea window_stats — un documento per (finestra temporale, nodo).

    Lo alimenta la query streaming a finestra (tumbling 1 min, watermark 2 min):
    una riga per ogni coppia (window_start, node_id) con avg/max/min dei sensori.
    Mapping esplicito come per gli altri indici: senza di esso ES indovinerebbe i
    tipi dal primo documento (es. float→long se il primo avg è intero), rompendo
    poi le aggregazioni Grafana. window_start/window_end sono date strict così
    Grafana può usarli come timeField nella date histogram.
    """
    if es_client.indices.exists(index=ES_WINDOW_INDEX):
        return
    mapping = {
        "settings": {"index": {"number_of_replicas": 0, "refresh_interval": "5s"}},
        "mappings": {
            "properties": {
                "node_id":       {"type": "keyword"},
                "window_start":  {"type": "date", "format": "strict_date_optional_time"},
                "window_end":    {"type": "date", "format": "strict_date_optional_time"},
                "avg_temp":      {"type": "float"},
                "max_temp":      {"type": "float"},
                "avg_co":        {"type": "float"},
                "max_co":        {"type": "float"},
                "avg_smoke":     {"type": "float"},
                "max_smoke":     {"type": "float"},
                "avg_gas":       {"type": "float"},
                "min_gas":       {"type": "float"},
                "reading_count": {"type": "long"},
            }
        },
    }
    _safe_create_index(es_client, ES_WINDOW_INDEX, mapping)


def _fire_state_label(fire_val) -> str:
    """Mappa il valore Fire intero a una label leggibile."""
    if fire_val is None:
        return "N/A"
    if fire_val == 0:
        return "NORMAL"
    if fire_val == 1:
        return "FIRE"
    return "SPECIAL"     # fire_val == 2 (nodo_3)


def _send_fire_alerts_async(fire_docs: list[dict]) -> None:
    """
    Invia gli alert webhook in un thread daemon separato — best-effort.
    Non deve mai bloccare il micro-batch Spark: un endpoint lento o irraggiungibile
    farebbe sforare il trigger di 5s e accumulare ritardo nello streaming.
    """
    if not ALERT_WEBHOOK_URL or not fire_docs:
        return

    def _worker(docs: list[dict]) -> None:
        for d in docs:
            try:
                payload = {
                    "node_id":         d["node_id"],
                    "fire_value":      d["fire_value"],
                    "fire_value_prev": d["fire_value_prev"],
                    "ingest_ts":       d["ingest_ts"].isoformat(),
                    "temperature_c":   d.get("temperature_c"),
                    "co":              d.get("co"),
                    "smoke_ppm":       d.get("smoke_ppm"),
                }
                requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
            except Exception as exc:
                print(f"[WARN] webhook alert fallito per {d['node_id']}: {exc}")

    threading.Thread(target=_worker, args=(fire_docs,), daemon=True).start()


def welford_update(stats: dict, short: str, val: float) -> tuple[float, float, float]:
    """
    Aggiorna le statistiche online (algoritmo Welford).
    Usa varianza campionaria (m2/(n-1)) per z-score statisticamente corretto.
    """
    n    = stats.get(f"{short}_count", 0) + 1
    mean = stats.get(f"{short}_mean",  0.0)
    m2   = stats.get(f"{short}_m2",    0.0)
    delta  = val - mean
    mean   = mean + delta / n
    delta2 = val - mean
    m2     = m2 + delta * delta2
    return n, mean, m2


def _enrich_and_persist_node(node_id: str, rows: list, db, es) -> dict:
    """
    Logica per-nodo eseguita SUI WORKER (una partizione = un nodo).

    Riceve tutte le righe (ordinate per reading_index) di un singolo nodo,
    applica Welford + z-score + arricchimento fire e scrive i sink per-nodo:
    processed_readings, sensors_live_index, node_stats, fire_events,
    node_status_index. Restituisce un piccolo dict di conteggi per il log.

    Perché qui e non sul driver: lo stato Welford è seriale PER NODO, e con
    1 partizione = 1 nodo questa funzione è completamente isolata. Nodi diversi
    girano in parallelo su worker diversi → niente collect() sul driver.
    """
    # Stato Welford del nodo dal batch precedente (o vuoto al primo avvio)
    stats = db.node_stats.find_one({"node_id": node_id}) or {"node_id": node_id}
    stats.pop("_id", None)

    enriched_rows = []
    for row in rows:
        row_dict = row.asDict()

        for col_name, short in ANOMALY_SENSORS:
            val = row_dict.get(col_name)
            if val is None:
                row_dict[f"zscore_{short}"] = None
                continue
            n, mean, m2 = welford_update(stats, short, val)
            stats[f"{short}_count"] = n
            stats[f"{short}_mean"]  = mean
            stats[f"{short}_m2"]    = m2
            std    = (m2 / (n - 1)) ** 0.5 if n > 1 else 0.0
            # Z-score firmato: positivo per picchi sopra la media, negativo sotto.
            # Permette di distinguere un'impennata da un crollo (es. guasto sensore)
            # nei pannelli Grafana. La detection usa il valore assoluto (vedi sotto).
            zscore = round((val - mean) / std, 3) if std > 0 else 0.0
            row_dict[f"zscore_{short}"] = zscore

        # Anomalia z-score: scatta sul valore assoluto, in entrambe le direzioni.
        anomaly_flags = [
            short for _, short in ANOMALY_SENSORS
            if abs(row_dict.get(f"zscore_{short}") or 0.0) > ZSCORE_THRESHOLD
        ]
        # Soglie assolute: scattano indipendentemente dallo z-score.
        # Utili nelle prime letture (Welford ha poca storia, std=0 → z-score=0) e
        # quando tutti i valori sono alti (z-score basso ma valore pericoloso).
        # Coprono tutti e 4 i sensori per non lasciare scoperto il warm-up.
        # Usano gli stessi nomi dei flag z-score per coerenza nei filtri Grafana.
        co_val    = row_dict.get("CO")
        smoke_val = row_dict.get("Smoke (ppm)")
        temp_val  = row_dict.get("Temperature (C)")
        gas_val   = row_dict.get("Gas (Ohm)")
        if co_val    is not None and co_val    > CO_ANOMALY_THRESHOLD    and "CO"          not in anomaly_flags:
            anomaly_flags.append("CO")
        if smoke_val is not None and smoke_val > SMOKE_ANOMALY_THRESHOLD and "Smoke"       not in anomaly_flags:
            anomaly_flags.append("Smoke")
        if temp_val  is not None and temp_val  > TEMP_ANOMALY_THRESHOLD  and "Temperature" not in anomaly_flags:
            anomaly_flags.append("Temperature")
        if gas_val   is not None and gas_val   < GAS_ANOMALY_THRESHOLD   and "Gas"         not in anomaly_flags:
            anomaly_flags.append("Gas")

        row_dict["is_anomaly"]      = len(anomaly_flags) > 0
        row_dict["anomaly_sensors"] = ", ".join(anomaly_flags)

        # ── Arricchimento fire ────────────────────────────────────────────────
        fire_val  = row_dict.get("Fire")
        last_fire = stats.get("last_fire_value")   # None = nodo mai visto prima
        # Transizione: nodo passa da no-fire (None o 0) a fire (>=1)
        is_transition = bool(
            fire_val is not None
            and fire_val >= 1
            and (last_fire is None or last_fire == 0)
        )
        row_dict["is_fire"]            = bool(fire_val is not None and fire_val >= 1)
        row_dict["is_fire_transition"] = is_transition
        row_dict["fire_value_prev"]    = int(last_fire) if last_fire is not None else 0
        row_dict["fire_state_label"]   = _fire_state_label(fire_val)
        # Aggiorna last_fire_value per le righe successive dello stesso nodo
        if fire_val is not None:
            stats["last_fire_value"] = fire_val

        enriched_rows.append(row_dict)

    if not enriched_rows:
        return {"node_id": node_id, "count": 0, "fire": 0, "anomaly": 0}

    # ── Persisti stato Welford aggiornato (upsert atomico per node_id) ─────────
    db.node_stats.replace_one({"node_id": node_id}, stats, upsert=True)

    # ── processed_readings (batch layer — dati arricchiti) ─────────────────────
    processed_docs = []
    for row_dict in enriched_rows:
        doc = {k: v for k, v in row_dict.items()
               if not (isinstance(v, float) and v != v)}  # scarta NaN
        if doc.get("Fire") is not None:
            doc["Fire"] = int(doc["Fire"])
        doc["is_anomaly"] = bool(doc.get("is_anomaly", False))
        doc["is_fire"]    = bool(doc.get("is_fire", False))
        processed_docs.append(doc)
    db.processed_readings.bulk_write([
        ReplaceOne({"node_id": d["node_id"], "reading_index": d["reading_index"]},
                   d, upsert=True)
        for d in processed_docs
    ], ordered=False)

    # ── sensors_live_index (serving layer — time-series) ───────────────────────
    actions = []
    for row_dict in enriched_rows:
        doc = {}
        for k, v in row_dict.items():
            if isinstance(v, float) and v != v:   # scarta NaN
                continue
            doc[FIELD_RENAME_FOR_ES.get(k, k)] = v
        if "fire" in doc and doc["fire"] is not None:
            doc["fire"] = int(doc["fire"])
        doc["is_anomaly"] = bool(doc.get("is_anomaly", False))
        doc["is_fire"]    = bool(doc.get("is_fire", False))
        actions.append({
            "_index": ES_INDEX,
            "_id":    f"{doc.get('node_id', '')}_{doc.get('reading_index', '')}",
            "_source": doc,
        })
    _, errors = helpers.bulk(es, actions, chunk_size=500,
                             raise_on_error=False, raise_on_exception=True)
    if errors:
        print(f"[WARN] sensors_live bulk ({node_id}): {len(errors)} falliti")

    # ── fire_events su MongoDB (solo transizioni no-fire → fire) ───────────────
    fire_rows = [r for r in enriched_rows if r.get("is_fire_transition")]
    if fire_rows:
        fire_docs = [{
            "node_id":         rd["node_id"],
            "reading_index":   rd.get("reading_index"),
            "fire_value":      int(rd["Fire"]),
            "fire_value_prev": rd.get("fire_value_prev", 0),
            # ingest_ts è nullable nello schema: fallback a now() se il producer
            # non l'ha valorizzato, per non far crashare il micro-batch.
            "ingest_ts":       (datetime.fromisoformat(rd["ingest_ts"].replace("Z", "+00:00"))
                                if rd.get("ingest_ts") else datetime.now(timezone.utc)),
            "temperature_c":   rd.get("Temperature (C)"),
            "co":              rd.get("CO"),
            "smoke_ppm":       rd.get("Smoke (ppm)"),
        } for rd in fire_rows]
        db.fire_events.bulk_write([
            ReplaceOne({"node_id": d["node_id"], "reading_index": d.get("reading_index")},
                       d, upsert=True)
            for d in fire_docs
        ], ordered=False)
        # Alert webhook best-effort, fuori dal path critico del micro-batch.
        _send_fire_alerts_async(fire_docs)

    # ── node_status_index: ultima riga del nodo (max reading_index) ────────────
    # I running_min/max sono già su agg_per_nodo (aggiornato dal driver, step 9
    # del batch PRECEDENTE). Li leggiamo qui per riportarli nel doc di stato.
    rd = max(enriched_rows, key=lambda r: r.get("reading_index", 0))
    cum = db.agg_per_nodo.find_one(
        {"node_id": node_id},
        {"running_min_temp": 1, "running_max_temp": 1, "running_min_co": 1,
         "running_max_co": 1, "running_min_smoke": 1, "running_max_smoke": 1,
         "running_min_gas": 1, "running_max_gas": 1, "total_processed": 1, "_id": 0}
    ) or {}
    fire_val = rd.get("Fire")
    # Coordinate fittizie + livello di stato (0 normale / 1 anomalia / 2 incendio)
    # per il pannello mappa Grafana (Geomap). map_status riusa i flag già calcolati:
    # per nodo_4 (Fire null) is_fire è False, quindi riflette l'anomalia.
    _lat, _lon = NODE_COORDS.get(node_id, (None, None))
    _map_status = (2 if (fire_val is not None and fire_val >= 1)
                   else 1 if rd.get("is_anomaly") else 0)
    status_doc = {
        "node_id":            node_id,
        "last_ingest_ts":     rd.get("ingest_ts"),
        "temperature_c":      rd.get("Temperature (C)"),
        "humidity_pct":       rd.get("Humidity (%)"),
        "pressure_hpa":       rd.get("Pressure (hPA)"),
        "gas_ohm":            rd.get("Gas (Ohm)"),
        "co":                 rd.get("CO"),
        "smoke_ppm":          rd.get("Smoke (ppm)"),
        "fire":               int(fire_val) if fire_val is not None else None,
        "is_fire":            bool(fire_val is not None and fire_val >= 1),
        "fire_state_label":   _fire_state_label(fire_val),
        "is_anomaly_current": bool(rd.get("is_anomaly", False)),
        "anomaly_sensors":    rd.get("anomaly_sensors", ""),
        "zscore_Temperature": rd.get("zscore_Temperature"),
        "zscore_CO":          rd.get("zscore_CO"),
        "zscore_Smoke":       rd.get("zscore_Smoke"),
        "zscore_Gas":         rd.get("zscore_Gas"),
        "running_min_temp":   cum.get("running_min_temp"),
        "running_max_temp":   cum.get("running_max_temp"),
        "running_min_co":     cum.get("running_min_co"),
        "running_max_co":     cum.get("running_max_co"),
        "running_min_smoke":  cum.get("running_min_smoke"),
        "running_max_smoke":  cum.get("running_max_smoke"),
        "running_min_gas":    cum.get("running_min_gas"),
        "running_max_gas":    cum.get("running_max_gas"),
        "total_processed":    cum.get("total_processed"),
        "last_update_ts":     datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "lat":                _lat,
        "lon":                _lon,
        "map_status":         _map_status,
    }
    # helpers.bulk (non es.index): API stabile su elasticsearch-py 7.17 e coerente
    # con le altre scritture ES. _id = node_id → un solo documento per nodo (upsert).
    helpers.bulk(es, [{"_index": ES_STATUS_INDEX, "_id": node_id, "_source": status_doc}],
                 raise_on_error=False, raise_on_exception=True)

    return {
        "node_id": node_id,
        "count":   len(enriched_rows),
        "fire":    sum(1 for r in enriched_rows if r.get("is_fire")),
        "anomaly": sum(1 for r in enriched_rows if r["is_anomaly"]),
    }


def process_partition(rows_iter):
    """
    Callback foreachPartition — gira SUL WORKER, una volta per partizione.

    Con il partizionamento Kafka 1 nodo = 1 partizione, ogni partizione contiene
    le righe di un solo nodo. Le raggruppa per node_id (difensivo: se più nodi
    finissero nella stessa partizione, restano comunque corretti), ordina per
    reading_index — Spark NON garantisce l'ordine dopo il filter, ma Welford lo
    richiede — e delega a _enrich_and_persist_node. Le connessioni Mongo/ES sono
    aperte qui, una per partizione (non riusabili dal driver).
    """
    by_node: dict[str, list] = {}
    for row in rows_iter:
        by_node.setdefault(row["node_id"], []).append(row)
    if not by_node:
        return

    db = get_mongo_db()
    es = get_es_client()
    for node_id, rows in by_node.items():
        # Welford è seriale: serve l'ordine di lettura corretto dentro la partizione
        rows.sort(key=lambda r: r["reading_index"] if r["reading_index"] is not None else 0)
        _enrich_and_persist_node(node_id, rows, db, es)


def process_batch(batch_df, batch_id: int) -> None:
    """
    Callback foreachBatch — orchestrazione del micro-batch:
      1. Scrivi raw su MongoDB raw_readings (nativo Spark, distribuito)
      2. Filtra outlier (CO > 1000 o Gas > 1M) (nativo Spark, distribuito)
      3. Arricchimento per-nodo SUI WORKER via foreachPartition:
         z-score Welford, flag fire/anomalia, scrittura di processed_readings,
         sensors_live_index, node_stats, fire_events, node_status_index
      4. Rolling stats cumulative su MongoDB agg_per_nodo (groupBy nativo + driver)

    La logica stateful per-nodo (step 3) è distribuita sugli executor: con
    1 partizione = 1 nodo i nodi vengono elaborati in parallelo. Sul driver
    restano solo le operazioni che richiedono una vista globale o sono già
    espresse come trasformazioni native Spark.
    """
    if batch_df.isEmpty():
        return

    # ── 1. Raw write su MongoDB (nativo Spark, gira sui worker) ────────────────
    (batch_df.write
     .format("mongodb")
     .option("connection.uri", MONGO_URI)
     .option("collection", "raw_readings")
     .option("idFieldList", "node_id,reading_index")
     .option("operationType", "replace")
     .mode("append")
     .save())

    # ── 2. Filtro outlier (nativo Spark, lazy, gira sui worker) ────────────────
    # cache(): batch_clean è usato due volte (foreachPartition allo step 3 e
    # groupBy allo step 4). Senza cache Spark ricalcolerebbe filtro+parsing due
    # volte; con cache il batch filtrato è materializzato una sola volta.
    batch_clean = batch_df.filter(
        (F.col("CO") < CO_MAX) &
        (F.col("Gas (Ohm)") < GAS_MAX)
    ).cache()

    # ── 3. Arricchimento per-nodo SUI WORKER ──────────────────────────────────
    # Ripartiziona per node_id così ogni partizione contiene un solo nodo e
    # gli executor lavorano in parallelo. foreachPartition non fa collect():
    # i dati arricchiti non tornano mai al driver.
    # numPartitions=4 esplicito (= numero di nodi): senza, repartition userebbe
    # spark.sql.shuffle.partitions (16) creando ~12 task vuoti ogni micro-batch.
    # Eventuali collisioni hash (2 nodi nella stessa partizione) sono già gestite
    # dal raggruppamento difensivo per node_id dentro process_partition.
    (batch_clean
     .repartition(4, F.col("node_id"))
     .foreachPartition(process_partition))

    # ── 4. Rolling stats cumulative su MongoDB agg_per_nodo ────────────────────
    # groupBy nativo Spark (distribuito) + update atomici sul driver. Resta sul
    # driver perché aggrega l'intero batch e alimenta i running_min/max che lo
    # step 3 del batch SUCCESSIVO leggerà per il node_status_index.
    # Client Mongo LOCALE (non get_mongo_db()): tenere il client fuori dai globali
    # del modulo evita che cloudpickle lo catturi quando serializza la closure di
    # foreachPartition al batch successivo (un MongoClient contiene un thread.lock).
    db_client = MongoClient(MONGO_URI)
    db = db_client["sensor_data"]
    batch_agg_map: dict[str, dict] = {}
    for agg_row in (batch_clean
                    .groupBy("node_id")
                    .agg(
                        F.count("*").alias("batch_count"),
                        F.round(F.avg(F.col("Temperature (C)")),  2).alias("batch_avg_temp"),
                        F.round(F.min(F.col("Temperature (C)")),  2).alias("batch_min_temp"),
                        F.round(F.max(F.col("Temperature (C)")),  2).alias("batch_max_temp"),
                        F.round(F.sum(F.col("Temperature (C)")),  4).alias("batch_sum_temp"),
                        F.round(F.avg(F.col("CO")),                2).alias("batch_avg_co"),
                        F.round(F.min(F.col("CO")),                2).alias("batch_min_co"),
                        F.round(F.max(F.col("CO")),                2).alias("batch_max_co"),
                        F.round(F.sum(F.col("CO")),                4).alias("batch_sum_co"),
                        F.round(F.avg(F.col("Smoke (ppm)")),       4).alias("batch_avg_smoke"),
                        F.round(F.min(F.col("Smoke (ppm)")),       4).alias("batch_min_smoke"),
                        F.round(F.max(F.col("Smoke (ppm)")),       4).alias("batch_max_smoke"),
                        F.round(F.sum(F.col("Smoke (ppm)")),       4).alias("batch_sum_smoke"),
                        F.round(F.avg(F.col("Gas (Ohm)")),         2).alias("batch_avg_gas"),
                        F.round(F.min(F.col("Gas (Ohm)")),         2).alias("batch_min_gas"),
                        F.round(F.max(F.col("Gas (Ohm)")),         2).alias("batch_max_gas"),
                        F.round(F.sum(F.col("Gas (Ohm)")),         2).alias("batch_sum_gas"),
                    )
                    .collect()):
        batch_agg_map[agg_row["node_id"]] = agg_row.asDict()

    # I running_min/max/sum cumulativi vengono aggiornati qui sul driver con
    # operatori atomici. Il node_status_index NON viene scritto qui: lo scrive
    # ogni worker (step 3) leggendo questi running_* — che riflettono i batch
    # PRECEDENTI. C'è quindi un ritardo di un micro-batch (5 s) sui min/max
    # mostrati nello stato corrente: accettabile per un cruscotto live.
    for agg in batch_agg_map.values():
        db.agg_per_nodo.update_one(
            {"node_id": agg["node_id"]},
            {
                # Contatori e somme cumulativi (atomic)
                "$inc": {
                    "total_processed":  agg["batch_count"],
                    "running_sum_temp": float(agg["batch_sum_temp"] or 0),
                    "running_sum_co":   float(agg["batch_sum_co"]   or 0),
                    "running_sum_smoke":float(agg["batch_sum_smoke"] or 0),
                    "running_sum_gas":  float(agg["batch_sum_gas"]   or 0),
                },
                # Min/max cumulativi — $min/$max sono atomici su MongoDB 7.0
                # Guard None: $min/$max con null resetta il valore su MongoDB 7.0
                "$min": {k: v for k, v in {
                    "running_min_temp":  agg["batch_min_temp"],
                    "running_min_co":    agg["batch_min_co"],
                    "running_min_smoke": agg["batch_min_smoke"],
                    "running_min_gas":   agg["batch_min_gas"],
                }.items() if v is not None},
                "$max": {k: v for k, v in {
                    "running_max_temp":  agg["batch_max_temp"],
                    "running_max_co":    agg["batch_max_co"],
                    "running_max_smoke": agg["batch_max_smoke"],
                    "running_max_gas":   agg["batch_max_gas"],
                }.items() if v is not None},
                # Ultimi valori del batch per riferimento rapido
                "$set": {
                    "last_batch_avg_temp":  agg["batch_avg_temp"],
                    "last_batch_avg_co":    agg["batch_avg_co"],
                    "last_batch_avg_smoke": agg["batch_avg_smoke"],
                    "last_batch_avg_gas":   agg["batch_avg_gas"],
                    "last_update":          datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )

    # Nota: i conteggi fire/anomalie sono calcolati sui worker (step 3) e non
    # tornano al driver (niente collect()). Qui logghiamo il volume del batch
    # aggregato dalla groupBy; il dettaglio per nodo è nei log degli executor.
    batch_total = sum(int(a.get("batch_count") or 0) for a in batch_agg_map.values())
    print(
        f"Batch {batch_id}: {batch_total} record su {len(batch_agg_map)} nodi "
        f"→ arricchimento distribuito sui worker → ES + MongoDB"
    )

    # Libera la cache del batch filtrato: senza unpersist le copie cache si
    # accumulerebbero in memoria executor tra un micro-batch e l'altro.
    batch_clean.unpersist()

    # Chiudi il client Mongo locale del driver (aperto per lo step 4).
    db_client.close()


# ── Query a finestra (streaming aggregation) ──────────────────────────────────
# Pipeline SEPARATA e ADDITIVA: legge lo stesso topic Kafka in modo indipendente
# (proprio reader, proprio checkpoint) e scrive solo su window_stats. Non tocca
# in alcun modo la query principale né i suoi sink. Tutto ciò che segue serve
# esclusivamente a questa seconda query.


# Client ES della query a finestra, vivo SOLO sul driver (process_window_batch è
# un foreachBatch, gira sul driver). DEVE stare in una variabile DEDICATA e NON
# in _CONN: _CONN è referenziato da get_es_client()/get_mongo_db(), quindi da
# process_partition, quindi finisce nel grafo di cattura di cloudpickle quando la
# pipeline PRINCIPALE serializza process_partition verso i worker. Se il client ES
# (che contiene un socket) fosse in _CONN, quella serializzazione fallirebbe con
# "cannot pickle 'socket' object" — ed è esattamente l'interazione che va evitata
# tra le due query. Tenendolo qui, fuori da _CONN, process_partition non lo vede.
_DRIVER_ES_WINDOW: Elasticsearch | None = None


def get_window_es_client() -> Elasticsearch:
    """Client ES per la query a finestra — driver-only, lazy, fuori da _CONN."""
    global _DRIVER_ES_WINDOW
    if _DRIVER_ES_WINDOW is None:
        es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
        ensure_window_stats_index(es)
        _DRIVER_ES_WINDOW = es
    return _DRIVER_ES_WINDOW


def process_window_batch(batch_df, batch_id: int) -> None:
    """
    Callback foreachBatch della query a finestra: scrive su window_stats con upsert.

    outputMode("update") riemette una finestra ad ogni micro-batch finché il
    watermark non la chiude: lo stesso (window_start, node_id) può quindi arrivare
    più volte con conteggi/aggregati aggiornati. Usando _id = f"{node_id}_{window_start}"
    ogni emissione SOVRASCRIVE il documento della finestra (upsert idempotente),
    così l'indice riflette sempre l'ultimo valore consolidato della finestra e non
    accumula duplicati.
    """
    if batch_df.isEmpty():
        return

    es = get_window_es_client()
    actions = []
    for row in batch_df.collect():
        rd = row.asDict()
        # Le colonne window_start/window_end sono Timestamp Spark: le serializziamo
        # in ISO8601 UTC con suffisso "Z" per allinearle al format strict_date_*
        # del mapping (e a come il producer scrive ingest_ts).
        ws = rd["window_start"]
        we = rd["window_end"]
        ws_iso = ws.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if ws is not None else None
        we_iso = we.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if we is not None else None
        doc = {
            "node_id":       rd.get("node_id"),
            "window_start":  ws_iso,
            "window_end":    we_iso,
            "avg_temp":      rd.get("avg_temp"),
            "max_temp":      rd.get("max_temp"),
            "avg_co":        rd.get("avg_co"),
            "max_co":        rd.get("max_co"),
            "avg_smoke":     rd.get("avg_smoke"),
            "max_smoke":     rd.get("max_smoke"),
            "avg_gas":       rd.get("avg_gas"),
            "min_gas":       rd.get("min_gas"),
            "reading_count": rd.get("reading_count"),
        }
        actions.append({
            "_index": ES_WINDOW_INDEX,
            "_id":    f"{doc['node_id']}_{ws_iso}",
            "_source": doc,
        })

    if actions:
        _, errors = helpers.bulk(es, actions, chunk_size=500,
                                 raise_on_error=False, raise_on_exception=True)
        if errors:
            print(f"[WARN] window_stats bulk (batch {batch_id}): {len(errors)} falliti")
        print(f"Window batch {batch_id}: {len(actions)} finestre → ES:{ES_WINDOW_INDEX}")


def main():
    spark = (SparkSession.builder
             .appName("IoT-Sensor-Streaming")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    df_raw = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", KAFKA_TOPIC)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", 200)
              .load())

    df_parsed = (df_raw
                 .select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
                 .select("d.*"))

    query = (df_parsed.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", "/spark/checkpoints/iot_stream")
             .trigger(processingTime="5 seconds")
             .outputMode("append")
             .start())

    print(f"Streaming query avviata. Topic: {KAFKA_TOPIC} → ES:{ES_INDEX} + {ES_STATUS_INDEX} + MongoDB")

    # ── Seconda query: aggregazione a finestra (additiva, indipendente) ───────
    # Riusa lo stesso df_parsed (stesso reader Kafka logico) ma con il PROPRIO
    # checkpoint /spark/checkpoints/iot_window: Spark materializza un secondo
    # stream con offset tracciati separatamente, quindi le due query consumano il
    # topic in modo indipendente e non si influenzano a vicenda.
    #
    # Event-time: ingest_ts è una STRINGA ISO8601 nello schema → la convertiamo a
    # timestamp con F.to_timestamp() per poter applicare watermark e F.window().
    # Le righe con ingest_ts nullo verrebbero escluse dall'aggregazione a finestra
    # (event_ts null), il che è corretto: senza event-time non collocabili in una
    # finestra.
    df_window_src = df_parsed.withColumn("event_ts", F.to_timestamp("ingest_ts"))

    # Tumbling window di 1 minuto (solo durata, nessuno slide → finestre disgiunte)
    # con watermark di 2 minuti: dati più vecchi di (max_event_ts - 2 min) sono
    # scartati e le finestre già chiuse non vengono più aggiornate, limitando lo
    # stato mantenuto in memoria. outputMode("update") riemette solo le finestre
    # cambiate nel micro-batch (vedi process_window_batch per l'upsert idempotente).
    df_windowed = (df_window_src
                   .withWatermark("event_ts", "2 minutes")
                   .groupBy(
                       F.window("event_ts", "1 minute"),
                       F.col("node_id"),
                   )
                   .agg(
                       F.round(F.avg(F.col("Temperature (C)")), 2).alias("avg_temp"),
                       F.round(F.max(F.col("Temperature (C)")), 2).alias("max_temp"),
                       F.round(F.avg(F.col("CO")),              2).alias("avg_co"),
                       F.round(F.max(F.col("CO")),              2).alias("max_co"),
                       F.round(F.avg(F.col("Smoke (ppm)")),     4).alias("avg_smoke"),
                       F.round(F.max(F.col("Smoke (ppm)")),     4).alias("max_smoke"),
                       F.round(F.avg(F.col("Gas (Ohm)")),       2).alias("avg_gas"),
                       F.round(F.min(F.col("Gas (Ohm)")),       2).alias("min_gas"),
                       F.count(F.lit(1)).alias("reading_count"),
                   )
                   # "esplode" la struct window in due colonne scalari, più comode
                   # da serializzare nel sink (vedi process_window_batch).
                   .select(
                       F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       F.col("node_id"),
                       "avg_temp", "max_temp",
                       "avg_co", "max_co",
                       "avg_smoke", "max_smoke",
                       "avg_gas", "min_gas",
                       "reading_count",
                   ))

    window_query = (df_windowed.writeStream
                    .foreachBatch(process_window_batch)
                    .option("checkpointLocation", "/spark/checkpoints/iot_window")
                    .trigger(processingTime="5 seconds")
                    .outputMode("update")
                    .start())

    print(f"Window query avviata. Topic: {KAFKA_TOPIC} → ES:{ES_WINDOW_INDEX} "
          f"(tumbling 1m, watermark 2m, outputMode update)")

    # awaitAnyTermination invece di query.awaitTermination(): con due query attive
    # vogliamo che il job termini (e si riavvii) se UNA QUALSIASI delle due cade,
    # invece di restare appeso sulla sola query principale ignorando un crash della
    # window query. Entrambe sono già state .start()-ate prima di questa chiamata.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
