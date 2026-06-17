"""
BATCH LAYER STORICO — Lambda Architecture.

Job Spark batch (NON streaming) FISICAMENTE separato da spark_stream_job.py.
Rilegge l'INTERA collection MongoDB raw_readings, calcola statistiche pesanti
per nodo (percentili, baseline, trend orari, copertura) e scrive uno SNAPSHOT
completo (overwrite) nella collection MongoDB node_baseline.

Perché un job separato e non parte dello streaming:
  - lo streaming è stateful per micro-batch (Welford incrementale): non può
    calcolare un percentile globale, che richiede di vedere TUTTI i dati insieme;
  - i percentili sono pesanti (shuffle/sort sull'intero storico): vanno fatti
    a freddo, ogni 10 minuti, non a ogni micro-batch da 5s;
  - mantenere i due path separati è il cuore della Lambda Architecture (speed
    layer = streaming, batch layer = questo job).

READ-ONLY su raw_readings. Scrive SOLO su:
  - MongoDB node_baseline (source of truth dello snapshot batch);
  - Elasticsearch node_baseline_index (mirror per Grafana — ES è l'unico
    datasource che Grafana ha; Mongo non ha datasource Grafana nativo).
NON tocca processed_readings, agg_per_nodo, node_stats, fire_events.
"""

import os
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Configurazione da variabili d'ambiente (stessi default dello streaming) ────
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/sensor_data")
ES_HOST   = os.environ.get("ES_HOST", "elasticsearch")
ES_PORT   = int(os.environ.get("ES_PORT", "9200"))
# Collection MongoDB di output (snapshot completo, overwrite a ogni run).
BASELINE_COLLECTION = os.environ.get("BASELINE_COLLECTION", "node_baseline")
# Index ES mirror per Grafana.
ES_BASELINE_INDEX = os.environ.get("ES_BASELINE_INDEX", "node_baseline_index")

# Sensori su cui calcoliamo percentili e baseline. I nomi colonna sono ESATTI
# come in raw_readings (con spazi e parentesi): il job legge i documenti Mongo
# così come li ha scritti lo streaming, quindi le chiavi devono combaciare.
# Per ogni sensore associamo un suffisso snake_case usato nei campi di output
# (Mongo + ES): es. "Temperature (C)" → temp → p50_temp, avg_temp, ...
SENSORS = [
    ("Temperature (C)", "temp"),
    ("CO",              "co"),
    ("Smoke (ppm)",     "smoke"),
    ("Gas (Ohm)",       "gas"),
]

# percentile_approx vuole una accuracy: più alta = più precisa ma più costosa.
# 10000 è il default Spark, abbondante per ~poche migliaia di righe per nodo.
PCTL_ACCURACY = 10000

# Colonne (numeriche) usate per la matrice di correlazione tra sensori. Nomi
# ESATTI come in raw_readings. L'ordine definisce gli indici di riga/colonna
# della matrice prodotta da compute_correlations.
CORR_SENSORS = [
    "Temperature (C)", "Humidity (%)", "Pressure (hPA)", "Gas (Ohm)",
    "CO", "NO2", "Smoke (ppm)",
]

# Coppie di sensori la cui correlazione viene estratta come campo scalare nel
# documento nodo (le più informative per la combustione). (label, sensore_a,
# sensore_b); i sensori devono comparire in CORR_SENSORS.
CORR_PAIRS = [
    ("corr_temp_co",    "Temperature (C)", "CO"),
    ("corr_temp_gas",   "Temperature (C)", "Gas (Ohm)"),
    ("corr_temp_smoke", "Temperature (C)", "Smoke (ppm)"),
    ("corr_co_smoke",   "CO",              "Smoke (ppm)"),
    ("corr_co_gas",     "CO",              "Gas (Ohm)"),
    ("corr_smoke_gas",  "Smoke (ppm)",     "Gas (Ohm)"),
]

# ── Soglie assolute "fire-oriented" per la validazione della detection ─────────
# DEVONO restare allineate a spark_stream_job.py (stesse soglie tarate sui dati
# reali). La validazione misura QUESTE regole fisiche di combustione contro il
# ground-truth Fire — NON il flag generico is_anomaly, che include anche lo
# z-score (outlier statistici in qualunque direzione, es. il crollo di un
# sensore): usarlo conterebbe come falsi positivi anche anomalie non legate a un
# incendio, deprimendo artificiosamente la precision.
CO_FIRE_THRESHOLD    = 50.0     # ppm — CO > 50 indica combustione
SMOKE_FIRE_THRESHOLD = 0.08     # ppm — fumo oltre il doppio del max baseline
TEMP_FIRE_THRESHOLD  = 35.0     # °C  — calore anomalo
GAS_FIRE_THRESHOLD   = 5000.0   # Ohm — caduta sotto 5k = alta concentrazione di volatili


# ── Utility Elasticsearch (mirror per Grafana) ────────────────────────────────
def _safe_create_index(es_client: Elasticsearch, index: str, body: dict) -> None:
    """Crea un indice ES; ignora l'errore se esiste già (TOCTOU-safe).

    Stesso pattern di spark_stream_job.py per coerenza: due run del batch
    ravvicinate non devono fallire sul secondo create.
    """
    try:
        es_client.indices.create(index=index, body=body)
        print(f"Indice ES '{index}' creato.")
    except RequestError as e:
        if e.error != "resource_already_exists_exception":
            raise


def ensure_baseline_index(es_client: Elasticsearch) -> None:
    """Crea node_baseline_index con mapping esplicito (un documento per nodo).

    Mapping esplicito (non dynamic) così Grafana vede i tipi corretti: i campi
    percentile come float, i timestamp come date, node_id come keyword per i
    filtri/term. trend_hourly è un nested array di {hour, avg_temp}.
    """
    if es_client.indices.exists(index=ES_BASELINE_INDEX):
        return
    props = {
        "node_id":             {"type": "keyword"},
        "report_generated_at": {"type": "date", "format": "strict_date_optional_time"},
        "total_readings":      {"type": "long"},
        "distinct_reading_index": {"type": "long"},
        "first_ingest_ts":     {"type": "date", "format": "strict_date_optional_time"},
        "last_ingest_ts":      {"type": "date", "format": "strict_date_optional_time"},
        # Trend orario: array di oggetti {hour, avg_temp}. nested per poterlo
        # eventualmente interrogare in modo strutturato; per i pannelli table di
        # Grafana basta che il campo esista.
        "trend_hourly": {
            "type": "nested",
            "properties": {
                "hour":     {"type": "integer"},
                "avg_temp": {"type": "float"},
            },
        },
    }
    # Campi percentile/baseline per ogni sensore: p50/p95/p99/avg/min/max.
    for _, short in SENSORS:
        for stat in ("p50", "p95", "p99", "avg", "min", "max"):
            props[f"{stat}_{short}"] = {"type": "float"}
    # Metriche di validazione detection (vs ground-truth Fire).
    for k in ("val_tp", "val_fp", "val_tn", "val_fn"):
        props[k] = {"type": "long"}
    for k in ("val_precision", "val_recall", "val_f1", "val_accuracy"):
        props[k] = {"type": "float"}
    # Correlazioni chiave tra sensori (coppie).
    for label, _, _ in CORR_PAIRS:
        props[label] = {"type": "float"}
    mapping = {
        "settings": {"index": {"number_of_replicas": 0, "refresh_interval": "5s"}},
        "mappings": {"properties": props},
    }
    _safe_create_index(es_client, ES_BASELINE_INDEX, mapping)


def mirror_to_elasticsearch(docs: list[dict]) -> None:
    """Scrive lo snapshot baseline su ES (mirror per Grafana).

    Chiamato DAL DRIVER dopo collect(): i documenti sono pochissimi (1 per nodo,
    ~4 totali), quindi il collect è sicuro e il bulk è banale. _id = node_id →
    un solo documento per nodo, sovrascritto a ogni run (idempotente).

    Best-effort: se ES è irraggiungibile NON facciamo fallire il batch — la
    source of truth è Mongo node_baseline, il mirror ES è solo per la dashboard.
    """
    if not docs:
        return
    try:
        es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
        ensure_baseline_index(es)
        actions = [
            {"_index": ES_BASELINE_INDEX, "_id": d["node_id"], "_source": d}
            for d in docs
        ]
        _, errors = helpers.bulk(es, actions, raise_on_error=False,
                                 raise_on_exception=False)
        if errors:
            print(f"[WARN] mirror ES node_baseline: {len(errors)} documenti falliti")
        else:
            print(f"Mirror ES '{ES_BASELINE_INDEX}': {len(actions)} documenti scritti.")
    except Exception as exc:
        # Il mirror ES è secondario: logghiamo e proseguiamo. Mongo ha già il dato.
        print(f"[WARN] mirror ES fallito (Mongo resta source of truth): {exc}")


def mirror_global_correlation(matrix: list) -> None:
    """Scrive la matrice di correlazione GLOBALE (7×7) su un index ES dedicato.

    Best-effort, come gli altri mirror ES. La matrice è troppo grande per stare
    comodamente nel documento nodo: la salviamo a parte come righe lunghe
    (sensore_a, sensore_b, corr) così è ispezionabile/tabellabile in Grafana.
    """
    if not matrix:
        return
    try:
        es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
        index = "sensor_correlation_index"
        _safe_create_index(es, index, {
            "settings": {"index": {"number_of_replicas": 0}},
            "mappings": {"properties": {
                "sensor_a": {"type": "keyword"},
                "sensor_b": {"type": "keyword"},
                "correlation": {"type": "float"},
                "report_generated_at": {"type": "date", "format": "strict_date_optional_time"},
            }},
        })
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        actions = []
        for i, a in enumerate(CORR_SENSORS):
            for j, b in enumerate(CORR_SENSORS):
                actions.append({
                    "_index": index,
                    # _id deterministico: ogni run sovrascrive la stessa cella.
                    "_id": f"{a}__{b}",
                    "_source": {
                        "sensor_a": a, "sensor_b": b,
                        "correlation": matrix[i][j],
                        "report_generated_at": generated_at,
                    },
                })
        _, errors = helpers.bulk(es, actions, raise_on_error=False, raise_on_exception=False)
        if errors:
            print(f"[WARN] mirror ES correlazione globale: {len(errors)} celle fallite")
        else:
            print(f"Mirror ES 'sensor_correlation_index': matrice {len(CORR_SENSORS)}×{len(CORR_SENSORS)} scritta.")
    except Exception as exc:
        print(f"[WARN] mirror correlazione globale fallito: {exc}")


# ── Calcolo del report ────────────────────────────────────────────────────────
def build_baseline(spark: SparkSession):
    """Legge raw_readings e calcola il DataFrame del report per nodo.

    Restituisce un DataFrame con una riga per node_id. La lettura Mongo usa il
    connettore mongo-spark identico allo streaming (.format("mongodb")), in sola
    lettura: NON tocchiamo raw_readings.
    """
    # ── Lettura completa di raw_readings (READ-ONLY) ──────────────────────────
    raw = (spark.read
           .format("mongodb")
           .option("connection.uri", MONGO_URI)
           .option("collection", "raw_readings")
           .load())

    # ingest_ts è stringa ISO8601 (così la scrive il producer/streaming): la
    # convertiamo a timestamp per poter estrarre l'ora-del-giorno e per i
    # min/max temporali (first/last). to_timestamp gestisce il formato ISO.
    raw = raw.withColumn("ingest_ts_parsed", F.to_timestamp(F.col("ingest_ts")))

    # ── 1. Percentili + baseline per sensore ──────────────────────────────────
    # percentile_approx in un colpo solo per p50/p95/p99 (ritorna un array),
    # più avg/min/max. Costruiamo dinamicamente le aggregazioni per ogni sensore.
    # Nota null-safety: percentile_approx/avg/min/max ignorano i null per default,
    # quindi un sensore con qualche null (o l'intero Fire mancante su nodo_4, che
    # comunque NON è tra i SENSORS) non fa fallire il job.
    agg_exprs = [
        F.count(F.lit(1)).alias("total_readings"),
        F.countDistinct(F.col("reading_index")).alias("distinct_reading_index"),
        F.min(F.col("ingest_ts_parsed")).alias("first_ingest_ts"),
        F.max(F.col("ingest_ts_parsed")).alias("last_ingest_ts"),
    ]
    for col_name, short in SENSORS:
        c = F.col(f"`{col_name}`")  # backtick: il nome ha spazi/parentesi
        # percentile_approx con array di percentili → 1 sola scansione per i 3
        pctls = F.percentile_approx(c, [0.5, 0.95, 0.99], PCTL_ACCURACY)
        agg_exprs += [
            F.round(pctls.getItem(0), 4).alias(f"p50_{short}"),
            F.round(pctls.getItem(1), 4).alias(f"p95_{short}"),
            F.round(pctls.getItem(2), 4).alias(f"p99_{short}"),
            F.round(F.avg(c), 4).alias(f"avg_{short}"),
            F.round(F.min(c), 4).alias(f"min_{short}"),
            F.round(F.max(c), 4).alias(f"max_{short}"),
        ]

    base = raw.groupBy("node_id").agg(*agg_exprs)

    # ── 2. Trend orario (avg temperatura per ora-del-giorno) ──────────────────
    # Estraiamo l'ora (0-23) da ingest_ts_parsed e mediamo la temperatura.
    # Poi collassiamo in un array di struct {hour, avg_temp} per nodo: una
    # struttura compatta da mettere nel singolo documento baseline del nodo.
    hourly = (raw
              .withColumn("hour", F.hour(F.col("ingest_ts_parsed")))
              # Scartiamo le righe senza ora valida (ingest_ts non parsabile):
              # non devono inquinare il trend né creare un bucket hour=null.
              .filter(F.col("hour").isNotNull())
              .groupBy("node_id", "hour")
              .agg(F.round(F.avg(F.col("`Temperature (C)`")), 4).alias("avg_temp"))
              .groupBy("node_id")
              .agg(F.sort_array(
                       F.collect_list(F.struct("hour", "avg_temp"))
                   ).alias("trend_hourly")))

    # Join trend nel report. left join: se per qualche motivo un nodo non avesse
    # ore valide, resta comunque nel report (trend_hourly = null, gestito a valle).
    report = base.join(hourly, on="node_id", how="left")
    return report


def compute_validation(spark: SparkSession) -> dict[str, dict]:
    """Valida la detection "fire-oriented" contro il ground-truth Fire.

    Legge processed_readings (contiene Fire + i valori dei sensori nella stessa
    riga: nessun join necessario). Definizione:
      - positivo reale  = Fire >= 1 (incendio);
      - negativo reale  = Fire == 0;
      - predizione      = regola sulle soglie assolute di combustione
        (CO > 50  OR  Smoke > 0.08  OR  Temp > 35  OR  Gas < 5000), calcolata
        sui VALORI dei sensori — un vero predittore di incendio, indipendente
        dal label.

    NB: NON usiamo il flag generico is_anomaly come predizione. is_anomaly scatta
    anche per z-score (outlier statistici in qualunque direzione, incluso il
    crollo di un sensore), che NON è un incendio: ogni anomalia non-fire con
    Fire==0 risulterebbe un falso positivo, rendendo precision/F1 strutturalmente
    bassi e fuorvianti. Validare la sola regola fisica di combustione misura ciò
    che davvero vogliamo: la capacità di rilevare il fuoco.

    Calcola la confusion matrix (TP/FP/TN/FN) e precision/recall/F1/accuracy PER
    NODO. nodo_4 (Fire == null, niente ground-truth) è escluso.

    Restituisce {node_id: {tp, fp, tn, fn, precision, recall, f1, accuracy}}.
    Gira sul driver via un'unica aggregazione distribuita + collect (poche righe).
    """
    proc = (spark.read
            .format("mongodb")
            .option("connection.uri", MONGO_URI)
            .option("collection", "processed_readings")
            .load())

    # Solo righe con ground-truth valido (Fire non null → esclude nodo_4).
    proc = proc.filter(F.col("Fire").isNotNull())

    # actual = Fire >= 1 (ground-truth incendio).
    actual_pos = F.col("Fire") >= 1
    # predicted = regola fire-oriented sulle soglie di combustione, sui VALORI dei
    # sensori. coalesce(..., False): un sensore null non deve propagare null
    # nell'OR (verrebbe trattato come "soglia non superata"). 1 sola scansione.
    pred_pos = (
        F.coalesce(F.col("CO")                  > CO_FIRE_THRESHOLD,    F.lit(False))
        | F.coalesce(F.col("`Smoke (ppm)`")     > SMOKE_FIRE_THRESHOLD, F.lit(False))
        | F.coalesce(F.col("`Temperature (C)`") > TEMP_FIRE_THRESHOLD,  F.lit(False))
        | F.coalesce(F.col("`Gas (Ohm)`")       < GAS_FIRE_THRESHOLD,   F.lit(False))
    )
    agg = (proc.groupBy("node_id").agg(
        F.sum(F.when(actual_pos & pred_pos, 1).otherwise(0)).alias("tp"),
        F.sum(F.when(~actual_pos & pred_pos, 1).otherwise(0)).alias("fp"),
        F.sum(F.when(~actual_pos & ~pred_pos, 1).otherwise(0)).alias("tn"),
        F.sum(F.when(actual_pos & ~pred_pos, 1).otherwise(0)).alias("fn"),
    ))

    out: dict[str, dict] = {}
    for r in agg.collect():
        tp, fp, tn, fn = r["tp"], r["fp"], r["tn"], r["fn"]
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall    = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)
              if (precision + recall) else 0.0)
        total = tp + fp + tn + fn
        accuracy = (tp + tn) / total if total else 0.0
        out[r["node_id"]] = {
            "val_tp": int(tp), "val_fp": int(fp),
            "val_tn": int(tn), "val_fn": int(fn),
            "val_precision": round(precision, 4),
            "val_recall":    round(recall, 4),
            "val_f1":        round(f1, 4),
            "val_accuracy":  round(accuracy, 4),
        }
    return out


def compute_correlations(spark) -> dict[str, list]:
    """Matrice di correlazione di Pearson tra i sensori (MLlib), per nodo + globale.

    Usa pyspark.ml.stat.Correlation su un vettore delle feature numeriche. È
    un'analisi multivariata vera, impossibile in streaming (richiede l'intero
    dataset). Le righe con un sensore null le scartiamo (VectorAssembler con
    handleInvalid="skip") per non rompere il calcolo.

    Restituisce {scope: matrix} dove scope ∈ {"global","nodo_1",...} e matrix è
    una lista di righe (lista di float), allineata all'ordine di CORR_SENSORS.
    """
    raw = (spark.read
           .format("mongodb")
           .option("connection.uri", MONGO_URI)
           .option("collection", "raw_readings")
           .load())

    # I nomi sensore hanno spazi/parentesi ("Temperature (C)"): VectorAssembler
    # vuole nomi colonna SEMPLICI (i backtick valgono in F.col/SQL, non come nome
    # letterale passato ad inputCols). Rinominiamo a alias posizionali f0..fN
    # mantenendo node_id, così l'ordine resta allineato a CORR_SENSORS.
    alias = [f"f{i}" for i in range(len(CORR_SENSORS))]
    renamed = raw.select(
        F.col("node_id"),
        *[F.col(f"`{c}`").cast("double").alias(a) for c, a in zip(CORR_SENSORS, alias)],
    )

    assembler = VectorAssembler(
        inputCols=alias,
        outputCol="features",
        handleInvalid="skip",  # scarta le righe con qualche sensore null
    )

    def _matrix_for(df) -> list:
        vec = assembler.transform(df)
        # Correlation.corr restituisce una Row con una DenseMatrix; la riduciamo a
        # lista di liste arrotondata, ordine = CORR_SENSORS.
        m = Correlation.corr(vec, "features", "pearson").head()[0]
        n = len(CORR_SENSORS)
        return [[round(float(m[i, j]), 4) for j in range(n)] for i in range(n)]

    result: dict[str, list] = {}
    # Globale su tutto il dataset.
    result["global"] = _matrix_for(renamed)
    # Per nodo (i node_id presenti nei dati).
    node_ids = [r["node_id"] for r in renamed.select("node_id").distinct().collect()]
    for nid in sorted(node_ids):
        result[nid] = _matrix_for(renamed.filter(F.col("node_id") == nid))
    return result


def _corr_pairs_from_matrix(matrix: list) -> dict:
    """Estrae le coppie di CORR_PAIRS da una matrice (lista di liste) come scalari."""
    idx = {name: i for i, name in enumerate(CORR_SENSORS)}
    out = {}
    for label, a, b in CORR_PAIRS:
        i, j = idx[a], idx[b]
        try:
            out[label] = matrix[i][j]
        except (IndexError, TypeError):
            out[label] = None
    return out


def to_documents(report_rows: list, validation: dict, correlations: dict) -> list[dict]:
    """Converte le Row Spark collezionate in dict puliti per Mongo/ES.

    I timestamp Spark arrivano come datetime Python: li serializziamo in stringa
    ISO8601 UTC (coerente con come ingest_ts è memorizzato altrove e con il
    mapping `date` di ES). trend_hourly è una lista di Row → lista di dict.
    Aggiungiamo report_generated_at uguale per tutti i documenti della stessa run.

    validation: {node_id: {val_*}} dalle metriche di detection (può non avere il
      nodo se senza ground-truth, es. nodo_4 → campi assenti, gestito a valle).
    correlations: {scope: matrix}; per ogni nodo estraiamo le coppie CORR_PAIRS.
    """
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def _iso(dt):
        # Spark restituisce datetime naive in UTC per i timestamp Mongo.
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    docs = []
    for row in report_rows:
        d = row.asDict(recursive=True)
        d["report_generated_at"] = generated_at
        d["first_ingest_ts"] = _iso(d.get("first_ingest_ts"))
        d["last_ingest_ts"]  = _iso(d.get("last_ingest_ts"))
        # trend_hourly: garantiamo sempre una lista (mai null) per semplicità a valle.
        trend = d.get("trend_hourly") or []
        d["trend_hourly"] = [
            {"hour": int(t["hour"]), "avg_temp": t.get("avg_temp")}
            for t in trend if t.get("hour") is not None
        ]
        nid = d["node_id"]
        # ── Metriche di validazione detection (assenti per nodi senza Fire) ───────
        val = validation.get(nid, {})
        for k in ("val_tp", "val_fp", "val_tn", "val_fn"):
            d[k] = val.get(k)
        for k in ("val_precision", "val_recall", "val_f1", "val_accuracy"):
            d[k] = val.get(k)
        # ── Correlazioni chiave del nodo (coppie scalari) ────────────────────────
        node_matrix = correlations.get(nid)
        pairs = _corr_pairs_from_matrix(node_matrix) if node_matrix else {}
        for label, _, _ in CORR_PAIRS:
            d[label] = pairs.get(label)
        docs.append(d)
    return docs


def _baseline_schema() -> StructType:
    """Schema ESPLICITO del DataFrame di output, ricostruito dai dict.

    Senza schema esplicito, spark.createDataFrame(docs) inferisce trend_hourly
    (lista di dict {hour, avg_temp}) come array<map<string,long>>: una map vuole
    valori OMOGENEI, e poiché hour è intero il value type viene forzato a long →
    avg_temp (float) finisce NULL. Dichiarando trend_hourly come
    array<struct<hour:int, avg_temp:double>> i due campi mantengono il proprio
    tipo. I campi sensore (p50/p95/p99/avg/min/max per ogni SENSORS) sono generati
    qui per non duplicare i nomi e restare allineati al calcolo in compute_report.
    """
    fields = [
        StructField("node_id",                StringType(),  True),
        StructField("total_readings",         IntegerType(), True),
        StructField("distinct_reading_index", IntegerType(), True),
        StructField("first_ingest_ts",        StringType(),  True),
        StructField("last_ingest_ts",         StringType(),  True),
        StructField("report_generated_at",    StringType(),  True),
    ]
    for _, short in SENSORS:
        for stat in ("p50", "p95", "p99", "avg", "min", "max"):
            fields.append(StructField(f"{stat}_{short}", DoubleType(), True))
    fields.append(StructField(
        "trend_hourly",
        ArrayType(StructType([
            StructField("hour",     IntegerType(), True),
            StructField("avg_temp", DoubleType(),  True),
        ])),
        True,
    ))
    # Metriche di validazione detection (conteggi interi + metriche float).
    for k in ("val_tp", "val_fp", "val_tn", "val_fn"):
        fields.append(StructField(k, IntegerType(), True))
    for k in ("val_precision", "val_recall", "val_f1", "val_accuracy"):
        fields.append(StructField(k, DoubleType(), True))
    # Correlazioni chiave (coppie di sensori) come scalari.
    for label, _, _ in CORR_PAIRS:
        fields.append(StructField(label, DoubleType(), True))
    return StructType(fields)


def write_to_mongo(spark: SparkSession, docs: list[dict]) -> None:
    """Scrive lo snapshot su MongoDB node_baseline in overwrite.

    Ricostruiamo un DataFrame dai dict (già con i timestamp in stringa ISO e il
    report_generated_at) e usiamo il connettore mongo-spark con mode("overwrite"):
    il connettore TRONCA la collection e riscrive — esattamente lo snapshot
    completo che vogliamo (1 documento per nodo, niente residui di run vecchie).

    Scriviamo via DataFrame (e non pymongo) per restare coerenti con lo stile del
    progetto e perché overwrite del connettore dà la semantica snapshot pulita.
    """
    if not docs:
        print("[WARN] Nessun documento baseline da scrivere (raw_readings vuoto?).")
        return
    out_df = spark.createDataFrame(docs, schema=_baseline_schema())
    (out_df.write
     .format("mongodb")
     .option("connection.uri", MONGO_URI)
     .option("collection", BASELINE_COLLECTION)
     .mode("overwrite")
     .save())
    print(f"MongoDB '{BASELINE_COLLECTION}': snapshot di {len(docs)} nodi scritto (overwrite).")


def main():
    spark = (SparkSession.builder
             .appName("IoT-Batch-Baseline")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    started = datetime.now(timezone.utc)
    print(f"[BATCH] Avvio calcolo baseline su raw_readings @ {started.isoformat()}")

    report = build_baseline(spark)
    # collect(): il report ha 1 riga per nodo (~4 righe). Sicuro sul driver.
    rows = report.collect()

    # ── Analisi aggiuntive (sfruttano dati che la baseline descrittiva ignora) ──
    # 1. Validazione della detection vs ground-truth Fire (precision/recall/F1).
    validation = compute_validation(spark)
    print(f"[BATCH] Validazione detection calcolata per {len(validation)} nodi.")
    # 2. Correlazioni tra sensori (MLlib), per nodo + globale.
    correlations = compute_correlations(spark)
    print(f"[BATCH] Matrici di correlazione calcolate ({len(correlations)} scope).")

    docs = to_documents(rows, validation, correlations)

    # Source of truth → Mongo (overwrite snapshot).
    write_to_mongo(spark, docs)
    # Mirror per Grafana → ES (best-effort).
    mirror_to_elasticsearch(docs)
    # La matrice di correlazione globale (7×7) è troppo grande per il doc nodo:
    # la scriviamo a parte come mirror ES per ispezione/relazione.
    mirror_global_correlation(correlations.get("global"))

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"[BATCH] Completato: {len(docs)} nodi in {elapsed:.1f}s.")
    spark.stop()


if __name__ == "__main__":
    main()
