// Eseguito automaticamente da MongoDB al primo avvio del container
db = db.getSiblingDB("sensor_data");

// Helper: createCollection idempotente (MongoDB 7.0 lancia errore se la collezione esiste già)
function createIfMissing(name) {
    try {
        db.createCollection(name);
    } catch (e) {
        if (e.codeName !== "NamespaceExists") throw e;
    }
}

// ── raw_readings: tutti i record inviati dal producer (pre-filtro) ────────────
createIfMissing("raw_readings");
db.raw_readings.createIndex({ node_id: 1 });
// TTL 7 giorni — raw è storico immutabile ma non serve tenerlo per sempre
db.raw_readings.createIndex({ ingest_ts: 1 }, { expireAfterSeconds: 604800 });
db.raw_readings.createIndex({ node_id: 1, ingest_ts: 1 });
// M3: indice unico per deduplicazione — il connettore Spark usa idFieldList=node_id,reading_index
db.raw_readings.createIndex({ node_id: 1, reading_index: 1 }, { unique: true });

// ── processed_readings: dati post-elaborazione Spark (batch layer) ───────────
// Contiene i dati filtrati e arricchiti: z-score, flag anomalia, fire_state_label.
// Source of truth per rianalisi future senza ripassare da raw_readings + Welford.
createIfMissing("processed_readings");
db.processed_readings.createIndex({ node_id: 1 });
// TTL 3 giorni — si può riprocessare da raw_readings se serve uno storico più lungo
db.processed_readings.createIndex({ ingest_ts: 1 }, { expireAfterSeconds: 259200 });
db.processed_readings.createIndex({ node_id: 1, ingest_ts: 1 });
db.processed_readings.createIndex({ node_id: 1, reading_index: 1 }, { unique: true });
db.processed_readings.createIndex({ is_anomaly: 1 });
db.processed_readings.createIndex({ is_fire: 1 });

// ── node_stats: statistiche Welford online per z-score (upsert per nodo) ──────
createIfMissing("node_stats");
db.node_stats.createIndex({ node_id: 1 }, { unique: true });

// ── agg_per_nodo: aggregati rolling per nodo (corrisponde al notebook) ─────────
createIfMissing("agg_per_nodo");
db.agg_per_nodo.createIndex({ node_id: 1 }, { unique: true });

// ── fire_events: eventi incendio (Fire >= 1), TTL 30 giorni ───────────────────
createIfMissing("fire_events");
db.fire_events.createIndex({ node_id: 1, ingest_ts: -1 });
db.fire_events.createIndex({ ingest_ts: 1 }, { expireAfterSeconds: 2592000 });

// ── node_baseline: snapshot del BATCH LAYER storico (Lambda) ──────────────────
// 1 documento per nodo, riscritto in overwrite a ogni run del job batch
// (batch_analytics.py). Contiene percentili/baseline/trend orari calcolati
// sull'INTERA storia di raw_readings. Indice unico su node_id: c'è esattamente
// un documento per nodo. Niente TTL: è uno snapshot, non una serie storica.
createIfMissing("node_baseline");
db.node_baseline.createIndex({ node_id: 1 }, { unique: true });

print("MongoDB: collezioni e indici inizializzati su sensor_data.");
