import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC     = os.environ["KAFKA_TOPIC"]
DELAY_MS  = int(os.environ.get("PRODUCER_DELAY_MS", "500"))
LOOP      = os.environ.get("LOOP_FOREVER", "true").lower() == "true"
NODE_ID   = os.environ["NODE_ID"]    # "nodo_1" | "nodo_2" | "nodo_3" | "nodo_4"
CSV_PATH  = os.environ["CSV_PATH"]   # path al CSV di questo nodo
# Partizione esplicita: evita collisioni di hash murmur2 (nodo_1 e nodo_4 collidono
# sulla stessa partizione). Se non settata, Kafka usa l'hash della chiave (node_id)
# e due nodi diversi possono finire sulla stessa partizione → ordine non garantito
# per nodo. Avvisiamo esplicitamente invece di fallire silenziosamente.
_part_env = os.environ.get("KAFKA_PARTITION", "").strip()
if _part_env.isdigit():
    PARTITION = int(_part_env)
else:
    PARTITION = None
    print(
        f"[WARN] KAFKA_PARTITION non impostata per {NODE_ID}: i messaggi saranno "
        f"instradati per hash della chiave. Possibili collisioni di partizione tra nodi."
    )

NUMERIC_FIELDS = [
    "Temperature (C)", "Humidity (%)", "Pressure (hPA)", "Gas (Ohm)",
    "Visible Light", "IR", "UV index", "CO", "NO2", "Smoke (ppm)",
]


def get_producer(retries: int = 20, wait: int = 5) -> KafkaProducer:
    for attempt in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                retry_backoff_ms=500,
                request_timeout_ms=120000,
                max_block_ms=30000,
            )
        except NoBrokersAvailable:
            print(f"Kafka non disponibile (tentativo {attempt + 1}/{retries}), riprovo in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossibile connettersi a Kafka dopo tutti i tentativi")


def load_csv_rows(node_id: str, path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {}
            for field in NUMERIC_FIELDS:
                raw = row.get(field, "").strip()
                if not raw:
                    record[field] = None
                else:
                    try:
                        record[field] = float(raw)
                    except ValueError:
                        record[field] = None

            fire_raw = row.get("Fire", "").strip()
            try:
                record["Fire"] = int(fire_raw) if fire_raw != "" else None
            except ValueError:
                record["Fire"] = None

            record["node_id"] = node_id
            rows.append(record)

    print(f"  {node_id}: {len(rows)} righe caricate da {path.split('/')[-1]}")
    return rows


def _on_send_error(exc):
    print(f"[ERROR] Invio Kafka fallito: {exc}")


def main():
    producer = get_producer()
    print(f"Connesso a Kafka ({BOOTSTRAP}), topic: {TOPIC}, nodo: {NODE_ID}")

    rows = load_csv_rows(NODE_ID, CSV_PATH)
    print(f"Caricati {len(rows)} record. Loop continuo: {LOOP}")

    fire_counts: dict = {}
    for r in rows:
        k = str(r.get("Fire"))
        fire_counts[k] = fire_counts.get(k, 0) + 1
    print(f"  {NODE_ID} Fire distribution: {dict(sorted(fire_counts.items()))}")

    delay_s    = DELAY_MS / 1000.0
    sent       = 0
    pass_n     = 0
    global_idx = 0

    try:
        while True:
            pass_n += 1
            print(f"--- Pass #{pass_n} ---")
            for record in rows:
                msg = {
                    **record,
                    "reading_index": global_idx,
                    "ingest_ts": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S.%f"
                    )[:-3] + "Z",
                }
                global_idx += 1
                producer.send(TOPIC, key=NODE_ID, value=msg, partition=PARTITION).add_errback(_on_send_error)
                sent += 1
                if sent % 500 == 0:
                    print(f"  Inviati {sent} messaggi (pass #{pass_n})...")
                time.sleep(delay_s)

            producer.flush()
            print(f"Pass #{pass_n} completato: {sent} messaggi totali inviati.")

            if not LOOP:
                break
    finally:
        producer.close()
        print("Producer terminato.")


if __name__ == "__main__":
    main()
