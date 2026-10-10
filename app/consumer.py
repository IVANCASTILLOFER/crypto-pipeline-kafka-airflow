"""Consume crypto.prices y lo guarda en Postgres (raw_prices) de forma idempotente."""
import json
import os

import psycopg2
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
    "group.id": "crypto-loader",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # commit manual: solo despues de guardar en la BD
})
consumer.subscribe(["crypto.prices"])
conn = psycopg2.connect(os.environ["PG_DSN"])

while True:
    msgs = consumer.consume(num_messages=50, timeout=2.0)
    rows = []
    for m in msgs:
        if m.error():
            print("error:", m.error())
            continue
        e = json.loads(m.value())
        rows.append((e["symbol"], e["price"], e["ts"], m.partition(), m.offset()))
    if not rows:
        continue
    with conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO raw_prices (symbol, price, event_ts, kafka_partition, kafka_offset)
               VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            rows,
        )
    consumer.commit(asynchronous=False)
    print(f"guardados {len(rows)} registros")
