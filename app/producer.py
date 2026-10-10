"""Lee precios de Binance (API publica) y los publica en el topic crypto.prices."""
import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

TOPIC = "crypto.prices"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
URL = "https://data-api.binance.vision/api/v3/ticker/price"

producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")})


def on_delivery(err, msg):
    if err:
        print("ERROR entrega:", err)


while True:
    for sym in SYMBOLS:
        try:
            price = requests.get(URL, params={"symbol": sym}, timeout=5).json()["price"]
        except Exception as e:
            print("fallo API:", e)
            continue
        event = {"symbol": sym, "price": price, "ts": datetime.now(timezone.utc).isoformat()}
        # key=symbol -> mismo simbolo siempre en la misma particion (orden garantizado)
        producer.produce(TOPIC, key=sym, value=json.dumps(event), callback=on_delivery)
        print("enviado", event)
    producer.flush()
    time.sleep(5)
