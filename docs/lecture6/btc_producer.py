# btc_producer.py
# Lecture 6 — Streaming Data Analytics (MBA)
# Streams BTC-USD price in REAL TIME (24x7) from Yahoo Finance → Kafka
# Polls every 10 seconds. Run indefinitely: Ctrl+C to stop.
# Requires: pip install yfinance kafka-python

import json
import time
import sys
from datetime import datetime
import yfinance as yf
from kafka import KafkaProducer, errors

KAFKA_BROKER  = 'localhost:9092'
TOPIC         = 'stock-topic'
POLL_INTERVAL = 10    # seconds between each price check

# ── Connect to Kafka ──────────────────────────────────────────────────────────
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
    producer.partitions_for(TOPIC)
    print("✅ Connected to Kafka at", KAFKA_BROKER)
except errors.NoBrokersAvailable:
    print("❌ Kafka not available. Run: docker-compose up -d")
    sys.exit(1)

# ── Stream BTC-USD live price ─────────────────────────────────────────────────
print(f"\n₿  Streaming BTC-USD live → topic '{TOPIC}'")
print(f"   Polling every {POLL_INTERVAL}s — Ctrl+C to stop\n")

count = 0
while True:
    try:
        btc  = yf.Ticker("BTC-USD")
        data = btc.fast_info         # lightweight call — just latest price

        record = {
            "symbol":        "BTC-USD",
            "timestamp":     datetime.utcnow().isoformat(),
            "price":         round(float(data.last_price), 2),
            "previous_close":round(float(data.previous_close), 2),
            "market":        "crypto"
        }
        producer.send(TOPIC, value=record)
        count += 1
        print(f"  [{count}] {record['timestamp']} | BTC: ${record['price']:,.2f}")

    except Exception as e:
        print(f"  ⚠️  Error fetching price: {e}")

    time.sleep(POLL_INTERVAL)
