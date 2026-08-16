# tcs_producer.py
# Lecture 6 — Streaming Data Analytics (MBA)
# Fetches TCS.NS historical data from Yahoo Finance and streams to Kafka
# Run: python tcs_producer.py
# Requires: pip install yfinance kafka-python

import json
import time
import sys
import yfinance as yf
from kafka import KafkaProducer, errors

KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'stock-topic'

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
    print("❌ Kafka broker not available. Run: docker-compose up -d")
    sys.exit(1)

# ── Fetch TCS historical data from Yahoo Finance ──────────────────────────────
print("\n📥 Fetching TCS.NS data from Yahoo Finance...")
tcs = yf.Ticker("TCS.NS")
df  = tcs.history(period="5d", interval="1m")   # last 5 days, 1-min bars
df  = df.reset_index()

print(f"   Got {len(df)} rows. Streaming to topic '{TOPIC}'...\n")

# ── Stream each row to Kafka ──────────────────────────────────────────────────
for _, row in df.iterrows():
    record = {
        "symbol":    "TCS.NS",
        "timestamp": str(row["Datetime"]),
        "open":      round(float(row["Open"]),  2),
        "high":      round(float(row["High"]),  2),
        "low":       round(float(row["Low"]),   2),
        "close":     round(float(row["Close"]), 2),
        "volume":    int(row["Volume"])
    }
    producer.send(TOPIC, value=record)
    print(f"  Produced: {record['timestamp']} | Close: ₹{record['close']}")
    time.sleep(0.5)   # stream at 2 records/sec

producer.flush()
print("\n✅ TCS stream complete.")
