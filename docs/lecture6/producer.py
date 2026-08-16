# producer.py
# Lecture 6 — Streaming Data Analytics (MBA)
# Reads stock_data.csv and streams each row to Kafka topic: stock-topic
# Run: python producer.py

import csv
import time
import json
import sys
from kafka import KafkaProducer, errors

KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'stock-topic'

# ── Step 1: Connect to Kafka broker ──────────────────────────────────────────
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
    producer.partitions_for(TOPIC)   # confirms the broker is reachable
    print("✅ Connected to Kafka broker at", KAFKA_BROKER)
except errors.NoBrokersAvailable:
    print("❌ Kafka broker not available at", KAFKA_BROKER)
    print("   Make sure Docker + Kafka are running: docker-compose up -d")
    sys.exit(1)
except Exception as e:
    print("❌ Error connecting to Kafka:", e)
    sys.exit(1)

# ── Step 2: Read CSV and stream each row ─────────────────────────────────────
print(f"\n📤 Streaming stock_data.csv → topic '{TOPIC}'\n")

with open('stock_data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        try:
            producer.send(TOPIC, value=row)
            print(f"  Produced: {row}")
            time.sleep(1)          # 1 second between messages (simulates live feed)
        except Exception as e:
            print("❌ Failed to send message:", e)

producer.flush()
print("\n✅ All rows streamed. Producer done.")
