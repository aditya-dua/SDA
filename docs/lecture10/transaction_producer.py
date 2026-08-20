"""
transaction_producer.py
Lecture 10 — Banking Fraud Detection Pipeline
Streaming Data Analytics | MBA Course

Reads transactions.csv and streams each row to Kafka's
'transactions-topic' with a 0.3s delay to simulate live feed.

Run AFTER: python generate_transactions.py
Terminal 1: python transaction_producer.py
"""

import csv
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER  = 'localhost:9092'
TOPIC         = 'transactions-topic'
DELAY_SECONDS = 0.3   # Change to 0.05 for a faster feed

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🏦 Transaction Producer started")
print(f"   Topic  : {TOPIC}")
print(f"   Delay  : {DELAY_SECONDS}s per message")
print("-" * 60)

count = 0
try:
    with open('transactions.csv', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(TOPIC, value=row)
            count += 1
            flag = "🚨" if row.get('is_fraud') == 'true' else "→ "
            print(f"{flag} [{count:04d}] {row['user_name']:<22} "
                  f"{row['city']:<12} ₹{float(row['amount']):>10,.2f}  "
                  f"{row['merchant']}")
            time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"\n✅ Done. Sent {count} transactions to '{TOPIC}'")

except FileNotFoundError:
    print("❌ transactions.csv not found.")
    print("   Run: python generate_transactions.py  first.")
except KeyboardInterrupt:
    print(f"\n⏹  Stopped after {count} messages.")
    producer.flush()
