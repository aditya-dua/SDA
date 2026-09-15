"""
transaction_producer_16.py
Lecture 16 Extension — Live Fraud Dashboard

Streams transactions_16.csv to the isolated transactions-topic-l16 topic.
"""

import csv
import json
import time

from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
TOPIC = "transactions-topic-l16"
CSV_FILE = "transactions_16.csv"
DELAY_SECONDS = 0.3

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print("Lecture 16 transaction producer started")
print(f"   File: {CSV_FILE}")
print(f"   Topic: {TOPIC}")
print("-" * 60)

count = 0
try:
    with open(CSV_FILE, newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            producer.send(TOPIC, value=row)
            count += 1
            marker = "ALERT" if row["is_fraud"] == "true" else "->"
            print(
                f"{marker:<5} [{count:03d}] {row['transaction_id']} "
                f"{row['user_name']:<16} INR {float(row['amount']):>10,.2f}"
            )
            time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"\nSent {count} transactions to {TOPIC}")
except FileNotFoundError:
    print(f"{CSV_FILE} not found. Run: python generate_transactions_16.py")
except KeyboardInterrupt:
    producer.flush()
    print(f"\nStopped after {count} transactions.")
finally:
    producer.close()
