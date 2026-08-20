"""
consumer_store.py
Lecture 10 — Banking Fraud Detection Pipeline
Streaming Data Analytics | MBA Course

Consumes from 'transactions-topic' and:
  1. Appends every transaction to MongoDB 'transactions' collection
  2. Upserts 'user_profiles' with latest location + running spend totals

Terminal 2 (run alongside consumer_fraud.py)

UPDATE the MONGO_URI below before running.
"""

import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIG — update MONGO_URI with your Atlas connection string
# ---------------------------------------------------------------------------
MONGO_URI    = "mongodb+srv://your-user:your-pass@cluster0.xxxxx.mongodb.net/"
DB_NAME      = "sda_course"
KAFKA_BROKER = "localhost:9092"
TOPIC        = "transactions-topic"

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
client = MongoClient(MONGO_URI)
db     = client[DB_NAME]
txns   = db["transactions"]
profiles = db["user_profiles"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='store-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("💾 Storage Consumer started")
print(f"   Kafka  : {KAFKA_BROKER} → {TOPIC}")
print(f"   MongoDB: {DB_NAME}.transactions  |  {DB_NAME}.user_profiles")
print("-" * 60)

count = 0
for msg in consumer:
    txn = msg.value

    # 1. Append raw transaction
    txns.insert_one({**txn, "ingested_at": datetime.utcnow()})

    # 2. Upsert user_profiles — update last location + running totals
    profiles.update_one(
        {"user_id": txn["user_id"]},
        {
            "$set": {
                "user_name":       txn["user_name"],
                "card_last4":      txn["card_last4"],
                "last_city":       txn["city"],
                "last_country":    txn["country"],
                "last_lat":        float(txn["lat"]),
                "last_lon":        float(txn["lon"]),
                "last_timestamp":  txn["timestamp"],
            },
            "$inc": {
                "total_spend": float(txn["amount"]),
                "txn_count":   1,
            }
        },
        upsert=True
    )

    count += 1
    print(f"💾 [{count:04d}] {txn['transaction_id']}  "
          f"{txn['user_name']:<22} {txn['city']:<12} ₹{float(txn['amount']):>10,.2f}")
    print(f"   📍 Profile updated → last seen in {txn['city']}")
