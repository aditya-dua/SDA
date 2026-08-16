from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime

# ── consumer_mongodb.py ──────────────────────────────────────────────────────
# Reads from stock-topic and saves every message to MongoDB Atlas
# Prerequisites:
#   pip install kafka-python pymongo
#   Replace MONGO_URI with your Atlas connection string
# ─────────────────────────────────────────────────────────────────────────────

# 1. Paste your MongoDB Atlas connection string here
MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"

# 2. Connect to MongoDB
client     = MongoClient(MONGO_URI)
db         = client["sda_course"]          # database name
collection = db["stock_prices"]            # collection name

print("✅ Connected to MongoDB Atlas")
print(f"   Database  : {db.name}")
print(f"   Collection: {collection.name}")
print("-" * 45)

# 3. Kafka consumer
consumer = KafkaConsumer(
    'stock-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

saved = 0

print("📡 Listening on stock-topic...")
for msg in consumer:
    data = msg.value

    # Add metadata before saving
    data['_kafka_offset']    = msg.offset
    data['_kafka_partition'] = msg.partition
    data['_saved_at']        = datetime.utcnow().isoformat()

    # Insert into MongoDB
    result = collection.insert_one(data)
    saved += 1

    sym   = data.get('symbol', '?')
    price = data.get('price', data.get('Close', '?'))
    print(f"💾 Saved [{sym}] ₹{price}  →  _id: {result.inserted_id}  (total saved: {saved})")
