"""
consumer_fraud.py
Lecture 10 — Banking Fraud Detection Pipeline
Streaming Data Analytics | MBA Course

Consumes from 'transactions-topic' and applies THREE fraud rules:

  Rule 1 — Impossible Travel
      Distance between last known location and current location > 500 km
      AND time elapsed < 60 minutes → physically impossible to travel

  Rule 2 — Unusual Amount
      Current transaction amount > 3x user's historical average spend

  Rule 3 — Rapid Fire (Card Cloning Signal)
      3 or more transactions from the same user in the last 60 seconds

Terminal 3 (run alongside consumer_store.py)

UPDATE the MONGO_URI below before running.
"""

import json
import math
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# CONFIG — update MONGO_URI with your Atlas connection string
# ---------------------------------------------------------------------------
MONGO_URI    = "mongodb+srv://your-user:your-pass@cluster0.xxxxx.mongodb.net/"
DB_NAME      = "sda_course"
KAFKA_BROKER = "localhost:9092"
TOPIC        = "transactions-topic"

# Fraud thresholds (business decisions — change and discuss impact)
IMPOSSIBLE_TRAVEL_KM  = 500    # flag if moved this far
IMPOSSIBLE_TRAVEL_MIN = 60     # within this many minutes
UNUSUAL_AMOUNT_MULT   = 3.0    # flag if amount > X * avg
RAPID_FIRE_COUNT      = 3      # flag if this many txns in 60s

# ---------------------------------------------------------------------------
# Haversine formula — great-circle distance between two GPS coordinates
# ---------------------------------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam/2)**2
    return R * 2 * math.asin(math.sqrt(a))

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
client   = MongoClient(MONGO_URI)
db       = client[DB_NAME]
txns     = db["transactions"]
profiles = db["user_profiles"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='fraud-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🔍 Fraud Detection Consumer started")
print(f"   Rules: Impossible Travel >{IMPOSSIBLE_TRAVEL_KM}km in <{IMPOSSIBLE_TRAVEL_MIN}min  |  "
      f"Unusual Amount >{UNUSUAL_AMOUNT_MULT}x avg  |  "
      f"Rapid Fire ≥{RAPID_FIRE_COUNT} txns/60s")
print("=" * 70)

count = 0
for msg in consumer:
    txn     = msg.value
    user_id = txn["user_id"]
    amount  = float(txn["amount"])
    cur_lat = float(txn["lat"])
    cur_lon = float(txn["lon"])
    cur_ts  = datetime.strptime(txn["timestamp"], "%Y-%m-%d %H:%M:%S")

    alerts = []

    # ------------------------------------------------------------------
    # Fetch user profile from MongoDB (last known location + spend stats)
    # ------------------------------------------------------------------
    profile = profiles.find_one({"user_id": user_id})

    if profile:

        # Rule 1 — Impossible Travel
        last_lat  = profile.get("last_lat")
        last_lon  = profile.get("last_lon")
        last_ts_str = profile.get("last_timestamp")

        if last_lat and last_lon and last_ts_str:
            distance = haversine(last_lat, last_lon, cur_lat, cur_lon)
            last_ts  = datetime.strptime(last_ts_str, "%Y-%m-%d %H:%M:%S")
            minutes  = (cur_ts - last_ts).total_seconds() / 60

            if distance > IMPOSSIBLE_TRAVEL_KM and 0 < minutes < IMPOSSIBLE_TRAVEL_MIN:
                alerts.append({
                    "rule": "IMPOSSIBLE TRAVEL",
                    "detail": (
                        f"Last seen in {profile['last_city']} "
                        f"({last_lat:.2f}°, {last_lon:.2f}°) at {last_ts_str}\n"
                        f"        Now at    {txn['city']} "
                        f"({cur_lat:.2f}°, {cur_lon:.2f}°) at {txn['timestamp']}\n"
                        f"        Distance  {distance:,.0f} km in {minutes:.0f} min"
                    )
                })

        # Rule 2 — Unusual Amount
        txn_count   = profile.get("txn_count", 0)
        total_spend = profile.get("total_spend", 0)
        if txn_count > 5:
            avg_spend = total_spend / txn_count
            if amount > avg_spend * UNUSUAL_AMOUNT_MULT:
                alerts.append({
                    "rule": "UNUSUAL AMOUNT",
                    "detail": (
                        f"Avg spend ₹{avg_spend:,.2f} over {txn_count} transactions\n"
                        f"        This txn  ₹{amount:,.2f} at {txn['merchant']} "
                        f"({amount/avg_spend:.1f}x average)"
                    )
                })

        # Rule 3 — Rapid Fire
        one_minute_ago = cur_ts - timedelta(seconds=60)
        recent_count = txns.count_documents({
            "user_id":    user_id,
            "timestamp":  {"$gte": one_minute_ago.strftime("%Y-%m-%d %H:%M:%S")}
        })
        if recent_count >= RAPID_FIRE_COUNT:
            recent_txns = list(txns.find(
                {"user_id": user_id,
                 "timestamp": {"$gte": one_minute_ago.strftime("%Y-%m-%d %H:%M:%S")}},
                {"_id": 0, "transaction_id": 1, "amount": 1, "merchant": 1}
            ).limit(5))
            detail_lines = "  |  ".join(
                f"{t['transaction_id']} ₹{float(t['amount']):,.0f} @ {t['merchant']}"
                for t in recent_txns
            )
            alerts.append({
                "rule": "RAPID FIRE",
                "detail": (
                    f"{recent_count} transactions in the last 60 seconds\n"
                    f"        {detail_lines}"
                )
            })

    count += 1

    if alerts:
        print()
        print(f"🚨 {'  🚨  '.join(a['rule'] for a in alerts)}")
        print(f"   Transaction : {txn['transaction_id']}")
        print(f"   User        : {txn['user_name']} ({user_id}) · card ···{txn['card_last4']}")
        print(f"   Amount      : ₹{amount:,.2f}  at  {txn['merchant']}")
        for a in alerts:
            print(f"   [{a['rule']}]")
            print(f"        {a['detail']}")
        print(f"   ⛔ ACTION : HOLD TRANSACTION")
        print("-" * 70)
    else:
        print(f"✅ [{count:04d}] APPROVED  {txn['transaction_id']}  "
              f"{txn['user_name']:<22} {txn['city']:<12} ₹{amount:>10,.2f}")
