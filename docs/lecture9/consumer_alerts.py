# consumer_alerts.py
# Lecture 9 — Smarter Consumers: Alert Routing with MongoDB
#
# Extends consumer_mongodb.py from Lecture 8:
#   - Reads from stock-topic (same as L8)
#   - Saves EVERY message to 'stock_prices' (same as L8)
#   - NEW: evaluates alert rules and saves flagged events to 'stock_alerts'
#
# Prerequisites:
#   pip install kafka-python pymongo
#   Replace MONGO_URI with your Atlas connection string from L8
#
# Run order:
#   Terminal 1: python consumer_alerts.py
#   Terminal 2: python producer.py   (your L6 producer)
#   Terminal 3: python check_alerts.py  (optional — check anytime)

from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import json

# ── MongoDB setup (same connection string as L8) ──────────────────────────────
MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"

client     = MongoClient(MONGO_URI)
db         = client["sda_course"]
prices_col = db["stock_prices"]    # same collection as L8 — no data loss
alerts_col = db["stock_alerts"]    # NEW — created automatically on first insert

print("✅ Connected to MongoDB Atlas")
print(f"   Prices → {db.name}.{prices_col.name}")
print(f"   Alerts → {db.name}.{alerts_col.name}")
print("-" * 45)

# ── Alert state (stateful — lives in memory) ──────────────────────────────────
last_price = {}   # remembers last price per symbol: {"INFY": 1423.50, ...}

# ── Alert thresholds (adjust these to match your data) ───────────────────────
PRICE_FLOORS = {
    "INFY":    1400,   # alert if INFY price drops below ₹1,400
    "INFY.NS": 1400,
    "TCS":     3200,   # alert if TCS price drops below ₹3,200
    "TCS.NS":  3200,
}
RAPID_MOVE_PCT = 1.5  # alert if price changes more than 1.5% from previous tick

def get_alert_reasons(symbol: str, price: float) -> list:
    """
    Evaluate both stateless and stateful alert rules.
    Returns a list of triggered reason strings.
    Empty list = no alert.
    """
    reasons = []

    # Rule 1 — Stateless: price below floor
    if symbol in PRICE_FLOORS and price < PRICE_FLOORS[symbol]:
        reasons.append(f"BELOW_FLOOR:{symbol}<{PRICE_FLOORS[symbol]}")

    # Rule 2 — Stateful: rapid movement vs last tick
    if symbol in last_price and last_price[symbol] > 0:
        pct = (price - last_price[symbol]) / last_price[symbol] * 100
        if abs(pct) > RAPID_MOVE_PCT:
            reasons.append(f"RAPID_MOVE:{pct:+.2f}%")

    # Always update last price regardless of whether alert fired
    last_price[symbol] = price
    return reasons

# ── Kafka consumer ─────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    'stock-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',      # pick up from now (history already in L8's stock_prices)
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📡 Listening on stock-topic (Ctrl+C to stop)...\n")
saved   = 0
alerted = 0

try:
    for msg in consumer:
        data   = msg.value
        symbol = data.get('symbol', '?')
        price  = float(data.get('price', data.get('Close', 0)))

        # Step 1 — Save to stock_prices (every message, same as L8)
        prices_col.insert_one({
            **data,
            '_saved_at':        datetime.utcnow().isoformat(),
            '_kafka_offset':    msg.offset,
            '_kafka_partition': msg.partition,
        })
        saved += 1

        # Step 2 — Evaluate alert rules
        reasons = get_alert_reasons(symbol, price)

        if reasons:
            alerts_col.insert_one({
                'symbol':        symbol,
                'price':         price,
                'alert_reasons': reasons,
                'alert_at':      datetime.utcnow(),
                'status':        'open',    # open | reviewed | resolved
            })
            alerted += 1
            print(f"🚨 ALERT [{symbol}] ₹{price:<10.2f} → {', '.join(reasons)}")
        else:
            print(f"💾 Saved [{symbol}] ₹{price:<10.2f}  (offset {msg.offset})")

except KeyboardInterrupt:
    print(f"\n⏹️  Stopped.  Total saved: {saved}  |  Alerts: {alerted}")
finally:
    consumer.close()
    client.close()
