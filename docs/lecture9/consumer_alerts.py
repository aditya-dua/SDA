# consumer_alerts.py
# Lecture 9 — Aggregations & Alert Routing with MongoDB
#
# Build progression:
#   Step 1: Buffer N price ticks per symbol, compute sum / mean / min / max
#   Step 2: Alert if min or max deviates > 5% from the previous close
#   Step 3: Route — all messages → stock_prices | alerts → stock_alerts
#
# Prerequisites:
#   pip install kafka-python pymongo
#   Replace MONGO_URI with your Atlas connection string from Lecture 8
#
# What must be running:
#   Terminal 1: Zookeeper  (docker compose up -d)
#   Terminal 2: Kafka      (docker compose up -d)
#   Terminal 3: python producer.py    (your Lecture 6 producer)
#   Terminal 4: python consumer_alerts.py   ← this file

from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime
import json, statistics

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI   = "mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"
BUFFER_SIZE = 5     # compute stats after every N ticks per symbol
ALERT_PCT   = 5.0   # alert if min or max moves > 5% from previous close

# ── MongoDB ───────────────────────────────────────────────────────────────────
client     = MongoClient(MONGO_URI)
db         = client["sda_course"]
prices_col = db["stock_prices"]   # every message (same collection as Lecture 8)
alerts_col = db["stock_alerts"]   # only flagged batches — created automatically

print("✅ Connected to MongoDB Atlas")
print(f"   Prices → {db.name}.{prices_col.name}")
print(f"   Alerts → {db.name}.{alerts_col.name}")
print("-" * 50)

# ── State ─────────────────────────────────────────────────────────────────────
price_buffer = defaultdict(list)  # {symbol: [p1, p2, ...]} rolling buffer
prev_close   = {}                  # {symbol: mean of last completed batch}

# ── Step 1: Aggregation helper ────────────────────────────────────────────────
def compute_stats(prices: list) -> dict:
    """Return sum, mean, min, max for a list of prices."""
    return {
        "count": len(prices),
        "sum":   round(sum(prices), 2),
        "mean":  round(statistics.mean(prices), 2),
        "min":   round(min(prices), 2),
        "max":   round(max(prices), 2),
    }

# ── Step 2: Alert rule ────────────────────────────────────────────────────────
def check_alert(symbol: str, stats: dict) -> list:
    """
    Compare this batch's min/max against the previous close.
    Returns a list of reason strings; empty list = no alert.
    """
    if symbol not in prev_close:
        return []  # not enough history yet

    pc      = prev_close[symbol]
    reasons = []

    min_dev = abs(stats["min"] - pc) / pc * 100
    max_dev = abs(stats["max"] - pc) / pc * 100

    if min_dev > ALERT_PCT:
        reasons.append(f"MIN_DEV {stats['min']:.2f} ({-min_dev:.1f}% vs close {pc:.2f})")
    if max_dev > ALERT_PCT:
        reasons.append(f"MAX_DEV {stats['max']:.2f} (+{max_dev:.1f}% vs close {pc:.2f})")

    return reasons

# ── Kafka Consumer ────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    "stock-topic",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("📡 Listening on stock-topic (Ctrl+C to stop)...\n")
total_saved = 0
total_alerts = 0

try:
    for msg in consumer:
        data   = msg.value
        symbol = data.get("symbol", "?")
        price  = float(data.get("price", data.get("Close", 0)))

        # ── Step 3a: Save every message to stock_prices (same as L8) ──────────
        prices_col.insert_one({
            **data,
            "_saved_at": datetime.utcnow().isoformat(),
            "_offset":   msg.offset,
        })
        total_saved += 1

        # ── Step 1: Accumulate price in buffer ─────────────────────────────────
        price_buffer[symbol].append(price)
        buf_len = len(price_buffer[symbol])
        print(f"   ↳ [{symbol}]  ₹{price:.2f}  buffer {buf_len}/{BUFFER_SIZE}")

        # ── When buffer is full: compute stats & check alert ───────────────────
        if buf_len >= BUFFER_SIZE:
            stats = compute_stats(price_buffer[symbol])
            print(
                f"📊 [{symbol}] Batch stats — "
                f"sum={stats['sum']}  mean={stats['mean']}  "
                f"min={stats['min']}  max={stats['max']}"
            )

            # Step 2: Evaluate alert rule
            reasons = check_alert(symbol, stats)

            if reasons:
                # Step 3b: Route alert to stock_alerts collection
                alerts_col.insert_one({
                    "symbol":        symbol,
                    "stats":         stats,
                    "prev_close":    prev_close.get(symbol),
                    "alert_reasons": reasons,
                    "alert_at":      datetime.utcnow(),
                    "status":        "open",   # open | reviewed | resolved
                })
                total_alerts += 1
                print(f"🚨 ALERT [{symbol}] → {' | '.join(reasons)}\n")
            else:
                print(f"✅ [{symbol}] No alert — within ±{ALERT_PCT}% of close {prev_close.get(symbol, 'N/A')}\n")

            # Update prev_close to this batch's mean, reset buffer
            prev_close[symbol] = stats["mean"]
            price_buffer[symbol] = []

except KeyboardInterrupt:
    print(f"\n⏹  Stopped.  Saved: {total_saved}  |  Alerts: {total_alerts}")
finally:
    consumer.close()
    client.close()
