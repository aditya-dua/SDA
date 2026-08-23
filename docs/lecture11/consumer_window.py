# consumer_window.py
# Lecture 11 — Windowing & Aggregations
# Reads from transactions-topic and demonstrates:
#   1. Tumbling window — total spend per 60-second bucket
#   2. Sliding window  — rolling 3-minute average, updated every 30 seconds
#
# Pre-requisites:
#   - Docker + Kafka running (docker compose up -d)
#   - transactions-topic populated (run generate_transactions.py + transaction_producer.py from Lecture 10)
#   - pip install kafka-python

from kafka import KafkaConsumer
import json
import time
import collections

consumer = KafkaConsumer(
    'transactions-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# ── Tumbling window state ──────────────────────────────────────────────────────
TW_SIZE  = 60          # window duration in seconds
tw_start = time.time()
tw_total = 0.0
tw_count = 0

# ── Sliding window state ───────────────────────────────────────────────────────
SW_SIZE   = 180        # 3-minute window
SW_STEP   = 30         # print rolling average every 30 seconds
sw_events = collections.deque()   # stores (wall_clock_ts, amount) pairs
sw_last   = time.time()

print("=== Windowed Consumer Starting ===")
print(f"Tumbling : {TW_SIZE}s buckets")
print(f"Sliding  : {SW_SIZE}s window / {SW_STEP}s update interval")
print("─" * 60)

for msg in consumer:
    data   = msg.value
    amount = float(data.get('amount', 0))
    city   = data.get('city', '?')
    now    = time.time()

    # ── TUMBLING WINDOW ────────────────────────────────────────────────────────
    tw_total += amount
    tw_count += 1

    if now - tw_start >= TW_SIZE:
        avg = tw_total / tw_count if tw_count else 0
        print(
            f"[TUMBLING] {tw_count:>4} events | "
            f"Total: ₹{tw_total:>12,.2f} | "
            f"Avg: ₹{avg:>9,.2f}"
        )
        tw_total  = 0.0
        tw_count  = 0
        tw_start  = now

    # ── SLIDING WINDOW ─────────────────────────────────────────────────────────
    sw_events.append((now, amount))

    # Evict events that have fallen outside the window
    cutoff = now - SW_SIZE
    while sw_events and sw_events[0][0] < cutoff:
        sw_events.popleft()

    # Print every SW_STEP seconds
    if now - sw_last >= SW_STEP and sw_events:
        amounts     = [e[1] for e in sw_events]
        rolling_avg = sum(amounts) / len(amounts)
        print(
            f"[SLIDING]  {len(amounts):>4} events in last {SW_SIZE}s | "
            f"Rolling Avg: ₹{rolling_avg:>9,.2f}"
        )
        sw_last = now
