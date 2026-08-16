from kafka import KafkaConsumer
import json

# ── consumer_stock.py ────────────────────────────────────────────────────────
# Reads from stock-topic — works with both tcs_producer.py and infy_producer.py
# running simultaneously (Lecture 7 demo)
# ─────────────────────────────────────────────────────────────────────────────

consumer = KafkaConsumer(
    'stock-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',        # 'latest' to only see new messages
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

tcs_count = 0
infy_count = 0

print("📡 Listening on stock-topic (TCS + INFY)...")
print("-" * 45)

for msg in consumer:
    data  = msg.value
    sym   = data.get('symbol', '?')
    price = data.get('price', data.get('Close', '?'))

    if sym == 'TCS.NS':
        tcs_count += 1
        print(f"🔵 TCS   ₹{price:<10}  (msg #{tcs_count})")
    elif sym == 'INFY.NS':
        infy_count += 1
        print(f"🟢 INFY  ₹{price:<10}  (msg #{infy_count})")
    else:
        print(f"⚪ {sym} → {data}")

    # Print summary every 5 total messages
    total = tcs_count + infy_count
    if total > 0 and total % 5 == 0:
        print(f"\n  📊 Summary so far: TCS={tcs_count}  INFY={infy_count}  Total={total}\n")
