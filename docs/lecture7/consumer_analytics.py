# consumer_analytics.py
# Lecture 7 — Streaming Data Analytics (MBA)
# Level 2: Running analytics — revenue, top products, top spenders

from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ── Analytics State ───────────────────────────────────────────
total_orders   = 0
total_revenue  = 0.0
product_counts = {}    # { "Laptop": 5, "Phone": 3, ... }
user_spending  = {}    # { "USR-123": 450.00, ... }
start_time     = time.time()

print("📊 Streaming Analytics Dashboard")
print("=" * 48)

for message in consumer:
    order  = message.value
    amount = order.get('amount', 0)

    # ── Update Running Totals ─────────────────────────────────
    total_orders  += 1
    total_revenue += amount

    product = order.get('product', 'Unknown')
    product_counts[product] = product_counts.get(product, 0) + 1

    user = order.get('user_id', 'unknown')
    user_spending[user] = user_spending.get(user, 0) + amount

    # ── Print Summary Every 5 Orders ─────────────────────────
    if total_orders % 5 == 0:
        elapsed     = time.time() - start_time
        avg_order   = total_revenue / total_orders
        top_product = max(product_counts, key=product_counts.get)
        top_spender = max(user_spending,  key=user_spending.get)

        print(f"\n  ── Summary after {total_orders} orders ──────────────────")
        print(f"  💰 Total Revenue  : ${total_revenue:,.2f}")
        print(f"  📦 Average Order  : ${avg_order:.2f}")
        print(f"  🏆 Top Product    : {top_product}  ({product_counts[top_product]} sold)")
        print(f"  👤 Top Spender    : {top_spender}  (${user_spending[top_spender]:.2f} total)")
        print(f"  ⚡ Throughput     : {total_orders / elapsed:.1f} orders/sec")
        print(f"  ──────────────────────────────────────────────────────")
