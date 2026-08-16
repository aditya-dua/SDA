# consumer_alerts.py
# Lecture 7 — Streaming Data Analytics (MBA)
# Level 3: Trigger-based consumer — act when a condition is met

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',          # only NEW orders from now
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ── Alert Rules — change these to match your business ────────
HIGH_VALUE_THRESHOLD = 1000.00
WATCHED_PRODUCTS     = ["Laptop Pro", "4K Monitor"]
VIP_SPEND_THRESHOLD  = 3000.00

print("🚨 Alert Monitor Running  (Ctrl+C to stop)\n")

# Track cumulative spend per user for VIP detection
user_spending = {}

for message in consumer:
    order   = message.value
    amount  = order.get('amount', 0)
    product = order.get('product', '')
    user    = order.get('user_id', '')

    user_spending[user] = user_spending.get(user, 0) + amount

    # ── Alert 1: Single high-value order ─────────────────────
    if amount >= HIGH_VALUE_THRESHOLD:
        print(f"🔔 HIGH VALUE ORDER")
        print(f"   User: {user}  |  Product: {product}  |  Amount: ${amount:.2f}\n")

    # ── Alert 2: Watched product sold ─────────────────────────
    if product in WATCHED_PRODUCTS:
        print(f"📌 WATCHED PRODUCT SOLD")
        print(f"   {product} purchased by {user}  (${amount:.2f})\n")

    # ── Alert 3: VIP spender threshold reached ─────────────────
    if user_spending[user] >= VIP_SPEND_THRESHOLD:
        if user_spending[user] - amount < VIP_SPEND_THRESHOLD:
            # Only fire once when threshold is first crossed
            print(f"⭐ VIP SPENDER DETECTED")
            print(f"   {user} has now spent ${user_spending[user]:,.2f} in total\n")
