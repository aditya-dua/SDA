# consumer_basic.py
# Lecture 7 — Streaming Data Analytics (MBA)
# Level 1: Read and print every message from a Kafka topic

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',   # 'earliest' = read from beginning
                                    # 'latest'   = only new messages
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("👂 Listening for messages...\n")

for message in consumer:
    order = message.value
    print(
        f"[Offset {message.offset}]  "
        f"Order: {order.get('order_id', 'N/A')} | "
        f"Product: {order.get('product', 'N/A'):20s} | "
        f"Amount: ${order.get('amount', 0):.2f}"
    )
