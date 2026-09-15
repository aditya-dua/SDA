"""
consumer_store_mysql_16.py
Lecture 16 Extension — Live Fraud Dashboard

Consumes every Lecture 16 transaction and writes it to
sda_course_l16.transactions.
"""

import json
import os
from datetime import datetime
from decimal import Decimal

import mysql.connector
from kafka import KafkaConsumer


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "admin")
DB_NAME = "sda_course_l16"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "transactions-topic-l16"
GROUP_ID = "mysql-store-l16-group"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    user_id VARCHAR(20),
    user_name VARCHAR(100),
    card_last4 VARCHAR(4),
    amount DECIMAL(12,2),
    merchant VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    lat DECIMAL(9,4),
    lon DECIMAL(9,4),
    `timestamp` DATETIME,
    is_fraud VARCHAR(5),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

UPSERT_SQL = """
INSERT INTO transactions (
    transaction_id, user_id, user_name, card_last4, amount, merchant,
    city, country, lat, lon, `timestamp`, is_fraud
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    user_id = VALUES(user_id),
    user_name = VALUES(user_name),
    card_last4 = VALUES(card_last4),
    amount = VALUES(amount),
    merchant = VALUES(merchant),
    city = VALUES(city),
    country = VALUES(country),
    lat = VALUES(lat),
    lon = VALUES(lon),
    `timestamp` = VALUES(`timestamp`),
    is_fraud = VALUES(is_fraud)
"""


def transaction_values(txn):
    return (
        txn["transaction_id"],
        txn["user_id"],
        txn["user_name"],
        txn["card_last4"],
        Decimal(txn["amount"]),
        txn["merchant"],
        txn["city"],
        txn["country"],
        Decimal(txn["lat"]),
        Decimal(txn["lon"]),
        datetime.strptime(txn["timestamp"], "%Y-%m-%d %H:%M:%S"),
        txn["is_fraud"].lower(),
    )


db = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
)
cursor = db.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")
cursor.execute(CREATE_TABLE_SQL)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print("Lecture 16 MySQL storage consumer started")
print(f"   Kafka: {TOPIC} ({GROUP_ID})")
print(f"   MySQL: {DB_NAME}.transactions")
print("-" * 70)

count = 0
try:
    for message in consumer:
        txn = message.value
        try:
            cursor.execute(UPSERT_SQL, transaction_values(txn))
            db.commit()
            consumer.commit()
        except mysql.connector.Error:
            db.rollback()
            raise

        count += 1
        print(
            f"[{count:03d}] Saved {txn['transaction_id']} "
            f"{txn['user_name']:<16} INR {Decimal(txn['amount']):>10,.2f}"
        )
except KeyboardInterrupt:
    print(f"\nStopped after saving {count} transactions.")
finally:
    consumer.close()
    cursor.close()
    db.close()
