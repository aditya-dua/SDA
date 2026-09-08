"""
consumer_store_mysql.py
Lecture 10 — Banking Fraud Detection Pipeline
Streaming Data Analytics | MBA Course

Consumes from 'transactions-topic' and writes each transaction to MySQL.
The table matches the schema used by the Lecture 14 Grafana dashboard.

Prerequisites:
    pip install kafka-python mysql-connector-python

Start the Lecture 14 MySQL container before running this consumer.
Override the classroom defaults with MYSQL_HOST, MYSQL_PORT, MYSQL_USER,
MYSQL_PASSWORD, or KAFKA_BROKER environment variables when needed.
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
DB_NAME = "sda_course"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "transactions-topic"

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
    is_fraud VARCHAR(5)
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
    """Convert Kafka's CSV strings into values suitable for MySQL."""
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
    group_id="mysql-store-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print("MySQL storage consumer started")
print(f"   Kafka: {KAFKA_BROKER} -> {TOPIC}")
print(f"   MySQL: {MYSQL_HOST}:{MYSQL_PORT} -> {DB_NAME}.transactions")
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
            f"[{count:04d}] Saved {txn['transaction_id']}  "
            f"{txn['user_name']:<22} {txn['city']:<12} "
            f"INR {Decimal(txn['amount']):>10,.2f}"
        )
except KeyboardInterrupt:
    print(f"\nStopped after saving {count} transactions.")
finally:
    consumer.close()
    cursor.close()
    db.close()
