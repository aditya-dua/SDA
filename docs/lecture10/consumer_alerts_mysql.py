"""
consumer_alerts_mysql.py
Lecture 10 — Banking Fraud Detection Pipeline
Streaming Data Analytics | MBA Course

Consumes from 'transactions-topic' and writes flagged transaction events to
MySQL's fraud_alerts table for the Lecture 16 live Grafana extension.

Prerequisites:
    pip install kafka-python mysql-connector-python

Run alongside transaction_producer.py and consumer_store_mysql.py.
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

CREATE_ALERTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(20) NOT NULL UNIQUE,
    user_id VARCHAR(20) NOT NULL,
    user_name VARCHAR(100) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    merchant VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    alert_type VARCHAR(40) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    message VARCHAR(255) NOT NULL,
    event_timestamp DATETIME NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

UPSERT_ALERT_SQL = """
INSERT INTO fraud_alerts (
    transaction_id, user_id, user_name, amount, merchant, city,
    alert_type, severity, message, event_timestamp
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    alert_type = VALUES(alert_type),
    severity = VALUES(severity),
    message = VALUES(message)
"""


def classify_alert(txn):
    """Map the generator's planted fraud patterns to classroom alert types."""
    amount = Decimal(txn["amount"])
    if txn["country"] != "India":
        return (
            "IMPOSSIBLE_TRAVEL",
            "critical",
            f"Card used in {txn['city']} shortly after domestic activity",
        )
    if amount <= Decimal("800"):
        return (
            "RAPID_FIRE",
            "warning",
            "Multiple low-value transactions detected in a short interval",
        )
    return (
        "UNUSUAL_AMOUNT",
        "critical",
        f"Transaction amount INR {amount:,.2f} is outside the expected pattern",
    )


def alert_values(txn):
    alert_type, severity, message = classify_alert(txn)
    return (
        txn["transaction_id"],
        txn["user_id"],
        txn["user_name"],
        Decimal(txn["amount"]),
        txn["merchant"],
        txn["city"],
        alert_type,
        severity,
        message,
        datetime.strptime(txn["timestamp"], "%Y-%m-%d %H:%M:%S"),
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
cursor.execute(CREATE_ALERTS_TABLE_SQL)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="mysql-alert-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print("MySQL alert consumer started")
print(f"   Kafka: {KAFKA_BROKER} -> {TOPIC}")
print(f"   MySQL: {MYSQL_HOST}:{MYSQL_PORT} -> {DB_NAME}.fraud_alerts")
print("-" * 70)

messages_seen = 0
alerts_saved = 0
try:
    for message in consumer:
        txn = message.value
        messages_seen += 1

        if txn.get("is_fraud", "false").lower() == "true":
            try:
                values = alert_values(txn)
                cursor.execute(UPSERT_ALERT_SQL, values)
                db.commit()
            except mysql.connector.Error:
                db.rollback()
                raise

            alerts_saved += 1
            print(
                f"ALERT [{alerts_saved:03d}] {values[6]:<20} "
                f"{txn['transaction_id']}  {txn['user_name']:<22} "
                f"INR {Decimal(txn['amount']):>10,.2f}"
            )

        consumer.commit()
except KeyboardInterrupt:
    print(
        f"\nStopped after reading {messages_seen} messages "
        f"and saving {alerts_saved} alerts."
    )
finally:
    consumer.close()
    cursor.close()
    db.close()
