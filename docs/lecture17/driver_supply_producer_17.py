"""Stream Lecture 17 driver-supply updates to Kafka."""

import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "driver-supply-l17"
CSV_FILE = "driver_supply_17.csv"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Supply producer: {CSV_FILE} -> {TOPIC}")
last_second = None
count = 0
with open(CSV_FILE, newline="") as csv_file:
    for row in csv.DictReader(csv_file):
        scenario_second = int(row["scenario_second"])
        if last_second is not None and scenario_second != last_second:
            time.sleep(1)
        row["produced_at"] = datetime.now(timezone.utc).isoformat()
        producer.send(TOPIC, value=row)
        last_second = scenario_second
        count += 1
        print(
            f"[{scenario_second:02d}s] supply {row['zone']}: "
            f"{row['available_drivers']} available"
        )

producer.flush()
producer.close()
print(f"Sent {count} driver-supply updates.")
