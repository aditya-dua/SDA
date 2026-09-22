"""Stream Lecture 17 ride-request demand events to Kafka."""

import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "ride-requests-l17"
CSV_FILE = "ride_requests_17.csv"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Demand producer: {CSV_FILE} -> {TOPIC}")
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
            f"[{scenario_second:02d}s] request {row['event_id']} "
            f"{row['zone']} INR {float(row['base_fare']):,.0f}"
        )

producer.flush()
producer.close()
print(f"Sent {count} ride requests.")
