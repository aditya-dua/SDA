"""
surge_pricing_consumer_17.py
Lecture 17 — Multi-Source Surge Pricing Project

Consumes demand, supply, and context topics; maintains a 10-second demand
window by zone; calculates an explainable multiplier; and stores source
events plus pricing decisions in MySQL.
"""

import json
import os
import time
from collections import defaultdict, deque
from decimal import Decimal

import mysql.connector
from kafka import KafkaConsumer

from surge_pricing_logic_17 import PricingInputs, calculate_surge


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "admin")
DB_NAME = "sda_course_l17"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
REQUEST_TOPIC = "ride-requests-l17"
SUPPLY_TOPIC = "driver-supply-l17"
CONTEXT_TOPIC = "zone-context-l17"
GROUP_ID = "surge-engine-l17"
DEMAND_WINDOW_SECONDS = 10

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS ride_requests (
        event_id VARCHAR(40) PRIMARY KEY,
        scenario_second INT NOT NULL,
        phase VARCHAR(20) NOT NULL,
        zone VARCHAR(60) NOT NULL,
        rider_id VARCHAR(20) NOT NULL,
        pickup_lat DECIMAL(9,5) NOT NULL,
        pickup_lon DECIMAL(9,5) NOT NULL,
        base_fare DECIMAL(10,2) NOT NULL,
        received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS driver_supply (
        event_id VARCHAR(40) PRIMARY KEY,
        scenario_second INT NOT NULL,
        phase VARCHAR(20) NOT NULL,
        zone VARCHAR(60) NOT NULL,
        available_drivers INT NOT NULL,
        occupied_drivers INT NOT NULL,
        received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS zone_context (
        event_id VARCHAR(40) PRIMARY KEY,
        scenario_second INT NOT NULL,
        phase VARCHAR(20) NOT NULL,
        zone VARCHAR(60) NOT NULL,
        rain_intensity DECIMAL(4,2) NOT NULL,
        traffic_index DECIMAL(4,2) NOT NULL,
        event_intensity DECIMAL(4,2) NOT NULL,
        context_label VARCHAR(120) NOT NULL,
        received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pricing_decisions (
        decision_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        trigger_event_id VARCHAR(40) NOT NULL UNIQUE,
        trigger_source VARCHAR(30) NOT NULL,
        scenario_second INT NOT NULL,
        phase VARCHAR(20) NOT NULL,
        zone VARCHAR(60) NOT NULL,
        demand_window INT NOT NULL,
        available_drivers INT NOT NULL,
        demand_supply_ratio DECIMAL(8,3) NOT NULL,
        rain_intensity DECIMAL(4,2) NOT NULL,
        traffic_index DECIMAL(4,2) NOT NULL,
        event_intensity DECIMAL(4,2) NOT NULL,
        demand_adjustment DECIMAL(6,3) NOT NULL,
        weather_adjustment DECIMAL(6,3) NOT NULL,
        traffic_adjustment DECIMAL(6,3) NOT NULL,
        event_adjustment DECIMAL(6,3) NOT NULL,
        raw_multiplier DECIMAL(4,2) NOT NULL,
        final_multiplier DECIMAL(4,2) NOT NULL,
        reason VARCHAR(255) NOT NULL,
        decision_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_pricing_zone_time (zone, decision_time)
    )
    """,
]

REQUEST_SQL = """
INSERT INTO ride_requests (
    event_id, scenario_second, phase, zone, rider_id,
    pickup_lat, pickup_lon, base_fare
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE phase = VALUES(phase)
"""
SUPPLY_SQL = """
INSERT INTO driver_supply (
    event_id, scenario_second, phase, zone,
    available_drivers, occupied_drivers
) VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE available_drivers = VALUES(available_drivers)
"""
CONTEXT_SQL = """
INSERT INTO zone_context (
    event_id, scenario_second, phase, zone,
    rain_intensity, traffic_index, event_intensity, context_label
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE context_label = VALUES(context_label)
"""
DECISION_SQL = """
INSERT INTO pricing_decisions (
    trigger_event_id, trigger_source, scenario_second, phase, zone,
    demand_window, available_drivers, demand_supply_ratio,
    rain_intensity, traffic_index, event_intensity,
    demand_adjustment, weather_adjustment, traffic_adjustment,
    event_adjustment, raw_multiplier, final_multiplier, reason
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    demand_window = VALUES(demand_window),
    available_drivers = VALUES(available_drivers),
    final_multiplier = VALUES(final_multiplier),
    reason = VALUES(reason)
"""


db = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
)
cursor = db.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")
for statement in SCHEMA_STATEMENTS:
    cursor.execute(statement)
cursor.execute("CREATE USER IF NOT EXISTS 'grafana'@'%' IDENTIFIED BY 'grafana'")
cursor.execute(f"GRANT SELECT ON {DB_NAME}.* TO 'grafana'@'%'")
db.commit()

consumer = KafkaConsumer(
    REQUEST_TOPIC,
    SUPPLY_TOPIC,
    CONTEXT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

request_times = defaultdict(deque)
latest_supply = {}
latest_context = {}
previous_multiplier = defaultdict(lambda: 1.0)

print("Lecture 17 surge-pricing engine started")
print(f"   Topics: {REQUEST_TOPIC}, {SUPPLY_TOPIC}, {CONTEXT_TOPIC}")
print(f"   MySQL : {DB_NAME}")
print("=" * 78)


def purge_old_requests(zone, now):
    cutoff = now - DEMAND_WINDOW_SECONDS
    while request_times[zone] and request_times[zone][0] < cutoff:
        request_times[zone].popleft()


def store_source_event(topic, event):
    if topic == REQUEST_TOPIC:
        cursor.execute(
            REQUEST_SQL,
            (
                event["event_id"],
                int(event["scenario_second"]),
                event["phase"],
                event["zone"],
                event["rider_id"],
                Decimal(event["pickup_lat"]),
                Decimal(event["pickup_lon"]),
                Decimal(event["base_fare"]),
            ),
        )
    elif topic == SUPPLY_TOPIC:
        cursor.execute(
            SUPPLY_SQL,
            (
                event["event_id"],
                int(event["scenario_second"]),
                event["phase"],
                event["zone"],
                int(event["available_drivers"]),
                int(event["occupied_drivers"]),
            ),
        )
    else:
        cursor.execute(
            CONTEXT_SQL,
            (
                event["event_id"],
                int(event["scenario_second"]),
                event["phase"],
                event["zone"],
                Decimal(event["rain_intensity"]),
                Decimal(event["traffic_index"]),
                Decimal(event["event_intensity"]),
                event["context_label"],
            ),
        )


def update_state(topic, event, now):
    zone = event["zone"]
    if topic == REQUEST_TOPIC:
        request_times[zone].append(now)
    elif topic == SUPPLY_TOPIC:
        latest_supply[zone] = {
            "available_drivers": int(event["available_drivers"]),
            "occupied_drivers": int(event["occupied_drivers"]),
        }
    else:
        latest_context[zone] = {
            "rain_intensity": float(event["rain_intensity"]),
            "traffic_index": float(event["traffic_index"]),
            "event_intensity": float(event["event_intensity"]),
            "context_label": event["context_label"],
        }
    purge_old_requests(zone, now)


def store_pricing_decision(topic, event):
    zone = event["zone"]
    if zone not in latest_supply or zone not in latest_context:
        return None

    context = latest_context[zone]
    inputs = PricingInputs(
        demand_window=len(request_times[zone]),
        available_drivers=latest_supply[zone]["available_drivers"],
        rain_intensity=context["rain_intensity"],
        traffic_index=context["traffic_index"],
        event_intensity=context["event_intensity"],
    )
    decision = calculate_surge(inputs, previous_multiplier[zone])
    previous_multiplier[zone] = decision.final_multiplier

    cursor.execute(
        DECISION_SQL,
        (
            event["event_id"],
            topic,
            int(event["scenario_second"]),
            event["phase"],
            zone,
            inputs.demand_window,
            inputs.available_drivers,
            decision.demand_supply_ratio,
            inputs.rain_intensity,
            inputs.traffic_index,
            inputs.event_intensity,
            decision.demand_adjustment,
            decision.weather_adjustment,
            decision.traffic_adjustment,
            decision.event_adjustment,
            decision.raw_multiplier,
            decision.final_multiplier,
            decision.reason,
        ),
    )
    return decision


try:
    for message in consumer:
        event = message.value
        now = time.time()
        try:
            store_source_event(message.topic, event)
            update_state(message.topic, event, now)
            decision = store_pricing_decision(message.topic, event)
            db.commit()
            consumer.commit()
        except mysql.connector.Error:
            db.rollback()
            raise

        if decision:
            print(
                f"[{event['scenario_second']:>2}s] {event['zone']:<27} "
                f"demand={len(request_times[event['zone']]):>2} "
                f"supply={latest_supply[event['zone']]['available_drivers']:>2} "
                f"ratio={decision.demand_supply_ratio:>5.2f} "
                f"surge={decision.final_multiplier:.2f}x "
                f"({decision.reason})"
            )
except KeyboardInterrupt:
    print("\nPricing engine stopped.")
finally:
    consumer.close()
    cursor.close()
    db.close()
