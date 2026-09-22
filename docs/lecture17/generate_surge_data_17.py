"""
generate_surge_data_17.py
Lecture 17 — Multi-Source Surge Pricing Project

Creates three deterministic source files for a 60-second classroom scenario:
  - ride_requests_17.csv
  - driver_supply_17.csv
  - zone_context_17.csv

The scenario has three phases: normal operations, rain + an event ending,
and recovery. Event IDs include a run timestamp to avoid database conflicts.
"""

import csv
import random
from datetime import datetime


random.seed(17)

RUN_ID = datetime.now().strftime("%y%m%d%H%M%S")
ZONES = {
    "Airport": (19.0896, 72.8656),
    "Central Business District": (18.9256, 72.8242),
    "Railway Station": (18.9398, 72.8355),
    "Tech Park": (19.1176, 72.8647),
    "Residential": (19.0748, 72.8856),
}
BASE_REQUESTS = {
    "Airport": 2,
    "Central Business District": 2,
    "Railway Station": 1,
    "Tech Park": 2,
    "Residential": 1,
}
BASE_DRIVERS = {
    "Airport": 30,
    "Central Business District": 35,
    "Railway Station": 25,
    "Tech Park": 30,
    "Residential": 20,
}


def phase_for(second):
    if second < 20:
        return "NORMAL"
    if second < 45:
        return "SURGE"
    return "RECOVERY"


ride_rows = []
driver_rows = []
context_rows = []
ride_counter = 1
driver_counter = 1
context_counter = 1

for second in range(60):
    phase = phase_for(second)

    for zone, (lat, lon) in ZONES.items():
        request_count = BASE_REQUESTS[zone]
        available_drivers = BASE_DRIVERS[zone]
        occupied_drivers = random.randint(8, 18)
        rain = 0.05
        traffic = 0.35
        event = 0.0
        label = "Normal operations"

        if phase == "SURGE":
            rain = 0.80
            traffic = 0.65
            label = "Heavy rain"
            if zone == "Central Business District":
                request_count = 7
                available_drivers = 12
                traffic = 0.95
                event = 1.0
                label = "Concert ending + heavy rain"
            elif zone == "Railway Station":
                request_count = 4
                available_drivers = 10
                traffic = 0.82
                event = 0.70
                label = "Concert dispersal near station"
            elif zone == "Airport":
                request_count = 3
                available_drivers = 20
        elif phase == "RECOVERY":
            rain = 0.25
            traffic = 0.50
            label = "Demand recovering"
            if zone == "Central Business District":
                request_count = 3
                available_drivers = 24
                traffic = 0.68
                event = 0.25
            elif zone == "Railway Station":
                request_count = 2
                available_drivers = 18

        available_drivers += random.choice([-1, 0, 0, 1])

        for _ in range(request_count):
            ride_rows.append(
                {
                    "event_id": f"R17-{RUN_ID}-{ride_counter:05d}",
                    "scenario_second": second,
                    "phase": phase,
                    "zone": zone,
                    "rider_id": f"RIDER-{random.randint(1, 500):04d}",
                    "pickup_lat": f"{lat + random.uniform(-0.01, 0.01):.5f}",
                    "pickup_lon": f"{lon + random.uniform(-0.01, 0.01):.5f}",
                    "base_fare": f"{random.uniform(120, 650):.2f}",
                }
            )
            ride_counter += 1

        driver_rows.append(
            {
                "event_id": f"D17-{RUN_ID}-{driver_counter:05d}",
                "scenario_second": second,
                "phase": phase,
                "zone": zone,
                "available_drivers": max(1, available_drivers),
                "occupied_drivers": occupied_drivers,
            }
        )
        driver_counter += 1

        context_rows.append(
            {
                "event_id": f"C17-{RUN_ID}-{context_counter:05d}",
                "scenario_second": second,
                "phase": phase,
                "zone": zone,
                "rain_intensity": f"{rain:.2f}",
                "traffic_index": f"{traffic:.2f}",
                "event_intensity": f"{event:.2f}",
                "context_label": label,
            }
        )
        context_counter += 1


def write_csv(filename, rows):
    with open(filename, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


write_csv("ride_requests_17.csv", ride_rows)
write_csv("driver_supply_17.csv", driver_rows)
write_csv("zone_context_17.csv", context_rows)

print(f"Run ID: {RUN_ID}")
print(f"Ride requests : {len(ride_rows):,} -> ride_requests_17.csv")
print(f"Supply updates: {len(driver_rows):,} -> driver_supply_17.csv")
print(f"Context rows  : {len(context_rows):,} -> zone_context_17.csv")
print("Next: start the pricing consumer, then all three producers.")
