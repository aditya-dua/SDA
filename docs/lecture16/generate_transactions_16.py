"""
generate_transactions_16.py
Lecture 16 Extension — Live Fraud Dashboard

Creates a compact, deterministic banking dataset for the live classroom demo.
Output: transactions_16.csv
"""

import csv
import random
from datetime import datetime, timedelta


random.seed(16)

OUTPUT_FILE = "transactions_16.csv"
USERS = [
    ("USR-161", "Aarav Sharma", "4521"),
    ("USR-162", "Priya Mehta", "7834"),
    ("USR-163", "Rohan Gupta", "2290"),
    ("USR-164", "Anjali Singh", "9012"),
    ("USR-165", "Vikram Nair", "3345"),
    ("USR-166", "Kavya Reddy", "6678"),
    ("USR-167", "Arjun Kumar", "1123"),
    ("USR-168", "Sneha Patel", "8856"),
    ("USR-169", "Sunita Verma", "4490"),
    ("USR-170", "Rahul Joshi", "7723"),
]
CITIES = {
    "Mumbai": ("India", 19.0760, 72.8770),
    "Delhi": ("India", 28.6139, 77.2090),
    "Bengaluru": ("India", 12.9716, 77.5946),
    "Hyderabad": ("India", 17.3850, 78.4867),
    "Dubai": ("UAE", 25.2048, 55.2708),
}
MERCHANTS = [
    "Swiggy",
    "Zomato",
    "Amazon",
    "Flipkart",
    "DMart",
    "BookMyShow",
    "Ola",
    "Apollo Pharmacy",
]
BASE_TIME = datetime(2025, 8, 16, 9, 0, 0)


def make_row(user, amount, merchant, city, offset_seconds, is_fraud):
    user_id, user_name, card_last4 = user
    country, lat, lon = CITIES[city]
    return {
        "transaction_id": "",
        "user_id": user_id,
        "user_name": user_name,
        "card_last4": card_last4,
        "amount": f"{amount:.2f}",
        "merchant": merchant,
        "city": city,
        "country": country,
        "lat": f"{lat:.4f}",
        "lon": f"{lon:.4f}",
        "timestamp": (BASE_TIME + timedelta(seconds=offset_seconds)).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "is_fraud": "true" if is_fraud else "false",
    }


rows = []
home_city = {
    user[0]: random.choice(["Mumbai", "Delhi", "Bengaluru", "Hyderabad"])
    for user in USERS
}

# Normal activity distributed across the demo sequence.
for _ in range(180):
    user = random.choice(USERS)
    rows.append(
        make_row(
            user,
            random.uniform(150, 12000),
            random.choice(MERCHANTS),
            home_city[user[0]],
            random.randint(0, 3600),
            False,
        )
    )

# Ten international transactions model impossible-travel alerts.
for user in USERS:
    rows.append(
        make_row(
            user,
            random.uniform(8000, 40000),
            "Dubai Duty Free",
            "Dubai",
            random.randint(300, 3300),
            True,
        )
    )

# Ten large domestic transactions model unusual-amount alerts.
for user in USERS:
    rows.append(
        make_row(
            user,
            random.uniform(55000, 125000),
            "Luxury Retail",
            home_city[user[0]],
            random.randint(300, 3300),
            True,
        )
    )

# Five rapid-fire sequences: first event is normal, next three are alerts.
for user in USERS[:5]:
    start = random.randint(600, 3000)
    for position in range(4):
        rows.append(
            make_row(
                user,
                random.uniform(200, 750),
                random.choice(MERCHANTS),
                home_city[user[0]],
                start + position * 15,
                position > 0,
            )
        )

rows.sort(key=lambda row: row["timestamp"])
for index, row in enumerate(rows, 1):
    row["transaction_id"] = f"L16-TXN-{index:04d}"

fieldnames = [
    "transaction_id",
    "user_id",
    "user_name",
    "card_last4",
    "amount",
    "merchant",
    "city",
    "country",
    "lat",
    "lon",
    "timestamp",
    "is_fraud",
]
with open(OUTPUT_FILE, "w", newline="") as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

flagged = sum(row["is_fraud"] == "true" for row in rows)
print(f"Generated {len(rows)} transactions -> {OUTPUT_FILE}")
print(f"Normal: {len(rows) - flagged} | Flagged: {flagged}")
print("Run next: python transaction_producer_16.py")
