"""
generate_transactions.py
Lecture 10 — End-to-End Capstone: Banking Fraud Detection
Streaming Data Analytics | MBA Course

Generates a 2000-row synthetic banking transaction dataset with
~5% deliberately planted fraud patterns for classroom use.

Usage:
    python generate_transactions.py
Output:
    transactions.csv
"""

import csv
import random
import math
from datetime import datetime, timedelta

random.seed(42)

# ---------------------------------------------------------------------------
# 50 Synthetic Users
# ---------------------------------------------------------------------------
USERS = [
    ("USR-001","Aarav Sharma","4521"),   ("USR-002","Priya Mehta","7834"),
    ("USR-003","Rohan Gupta","2290"),    ("USR-004","Anjali Singh","9012"),
    ("USR-005","Vikram Nair","3345"),    ("USR-006","Kavya Reddy","6678"),
    ("USR-007","Arjun Kumar","1123"),    ("USR-008","Sneha Patel","8856"),
    ("USR-009","Sunita Verma","4490"),   ("USR-010","Rahul Joshi","7723"),
    ("USR-011","Deepika Rao","2234"),    ("USR-012","Manish Iyer","5567"),
    ("USR-013","Pooja Pillai","8890"),   ("USR-014","Nikhil Bose","1112"),
    ("USR-015","Ananya Das","4445"),     ("USR-016","Karthik Menon","7778"),
    ("USR-017","Revathi Nair","2221"),   ("USR-018","Siddharth Shah","5554"),
    ("USR-019","Lavanya Iyer","8887"),   ("USR-020","Aditya Verma","1120"),
    ("USR-021","Meera Krishnan","4453"), ("USR-022","Tarun Agarwal","7786"),
    ("USR-023","Ishaan Chopra","2219"),  ("USR-024","Riya Malhotra","5552"),
    ("USR-025","Varun Tiwari","8885"),   ("USR-026","Nisha Pillai","1118"),
    ("USR-027","Gaurav Pandey","4447"),  ("USR-028","Shweta Mishra","7780"),
    ("USR-029","Rahul Srivastava","2213"),("USR-030","Pallavi Desai","5546"),
    ("USR-031","Kunal Jain","8879"),     ("USR-032","Preeti Yadav","1104"),
    ("USR-033","Vivek Khanna","4433"),   ("USR-034","Smita Banerjee","7766"),
    ("USR-035","Harish Dubey","2205"),   ("USR-036","Divya Kapoor","5538"),
    ("USR-037","Suresh Nambiar","8871"), ("USR-038","Tanya Bajaj","1096"),
    ("USR-039","Mohit Soni","4419"),     ("USR-040","Ritika Ahuja","7752"),
    ("USR-041","Ajay Chauhan","2197"),   ("USR-042","Bhavna Thakur","5530"),
    ("USR-043","Vinod Saxena","8863"),   ("USR-044","Jyoti Rastogi","1088"),
    ("USR-045","Naveen Rao","4405"),     ("USR-046","Poonam Shukla","7738"),
    ("USR-047","Sandeep Ghosh","2189"),  ("USR-048","Alka Mehra","5522"),
    ("USR-049","Rajesh Kumar","8855"),   ("USR-050","Sundar Rajan","1080"),
]

# ---------------------------------------------------------------------------
# City reference data (Indian + international for fraud rows)
# ---------------------------------------------------------------------------
CITIES = {
    "Mumbai":    ("India",  19.076,  72.877),
    "Delhi":     ("India",  28.613,  77.209),
    "Bengaluru": ("India",  12.971,  77.594),
    "Hyderabad": ("India",  17.385,  78.486),
    "Chennai":   ("India",  13.082,  80.270),
    "Kolkata":   ("India",  22.572,  88.363),
    "Pune":      ("India",  18.520,  73.856),
    "Ahmedabad": ("India",  23.022,  72.571),
    # International (used only in fraud rows)
    "London":    ("UK",     51.507,  -0.127),
    "Singapore": ("Singapore", 1.352, 103.819),
    "Dubai":     ("UAE",    25.204,  55.270),
}

DOMESTIC_CITIES = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Kolkata","Pune","Ahmedabad"]

MERCHANTS = [
    "Swiggy","Zomato","Amazon","Flipkart","BigBasket","DMart",
    "BookMyShow","MakeMyTrip","IRCTC","Ola","Uber","Rapido",
    "Cafe Coffee Day","McDonald's","Domino's","KFC",
    "Reliance Digital","Croma","Apple Store",
    "Gold Palace Jewellers","SBI ATM","HDFC ATM",
    "Apollo Pharmacy","Myntra","Nykaa","Ajio",
]

def random_amount(merchant, base_spend):
    """Amounts vary by merchant category."""
    if "ATM" in merchant:
        return round(random.choice([2000,5000,10000,20000]), 2)
    if "Jewellers" in merchant:
        return round(random.uniform(15000, 150000), 2)
    if merchant in ["Amazon","Flipkart","Reliance Digital","Croma"]:
        return round(random.uniform(500, 25000), 2)
    if merchant in ["Swiggy","Zomato","Domino's","KFC","McDonald's","Cafe Coffee Day"]:
        return round(random.uniform(150, 1200), 2)
    return round(random.uniform(base_spend * 0.3, base_spend * 2.5), 2)

def make_timestamp(base_dt, offset_minutes):
    return (base_dt + timedelta(minutes=offset_minutes)).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# Build the transaction list
# ---------------------------------------------------------------------------
transactions = []
txn_id_counter = 1
fraud_count = 0

# Assign each user a home city and typical spend range
user_home = {u[0]: random.choice(DOMESTIC_CITIES) for u in USERS}
user_avg  = {u[0]: random.uniform(600, 4000) for u in USERS}

base_date = datetime(2025, 8, 1, 8, 0, 0)

# 1900 legitimate transactions
for _ in range(1900):
    user_id, name, card = random.choice(USERS)
    city = user_home[user_id]
    country, lat, lon = CITIES[city]
    merchant = random.choice(MERCHANTS)
    amount = random_amount(merchant, user_avg[user_id])
    offset = random.randint(0, 43200)   # spread over 30 days

    transactions.append({
        "transaction_id": f"TXN-{txn_id_counter:05d}",
        "user_id": user_id,
        "user_name": name,
        "card_last4": card,
        "amount": f"{amount:.2f}",
        "merchant": merchant,
        "city": city,
        "country": country,
        "lat": f"{lat:.4f}",
        "lon": f"{lon:.4f}",
        "timestamp": make_timestamp(base_date, offset),
        "is_fraud": "false",
    })
    txn_id_counter += 1

# 2a. Impossible Travel fraud (~40 rows)
intl_cities = ["London", "Singapore", "Dubai"]
fraud_users = ["USR-007","USR-015","USR-023","USR-031","USR-039","USR-042",
               "USR-048","USR-003","USR-011","USR-019"]
for fu in fraud_users:
    user_id, name, card = next(u for u in USERS if u[0] == fu)
    home_city = user_home[user_id]
    h_country, h_lat, h_lon = CITIES[home_city]
    intl = random.choice(intl_cities)
    i_country, i_lat, i_lon = CITIES[intl]
    offset = random.randint(5000, 40000)

    # Legitimate transaction at home
    transactions.append({
        "transaction_id": f"TXN-{txn_id_counter:05d}",
        "user_id": user_id, "user_name": name, "card_last4": card,
        "amount": f"{random_amount('Swiggy', user_avg[user_id]):.2f}",
        "merchant": "Swiggy", "city": home_city, "country": h_country,
        "lat": f"{h_lat:.4f}", "lon": f"{h_lon:.4f}",
        "timestamp": make_timestamp(base_date, offset),
        "is_fraud": "false",
    })
    txn_id_counter += 1

    # Impossible: same card in international city 30-45 min later
    fraud_offset = offset + random.randint(30, 45)
    transactions.append({
        "transaction_id": f"TXN-{txn_id_counter:05d}",
        "user_id": user_id, "user_name": name, "card_last4": card,
        "amount": f"{random.uniform(8000, 40000):.2f}",
        "merchant": random.choice(["Harrods","Luxury Retail","Duty Free","Casino Royale"]),
        "city": intl, "country": i_country,
        "lat": f"{i_lat:.4f}", "lon": f"{i_lon:.4f}",
        "timestamp": make_timestamp(base_date, fraud_offset),
        "is_fraud": "true",
    })
    txn_id_counter += 1
    fraud_count += 1

# 2b. Unusual amount fraud (~30 rows)
amount_fraud_users = ["USR-002","USR-010","USR-018","USR-026","USR-034",
                       "USR-004","USR-012","USR-020","USR-028","USR-036",
                       "USR-044","USR-050","USR-006","USR-014","USR-022"]
for fu in amount_fraud_users:
    user_id, name, card = next(u for u in USERS if u[0] == fu)
    city = user_home[user_id]
    country, lat, lon = CITIES[city]
    spike = round(user_avg[user_id] * random.uniform(12, 40), 2)
    offset = random.randint(2000, 38000)
    transactions.append({
        "transaction_id": f"TXN-{txn_id_counter:05d}",
        "user_id": user_id, "user_name": name, "card_last4": card,
        "amount": f"{spike:.2f}",
        "merchant": random.choice(["Gold Palace Jewellers","Luxury Watch Store","Apple Store","Rolex Boutique"]),
        "city": city, "country": country,
        "lat": f"{lat:.4f}", "lon": f"{lon:.4f}",
        "timestamp": make_timestamp(base_date, offset),
        "is_fraud": "true",
    })
    txn_id_counter += 1
    fraud_count += 1

# 2c. Rapid fire fraud — 4 txns within 90 seconds (~17 users × 4 txns)
rapid_users = ["USR-005","USR-013","USR-021","USR-029","USR-037",
               "USR-045","USR-008","USR-016","USR-024","USR-032",
               "USR-040","USR-001","USR-009","USR-017","USR-025",
               "USR-033","USR-041"]
for fu in rapid_users:
    user_id, name, card = next(u for u in USERS if u[0] == fu)
    city = user_home[user_id]
    country, lat, lon = CITIES[city]
    offset = random.randint(3000, 35000)
    rapid_merchants = random.sample(MERCHANTS, 4)
    for i, m in enumerate(rapid_merchants):
        sec_offset = offset + (i * 22) / 60.0   # 22 seconds apart
        transactions.append({
            "transaction_id": f"TXN-{txn_id_counter:05d}",
            "user_id": user_id, "user_name": name, "card_last4": card,
            "amount": f"{random.uniform(200, 800):.2f}",
            "merchant": m, "city": city, "country": country,
            "lat": f"{lat:.4f}", "lon": f"{lon:.4f}",
            "timestamp": make_timestamp(base_date, sec_offset),
            "is_fraud": "true" if i > 0 else "false",
        })
        txn_id_counter += 1
    fraud_count += 3

# Sort chronologically
transactions.sort(key=lambda x: x["timestamp"])

# Renumber IDs after sort
for i, t in enumerate(transactions, 1):
    t["transaction_id"] = f"TXN-{i:05d}"

# Write CSV
fieldnames = ["transaction_id","user_id","user_name","card_last4",
              "amount","merchant","city","country","lat","lon",
              "timestamp","is_fraud"]

with open("transactions.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(transactions)

legitimate = len(transactions) - fraud_count
print(f"✅ Generated {len(transactions)} transactions → transactions.csv")
print(f"   Legitimate    : {legitimate}")
print(f"   Fraud planted : {fraud_count}")
print(f"   Fraud rate    : {fraud_count/len(transactions)*100:.1f}%")
print()
print("Run next: python transaction_producer.py")
