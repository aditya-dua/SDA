# check_alerts.py
# Lecture 9 — Quick viewer for both MongoDB collections
#
# Run anytime (even while consumer_alerts.py is running) to see a snapshot.
# Prerequisites: pip install pymongo
# Replace MONGO_URI with your Atlas connection string.

from pymongo import MongoClient, DESCENDING
from datetime import datetime

MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(MONGO_URI)
db     = client["sda_course"]

total_prices = db["stock_prices"].count_documents({})
total_alerts = db["stock_alerts"].count_documents({})

print(f"\n{'='*52}")
print(f"  Atlas Snapshot — {datetime.now().strftime('%H:%M:%S')}")
print(f"{'='*52}")
print(f"  stock_prices : {total_prices:,} documents")
print(f"  stock_alerts : {total_alerts:,} documents")
if total_prices:
    print(f"  Alert rate   : {total_alerts / total_prices * 100:.1f}%")

if total_alerts:
    print(f"\n  Recent Alerts (last 8):")
    print(f"  {'Symbol':<10} {'Price':>10}  Reason")
    print(f"  {'─'*50}")
    for doc in db["stock_alerts"].find({}, {"_id": 0}).sort("alert_at", DESCENDING).limit(8):
        reasons = ", ".join(doc.get("alert_reasons", []))
        print(f"  {doc['symbol']:<10} ₹{doc['price']:>9.2f}  {reasons}")

    # Breakdown by reason prefix
    print(f"\n  Alert breakdown by reason type:")
    pipeline = [
        {"$unwind": "$alert_reasons"},
        {"$group": {
            "_id": {"$substr": ["$alert_reasons", 0, 15]},
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    for r in db["stock_alerts"].aggregate(pipeline):
        print(f"    {r['_id']+'...':<22} {r['count']:>4}")

client.close()
