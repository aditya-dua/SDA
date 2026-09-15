"""
generate_transactions_custom.py
Configurable transaction generator for the SDA course.

Supports interactive prompts or command-line arguments for:
  - total transaction count
  - total fraud as a count or percentage
  - impossible-travel, unusual-amount, and rapid-fire allocation

Examples:
    python generate_transactions_custom.py
    python generate_transactions_custom.py --total 20000 --fraud-percent 5
    python generate_transactions_custom.py --total 20000 --fraud-count 1000 \
        --allocation-mode percent --impossible 20 --unusual 30 --rapid 50
"""

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


USERS = [
    (f"USR-{index:03d}", f"Customer {index:03d}", f"{1000 + index:04d}")
    for index in range(1, 101)
]
CITIES = {
    "Mumbai": ("India", 19.0760, 72.8770),
    "Delhi": ("India", 28.6139, 77.2090),
    "Bengaluru": ("India", 12.9716, 77.5946),
    "Hyderabad": ("India", 17.3850, 78.4867),
    "Chennai": ("India", 13.0827, 80.2707),
    "Kolkata": ("India", 22.5726, 88.3639),
    "Pune": ("India", 18.5204, 73.8567),
    "Ahmedabad": ("India", 23.0225, 72.5714),
    "London": ("UK", 51.5074, -0.1278),
    "Singapore": ("Singapore", 1.3521, 103.8198),
    "Dubai": ("UAE", 25.2048, 55.2708),
}
DOMESTIC_CITIES = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Pune",
    "Ahmedabad",
]
INTERNATIONAL_CITIES = ["London", "Singapore", "Dubai"]
MERCHANTS = [
    "Swiggy",
    "Zomato",
    "Amazon",
    "Flipkart",
    "BigBasket",
    "DMart",
    "BookMyShow",
    "MakeMyTrip",
    "IRCTC",
    "Ola",
    "Uber",
    "Apollo Pharmacy",
    "Myntra",
    "Nykaa",
]
FIELDNAMES = [
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


@dataclass(frozen=True)
class GeneratorConfig:
    total: int
    fraud_count: int
    impossible_count: int
    unusual_count: int
    rapid_count: int
    output_file: str = "transactions_custom.csv"
    seed: int = 42


def allocate_by_percentage(total, percentages):
    """Convert percentages into exact counts while preserving the total."""
    raw = [total * value / 100 for value in percentages]
    counts = [int(value) for value in raw]
    remaining = total - sum(counts)
    order = sorted(
        range(len(raw)),
        key=lambda index: raw[index] - counts[index],
        reverse=True,
    )
    for index in order[:remaining]:
        counts[index] += 1
    return counts


def validate_config(config):
    if config.total < 1:
        raise ValueError("Total transactions must be at least 1.")
    if not 0 <= config.fraud_count <= config.total:
        raise ValueError("Fraud count must be between 0 and total transactions.")
    type_total = (
        config.impossible_count + config.unusual_count + config.rapid_count
    )
    if min(
        config.impossible_count,
        config.unusual_count,
        config.rapid_count,
    ) < 0:
        raise ValueError("Fraud-type counts cannot be negative.")
    if type_total != config.fraud_count:
        raise ValueError(
            "Impossible-travel, unusual-amount, and rapid-fire counts "
            "must add up to the total fraud count."
        )


def next_transaction_number(output_file):
    output_path = Path(output_file)
    sequence_path = output_path.with_name(f".{output_path.stem}_sequence")
    candidates = [0]

    if sequence_path.exists():
        try:
            candidates.append(int(sequence_path.read_text().strip()))
        except ValueError:
            pass

    if output_path.exists():
        with output_path.open(newline="") as csv_file:
            for row in csv.DictReader(csv_file):
                try:
                    candidates.append(
                        int(row["transaction_id"].rsplit("-", 1)[1])
                    )
                except (KeyError, ValueError):
                    continue

    return max(candidates) + 1, sequence_path


def make_row(user, amount, merchant, city, timestamp, is_fraud):
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
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "is_fraud": "true" if is_fraud else "false",
    }


def generate_transactions(config):
    validate_config(config)
    rng = random.Random(config.seed)
    output_path = Path(config.output_file)
    start_number, sequence_path = next_transaction_number(output_path)
    base_time = datetime.now().replace(microsecond=0) - timedelta(days=1)
    normal_count = config.total - config.fraud_count
    rows = []

    for _ in range(normal_count):
        user = rng.choice(USERS)
        city = rng.choice(DOMESTIC_CITIES)
        rows.append(
            make_row(
                user,
                rng.uniform(150, 25000),
                rng.choice(MERCHANTS),
                city,
                base_time + timedelta(seconds=rng.randint(0, 86400)),
                False,
            )
        )

    for _ in range(config.impossible_count):
        rows.append(
            make_row(
                rng.choice(USERS),
                rng.uniform(8000, 45000),
                "International Duty Free",
                rng.choice(INTERNATIONAL_CITIES),
                base_time + timedelta(seconds=rng.randint(0, 86400)),
                True,
            )
        )

    for _ in range(config.unusual_count):
        rows.append(
            make_row(
                rng.choice(USERS),
                rng.uniform(50000, 200000),
                rng.choice(
                    ["Luxury Retail", "Gold Palace Jewellers", "Rolex Boutique"]
                ),
                rng.choice(DOMESTIC_CITIES),
                base_time + timedelta(seconds=rng.randint(0, 86400)),
                True,
            )
        )

    rapid_start = base_time + timedelta(hours=12)
    for index in range(config.rapid_count):
        rows.append(
            make_row(
                USERS[index % len(USERS)],
                rng.uniform(200, 750),
                rng.choice(MERCHANTS),
                rng.choice(DOMESTIC_CITIES),
                rapid_start + timedelta(seconds=index * 10),
                True,
            )
        )

    rows.sort(key=lambda row: row["timestamp"])
    for offset, row in enumerate(rows):
        row["transaction_id"] = f"CUSTOM-TXN-{start_number + offset:08d}"

    with output_path.open("w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    last_number = start_number + len(rows) - 1
    sequence_path.write_text(str(last_number))
    print(f"Generated {len(rows):,} transactions -> {output_path}")
    print(
        f"ID range: CUSTOM-TXN-{start_number:08d} "
        f"to CUSTOM-TXN-{last_number:08d}"
    )
    print(
        f"Fraud: {config.fraud_count:,} "
        f"(impossible={config.impossible_count:,}, "
        f"unusual={config.unusual_count:,}, rapid={config.rapid_count:,})"
    )


def prompt_number(label, default, number_type=float):
    value = input(f"{label} [{default}]: ").strip()
    return number_type(value) if value else number_type(default)


def interactive_config():
    total = prompt_number("Total transactions", 20000, int)
    fraud_mode = input("Fraud total as count or percent? [percent]: ").strip()
    fraud_mode = fraud_mode.lower() or "percent"
    if fraud_mode.startswith("c"):
        fraud_count = prompt_number("Fraud count", max(1, total // 20), int)
    else:
        fraud_percent = prompt_number("Fraud percentage", 5.0)
        fraud_count = round(total * fraud_percent / 100)

    allocation_mode = (
        input("Fraud types as count or percent? [percent]: ").strip().lower()
        or "percent"
    )
    if allocation_mode.startswith("c"):
        impossible = prompt_number("Impossible-travel count", fraud_count // 3, int)
        unusual = prompt_number("Unusual-amount count", fraud_count // 3, int)
        rapid = prompt_number(
            "Rapid-fire count",
            fraud_count - impossible - unusual,
            int,
        )
    else:
        impossible_percent = prompt_number("Impossible-travel percentage", 30.0)
        unusual_percent = prompt_number("Unusual-amount percentage", 30.0)
        rapid_percent = prompt_number("Rapid-fire percentage", 40.0)
        if round(
            impossible_percent + unusual_percent + rapid_percent,
            6,
        ) != 100:
            raise ValueError("Fraud-type percentages must add up to 100.")
        impossible, unusual, rapid = allocate_by_percentage(
            fraud_count,
            [impossible_percent, unusual_percent, rapid_percent],
        )

    output_file = (
        input("Output CSV [transactions_custom.csv]: ").strip()
        or "transactions_custom.csv"
    )
    return GeneratorConfig(
        total=total,
        fraud_count=fraud_count,
        impossible_count=impossible,
        unusual_count=unusual,
        rapid_count=rapid,
        output_file=output_file,
    )


def config_from_args(args):
    if args.fraud_count is not None:
        fraud_count = args.fraud_count
    else:
        fraud_count = round(args.total * args.fraud_percent / 100)

    values = [args.impossible, args.unusual, args.rapid]
    if args.allocation_mode == "percent":
        if round(sum(values), 6) != 100:
            raise ValueError("Fraud-type percentages must add up to 100.")
        impossible, unusual, rapid = allocate_by_percentage(
            fraud_count,
            values,
        )
    else:
        impossible, unusual, rapid = [int(value) for value in values]

    return GeneratorConfig(
        total=args.total,
        fraud_count=fraud_count,
        impossible_count=impossible,
        unusual_count=unusual,
        rapid_count=rapid,
        output_file=args.output,
        seed=args.seed,
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int)
    fraud_group = parser.add_mutually_exclusive_group()
    fraud_group.add_argument("--fraud-count", type=int)
    fraud_group.add_argument("--fraud-percent", type=float, default=5.0)
    parser.add_argument(
        "--allocation-mode",
        choices=["count", "percent"],
        default="percent",
    )
    parser.add_argument("--impossible", type=float, default=30)
    parser.add_argument("--unusual", type=float, default=30)
    parser.add_argument("--rapid", type=float, default=40)
    parser.add_argument("--output", default="transactions_custom.csv")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    configuration = (
        interactive_config()
        if arguments.total is None
        else config_from_args(arguments)
    )
    generate_transactions(configuration)
