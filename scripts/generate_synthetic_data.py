#!/usr/bin/env python3
"""Generate synthetic data for Apiary benchmarks.

This script generates configurable synthetic datasets for benchmarking
Apiary's performance across different workloads.
"""

import argparse
import random
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

try:
    import pyarrow as pa
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip install pyarrow", file=sys.stderr)
    sys.exit(1)


def generate_user_events(
    num_rows: int,
    num_users: int = 1000,
    num_event_types: int = 10,
    start_date: str = "2024-01-01",
) -> pa.Table:
    """Generate synthetic user event data.
    
    Args:
        num_rows: Number of event rows to generate
        num_users: Number of unique users
        num_event_types: Number of different event types
        start_date: Start date for events (YYYY-MM-DD)
    
    Returns:
        PyArrow Table with columns: user_id, event_type, timestamp, value, metadata
    """
    event_types = [f"event_{i}" for i in range(num_event_types)]
    start = datetime.fromisoformat(start_date)
    
    user_ids = []
    event_types_col = []
    timestamps = []
    values = []
    metadata = []
    
    for i in range(num_rows):
        user_ids.append(f"user_{random.randint(0, num_users - 1)}")
        event_types_col.append(random.choice(event_types))
        # Spread events over 30 days
        ts = start + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        timestamps.append(ts.isoformat())
        values.append(random.uniform(0.0, 1000.0))
        metadata.append(f"metadata_{i % 100}")
    
    return pa.table({
        "user_id": user_ids,
        "event_type": event_types_col,
        "timestamp": timestamps,
        "value": values,
        "metadata": metadata,
    })


def generate_sensor_readings(
    num_rows: int,
    num_sensors: int = 100,
    start_date: str = "2024-01-01",
) -> pa.Table:
    """Generate synthetic sensor reading data.
    
    Args:
        num_rows: Number of reading rows to generate
        num_sensors: Number of unique sensors
        start_date: Start date for readings (YYYY-MM-DD)
    
    Returns:
        PyArrow Table with columns: sensor_id, timestamp, temperature, humidity, pressure
    """
    start = datetime.fromisoformat(start_date)
    
    sensor_ids = []
    timestamps = []
    temperatures = []
    humidities = []
    pressures = []
    
    for i in range(num_rows):
        sensor_ids.append(f"sensor_{random.randint(0, num_sensors - 1)}")
        # Spread readings over 7 days at regular intervals
        ts = start + timedelta(seconds=i * 60)  # One reading per minute
        timestamps.append(ts.isoformat())
        temperatures.append(random.uniform(15.0, 35.0))
        humidities.append(random.uniform(30.0, 90.0))
        pressures.append(random.uniform(980.0, 1040.0))
    
    return pa.table({
        "sensor_id": sensor_ids,
        "timestamp": timestamps,
        "temperature": temperatures,
        "humidity": humidities,
        "pressure": pressures,
    })


def generate_sales_transactions(
    num_rows: int,
    num_products: int = 500,
    num_stores: int = 50,
    start_date: str = "2024-01-01",
) -> pa.Table:
    """Generate synthetic sales transaction data.
    
    Args:
        num_rows: Number of transaction rows to generate
        num_products: Number of unique products
        num_stores: Number of unique stores
        start_date: Start date for transactions (YYYY-MM-DD)
    
    Returns:
        PyArrow Table with columns: transaction_id, store_id, product_id, quantity, amount, timestamp
    """
    start = datetime.fromisoformat(start_date)
    
    transaction_ids = []
    store_ids = []
    product_ids = []
    quantities = []
    amounts = []
    timestamps = []
    
    for i in range(num_rows):
        transaction_ids.append(f"txn_{i}")
        store_ids.append(f"store_{random.randint(0, num_stores - 1)}")
        product_ids.append(f"product_{random.randint(0, num_products - 1)}")
        quantities.append(random.randint(1, 10))
        amounts.append(random.uniform(5.0, 500.0))
        # Spread transactions over 90 days
        ts = start + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(8, 22),  # Business hours
            minutes=random.randint(0, 59),
        )
        timestamps.append(ts.isoformat())
    
    return pa.table({
        "transaction_id": transaction_ids,
        "store_id": store_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "amount": amounts,
        "timestamp": timestamps,
    })


DATASET_GENERATORS = {
    "user_events": generate_user_events,
    "sensor_readings": generate_sensor_readings,
    "sales_transactions": generate_sales_transactions,
}


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for Apiary benchmarks"
    )
    parser.add_argument(
        "dataset",
        choices=list(DATASET_GENERATORS.keys()),
        help="Type of dataset to generate",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10000,
        help="Number of rows to generate (default: 10000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (Parquet format). If not specified, prints schema only.",
    )
    
    args = parser.parse_args()
    
    print(f"Generating {args.rows} rows of {args.dataset} data...", file=sys.stderr)
    
    generator = DATASET_GENERATORS[args.dataset]
    table = generator(args.rows)
    
    print(f"Generated table with {len(table)} rows", file=sys.stderr)
    print(f"Schema: {table.schema}", file=sys.stderr)
    
    if args.output:
        import pyarrow.parquet as pq
        pq.write_table(table, args.output, compression="lz4")
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        # Just print the first few rows
        print("\nFirst 5 rows:")
        print(table.slice(0, min(5, len(table))).to_pandas())
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
