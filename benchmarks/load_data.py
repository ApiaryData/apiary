#!/usr/bin/env python3
"""
Apiary Benchmark Data Loader

Designed to run INSIDE an Apiary Docker container. Generates SSB or TPC-H
data using the benchmark generators, then ingests each table into Apiary's
storage layer via the Python SDK.

The benchmarks/ directory must be mounted at /benchmarks inside the container
so that the generators package is importable.

Usage (inside container):
  python3 /benchmarks/load_data.py --suite ssb --scale-factor 1 \
      --storage-url s3://apiary/bench --hive ssb --box sf1

  python3 /benchmarks/load_data.py --suite tpch --scale-factor 1 \
      --storage-url s3://apiary/bench --hive tpch --box sf1

Output:
  Prints a JSON manifest to stdout with table names, row counts, and schemas.
  All progress/log output goes to stderr.
"""

import argparse
import json
import sys
import time

import numpy as np
import pyarrow as pa


def _arrow_type_to_apiary(arrow_type: pa.DataType) -> str:
    """Map a PyArrow type to an Apiary frame schema type string."""
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return "int32"
    if pa.types.is_int32(arrow_type):
        return "int32"
    if pa.types.is_int64(arrow_type):
        return "int64"
    if pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type):
        return "int32"
    if pa.types.is_uint32(arrow_type):
        return "int64"
    if pa.types.is_uint64(arrow_type):
        return "int64"
    if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type):
        return "float32"
    if pa.types.is_float64(arrow_type):
        return "float64"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_date(arrow_type):
        return "string"
    if pa.types.is_timestamp(arrow_type):
        return "string"
    # Fallback for anything else
    return "string"


def _schema_to_apiary_dict(schema: pa.Schema) -> dict:
    """Convert a PyArrow schema to an Apiary frame column dict."""
    return {field.name: _arrow_type_to_apiary(field.type) for field in schema}


def _write_table_to_apiary(ap, hive: str, box: str, table_name: str,
                           table: pa.Table, chunk_size: int = 500_000) -> int:
    """Write a PyArrow table into Apiary via IPC serialisation.

    Writes in chunks to avoid memory pressure on constrained hardware.
    Returns total rows written.
    """
    schema_dict = _schema_to_apiary_dict(table.schema)

    # Create the frame (table) in Apiary
    try:
        ap.create_frame(hive, box, table_name, schema_dict)
    except Exception as e:
        # Frame may already exist from a previous run
        print(f"    Note: create_frame {table_name}: {e}", file=sys.stderr)

    total_rows = len(table)
    rows_written = 0

    while rows_written < total_rows:
        end = min(rows_written + chunk_size, total_rows)
        chunk = table.slice(rows_written, end - rows_written)

        # Serialise chunk to IPC stream bytes
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, chunk.schema)
        writer.write_table(chunk)
        writer.close()
        ipc_data = sink.getvalue().to_pybytes()

        ap.write_to_frame(hive, box, table_name, ipc_data)
        rows_written = end
        print(f"    {table_name}: {rows_written:,}/{total_rows:,} rows",
              file=sys.stderr)

    return total_rows


def generate_and_load_ssb(ap, hive: str, box: str, scale_factor: int,
                          seed: int, chunk_size: int) -> dict:
    """Generate SSB tables in-memory and load into Apiary."""
    # Import generators from the mounted benchmarks directory
    sys.path.insert(0, "/benchmarks")
    from generators import SSBGenerator

    gen = SSBGenerator(scale_factor=scale_factor, seed=seed)
    manifest = {}

    print("\nGenerating SSB dimensions...", file=sys.stderr)

    print("  customer...", file=sys.stderr)
    customer = gen.generate_customer()
    rows = _write_table_to_apiary(ap, hive, box, "customer", customer, chunk_size)
    manifest["customer"] = {"rows": rows, "columns": list(customer.schema.names)}

    print("  supplier...", file=sys.stderr)
    supplier = gen.generate_supplier()
    rows = _write_table_to_apiary(ap, hive, box, "supplier", supplier, chunk_size)
    manifest["supplier"] = {"rows": rows, "columns": list(supplier.schema.names)}

    print("  part...", file=sys.stderr)
    part = gen.generate_part()
    rows = _write_table_to_apiary(ap, hive, box, "part", part, chunk_size)
    manifest["part"] = {"rows": rows, "columns": list(part.schema.names)}

    # SSB uses "date" as table name, but this is a SQL reserved word.
    # Store as "date_dim" in Apiary; the query rewriter handles the mapping.
    print("  date_dim...", file=sys.stderr)
    date = gen.generate_date()
    rows = _write_table_to_apiary(ap, hive, box, "date_dim", date, chunk_size)
    manifest["date_dim"] = {"rows": rows, "columns": list(date.schema.names)}

    print("\nGenerating SSB fact table...", file=sys.stderr)
    print("  lineorder...", file=sys.stderr)
    lineorder = gen.generate_lineorder(customer, supplier, part, date)
    rows = _write_table_to_apiary(ap, hive, box, "lineorder", lineorder, chunk_size)
    manifest["lineorder"] = {"rows": rows, "columns": list(lineorder.schema.names)}

    return manifest


def generate_and_load_tpch(ap, hive: str, box: str, scale_factor: int,
                           seed: int, chunk_size: int) -> dict:
    """Generate TPC-H tables in-memory and load into Apiary."""
    sys.path.insert(0, "/benchmarks")
    from generators import TPCHGenerator

    gen = TPCHGenerator(scale_factor=scale_factor, seed=seed)
    manifest = {}

    print("\nGenerating TPC-H fixed tables...", file=sys.stderr)

    print("  region...", file=sys.stderr)
    region = gen.generate_region()
    rows = _write_table_to_apiary(ap, hive, box, "region", region, chunk_size)
    manifest["region"] = {"rows": rows, "columns": list(region.schema.names)}

    print("  nation...", file=sys.stderr)
    nation = gen.generate_nation()
    rows = _write_table_to_apiary(ap, hive, box, "nation", nation, chunk_size)
    manifest["nation"] = {"rows": rows, "columns": list(nation.schema.names)}

    print("\nGenerating TPC-H dimensions...", file=sys.stderr)

    print("  supplier...", file=sys.stderr)
    supplier = gen.generate_supplier()
    rows = _write_table_to_apiary(ap, hive, box, "supplier", supplier, chunk_size)
    manifest["supplier"] = {"rows": rows, "columns": list(supplier.schema.names)}

    print("  customer...", file=sys.stderr)
    customer = gen.generate_customer()
    rows = _write_table_to_apiary(ap, hive, box, "customer", customer, chunk_size)
    manifest["customer"] = {"rows": rows, "columns": list(customer.schema.names)}

    print("  part...", file=sys.stderr)
    part = gen.generate_part()
    rows = _write_table_to_apiary(ap, hive, box, "part", part, chunk_size)
    manifest["part"] = {"rows": rows, "columns": list(part.schema.names)}

    print("  partsupp...", file=sys.stderr)
    partsupp = gen.generate_partsupp()
    rows = _write_table_to_apiary(ap, hive, box, "partsupp", partsupp, chunk_size)
    manifest["partsupp"] = {"rows": rows, "columns": list(partsupp.schema.names)}

    print("\nGenerating TPC-H orders...", file=sys.stderr)
    orders = gen.generate_orders()
    rows = _write_table_to_apiary(ap, hive, box, "orders", orders, chunk_size)
    manifest["orders"] = {"rows": rows, "columns": list(orders.schema.names)}

    print("\nGenerating TPC-H lineitem (fact table)...", file=sys.stderr)
    lineitem = gen.generate_lineitem(orders)
    rows = _write_table_to_apiary(ap, hive, box, "lineitem", lineitem, chunk_size)
    manifest["lineitem"] = {"rows": rows, "columns": list(lineitem.schema.names)}

    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Generate benchmark data and load into Apiary (runs inside container)",
    )
    parser.add_argument(
        "--suite", required=True, choices=["ssb", "tpch"],
        help="Benchmark suite to generate and load",
    )
    parser.add_argument(
        "--scale-factor", type=int, default=1,
        help="Scale factor (default: 1)",
    )
    parser.add_argument(
        "--storage-url", required=True,
        help="Apiary storage URL (e.g., s3://apiary/bench)",
    )
    parser.add_argument(
        "--hive", required=True,
        help="Apiary hive name for the benchmark tables",
    )
    parser.add_argument(
        "--box", required=True,
        help="Apiary box name (typically sf1, sf10, etc.)",
    )
    parser.add_argument(
        "--name", default="benchmark",
        help="Apiary instance name (default: benchmark)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=500_000,
        help="Rows per write chunk (default: 500000). Lower for memory-constrained hardware.",
    )

    args = parser.parse_args()

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Apiary Benchmark Data Loader", file=sys.stderr)
    print(f"Suite:        {args.suite}", file=sys.stderr)
    print(f"Scale Factor: {args.scale_factor}", file=sys.stderr)
    print(f"Storage:      {args.storage_url}", file=sys.stderr)
    print(f"Hive/Box:     {args.hive}.{args.box}", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    # Import Apiary SDK
    from apiary import Apiary

    ap = Apiary(args.name, storage=args.storage_url)

    # Retry ap.start() — the S3 backend (MinIO) may not be fully ready
    # immediately after the Docker cluster reports healthy.
    max_start_retries = 3
    for attempt in range(1, max_start_retries + 1):
        try:
            ap.start()
            break
        except RuntimeError as e:
            if attempt < max_start_retries:
                wait = attempt * 5
                print(f"  start() attempt {attempt}/{max_start_retries} failed: {e}",
                      file=sys.stderr)
                print(f"  Retrying in {wait}s...", file=sys.stderr)
                time.sleep(wait)
                ap = Apiary(args.name, storage=args.storage_url)
            else:
                raise

    time.sleep(2)  # Allow node to initialise

    # Create hive and box
    try:
        ap.create_hive(args.hive)
        print(f"Created hive: {args.hive}", file=sys.stderr)
    except Exception as e:
        print(f"Hive {args.hive} already exists or error: {e}", file=sys.stderr)

    try:
        ap.create_box(args.hive, args.box)
        print(f"Created box: {args.hive}.{args.box}", file=sys.stderr)
    except Exception as e:
        print(f"Box {args.hive}.{args.box} already exists or error: {e}", file=sys.stderr)

    # Generate and load data
    start = time.time()
    if args.suite == "ssb":
        manifest = generate_and_load_ssb(
            ap, args.hive, args.box, args.scale_factor, args.seed, args.chunk_size
        )
    elif args.suite == "tpch":
        manifest = generate_and_load_tpch(
            ap, args.hive, args.box, args.scale_factor, args.seed, args.chunk_size
        )
    else:
        print(f"Unknown suite: {args.suite}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - start

    # Build final manifest
    output = {
        "suite": args.suite,
        "scale_factor": args.scale_factor,
        "hive": args.hive,
        "box": args.box,
        "storage_url": args.storage_url,
        "tables": manifest,
        "total_rows": sum(t["rows"] for t in manifest.values()),
        "elapsed_seconds": round(elapsed, 2),
    }

    print(f"\nData loading complete in {elapsed:.1f}s", file=sys.stderr)
    print(f"Total rows: {output['total_rows']:,}", file=sys.stderr)

    ap.shutdown()

    # Print manifest to stdout (machine-readable)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
