#!/usr/bin/env python3
"""
DuckDB Single-Node SSB Baseline Benchmark

Runs the same 13 SSB queries against SF1 Parquet data using DuckDB
on a single node, providing a theoretical lower-bound for comparison
with Apiary's distributed execution.

Methodology matches the Apiary bench_runner.py:
  - 1 warmup run (not counted)
  - 3 timed runs per query
  - Reports median / best / worst per query
  - Geometric mean and total wall clock time

Usage:
  python benchmarks/baselines/duckdb_ssb.py --data-dir ./data/ssb/sf1/parquet
"""

import argparse
import json
import math
import os
import platform
import re
import sys
import time
from pathlib import Path
from statistics import median, mean

import duckdb


# ----------------------------------------------------------------
# Query parser (mirrors bench_runner.parse_sql_file)
# ----------------------------------------------------------------

def parse_sql_file(filepath: str) -> list[tuple[str, str]]:
    """Parse a .sql file into (query_id, query_text) pairs.

    Expects queries separated by comments like:
      -- Q1.1: Description
      SELECT ...;
    """
    with open(filepath) as f:
        content = f.read()

    queries: list[tuple[str, str]] = []
    current_id: str | None = None
    current_sql: list[str] = []

    for line in content.split("\n"):
        stripped = line.strip()

        id_match = re.match(
            r'^--\s+(Q[\d.]+|FA-\d+|AC-\d+|HC-\d+|SE-\d+)[:\s]', stripped
        )
        if id_match:
            if current_id and current_sql:
                sql = "\n".join(current_sql).strip().rstrip(";")
                if sql:
                    queries.append((current_id, sql))
            current_id = id_match.group(1)
            current_sql = []
            continue

        if stripped.startswith("--") or stripped.startswith("=="):
            continue

        if current_id is not None:
            current_sql.append(line)

    if current_id and current_sql:
        sql = "\n".join(current_sql).strip().rstrip(";")
        if sql:
            queries.append((current_id, sql))

    return queries


# ----------------------------------------------------------------
# System info (subset of bench_runner.collect_system_info)
# ----------------------------------------------------------------

def collect_system_info() -> dict:
    """Collect hardware and OS information for reproducibility."""
    info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "hostname": platform.node(),
    }

    try:
        import psutil
        mem = psutil.virtual_memory()
        info["ram_total_gb"] = round(mem.total / (1024**3), 2)
        info["ram_available_gb"] = round(mem.available / (1024**3), 2)
        info["cpu_count_physical"] = psutil.cpu_count(logical=False)
        info["cpu_count_logical"] = psutil.cpu_count(logical=True)
    except ImportError:
        pass

    return info


# ----------------------------------------------------------------
# DuckDB engine
# ----------------------------------------------------------------

def register_tables(con: duckdb.DuckDBPyConnection,
                    data_dir: str) -> list[tuple[str, str]]:
    """Register Parquet/CSV files from *data_dir* as DuckDB tables.

    Returns a list of (table_name, file_path) that were registered.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    registered: list[tuple[str, str]] = []
    for f in sorted(data_path.rglob("*")):
        if f.suffix not in (".parquet", ".csv"):
            continue

        table_name = f.stem
        # Handle partitioned datasets where the file is named "data.parquet"
        if table_name == "data":
            table_name = f.parent.parent.name

        if f.suffix == ".parquet":
            con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS "
                f"SELECT * FROM read_parquet('{f}')"
            )
        elif f.suffix == ".csv":
            con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS "
                f"SELECT * FROM read_csv_auto('{f}')"
            )
        registered.append((table_name, str(f)))

    return registered


def execute_query(con: duckdb.DuckDBPyConnection,
                  sql: str) -> int:
    """Execute *sql* and return the number of result rows."""
    result = con.execute(sql)
    rows = result.fetchall()
    return len(rows)


# ----------------------------------------------------------------
# Benchmark loop
# ----------------------------------------------------------------

def run_benchmark(
    con: duckdb.DuckDBPyConnection,
    queries: list[tuple[str, str]],
    runs: int = 3,
    warmup: int = 1,
) -> list[dict]:
    """Run each query with *warmup* + *runs* iterations.

    Returns a list of summary dicts (one per query).
    """
    summaries: list[dict] = []

    for query_id, sql in queries:
        print(f"\n--- {query_id} ---")

        # Warmup
        for w in range(warmup):
            try:
                execute_query(con, sql)
                print(f"  Warmup {w + 1}/{warmup}: OK")
            except Exception as e:
                print(f"  Warmup {w + 1}/{warmup}: ERROR - {e}")

        # Timed runs
        times: list[float] = []
        row_count = 0
        for r in range(runs):
            try:
                start = time.perf_counter()
                row_count = execute_query(con, sql)
                elapsed_ms = (time.perf_counter() - start) * 1000

                times.append(round(elapsed_ms, 2))
                print(f"  Run {r + 1}/{runs}: {elapsed_ms:>8.1f}ms "
                      f"({row_count} rows)")
            except Exception as e:
                print(f"  Run {r + 1}/{runs}: ERROR - {e}")

        if times:
            summaries.append({
                "query_id": query_id,
                "runs": len(times),
                "median_ms": round(median(times), 2),
                "mean_ms": round(mean(times), 2),
                "best_ms": round(min(times), 2),
                "worst_ms": round(max(times), 2),
                "rows_returned": row_count,
                "all_times_ms": times,
            })
        else:
            summaries.append({
                "query_id": query_id,
                "runs": 0,
                "median_ms": 0,
                "mean_ms": 0,
                "best_ms": 0,
                "worst_ms": 0,
                "rows_returned": 0,
                "all_times_ms": [],
            })

    return summaries


# ----------------------------------------------------------------
# Reporting (matches bench_runner.print_report format)
# ----------------------------------------------------------------

def print_report(summaries: list[dict], *, engine_version: str,
                 scale_factor: int, data_format: str,
                 hardware: dict, total_wall_ms: float):
    """Print a formatted report to stdout matching Apiary's format."""
    print(f"\n{'=' * 72}")
    print(f"BENCHMARK REPORT: ssb")
    print(f"{'=' * 72}")
    print(f"Engine:       DuckDB {engine_version}")
    print(f"Scale Factor: {scale_factor}")
    print(f"Format:       {data_format}")
    print(f"Platform:     {hardware.get('platform', 'unknown')}")
    if "device_model" in hardware:
        print(f"Device:       {hardware['device_model']}")
    print(f"RAM:          {hardware.get('ram_total_gb', '?')} GB")
    print(f"CPUs:         {hardware.get('cpu_count_logical', '?')}")
    print(f"{'=' * 72}")

    print(f"\n{'Query':<10} {'Median':>10} {'Best':>10} {'Worst':>10} "
          f"{'Rows':>10} {'Mem MB':>8}")
    print("-" * 72)

    for q in summaries:
        # Mem MB is not tracked by DuckDB baseline; report 0.0
        print(f"{q['query_id']:<10} {q['median_ms']:>10.1f} "
              f"{q['best_ms']:>10.1f} {q['worst_ms']:>10.1f} "
              f"{q['rows_returned']:>10} {'0.0':>8}")

    valid_medians = [q["median_ms"] for q in summaries if q["median_ms"] > 0]
    if valid_medians:
        geomean = math.exp(
            sum(math.log(t) for t in valid_medians) / len(valid_medians)
        )
        total_median = sum(valid_medians)
        print("-" * 72)
        print(f"{'GeoMean':<10} {geomean:>10.1f}")
        print(f"{'Total':<10} {total_median:>10.1f}")

    print(f"\nWall clock: {total_wall_ms / 1000:.1f}s")
    print(f"{'=' * 72}\n")


def save_report(summaries: list[dict], *, engine_version: str,
                scale_factor: int, data_format: str,
                hardware: dict, total_wall_ms: float,
                output_dir: str) -> str:
    """Save report as JSON (same schema as bench_runner)."""
    os.makedirs(output_dir, exist_ok=True)

    valid_medians = [q["median_ms"] for q in summaries if q["median_ms"] > 0]
    geomean = 0.0
    if valid_medians:
        geomean = math.exp(
            sum(math.log(t) for t in valid_medians) / len(valid_medians)
        )

    report = {
        "suite": "ssb",
        "scale_factor": scale_factor,
        "data_format": data_format,
        "engine": "DuckDB",
        "engine_version": engine_version,
        "hardware": hardware,
        "queries": summaries,
        "geomean_ms": round(geomean, 2),
        "total_time_ms": round(total_wall_ms, 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    filename = f"ssb_DuckDB_{data_format}_sf{scale_factor}.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"Report saved: {filepath}")
    return filepath


# ----------------------------------------------------------------
# CLI
# ----------------------------------------------------------------

def _resolve_query_file() -> str:
    """Locate the SSB query SQL file relative to this script."""
    # benchmarks/baselines/duckdb_ssb.py  →  benchmarks/queries/ssb/ssb_queries.sql
    baselines_dir = Path(__file__).resolve().parent
    candidates = [
        baselines_dir.parent / "queries" / "ssb" / "ssb_queries.sql",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        "Cannot find ssb_queries.sql. Expected at "
        f"{candidates[0]}"
    )


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="DuckDB single-node SSB baseline benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmarks/baselines/duckdb_ssb.py --data-dir ./data/ssb/sf1/parquet
  python benchmarks/baselines/duckdb_ssb.py --data-dir ./data --runs 5
        """,
    )
    parser.add_argument(
        "--data-dir", "-d", required=True,
        help="Path to directory containing SSB Parquet/CSV files",
    )
    parser.add_argument(
        "--output", "-o", default="./results",
        help="Output directory for JSON results (default: ./results)",
    )
    parser.add_argument(
        "--runs", "-r", type=int, default=3,
        help="Number of timed runs per query (default: 3)",
    )
    parser.add_argument(
        "--warmup", "-w", type=int, default=1,
        help="Number of warmup runs (default: 1)",
    )
    parser.add_argument(
        "--queries", "-q", nargs="+",
        help="Specific query IDs to run (e.g., Q1.1 Q2.3)",
    )
    parser.add_argument(
        "--scale-factor", type=int, default=1,
        help="Scale factor for reporting (default: 1)",
    )
    parser.add_argument(
        "--query-file", default=None,
        help="Path to SSB SQL file (auto-detected if omitted)",
    )

    args = parser.parse_args(argv)

    # Locate query file
    query_file = args.query_file or _resolve_query_file()
    queries = parse_sql_file(query_file)
    if args.queries:
        queries = [(qid, sql) for qid, sql in queries
                   if qid in args.queries]

    if not queries:
        print(f"No queries found in {query_file}", file=sys.stderr)
        sys.exit(1)

    # Detect data format from path
    data_format = "mixed"
    if "parquet" in args.data_dir:
        data_format = "parquet"
    elif "csv" in args.data_dir:
        data_format = "csv"

    # DuckDB version
    engine_version = duckdb.__version__

    print(f"\n{'=' * 60}")
    print(f"DuckDB SSB Baseline Benchmark")
    print(f"{'=' * 60}")
    print(f"Engine:  DuckDB {engine_version}")
    print(f"Data:    {args.data_dir}")
    print(f"Queries: {len(queries)}")
    print(f"Runs:    {args.warmup} warmup + {args.runs} timed")
    print(f"{'=' * 60}")

    # Connect and load data
    con = duckdb.connect(":memory:")
    registered = register_tables(con, args.data_dir)
    print(f"\nRegistered {len(registered)} tables:")
    for name, path in registered:
        print(f"  {name}")

    # Collect system info
    hardware = collect_system_info()

    # Run benchmark
    total_start = time.time()
    summaries = run_benchmark(con, queries, runs=args.runs,
                              warmup=args.warmup)
    total_wall_ms = (time.time() - total_start) * 1000

    con.close()

    # Report
    report_kwargs = dict(
        engine_version=engine_version,
        scale_factor=args.scale_factor,
        data_format=data_format,
        hardware=hardware,
        total_wall_ms=total_wall_ms,
    )

    print_report(summaries, **report_kwargs)
    save_report(summaries, output_dir=args.output, **report_kwargs)


if __name__ == "__main__":
    main()
