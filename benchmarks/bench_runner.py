#!/usr/bin/env python3
"""
Apiary Benchmark Runner

Executes benchmark queries against Apiary (or comparison engines),
captures timing metrics, and produces structured results.

Usage:
  # Run SSB benchmark suite
  python bench_runner.py --suite ssb --data-dir ./data/ssb/sf1/parquet

  # Run all suites with custom config
  python bench_runner.py --config config.yaml

  # Run specific queries
  python bench_runner.py --suite ssb --queries Q1.1 Q2.1 Q3.1 Q4.1

  # Compare formats (run same queries across parquet/csv/arrow)
  python bench_runner.py --suite apiary_format --data-dir ./data/apiary/sf1/format_agnostic

  # Dry run (parse queries, validate config, don't execute)
  python bench_runner.py --suite ssb --dry-run
"""

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from statistics import median, mean

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# ----------------------------------------------------------------
# Data classes for structured results
# ----------------------------------------------------------------

@dataclass
class QueryResult:
    """Result of a single query execution."""
    query_id: str
    query_text: str
    run_number: int
    wall_clock_ms: float
    rows_returned: int = 0
    peak_memory_mb: float = 0.0
    error: str | None = None
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class QuerySummary:
    """Aggregated results across runs for a single query."""
    query_id: str
    runs: int
    median_ms: float
    mean_ms: float
    best_ms: float
    worst_ms: float
    rows_returned: int
    peak_memory_mb: float
    all_times_ms: list[float] = field(default_factory=list)


@dataclass
class BenchmarkReport:
    """Complete benchmark report."""
    suite: str
    scale_factor: int
    data_format: str
    engine: str
    engine_version: str
    hardware: dict
    queries: list[QuerySummary]
    total_time_ms: float
    timestamp: str = ""
    config: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------
# Query parser
# ----------------------------------------------------------------

def parse_sql_file(filepath: str) -> list[tuple[str, str]]:
    """Parse a .sql file into (query_id, query_text) pairs.

    Expects queries separated by comments like:
      -- Q1.1: Description
      SELECT ...;

    Returns list of (query_id, sql_text) tuples.
    """
    with open(filepath) as f:
        content = f.read()

    queries = []
    current_id = None
    current_sql = []

    for line in content.split("\n"):
        stripped = line.strip()

        # Detect query ID from comment patterns like "-- Q1.1:", "-- FA-1:", "-- AC-1:"
        id_match = re.match(r'^--\s+(Q[\d.]+|FA-\d+|AC-\d+|HC-\d+|SE-\d+)[:\s]', stripped)
        if id_match:
            # Save previous query if exists
            if current_id and current_sql:
                sql = "\n".join(current_sql).strip().rstrip(";")
                if sql:
                    queries.append((current_id, sql))
            current_id = id_match.group(1)
            current_sql = []
            continue

        # Skip pure comment lines and empty lines at the start
        if stripped.startswith("--") or stripped.startswith("=="):
            continue

        if current_id is not None:
            current_sql.append(line)

    # Don't forget the last query
    if current_id and current_sql:
        sql = "\n".join(current_sql).strip().rstrip(";")
        if sql:
            queries.append((current_id, sql))

    return queries


# ----------------------------------------------------------------
# System info collection
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

    if HAS_PSUTIL:
        mem = psutil.virtual_memory()
        info["ram_total_gb"] = round(mem.total / (1024**3), 2)
        info["ram_available_gb"] = round(mem.available / (1024**3), 2)
        info["cpu_count_physical"] = psutil.cpu_count(logical=False)
        info["cpu_count_logical"] = psutil.cpu_count(logical=True)

        # Detect Raspberry Pi
        try:
            with open("/proc/device-tree/model") as f:
                info["device_model"] = f.read().strip().rstrip("\x00")
        except FileNotFoundError:
            pass

        # Detect storage type
        try:
            disk = psutil.disk_partitions()
            for part in disk:
                if part.mountpoint == "/":
                    info["root_device"] = part.device
                    info["root_fstype"] = part.fstype
        except Exception:
            pass

    return info


def clear_os_cache():
    """Clear OS page cache for cold-start benchmarks.

    Requires root on Linux. Silently skips if not available.
    """
    try:
        subprocess.run(
            ["sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            capture_output=True, timeout=10
        )
        return True
    except (subprocess.TimeoutExpired, PermissionError, FileNotFoundError):
        return False


# ----------------------------------------------------------------
# Benchmark execution (pluggable engine interface)
# ----------------------------------------------------------------

class BenchmarkEngine:
    """Base class for benchmark execution engines.

    Subclass this for Apiary, DuckDB, DataFusion CLI, etc.
    """

    def __init__(self, name: str, version: str = "unknown"):
        self.name = name
        self.version = version

    def setup(self, data_dir: str, **kwargs):
        """Initialize the engine and register tables."""
        raise NotImplementedError

    def execute(self, sql: str) -> tuple[int, float]:
        """Execute a query, return (row_count, peak_memory_mb)."""
        raise NotImplementedError

    def teardown(self):
        """Clean up engine resources."""
        pass


class DataFusionEngine(BenchmarkEngine):
    """Execute queries via DataFusion Python bindings.

    This is the default engine for benchmarking Apiary's SQL layer
    in isolation (before distributed coordination is added).
    """

    def __init__(self):
        try:
            import datafusion
            version = getattr(datafusion, "__version__", "unknown")
        except ImportError:
            version = "not installed"
        super().__init__("DataFusion", version)
        self.ctx = None

    def setup(self, data_dir: str, **kwargs):
        """Register Parquet/CSV/Arrow files as tables."""
        import datafusion
        self.ctx = datafusion.SessionContext()

        data_path = Path(data_dir)
        if not data_path.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")

        # Auto-discover and register tables
        registered = []
        for f in sorted(data_path.rglob("*")):
            if f.suffix in (".parquet", ".csv", ".arrow"):
                table_name = f.stem
                # Handle partitioned datasets (use parent dir name)
                if table_name == "data":
                    table_name = f.parent.parent.name

                if f.suffix == ".parquet":
                    self.ctx.register_parquet(table_name, str(f))
                elif f.suffix == ".csv":
                    self.ctx.register_csv(table_name, str(f))
                registered.append((table_name, f.suffix, str(f)))

        print(f"\nRegistered {len(registered)} tables:")
        for name, fmt, path in registered:
            print(f"  {name} ({fmt})")

        return registered

    def execute(self, sql: str) -> tuple[int, float]:
        """Execute query and return (row_count, peak_memory_mb)."""
        if self.ctx is None:
            raise RuntimeError("Engine not set up. Call setup() first.")

        mem_before = 0
        if HAS_PSUTIL:
            proc = psutil.Process()
            mem_before = proc.memory_info().rss

        result = self.ctx.sql(sql)
        rows = result.collect()
        row_count = sum(batch.num_rows for batch in rows)

        peak_mem = 0
        if HAS_PSUTIL:
            mem_after = proc.memory_info().rss
            peak_mem = max(0, (mem_after - mem_before)) / (1024 * 1024)

        return row_count, peak_mem

    def teardown(self):
        self.ctx = None


class DryRunEngine(BenchmarkEngine):
    """Validates queries without executing them."""

    def __init__(self):
        super().__init__("DryRun", "1.0")

    def setup(self, data_dir: str, **kwargs):
        print(f"\n[DRY RUN] Would register tables from: {data_dir}")
        return []

    def execute(self, sql: str) -> tuple[int, float]:
        return 0, 0.0


# ----------------------------------------------------------------
# Main runner
# ----------------------------------------------------------------

def run_benchmark(
    suite: str,
    query_file: str,
    data_dir: str,
    engine: BenchmarkEngine,
    runs: int = 3,
    warmup: int = 1,
    clear_cache: bool = True,
    query_filter: list[str] | None = None,
    scale_factor: int = 1,
) -> BenchmarkReport:
    """Run a complete benchmark suite and return structured results."""

    # Parse queries
    queries = parse_sql_file(query_file)
    if query_filter:
        queries = [(qid, sql) for qid, sql in queries if qid in query_filter]

    if not queries:
        print(f"No queries found in {query_file}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"Running: {suite} ({len(queries)} queries × {runs} runs)")
    print(f"Engine:  {engine.name} {engine.version}")
    print(f"Data:    {data_dir}")
    print(f"{'='*60}\n")

    # Setup engine
    engine.setup(data_dir)

    # Collect system info
    hw_info = collect_system_info()

    # Detect data format from directory
    data_format = "mixed"
    if "parquet" in data_dir:
        data_format = "parquet"
    elif "csv" in data_dir:
        data_format = "csv"
    elif "arrow" in data_dir:
        data_format = "arrow"

    all_summaries = []
    total_start = time.time()

    for query_id, sql in queries:
        print(f"\n--- {query_id} ---")
        results: list[QueryResult] = []

        # Warmup runs
        for w in range(warmup):
            try:
                engine.execute(sql)
                print(f"  Warmup {w+1}/{warmup}: OK")
            except Exception as e:
                print(f"  Warmup {w+1}/{warmup}: ERROR - {e}")

        # Timed runs
        for r in range(runs):
            if clear_cache:
                clear_os_cache()

            try:
                start = time.perf_counter()
                row_count, peak_mem = engine.execute(sql)
                elapsed_ms = (time.perf_counter() - start) * 1000

                result = QueryResult(
                    query_id=query_id,
                    query_text=sql[:200],
                    run_number=r + 1,
                    wall_clock_ms=round(elapsed_ms, 2),
                    rows_returned=row_count,
                    peak_memory_mb=round(peak_mem, 2),
                )
                results.append(result)
                print(f"  Run {r+1}/{runs}: {elapsed_ms:>8.1f}ms "
                      f"({row_count} rows, {peak_mem:.1f}MB)")

            except Exception as e:
                result = QueryResult(
                    query_id=query_id,
                    query_text=sql[:200],
                    run_number=r + 1,
                    wall_clock_ms=0,
                    error=str(e),
                )
                results.append(result)
                print(f"  Run {r+1}/{runs}: ERROR - {e}")

        # Summarise
        times = [r.wall_clock_ms for r in results if r.error is None]
        if times:
            summary = QuerySummary(
                query_id=query_id,
                runs=len(times),
                median_ms=round(median(times), 2),
                mean_ms=round(mean(times), 2),
                best_ms=round(min(times), 2),
                worst_ms=round(max(times), 2),
                rows_returned=results[0].rows_returned,
                peak_memory_mb=max(r.peak_memory_mb for r in results),
                all_times_ms=times,
            )
        else:
            summary = QuerySummary(
                query_id=query_id, runs=0,
                median_ms=0, mean_ms=0, best_ms=0, worst_ms=0,
                rows_returned=0, peak_memory_mb=0,
            )
        all_summaries.append(summary)

    total_ms = (time.time() - total_start) * 1000

    # Build report
    report = BenchmarkReport(
        suite=suite,
        scale_factor=scale_factor,
        data_format=data_format,
        engine=engine.name,
        engine_version=engine.version,
        hardware=hw_info,
        queries=all_summaries,
        total_time_ms=round(total_ms, 2),
    )

    engine.teardown()
    return report


def print_report(report: BenchmarkReport):
    """Print a formatted report to stdout."""
    print(f"\n{'='*72}")
    print(f"BENCHMARK REPORT: {report.suite}")
    print(f"{'='*72}")
    print(f"Engine:       {report.engine} {report.engine_version}")
    print(f"Scale Factor: {report.scale_factor}")
    print(f"Format:       {report.data_format}")
    print(f"Platform:     {report.hardware.get('platform', 'unknown')}")
    if "device_model" in report.hardware:
        print(f"Device:       {report.hardware['device_model']}")
    print(f"RAM:          {report.hardware.get('ram_total_gb', '?')} GB")
    print(f"CPUs:         {report.hardware.get('cpu_count_logical', '?')}")
    print(f"{'='*72}")

    print(f"\n{'Query':<10} {'Median':>10} {'Best':>10} {'Worst':>10} "
          f"{'Rows':>10} {'Mem MB':>8}")
    print("-" * 72)

    for q in report.queries:
        print(f"{q.query_id:<10} {q.median_ms:>10.1f} {q.best_ms:>10.1f} "
              f"{q.worst_ms:>10.1f} {q.rows_returned:>10} "
              f"{q.peak_memory_mb:>8.1f}")

    # Compute geometric mean of median times (standard TPC-H metric)
    valid_medians = [q.median_ms for q in report.queries if q.median_ms > 0]
    if valid_medians:
        import math
        geomean = math.exp(sum(math.log(t) for t in valid_medians) / len(valid_medians))
        total_median = sum(valid_medians)
        print("-" * 72)
        print(f"{'GeoMean':<10} {geomean:>10.1f}")
        print(f"{'Total':<10} {total_median:>10.1f}")

    print(f"\nWall clock: {report.total_time_ms/1000:.1f}s")
    print(f"{'='*72}\n")


def save_report(report: BenchmarkReport, output_dir: str):
    """Save report as JSON."""
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{report.suite}_{report.engine}_{report.data_format}_sf{report.scale_factor}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(asdict(report), f, indent=2, default=str)

    print(f"Report saved: {filepath}")
    return filepath


# ----------------------------------------------------------------
# CLI
# ----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Apiary Benchmark Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--suite", "-s",
        choices=["ssb", "tpch", "apiary_format", "apiary_acid",
                 "apiary_heterogeneous", "apiary_elasticity"],
        required=True,
        help="Benchmark suite to run"
    )
    parser.add_argument(
        "--data-dir", "-d",
        required=True,
        help="Path to the data directory for this benchmark"
    )
    parser.add_argument(
        "--output", "-o",
        default="./results",
        help="Output directory for results (default: ./results)"
    )
    parser.add_argument(
        "--engine", "-e",
        choices=["datafusion", "dryrun"],
        default="datafusion",
        help="Execution engine (default: datafusion)"
    )
    parser.add_argument(
        "--runs", "-r",
        type=int, default=3,
        help="Number of timed runs per query (default: 3)"
    )
    parser.add_argument(
        "--warmup", "-w",
        type=int, default=1,
        help="Number of warmup runs (default: 1)"
    )
    parser.add_argument(
        "--queries", "-q",
        nargs="+",
        help="Specific query IDs to run (e.g., Q1.1 Q2.3)"
    )
    parser.add_argument(
        "--scale-factor",
        type=int, default=1,
        help="Scale factor for reporting (default: 1)"
    )
    parser.add_argument(
        "--no-cache-clear",
        action="store_true",
        help="Skip clearing OS page cache between runs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse queries and validate config without executing"
    )

    args = parser.parse_args()

    # Determine query file
    query_files = {
        "ssb": "queries/ssb/ssb_queries.sql",
        "tpch": "queries/tpch/tpch_queries.sql",
        "apiary_format": "queries/apiary/apiary_queries.sql",
        "apiary_acid": "queries/apiary/apiary_queries.sql",
        "apiary_heterogeneous": "queries/apiary/apiary_queries.sql",
        "apiary_elasticity": "queries/apiary/apiary_queries.sql",
    }

    # Filter queries by suite prefix
    suite_query_prefix = {
        "apiary_format": ["FA-"],
        "apiary_acid": ["AC-"],
        "apiary_heterogeneous": ["HC-"],
        "apiary_elasticity": ["SE-"],
    }

    query_file = query_files[args.suite]

    # Select engine
    if args.dry_run:
        engine = DryRunEngine()
    elif args.engine == "datafusion":
        engine = DataFusionEngine()
    else:
        engine = DryRunEngine()

    # Build query filter
    query_filter = args.queries
    if not query_filter and args.suite in suite_query_prefix:
        # Auto-filter based on suite
        all_queries = parse_sql_file(query_file)
        prefixes = suite_query_prefix[args.suite]
        query_filter = [qid for qid, _ in all_queries
                        if any(qid.startswith(p) for p in prefixes)]

    # Run benchmark
    report = run_benchmark(
        suite=args.suite,
        query_file=query_file,
        data_dir=args.data_dir,
        engine=engine,
        runs=args.runs,
        warmup=args.warmup,
        clear_cache=not args.no_cache_clear,
        query_filter=query_filter,
        scale_factor=args.scale_factor,
    )

    # Output
    print_report(report)
    save_report(report, args.output)


if __name__ == "__main__":
    main()
