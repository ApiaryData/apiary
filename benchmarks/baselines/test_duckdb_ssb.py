#!/usr/bin/env python3
"""
Tests for benchmarks/baselines/duckdb_ssb.py

Validates the DuckDB SSB baseline benchmark script without requiring
the full SF1 dataset. Uses a tiny synthetic dataset to exercise
the query parsing, table registration, benchmark loop, and reporting.
"""

import json
import math
import os
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Ensure the benchmarks package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from baselines.duckdb_ssb import (
    parse_sql_file,
    register_tables,
    execute_query,
    run_benchmark,
    print_report,
    save_report,
    collect_system_info,
    main,
)


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------

def _write_tiny_ssb(tmpdir: str):
    """Write minimal SSB Parquet files for testing."""
    date_table = pa.table({
        "d_datekey": pa.array([19930101, 19940101, 19940201, 19970101, 19971201], pa.int32()),
        "d_year": pa.array([1993, 1994, 1994, 1997, 1997], pa.int32()),
        "d_yearmonthnum": pa.array([199301, 199401, 199402, 199701, 199712], pa.int32()),
        "d_yearmonth": pa.array(["Jan1993", "Jan1994", "Feb1994", "Jan1997", "Dec1997"]),
        "d_weeknuminyear": pa.array([1, 1, 6, 1, 50], pa.int32()),
    })
    customer_table = pa.table({
        "c_custkey": pa.array([1, 2], pa.int32()),
        "c_nation": pa.array(["UNITED STATES", "CHINA"]),
        "c_region": pa.array(["AMERICA", "ASIA"]),
        "c_city": pa.array(["UNITED KI1", "CHINA    1"]),
    })
    supplier_table = pa.table({
        "s_suppkey": pa.array([1, 2], pa.int32()),
        "s_nation": pa.array(["UNITED STATES", "CHINA"]),
        "s_region": pa.array(["AMERICA", "ASIA"]),
        "s_city": pa.array(["UNITED KI1", "CHINA    1"]),
    })
    part_table = pa.table({
        "p_partkey": pa.array([1, 2], pa.int32()),
        "p_mfgr": pa.array(["MFGR#1", "MFGR#2"]),
        "p_category": pa.array(["MFGR#12", "MFGR#14"]),
        "p_brand": pa.array(["MFGR#2239", "MFGR#2221"]),
    })
    lineorder_table = pa.table({
        "lo_orderdate": pa.array([19930101, 19940101], pa.int32()),
        "lo_custkey": pa.array([1, 2], pa.int32()),
        "lo_suppkey": pa.array([1, 2], pa.int32()),
        "lo_partkey": pa.array([1, 2], pa.int32()),
        "lo_revenue": pa.array([1000, 2000], pa.int64()),
        "lo_extendedprice": pa.array([1200, 2400], pa.int64()),
        "lo_discount": pa.array([2, 5], pa.int32()),
        "lo_quantity": pa.array([10, 30], pa.int32()),
        "lo_supplycost": pa.array([100, 200], pa.int64()),
    })

    pq.write_table(date_table, os.path.join(tmpdir, "date.parquet"))
    pq.write_table(customer_table, os.path.join(tmpdir, "customer.parquet"))
    pq.write_table(supplier_table, os.path.join(tmpdir, "supplier.parquet"))
    pq.write_table(part_table, os.path.join(tmpdir, "part.parquet"))
    pq.write_table(lineorder_table, os.path.join(tmpdir, "lineorder.parquet"))


# ----------------------------------------------------------------
# Tests
# ----------------------------------------------------------------

def test_parse_sql_file():
    """parse_sql_file extracts all 13 SSB queries."""
    query_file = os.path.join(
        os.path.dirname(__file__), "..", "queries", "ssb", "ssb_queries.sql"
    )
    queries = parse_sql_file(query_file)
    assert len(queries) == 13, f"Expected 13 queries, got {len(queries)}"

    expected_ids = [
        "Q1.1", "Q1.2", "Q1.3",
        "Q2.1", "Q2.2", "Q2.3",
        "Q3.1", "Q3.2", "Q3.3", "Q3.4",
        "Q4.1", "Q4.2", "Q4.3",
    ]
    actual_ids = [qid for qid, _ in queries]
    assert actual_ids == expected_ids, f"Query IDs mismatch: {actual_ids}"
    print("✓ parse_sql_file: found all 13 SSB queries")


def test_register_tables():
    """register_tables loads Parquet files into DuckDB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_tiny_ssb(tmpdir)
        con = duckdb.connect(":memory:")
        registered = register_tables(con, tmpdir)
        names = sorted(name for name, _ in registered)
        assert "lineorder" in names
        assert "date" in names
        assert "customer" in names
        assert len(registered) == 5, f"Expected 5 tables, got {len(registered)}"
        con.close()
    print("✓ register_tables: 5 tables loaded")


def test_execute_query():
    """execute_query returns correct row count."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_tiny_ssb(tmpdir)
        con = duckdb.connect(":memory:")
        register_tables(con, tmpdir)

        count = execute_query(con, "SELECT COUNT(*) FROM lineorder")
        assert count == 1  # COUNT(*) returns 1 row

        count = execute_query(con, "SELECT * FROM customer")
        assert count == 2  # 2 customer rows
        con.close()
    print("✓ execute_query: row counts correct")


def test_run_benchmark_structure():
    """run_benchmark returns properly structured summaries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_tiny_ssb(tmpdir)
        con = duckdb.connect(":memory:")
        register_tables(con, tmpdir)

        queries = [("T1", "SELECT COUNT(*) FROM lineorder")]
        summaries = run_benchmark(con, queries, runs=2, warmup=1)

        assert len(summaries) == 1
        s = summaries[0]
        assert s["query_id"] == "T1"
        assert s["runs"] == 2
        assert s["median_ms"] > 0
        assert s["best_ms"] > 0
        assert s["worst_ms"] >= s["best_ms"]
        assert s["rows_returned"] == 1
        assert len(s["all_times_ms"]) == 2
        con.close()
    print("✓ run_benchmark: summary structure correct")


def test_run_benchmark_geomean_and_total():
    """Geometric mean and total are computed from valid medians."""
    summaries = [
        {"query_id": "Q1", "median_ms": 10.0, "best_ms": 9.0,
         "worst_ms": 11.0, "rows_returned": 1, "runs": 3,
         "mean_ms": 10.0, "all_times_ms": [9.0, 10.0, 11.0]},
        {"query_id": "Q2", "median_ms": 40.0, "best_ms": 38.0,
         "worst_ms": 42.0, "rows_returned": 5, "runs": 3,
         "mean_ms": 40.0, "all_times_ms": [38.0, 40.0, 42.0]},
    ]
    valid = [s["median_ms"] for s in summaries if s["median_ms"] > 0]
    geomean = math.exp(sum(math.log(t) for t in valid) / len(valid))
    total = sum(valid)

    assert abs(geomean - 20.0) < 0.01  # sqrt(10*40) = 20
    assert abs(total - 50.0) < 0.01
    print("✓ geomean and total computation correct")


def test_save_report_json():
    """save_report writes valid JSON with expected keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        summaries = [
            {"query_id": "Q1.1", "median_ms": 15.0, "best_ms": 14.0,
             "worst_ms": 16.0, "rows_returned": 1, "runs": 3,
             "mean_ms": 15.0, "all_times_ms": [14.0, 15.0, 16.0]},
        ]
        filepath = save_report(
            summaries,
            engine_version="0.0.0-test",
            scale_factor=1,
            data_format="parquet",
            hardware={"platform": "test"},
            total_wall_ms=100.0,
            output_dir=tmpdir,
        )
        assert os.path.isfile(filepath)
        with open(filepath) as f:
            data = json.load(f)
        assert data["suite"] == "ssb"
        assert data["engine"] == "DuckDB"
        assert data["engine_version"] == "0.0.0-test"
        assert data["scale_factor"] == 1
        assert len(data["queries"]) == 1
        assert "geomean_ms" in data
        assert "total_time_ms" in data
    print("✓ save_report: JSON output valid")


def test_collect_system_info():
    """collect_system_info returns a dict with required keys."""
    info = collect_system_info()
    assert "platform" in info
    assert "python_version" in info
    print("✓ collect_system_info: basic keys present")


def test_main_cli():
    """main() runs end-to-end with tiny data."""
    with tempfile.TemporaryDirectory() as data_dir, \
         tempfile.TemporaryDirectory() as out_dir:
        _write_tiny_ssb(data_dir)

        # Use only Q1.1 for speed; point --query-file at the real SQL
        query_file = os.path.join(
            os.path.dirname(__file__), "..", "queries", "ssb", "ssb_queries.sql"
        )
        main([
            "--data-dir", data_dir,
            "--output", out_dir,
            "--queries", "Q1.1",
            "--runs", "2",
            "--warmup", "1",
            "--query-file", query_file,
        ])

        # Check output was produced
        files = os.listdir(out_dir)
        assert any(f.endswith(".json") for f in files), f"No JSON output: {files}"
    print("✓ main CLI: end-to-end run succeeded")


# ----------------------------------------------------------------
# Runner
# ----------------------------------------------------------------

def main_test():
    print("=" * 60)
    print("DuckDB SSB Baseline — Unit Tests")
    print("=" * 60)

    test_parse_sql_file()
    test_register_tables()
    test_execute_query()
    test_run_benchmark_structure()
    test_run_benchmark_geomean_and_total()
    test_save_report_json()
    test_collect_system_info()
    test_main_cli()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED ✓")
    print("=" * 60)


if __name__ == "__main__":
    main_test()
