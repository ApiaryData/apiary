#!/usr/bin/env python3
"""Step 7 Acceptance Tests — Distributed Query Execution.

Tests that the distributed query infrastructure is in place and
single-node queries still work.
"""

import os
import sys
import tempfile
import time

import pyarrow as pa


def serialize_table(table: pa.Table) -> bytes:
    """Serialize a PyArrow Table to IPC bytes."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def deserialize_table(data: bytes) -> pa.Table:
    """Deserialize Arrow IPC stream bytes to a PyArrow table."""
    reader = pa.ipc.open_stream(data)
    return reader.read_all()


passed = 0
failed = 0


def check(condition, message):
    global passed, failed
    if condition:
        print(f"  ✓ {message}")
        passed += 1
    else:
        print(f"  ✗ {message}")
        failed += 1


def run_test(name, func):
    global failed
    print(f"\n[{name}]")
    try:
        func()
    except Exception as e:
        import traceback
        print(f"  ✗ EXCEPTION: {e}")
        traceback.print_exc()
        failed += 1


def test_1_single_node_query():
    """Test 1: Single-node queries still work."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    storage_path = os.path.join(tmp, "data")
    ap = Apiary("test_single", storage=f"local://{storage_path}")
    ap.start()
    try:
        # Create and write data
        ap.create_hive("analytics")
        ap.create_box("analytics", "sensors")
        ap.create_frame(
            "analytics", "sensors", "temperature",
            {"device_id": "string", "temp": "float", "region": "string"},
            partition_by=["region"],
        )

        data = pa.table({
            "device_id": ["d1", "d2", "d3"],
            "temp": [22.5, 23.1, 21.8],
            "region": ["north", "north", "south"],
        })
        ipc_data = serialize_table(data)
        result = ap.write_to_frame("analytics", "sensors", "temperature", ipc_data)
        check(result["version"] >= 1, "Write succeeded")

        # Query the data
        results_bytes = ap.sql("SELECT region, COUNT(*) as count FROM analytics.sensors.temperature GROUP BY region")
        results = deserialize_table(results_bytes)
        check(len(results) > 0, "Query returned results")
        df = results.to_pandas()
        check(len(df) == 2, f"Expected 2 regions (got {len(df)})")
        
    finally:
        ap.shutdown()


def test_2_world_view_works():
    """Test 2: World view and heartbeat infrastructure works."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    storage_path = os.path.join(tmp, "data")
    ap = Apiary("test_wv", storage=f"local://{storage_path}")
    ap.start()
    try:
        time.sleep(0.5)  # Give heartbeat writer time
        
        # Check heartbeat directory exists
        heartbeat_dir = os.path.join(storage_path, "_heartbeats")
        check(os.path.isdir(heartbeat_dir), "_heartbeats directory exists")
        
        # Check world view
        status = ap.swarm_status()
        check(status is not None, "swarm_status() works")
        check(len(status.get("nodes", [])) == 1, "World view shows 1 node")
        check(status.get("total_bees", 0) > 0, "Total bees > 0")
        
    finally:
        ap.shutdown()


def test_3_aggregation_query():
    """Test 3: Aggregation queries work."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    storage_path = os.path.join(tmp, "data")
    ap = Apiary("test_agg", storage=f"local://{storage_path}")
    ap.start()
    try:
        # Create and write data
        ap.create_hive("analytics")
        ap.create_box("analytics", "sensors")
        ap.create_frame(
            "analytics", "sensors", "temperature",
            {"device_id": "string", "temp": "float", "region": "string"},
            partition_by=["region"],
        )

        data = pa.table({
            "device_id": ["d1", "d2", "d3", "d4", "d5"],
            "temp": [22.0, 23.0, 21.0, 24.0, 25.0],
            "region": ["north", "north", "south", "south", "south"],
        })
        ipc_data = serialize_table(data)
        result = ap.write_to_frame("analytics", "sensors", "temperature", ipc_data)
        check(result["version"] >= 1, "Write succeeded")

        # Test AVG aggregation
        results_bytes = ap.sql("SELECT region, AVG(temp) as avg_temp FROM analytics.sensors.temperature GROUP BY region")
        results = deserialize_table(results_bytes)
        check(len(results) == 2, "AVG aggregation returns 2 regions")
        df = results.to_pandas()
        north_avg = df[df['region'] == 'north']['avg_temp'].iloc[0]
        check(abs(north_avg - 22.5) < 0.01, f"North avg is correct")
        
    finally:
        ap.shutdown()


def test_4_query_with_filter():
    """Test 4: WHERE clause filtering works."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    storage_path = os.path.join(tmp, "data")
    ap = Apiary("test_filter", storage=f"local://{storage_path}")
    ap.start()
    try:
        # Create and write data
        ap.create_hive("analytics")
        ap.create_box("analytics", "sensors")
        ap.create_frame(
            "analytics", "sensors", "temperature",
            {"device_id": "string", "temp": "float", "region": "string"},
            partition_by=["region"],
        )

        data = pa.table({
            "device_id": ["d1", "d2", "d3", "d4"],
            "temp": [22.0, 28.0, 21.0, 30.0],
            "region": ["north", "north", "south", "south"],
        })
        ipc_data = serialize_table(data)
        ap.write_to_frame("analytics", "sensors", "temperature", ipc_data)

        # Query with filter
        results_bytes = ap.sql("SELECT * FROM analytics.sensors.temperature WHERE temp > 25.0")
        results = deserialize_table(results_bytes)
        check(len(results) == 2, f"Filter returns correct count")
        df = results.to_pandas()
        check(all(df['temp'] > 25.0), "All results match filter")
        
    finally:
        ap.shutdown()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Step 7 Acceptance Tests — Distributed Query Execution")
    print("=" * 70)

    run_test("Test 1: Single-node query", test_1_single_node_query)
    run_test("Test 2: World view works", test_2_world_view_works)
    run_test("Test 3: Aggregation query", test_3_aggregation_query)
    run_test("Test 4: Query with filter", test_4_query_with_filter)

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)

    sys.exit(0 if failed == 0 else 1)
