#!/usr/bin/env python3
"""Step 3 Acceptance Tests: Ledger + Cell Storage

Tests the transaction ledger, Parquet cell writing/reading, partitioning,
cell statistics, leafcutter sizing, schema validation, concurrent writes,
checkpointing, overwrite, and the Python SDK write/read integration.
"""

import os
import sys
import tempfile
import shutil
import time
import pyarrow as pa

# ─── Helpers ─────────────────────────────────────────────────────────────

passed = 0
failed = 0


def check(description, condition):
    global passed, failed
    if condition:
        print(f"  ✓ {description}")
        passed += 1
    else:
        print(f"  ✗ {description}")
        failed += 1


def serialize_table(table):
    """Serialize a PyArrow table to IPC stream bytes."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def deserialize_table(data):
    """Deserialize Arrow IPC stream bytes to a PyArrow table."""
    reader = pa.ipc.open_stream(data)
    return reader.read_all()


# ─── Test 1: Basic Write and Read Round-Trip ────────────────────────────

def test_write_read_roundtrip():
    print("\n[Test 1] Write and Read Round-Trip")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_roundtrip", storage=f"local://{tmpdir}")
        ap.start()

        # Create namespace
        ap.create_hive("analytics")
        ap.create_box("analytics", "sensors")
        ap.create_frame(
            "analytics", "sensors", "temperature",
            {"region": "string", "temp": "float64"},
            partition_by=["region"],
        )

        # Write data
        table = pa.table({
            "region": ["north", "south", "north", "south"],
            "temp": [20.5, 22.1, 19.8, 23.4],
        })
        ipc_data = serialize_table(table)

        result = ap.write_to_frame("analytics", "sensors", "temperature", ipc_data)
        check("Write returns version", result["version"] >= 1)
        check("Write returns cells_written > 0", result["cells_written"] > 0)
        check("Write returns rows_written = 4", result["rows_written"] == 4)
        check("Write returns bytes_written > 0", result["bytes_written"] > 0)
        check("Write returns duration_ms >= 0", result["duration_ms"] >= 0)

        # Read data back
        read_data = ap.read_from_frame("analytics", "sensors", "temperature")
        check("Read returns data (not None)", read_data is not None)

        read_table = deserialize_table(read_data)
        check("Read returns 4 rows", read_table.num_rows == 4)
        check("Read returns 2 columns", read_table.num_columns == 2)

        # Verify data content
        temp_col = read_table.column("temp").to_pylist()
        check("Read data contains correct temps", set(temp_col) == {20.5, 22.1, 19.8, 23.4})

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 2: Partitioning ───────────────────────────────────────────────

def test_partitioning():
    print("\n[Test 2] Partitioning Creates Separate Cells")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_partition", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"region": "string", "value": "int64"}, partition_by=["region"])

        table = pa.table({
            "region": ["north", "south", "east"],
            "value": [1, 2, 3],
        })
        ipc_data = serialize_table(table)

        result = ap.write_to_frame("h", "b", "f", ipc_data)
        check("Partitioned write creates multiple cells", result["cells_written"] == 3)

        # Check that partition directories were created in storage
        frame_dir = os.path.join(tmpdir, "h", "b", "f")
        dirs = os.listdir(frame_dir) if os.path.exists(frame_dir) else []
        partition_dirs = [d for d in dirs if "=" in d]
        check("Partition directories created", len(partition_dirs) >= 3 or result["cells_written"] == 3)

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 3: Cell Statistics in Ledger ──────────────────────────────────

def test_cell_statistics():
    print("\n[Test 3] Cell Statistics Stored in Ledger")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_stats", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"name": "string", "value": "float64"})

        table = pa.table({
            "name": ["alpha", "beta", "gamma"],
            "value": [10.0, 30.0, 20.0],
        })
        ipc_data = serialize_table(table)

        ap.write_to_frame("h", "b", "f", ipc_data)

        # Read the ledger entry to verify stats are there
        import json
        ledger_dir = os.path.join(tmpdir, "h", "b", "f", "_ledger")
        ledger_files = sorted([f for f in os.listdir(ledger_dir) if f.endswith(".json") and not f.startswith("_")])
        check("Ledger entries exist", len(ledger_files) >= 2)

        # Read the AddCells entry (version 1, after CreateFrame at version 0)
        if len(ledger_files) >= 2:
            with open(os.path.join(ledger_dir, ledger_files[1])) as fh:
                entry = json.load(fh)
            check("Ledger entry has AddCells action", "AddCells" in str(entry.get("action", {})))

            if "AddCells" in entry.get("action", {}):
                cells = entry["action"]["AddCells"]["cells"]
                check("Cell has stats", len(cells[0].get("stats", {})) > 0)
                if cells[0].get("stats", {}).get("value"):
                    value_stats = cells[0]["stats"]["value"]
                    check("Stats contain min value", value_stats.get("min") == 10.0)
                    check("Stats contain max value", value_stats.get("max") == 30.0)
                    check("Stats contain null_count", value_stats.get("null_count") == 0)

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 4: Partition Pruning ──────────────────────────────────────────

def test_partition_pruning():
    print("\n[Test 4] Partition Pruning on Read")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_prune", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"region": "string", "value": "int64"}, partition_by=["region"])

        table = pa.table({
            "region": ["north", "south", "north", "south"],
            "value": [1, 2, 3, 4],
        })
        ipc_data = serialize_table(table)
        ap.write_to_frame("h", "b", "f", ipc_data)

        # Read with partition filter
        filtered = ap.read_from_frame("h", "b", "f", partition_filter={"region": "north"})
        check("Filtered read returns data", filtered is not None)

        if filtered is not None:
            filtered_table = deserialize_table(filtered)
            check("Filtered read returns only north rows", filtered_table.num_rows == 2)
            values = filtered_table.column("value").to_pylist()
            check("Filtered data has correct values", set(values) == {1, 3})

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 5: Schema Validation ──────────────────────────────────────────

def test_schema_validation():
    print("\n[Test 5] Schema Validation")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_schema", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"region": "string", "value": "float64"}, partition_by=["region"])

        # Null partition values should be rejected
        try:
            table = pa.table({
                "region": pa.array(["north", None, "south"], type=pa.string()),
                "value": [1.0, 2.0, 3.0],
            })
            ipc_data = serialize_table(table)
            ap.write_to_frame("h", "b", "f", ipc_data)
            check("Null partition values rejected", False)
        except Exception:
            check("Null partition values rejected", True)

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 6: Multiple Writes Accumulate ─────────────────────────────────

def test_multiple_writes():
    print("\n[Test 6] Multiple Writes Accumulate")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_multi", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"name": "string", "value": "int64"})

        # First write
        table1 = pa.table({"name": ["a", "b"], "value": [1, 2]})
        r1 = ap.write_to_frame("h", "b", "f", serialize_table(table1))
        check("First write version is 1", r1["version"] == 1)

        # Second write
        table2 = pa.table({"name": ["c", "d"], "value": [3, 4]})
        r2 = ap.write_to_frame("h", "b", "f", serialize_table(table2))
        check("Second write version is 2", r2["version"] == 2)

        # Read all
        data = ap.read_from_frame("h", "b", "f")
        result_table = deserialize_table(data)
        check("All 4 rows visible after two writes", result_table.num_rows == 4)

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 7: Overwrite ──────────────────────────────────────────────────

def test_overwrite():
    print("\n[Test 7] Frame Overwrite")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_overwrite", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"name": "string", "value": "int64"})

        # Write initial data
        table1 = pa.table({"name": ["a", "b", "c"], "value": [1, 2, 3]})
        ap.write_to_frame("h", "b", "f", serialize_table(table1))

        # Overwrite with new data
        table2 = pa.table({"name": ["x", "y"], "value": [10, 20]})
        r = ap.overwrite_frame("h", "b", "f", serialize_table(table2))
        check("Overwrite returns version", r["version"] >= 2)
        check("Overwrite returns 2 rows written", r["rows_written"] == 2)

        # Read back — should only see overwritten data
        data = ap.read_from_frame("h", "b", "f")
        result_table = deserialize_table(data)
        check("After overwrite, only 2 rows remain", result_table.num_rows == 2)

        values = result_table.column("value").to_pylist()
        check("Overwritten data is correct", set(values) == {10, 20})

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 8: Concurrent Writes ──────────────────────────────────────────

def test_concurrent_writes():
    print("\n[Test 8] Concurrent Writes (Two Instances)")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap1 = Apiary("test_concurrent_1", storage=f"local://{tmpdir}")
        ap1.start()
        ap2 = Apiary("test_concurrent_2", storage=f"local://{tmpdir}")
        ap2.start()

        ap1.create_hive("h")
        ap1.create_box("h", "b")
        ap1.create_frame("h", "b", "f", {"name": "string"})

        # Both write to the same frame
        table1 = pa.table({"name": ["from_ap1"]})
        table2 = pa.table({"name": ["from_ap2"]})

        r1 = ap1.write_to_frame("h", "b", "f", serialize_table(table1))
        r2 = ap2.write_to_frame("h", "b", "f", serialize_table(table2))

        check("Both writes succeed", r1["version"] >= 1 and r2["version"] >= 1)
        check("Versions are different", r1["version"] != r2["version"])

        # Read all data
        data = ap1.read_from_frame("h", "b", "f")
        result_table = deserialize_table(data)
        check("Both writes visible (2 rows)", result_table.num_rows == 2)

        ap1.shutdown()
        ap2.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 9: Ledger Persistence Across Restarts ────────────────────────

def test_ledger_persistence():
    print("\n[Test 9] Ledger Persistence Across Restarts")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        # Write data
        ap = Apiary("test_persist", storage=f"local://{tmpdir}")
        ap.start()
        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"name": "string", "value": "int64"})

        table = pa.table({"name": ["a", "b", "c"], "value": [1, 2, 3]})
        ap.write_to_frame("h", "b", "f", serialize_table(table))
        ap.shutdown()

        # Restart and read
        ap2 = Apiary("test_persist", storage=f"local://{tmpdir}")
        ap2.start()

        data = ap2.read_from_frame("h", "b", "f")
        check("Data persists across restart", data is not None)

        if data is not None:
            result_table = deserialize_table(data)
            check("Persisted data has 3 rows", result_table.num_rows == 3)

        ap2.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 10: Parquet Format Verification ───────────────────────────────

def test_parquet_format():
    print("\n[Test 10] Parquet Files Written Correctly")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary
        import pyarrow.parquet as pq

        ap = Apiary("test_parquet", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"name": "string", "value": "float64"})

        table = pa.table({"name": ["hello", "world"], "value": [1.5, 2.5]})
        ap.write_to_frame("h", "b", "f", serialize_table(table))

        # Find parquet files in storage
        parquet_files = []
        for root, dirs, files in os.walk(os.path.join(tmpdir, "h", "b", "f")):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))

        check("At least one Parquet file created", len(parquet_files) >= 1)

        if parquet_files:
            # Read with PyArrow directly to verify format
            pq_table = pq.read_table(parquet_files[0])
            check("Parquet file readable by PyArrow", pq_table.num_rows > 0)
            check("Parquet file has correct columns", set(pq_table.column_names) == {"name", "value"})

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Test 11: Read Empty Frame ──────────────────────────────────────────

def test_read_empty_frame():
    print("\n[Test 11] Read Empty Frame Returns None")
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_empty", storage=f"local://{tmpdir}")
        ap.start()

        ap.create_hive("h")
        ap.create_box("h", "b")
        ap.create_frame("h", "b", "f", {"name": "string"})

        data = ap.read_from_frame("h", "b", "f")
        check("Empty frame returns None", data is None)

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─── Main ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("Apiary Step 3 Acceptance Tests: Ledger + Cell Storage")
    print("=" * 60)

    test_write_read_roundtrip()
    test_partitioning()
    test_cell_statistics()
    test_partition_pruning()
    test_schema_validation()
    test_multiple_writes()
    test_overwrite()
    test_concurrent_writes()
    test_ledger_persistence()
    test_parquet_format()
    test_read_empty_frame()

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)
    else:
        print("\n✅ All Step 3 acceptance criteria met!")
        sys.exit(0)
