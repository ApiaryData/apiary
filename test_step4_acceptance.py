#!/usr/bin/env python3
"""Step 4 Acceptance Tests — DataFusion Integration.

Tests SQL queries over Apiary frames using DataFusion, including:
1. SELECT * with fully qualified table names
2. Partition pruning via WHERE clause
3. Cell stat pruning via numeric WHERE clause
4. Projection pushdown (SELECT specific columns)
5. Aggregation (GROUP BY, AVG, SUM, COUNT, MIN, MAX)
6. USE HIVE / USE BOX context setting
7. SHOW HIVES / SHOW BOXES / SHOW FRAMES
8. DESCRIBE frame metadata
9. DELETE / UPDATE error messages
10. Solo mode end-to-end: create → write → SQL query
"""

import os
import sys
import tempfile
import traceback

import pyarrow as pa


def ipc_serialize(table: pa.Table) -> bytes:
    """Serialize a PyArrow Table to IPC stream bytes."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def ipc_deserialize(data: bytes) -> pa.Table:
    """Deserialize IPC stream bytes to a PyArrow Table."""
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
        print(f"  ✗ EXCEPTION: {e}")
        traceback.print_exc()
        failed += 1


def setup_apiary():
    """Create an Apiary instance with test data."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    ap = Apiary("test_sql", storage=f"local://{tmp}/data")
    ap.start()

    # Create namespace
    ap.create_hive("analytics")
    ap.create_box("analytics", "sensors")
    ap.create_frame(
        "analytics",
        "sensors",
        "temperature",
        {"region": "string", "temp": "float64", "humidity": "int64"},
        partition_by=["region"],
    )

    # Write test data: north region
    north_table = pa.table(
        {
            "region": pa.array(["north", "north", "north"], type=pa.string()),
            "temp": pa.array([10.0, 15.0, 20.0], type=pa.float64()),
            "humidity": pa.array([50, 55, 60], type=pa.int64()),
        }
    )
    ap.write_to_frame("analytics", "sensors", "temperature", ipc_serialize(north_table))

    # Write test data: south region
    south_table = pa.table(
        {
            "region": pa.array(["south", "south"], type=pa.string()),
            "temp": pa.array([30.0, 40.0], type=pa.float64()),
            "humidity": pa.array([70, 80], type=pa.int64()),
        }
    )
    ap.write_to_frame("analytics", "sensors", "temperature", ipc_serialize(south_table))

    return ap, tmp


# ---- Tests ----


def test_1_select_all():
    """Test 1: SELECT * with fully qualified name works end-to-end."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql("SELECT * FROM analytics.sensors.temperature")
        check(result is not None, "SQL result is not None")
        table = ipc_deserialize(result)
        check(table.num_rows == 5, f"SELECT * returns all 5 rows (got {table.num_rows})")
        check(
            table.num_columns == 3,
            f"SELECT * returns 3 columns (got {table.num_columns})",
        )
    finally:
        ap.shutdown()


def test_2_partition_pruning():
    """Test 2: WHERE on partition column prunes cells."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql(
            "SELECT * FROM analytics.sensors.temperature WHERE region = 'north'"
        )
        table = ipc_deserialize(result)
        check(
            table.num_rows == 3,
            f"Partition pruning: 3 north rows (got {table.num_rows})",
        )
        # Verify all rows are north
        regions = table.column("region").to_pylist()
        check(
            all(r == "north" for r in regions),
            "All rows have region='north'",
        )
    finally:
        ap.shutdown()


def test_3_stat_pruning():
    """Test 3: WHERE on numeric column uses cell stats for pruning."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql(
            "SELECT * FROM analytics.sensors.temperature WHERE temp > 25"
        )
        table = ipc_deserialize(result)
        temps = table.column("temp").to_pylist()
        check(
            all(t > 25 for t in temps),
            f"Stat-filtered rows all have temp > 25: {temps}",
        )
        check(
            len(temps) == 2,
            f"Stat pruning: 2 rows with temp > 25 (got {len(temps)})",
        )
    finally:
        ap.shutdown()


def test_4_projection():
    """Test 4: SELECT specific columns only returns those columns."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql("SELECT temp FROM analytics.sensors.temperature")
        table = ipc_deserialize(result)
        check(
            table.num_columns == 1,
            f"Projection: 1 column (got {table.num_columns})",
        )
        check(
            table.schema.field(0).name == "temp",
            f"Projected column is 'temp' (got {table.schema.field(0).name})",
        )
    finally:
        ap.shutdown()


def test_5_aggregation():
    """Test 5: GROUP BY with aggregation functions."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql(
            "SELECT region, AVG(temp) as avg_temp, COUNT(*) as cnt "
            "FROM analytics.sensors.temperature GROUP BY region ORDER BY region"
        )
        table = ipc_deserialize(result)
        check(
            table.num_rows == 2,
            f"GROUP BY produces 2 groups (got {table.num_rows})",
        )
        regions = table.column("region").to_pylist()
        check(
            "north" in regions and "south" in regions,
            f"Groups are north and south (got {regions})",
        )

        # Check aggregation values
        avg_temps = table.column("avg_temp").to_pylist()
        counts = table.column("cnt").to_pylist()
        north_idx = regions.index("north")
        south_idx = regions.index("south")
        check(
            abs(avg_temps[north_idx] - 15.0) < 0.01,
            f"North avg temp ≈ 15.0 (got {avg_temps[north_idx]})",
        )
        check(
            abs(avg_temps[south_idx] - 35.0) < 0.01,
            f"South avg temp ≈ 35.0 (got {avg_temps[south_idx]})",
        )
        check(
            counts[north_idx] == 3,
            f"North count = 3 (got {counts[north_idx]})",
        )
        check(
            counts[south_idx] == 2,
            f"South count = 2 (got {counts[south_idx]})",
        )
    finally:
        ap.shutdown()


def test_6_use_hive_and_box():
    """Test 6: USE HIVE / USE BOX set context for unqualified names."""
    ap, tmp = setup_apiary()
    try:
        ap.sql("USE HIVE analytics")
        ap.sql("USE BOX sensors")

        result = ap.sql("SELECT * FROM temperature")
        table = ipc_deserialize(result)
        check(
            table.num_rows == 5,
            f"Unqualified query after USE: 5 rows (got {table.num_rows})",
        )

        # Test with filter using short name
        result = ap.sql("SELECT * FROM temperature WHERE region = 'south'")
        table = ipc_deserialize(result)
        check(
            table.num_rows == 2,
            f"Unqualified query with filter: 2 rows (got {table.num_rows})",
        )
    finally:
        ap.shutdown()


def test_7_show_commands():
    """Test 7: SHOW HIVES / SHOW BOXES / SHOW FRAMES return correct results."""
    ap, tmp = setup_apiary()
    try:
        # SHOW HIVES
        result = ap.sql("SHOW HIVES")
        table = ipc_deserialize(result)
        check(table.num_rows >= 1, f"SHOW HIVES returns ≥1 row (got {table.num_rows})")
        hives = table.column("hive").to_pylist()
        check("analytics" in hives, f"SHOW HIVES includes 'analytics' (got {hives})")

        # SHOW BOXES
        result = ap.sql("SHOW BOXES IN analytics")
        table = ipc_deserialize(result)
        check(
            table.num_rows >= 1,
            f"SHOW BOXES returns ≥1 row (got {table.num_rows})",
        )
        boxes = table.column("box").to_pylist()
        check("sensors" in boxes, f"SHOW BOXES includes 'sensors' (got {boxes})")

        # SHOW FRAMES
        result = ap.sql("SHOW FRAMES IN analytics.sensors")
        table = ipc_deserialize(result)
        check(
            table.num_rows >= 1,
            f"SHOW FRAMES returns ≥1 row (got {table.num_rows})",
        )
        frames = table.column("frame").to_pylist()
        check(
            "temperature" in frames,
            f"SHOW FRAMES includes 'temperature' (got {frames})",
        )
    finally:
        ap.shutdown()


def test_8_describe():
    """Test 8: DESCRIBE returns frame metadata."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql("DESCRIBE analytics.sensors.temperature")
        table = ipc_deserialize(result)
        check(table.num_rows >= 3, f"DESCRIBE returns ≥3 rows (got {table.num_rows})")

        properties = table.column("property").to_pylist()
        values = table.column("value").to_pylist()

        check("schema" in properties, "DESCRIBE includes 'schema'")
        check("partition_by" in properties, "DESCRIBE includes 'partition_by'")
        check("total_rows" in properties, "DESCRIBE includes 'total_rows'")

        # Check total_rows value
        rows_idx = properties.index("total_rows")
        check(
            values[rows_idx] == "5",
            f"DESCRIBE total_rows = 5 (got {values[rows_idx]})",
        )
    finally:
        ap.shutdown()


def test_9_dml_errors():
    """Test 9: DELETE / UPDATE produce clear error messages."""
    ap, tmp = setup_apiary()
    try:
        # DELETE
        try:
            ap.sql("DELETE FROM analytics.sensors.temperature")
            check(False, "DELETE should raise an error")
        except RuntimeError as e:
            err_msg = str(e)
            check("not supported" in err_msg.lower(), f"DELETE error is clear: {err_msg}")

        # UPDATE
        try:
            ap.sql("UPDATE analytics.sensors.temperature SET temp = 0")
            check(False, "UPDATE should raise an error")
        except RuntimeError as e:
            err_msg = str(e)
            check("not supported" in err_msg.lower(), f"UPDATE error is clear: {err_msg}")
    finally:
        ap.shutdown()


def test_10_solo_mode_e2e():
    """Test 10: Solo mode end-to-end — create, write, query on a single node."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    ap = Apiary("solo_test", storage=f"local://{tmp}/data")
    ap.start()
    try:
        # Create namespace
        ap.create_hive("mydb")
        ap.create_box("mydb", "myschema")
        ap.create_frame(
            "mydb",
            "myschema",
            "events",
            {"event_type": "string", "count": "int64"},
        )

        # Write data
        table = pa.table(
            {
                "event_type": pa.array(["click", "view", "click", "purchase"], type=pa.string()),
                "count": pa.array([10, 20, 15, 5], type=pa.int64()),
            }
        )
        ap.write_to_frame("mydb", "myschema", "events", ipc_serialize(table))

        # Query with SQL
        result = ap.sql(
            "SELECT event_type, SUM(count) as total "
            "FROM mydb.myschema.events "
            "GROUP BY event_type ORDER BY total DESC"
        )
        result_table = ipc_deserialize(result)
        check(
            result_table.num_rows == 3,
            f"Solo E2E: 3 event types (got {result_table.num_rows})",
        )

        event_types = result_table.column("event_type").to_pylist()
        totals = result_table.column("total").to_pylist()
        check(
            event_types[0] == "click" and totals[0] == 25,
            f"Click total = 25 (got {event_types[0]}={totals[0]})",
        )
        check(
            "view" in event_types and "purchase" in event_types,
            "All event types present in results",
        )
    finally:
        ap.shutdown()


def test_11_order_by_and_limit():
    """Test 11: ORDER BY and LIMIT work correctly."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql(
            "SELECT temp FROM analytics.sensors.temperature ORDER BY temp DESC LIMIT 2"
        )
        table = ipc_deserialize(result)
        check(
            table.num_rows == 2,
            f"LIMIT 2 returns 2 rows (got {table.num_rows})",
        )
        temps = table.column("temp").to_pylist()
        check(
            temps[0] == 40.0 and temps[1] == 30.0,
            f"ORDER BY DESC: top 2 = [40, 30] (got {temps})",
        )
    finally:
        ap.shutdown()


# ---- Main ----

if __name__ == "__main__":
    print("=" * 60)
    print("Step 4 Acceptance Criteria Verification")
    print("=" * 60)

    run_test("Test 1: SELECT * End-to-End", test_1_select_all)
    run_test("Test 2: Partition Pruning", test_2_partition_pruning)
    run_test("Test 3: Cell Stat Pruning", test_3_stat_pruning)
    run_test("Test 4: Projection Pushdown", test_4_projection)
    run_test("Test 5: Aggregation (GROUP BY)", test_5_aggregation)
    run_test("Test 6: USE HIVE / USE BOX", test_6_use_hive_and_box)
    run_test("Test 7: SHOW Commands", test_7_show_commands)
    run_test("Test 8: DESCRIBE", test_8_describe)
    run_test("Test 9: DELETE / UPDATE Errors", test_9_dml_errors)
    run_test("Test 10: Solo Mode E2E", test_10_solo_mode_e2e)
    run_test("Test 11: ORDER BY + LIMIT", test_11_order_by_and_limit)

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'=' * 60}")

    if failed == 0:
        print("\n✅ All Step 4 acceptance criteria met!")
    else:
        print(f"\n❌ {failed} check(s) failed")

    sys.exit(0 if failed == 0 else 1)
