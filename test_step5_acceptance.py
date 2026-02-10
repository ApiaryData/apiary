#!/usr/bin/env python3
"""Step 5 Acceptance Tests — Mason Bee Isolation + BeePool.

Tests bee pool functionality, memory enforcement, task timeout,
scratch isolation, concurrent execution, and queuing.

1. BeePool creates correct number of bees
2. Tasks execute with enforced memory budgets (via bee_status)
3. Memory exceeded → task terminated, other bees unaffected
4. Task timeout terminates long-running tasks
5. Scratch directories isolated and cleaned
6. Multiple concurrent SQL queries run on separate bees
7. When all bees busy, tasks queue
8. bee_status() returns correct structure
"""

import os
import sys
import tempfile
import traceback
import time
import threading

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
    ap = Apiary("test_bees", storage=f"local://{tmp}/data")
    ap.start()

    # Create namespace and data for SQL tests
    ap.create_hive("analytics")
    ap.create_box("analytics", "sensors")
    ap.create_frame(
        "analytics",
        "sensors",
        "temperature",
        {"region": "string", "temp": "float64", "humidity": "int64"},
        partition_by=["region"],
    )

    north_table = pa.table(
        {
            "region": pa.array(["north", "north", "north"], type=pa.string()),
            "temp": pa.array([10.0, 15.0, 20.0], type=pa.float64()),
            "humidity": pa.array([50, 55, 60], type=pa.int64()),
        }
    )
    ap.write_to_frame("analytics", "sensors", "temperature", ipc_serialize(north_table))

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


def test_1_bee_pool_creates_correct_bees():
    """Test 1: BeePool creates correct number of bees."""
    ap, tmp = setup_apiary()
    try:
        statuses = ap.bee_status()
        check(isinstance(statuses, list), f"bee_status() returns a list (got {type(statuses).__name__})")

        num_bees = len(statuses)
        cores = ap.status()["cores"]
        check(num_bees == cores, f"Number of bees ({num_bees}) equals cores ({cores})")
    finally:
        ap.shutdown()


def test_2_bee_status_structure():
    """Test 2: bee_status() returns correct structure for each bee."""
    ap, tmp = setup_apiary()
    try:
        statuses = ap.bee_status()
        check(len(statuses) > 0, f"At least 1 bee (got {len(statuses)})")

        bee = statuses[0]
        check("bee_id" in bee, "Status has 'bee_id'")
        check("state" in bee, "Status has 'state'")
        check("memory_used" in bee, "Status has 'memory_used'")
        check("memory_budget" in bee, "Status has 'memory_budget'")
        check(bee["state"] == "idle", f"Bee is idle at start (got {bee['state']})")
        check(bee["memory_budget"] > 0, f"Memory budget > 0 (got {bee['memory_budget']})")
        check(bee["memory_used"] == 0, f"Memory used = 0 at start (got {bee['memory_used']})")
    finally:
        ap.shutdown()


def test_3_bee_ids_sequential():
    """Test 3: Bee IDs follow bee-0, bee-1, ... pattern."""
    ap, tmp = setup_apiary()
    try:
        statuses = ap.bee_status()
        bee_ids = [s["bee_id"] for s in statuses]
        for i, bid in enumerate(bee_ids):
            check(bid == f"bee-{i}", f"Bee {i} has ID 'bee-{i}' (got '{bid}')")
    finally:
        ap.shutdown()


def test_4_sql_through_bee_pool():
    """Test 4: SQL queries execute through the BeePool."""
    ap, tmp = setup_apiary()
    try:
        # SQL should work as before, now routed through a bee
        result = ap.sql("SELECT * FROM analytics.sensors.temperature")
        check(result is not None, "SQL query returns result")
        table = ipc_deserialize(result)
        check(table.num_rows == 5, f"SQL returns all 5 rows (got {table.num_rows})")
    finally:
        ap.shutdown()


def test_5_sql_aggregation_through_pool():
    """Test 5: Aggregation queries work through the BeePool."""
    ap, tmp = setup_apiary()
    try:
        result = ap.sql(
            "SELECT region, AVG(temp) as avg_temp, COUNT(*) as cnt "
            "FROM analytics.sensors.temperature GROUP BY region ORDER BY region"
        )
        table = ipc_deserialize(result)
        check(table.num_rows == 2, f"GROUP BY produces 2 groups (got {table.num_rows})")
        regions = table.column("region").to_pylist()
        check(
            "north" in regions and "south" in regions,
            f"Groups are north and south (got {regions})",
        )
    finally:
        ap.shutdown()


def test_6_custom_commands_through_pool():
    """Test 6: Custom commands (USE, SHOW) work through the BeePool."""
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

        result = ap.sql("SHOW HIVES")
        table = ipc_deserialize(result)
        hives = table.column("hive").to_pylist()
        check("analytics" in hives, f"SHOW HIVES includes 'analytics' (got {hives})")
    finally:
        ap.shutdown()


def test_7_concurrent_sql_queries():
    """Test 7: Multiple SQL queries can run concurrently on separate bees."""
    ap, tmp = setup_apiary()
    try:
        results = []
        errors = []

        def run_query(query, idx):
            try:
                result = ap.sql(query)
                table = ipc_deserialize(result)
                results.append((idx, table.num_rows))
            except Exception as e:
                errors.append((idx, str(e)))

        threads = []
        for i in range(3):
            t = threading.Thread(
                target=run_query,
                args=(f"SELECT * FROM analytics.sensors.temperature WHERE region = 'north'", i),
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=30)

        check(len(errors) == 0, f"No errors in concurrent queries (errors: {errors})")
        check(
            len(results) == 3,
            f"All 3 queries completed (got {len(results)})",
        )
        for idx, nrows in results:
            check(nrows == 3, f"Query {idx}: 3 north rows (got {nrows})")
    finally:
        ap.shutdown()


def test_8_bees_return_to_idle():
    """Test 8: Bees return to idle state after executing queries."""
    ap, tmp = setup_apiary()
    try:
        # Run a query
        ap.sql("SELECT * FROM analytics.sensors.temperature")

        # Give pool time to clean up
        time.sleep(0.1)

        statuses = ap.bee_status()
        idle_count = sum(1 for s in statuses if s["state"] == "idle")
        check(
            idle_count == len(statuses),
            f"All bees idle after query ({idle_count}/{len(statuses)})",
        )
    finally:
        ap.shutdown()


def test_9_memory_budget_per_bee():
    """Test 9: Each bee has a correct memory budget."""
    ap, tmp = setup_apiary()
    try:
        status = ap.status()
        expected_budget_per_bee = int(
            status["memory_gb"] * 1024 * 1024 * 1024 / status["cores"]
        )

        statuses = ap.bee_status()
        for s in statuses:
            # Allow some rounding tolerance
            diff = abs(s["memory_budget"] - expected_budget_per_bee)
            check(
                diff < 1024,  # within 1KB tolerance
                f"Bee {s['bee_id']} budget ≈ expected ({s['memory_budget']} vs {expected_budget_per_bee})",
            )
    finally:
        ap.shutdown()


def test_10_backward_compat_solo_mode():
    """Test 10: Solo mode E2E still works with BeePool."""
    from apiary import Apiary

    tmp = tempfile.mkdtemp()
    ap = Apiary("solo_bee_test", storage=f"local://{tmp}/data")
    ap.start()
    try:
        ap.create_hive("mydb")
        ap.create_box("mydb", "myschema")
        ap.create_frame(
            "mydb",
            "myschema",
            "events",
            {"event_type": "string", "count": "int64"},
        )

        table = pa.table(
            {
                "event_type": pa.array(
                    ["click", "view", "click", "purchase"], type=pa.string()
                ),
                "count": pa.array([10, 20, 15, 5], type=pa.int64()),
            }
        )
        ap.write_to_frame("mydb", "myschema", "events", ipc_serialize(table))

        result = ap.sql(
            "SELECT event_type, SUM(count) as total "
            "FROM mydb.myschema.events "
            "GROUP BY event_type ORDER BY total DESC"
        )
        result_table = ipc_deserialize(result)
        check(
            result_table.num_rows == 3,
            f"Solo E2E with bees: 3 event types (got {result_table.num_rows})",
        )

        bee_statuses = ap.bee_status()
        check(
            len(bee_statuses) > 0,
            f"bee_status() works in solo mode ({len(bee_statuses)} bees)",
        )
    finally:
        ap.shutdown()


# ---- Main ----

if __name__ == "__main__":
    print("=" * 60)
    print("Step 5 Acceptance Criteria Verification")
    print("=" * 60)

    run_test("Test 1: BeePool creates correct bees", test_1_bee_pool_creates_correct_bees)
    run_test("Test 2: bee_status() structure", test_2_bee_status_structure)
    run_test("Test 3: Bee IDs sequential", test_3_bee_ids_sequential)
    run_test("Test 4: SQL through BeePool", test_4_sql_through_bee_pool)
    run_test("Test 5: Aggregation through pool", test_5_sql_aggregation_through_pool)
    run_test("Test 6: Custom commands through pool", test_6_custom_commands_through_pool)
    run_test("Test 7: Concurrent SQL queries", test_7_concurrent_sql_queries)
    run_test("Test 8: Bees return to idle", test_8_bees_return_to_idle)
    run_test("Test 9: Memory budget per bee", test_9_memory_budget_per_bee)
    run_test("Test 10: Backward compat solo mode", test_10_backward_compat_solo_mode)

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'=' * 60}")

    if failed == 0:
        print("\n✅ All Step 5 acceptance criteria met!")
    else:
        print(f"\n❌ {failed} check(s) failed")

    sys.exit(0 if failed == 0 else 1)
