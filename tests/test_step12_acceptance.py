#!/usr/bin/env python3
"""Step 12 Acceptance Tests: ripening, capping, harvest and clearing.

A frame's recipe (sort and deduplicate keys) is applied when the crop is
deposited. Capping merges the comb's small nectar cells into sealed capped
cells; harvest copies capped cells to a second store; clearing deletes the
files capping replaced.
"""

import os
import shutil
import sys
import tempfile
import traceback

import pyarrow as pa


def serialize_table(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def deserialize_table(data: bytes) -> pa.Table:
    return pa.ipc.open_stream(data).read_all()


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
    print(f"\n{name}")
    try:
        func()
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ EXCEPTION: {e}")
        traceback.print_exc()
        failed += 1


def new_node(tmpdir, harvest=None, retention_seconds=None):
    from apiary import Apiary

    ap = Apiary(
        "test_ripen",
        storage=f"local://{tmpdir}/site",
        harvest=f"local://{tmpdir}/harvest" if harvest else None,
        retention_seconds=retention_seconds,
    )
    ap.start()
    ap.create_hive("farm")
    ap.create_box("farm", "field")
    ap.create_frame("farm", "field", "readings", {"id": "int64", "val": "string"})
    return ap


def rows(ids, val):
    return pa.table({"id": ids, "val": [val] * len(ids)})


def ids_of(ap, sql="SELECT id FROM farm.field.readings"):
    return deserialize_table(ap.sql(sql)).column(0).to_pylist()


def parquet_files(tmpdir):
    path = os.path.join(tmpdir, "site", "farm", "field", "readings")
    return len([f for f in os.listdir(path) if f.endswith(".parquet")])


def test_recipe_round_trips_and_is_checked():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        check(
            ap.recipe("farm", "field", "readings") == {"sort_by": [], "dedup_by": []},
            "a new frame has an empty recipe",
        )
        ap.set_recipe("farm", "field", "readings", sort_by=["id"], dedup_by=["id"])
        check(
            ap.recipe("farm", "field", "readings") == {"sort_by": ["id"], "dedup_by": ["id"]},
            "the recipe reads back",
        )
        try:
            ap.set_recipe("farm", "field", "readings", sort_by=["nope"])
            check(False, "a recipe naming a missing column is refused")
        except Exception as e:  # noqa: BLE001
            check("nope" in str(e), "a recipe naming a missing column is refused")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_deposit_ripens_with_the_recipe():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        ap.set_recipe("farm", "field", "readings", sort_by=["id"], dedup_by=["id"])
        ap.ingest("farm", "field", "readings", serialize_table(rows([3, 1, 3, 2, 1], "x")))
        flushed = ap.flush_crop()
        check(flushed["rows"] == 3, "duplicates collapse when the crop is deposited")
        check(ids_of(ap) == [1, 2, 3], "and the deposited rows are sorted")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_capping_merges_nectar_into_one_capped_cell():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        ap.set_recipe("farm", "field", "readings", sort_by=["id"], dedup_by=["id"])
        for ids, val in [([5, 2, 9], "old"), ([7, 2, 1], "new")]:
            ap.ingest("farm", "field", "readings", serialize_table(rows(ids, val)))
            ap.flush_crop()
        check(parquet_files(tmpdir) == 2, "two deposits make two nectar cells")

        report = ap.cap()
        check(report["nectar_cells"] == 2 and report["capped_cells"] == 1, "capping merges them into one")
        check(report["rows"] == 5, "keeping five distinct rows")
        check(ids_of(ap) == [1, 2, 5, 7, 9], "sorted across the merged cells")
        check(
            ids_of(ap, "SELECT val FROM farm.field.readings WHERE id = 2") == ["new"],
            "the latest row of a duplicate wins",
        )
        check(ap.cap()["nectar_cells"] == 0, "a capped cell is never touched again")

        cleared = ap.clear()
        check(cleared["deleted"] == 0, "files capping replaced are kept for the grace period")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_harvest_copies_capped_cells_once():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir, harvest=True)
        ap.ingest("farm", "field", "readings", serialize_table(rows([1, 2, 3], "a")))
        ap.flush_crop()

        check(ap.harvest()["cells"] == 0, "nectar is not harvested")
        ap.cap()
        first = ap.harvest()
        check(first["cells"] == 1 and first["remaining"] == 0, "a capped cell is harvested")
        check(ap.harvest()["cells"] == 0, "and not harvested twice")

        harvest_table = os.path.join(tmpdir, "harvest", "farm", "field", "readings", "_delta_log")
        check(os.path.isdir(harvest_table), "the harvest is a Delta table of its own")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_harvest_needs_a_harvest_store():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        try:
            ap.harvest()
            check(False, "harvest without a harvest store is refused")
        except Exception as e:  # noqa: BLE001
            check("harvest" in str(e), "harvest without a harvest store is refused")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_retention_retires_only_harvested_cells():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir, harvest=True, retention_seconds=0)
        ap.ingest("farm", "field", "readings", serialize_table(rows([1, 2], "a")))
        ap.flush_crop()
        ap.cap()

        check(ap.clear()["retired"] == 0, "a cell that is not harvested never leaves the drive")
        check(ids_of(ap) == [1, 2], "so it still reads")
        ap.harvest()
        check(ap.clear()["retired"] == 1, "a harvested cell past retention is retired")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    run_test("Test 1: The recipe round-trips and is checked", test_recipe_round_trips_and_is_checked)
    run_test("Test 2: Deposit ripens with the recipe", test_deposit_ripens_with_the_recipe)
    run_test("Test 3: Capping merges nectar into a capped cell", test_capping_merges_nectar_into_one_capped_cell)
    run_test("Test 4: Harvest copies capped cells once", test_harvest_copies_capped_cells_once)
    run_test("Test 5: Harvest needs a harvest store", test_harvest_needs_a_harvest_store)
    run_test("Test 6: Retention retires only harvested cells", test_retention_retires_only_harvested_cells)

    print(f"\nResults: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
