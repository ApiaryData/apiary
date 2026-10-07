#!/usr/bin/env python3
"""Step 14 Acceptance Tests: another engine can read what Apiary writes.

The phase 1 gate wants Databricks to read a harvest table registered in Unity
Catalog. That needs a Databricks workspace, so this is the part that can be
checked anywhere: an independent Delta reader (the `deltalake` Python package,
which is delta-rs's own reader but a separate build from Apiary's) must open the
site table and the harvest table, see the same rows, the right schema, the file
statistics and the partitions, and read old versions.

Skipped if the `deltalake` package is not installed.
"""

import os
import shutil
import sys
import tempfile
import traceback

import pyarrow as pa

try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None


def serialize_table(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


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


def readings(region, ids):
    return pa.table(
        {
            "region": [region] * len(ids),
            "id": ids,
            "temp": [20.0 + i for i in ids],
        }
    )


def build(tmpdir):
    """A partitioned frame, ingested, ripened, capped and harvested."""
    from apiary import Apiary

    ap = Apiary(
        "test_interop",
        storage=f"local://{tmpdir}/site",
        harvest=f"local://{tmpdir}/harvest",
    )
    ap.start()
    ap.create_hive("farm")
    ap.create_box("farm", "field")
    ap.create_frame(
        "farm",
        "field",
        "readings",
        {"region": "string", "id": "int64", "temp": "float64"},
        partition_by=["region"],
    )
    ap.set_recipe("farm", "field", "readings", sort_by=["id"], dedup_by=["id"])
    for region, ids in [("north", [3, 1, 2]), ("south", [5, 4]), ("north", [2, 6])]:
        ap.ingest("farm", "field", "readings", serialize_table(readings(region, ids)))
        ap.flush_crop()
    ap.cap()
    ap.harvest()
    return ap


def test_site_and_harvest_tables_read_in_another_engine():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = build(tmpdir)
        site = DeltaTable(os.path.join(tmpdir, "site", "farm", "field", "readings"))
        harvest = DeltaTable(os.path.join(tmpdir, "harvest", "farm", "field", "readings"))

        for label, table in (("site", site), ("harvest", harvest)):
            data = table.to_pyarrow_table()
            ids = sorted(data.column("id").to_pylist())
            check(ids == [1, 2, 3, 4, 5, 6], f"the {label} table reads as the same six rows")
            check(
                data.schema.field("temp").type == pa.float64()
                and data.schema.field("id").type == pa.int64(),
                f"the {label} table has the declared column types",
            )
            check(table.metadata().partition_columns == ["region"], f"the {label} table is partitioned by region")

        # Another engine can prune partitions and read file statistics.
        north = site.to_pyarrow_table(partitions=[("region", "=", "north")])
        check(sorted(north.column("id").to_pylist()) == [1, 2, 3, 6], "partition filtering works in the other engine")
        actions = pa.table(site.get_add_actions(flatten=True)).to_pylist()
        check(sum(a["num_records"] for a in actions) == 6, "file statistics add up to the row count")
        check(all(a["size_bytes"] > 0 for a in actions), "every file has a size")

        # The harvest holds only capped Cells.
        harvested = pa.table(harvest.get_add_actions(flatten=True)).to_pylist()
        check(len(harvested) > 0, "the harvest table has files")
        check(all(a["path"].endswith(".parquet") for a in harvested), "its files are Parquet")

        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_history_and_old_versions_are_readable():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = build(tmpdir)
        site = DeltaTable(os.path.join(tmpdir, "site", "farm", "field", "readings"))
        history = site.history()
        operations = [h.get("operation") for h in history]
        check("OPTIMIZE" in operations, "capping shows in the history as a compaction")
        check(site.version() >= 3, "every deposit and the capping are separate versions")

        first = DeltaTable(
            os.path.join(tmpdir, "site", "farm", "field", "readings"), version=1
        )
        check(first.to_pyarrow_table().num_rows <= site.to_pyarrow_table().num_rows, "an old version reads")

        # Capping changed no data: the rows before and after are the same set.
        before = DeltaTable(
            os.path.join(tmpdir, "site", "farm", "field", "readings"), version=site.version() - 1
        )
        check(
            sorted(before.to_pyarrow_table().column("id").to_pylist())
            == sorted(site.to_pyarrow_table().column("id").to_pylist()),
            "the version before capping holds the same rows as the one after",
        )
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_the_recipe_is_visible_as_table_properties():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = build(tmpdir)
        site = DeltaTable(os.path.join(tmpdir, "site", "farm", "field", "readings"))
        config = site.metadata().configuration
        check(config.get("apiary.ripen.sort_by") == "id", "the sort key is in the table properties")
        check(config.get("apiary.ripen.dedup_by") == "id", "and so is the dedup key")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    if DeltaTable is None:
        print("SKIP: the `deltalake` package is not installed (pip install deltalake)")
        print("\nResults: 0 passed, 0 failed")
        sys.exit(0)

    run_test("Test 1: Site and harvest tables read in another engine", test_site_and_harvest_tables_read_in_another_engine)
    run_test("Test 2: History and old versions are readable", test_history_and_old_versions_are_readable)
    run_test("Test 3: The recipe is visible as table properties", test_the_recipe_is_visible_as_table_properties)

    print(f"\nResults: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
