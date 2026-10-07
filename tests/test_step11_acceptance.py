#!/usr/bin/env python3
"""Step 11 Acceptance Tests: the crop, ingest, and the _stage column.

Ingested rows land in the node's crop, are queryable at once (flagged
_stage = 'crop'), and move into the Delta table on deposit (_stage = 'comb').
"""

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


def new_node(tmpdir):
    from apiary import Apiary

    ap = Apiary("test_crop", storage=f"local://{tmpdir}")
    ap.start()
    ap.create_hive("farm")
    ap.create_box("farm", "field")
    ap.create_frame("farm", "field", "readings", {"region": "string", "n": "int64"})
    return ap


def readings(region, start, stop):
    values = list(range(start, stop))
    return pa.table({"region": [region] * len(values), "n": values})


def scalar(ap, sql):
    return deserialize_table(ap.sql(sql)).column(0)[0].as_py()


def test_ingest_is_queryable_at_once():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        result = ap.ingest("farm", "field", "readings", serialize_table(readings("north", 0, 5)))
        check(result["rows"] == 5, "ingest reports the rows landed")
        check(result["segment"] == 1, "ingest reports the crop segment")

        check(scalar(ap, "SELECT count(n) FROM farm.field.readings") == 5, "ingested rows are queryable")
        check(
            scalar(ap, "SELECT count(n) FROM farm.field.readings WHERE _stage = 'crop'") == 5,
            "they are flagged _stage = 'crop'",
        )
        check(
            scalar(ap, "SELECT count(n) FROM farm.field.readings WHERE _stage = 'comb'") == 0,
            "none is in the comb yet",
        )
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_flush_moves_the_crop_into_the_comb():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        ap.ingest("farm", "field", "readings", serialize_table(readings("north", 0, 5)))
        report = ap.flush_crop()
        check(report["rows"] == 5 and report["segments"] == 1, "flush_crop reports what it deposited")

        check(scalar(ap, "SELECT count(n) FROM farm.field.readings") == 5, "no row lost or repeated")
        check(
            scalar(ap, "SELECT count(n) FROM farm.field.readings WHERE _stage = 'comb'") == 5,
            "the rows are now _stage = 'comb'",
        )
        check(
            scalar(ap, "SELECT count(n) FROM farm.field.readings WHERE _stage = 'crop'") == 0,
            "the crop is empty",
        )
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_results_report_rows_per_stage():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        ap.ingest("farm", "field", "readings", serialize_table(readings("north", 0, 4)))
        ap.flush_crop()
        ap.ingest("farm", "field", "readings", serialize_table(readings("south", 4, 6)))

        table = deserialize_table(ap.sql("SELECT sum(n) FROM farm.field.readings"))
        metadata = table.schema.metadata or {}
        check(metadata.get(b"apiary.rows.comb") == b"4", "the result says 4 rows came from the comb")
        check(metadata.get(b"apiary.rows.crop") == b"2", "the result says 2 rows came from the crop")
        check(table.column(0)[0].as_py() == sum(range(6)), "the answer covers both stages")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_read_from_frame_includes_the_crop():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        ap.ingest("farm", "field", "readings", serialize_table(readings("north", 0, 3)))
        data = ap.read_from_frame("farm", "field", "readings")
        check(data is not None, "read_from_frame returns the ingested rows")
        table = deserialize_table(data)
        check(table.num_rows == 3, "all 3 rows are returned")
        check(table.column_names == ["region", "n"], "without the _stage column")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_a_bad_batch_is_refused():
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_crop_bad", storage=f"local://{tmpdir}")
        ap.start()
        ap.create_hive("farm")
        ap.create_box("farm", "field")
        ap.create_frame("farm", "field", "readings", {"n": "int64"})
        try:
            ap.ingest("farm", "field", "readings", serialize_table(pa.table({"n": ["not a number"]})))
            check(False, "a value that does not fit the frame type is refused")
        except Exception as e:  # noqa: BLE001
            check("cannot be written" in str(e) or "Failed to ingest" in str(e), "a value that does not fit is refused")
        check(
            scalar(ap, "SELECT count(n) FROM farm.field.readings") == 0,
            "nothing was written",
        )
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_stage_is_a_reserved_column_name():
    tmpdir = tempfile.mkdtemp()
    try:
        from apiary import Apiary

        ap = Apiary("test_crop_reserved", storage=f"local://{tmpdir}")
        ap.start()
        ap.create_hive("farm")
        ap.create_box("farm", "field")
        try:
            ap.create_frame("farm", "field", "bad", {"_stage": "string"})
            check(False, "a frame cannot declare a _stage column")
        except Exception as e:  # noqa: BLE001
            check("reserved" in str(e), "a frame cannot declare a _stage column")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_a_query_with_no_rows_keeps_its_columns():
    tmpdir = tempfile.mkdtemp()
    try:
        ap = new_node(tmpdir)
        ap.ingest("farm", "field", "readings", serialize_table(readings("north", 0, 3)))

        empty = deserialize_table(ap.sql("SELECT region, n FROM farm.field.readings WHERE n > 1000"))
        check(empty.num_rows == 0, "a query that matches nothing returns no rows")
        check(empty.column_names == ["region", "n"], "but still has its columns")
        meta = empty.schema.metadata or {}
        check(meta.get(b"apiary.rows.crop") is not None, "and still reports the rows read per stage")
        ap.shutdown()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    run_test("Test 1: Ingest is queryable at once", test_ingest_is_queryable_at_once)
    run_test("Test 2: Flush moves the crop into the comb", test_flush_moves_the_crop_into_the_comb)
    run_test("Test 3: Results report rows per stage", test_results_report_rows_per_stage)
    run_test("Test 4: read_from_frame includes the crop", test_read_from_frame_includes_the_crop)
    run_test("Test 5: A bad batch is refused", test_a_bad_batch_is_refused)
    run_test("Test 6: _stage is reserved", test_stage_is_a_reserved_column_name)
    run_test("Test 7: A query with no rows keeps its columns", test_a_query_with_no_rows_keeps_its_columns)

    print(f"\nResults: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
