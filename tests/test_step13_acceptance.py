#!/usr/bin/env python3
"""Step 13 Acceptance Tests: the apiary binary and apiary.connect().

Starts a real `apiary node run` process from a config file and drives it over
Arrow Flight SQL: create a frame, deposit, query, refusals, a token, ripening.

The binary is found from $APIARY_BIN, or under $CARGO_TARGET_DIR and ./target
(release, then debug). If there is none the test is skipped, not failed.
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback

import pyarrow as pa


def find_binary():
    candidates = []
    if os.environ.get("APIARY_BIN"):
        candidates.append(os.environ["APIARY_BIN"])
    for root in (os.environ.get("CARGO_TARGET_DIR"), "target"):
        if root:
            candidates += [os.path.join(root, "release", "apiary"), os.path.join(root, "debug", "apiary")]
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


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


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    """A running `apiary node run`."""

    def __init__(self, binary, token=None):
        self.dir = tempfile.mkdtemp()
        self.port = free_port()
        flight = f'listen = "127.0.0.1:{self.port}"\n'
        if token:
            flight += f'token = "{token}"\n'
        config = (
            "[node]\n"
            f'storage = "local://{self.dir}/site"\n'
            f'cache_dir = "{self.dir}/cache"\n'
            "deposit_interval_secs = 3600\n"
            "\n[flight]\n" + flight
        )
        path = os.path.join(self.dir, "apiary.toml")
        with open(path, "w") as f:
            f.write(config)
        self.proc = subprocess.Popen(
            [binary, "node", "run", "--config", path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(200):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError("the node never listened")

    @property
    def url(self):
        return f"grpc://127.0.0.1:{self.port}"

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


def with_node(binary, token=None):
    node = Node(binary, token)
    import apiary

    ap = apiary.connect(node.url, token=token)
    return node, ap


def make_frame(ap):
    ap.create_hive("farm")
    ap.create_box("farm", "field")
    ap.create_frame("farm", "field", "readings", {"id": "int64", "temp": "float64"})


def test_deposit_and_query(binary):
    node, ap = with_node(binary)
    try:
        make_frame(ap)
        landed = ap.ingest("farm", "field", "readings", pa.table({"id": [1, 2, 3], "temp": [20.5, 21.0, 19.5]}))
        check(landed == 3, "a deposit reports the rows that landed")

        table = ap.sql("SELECT id, temp, _stage FROM farm.field.readings ORDER BY id")
        check(table.num_rows == 3, "the rows are queryable at once")
        check(set(table.column("_stage").to_pylist()) == {"crop"}, "and flagged as not yet shipped")
        meta = table.schema.metadata or {}
        check(meta.get(b"apiary.rows.crop") == b"3", "the result says how many rows came from the crop")

        empty = ap.sql("SELECT id, temp FROM farm.field.readings WHERE id > 100")
        check(empty.num_rows == 0 and empty.column_names == ["id", "temp"], "a query with no rows keeps its columns")

        flushed = ap.flush_crop()
        check(flushed["rows"] == 3, "flush_crop deposits into the comb")
        after = ap.sql("SELECT _stage FROM farm.field.readings")
        check(set(after.column("_stage").to_pylist()) == {"comb"}, "after which the rows read as comb")

        check(ap.sql("SHOW HIVES").num_rows >= 1, "SHOW HIVES works")
    finally:
        ap.close()
        node.stop()


def test_a_batch_that_does_not_fit_is_refused(binary):
    import apiary.client

    node, ap = with_node(binary)
    try:
        make_frame(ap)
        try:
            ap.ingest("farm", "field", "readings", pa.table({"id": [1], "humidity": [0.4]}))
            check(False, "an unknown column is refused")
        except apiary.client.ApiaryError as e:
            check("humidity" in str(e), "an unknown column is refused, naming it")
        try:
            ap.ingest("farm", "field", "nope", pa.table({"id": [1]}))
            check(False, "an unknown frame is refused")
        except apiary.client.ApiaryError as e:
            check("not found" in str(e).lower(), "an unknown frame is refused")
        check(ap.sql("SELECT count(id) FROM farm.field.readings").column(0)[0].as_py() == 0, "nothing landed")
    finally:
        ap.close()
        node.stop()


def test_a_token_is_required_when_set(binary):
    import apiary
    import apiary.client

    node = Node(binary, token="s3cret")
    try:
        anonymous = apiary.connect(node.url)
        try:
            anonymous.sql("SHOW HIVES")
            check(False, "a call without the token is refused")
        except apiary.client.ApiaryError:
            check(True, "a call without the token is refused")
        finally:
            anonymous.close()

        authorised = apiary.connect(node.url, token="s3cret")
        check(authorised.sql("SHOW HIVES").num_columns >= 1, "a call with the token works")
        authorised.close()
    finally:
        node.stop()


def test_the_recipe_ripens_a_deposit(binary):
    node, ap = with_node(binary)
    try:
        make_frame(ap)
        ap.set_recipe("farm", "field", "readings", sort_by=["id"], dedup_by=["id"])
        ap.ingest("farm", "field", "readings", pa.table({"id": [3, 1, 3, 2], "temp": [1.0, 2.0, 3.0, 4.0]}))
        ap.flush_crop()
        table = ap.sql("SELECT id, temp FROM farm.field.readings")
        check(table.column("id").to_pylist() == [1, 2, 3], "duplicates collapse and rows come out sorted")
        check(table.column("temp").to_pylist() == [2.0, 4.0, 3.0], "the latest duplicate wins")
    finally:
        ap.close()
        node.stop()


def test_embedded_is_the_same_class():
    import apiary

    check(apiary.embedded is apiary.Apiary, "apiary.embedded is the embedded node")


if __name__ == "__main__":
    binary = find_binary()
    if binary is None:
        print("SKIP: no apiary binary found (build with `cargo build -p apiary-cli` or set APIARY_BIN)")
        print("\nResults: 0 passed, 0 failed")
        sys.exit(0)

    run_test("Test 1: Deposit and query over Flight", lambda: test_deposit_and_query(binary))
    run_test("Test 2: A batch that does not fit is refused", lambda: test_a_batch_that_does_not_fit_is_refused(binary))
    run_test("Test 3: A token is required when set", lambda: test_a_token_is_required_when_set(binary))
    run_test("Test 4: The recipe ripens a deposit", lambda: test_the_recipe_ripens_a_deposit(binary))
    run_test("Test 5: embedded is the same class", test_embedded_is_the_same_class)

    print(f"\nResults: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
