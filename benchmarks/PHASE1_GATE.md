# Phase 1 gate runbook

The design document's gate for phase 1 (section 12, step 1):

> The SSB and TPC-H-derived suites and a sensor-ingest benchmark run on a single Pi 4 with
> an external drive and on one cloud machine, with baselines recorded before any colony code
> exists; a Node killed mid-ingest loses at most one cadence of rows; Databricks reads a
> harvest table registered in Unity Catalog.

Parts of this can be checked on any machine and are checked in CI-style tests and in the
recorded results. Parts need hardware or an account. This file says which is which and gives
the exact commands for the rest, so whoever has a Pi 4 or a Databricks workspace can finish
the gate and commit the results.

## What has been checked, and where

| Gate item | Status | Evidence |
|---|---|---|
| SSB SF1, V1 baseline | Done, laptop under Docker with Pi 4 limits (2 CPUs, 2 GB) | `results/v1-baseline` |
| SSB SF1, each phase | Done, same setup, no regression | `results/phase-1b`, `phase-1c-ssb`, `phase-1d-ssb`, `phase-1e-ssb` |
| TPC-H-derived, V1 baseline and phase 1e | Done, same setup | `results/v1-baseline-tpch`, `results/phase-1e-tpch` |
| Sensor-ingest benchmark | Done, same setup | `results/phase-1c` (embedded), `results/phase-1e` (Flight and MQTT) |
| Kill mid-ingest | Done by test: SIGKILL five times mid-stream, nothing acknowledged lost, nothing duplicated | `crates/apiary-cli/tests/binary.rs`, `a_node_killed_mid_ingest_loses_nothing_it_acknowledged` |
| Another engine reads Apiary's tables | Done with `deltalake` (a separate Delta reader) | `tests/test_step14_acceptance.py` |
| Pi 4 with an external drive | **Needs a Pi 4** | commands below |
| One cloud machine | **Needs a cloud machine** | commands below |
| Databricks reads a harvest table in Unity Catalog | **Needs a Databricks workspace** | steps below |
| Power cut (not a process kill) | **Needs hardware** | steps below |

The Docker runs apply the Pi 4 CPU and memory limits but run on a laptop's CPU, memory
bandwidth and SSD. They detect regressions between phases; they are not Pi 4 numbers.

## On a Pi 4 with an external drive

Mount the drive (say at `/mnt/drive`) and make a directory on it for the comb. Build, or
cross-compile for `aarch64-unknown-linux-gnu` and copy the binary.

```bash
cargo build --release -p apiary-cli
```

Run a node with the comb on the drive and the crop on the Pi's own storage:

```toml
# apiary.toml
[node]
storage = "local:///mnt/drive/apiary"
cache_dir = "/var/lib/apiary"        # the crop; try the SD card and then a USB SSD
[flight]
listen = "127.0.0.1:50051"
```

```bash
./target/release/apiary node run --config apiary.toml
```

**SSB and TPC-H-derived**, through the benchmark harness (it generates the data in the
container and runs on the Pi 4 profile). The stock `pi4-4gb` compose file needs MinIO, whose
images are no longer on Docker Hub, so use `deploy/docker-compose.pi4-4gb-local.yml`, the same limits with the store on a local
volume, as the earlier results did:

```bash
cd benchmarks
python bench_runner.py --engine apiary-docker --suite ssb  --image apiary:latest \
  --compose-file ../deploy/docker-compose.pi4-4gb-local.yml --storage-url local:///home/apiary/data/bench \
  --nodes 1 --no-cache-clear --output results/phase1-gate-pi4
python bench_runner.py --engine apiary-docker --suite tpch --image apiary:latest \
  --compose-file ../deploy/docker-compose.pi4-4gb-local.yml --storage-url local:///home/apiary/data/bench \
  --nodes 1 --no-cache-clear --output results/phase1-gate-pi4
```

**Sensor-ingest.** Both examples build their Nodes under the system temporary directory, so
point `TMPDIR` at the drive to put the comb and the crop there, and again at the SD card or SSD
to compare:

```bash
TMPDIR=/mnt/drive/tmp cargo run --release -p apiary-runtime  --example ingest_bench   -- --out ingest.json
TMPDIR=/mnt/drive/tmp cargo run --release -p apiary-entrance --example entrance_bench -- --out entrance.json
```

The sync cost (about 2.3 ms per ingest on the laptop) is the number most likely to differ,
especially on an SD card.

**Kill mid-ingest.** The automated test kills the process:

```bash
cargo test --release -p apiary-cli --test binary a_node_killed_mid_ingest -- --nocapture
```

For a power cut, which a process kill does not simulate (the page cache survives a kill),
run a node with `crop_sync = true`, a client depositing numbered rows over Flight and
recording the last acknowledged id, and pull the power mid-stream. After the Pi restarts and
the node has started, every acknowledged id must be present exactly once. With
`crop_sync = false` expect to lose up to the last few seconds, which is why `crop_sync` is on
by default.

## On one cloud machine

The same commands, on a machine with the comb on its local disk or an attached volume.
Record `lscpu` and the disk type with the results.

## Databricks reading a harvest table

The harvest store must support conditional writes (AWS S3, Cloudflare R2 and MinIO do).

1. Run a node with `harvest = "s3://<bucket>/<prefix>"` in `[node]`, with credentials in the
   usual `AWS_*` environment variables, and let some data ripen and harvest. Or call
   `ap.cap()` and `ap.harvest()` from the Python client on an embedded node to do it at once.
2. In Databricks, create an external location for the bucket, then register the table:

   ```sql
   CREATE TABLE main.apiary.readings USING DELTA
   LOCATION 's3://<bucket>/<prefix>/<hive>/<box>/<frame>';
   SELECT count(*), min(<a column>), max(<a column>) FROM main.apiary.readings;
   ```

3. Compare the count with `apiary sql "SELECT count(*) FROM ..."` after the data is harvested.

Apiary must stay the only writer of a harvest table: Spark does not yet interoperate with
conditional-put writers (delta-io/delta-rs#4482). Databricks reads only.

`tests/test_step14_acceptance.py` already shows that an independent Delta reader opens the
harvest and site tables, sees the same rows and statistics, prunes partitions and reads old
versions. If Databricks refuses the table, the first things to check are the table's
reader/writer protocol versions and whether it has any features Databricks lacks.

## Recording results

Commit each run under `benchmarks/results/phase1-gate-<machine>/` with a `README.md` that
states the machine, the drive, the limits and the commit, in the style of the existing result
directories.
