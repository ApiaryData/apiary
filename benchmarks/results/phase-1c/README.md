# Phase 1c benchmark: what the crop costs and what it buys

Run with `cargo run --release -p apiary-runtime --example ingest_bench` (source:
`crates/apiary-runtime/examples/ingest_bench.rs`, raw numbers: `ingest.json`).

Setup: Rust container limited to 2 CPUs and 2 GB (the pi4-4gb node profile), on a
Windows 11 laptop under Docker Desktop. Rows are sensor-shaped: a timestamp, a device
name, two readings and a status. The crop and the comb are on the container's disk.

## Ingest into the crop

Each `ingest` call lands one batch on the Node's disk and returns once it is synced
(`crop_sync` on, the default). With the sync off it is only written.

| batch rows | sync on: rows/s | p50 / p99 ms | sync off: rows/s | p50 / p99 ms |
|---:|---:|---:|---:|---:|
| 10 | 4,084 | 2.31 / 3.78 | 92,596 | 0.09 / 0.31 |
| 100 | 37,768 | 2.40 / 6.65 | 669,004 | 0.11 / 0.45 |
| 1,000 | 392,612 | 2.38 / 3.80 | 4,748,083 | 0.15 / 0.41 |
| 10,000 | 2,671,348 | 3.06 / 5.57 | 16,242,072 | 0.24 / 0.71 |

**The sync is the cost.** With it on, a call takes 2.3-3 ms whatever its size, so throughput
is simply `batch rows / 2.4 ms`. One message per call (10 rows) tops out near 4,000 rows/s;
batches of 1,000 or more reach hundreds of thousands. A call that does not sync takes
0.1-0.2 ms.

Calls on one Frame are serialised (a segment is numbered, written and synced under a lock),
so concurrent writers to the same Frame share that one sync budget. Many tiny ingests, such
as one per MQTT message, should be coalesced into larger batches before they reach `ingest`.

## What the crop buys: comparison with a direct commit

`write_to_frame` commits to the Delta table before it returns:

| batch rows | direct commit: p50 / p99 ms | ingest (sync on): p50 / p99 ms |
|---:|---:|---:|
| 10 | 31.0 / 102.7 | 2.31 / 3.78 |
| 100 | 29.8 / 52.2 | 2.40 / 6.65 |
| 1,000 | 26.4 / 54.8 | 2.38 / 3.80 |
| 10,000 | 9.6 / 14.6 | 3.06 / 5.57 |

For batches up to 1,000 rows ingest answers 11-13 times faster at the median (and 27 times
faster at p99 for 10-row batches), because it writes one small file instead of committing to
the Delta log. At 10,000 rows the gap narrows to about 3 times.

## Deposit

500,000 rows in 500 segments were deposited into the comb in 0.11 s (about 4.6 million
rows/s). Deposits keep up easily; the cadence, not the rate, sets the loss window.

## Query latency by where the rows are

`SELECT avg(temp), max(humidity)` over the frame, median of 5:

| rows | all in the crop (ms) | all in the comb (ms) |
|---:|---:|---:|
| 0 | 1.2 | 1.6 |
| 10,000 | 1.9 | 4.8 |
| 100,000 | 6.8 | 5.4 |
| 500,000 | 25.4 | 6.9 |
| 1,000,000 | 46.6 | 8.8 |

A query reads every pending crop segment into memory when it is planned, so latency grows
with the crop. Up to about 100,000 pending rows it is the same as querying the comb; a
million rows left in the crop costs about five times as much. At the default 10 s deposit
cadence that is a crop of hundreds of thousands of rows only at sustained rates above
tens of thousands of rows per second.

## Caveats

- **Not a Raspberry Pi.** This is a laptop under Docker Desktop, whose virtual disk may
  absorb some of the sync cost. A Pi's SD card typically syncs far more slowly (often
  10-50 ms or more) and an SSD about as fast as here. Treat the sync-on figures as a floor
  for latency on slow media and re-run on the hardware; `crop_sync` exists for exactly that
  trade-off.
- Single Frame, single Node, one writer at a time. Concurrent ingest to many Frames is not
  measured.
- Local-filesystem comb, not S3.
