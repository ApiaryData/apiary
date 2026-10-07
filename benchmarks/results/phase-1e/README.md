# Phase 1e entrance benchmark

`entrance_bench` (`crates/apiary-entrance/examples/entrance_bench.rs`) on sensor-shaped rows
(timestamp, device, two readings, status). Release build, run in a container limited to 2 CPUs
and 2 GB (the `pi4-4gb` limits) on a laptop under Docker Desktop, loopback networking, an
embedded `rumqttd` broker, crop synced to disk before every ingest returns. Raw numbers:
[`entrance.json`](entrance.json).

## Deposits over Flight SQL

Median time for one deposit call, directly on the Node (through the Guard) and over a Flight
SQL bulk ingest on loopback:

| Rows per batch | Direct | Over Flight | Throughput over Flight |
|---:|---:|---:|---:|
| 10 | 2.56 ms | 2.67 ms | 3,600 rows/s |
| 100 | 2.18 ms | 2.57 ms | 36,600 rows/s |
| 1,000 | 2.29 ms | 2.70 ms | 358,000 rows/s |
| 10,000 | 3.39 ms | 4.03 ms | 2.26 M rows/s |

A call costs the disk sync (about 2.3 ms) whatever its size, plus 0.1-0.7 ms for gRPC and the
Guard. Small batches are bounded by the sync, so a client sending a few rows at a time should
batch them first.

## Queries over Flight SQL

100,000 rows, `GROUP BY device` aggregate and a filtered count, five runs each:

| | In the crop | In the comb |
|---|---:|---:|
| Aggregate over Flight | 7.2 ms | 6.1 ms |
| Aggregate called directly | 4.8 ms | 6.1 ms |
| Filtered count over Flight | 5.0 ms | 6.2 ms |

## Deposits over MQTT

20,000 rows published to the broker at QoS 1, until all are queryable:

| Rows per message | Time | Rows/s | Messages/s |
|---:|---:|---:|---:|
| 1 | 3.19 s | 6,276 | 6,276 |
| 20 | 0.12 s | 171,324 | 8,566 |

A message is acknowledged only after it is deposited. A broker keeps a window of messages in
flight (100 here, 20 in Mosquitto's default), so a Node that waited to fill a 1,000-row batch
stalled until its interval each time: the first run of this benchmark gave 667 rows/s for
one-row messages. The idle flush (default 10 ms) deposits when the stream pauses, which fixed
it. Messages carrying several rows, or a broker with a larger window, go faster still.

## What the benchmark found and fixed

Running it found three problems, all fixed in the same phase:

- MQTT stalled on the broker's in-flight window (above): 667 to 6,276 rows/s.
- A 10,000-row Flight deposit took 44 ms against 3 ms direct, from HTTP/2's 64 KiB default
  flow-control window. The server window is 8 MiB now.
- Every streamed Flight query result took about 92 ms against 5 ms direct. Binding the listener
  by hand had skipped TCP_NODELAY, so Nagle's algorithm and delayed ACKs held small writes back.
  Set on accepted sockets, queries cost what a direct call does.

## Caveats

Loopback hides network latency and the Flight window effect may differ on a real LAN; the
broker is in-process and the same machine as the publisher; it is a laptop under Docker
Desktop, not a Raspberry Pi, and the sync cost especially will differ on an SD card; there is
no TLS or authentication cost in these numbers; the comb is local, not S3.
