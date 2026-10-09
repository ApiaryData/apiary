# Phase 4 benchmarks (regression check)

Phase 4 moves query execution onto the Node's Bees (a query waits for a Forager and runs
under that Bee's share of the memory pool) and deposit, capping and harvest onto Ripener
Bees. These runs check that the read path, the entrance and ingest did not slow down. Same
setup as the earlier phases: the `pi4-4gb` limits (2 CPUs, 2 GB per node), one node,
local-filesystem storage (`deploy/docker-compose.pi4-4gb-local.yml`), 1 warmup + 3 timed
runs, `--no-cache-clear`, same laptop. Raw numbers: [`../phase-4-ssb`](../phase-4-ssb),
[`../phase-4-tpch`](../phase-4-tpch) and [`entrance.json`](entrance.json).

The SSB and TPC-H harness spawns a process per query, so every query has a floor of about
3.5 s and only changes of a few percent or more mean anything.

## SSB, scale factor 1

| | Phase 1e | Phase 4 |
|---|---:|---:|
| Geometric mean | 3817 ms | 3824 ms |
| Total, 13 queries | 49.7 s | 49.8 s |

No query is more than 5% slower.

## TPC-H-derived, scale factor 1

| | Phase 1e | Phase 4 |
|---|---:|---:|
| Geometric mean | 3679 ms | 3825 ms |
| Total, 22 queries | 81.1 s | 84.4 s |

About 4% slower overall. Most of it is in the heavy join queries, and part of that is noise:
five timed runs of Q12, Q17, Q18 and Q21 a second time gave 3840, 3827, 4544 and 4166 ms
(Phase 1e: 3698, 3674, 4459, 3904; first Phase 4 run: 4174, 4041, 4946, 4170). Q12, Q17 and
Q18 came back to within 4% of Phase 1e; Q21 stayed about 7% up. A small real cost is likely:
each query now builds its session state for the Bee's memory share, and waits about a
millisecond for a Forager.

## Entrance, 2 CPUs and 2 GB

`entrance_bench`, release build, crop synced to disk before every ingest returns. The
Phase 3 numbers were measured the same day on the same machine, because the machine itself
drifts from day to day (the Phase 1e README's ingest numbers were 0.5 to 0.8 ms lower than
anything measured since, on either side of Phase 4).

| | Phase 3 | Phase 4 |
|---|---:|---:|
| Direct ingest, 10 to 1,000 rows | 3.1 to 3.5 ms | 3.1 to 3.4 ms |
| Flight ingest, 1,000 rows | 3.7 to 4.5 ms | 3.6 to 3.8 ms |
| MQTT, 1 row per message | 5,260 to 5,830 rows/s | 5,530 to 5,760 rows/s |
| MQTT, 20 rows per message | 0.11 to 0.12 s | 0.11 to 0.15 s |
| Query, 100,000 rows, called directly (crop) | 5.8 to 6.7 ms | 5.3 to 8.3 ms |
| Query over Flight (crop) | 6.4 to 9.7 ms | 7.9 to 15.9 ms |

Ingest does not go through the Bees and does not move. Queries pay the hop to a Forager, about
a millisecond (a trivial `SHOW HIVES` takes 1 ms on average and 6 ms at worst), and vary
more from run to run than before.

## What the benchmark found and fixed

- A Bee whose forager threshold had drifted high could leave a query waiting most of a
  second (the first measurement: 8 ms mean, 885 ms worst for `SHOW HIVES`). A call that goes
  unanswered now grows louder, and Bees look again every 2 ms while something is due.
- Bees polled every 2 ms whenever any stimulus was above zero, which is nearly always. They
  now poll quickly only while something is due (stimulus 1 or more) and at the idle pace
  while pressure is merely building.
- The MQTT part of `entrance_bench` polled for completion every 50 ms, which rounded the
  20-rows-per-message result to its own step (0.12 s or 0.17 s depending on where a poll
  fell). It polls every 5 ms now.
