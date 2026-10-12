# Phase 5 benchmarks (regression check)

Phase 5 wraps every Frame scan in a `FrameScanExec` that carries what another Node needs to
rebuild the scan, and adds the colony's machinery to a Node. A Node with no peers still runs
each query whole, so these runs check what the wrapper costs. Same setup as the earlier
phases: the `pi4-4gb` limits (2 CPUs, 2 GB per node), one node, local-filesystem storage
(`deploy/docker-compose.pi4-4gb-local.yml`), 1 warmup + 3 timed runs, `--no-cache-clear`,
scale factor 1. Both images were built and run the same day on the same machine, because the
machine drifts from day to day: `apiary-4:bench` is `main` (Phase 4), `apiary-5:bench` is this
branch. Raw numbers: [`../phase-5-ssb`](../phase-5-ssb), [`../phase-4-ssb-today`](../phase-4-ssb-today),
[`../phase-5-tpch-today`](../phase-5-tpch-today), [`../phase-4-tpch-today`](../phase-4-tpch-today).

The harness spawns a process per query, so every query has a floor of about 3.5 s and only
changes of a few percent or more mean anything.

## SSB

| | Phase 4 | Phase 5 |
|---|---:|---:|
| Geometric mean | 3881 ms | 3964 ms |
| Total, 13 queries | 50.6 s | 51.6 s |

About 2% slower, and every query moves the same way (Q1.x +1 to 3%, Q3.x +2 to 3%, Q2.2 +5%,
Q4.3 +4%).

## TPC-H-derived

| | Phase 4 | Phase 5 |
|---|---:|---:|
| Geometric mean | 3956 ms | 3871 ms |
| Total, 22 queries | 87.6 s | 85.3 s |

The geometric mean says Phase 5 is faster, but the Phase 4 run was the second of the two and
its first queries are slow (Q1 to Q4 and Q7, 7 to 28% over Phase 5), which looks like a cold
start and not a difference between the builds. Over Q5 to Q19, where neither run is distorted,
Phase 5 is 3 to 8% slower on most queries (Q5 +3%, Q10 +7%, Q13 +6%, Q14 +7%, Q19 +8%) and
level on a few (Q8, Q16). So the cost is most likely 2 to 4%, and this run cannot say more.

## What is probably costing it

A `FrameScanExec` is a leaf that hides the Delta scan, so DataFusion's physical optimiser
cannot push dynamic filters or projections into it as it can into the Delta scan's Parquet
child. The fix, if the cost matters, is to implement filter-pushdown delegation on
`FrameScanExec`. It also counts rows through a stream wrapper to report rows read per stage,
which is a small cost on every batch. Neither has been tried.

## Not measured

- A query run across the colony. Those are tested for correctness (two real Nodes, and the
  simulator) and not benchmarked: the simulator's Patches take virtual time, and the Pi 4
  gate needs real hardware or a multi-node compose topology.
- The gate item "TPC-H-derived joins complete within a Pi 4's per-Bee budget with no memory
  failures" is covered by a test (`joins_over_more_data_than_a_bee_may_hold_complete_as_patches`
  in `apiary-plan`: joins over about 1.2 million rows, run as Patches under a 12 MB share, give
  the answer the query gives with unlimited memory), not by the harness.
