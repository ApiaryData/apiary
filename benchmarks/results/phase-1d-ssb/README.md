# Phase 1d SSB benchmark (regression check)

SSB, scale factor 1, Apiary at the Phase 1d commit (typed Cells, recipes, capping,
harvest, clearing). Same setup as [`../phase-1c-ssb`](../phase-1c-ssb/README.md): pi4-4gb
limits (2 CPUs, 2 GB per node), one node, local-filesystem storage, 1 warmup + 3 timed runs,
`--no-cache-clear`, same laptop.

| | V1 (`0e87f72`) | Phase 1b | Phase 1c | Phase 1d |
|---|---:|---:|---:|---:|
| Geometric mean | 5307 ms | 3786 ms | 3747 ms | 3786 ms |
| Total, 13 queries | 69.1 s | 49.3 s | 48.8 s | 49.4 s |
| Row counts match V1 / 1c | | all 13 | all 13 | all 13 (vs 1c) |
| Failures | | none | none | none |

1d is within about 1% of 1c (3786 vs 3747 ms, inside run-to-run noise) and about 29%
faster than V1. 1d adds no work to the read path: capping, harvest and clearing run in
background loops that do nothing on a freshly loaded, never-capped table, and the
benchmark finishes well inside their first interval. So this confirms there is no
regression; it does not measure the effect of capping on query speed.

The caveats of the baseline apply: about 3.5 s of every query is the harness starting a
fresh `docker compose exec python3`, so this detects only large changes; the limits are
Docker-emulated on a laptop, not a Raspberry Pi; storage is local, not S3; SF1 barely
stresses the memory pool and join policy.
