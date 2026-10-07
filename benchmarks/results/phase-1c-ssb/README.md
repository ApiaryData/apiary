# Phase 1c SSB benchmark (regression check)

SSB, scale factor 1, Apiary at commit `655d3cd` (phases 1a-1c). Same setup as
[`../v1-baseline`](../v1-baseline/README.md) and [`../phase-1b`](../phase-1b/README.md):
pi4-4gb limits (2 CPUs, 2 GB per node), one node, local-filesystem storage, 1 warmup + 3
timed runs, `--no-cache-clear`, same laptop.

Phase 1c puts a view over every Frame (the Delta table unioned with the crop, plus a
`_stage` column), so this checks that the extra planning layer costs nothing measurable.

| | V1 (`0e87f72`) | Phase 1b (`4c7ebbf`) | Phase 1c (`655d3cd`) |
|---|---:|---:|---:|
| Geometric mean | 5307 ms | 3786 ms | 3747 ms |
| Total, 13 queries | 69.1 s | 49.3 s | 48.8 s |
| Row counts match V1 | | all 13 | all 13 |
| Failures | | none | none |

1c is within 1% of 1b (slightly faster, within run-to-run noise) and 29% faster than V1,
so the staged view has no measurable cost on a Frame whose crop is empty. These runs do
not exercise the crop: SSB data is loaded with `write_to_frame`, so every row is in the
comb. The cost of a populated crop is in [`../phase-1c`](../phase-1c/README.md).

The caveats of the baseline apply: about 3.5 s of every query is the harness starting a
fresh `docker compose exec python3`, so this detects only large changes; the limits are
Docker-emulated on a laptop, not a Raspberry Pi; storage is local, not S3; SF1 barely
stresses the memory pool and join policy.
