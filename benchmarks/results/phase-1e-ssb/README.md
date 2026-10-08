# Phase 1e SSB benchmark (regression check)

SSB, scale factor 1, Apiary at the Phase 1e commit (the `apiary` binary, the Flight SQL and
MQTT entrances, Guards). Same setup as [`../phase-1d-ssb`](../phase-1d-ssb/README.md): pi4-4gb
limits (2 CPUs, 2 GB per node), one node, local-filesystem storage, 1 warmup + 3 timed runs,
`--no-cache-clear`, same laptop. The harness drives the embedded Python node, so this checks
that 1e's changes to the node and to query output did not slow the read path.

| | V1 (`0e87f72`) | Phase 1b | Phase 1c | Phase 1d | Phase 1e |
|---|---:|---:|---:|---:|---:|
| Geometric mean | 5307 ms | 3786 ms | 3747 ms | 3786 ms | 3817 ms |
| Total, 13 queries | 69.1 s | 49.3 s | 48.8 s | 49.4 s | 49.7 s |
| Row counts match | | all 13 | all 13 | all 13 | all 13 (vs 1d) |
| Failures | | none | none | none | none |

1e is within 1% of 1d (3817 vs 3786 ms), inside run-to-run noise, and about 28% faster than
V1. The caveats of the baseline apply: about 3.5 s of every query is the harness starting a
fresh `docker compose exec python3`, so this detects only large changes; the limits are
Docker-emulated on a laptop, not a Raspberry Pi; storage is local, not S3; SF1 barely
stresses the memory pool and join policy. It does not exercise the entrances; those are in
[`../phase-1e`](../phase-1e/README.md).
