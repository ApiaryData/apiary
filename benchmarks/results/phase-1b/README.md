# Phase 1b benchmark (against the V1 baseline)

SSB, scale factor 1, Apiary at commit `4c7ebbf` (phases 1a and 1b: Delta tables, lazy
catalogue, shared query session with a memory pool and join policy). Same setup as
[`../v1-baseline`](../v1-baseline/README.md): pi4-4gb limits (2 CPUs, 2 GB per node), one
node, local-filesystem storage, 1 warmup + 3 timed runs, `--no-cache-clear`, same laptop.

| | V1 (`0e87f72`) | Phase 1b (`4c7ebbf`) |
|---|---|---|
| Geometric mean | 5307 ms | 3786 ms (-29%) |
| Total, 13 queries | 69.1 s | 49.3 s |
| Wall clock | 282 s | 200 s |
| Row counts | all 13 match V1 | |
| Failures | none | none (the new 1.5 GB memory pool caused no failures) |

Every query is 1.1-1.7 s faster, with the gap roughly constant, so most of it looks like a
fixed cost removed (V1 loaded every cell into a `MemTable` before running) rather than faster
operators.

Caveats, as for the baseline:
- About 3.5 s of every query is the harness starting a fresh `docker compose exec python3`,
  so this only detects large changes. Run the node with `APIARY_TIMING=1` for phase timings.
- Docker-emulated limits on a laptop, not a Raspberry Pi; local storage, not S3.
- SF1 only. The memory pool and join policy are not stressed at this scale; a larger scale
  factor on the Pi 4 profile is the real test.
