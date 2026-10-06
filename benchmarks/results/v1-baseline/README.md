# V1 baseline (redesign Phase 0 gate)

SSB, scale factor 1, Apiary V1 as of commit `0e87f72` (before any redesign change).

| | |
|---|---|
| Engine | `apiary-docker`, image built from `0e87f72` |
| Profile | pi4-4gb limits (2 CPUs, 2 GB RAM per node), 1 node |
| Storage | **local filesystem** on a Docker named volume (`local:///home/apiary/data/bench`), not S3/MinIO |
| Host | Windows 11, 8 CPUs, 31.6 GB RAM, Docker Desktop |
| Runs | 1 warmup + 3 timed, `--no-cache-clear` |
| Result | geometric mean 5307 ms, total 69.1 s (13 queries) |

Why local storage: `minio/minio` and `minio/mc` are no longer on Docker Hub and
`quay.io/minio` needs authentication, so the stock pi4 profiles cannot start. The
compose file used was `deploy/docker-compose.pi4-4gb.yml` with the MinIO services and
S3 environment removed and the same resource limits. Later phases must be compared on
this same basis.

Caveats:
- Every query lands at 4.8-5.9 s whatever it does. The harness runs each query as a
  fresh `docker compose exec python3 -c ...`, so process start and Apiary node start-up
  dominate. Differences between queries are small, so this baseline can only catch
  large regressions. The per-phase breakdown (`APIARY_TIMING=1`) is the better signal
  for engine changes.
- Docker-emulated limits on a laptop are not a Raspberry Pi. The phase 1 gate also needs a real Pi 4 run.
- `Mem MB` reads 0.0 for every query; peak memory is not captured in this setup.

Reproduce:

```bash
git worktree add --detach ../apiary-v1 0e87f72 && (cd ../apiary-v1 && docker build -t apiary-v1:baseline .)
cd benchmarks
python bench_runner.py --suite ssb --engine apiary-docker --image apiary-v1:baseline \
  --compose-file <pi4-4gb profile without MinIO> --nodes 1 --scale-factor 1 \
  --storage-url local:///home/apiary/data/bench --runs 3 --warmup 1 --no-cache-clear \
  --output results/v1-baseline
```
