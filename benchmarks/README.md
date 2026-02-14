# Apiary Benchmarking Suite

## Strategy Overview

This suite implements a three-tier benchmarking strategy designed to evaluate Apiary's
unique contributions: democratic distributed coordination, format-agnostic ACID storage,
and hybrid edge-to-cloud deployment on resource-constrained hardware.

### Tier 1 — Baseline Credibility (SSB + TPC-H Derived)

Standard analytical benchmarks that establish SQL execution competence and enable
direct comparison with DuckDB, DataFusion, and Polars.

| Benchmark | Tables | Queries | Dataset Size (SF1) | Purpose |
|-----------|--------|---------|-------------------|---------|
| SSB       | 5      | 13      | ~600MB            | Star schema analytics, primary baseline |
| TPC-H     | 8      | 22      | ~1GB              | Industry-standard comparability |

**Scale factors for Pi hardware:**
- SF1 (~1GB): Correctness validation, all Pi models
- SF10 (~10GB): Performance testing, Pi 4 8GB / Pi 5 16GB
- SF100 (~100GB): Stress testing, Pi 5 16GB + NVMe only

### Tier 2 — Differential Value (Apiary-Specific)

Custom benchmarks that exercise what no existing system does.

| Benchmark | What It Tests | Key Metric |
|-----------|--------------|------------|
| Format Agnostic | Query across Parquet/CSV/Arrow simultaneously | Overhead vs single-format |
| Heterogeneous Cluster | Mixed Pi 4/Pi 5/x86 coordination | Scaling efficiency |
| ACID Under Analytics | Concurrent reads + writes | Isolation overhead, consistency |
| Swarm Elasticity | Dynamic node join/leave during queries | Recovery time, throughput impact |

### Tier 3 — Stretch Goals

| Benchmark | Dataset | Purpose |
|-----------|---------|---------|
| JOB (Join Order) | IMDB ~3.6GB | Real-data optimizer stress test |
| ClickBench subset | Yandex ~14GB | Real-world web analytics |

---

## Quick Start

### Local mode (DataFusion direct)

```bash
# Install dependencies
pip install -r requirements.txt

# Generate all datasets at SF1
python generate_datasets.py --scale-factor 1 --output ./data

# Generate SSB only at SF10
python generate_datasets.py --benchmark ssb --scale-factor 10 --output ./data

# Generate multi-format datasets for Apiary-specific benchmarks
python generate_datasets.py --benchmark apiary --scale-factor 1 --output ./data

# Run benchmarks (DataFusion directly scanning Parquet)
python bench_runner.py --suite ssb --data-dir ./data/ssb/sf1/parquet --output ./results
```

### Docker mode (Apiary full stack) — Tier 1 on real/simulated hardware

Runs tier 1 benchmarks (SSB/TPC-H) through the complete Apiary stack inside
Docker containers built from a **pre-built image**. Data is generated and
ingested inside the container — no host-side data setup required.

**Prerequisites:** Docker with Compose plugin, a pre-built Apiary image.

```bash
# Build the image once (or pull from a registry)
docker build -t apiary:latest .

# --- Single-node examples ---

# SSB on default (unconstrained) hardware
python bench_runner.py --suite ssb --engine apiary-docker \
    --image apiary:latest --scale-factor 1

# TPC-H on a Pi 3-constrained profile
python bench_runner.py --suite tpch --engine apiary-docker \
    --image apiary:latest --profile pi3 --scale-factor 1

# SSB on Pi 4 4GB profile, 1 run per query (quick check)
python bench_runner.py --suite ssb --engine apiary-docker \
    --image apiary:latest --profile pi4-4gb --runs 1 --warmup 0

# --- Multi-node examples ---

# SSB on 3-node cluster, Pi 4 4GB constraints
python bench_runner.py --suite ssb --engine apiary-docker \
    --image apiary:latest --profile pi4-4gb --nodes 3 --scale-factor 1

# TPC-H on 2-node cluster, default compose
python bench_runner.py --suite tpch --engine apiary-docker \
    --image apiary:latest --nodes 2 --scale-factor 1

# Use an explicit compose file
python bench_runner.py --suite ssb --engine apiary-docker \
    --image apiary:latest \
    --compose-file ../deploy/docker-compose.pi5-16gb.yml \
    --nodes 4 --scale-factor 10

# --- Heterogeneous clusters (manual compose) ---
# For mixed Pi 3 + Pi 4 + Pi 5 clusters, create a custom compose file
# that assigns different resource constraints per service, then:
python bench_runner.py --suite tpch --engine apiary-docker \
    --image apiary:latest --compose-file ./my-heterogeneous-cluster.yml \
    --nodes 4 --scale-factor 1

# --- Other options ---

# Dry run (parse + rewrite queries, validate config, no containers)
python bench_runner.py --suite ssb --engine apiary-docker --dry-run

# Run specific queries only
python bench_runner.py --suite ssb --engine apiary-docker \
    --image apiary:latest --queries Q1.1 Q1.2 Q1.3
```

### Hardware profiles

The `--profile` flag selects a Docker Compose configuration that constrains
CPU and memory to simulate specific Raspberry Pi hardware:

| Profile | CPU | Memory/Node | Memory/MinIO | Compose File |
|---------|-----|-------------|-------------|--------------|
| `default` | unconstrained | unconstrained | unconstrained | `docker-compose.yml` |
| `pi3` | 2.0 | 512M | 512M | `deploy/docker-compose.pi3.yml` |
| `pi4-1gb` | 2.0 | 512M | 512M | `deploy/docker-compose.pi4-1gb.yml` |
| `pi4-2gb` | 2.0 | 1G | 768M | `deploy/docker-compose.pi4-2gb.yml` |
| `pi4-4gb` | 2.0 | 2G | 1.5G | `deploy/docker-compose.pi4-4gb.yml` |
| `pi4-8gb` | 2.0 | 4G | 3G | `deploy/docker-compose.pi4-8gb.yml` |
| `pi5-1gb` | 2.5 | 512M | 512M | `deploy/docker-compose.pi5-1gb.yml` |
| `pi5-2gb` | 2.5 | 1G | 768M | `deploy/docker-compose.pi5-2gb.yml` |
| `pi5-4gb` | 2.5 | 2G | 1.5G | `deploy/docker-compose.pi5-4gb.yml` |
| `pi5-8gb` | 2.5 | 4G | 3G | `deploy/docker-compose.pi5-8gb.yml` |
| `pi5-16gb` | 2.5 | 8G | 6G | `deploy/docker-compose.pi5-16gb.yml` |

## Directory Structure

```
apiary-bench/
├── README.md                     # This file
├── requirements.txt              # Python dependencies
├── config.yaml                   # Benchmark configuration
├── generate_datasets.py          # Main dataset generation entry point
├── load_data.py                  # In-container data loader for Docker engine
├── generators/
│   ├── __init__.py
│   ├── ssb_generator.py          # Star Schema Benchmark data
│   ├── tpch_generator.py         # TPC-H derived data
│   ├── apiary_generator.py       # Custom Apiary benchmark data
│   └── utils.py                  # Shared generation utilities
├── queries/
│   ├── ssb/                      # 13 SSB queries (4 flights)
│   ├── tpch/                     # 22 TPC-H derived queries
│   └── apiary/                   # Custom benchmark queries
├── bench_runner.py               # Benchmark execution framework
└── results/                      # Output directory
```

## Benchmark Methodology

Following Jim Gray's four principles: relevance, portability, scalability, simplicity.

**Execution protocol:**
1. Clear OS page cache between runs: `sync && echo 3 > /proc/sys/vm/drop_caches`
2. Run each query 3 times, report median (cold) and best (warm)
3. Separate data loading time from query execution time
4. Record: wall clock, peak RSS, CPU utilisation, disk I/O
5. Report hardware specs, OS version, storage type (SD/NVMe)

**Metrics captured per query:**
- Wall-clock latency (ms)
- Peak memory (RSS in MB)
- Rows processed / second
- Bytes scanned / second
- Partitions pruned vs total
