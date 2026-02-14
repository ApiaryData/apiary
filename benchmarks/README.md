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

```bash
# Install dependencies
pip install -r requirements.txt

# Generate all datasets at SF1
python generate_datasets.py --scale-factor 1 --output ./data

# Generate SSB only at SF10
python generate_datasets.py --benchmark ssb --scale-factor 10 --output ./data

# Generate multi-format datasets for Apiary-specific benchmarks
python generate_datasets.py --benchmark apiary --scale-factor 1 --output ./data

# Run benchmarks
python bench_runner.py --config config.yaml --benchmark ssb --output ./results
```

## Directory Structure

```
apiary-bench/
├── README.md                     # This file
├── requirements.txt              # Python dependencies
├── config.yaml                   # Benchmark configuration
├── generate_datasets.py          # Main dataset generation entry point
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
