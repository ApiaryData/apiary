# Apiary Performance Benchmarks

This document tracks Apiary's performance benchmarks over time. Benchmarks are run using the Docker image with synthetic data to provide consistent, reproducible performance metrics.

## Benchmark Suite

The benchmark suite measures the following workloads:

1. **Write Performance**: Measures throughput (rows/sec) for writing data to Apiary
2. **Query Performance**: Measures throughput (rows/sec) for SQL queries with aggregation

Each benchmark is run with different dataset sizes to understand performance scaling characteristics.

## How to Run Benchmarks

### Using Docker (Recommended)

```bash
# Build the Docker image
docker build -t apiary:latest .

# Run benchmarks
python3 scripts/run_benchmark.py --docker --image apiary:latest --output benchmark_results.json
```

### Without Docker (Local Development)

```bash
# Build and install Apiary
maturin develop --release

# Run benchmarks
python3 scripts/run_benchmark.py --no-docker --output benchmark_results.json
```

### Custom Dataset Sizes

```bash
# Test with specific dataset sizes (comma-separated)
python3 scripts/run_benchmark.py --sizes 5000,50000,500000 --output benchmark_results.json
```

## Latest Benchmark Results

### Environment
- **Date**: TBD (run benchmarks to populate)
- **Platform**: TBD
- **Docker Image**: apiary:latest
- **Rust Version**: 1.78+
- **Python Version**: 3.11

### Write Performance

| Dataset Size | Throughput (rows/sec) | Elapsed Time (sec) |
|--------------|----------------------|-------------------|
| 1,000        | TBD                  | TBD               |
| 10,000       | TBD                  | TBD               |
| 100,000      | TBD                  | TBD               |

### Query Performance (GROUP BY + AVG)

| Dataset Size | Rows Scanned | Result Rows | Throughput (rows/sec) | Elapsed Time (sec) |
|--------------|--------------|-------------|----------------------|-------------------|
| 1,000        | TBD          | TBD         | TBD                  | TBD               |
| 10,000       | TBD          | TBD         | TBD                  | TBD               |
| 100,000      | TBD          | TBD         | TBD                  | TBD               |

## Performance Trends

### v1.0.0 (Initial Release)

_Benchmarks to be run and populated during CI_

## Interpreting Results

### Write Throughput
- **Good**: > 10,000 rows/sec for small datasets (< 10K rows)
- **Acceptable**: 5,000-10,000 rows/sec
- **Needs Investigation**: < 5,000 rows/sec

### Query Throughput
- **Good**: > 50,000 rows/sec for aggregation queries
- **Acceptable**: 20,000-50,000 rows/sec
- **Needs Investigation**: < 20,000 rows/sec

Note: These thresholds are preliminary and will be refined as more data is collected.

## CI Integration

Benchmarks are automatically run as part of the CI pipeline and results are uploaded as artifacts. To view historical results:

1. Go to the [Actions tab](https://github.com/ApiaryData/apiary/actions)
2. Select a workflow run
3. Download the `benchmark-results` artifact

## Contributing

When making performance-related changes:

1. Run benchmarks before and after your change
2. Document significant performance improvements or regressions in PR descriptions
3. Update this file with new baseline results after major releases

## Future Enhancements

- [ ] Multi-node benchmark tests
- [ ] Different query patterns (filters, joins, etc.)
- [ ] Memory usage tracking
- [ ] Disk I/O metrics
- [ ] Network latency impact (for multi-node)
- [ ] Comparison with other systems (DuckDB, SQLite)
