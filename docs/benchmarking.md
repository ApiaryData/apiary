# Running Apiary Benchmarks

This guide explains how to run performance benchmarks for Apiary using synthetic data and Docker.

## Overview

The benchmark suite tests Apiary's performance with synthetic datasets to:
1. **Prove functionality**: Verify that the Docker image works correctly
2. **Measure performance**: Establish baseline metrics for write and query operations
3. **Track regressions**: Monitor performance changes over time

## Quick Start

### Prerequisites

- Docker installed and running
- Python 3.9+ with pyarrow installed
- Git repository cloned locally

### Run Benchmarks with Docker

```bash
# Build the Docker image (if not already built)
docker build -t apiary:latest .

# Install Python dependencies
pip install pyarrow pandas

# Run benchmarks with default settings
python3 scripts/run_benchmark.py --docker --image apiary:latest

# Run with custom dataset sizes
python3 scripts/run_benchmark.py --docker --sizes 1000,10000,100000

# Save results to a file
python3 scripts/run_benchmark.py --docker --output my_results.json
```

### Run Benchmarks Locally (Without Docker)

```bash
# Build and install Apiary
maturin develop --release

# Install Python dependencies
pip install pyarrow pandas

# Run benchmarks
python3 scripts/run_benchmark.py --no-docker --output local_results.json
```

## Benchmark Tests

### Write Benchmark

Tests the throughput of writing synthetic data to Apiary:

- Creates a hive, box, and frame
- Generates random user event data (user_id, value, timestamp)
- Writes data using `write_to_frame()`
- Measures rows/second throughput

**Metrics collected:**
- `rows`: Number of rows written
- `elapsed`: Time taken in seconds
- `throughput`: Rows per second

### Query Benchmark

Tests the throughput of SQL queries with aggregation:

- Writes synthetic data to a frame
- Executes `SELECT user_id, AVG(value) FROM ... GROUP BY user_id`
- Measures query execution time and throughput

**Metrics collected:**
- `rows_scanned`: Number of input rows
- `result_rows`: Number of output rows after aggregation
- `elapsed`: Query execution time in seconds
- `throughput`: Rows scanned per second

## Dataset Sizes

Default dataset sizes are: 1,000, 10,000, 100,000 rows

You can customize with the `--sizes` parameter:

```bash
# Test small datasets for quick validation
python3 scripts/run_benchmark.py --sizes 100,500,1000

# Test large datasets for stress testing
python3 scripts/run_benchmark.py --sizes 50000,100000,500000

# Single dataset size
python3 scripts/run_benchmark.py --sizes 10000
```

## Understanding Results

### Benchmark Output

The benchmark script outputs results in JSON format:

```json
{
  "timestamp": "2026-02-12T20:57:00.000000",
  "use_docker": true,
  "image": "apiary:latest",
  "dataset_sizes": [1000, 10000, 50000],
  "results": [
    {
      "name": "write_benchmark",
      "description": "Write 1000 rows to Apiary",
      "success": true,
      "error": null,
      "metrics": {
        "rows": 1000,
        "elapsed": 0.0027,
        "throughput": 371506.11
      }
    },
    ...
  ]
}
```

### Performance Expectations

Based on v1.0.0 benchmarks:

**Write Performance:**
- Small datasets (1K rows): 300K-500K rows/sec
- Medium datasets (10K rows): 1.5M-2M rows/sec
- Large datasets (50K+ rows): 2.5M+ rows/sec

**Query Performance:**
- Small datasets (1K rows): 200K+ rows/sec
- Medium datasets (10K rows): 1.5M+ rows/sec
- Large datasets (50K+ rows): 4M+ rows/sec

## Continuous Integration

Benchmarks run automatically in GitHub Actions on every push to main and every pull request.

### View CI Benchmark Results

1. Go to the [Actions tab](https://github.com/ApiaryData/apiary/actions)
2. Select a workflow run
3. Check the "Run Performance Benchmarks" job
4. View the summary with formatted results
5. Download the `benchmark-results-*` artifact for detailed JSON

### CI Configuration

The benchmark workflow (`.github/workflows/benchmark.yml`) runs:

- On push to `main` branch
- On pull requests to `main`
- Manually via workflow dispatch
- With dataset sizes: 1,000, 10,000, 50,000 rows

Results are:
- Displayed in the job summary
- Saved as artifacts (retained for 90 days)
- Available for historical comparison

## Advanced Usage

### Custom Benchmark Scenarios

You can create custom benchmark scripts by importing the `ApiaryBenchmark` class:

```python
from scripts.run_benchmark import ApiaryBenchmark

# Create benchmark runner
benchmark = ApiaryBenchmark(use_docker=True, apiary_image="apiary:latest")

# Run individual benchmarks
write_result = benchmark.run_write_benchmark(num_rows=5000)
query_result = benchmark.run_query_benchmark(num_rows=5000)

# Access metrics
print(f"Write throughput: {write_result.metrics['throughput']:.2f} rows/sec")
print(f"Query throughput: {query_result.metrics['throughput']:.2f} rows/sec")
```

### Synthetic Data Generation

Generate synthetic datasets for testing:

```bash
# Generate user events data
python3 scripts/generate_synthetic_data.py user_events --rows 10000 --output user_events.parquet

# Generate sensor readings
python3 scripts/generate_synthetic_data.py sensor_readings --rows 50000 --output sensors.parquet

# Generate sales transactions
python3 scripts/generate_synthetic_data.py sales_transactions --rows 100000 --output sales.parquet

# Preview data without saving
python3 scripts/generate_synthetic_data.py user_events --rows 100
```

Available dataset types:
- `user_events`: User activity events (user_id, event_type, timestamp, value, metadata)
- `sensor_readings`: IoT sensor data (sensor_id, timestamp, temperature, humidity, pressure)
- `sales_transactions`: Retail transactions (transaction_id, store_id, product_id, quantity, amount, timestamp)

## Troubleshooting

### Docker Image Not Found

```bash
# Build the image first
docker build -t apiary:latest .

# Or pull from registry if available
docker pull ghcr.io/apiarydata/apiary:latest
docker tag ghcr.io/apiarydata/apiary:latest apiary:latest
```

### Permission Errors

```bash
# On Linux, you may need to add your user to the docker group
sudo usermod -aG docker $USER
# Log out and back in for changes to take effect
```

### Slow Benchmarks

- Docker builds can be slow on first run (use cached builds)
- Large datasets (>100K rows) may take several minutes
- Use smaller dataset sizes for quick validation

### Python Import Errors

```bash
# Install required dependencies
pip install pyarrow pandas

# For local (non-Docker) mode, build the package
maturin develop --release
```

## Performance Tips

### For Faster Benchmarks

1. Use the Docker cache: Build once, run many times
2. Start with small datasets for quick validation
3. Use `--sizes` to test only specific scenarios
4. Run benchmarks in release mode (already default)

### For Accurate Results

1. Close other applications to minimize noise
2. Run multiple iterations and average results
3. Use consistent hardware for comparisons
4. Monitor system resources (CPU, memory, disk I/O)

## Contributing

When making performance-related changes:

1. Run benchmarks before your changes (baseline)
2. Run benchmarks after your changes
3. Compare results and document in PR description
4. Update BENCHMARKS.md if establishing new baselines
5. Include benchmark results in the PR for significant changes

## Next Steps

- Run benchmarks locally: `python3 scripts/run_benchmark.py --docker`
- View historical results: [BENCHMARKS.md](../BENCHMARKS.md)
- Understand the architecture: [docs/architecture-summary.md](architecture-summary.md)
- Explore the codebase: [README.md](../README.md)
