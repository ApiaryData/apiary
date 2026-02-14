# Raspberry Pi Benchmarking Configurations

This document describes the Docker Compose configurations for benchmarking Apiary performance against different Raspberry Pi hardware specifications.

## Overview

To accurately benchmark Apiary's performance on Raspberry Pi hardware without requiring physical devices, we provide Docker Compose configurations that constrain container resources to match different Raspberry Pi models. This allows for consistent, reproducible benchmarks that simulate Pi hardware characteristics.

## Available Configurations

### Raspberry Pi 3
- **CPU**: 1.4GHz 64-bit quad-core (Cortex-A53)
- **RAM**: 1GB
- **File**: `docker-compose.pi3.yml`

### Raspberry Pi 4
Four variants with different memory configurations:
- **CPU**: 1.5GHz 64-bit quad-core (Cortex-A72)
- **RAM Variants**:
  - 1GB: `docker-compose.pi4-1gb.yml`
  - 2GB: `docker-compose.pi4-2gb.yml`
  - 4GB: `docker-compose.pi4-4gb.yml`
  - 8GB: `docker-compose.pi4-8gb.yml`

### Raspberry Pi 5
Five variants with different memory configurations:
- **CPU**: 2.4GHz quad-core 64-bit (Cortex-A76)
- **RAM Variants**:
  - 1GB: `docker-compose.pi5-1gb.yml`
  - 2GB: `docker-compose.pi5-2gb.yml`
  - 4GB: `docker-compose.pi5-4gb.yml`
  - 8GB: `docker-compose.pi5-8gb.yml`
  - 16GB: `docker-compose.pi5-16gb.yml`

## Resource Constraints

Each configuration applies Docker resource limits to simulate Pi hardware:

### CPU Limits
Docker's CPU limits are relative to the host CPU performance. We use the following approximations:
- **Pi 3** (1.4GHz): `cpus: 2.0` (~50% of modern CPU performance)
- **Pi 4** (1.5GHz): `cpus: 2.0` (~50% of modern CPU performance)
- **Pi 5** (2.4GHz): `cpus: 2.5` (~62% of modern CPU performance)

### Memory Limits
Memory is divided between MinIO (storage backend) and Apiary nodes:
- For 1GB total: 512MB MinIO, 512MB per node
- For 2GB total: 768MB MinIO, 1GB per node
- For 4GB total: 1.5GB MinIO, 2GB per node
- For 8GB total: 3GB MinIO, 4GB per node
- For 16GB total: 6GB MinIO, 8GB per node

## Usage

### Single Node Setup

Start a single Apiary node with Pi 3 constraints:
```bash
docker build -t apiary:latest .
docker compose -f docker-compose.pi3.yml up -d
```

Access MinIO console at http://localhost:9001 (credentials: minioadmin/minioadmin)

### Multi-Node Setup

Scale to multiple nodes (e.g., 3 nodes) with Pi 4 4GB constraints:
```bash
docker build -t apiary:latest .
docker compose -f docker-compose.pi4-4gb.yml up -d --scale apiary-node=3
```

### Running Benchmarks

#### Single-Node Benchmarks
Run standard benchmarks with Pi constraints:
```bash
# Pi 3 benchmark
python3 scripts/run_benchmark.py \
  --docker \
  --image apiary:latest \
  --sizes 1000,10000,50000 \
  --output benchmark_pi3.json

# Start with Pi 3 constraints (manually for now)
docker compose -f docker-compose.pi3.yml up -d
# Then run benchmarks against the running container
```

#### Multi-Node Benchmarks
Run multi-node benchmarks with Pi constraints:
```bash
# Build the image
docker build -t apiary:latest .

# Pi 4 4GB with 2 nodes
python3 scripts/run_multinode_benchmark.py \
  --compose-file docker-compose.pi4-4gb.yml \
  --nodes 2 \
  --image apiary:latest \
  --sizes 5000,10000 \
  --output pi4_4gb_multinode.json

# Pi 5 8GB with 3 nodes
python3 scripts/run_multinode_benchmark.py \
  --compose-file docker-compose.pi5-8gb.yml \
  --nodes 3 \
  --image apiary:latest \
  --sizes 5000,10000 \
  --output pi5_8gb_multinode.json
```

### Stopping

Stop and remove containers:
```bash
docker compose -f docker-compose.pi3.yml down -v
```

## Benchmark Recommendations

### Pi 3 (1GB RAM)
- **Suitable workloads**: Small datasets (< 10K rows), simple queries
- **Nodes**: 1-2 nodes recommended
- **Dataset sizes**: 1K, 5K, 10K rows

### Pi 4 (1GB-2GB RAM)
- **Suitable workloads**: Small to medium datasets (< 50K rows)
- **Nodes**: 2-3 nodes recommended
- **Dataset sizes**: 5K, 10K, 25K rows

### Pi 4 (4GB-8GB RAM)
- **Suitable workloads**: Medium datasets (< 100K rows), complex queries
- **Nodes**: 2-4 nodes recommended
- **Dataset sizes**: 10K, 50K, 100K rows

### Pi 5 (1GB-2GB RAM)
- **Suitable workloads**: Small to medium datasets with better performance than Pi 4
- **Nodes**: 2-3 nodes recommended
- **Dataset sizes**: 5K, 10K, 25K rows

### Pi 5 (4GB-16GB RAM)
- **Suitable workloads**: Large datasets (100K+ rows), complex queries, production-like loads
- **Nodes**: 2-5 nodes recommended
- **Dataset sizes**: 10K, 50K, 100K, 500K rows

## Performance Expectations

Performance will vary based on:
1. **Host CPU**: The actual CPU used for benchmarking affects relative performance
2. **Host Memory**: Available system memory for Docker containers
3. **Storage**: Local disk vs. network storage performance
4. **Workload**: Query complexity, data size, concurrent operations

## Comparing Results

When comparing benchmark results:
1. Always use the same dataset sizes across configurations
2. Run benchmarks multiple times and average results
3. Note the host system specifications in your results
4. Compare relative performance between Pi models, not absolute numbers
5. Consider memory pressure indicators (swapping, OOM errors)

## Troubleshooting

### Out of Memory Errors
If you see OOM errors:
- Reduce dataset sizes
- Reduce number of nodes
- Use a higher memory configuration
- Check host system has sufficient memory

### Slow Performance
If benchmarks are unusually slow:
- Verify CPU limits are correctly applied (`docker stats`)
- Check for host system resource contention
- Ensure Docker has sufficient resources allocated
- Consider increasing `CLUSTER_STARTUP_WAIT_SECONDS` in benchmark scripts

### Container Names
The benchmark scripts automatically detect container names:
- Dynamic compose files: `apiary-node-1`, `apiary-node-2`, etc.
- External compose files with scale: `apiary-node_1`, `apiary-node_2`, etc.

## Future Enhancements

Potential improvements:
- [ ] CPU frequency scaling simulation
- [ ] I/O bandwidth limits to match Pi storage
- [ ] Network latency simulation
- [ ] ARM architecture emulation (currently x86/x64)
- [ ] Temperature throttling simulation
- [ ] Automated benchmark comparison reports

## Contributing

When adding new Pi configurations:
1. Match official Raspberry Pi specifications exactly
2. Use consistent naming: `docker-compose.pi{version}-{ram}.yml`
3. Document CPU and memory allocations
4. Test with multi-node benchmarks
5. Update this documentation

## References

- [Raspberry Pi 3 Specifications](https://www.raspberrypi.com/products/raspberry-pi-3-model-b/)
- [Raspberry Pi 4 Specifications](https://www.raspberrypi.com/products/raspberry-pi-4-model-b/)
- [Raspberry Pi 5 Specifications](https://www.raspberrypi.com/products/raspberry-pi-5/)
- [Docker Resource Constraints](https://docs.docker.com/config/containers/resource_constraints/)
- [Docker Compose Deploy Specification](https://docs.docker.com/compose/compose-file/deploy/)
