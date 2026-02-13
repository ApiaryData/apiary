#!/usr/bin/env python3
"""Run multi-node performance benchmarks for Apiary using Docker Compose.

This script runs benchmarks to measure Apiary's distributed query performance
with multiple nodes sharing the same storage backend.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip install pyarrow", file=sys.stderr)
    sys.exit(1)


# Constants for multi-node benchmark configuration
ANSI_ESCAPE_PREFIX = '\x1b'  # ANSI escape sequences for terminal colors
CLUSTER_STARTUP_WAIT_SECONDS = 10  # Time to wait for Docker Compose services to start
NODE_READY_WAIT_SECONDS = 2  # Time to wait for Apiary node initialization
CONTAINER_EXEC_TIMEOUT_SECONDS = 60  # Timeout for executing commands in containers


class BenchmarkResult:
    """Holds results from a single benchmark run."""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.metrics: Dict[str, Any] = {}
        self.success = False
        self.error = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "success": self.success,
            "error": self.error,
            "metrics": self.metrics,
        }


class MultiNodeBenchmark:
    """Runs multi-node Apiary benchmarks."""
    
    def __init__(self, num_nodes: int = 2, apiary_image: str = "apiary:latest"):
        self.num_nodes = num_nodes
        self.apiary_image = apiary_image
        self.results: List[BenchmarkResult] = []
        self.compose_file = None
    
    def _create_compose_file(self) -> str:
        """Create a Docker Compose file for multi-node testing."""
        compose_config = {
            "version": "3.8",
            "services": {
                "minio": {
                    "image": "minio/minio:latest",
                    "environment": {
                        "MINIO_ROOT_USER": "minioadmin",
                        "MINIO_ROOT_PASSWORD": "minioadmin",
                    },
                    "command": "server /data --console-address :9001",
                    "healthcheck": {
                        "test": ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"],
                        "interval": "5s",
                        "timeout": "3s",
                        "retries": 3,
                    },
                },
                "minio-setup": {
                    "image": "minio/mc:latest",
                    "depends_on": {
                        "minio": {"condition": "service_healthy"},
                    },
                    "entrypoint": [
                        "/bin/sh", "-c",
                        "/usr/bin/mc config host add myminio http://minio:9000 minioadmin minioadmin && "
                        "(/usr/bin/mc mb myminio/apiary --ignore-existing || true)"
                    ],
                    "restart": "no",
                },
            },
        }
        
        # Add apiary nodes
        for i in range(self.num_nodes):
            node_name = f"apiary-node-{i+1}"
            compose_config["services"][node_name] = {
                "image": self.apiary_image,
                "depends_on": {
                    "minio-setup": {"condition": "service_completed_successfully"},
                },
                "environment": {
                    "AWS_ACCESS_KEY_ID": "minioadmin",
                    "AWS_SECRET_ACCESS_KEY": "minioadmin",
                    "AWS_ENDPOINT_URL": "http://minio:9000",
                    "APIARY_STORAGE_URL": "s3://apiary/multinode-bench",
                    "APIARY_NAME": "multinode-benchmark",
                    "RUST_LOG": "info",
                },
            }
        
        # Write compose file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            import yaml
            yaml.dump(compose_config, f)
            return f.name
    
    def _start_cluster(self):
        """Start the multi-node cluster."""
        print(f"Starting {self.num_nodes}-node cluster...", file=sys.stderr)
        
        # Check if PyYAML is available
        try:
            import yaml
        except ImportError:
            print("ERROR: PyYAML is required. Install with: pip install PyYAML", file=sys.stderr)
            sys.exit(1)
        
        self.compose_file = self._create_compose_file()
        
        # Start services
        cmd = ["docker", "compose", "-f", self.compose_file, "up", "-d"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to start cluster: {result.stderr}")
        
        # Wait for services to be healthy
        print("Waiting for services to be ready...", file=sys.stderr)
        time.sleep(CLUSTER_STARTUP_WAIT_SECONDS)
        
        print(f"✓ Cluster started with {self.num_nodes} nodes", file=sys.stderr)
    
    def _stop_cluster(self):
        """Stop the multi-node cluster."""
        if self.compose_file:
            print("Stopping cluster...", file=sys.stderr)
            cmd = ["docker", "compose", "-f", self.compose_file, "down", "-v"]
            subprocess.run(cmd, capture_output=True)
            os.unlink(self.compose_file)
            print("✓ Cluster stopped", file=sys.stderr)
    
    def _run_python_in_container(self, container: str, script: str) -> subprocess.CompletedProcess:
        """Run a Python script in a specific container."""
        cmd = [
            "docker", "compose", "-f", self.compose_file,
            "exec", "-T", container,
            "python3", "-c", script
        ]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=CONTAINER_EXEC_TIMEOUT_SECONDS)
    
    def run_distributed_write_benchmark(self, num_rows: int = 10000) -> BenchmarkResult:
        """Benchmark distributed data writing.
        
        Writes data on one node and verifies it's visible from other nodes.
        """
        result = BenchmarkResult(
            "distributed_write_benchmark",
            f"Write {num_rows} rows on node 1, verify visibility from all nodes"
        )
        
        try:
            script = f"""
import sys
import time
import pyarrow as pa
from apiary import Apiary

# Connect to shared storage
ap = Apiary("multinode-benchmark", storage="s3://apiary/multinode-bench")
ap.start()

# Wait for node to be ready
time.sleep({NODE_READY_WAIT_SECONDS})

# Create namespace
try:
    ap.create_hive("bench_hive")
except:
    pass  # May already exist
    
try:
    ap.create_box("bench_hive", "bench_box")
except:
    pass

try:
    ap.create_frame("bench_hive", "bench_box", "bench_frame", 
                   {{"user_id": "string", "value": "float64", "timestamp": "string"}})
except:
    pass

# Generate data
import random
from datetime import datetime
user_ids = [f"user_{{random.randint(0, 999)}}" for _ in range({num_rows})]
values = [random.uniform(0.0, 1000.0) for _ in range({num_rows})]
timestamps = [datetime.now().isoformat() for _ in range({num_rows})]

table = pa.table({{
    "user_id": user_ids,
    "value": values,
    "timestamp": timestamps,
}})

# Serialize table to IPC stream bytes
sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream(sink, table.schema)
writer.write_table(table)
writer.close()
ipc_data = sink.getvalue().to_pybytes()

# Write data
start_time = time.time()
ap.write_to_frame("bench_hive", "bench_box", "bench_frame", ipc_data)
elapsed = time.time() - start_time

print(f"rows={num_rows}")
print(f"elapsed={{elapsed:.4f}}")
print(f"throughput={{{num_rows} / elapsed:.2f}}")

ap.shutdown()
"""
            
            print(f"  Writing data on node 1...", file=sys.stderr)
            proc = self._run_python_in_container("apiary-node-1", script)
            
            if proc.returncode != 0:
                result.error = f"Write failed: {proc.stderr}"
                return result
            
            # Parse write metrics
            write_metrics = {}
            for line in proc.stdout.strip().split('\n'):
                if '=' in line and not line.strip().startswith(ANSI_ESCAPE_PREFIX):
                    key, value = line.split('=', 1)
                    try:
                        write_metrics[key] = float(value)
                    except ValueError:
                        pass
            
            # Verify data is visible from all nodes
            verify_script = f"""
import sys
import time
import pyarrow as pa
from apiary import Apiary

ap = Apiary("multinode-benchmark", storage="s3://apiary/multinode-bench")
ap.start()

time.sleep({NODE_READY_WAIT_SECONDS})

# Query the data
try:
    results_bytes = ap.sql("SELECT COUNT(*) as cnt FROM bench_hive.bench_box.bench_frame")
    reader = pa.ipc.open_stream(results_bytes)
    results_table = reader.read_all()
    count = results_table.column(0)[0].as_py()
    print(f"count={{count}}")
except Exception as e:
    print(f"error={{e}}", file=sys.stderr)
    sys.exit(1)
finally:
    ap.shutdown()
"""
            
            # Verify from each node
            verification_counts = []
            for i in range(self.num_nodes):
                node_name = f"apiary-node-{i+1}"
                print(f"  Verifying data visibility from {node_name}...", file=sys.stderr)
                proc = self._run_python_in_container(node_name, verify_script)
                
                if proc.returncode != 0:
                    result.error = f"Verification failed on {node_name}: {proc.stderr}"
                    return result
                
                # Parse count
                for line in proc.stdout.strip().split('\n'):
                    if line.startswith("count="):
                        count = int(line.split('=')[1])
                        verification_counts.append(count)
                        break
            
            # Check all nodes see the same data
            if len(verification_counts) != self.num_nodes:
                result.error = f"Failed to verify from all nodes"
                return result
            
            if not all(c == num_rows for c in verification_counts):
                result.error = f"Inconsistent counts across nodes: {verification_counts}"
                return result
            
            result.metrics = {
                **write_metrics,
                "verified_nodes": self.num_nodes,
                "consistent": True,
            }
            result.success = True
            
            print(f"  ✓ Data visible and consistent across all {self.num_nodes} nodes", file=sys.stderr)
        
        except Exception as e:
            result.error = str(e)
            import traceback
            result.error += f"\n{traceback.format_exc()}"
        
        return result
    
    def run_distributed_query_benchmark(self, num_rows: int = 10000) -> BenchmarkResult:
        """Benchmark distributed query performance.
        
        Tests query execution across multiple nodes with swarm coordination.
        """
        result = BenchmarkResult(
            "distributed_query_benchmark",
            f"Distributed query with {self.num_nodes} nodes on {num_rows} rows"
        )
        
        try:
            # First write data
            setup_script = f"""
import sys
import time
import pyarrow as pa
from apiary import Apiary

ap = Apiary("multinode-benchmark", storage="s3://apiary/multinode-bench")
ap.start()
time.sleep({NODE_READY_WAIT_SECONDS})

try:
    ap.create_hive("query_bench")
except:
    pass
    
try:
    ap.create_box("query_bench", "data")
except:
    pass

try:
    ap.create_frame("query_bench", "data", "metrics", 
                   {{"user_id": "string", "value": "float64", "category": "string"}})
except:
    pass

# Generate data with categories for GROUP BY
import random
categories = ["A", "B", "C", "D", "E"]
user_ids = [f"user_{{random.randint(0, 999)}}" for _ in range({num_rows})]
values = [random.uniform(0.0, 1000.0) for _ in range({num_rows})]
cats = [random.choice(categories) for _ in range({num_rows})]

table = pa.table({{
    "user_id": user_ids,
    "value": values,
    "category": cats,
}})

sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream(sink, table.schema)
writer.write_table(table)
writer.close()
ipc_data = sink.getvalue().to_pybytes()

ap.write_to_frame("query_bench", "data", "metrics", ipc_data)
ap.shutdown()
"""
            
            print(f"  Setting up test data...", file=sys.stderr)
            proc = self._run_python_in_container("apiary-node-1", setup_script)
            if proc.returncode != 0:
                result.error = f"Setup failed: {proc.stderr}"
                return result
            
            # Run query from different nodes and measure performance
            query_script = f"""
import sys
import time
import pyarrow as pa
from apiary import Apiary

ap = Apiary("multinode-benchmark", storage="s3://apiary/multinode-bench")
ap.start()
time.sleep({NODE_READY_WAIT_SECONDS})

# Get swarm status
try:
    swarm = ap.swarm_status()
    print(f"nodes_alive={{swarm['alive']}}")
    print(f"total_bees={{swarm['total_bees']}}")
except:
    pass

# Run query with aggregation
start_time = time.time()
results_bytes = ap.sql(
    "SELECT category, AVG(value) as avg_val, COUNT(*) as cnt "
    "FROM query_bench.data.metrics "
    "GROUP BY category"
)
elapsed = time.time() - start_time

reader = pa.ipc.open_stream(results_bytes)
results_table = reader.read_all()
result_rows = len(results_table)

print(f"elapsed={{elapsed:.4f}}")
print(f"result_rows={{result_rows}}")

ap.shutdown()
"""
            
            # Run query from multiple nodes
            query_times = []
            nodes_alive = 0
            total_bees = 0
            
            for i in range(self.num_nodes):
                node_name = f"apiary-node-{i+1}"
                print(f"  Running query from {node_name}...", file=sys.stderr)
                
                proc = self._run_python_in_container(node_name, query_script)
                if proc.returncode != 0:
                    print(f"    Warning: Query failed on {node_name}: {proc.stderr}", file=sys.stderr)
                    continue
                
                # Parse metrics
                for line in proc.stdout.strip().split('\n'):
                    if '=' in line and not line.strip().startswith(ANSI_ESCAPE_PREFIX):
                        key, value = line.split('=', 1)
                        try:
                            if key == "elapsed":
                                query_times.append(float(value))
                            elif key == "nodes_alive":
                                nodes_alive = int(value)
                            elif key == "total_bees":
                                total_bees = int(value)
                        except ValueError:
                            pass
            
            if not query_times:
                result.error = "No successful queries"
                return result
            
            avg_elapsed = sum(query_times) / len(query_times)
            
            result.metrics = {
                "rows_queried": num_rows,
                "num_nodes": self.num_nodes,
                "nodes_alive": nodes_alive,
                "total_bees": total_bees,
                "avg_elapsed": avg_elapsed,
                "throughput": num_rows / avg_elapsed,
                "query_count": len(query_times),
            }
            result.success = True
            
            print(f"  ✓ Average query time: {avg_elapsed:.4f}s across {len(query_times)} queries", 
                  file=sys.stderr)
        
        except Exception as e:
            result.error = str(e)
            import traceback
            result.error += f"\n{traceback.format_exc()}"
        
        return result
    
    def run_all_benchmarks(self, dataset_sizes: List[int]) -> List[BenchmarkResult]:
        """Run all multi-node benchmarks."""
        all_results = []
        
        try:
            self._start_cluster()
            
            for size in dataset_sizes:
                print(f"\n{'='*60}", file=sys.stderr)
                print(f"Multi-node benchmarks with {size} rows", file=sys.stderr)
                print(f"{'='*60}", file=sys.stderr)
                
                print(f"\nDistributed write benchmark ({size} rows)...", file=sys.stderr)
                write_result = self.run_distributed_write_benchmark(size)
                all_results.append(write_result)
                if write_result.success:
                    throughput = write_result.metrics.get('throughput', 0)
                    print(f"  ✓ Throughput: {throughput:.2f} rows/sec", file=sys.stderr)
                else:
                    print(f"  ✗ Failed: {write_result.error}", file=sys.stderr)
                
                print(f"\nDistributed query benchmark ({size} rows)...", file=sys.stderr)
                query_result = self.run_distributed_query_benchmark(size)
                all_results.append(query_result)
                if query_result.success:
                    throughput = query_result.metrics.get('throughput', 0)
                    print(f"  ✓ Throughput: {throughput:.2f} rows/sec", file=sys.stderr)
                else:
                    print(f"  ✗ Failed: {query_result.error}", file=sys.stderr)
        
        finally:
            self._stop_cluster()
        
        return all_results


def main():
    parser = argparse.ArgumentParser(
        description="Run Apiary multi-node performance benchmarks"
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=2,
        help="Number of nodes in the cluster (default: 2)",
    )
    parser.add_argument(
        "--image",
        default="apiary:latest",
        help="Docker image to use (default: apiary:latest)",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default="5000,10000",
        help="Comma-separated list of dataset sizes to test (default: 5000,10000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for benchmark results (JSON format)",
    )
    
    args = parser.parse_args()
    
    dataset_sizes = [int(s.strip()) for s in args.sizes.split(',')]
    
    print(f"Apiary Multi-Node Benchmark Suite", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"Nodes: {args.nodes}", file=sys.stderr)
    print(f"Image: {args.image}", file=sys.stderr)
    print(f"Dataset sizes: {dataset_sizes}", file=sys.stderr)
    print(f"", file=sys.stderr)
    
    benchmark = MultiNodeBenchmark(num_nodes=args.nodes, apiary_image=args.image)
    results = benchmark.run_all_benchmarks(dataset_sizes)
    
    # Prepare output
    output = {
        "timestamp": datetime.now().isoformat(),
        "num_nodes": args.nodes,
        "image": args.image,
        "dataset_sizes": dataset_sizes,
        "results": [r.to_dict() for r in results],
    }
    
    # Print results
    print("\n" + "="*60, file=sys.stderr)
    print("Multi-Node Benchmark Summary", file=sys.stderr)
    print("="*60, file=sys.stderr)
    
    success_count = sum(1 for r in results if r.success)
    print(f"\nCompleted: {success_count}/{len(results)} benchmarks succeeded", file=sys.stderr)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to: {args.output}", file=sys.stderr)
    else:
        print(json.dumps(output, indent=2))
    
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
