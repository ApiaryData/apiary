#!/usr/bin/env python3
"""Run performance benchmarks for Apiary using Docker.

This script runs a suite of benchmarks to measure Apiary's performance
with synthetic data. Results are printed in a machine-readable format
and can be saved to track performance over time.
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


class ApiaryBenchmark:
    """Runs Apiary benchmarks."""
    
    def __init__(self, use_docker: bool = True, apiary_image: str = "apiary:latest"):
        self.use_docker = use_docker
        self.apiary_image = apiary_image
        self.results: List[BenchmarkResult] = []
    
    def _import_apiary(self):
        """Import Apiary module (only for non-Docker mode)."""
        try:
            from apiary import Apiary
            return Apiary
        except ImportError:
            print("ERROR: apiary module not found. Install with: maturin develop", file=sys.stderr)
            sys.exit(1)
    
    def _run_with_docker(self, script_path: str) -> subprocess.CompletedProcess:
        """Run a Python script inside the Docker container."""
        # Read the script content and pipe it to docker run
        with open(script_path, 'r') as f:
            script_content = f.read()
        
        # Run the script in Docker by piping it to python3
        cmd = [
            "docker", "run", "--rm", "-i",
            self.apiary_image,
            "python3"
        ]
        
        return subprocess.run(
            cmd,
            input=script_content,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
    
    def run_write_benchmark(self, num_rows: int = 10000) -> BenchmarkResult:
        """Benchmark data writing performance.
        
        Args:
            num_rows: Number of rows to write
        
        Returns:
            BenchmarkResult with write throughput metrics
        """
        result = BenchmarkResult(
            "write_benchmark",
            f"Write {num_rows} rows to Apiary"
        )
        
        try:
            if self.use_docker:
                # Create a temporary script to run in Docker
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    script_content = f"""
import sys
import time
import tempfile
import shutil
import pyarrow as pa
from apiary import Apiary

# Create temporary directory for Apiary data
tmpdir = tempfile.mkdtemp()

try:
    # Initialize Apiary
    ap = Apiary("benchmark_write", storage=tmpdir)
    ap.start()
    
    # Create namespace
    ap.create_hive("bench_hive")
    ap.create_box("bench_hive", "bench_box")
    ap.create_frame("bench_hive", "bench_box", "bench_frame", 
                   {{"user_id": "string", "value": "float64", "timestamp": "string"}})
    
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
    
    # Benchmark write
    start_time = time.time()
    ap.write_to_frame("bench_hive", "bench_box", "bench_frame", ipc_data)
    elapsed = time.time() - start_time
    
    # Output metrics
    print(f"rows={num_rows}")
    print(f"elapsed={{elapsed:.4f}}")
    print(f"throughput={{{num_rows} / elapsed:.2f}}")
    
    ap.shutdown()
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)
"""
                    f.write(script_content)
                    script_path = f.name
                
                proc = self._run_with_docker(script_path)
                os.unlink(script_path)
                
                if proc.returncode != 0:
                    result.error = f"Docker script failed: {proc.stderr}"
                    return result
                
                # Parse output - skip log lines and only parse metrics
                metrics = {}
                for line in proc.stdout.strip().split('\n'):
                    # Skip ANSI escape codes and log lines
                    if '=' in line and not line.strip().startswith('\x1b'):
                        key, value = line.split('=', 1)
                        try:
                            metrics[key] = float(value)
                        except ValueError:
                            # Skip non-numeric values
                            pass
                
                result.metrics = metrics
                result.success = True
            else:
                # Run directly with local Apiary
                Apiary = self._import_apiary()
                
                with tempfile.TemporaryDirectory() as tmpdir:
                    ap = Apiary("benchmark_write", storage=tmpdir)
                    ap.start()
                    
                    ap.create_hive("bench_hive")
                    ap.create_box("bench_hive", "bench_box")
                    ap.create_frame("bench_hive", "bench_box", "bench_frame",
                                   {"user_id": "string", "value": "float64", "timestamp": "string"})
                    
                    # Generate data
                    import random
                    user_ids = [f"user_{random.randint(0, 999)}" for _ in range(num_rows)]
                    values = [random.uniform(0.0, 1000.0) for _ in range(num_rows)]
                    timestamps = [datetime.now().isoformat() for _ in range(num_rows)]
                    
                    table = pa.table({
                        "user_id": user_ids,
                        "value": values,
                        "timestamp": timestamps,
                    })
                    
                    # Serialize table to IPC stream bytes
                    sink = pa.BufferOutputStream()
                    writer = pa.ipc.new_stream(sink, table.schema)
                    writer.write_table(table)
                    writer.close()
                    ipc_data = sink.getvalue().to_pybytes()
                    
                    start_time = time.time()
                    ap.write_to_frame("bench_hive", "bench_box", "bench_frame", ipc_data)
                    elapsed = time.time() - start_time
                    
                    result.metrics = {
                        "rows": num_rows,
                        "elapsed": elapsed,
                        "throughput": num_rows / elapsed,
                    }
                    result.success = True
                    
                    ap.shutdown()
        
        except Exception as e:
            result.error = str(e)
            import traceback
            result.error += f"\n{traceback.format_exc()}"
        
        return result
    
    def run_query_benchmark(self, num_rows: int = 10000) -> BenchmarkResult:
        """Benchmark query performance.
        
        Args:
            num_rows: Number of rows to write before querying
        
        Returns:
            BenchmarkResult with query execution time metrics
        """
        result = BenchmarkResult(
            "query_benchmark",
            f"Query {num_rows} rows with aggregation"
        )
        
        try:
            if self.use_docker:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    script_content = f"""
import sys
import time
import tempfile
import shutil
import pyarrow as pa
from apiary import Apiary

tmpdir = tempfile.mkdtemp()

try:
    ap = Apiary("benchmark_query", storage=tmpdir)
    ap.start()
    
    # Setup
    ap.create_hive("bench_hive")
    ap.create_box("bench_hive", "bench_box")
    ap.create_frame("bench_hive", "bench_box", "bench_frame", 
                   {{"user_id": "string", "value": "float64", "timestamp": "string"}})
    
    # Write data
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
    
    ap.write_to_frame("bench_hive", "bench_box", "bench_frame", ipc_data)
    
    # Benchmark query
    start_time = time.time()
    results_bytes = ap.sql("SELECT user_id, AVG(value) as avg_value FROM bench_hive.bench_box.bench_frame GROUP BY user_id")
    elapsed = time.time() - start_time
    
    # Deserialize results to get row count
    reader = pa.ipc.open_stream(results_bytes)
    results_table = reader.read_all()
    result_rows = len(results_table)
    
    rows_scanned = {num_rows}
    
    print(f"rows_scanned={{rows_scanned}}")
    print(f"result_rows={{result_rows}}")
    print(f"elapsed={{elapsed:.4f}}")
    print(f"throughput={{{num_rows} / elapsed:.2f}}")
    
    ap.shutdown()
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)
"""
                    f.write(script_content)
                    script_path = f.name
                
                proc = self._run_with_docker(script_path)
                os.unlink(script_path)
                
                if proc.returncode != 0:
                    result.error = f"Docker script failed: {proc.stderr}"
                    return result
                
                metrics = {}
                for line in proc.stdout.strip().split('\n'):
                    # Skip ANSI escape codes and log lines
                    if '=' in line and not line.strip().startswith('\x1b'):
                        key, value = line.split('=', 1)
                        try:
                            metrics[key] = float(value)
                        except ValueError:
                            # Skip non-numeric values
                            pass
                
                result.metrics = metrics
                result.success = True
            else:
                Apiary = self._import_apiary()
                
                with tempfile.TemporaryDirectory() as tmpdir:
                    ap = Apiary("benchmark_query", storage=tmpdir)
                    ap.start()
                    
                    ap.create_hive("bench_hive")
                    ap.create_box("bench_hive", "bench_box")
                    ap.create_frame("bench_hive", "bench_box", "bench_frame",
                                   {"user_id": "string", "value": "float64", "timestamp": "string"})
                    
                    import random
                    user_ids = [f"user_{random.randint(0, 999)}" for _ in range(num_rows)]
                    values = [random.uniform(0.0, 1000.0) for _ in range(num_rows)]
                    timestamps = [datetime.now().isoformat() for _ in range(num_rows)]
                    
                    table = pa.table({
                        "user_id": user_ids,
                        "value": values,
                        "timestamp": timestamps,
                    })
                    
                    # Serialize table to IPC stream bytes
                    sink = pa.BufferOutputStream()
                    writer = pa.ipc.new_stream(sink, table.schema)
                    writer.write_table(table)
                    writer.close()
                    ipc_data = sink.getvalue().to_pybytes()
                    
                    ap.write_to_frame("bench_hive", "bench_box", "bench_frame", ipc_data)
                    
                    start_time = time.time()
                    results_bytes = ap.sql("SELECT user_id, AVG(value) as avg_value FROM bench_hive.bench_box.bench_frame GROUP BY user_id")
                    elapsed = time.time() - start_time
                    
                    # Deserialize results to get row count
                    reader = pa.ipc.open_stream(results_bytes)
                    results_table = reader.read_all()
                    result_rows = len(results_table)
                    
                    result.metrics = {
                        "rows_scanned": num_rows,
                        "result_rows": result_rows,
                        "elapsed": elapsed,
                        "throughput": num_rows / elapsed,
                    }
                    result.success = True
                    
                    ap.shutdown()
        
        except Exception as e:
            result.error = str(e)
            import traceback
            result.error += f"\n{traceback.format_exc()}"
        
        return result
    
    def run_all_benchmarks(self, dataset_sizes: List[int]) -> List[BenchmarkResult]:
        """Run all benchmarks with different dataset sizes.
        
        Args:
            dataset_sizes: List of dataset sizes to test
        
        Returns:
            List of BenchmarkResults
        """
        all_results = []
        
        for size in dataset_sizes:
            print(f"\n{'='*60}", file=sys.stderr)
            print(f"Running benchmarks with {size} rows", file=sys.stderr)
            print(f"{'='*60}", file=sys.stderr)
            
            print(f"\nWrite benchmark ({size} rows)...", file=sys.stderr)
            write_result = self.run_write_benchmark(size)
            all_results.append(write_result)
            if write_result.success:
                print(f"  ✓ Write throughput: {write_result.metrics.get('throughput', 0):.2f} rows/sec",
                      file=sys.stderr)
            else:
                print(f"  ✗ Failed: {write_result.error}", file=sys.stderr)
            
            print(f"\nQuery benchmark ({size} rows)...", file=sys.stderr)
            query_result = self.run_query_benchmark(size)
            all_results.append(query_result)
            if query_result.success:
                print(f"  ✓ Query throughput: {query_result.metrics.get('throughput', 0):.2f} rows/sec",
                      file=sys.stderr)
            else:
                print(f"  ✗ Failed: {query_result.error}", file=sys.stderr)
        
        return all_results


def main():
    parser = argparse.ArgumentParser(
        description="Run Apiary performance benchmarks"
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        default=True,
        help="Run benchmarks using Docker (default: True)",
    )
    parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Run benchmarks without Docker (use local apiary installation)",
    )
    parser.add_argument(
        "--image",
        default="apiary:latest",
        help="Docker image to use (default: apiary:latest)",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default="1000,10000,100000",
        help="Comma-separated list of dataset sizes to test (default: 1000,10000,100000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for benchmark results (JSON format)",
    )
    
    args = parser.parse_args()
    
    use_docker = args.docker and not args.no_docker
    dataset_sizes = [int(s.strip()) for s in args.sizes.split(',')]
    
    print(f"Apiary Benchmark Suite", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"Mode: {'Docker' if use_docker else 'Local'}", file=sys.stderr)
    if use_docker:
        print(f"Image: {args.image}", file=sys.stderr)
    print(f"Dataset sizes: {dataset_sizes}", file=sys.stderr)
    print(f"", file=sys.stderr)
    
    benchmark = ApiaryBenchmark(use_docker=use_docker, apiary_image=args.image)
    results = benchmark.run_all_benchmarks(dataset_sizes)
    
    # Prepare output
    output = {
        "timestamp": datetime.now().isoformat(),
        "use_docker": use_docker,
        "image": args.image if use_docker else "local",
        "dataset_sizes": dataset_sizes,
        "results": [r.to_dict() for r in results],
    }
    
    # Print results
    print("\n" + "="*60, file=sys.stderr)
    print("Benchmark Summary", file=sys.stderr)
    print("="*60, file=sys.stderr)
    
    success_count = sum(1 for r in results if r.success)
    print(f"\nCompleted: {success_count}/{len(results)} benchmarks succeeded", file=sys.stderr)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to: {args.output}", file=sys.stderr)
    else:
        print(json.dumps(output, indent=2))
    
    # Exit with error if any benchmark failed
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
