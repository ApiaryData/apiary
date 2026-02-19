#!/usr/bin/env python3
"""
Apiary Benchmark Runner

Executes benchmark queries against Apiary (or comparison engines),
captures timing metrics, and produces structured results.

Usage:
  # Run SSB benchmark suite
  python bench_runner.py --suite ssb --data-dir ./data/ssb/sf1/parquet

  # Run all suites with custom config
  python bench_runner.py --config config.yaml

  # Run specific queries
  python bench_runner.py --suite ssb --queries Q1.1 Q2.1 Q3.1 Q4.1

  # Compare formats (run same queries across parquet/csv/arrow)
  python bench_runner.py --suite apiary_format --data-dir ./data/apiary/sf1/format_agnostic

  # Dry run (parse queries, validate config, don't execute)
  python bench_runner.py --suite ssb --dry-run
"""

import argparse
import concurrent.futures
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from statistics import median, mean

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# ----------------------------------------------------------------
# Constants
# ----------------------------------------------------------------

ANSI_ESCAPE_PREFIX = '\x1b'
CLUSTER_STARTUP_WAIT_SECONDS = 15
NODE_READY_WAIT_SECONDS = 3
CONTAINER_EXEC_TIMEOUT_SECONDS = 600

# Known table names per tier-1 suite (for SQL rewriting)
SSB_TABLES = ["lineorder", "customer", "supplier", "part", "date"]
TPCH_TABLES = ["lineitem", "orders", "customer", "part", "partsupp",
               "supplier", "nation", "region"]

# Hardware profile shortcuts → compose file paths (relative to benchmarks/)
HARDWARE_PROFILES = {
    "default": "../docker-compose.yml",
    "pi3": "../deploy/docker-compose.pi3.yml",
    "pi4-1gb": "../deploy/docker-compose.pi4-1gb.yml",
    "pi4-2gb": "../deploy/docker-compose.pi4-2gb.yml",
    "pi4-4gb": "../deploy/docker-compose.pi4-4gb.yml",
    "pi4-8gb": "../deploy/docker-compose.pi4-8gb.yml",
    "pi5-1gb": "../deploy/docker-compose.pi5-1gb.yml",
    "pi5-2gb": "../deploy/docker-compose.pi5-2gb.yml",
    "pi5-4gb": "../deploy/docker-compose.pi5-4gb.yml",
    "pi5-8gb": "../deploy/docker-compose.pi5-8gb.yml",
    "pi5-16gb": "../deploy/docker-compose.pi5-16gb.yml",
}


# ----------------------------------------------------------------
# Data classes for structured results
# ----------------------------------------------------------------

@dataclass
class QueryResult:
    """Result of a single query execution."""
    query_id: str
    query_text: str
    run_number: int
    wall_clock_ms: float
    rows_returned: int = 0
    peak_memory_mb: float = 0.0
    error: str | None = None
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class QuerySummary:
    """Aggregated results across runs for a single query."""
    query_id: str
    runs: int
    median_ms: float
    mean_ms: float
    best_ms: float
    worst_ms: float
    rows_returned: int
    peak_memory_mb: float
    all_times_ms: list[float] = field(default_factory=list)


@dataclass
class BenchmarkReport:
    """Complete benchmark report."""
    suite: str
    scale_factor: int
    data_format: str
    engine: str
    engine_version: str
    hardware: dict
    queries: list[QuerySummary]
    total_time_ms: float
    timestamp: str = ""
    config: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------
# Query parser
# ----------------------------------------------------------------

def parse_sql_file(filepath: str) -> list[tuple[str, str]]:
    """Parse a .sql file into (query_id, query_text) pairs.

    Expects queries separated by comments like:
      -- Q1.1: Description
      SELECT ...;

    Returns list of (query_id, sql_text) tuples.
    """
    with open(filepath) as f:
        content = f.read()

    queries = []
    current_id = None
    current_sql = []

    for line in content.split("\n"):
        stripped = line.strip()

        # Detect query ID from comment patterns like "-- Q1.1:", "-- FA-1:", "-- AC-1:"
        id_match = re.match(r'^--\s+(Q[\d.]+|FA-\d+|AC-\d+|HC-\d+|SE-\d+)[:\s]', stripped)
        if id_match:
            # Save previous query if exists
            if current_id and current_sql:
                sql = "\n".join(current_sql).strip().rstrip(";")
                if sql:
                    queries.append((current_id, sql))
            current_id = id_match.group(1)
            current_sql = []
            continue

        # Skip pure comment lines and empty lines at the start
        if stripped.startswith("--") or stripped.startswith("=="):
            continue

        if current_id is not None:
            current_sql.append(line)

    # Don't forget the last query
    if current_id and current_sql:
        sql = "\n".join(current_sql).strip().rstrip(";")
        if sql:
            queries.append((current_id, sql))

    return queries


# ----------------------------------------------------------------
# System info collection
# ----------------------------------------------------------------

def collect_system_info() -> dict:
    """Collect hardware and OS information for reproducibility."""
    info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "hostname": platform.node(),
    }

    if HAS_PSUTIL:
        mem = psutil.virtual_memory()
        info["ram_total_gb"] = round(mem.total / (1024**3), 2)
        info["ram_available_gb"] = round(mem.available / (1024**3), 2)
        info["cpu_count_physical"] = psutil.cpu_count(logical=False)
        info["cpu_count_logical"] = psutil.cpu_count(logical=True)

        # Detect Raspberry Pi
        try:
            with open("/proc/device-tree/model") as f:
                info["device_model"] = f.read().strip().rstrip("\x00")
        except FileNotFoundError:
            pass

        # Detect storage type
        try:
            disk = psutil.disk_partitions()
            for part in disk:
                if part.mountpoint == "/":
                    info["root_device"] = part.device
                    info["root_fstype"] = part.fstype
        except Exception:
            pass

    return info


def clear_os_cache():
    """Clear OS page cache for cold-start benchmarks.

    Requires root on Linux. Silently skips if not available.
    """
    try:
        subprocess.run(
            ["sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            capture_output=True, timeout=10
        )
        return True
    except (subprocess.TimeoutExpired, PermissionError, FileNotFoundError):
        return False


# ----------------------------------------------------------------
# Benchmark execution (pluggable engine interface)
# ----------------------------------------------------------------

class BenchmarkEngine:
    """Base class for benchmark execution engines.

    Subclass this for Apiary, DuckDB, DataFusion CLI, etc.
    """

    def __init__(self, name: str, version: str = "unknown"):
        self.name = name
        self.version = version

    def setup(self, data_dir: str, **kwargs):
        """Initialize the engine and register tables."""
        raise NotImplementedError

    def execute(self, sql: str) -> tuple[int, float]:
        """Execute a query, return (row_count, peak_memory_mb)."""
        raise NotImplementedError

    def teardown(self):
        """Clean up engine resources."""
        pass


class DataFusionEngine(BenchmarkEngine):
    """Execute queries via DataFusion Python bindings.

    This is the default engine for benchmarking Apiary's SQL layer
    in isolation (before distributed coordination is added).
    """

    def __init__(self):
        try:
            import datafusion
            version = getattr(datafusion, "__version__", "unknown")
        except ImportError:
            version = "not installed"
        super().__init__("DataFusion", version)
        self.ctx = None

    def setup(self, data_dir: str, **kwargs):
        """Register Parquet/CSV/Arrow files as tables."""
        import datafusion
        self.ctx = datafusion.SessionContext()

        data_path = Path(data_dir)
        if not data_path.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")

        # Auto-discover and register tables
        registered = []
        for f in sorted(data_path.rglob("*")):
            if f.suffix in (".parquet", ".csv", ".arrow"):
                table_name = f.stem
                # Handle partitioned datasets (use parent dir name)
                if table_name == "data":
                    table_name = f.parent.parent.name

                if f.suffix == ".parquet":
                    self.ctx.register_parquet(table_name, str(f))
                elif f.suffix == ".csv":
                    self.ctx.register_csv(table_name, str(f))
                registered.append((table_name, f.suffix, str(f)))

        print(f"\nRegistered {len(registered)} tables:")
        for name, fmt, path in registered:
            print(f"  {name} ({fmt})")

        return registered

    def execute(self, sql: str) -> tuple[int, float]:
        """Execute query and return (row_count, peak_memory_mb)."""
        if self.ctx is None:
            raise RuntimeError("Engine not set up. Call setup() first.")

        mem_before = 0
        if HAS_PSUTIL:
            proc = psutil.Process()
            mem_before = proc.memory_info().rss

        result = self.ctx.sql(sql)
        rows = result.collect()
        row_count = sum(batch.num_rows for batch in rows)

        peak_mem = 0
        if HAS_PSUTIL:
            mem_after = proc.memory_info().rss
            peak_mem = max(0, (mem_after - mem_before)) / (1024 * 1024)

        return row_count, peak_mem

    def teardown(self):
        self.ctx = None


class DryRunEngine(BenchmarkEngine):
    """Validates queries without executing them."""

    def __init__(self):
        super().__init__("DryRun", "1.0")

    def setup(self, data_dir: str, **kwargs):
        print(f"\n[DRY RUN] Would register tables from: {data_dir}")
        return []

    def execute(self, sql: str) -> tuple[int, float]:
        return 0, 0.0


# ----------------------------------------------------------------
# Docker Compose helpers
# ----------------------------------------------------------------

def _get_compose_cmd() -> list[str]:
    """Detect whether to use 'docker compose' (plugin) or 'docker-compose'."""
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            result = subprocess.run(
                [*cmd, "version"], capture_output=True, text=True,
            )
            if result.returncode == 0:
                return cmd
        except FileNotFoundError:
            pass
    raise RuntimeError(
        "Neither 'docker compose' (plugin) nor 'docker-compose' (standalone) found."
    )


def _create_compose_override(benchmarks_dir: str, image: str) -> str:
    """Generate a temporary docker-compose.override.yml.

    Adds a bind mount of the benchmarks/ directory into apiary-node
    containers and pins the image tag. Uses tmpfs for cache to avoid
    conflicts when scaling to multiple nodes.
    """
    # Normalise the host path for Docker (forward slashes, even on Windows)
    host_path = os.path.abspath(benchmarks_dir).replace("\\", "/")

    fd, path = tempfile.mkstemp(suffix=".yml", prefix="apiary-bench-override-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        # Write YAML manually to avoid requiring PyYAML at this stage
        f.write("# Auto-generated by bench_runner.py -- do not edit\n")
        f.write("services:\n")
        f.write("  apiary-node:\n")
        f.write(f"    image: {image}\n")
        f.write("    environment:\n")
        f.write("      # MinIO doesn't require a real region, but the AWS SDK does\n")
        f.write("      AWS_REGION: us-east-1\n")
        f.write("    volumes:\n")
        f.write(f"      - {host_path}:/benchmarks:ro\n")
        f.write("      # Use tmpfs for cache to avoid conflicts in multi-node scenarios.\n")
        f.write("      # 2GB matches the default max_cache_size in NodeConfig.\n")
        f.write("      # Long-form volume overrides the named volume in the base compose file.\n")
        f.write("      - type: tmpfs\n")
        f.write("        target: /home/apiary/cache\n")
        f.write("        tmpfs:\n")
        f.write("          size: 2147483648\n")
    return path


# ----------------------------------------------------------------
# Table-name rewriting for Apiary SQL
# ----------------------------------------------------------------

def rewrite_table_names(sql: str, suite: str, hive: str, box: str) -> str:
    """Rewrite bare table names in SQL to Apiary's hive.box.frame paths.

    For SSB, the 'date' table is rewritten to 'date_dim' (reserved word).
    Only replaces names that appear as standalone words preceded by
    FROM/JOIN/INTO keywords (or at table-reference positions).
    """
    if suite == "ssb":
        tables = SSB_TABLES
    elif suite == "tpch":
        tables = TPCH_TABLES
    else:
        return sql

    for table in tables:
        # The target frame name inside Apiary
        # SSB's "date" table → stored as "date_dim" to avoid SQL reserved word
        frame_name = "date_dim" if (suite == "ssb" and table == "date") else table
        fqn = f"{hive}.{box}.{frame_name}"

        # Replace word-boundary occurrences of the bare table name.
        # Matches table names after FROM, JOIN, INTO, UPDATE, or as a standalone
        # word in comma-separated table lists. Preserves column references like
        # lo_orderdate (contains 'date' but shouldn't match).
        # We use a lookbehind for common SQL keywords or comma/whitespace context.
        pattern = rf'(?i)(?<=\bFROM\s)({re.escape(table)})\b'
        sql = re.sub(pattern, fqn, sql)
        pattern = rf'(?i)(?<=\bJOIN\s)({re.escape(table)})\b'
        sql = re.sub(pattern, fqn, sql)

        # Also handle "FROM  table" (multiple spaces) and "FROM\ntable"
        pattern = rf'(?i)(\bFROM\s+)({re.escape(table)})\b'
        sql = re.sub(pattern, rf'\1{fqn}', sql)
        pattern = rf'(?i)(\bJOIN\s+)({re.escape(table)})\b'
        sql = re.sub(pattern, rf'\1{fqn}', sql)

    return sql


# ----------------------------------------------------------------
# ApiaryDockerEngine — runs queries through Apiary inside Docker
# ----------------------------------------------------------------

class ApiaryDockerEngine(BenchmarkEngine):
    """Execute tier-1 benchmarks through the full Apiary stack in Docker.

    Uses a pre-built Docker image and a Docker Compose file (optionally one
    of the hardware-profile configs in deploy/).

    Lifecycle:
      1. setup()  — starts the Docker Compose cluster, generates + ingests
                     benchmark data inside the first container.
      2. execute() — runs each SQL query inside a container via ap.sql().
      3. teardown() — tears down the cluster.
    """

    def __init__(self, image: str, compose_file: str | None = None,
                 num_nodes: int = 1, storage_url: str = "s3://apiary/bench"):
        super().__init__("Apiary-Docker", image)
        self.image = image
        self.compose_file = compose_file or os.path.join(
            os.path.dirname(__file__), "..", "docker-compose.yml"
        )
        self.num_nodes = num_nodes
        self.storage_url = storage_url
        self.override_file: str | None = None
        self._compose_cmd: list[str] = []
        self._node_index = 0  # round-robin counter for multi-node
        self._suite: str = ""
        self._hive: str = ""
        self._box: str = ""

    def _compose(self, *args: str, timeout: int = 120,
                 input_text: str | None = None) -> subprocess.CompletedProcess:
        """Run a docker compose command against our compose files."""
        cmd = [
            *self._compose_cmd,
            "-f", self.compose_file,
        ]
        if self.override_file:
            cmd.extend(["-f", self.override_file])
        cmd.extend(args)

        return subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout, input=input_text,
        )

    def _get_node_service(self) -> str:
        """Return the docker compose service name for apiary nodes."""
        return "apiary-node"

    def _run_python_in_node(self, script: str, node_index: int = 0,
                            timeout: int = CONTAINER_EXEC_TIMEOUT_SECONDS
                            ) -> subprocess.CompletedProcess:
        """Run a Python script inside the apiary-node container.

        For scaled services, docker compose exec sends to one of the
        running containers (we use --index for targeting).
        """
        cmd = [
            *self._compose_cmd,
            "-f", self.compose_file,
        ]
        if self.override_file:
            cmd.extend(["-f", self.override_file])
        cmd.extend([
            "exec", "-T",
            "--index", str(node_index + 1),  # 1-based
            self._get_node_service(),
            "python3", "-c", script,
        ])
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )

    def _ensure_image_exists(self):
        """Check that the Docker image exists locally; build it if not.

        Uses ``docker image inspect`` to probe the local daemon.  When the
        image is absent, we build it from the project root Dockerfile so
        the user does not need a manual build step.
        """
        inspect = subprocess.run(
            ["docker", "image", "inspect", self.image],
            capture_output=True, text=True,
        )
        if inspect.returncode == 0:
            return  # image already available

        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )
        dockerfile = os.path.join(project_root, "Dockerfile")
        if not os.path.isfile(dockerfile):
            raise RuntimeError(
                f"Docker image '{self.image}' not found locally and no "
                f"Dockerfile at {dockerfile} to build from.\n"
                f"Build or pull the image first:\n"
                f"  docker build -t {self.image} {project_root}"
            )

        print(f"\nImage '{self.image}' not found locally — building from "
              f"{dockerfile} ...")
        build = subprocess.run(
            ["docker", "build", "-t", self.image, project_root],
            timeout=900,  # 15 min ceiling for a from-scratch build
        )
        if build.returncode != 0:
            raise RuntimeError(
                f"Failed to build Docker image '{self.image}'.\n"
                f"Fix the build and retry, or provide a pre-built image "
                f"via --image."
            )
        print(f"Image '{self.image}' built successfully.\n")

    def setup(self, data_dir: str = "", **kwargs):
        """Start Docker cluster and load benchmark data.

        kwargs:
            suite (str): 'ssb' or 'tpch'
            scale_factor (int): 1, 10, 100
            hive (str): Apiary hive name
            box (str): Apiary box name
        """
        suite = kwargs.get("suite", "ssb")
        scale_factor = kwargs.get("scale_factor", 1)
        hive = kwargs.get("hive", suite)
        box = kwargs.get("box", f"sf{scale_factor}")

        self._suite = suite
        self._hive = hive
        self._box = box

        benchmarks_dir = os.path.dirname(os.path.abspath(__file__))
        self._compose_cmd = _get_compose_cmd()

        # Ensure the Docker image exists locally before starting the cluster.
        # If missing, build it from the project Dockerfile automatically.
        self._ensure_image_exists()

        # Generate compose override to mount benchmarks/ and pin image
        self.override_file = _create_compose_override(benchmarks_dir, self.image)
        print(f"\nCompose override: {self.override_file}")

        # Start the cluster
        print(f"Starting {self.num_nodes}-node cluster...")
        print(f"  Compose file: {self.compose_file}")
        print(f"  Image: {self.image}")

        up_result = self._compose(
            "up", "-d",
            "--scale", f"apiary-node={self.num_nodes}",
            "--wait",
            "--no-build",
            timeout=300,
        )
        if up_result.returncode != 0:
            print(f"WARNING: 'up' exited {up_result.returncode}:\n"
                  f"{up_result.stderr}", file=sys.stderr)

        print(f"Waiting for services to become healthy...")

        # Retry connecting to the node with geometric backoff
        max_retries = 10
        retry_delay = 2  # Start with 2 seconds
        for attempt in range(max_retries):
            time.sleep(retry_delay)
            
            # Verify at least one node is reachable
            probe = self._run_python_in_node('print("ok")', node_index=0, timeout=30)
            if probe.returncode == 0:
                break
            
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1}/{max_retries} failed, retrying in {retry_delay}s...")
                retry_delay = min(retry_delay * 1.5, 30)  # Geometric backoff, max 30s
        else:
            # All retries exhausted - capture container logs for debugging
            print("\n=== Container logs (last 50 lines) ===", file=sys.stderr)
            logs_result = self._compose("logs", "--tail=50", "apiary-node")
            if logs_result.stdout:
                print(logs_result.stdout, file=sys.stderr)
            print("=== End of logs ===\n", file=sys.stderr)
            
            raise RuntimeError(
                f"Cannot reach apiary-node container after {max_retries} attempts:\n{probe.stderr}"
            )

        # Verify S3/MinIO is reachable from within the container before loading data.
        # First check the health endpoint, then verify the bucket is accessible
        # via S3 API. The health endpoint can pass before the bucket is ready.
        s3_health_script = (
            "import urllib.request, sys; "
            "urllib.request.urlopen('http://minio:9000/minio/health/live', timeout=10); "
            "print('ok')"
        )
        s3_ready = False
        for attempt in range(15):
            s3_probe = self._run_python_in_node(s3_health_script, node_index=0, timeout=30)
            if s3_probe.returncode == 0:
                s3_ready = True
                break
            time.sleep(3)
        if not s3_ready:
            print("WARNING: MinIO health check did not pass; proceeding anyway.",
                  file=sys.stderr)

        # Verify the S3 bucket is accessible via the S3 API (list operation).
        # The health endpoint can return 200 before the bucket is created or
        # before S3 operations are fully functional.
        s3_bucket_script = (
            "import urllib.request, sys; "
            "req = urllib.request.Request('http://minio:9000/apiary/', method='GET'); "
            "urllib.request.urlopen(req, timeout=10); "
            "print('ok')"
        )
        bucket_ready = False
        for attempt in range(15):
            bucket_probe = self._run_python_in_node(s3_bucket_script, node_index=0, timeout=30)
            if bucket_probe.returncode == 0:
                bucket_ready = True
                break
            time.sleep(3)
        if not bucket_ready:
            print("WARNING: S3 bucket verification did not pass; proceeding anyway.",
                  file=sys.stderr)

        print(f"Cluster is up ({self.num_nodes} node(s)).")

        # Load data via load_data.py
        print(f"\nLoading {suite.upper()} SF{scale_factor} data into "
              f"{hive}.{box}...")

        load_script = (
            f"import subprocess, sys; "
            f"r = subprocess.run("
            f"['python3', '/benchmarks/load_data.py', "
            f"'--suite', '{suite}', "
            f"'--scale-factor', '{scale_factor}', "
            f"'--storage-url', '{self.storage_url}', "
            f"'--hive', '{hive}', "
            f"'--box', '{box}', "
            f"'--name', 'benchmark'], "
            f"capture_output=True, text=True); "
            f"sys.stderr.write(r.stderr); "
            f"print(r.stdout); "
            f"sys.exit(r.returncode)"
        )

        load_result = self._run_python_in_node(
            load_script, node_index=0,
            timeout=CONTAINER_EXEC_TIMEOUT_SECONDS,
        )

        if load_result.returncode != 0:
            raise RuntimeError(
                f"Data loading failed:\n{load_result.stderr}\n{load_result.stdout}"
            )

        # Parse manifest from stdout
        stdout = load_result.stdout.strip()
        # Find the JSON object in the output (skip any log lines)
        json_start = stdout.find("{")
        if json_start >= 0:
            try:
                manifest = json.loads(stdout[json_start:])
                total_rows = manifest.get("total_rows", "?")
                elapsed = manifest.get("elapsed_seconds", "?")
                print(f"Data loaded: {total_rows:,} rows in {elapsed}s")
                tables = list(manifest.get("tables", {}).keys())
                print(f"Tables: {', '.join(tables)}")
                return tables
            except json.JSONDecodeError:
                pass

        print("Data loading completed (could not parse manifest).")
        if load_result.stderr:
            # Print last few lines of loader output
            lines = load_result.stderr.strip().split("\n")
            for line in lines[-5:]:
                print(f"  {line}")

        return []

    def execute(self, sql: str) -> tuple[int, float]:
        """Execute a SQL query inside the apiary-node container.

        Returns (row_count, peak_memory_mb).
        Uses round-robin across nodes in multi-node mode.
        """
        node_index = self._node_index % self.num_nodes
        self._node_index += 1

        # Escape the SQL for safe embedding in a Python string
        escaped_sql = sql.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")

        exec_script = f"""
import sys, time
import pyarrow as pa
from apiary import Apiary

ap = Apiary("benchmark", storage="{self.storage_url}")
for _attempt in range(3):
    try:
        ap.start()
        break
    except RuntimeError as e:
        if _attempt < 2:
            # Shorter waits than load_data.py since S3 is already up at query time
            time.sleep(5 * (_attempt + 1))
            ap = Apiary("benchmark", storage="{self.storage_url}")
        else:
            raise
time.sleep({NODE_READY_WAIT_SECONDS})

try:
    start = time.time()
    results_bytes = ap.sql(\"\"\"{sql}\"\"\")
    elapsed = time.time() - start

    reader = pa.ipc.open_stream(results_bytes)
    results_table = reader.read_all()
    row_count = len(results_table)

    print(f"rows={{row_count}}")
    print(f"elapsed={{elapsed:.6f}}")
except Exception as e:
    print(f"error={{e}}", file=sys.stderr)
    sys.exit(1)
finally:
    ap.shutdown()
"""

        result = self._run_python_in_node(
            exec_script, node_index=node_index,
            timeout=CONTAINER_EXEC_TIMEOUT_SECONDS,
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"Query failed on node {node_index}: {error_msg}")

        row_count = 0
        for line in result.stdout.strip().split("\n"):
            if "=" in line and not line.strip().startswith(ANSI_ESCAPE_PREFIX):
                key, value = line.split("=", 1)
                if key.strip() == "rows":
                    row_count = int(value)

        # Memory is not easily measurable from outside; report 0
        return row_count, 0.0

    def teardown(self):
        """Stop the Docker Compose cluster and clean up."""
        print("\nStopping cluster...")
        try:
            self._compose("down", "-v", timeout=120)
            print("Cluster stopped.")
        except Exception as e:
            print(f"Warning: cluster teardown error: {e}", file=sys.stderr)

        if self.override_file and os.path.exists(self.override_file):
            os.unlink(self.override_file)
            self.override_file = None


# ----------------------------------------------------------------
# Main runner
# ----------------------------------------------------------------

def run_benchmark(
    suite: str,
    query_file: str,
    data_dir: str,
    engine: BenchmarkEngine,
    runs: int = 3,
    warmup: int = 1,
    clear_cache: bool = True,
    query_filter: list[str] | None = None,
    scale_factor: int = 1,
    **engine_kwargs,
) -> BenchmarkReport:
    """Run a complete benchmark suite and return structured results."""

    # Parse queries
    queries = parse_sql_file(query_file)
    if query_filter:
        queries = [(qid, sql) for qid, sql in queries if qid in query_filter]

    if not queries:
        print(f"No queries found in {query_file}")
        sys.exit(1)

    # Apply table-name rewriting for ApiaryDockerEngine
    is_apiary_docker = isinstance(engine, ApiaryDockerEngine)
    if is_apiary_docker:
        hive = engine_kwargs.get("hive", suite)
        box = engine_kwargs.get("box", f"sf{scale_factor}")
        queries = [
            (qid, rewrite_table_names(sql, suite, hive, box))
            for qid, sql in queries
        ]

    print(f"\n{'='*60}")
    print(f"Running: {suite} ({len(queries)} queries × {runs} runs)")
    print(f"Engine:  {engine.name} {engine.version}")
    if data_dir:
        print(f"Data:    {data_dir}")
    if is_apiary_docker:
        print(f"Profile: {engine.compose_file}")
        print(f"Nodes:   {engine.num_nodes}")
    print(f"{'='*60}\n")

    # Setup engine — pass through kwargs for ApiaryDockerEngine
    engine.setup(data_dir, suite=suite, scale_factor=scale_factor,
                 **engine_kwargs)

    # Collect system info
    hw_info = collect_system_info()

    # Detect data format
    data_format = "apiary" if is_apiary_docker else "mixed"
    if data_dir:
        if "parquet" in data_dir:
            data_format = "parquet"
        elif "csv" in data_dir:
            data_format = "csv"
        elif "arrow" in data_dir:
            data_format = "arrow"

    all_summaries = []
    total_start = time.time()

    for query_id, sql in queries:
        print(f"\n--- {query_id} ---")
        results: list[QueryResult] = []

        # Warmup runs
        for w in range(warmup):
            try:
                engine.execute(sql)
                print(f"  Warmup {w+1}/{warmup}: OK")
            except Exception as e:
                print(f"  Warmup {w+1}/{warmup}: ERROR - {e}")

        # Timed runs
        for r in range(runs):
            if clear_cache:
                clear_os_cache()

            try:
                start = time.perf_counter()
                row_count, peak_mem = engine.execute(sql)
                elapsed_ms = (time.perf_counter() - start) * 1000

                result = QueryResult(
                    query_id=query_id,
                    query_text=sql[:200],
                    run_number=r + 1,
                    wall_clock_ms=round(elapsed_ms, 2),
                    rows_returned=row_count,
                    peak_memory_mb=round(peak_mem, 2),
                )
                results.append(result)
                print(f"  Run {r+1}/{runs}: {elapsed_ms:>8.1f}ms "
                      f"({row_count} rows, {peak_mem:.1f}MB)")

            except Exception as e:
                result = QueryResult(
                    query_id=query_id,
                    query_text=sql[:200],
                    run_number=r + 1,
                    wall_clock_ms=0,
                    error=str(e),
                )
                results.append(result)
                print(f"  Run {r+1}/{runs}: ERROR - {e}")

        # Summarise
        times = [r.wall_clock_ms for r in results if r.error is None]
        if times:
            summary = QuerySummary(
                query_id=query_id,
                runs=len(times),
                median_ms=round(median(times), 2),
                mean_ms=round(mean(times), 2),
                best_ms=round(min(times), 2),
                worst_ms=round(max(times), 2),
                rows_returned=results[0].rows_returned,
                peak_memory_mb=max(r.peak_memory_mb for r in results),
                all_times_ms=times,
            )
        else:
            summary = QuerySummary(
                query_id=query_id, runs=0,
                median_ms=0, mean_ms=0, best_ms=0, worst_ms=0,
                rows_returned=0, peak_memory_mb=0,
            )
        all_summaries.append(summary)

    total_ms = (time.time() - total_start) * 1000

    # Build report
    report = BenchmarkReport(
        suite=suite,
        scale_factor=scale_factor,
        data_format=data_format,
        engine=engine.name,
        engine_version=engine.version,
        hardware=hw_info,
        queries=all_summaries,
        total_time_ms=round(total_ms, 2),
    )

    engine.teardown()
    return report


def print_report(report: BenchmarkReport):
    """Print a formatted report to stdout."""
    print(f"\n{'='*72}")
    print(f"BENCHMARK REPORT: {report.suite}")
    print(f"{'='*72}")
    print(f"Engine:       {report.engine} {report.engine_version}")
    print(f"Scale Factor: {report.scale_factor}")
    print(f"Format:       {report.data_format}")
    print(f"Platform:     {report.hardware.get('platform', 'unknown')}")
    if "device_model" in report.hardware:
        print(f"Device:       {report.hardware['device_model']}")
    print(f"RAM:          {report.hardware.get('ram_total_gb', '?')} GB")
    print(f"CPUs:         {report.hardware.get('cpu_count_logical', '?')}")
    print(f"{'='*72}")

    print(f"\n{'Query':<10} {'Median':>10} {'Best':>10} {'Worst':>10} "
          f"{'Rows':>10} {'Mem MB':>8}")
    print("-" * 72)

    for q in report.queries:
        print(f"{q.query_id:<10} {q.median_ms:>10.1f} {q.best_ms:>10.1f} "
              f"{q.worst_ms:>10.1f} {q.rows_returned:>10} "
              f"{q.peak_memory_mb:>8.1f}")

    # Compute geometric mean of median times (standard TPC-H metric)
    valid_medians = [q.median_ms for q in report.queries if q.median_ms > 0]
    if valid_medians:
        import math
        geomean = math.exp(sum(math.log(t) for t in valid_medians) / len(valid_medians))
        total_median = sum(valid_medians)
        print("-" * 72)
        print(f"{'GeoMean':<10} {geomean:>10.1f}")
        print(f"{'Total':<10} {total_median:>10.1f}")

    print(f"\nWall clock: {report.total_time_ms/1000:.1f}s")
    print(f"{'='*72}\n")


def save_report(report: BenchmarkReport, output_dir: str):
    """Save report as JSON."""
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{report.suite}_{report.engine}_{report.data_format}_sf{report.scale_factor}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(asdict(report), f, indent=2, default=str)

    print(f"Report saved: {filepath}")
    return filepath


# ----------------------------------------------------------------
# CLI
# ----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Apiary Benchmark Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run SSB via DataFusion (local, direct Parquet scan)
  python bench_runner.py --suite ssb --data-dir ./data/ssb/sf1/parquet

  # Run SSB via Apiary inside Docker (pre-built image, Pi 3 profile)
  python bench_runner.py --suite ssb --engine apiary-docker \\
      --image apiary:latest --profile pi3 --scale-factor 1

  # Run TPC-H on a 3-node cluster using Pi 4 4GB profile
  python bench_runner.py --suite tpch --engine apiary-docker \\
      --image ghcr.io/apiarydata/apiary:latest \\
      --compose-file ../deploy/docker-compose.pi4-4gb.yml \\
      --nodes 3 --scale-factor 1

  # Dry run (parse queries, show rewritten SQL, don't execute)
  python bench_runner.py --suite ssb --engine apiary-docker --dry-run
        """,
    )

    parser.add_argument(
        "--suite", "-s",
        choices=["ssb", "tpch", "apiary_format", "apiary_acid",
                 "apiary_heterogeneous", "apiary_elasticity"],
        required=True,
        help="Benchmark suite to run"
    )
    parser.add_argument(
        "--data-dir", "-d",
        default="",
        help="Path to data directory (required for datafusion engine; "
             "ignored for apiary-docker which generates data in-container)"
    )
    parser.add_argument(
        "--output", "-o",
        default="./results",
        help="Output directory for results (default: ./results)"
    )
    parser.add_argument(
        "--engine", "-e",
        choices=["datafusion", "apiary-docker", "dryrun"],
        default="datafusion",
        help="Execution engine (default: datafusion)"
    )
    parser.add_argument(
        "--runs", "-r",
        type=int, default=3,
        help="Number of timed runs per query (default: 3)"
    )
    parser.add_argument(
        "--warmup", "-w",
        type=int, default=1,
        help="Number of warmup runs (default: 1)"
    )
    parser.add_argument(
        "--queries", "-q",
        nargs="+",
        help="Specific query IDs to run (e.g., Q1.1 Q2.3)"
    )
    parser.add_argument(
        "--scale-factor",
        type=int, default=1,
        help="Scale factor for reporting (default: 1)"
    )
    parser.add_argument(
        "--no-cache-clear",
        action="store_true",
        help="Skip clearing OS page cache between runs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse queries and validate config without executing"
    )

    # --- apiary-docker engine options ---
    docker_group = parser.add_argument_group(
        "apiary-docker engine options",
        "Options for running benchmarks through Apiary inside Docker containers. "
        "Requires a pre-built Docker image."
    )
    docker_group.add_argument(
        "--image",
        default="apiary:latest",
        help="Pre-built Docker image tag (default: apiary:latest)"
    )
    docker_group.add_argument(
        "--compose-file",
        help="Docker Compose file for the cluster. Use a deploy/ profile "
             "to simulate specific hardware constraints. "
             "(default: ../docker-compose.yml)"
    )
    docker_group.add_argument(
        "--profile",
        choices=list(HARDWARE_PROFILES.keys()),
        help="Hardware profile shortcut (e.g., pi3, pi4-4gb, pi5-16gb). "
             "Overrides --compose-file."
    )
    docker_group.add_argument(
        "--nodes",
        type=int, default=1,
        help="Number of Apiary nodes to run (default: 1)"
    )
    docker_group.add_argument(
        "--storage-url",
        default="s3://apiary/bench",
        help="Apiary storage URL inside the cluster (default: s3://apiary/bench)"
    )

    args = parser.parse_args()

    # Resolve compose file from profile or explicit path
    compose_file = args.compose_file
    if args.profile:
        rel_path = HARDWARE_PROFILES[args.profile]
        compose_file = os.path.join(os.path.dirname(__file__), rel_path)

    # Validate engine-specific requirements
    if args.engine == "datafusion" and not args.data_dir and not args.dry_run:
        parser.error("--data-dir is required for the datafusion engine")

    # Determine query file
    query_files = {
        "ssb": "queries/ssb/ssb_queries.sql",
        "tpch": "queries/tpch/tpch_queries.sql",
        "apiary_format": "queries/apiary/apiary_queries.sql",
        "apiary_acid": "queries/apiary/apiary_queries.sql",
        "apiary_heterogeneous": "queries/apiary/apiary_queries.sql",
        "apiary_elasticity": "queries/apiary/apiary_queries.sql",
    }

    # Filter queries by suite prefix
    suite_query_prefix = {
        "apiary_format": ["FA-"],
        "apiary_acid": ["AC-"],
        "apiary_heterogeneous": ["HC-"],
        "apiary_elasticity": ["SE-"],
    }

    query_file = query_files[args.suite]

    # Select engine
    if args.dry_run:
        engine = DryRunEngine()
    elif args.engine == "datafusion":
        engine = DataFusionEngine()
    elif args.engine == "apiary-docker":
        engine = ApiaryDockerEngine(
            image=args.image,
            compose_file=compose_file,
            num_nodes=args.nodes,
            storage_url=args.storage_url,
        )
    else:
        engine = DryRunEngine()

    # Build query filter
    query_filter = args.queries
    if not query_filter and args.suite in suite_query_prefix:
        # Auto-filter based on suite
        all_queries = parse_sql_file(query_file)
        prefixes = suite_query_prefix[args.suite]
        query_filter = [qid for qid, _ in all_queries
                        if any(qid.startswith(p) for p in prefixes)]

    # Build engine kwargs for ApiaryDockerEngine
    engine_kwargs = {}
    if args.engine == "apiary-docker":
        engine_kwargs["hive"] = args.suite  # e.g., "ssb" or "tpch"
        engine_kwargs["box"] = f"sf{args.scale_factor}"

    # Run benchmark
    report = run_benchmark(
        suite=args.suite,
        query_file=query_file,
        data_dir=args.data_dir,
        engine=engine,
        runs=args.runs,
        warmup=args.warmup,
        clear_cache=not args.no_cache_clear,
        query_filter=query_filter,
        scale_factor=args.scale_factor,
        **engine_kwargs,
    )

    # Output
    print_report(report)
    save_report(report, args.output)


if __name__ == "__main__":
    main()
