"""
Apiary-Specific Benchmark Data Generator.

Generates datasets designed to exercise Apiary's unique capabilities:

1. FORMAT AGNOSTIC BENCHMARK
   Same logical dataset written in Parquet, CSV, and Arrow (IPC/Feather),
   partitioned so queries must read across multiple formats simultaneously.
   This tests the Comb storage format's ability to serve heterogeneous Cells.

2. ACID UNDER ANALYTICS BENCHMARK
   Time-series sensor data with append-friendly partitioning, designed for
   concurrent read (analytical queries) + write (streaming inserts) workloads.
   Tests Ledger-based transaction isolation and consistency guarantees.

3. HETEROGENEOUS CLUSTER BENCHMARK
   Skewed partition sizes that stress workload balancing across nodes with
   different compute capabilities (Pi 4 vs Pi 5 vs x86).

4. SWARM ELASTICITY BENCHMARK
   Long-running analytical workload over many partitions, designed so that
   removing or adding a node mid-query is observable in the results.
"""

import os
import time
import numpy as np
import pyarrow as pa
from .utils import write_table, print_generation_summary


# --- Sensor data domain for IoT/edge scenarios ---

DEVICE_TYPES = ["temperature", "humidity", "pressure", "vibration",
                "current", "voltage", "flow_rate", "level"]

LOCATIONS = [f"zone_{chr(65+i)}" for i in range(8)]     # zone_A through zone_H
FACILITIES = [f"facility_{i:02d}" for i in range(1, 5)]  # facility_01 through facility_04


class ApiaryBenchGenerator:
    """Generate datasets for Apiary-specific benchmarks."""

    def __init__(self, scale_factor: int = 1, seed: int = 42):
        self.sf = scale_factor
        self.rng = np.random.default_rng(seed)

        # Sensor readings: ~2M per SF (enough for meaningful aggregation)
        self.n_readings = self.sf * 2_000_000
        self.n_devices = self.sf * 200

    # ----------------------------------------------------------------
    # 1. FORMAT AGNOSTIC BENCHMARK
    # ----------------------------------------------------------------

    def generate_format_agnostic(self, output_dir: str,
                                  compression: str = "snappy") -> dict:
        """Generate the same dataset in three formats with interleaved partitions.

        Creates a sensor_readings dataset partitioned by date, where:
          - Even-numbered days are stored as Parquet
          - Days divisible by 3 are stored as CSV
          - Remaining days are stored as Arrow/IPC

        This simulates a real Apiary scenario where different nodes may write
        in different formats, and queries must read across all of them.
        """
        base = os.path.join(output_dir, "apiary", f"sf{self.sf}", "format_agnostic")
        os.makedirs(base, exist_ok=True)
        results = {}

        print(f"\n{'='*60}")
        print(f"Format Agnostic Benchmark — Scale Factor {self.sf}")
        print(f"{'='*60}")

        # Generate device registry (small dimension)
        devices = self._generate_device_registry()
        results["devices_parquet"] = write_table(
            devices, os.path.join(base, "devices"), "parquet", compression
        )
        results["devices_csv"] = write_table(
            devices, os.path.join(base, "devices"), "csv"
        )
        results["devices_arrow"] = write_table(
            devices, os.path.join(base, "devices"), "arrow"
        )

        # Generate 30 days of sensor readings, partitioned by day
        readings_per_day = self.n_readings // 30
        print(f"\nGenerating {30} daily partitions ({readings_per_day:,} readings/day)...")

        for day in range(1, 31):
            partition = self._generate_sensor_partition(
                day=day, n_readings=readings_per_day
            )

            # Assign format based on day number (simulating heterogeneous writes)
            if day % 6 == 0:
                # Days 6,12,18,24,30: CSV (oldest/legacy data)
                fmt = "csv"
            elif day % 2 == 0:
                # Days 2,4,8,10,14,16,20,22,26,28: Parquet (default)
                fmt = "parquet"
            else:
                # Days 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29: Arrow (hot data)
                fmt = "arrow"

            part_name = f"readings_day_{day:02d}"
            results[part_name] = write_table(
                partition,
                os.path.join(base, "readings", f"day={day:02d}", "data"),
                fmt, compression
            )
            print(f"  Day {day:02d}: {fmt:<8} ({readings_per_day:,} rows)")

        # Write a manifest describing the partition layout
        manifest = {
            "table": "sensor_readings",
            "partitions": 30,
            "partition_key": "day",
            "formats": {"parquet": 10, "arrow": 15, "csv": 5},
            "total_rows": self.n_readings,
            "schema": ["device_id", "timestamp_ms", "value", "quality",
                        "facility", "location"],
        }
        import json
        manifest_path = os.path.join(base, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        print_generation_summary(results)
        return results

    # ----------------------------------------------------------------
    # 2. ACID UNDER ANALYTICS BENCHMARK
    # ----------------------------------------------------------------

    def generate_acid_benchmark(self, output_dir: str,
                                 compression: str = "snappy") -> dict:
        """Generate dataset for concurrent read/write testing.

        Creates:
          - Base dataset: 80% of total readings (the "existing" data)
          - Write batches: 20% split into small append batches
          - Query workload expects consistent reads during writes
        """
        base = os.path.join(output_dir, "apiary", f"sf{self.sf}", "acid")
        os.makedirs(base, exist_ok=True)
        results = {}

        print(f"\n{'='*60}")
        print(f"ACID Under Analytics Benchmark — Scale Factor {self.sf}")
        print(f"{'='*60}")

        # Device dimension (shared)
        devices = self._generate_device_registry()
        results["devices"] = write_table(
            devices, os.path.join(base, "devices"), "parquet", compression
        )

        # Base data: 80% of readings, already committed
        n_base = int(self.n_readings * 0.8)
        n_append = self.n_readings - n_base
        batch_size = max(1000, n_append // 100)  # 100 write batches

        print(f"\nBase data: {n_base:,} rows")
        print(f"Append data: {n_append:,} rows in {n_append // batch_size} batches "
              f"of {batch_size:,}")

        # Generate base data partitioned by facility
        for i, facility in enumerate(FACILITIES):
            facility_rows = n_base // len(FACILITIES)
            partition = self._generate_sensor_partition(
                day=None, n_readings=facility_rows, facility_filter=facility
            )
            results[f"base_{facility}"] = write_table(
                partition,
                os.path.join(base, "base", f"facility={facility}", "data"),
                "parquet", compression
            )
            print(f"  Base partition {facility}: {facility_rows:,} rows")

        # Generate append batches (to be written during query execution)
        append_dir = os.path.join(base, "append_batches")
        os.makedirs(append_dir, exist_ok=True)
        batch_num = 0
        rows_remaining = n_append

        while rows_remaining > 0:
            current_batch = min(batch_size, rows_remaining)
            batch = self._generate_sensor_partition(
                day=None, n_readings=current_batch,
                timestamp_offset=n_base + (n_append - rows_remaining)
            )
            results[f"batch_{batch_num:04d}"] = write_table(
                batch,
                os.path.join(append_dir, f"batch_{batch_num:04d}"),
                "parquet", compression
            )
            batch_num += 1
            rows_remaining -= current_batch

        print(f"  Generated {batch_num} append batches")

        # Write expected aggregates for consistency validation
        # (After all batches are applied, these should be the correct totals)
        self._write_expected_aggregates(
            base, n_base + n_append, compression, results
        )

        print_generation_summary(results)
        return results

    # ----------------------------------------------------------------
    # 3. HETEROGENEOUS CLUSTER BENCHMARK
    # ----------------------------------------------------------------

    def generate_heterogeneous(self, output_dir: str,
                                compression: str = "snappy") -> dict:
        """Generate skewed partitions for heterogeneous cluster testing.

        Creates partitions with deliberately uneven sizes to test whether
        the swarm coordination (waggle dance / ABC worker allocation) can
        balance work across nodes with different capabilities.

        Partition size distribution (Zipf-like):
          - 2 large partitions:  ~40% of data each  (heavy hitters)
          - 4 medium partitions: ~10% of data total
          - 10 small partitions: ~10% of data total

        On a uniform scheduler, the large partitions become bottlenecks.
        On Apiary's swarm scheduler, faster nodes should claim more work.
        """
        base = os.path.join(output_dir, "apiary", f"sf{self.sf}", "heterogeneous")
        os.makedirs(base, exist_ok=True)
        results = {}

        print(f"\n{'='*60}")
        print(f"Heterogeneous Cluster Benchmark — Scale Factor {self.sf}")
        print(f"{'='*60}")

        total_rows = self.n_readings

        # Zipf-like distribution across 16 partitions
        partition_fractions = np.array([
            0.25, 0.20,                              # 2 large (45%)
            0.08, 0.07, 0.06, 0.05,                  # 4 medium (26%)
            0.04, 0.04, 0.03, 0.03,                  # 4 small (14%)
            0.025, 0.025, 0.02, 0.02, 0.02, 0.015,   # 6 tiny (15%)
        ])
        partition_fractions = partition_fractions / partition_fractions.sum()

        partition_rows = (partition_fractions * total_rows).astype(int)
        partition_rows[-1] = total_rows - partition_rows[:-1].sum()  # Fix rounding

        print(f"\nTotal rows: {total_rows:,}")
        print(f"Partitions: {len(partition_rows)}")
        print(f"Largest:  {partition_rows[0]:>10,} ({100*partition_fractions[0]:.1f}%)")
        print(f"Smallest: {partition_rows[-1]:>10,} ({100*partition_fractions[-1]:.1f}%)")
        print(f"Skew ratio: {partition_rows[0] / partition_rows[-1]:.1f}x\n")

        for i, n_rows in enumerate(partition_rows):
            partition = self._generate_sensor_partition(
                day=i + 1, n_readings=n_rows
            )
            results[f"partition_{i:02d}"] = write_table(
                partition,
                os.path.join(base, f"partition_{i:02d}", "data"),
                "parquet", compression
            )
            pct = 100 * n_rows / total_rows
            print(f"  Partition {i:02d}: {n_rows:>10,} rows ({pct:5.1f}%)")

        print_generation_summary(results)
        return results

    # ----------------------------------------------------------------
    # 4. SWARM ELASTICITY BENCHMARK
    # ----------------------------------------------------------------

    def generate_elasticity(self, output_dir: str,
                             compression: str = "snappy") -> dict:
        """Generate many small partitions for elasticity testing.

        Creates 100+ small partitions so that a multi-minute scan is in progress
        when nodes join/leave. The benchmark runner will kill/add nodes at
        specific timestamps and measure recovery.
        """
        base = os.path.join(output_dir, "apiary", f"sf{self.sf}", "elasticity")
        os.makedirs(base, exist_ok=True)
        results = {}

        n_partitions = 100
        rows_per_partition = self.n_readings // n_partitions

        print(f"\n{'='*60}")
        print(f"Swarm Elasticity Benchmark — Scale Factor {self.sf}")
        print(f"{'='*60}")
        print(f"\n{n_partitions} partitions × {rows_per_partition:,} rows each")

        for i in range(n_partitions):
            partition = self._generate_sensor_partition(
                day=i % 30 + 1, n_readings=rows_per_partition
            )
            results[f"part_{i:03d}"] = write_table(
                partition,
                os.path.join(base, f"partition_{i:03d}", "data"),
                "parquet", compression
            )
            if (i + 1) % 20 == 0:
                print(f"  {i+1}/{n_partitions} partitions generated")

        print_generation_summary(results)
        return results

    # ----------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------

    def _generate_device_registry(self) -> pa.Table:
        """Generate device dimension table."""
        n = self.n_devices
        rng = self.rng

        return pa.table({
            "device_id": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "device_type": pa.array(rng.choice(DEVICE_TYPES, size=n)),
            "facility": pa.array(rng.choice(FACILITIES, size=n)),
            "location": pa.array(rng.choice(LOCATIONS, size=n)),
            "install_date": pa.array([
                f"202{rng.integers(0,4)}-{rng.integers(1,13):02d}-"
                f"{rng.integers(1,29):02d}"
                for _ in range(n)
            ]),
            "calibration_offset": pa.array(
                np.round(rng.normal(0, 0.1, size=n), 4), type=pa.float64()
            ),
            "min_threshold": pa.array(
                np.round(rng.uniform(0, 10, size=n), 2), type=pa.float64()
            ),
            "max_threshold": pa.array(
                np.round(rng.uniform(90, 100, size=n), 2), type=pa.float64()
            ),
        })

    def _generate_sensor_partition(self, day: int | None, n_readings: int,
                                    facility_filter: str | None = None,
                                    timestamp_offset: int = 0) -> pa.Table:
        """Generate a partition of sensor readings.

        Args:
            day: Day of month (1-30) for timestamp generation, or None for random
            n_readings: Number of readings to generate
            facility_filter: If set, all readings come from this facility
            timestamp_offset: Offset for timestamp generation (for append batches)
        """
        rng = self.rng

        device_ids = rng.integers(1, self.n_devices + 1, size=n_readings)

        # Generate timestamps (milliseconds since epoch-ish)
        if day is not None:
            base_ts = day * 86_400_000  # Day offset in ms
            timestamps = base_ts + rng.integers(0, 86_400_000, size=n_readings)
        else:
            timestamps = (timestamp_offset * 1000 +
                          rng.integers(0, 86_400_000 * 30, size=n_readings))

        timestamps = timestamps.astype(np.int64)

        # Sensor values: realistic distribution with occasional anomalies
        values = rng.normal(50.0, 15.0, size=n_readings)
        # Inject ~2% anomalies (spikes)
        anomaly_mask = rng.random(size=n_readings) < 0.02
        values[anomaly_mask] = rng.uniform(90, 120, size=anomaly_mask.sum())
        values = np.round(values, 4)

        # Quality flags: 0=good, 1=suspect, 2=bad
        quality = rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
                              size=n_readings).astype(np.int8)

        if facility_filter:
            facilities = np.full(n_readings, facility_filter)
        else:
            facilities = rng.choice(FACILITIES, size=n_readings)

        locations = rng.choice(LOCATIONS, size=n_readings)

        return pa.table({
            "device_id": pa.array(device_ids, type=pa.int32()),
            "timestamp_ms": pa.array(timestamps, type=pa.int64()),
            "value": pa.array(values, type=pa.float64()),
            "quality": pa.array(quality, type=pa.int8()),
            "facility": pa.array(facilities),
            "location": pa.array(locations),
        })

    def _write_expected_aggregates(self, base_dir: str, total_rows: int,
                                    compression: str, results: dict):
        """Write expected aggregate values for ACID consistency checks.

        After all append batches are applied, queries should return these values.
        This allows the benchmark runner to verify read consistency.
        """
        import json
        expected = {
            "total_rows": total_rows,
            "n_facilities": len(FACILITIES),
            "n_locations": len(LOCATIONS),
            "facilities": FACILITIES,
            "locations": LOCATIONS,
            "note": "After all batches applied, COUNT(*) should equal total_rows. "
                    "Per-facility counts should be approximately equal.",
        }
        path = os.path.join(base_dir, "expected_aggregates.json")
        with open(path, "w") as f:
            json.dump(expected, f, indent=2)

    def generate_all(self, output_dir: str, compression: str = "snappy") -> dict:
        """Generate all Apiary-specific benchmark datasets."""
        all_results = {}

        all_results["format_agnostic"] = self.generate_format_agnostic(
            output_dir, compression
        )
        all_results["acid"] = self.generate_acid_benchmark(output_dir, compression)
        all_results["heterogeneous"] = self.generate_heterogeneous(
            output_dir, compression
        )
        all_results["elasticity"] = self.generate_elasticity(output_dir, compression)

        return all_results
