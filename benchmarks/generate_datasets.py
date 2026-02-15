#!/usr/bin/env python3
"""
Apiary Benchmark Dataset Generator

Generates datasets for all benchmark tiers:
  - SSB (Star Schema Benchmark): 5 tables, 13 queries
  - TPC-H Derived: 8 tables, 22 queries
  - Apiary-specific: format-agnostic, ACID, heterogeneous, elasticity

Usage:
  # Generate all benchmarks at SF1
  python generate_datasets.py --scale-factor 1 --output ./data

  # Generate SSB only at SF10 in Parquet
  python generate_datasets.py --benchmark ssb --scale-factor 10 --output ./data

  # Generate Apiary benchmarks (includes multi-format datasets)
  python generate_datasets.py --benchmark apiary --scale-factor 1 --output ./data

  # Generate everything in multiple formats for comparison
  python generate_datasets.py --scale-factor 1 --output ./data --formats parquet csv arrow
"""

import argparse
import os
import sys
import time
import json

from generators import SSBGenerator, TPCHGenerator, ApiaryBenchGenerator


def main():
    parser = argparse.ArgumentParser(
        description="Generate Apiary benchmark datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scale factor guidelines for Raspberry Pi:
  SF1  (~1GB total):   Any Pi model, correctness testing
  SF10 (~10GB total):  Pi 4 8GB / Pi 5, performance testing
  SF100 (~100GB total): Pi 5 16GB + NVMe only, stress testing

Examples:
  %(prog)s --benchmark ssb --scale-factor 1
  %(prog)s --benchmark tpch --scale-factor 10 --compression zstd
  %(prog)s --benchmark apiary --scale-factor 1
  %(prog)s --scale-factor 1 --formats parquet csv arrow
        """
    )

    parser.add_argument(
        "--benchmark", "-b",
        choices=["ssb", "tpch", "apiary", "all"],
        default="all",
        help="Which benchmark suite to generate (default: all)"
    )
    parser.add_argument(
        "--scale-factor", "-s",
        type=int, default=1,
        help="Scale factor: 1, 10, or 100 (default: 1)"
    )
    parser.add_argument(
        "--output", "-o",
        default="./data",
        help="Output directory (default: ./data)"
    )
    parser.add_argument(
        "--formats", "-f",
        nargs="+",
        choices=["parquet", "csv", "arrow"],
        default=["parquet"],
        help="Output formats for SSB/TPC-H (default: parquet). "
             "Apiary benchmarks always generate multi-format."
    )
    parser.add_argument(
        "--compression", "-c",
        choices=["snappy", "zstd", "gzip", "lz4", "none"],
        default="snappy",
        help="Parquet compression (default: snappy)"
    )
    parser.add_argument(
        "--row-group-size",
        type=int, default=100_000,
        help="Parquet row group size (default: 100000)"
    )
    parser.add_argument(
        "--seed",
        type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )

    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"# Apiary Benchmark Dataset Generator")
    print(f"# Benchmark:    {args.benchmark}")
    print(f"# Scale factor: {args.scale_factor}")
    print(f"# Output:       {args.output}")
    print(f"# Formats:      {', '.join(args.formats)}")
    print(f"# Compression:  {args.compression}")
    print(f"# Seed:         {args.seed}")
    print(f"{'#'*60}")

    os.makedirs(args.output, exist_ok=True)
    start = time.time()
    manifest = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "scale_factor": args.scale_factor,
                "benchmarks": {}}

    # --- SSB ---
    if args.benchmark in ("ssb", "all"):
        ssb = SSBGenerator(scale_factor=args.scale_factor, seed=args.seed)
        for fmt in args.formats:
            t0 = time.time()
            results = ssb.generate_all(
                args.output, fmt=fmt,
                compression=args.compression,
                row_group_size=args.row_group_size
            )
            elapsed = time.time() - t0
            manifest["benchmarks"][f"ssb_{fmt}"] = {
                "format": fmt,
                "tables": {k: {"path": v[0], "size_mb": round(v[1], 2)}
                           for k, v in results.items()},
                "generation_time_seconds": round(elapsed, 1),
            }
            print(f"\nSSB ({fmt}) generated in {elapsed:.1f}s")

    # --- TPC-H ---
    if args.benchmark in ("tpch", "all"):
        tpch = TPCHGenerator(scale_factor=args.scale_factor, seed=args.seed)
        for fmt in args.formats:
            t0 = time.time()
            results = tpch.generate_all(
                args.output, fmt=fmt,
                compression=args.compression,
                row_group_size=args.row_group_size
            )
            elapsed = time.time() - t0
            manifest["benchmarks"][f"tpch_{fmt}"] = {
                "format": fmt,
                "tables": {k: {"path": v[0], "size_mb": round(v[1], 2)}
                           for k, v in results.items()},
                "generation_time_seconds": round(elapsed, 1),
            }
            print(f"\nTPC-H Derived ({fmt}) generated in {elapsed:.1f}s")

    # --- Apiary-specific ---
    if args.benchmark in ("apiary", "all"):
        apiary = ApiaryBenchGenerator(
            scale_factor=args.scale_factor, seed=args.seed
        )
        t0 = time.time()
        results = apiary.generate_all(args.output, compression=args.compression)
        elapsed = time.time() - t0
        manifest["benchmarks"]["apiary"] = {
            "suites": list(results.keys()),
            "generation_time_seconds": round(elapsed, 1),
        }
        print(f"\nApiary benchmarks generated in {elapsed:.1f}s")

    # Write generation manifest
    total_elapsed = time.time() - start
    manifest["total_generation_time_seconds"] = round(total_elapsed, 1)
    manifest_path = os.path.join(args.output, "generation_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n{'='*60}")
    print(f"All datasets generated in {total_elapsed:.1f}s")
    print(f"Manifest: {manifest_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
