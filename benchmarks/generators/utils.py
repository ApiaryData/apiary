"""
Shared utilities for Apiary benchmark dataset generation.
Handles output format writing (Parquet, CSV, Arrow/IPC) and common data patterns.
"""

import os
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
import pyarrow.feather as feather


def write_table(table: pa.Table, path: str, fmt: str, compression: str = "snappy",
                row_group_size: int = 100_000):
    """Write a PyArrow table to the specified format."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if fmt == "parquet":
        filepath = f"{path}.parquet"
        pq.write_table(
            table, filepath,
            compression=compression if compression != "none" else None,
            row_group_size=row_group_size,
            use_dictionary=True,
            write_statistics=True,
        )
    elif fmt == "csv":
        filepath = f"{path}.csv"
        pa_csv.write_csv(table, filepath)
    elif fmt == "arrow":
        filepath = f"{path}.arrow"
        feather.write_feather(table, filepath, compression="lz4")
    else:
        raise ValueError(f"Unknown format: {fmt}")

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    return filepath, size_mb


def random_dates(rng: np.random.Generator, start_year: int, end_year: int,
                 size: int) -> np.ndarray:
    """Generate random dates as int32 (YYYYMMDD format) for SSB/TPC-H date keys."""
    years = rng.integers(start_year, end_year + 1, size=size)
    months = rng.integers(1, 13, size=size)
    days = rng.integers(1, 29, size=size)  # Safe max for all months
    return years * 10000 + months * 100 + days


def random_strings(rng: np.random.Generator, choices: list[str],
                   size: int) -> np.ndarray:
    """Pick random strings from a list of choices."""
    indices = rng.integers(0, len(choices), size=size)
    return np.array([choices[i] for i in indices])


def generate_date_dimension(start_year: int = 1992, end_year: int = 1998) -> pa.Table:
    """Generate a date dimension table covering the specified year range.
    Used by both SSB and TPC-H benchmarks.
    """
    dates = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for day in range(1, 29):  # 28 days per month for simplicity
                datekey = year * 10000 + month * 100 + day
                month_names = [
                    "January", "February", "March", "April", "May", "June",
                    "July", "August", "September", "October", "November", "December"
                ]
                day_names = [
                    "Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday"
                ]
                weekday_idx = (datekey % 7)
                dates.append({
                    "d_datekey": datekey,
                    "d_date": f"{year}-{month:02d}-{day:02d}",
                    "d_dayofweek": day_names[weekday_idx],
                    "d_month": month_names[month - 1],
                    "d_year": year,
                    "d_yearmonthnum": year * 100 + month,
                    "d_yearmonth": f"{month_names[month-1][:3]}{year}",
                    "d_daynuminweek": weekday_idx + 1,
                    "d_daynuminmonth": day,
                    "d_daynuminyear": (month - 1) * 28 + day,
                    "d_monthnuminyear": month,
                    "d_weeknuminyear": ((month - 1) * 28 + day - 1) // 7 + 1,
                    "d_sellingseason": "Winter" if month in (12, 1, 2)
                                      else "Spring" if month in (3, 4, 5)
                                      else "Summer" if month in (6, 7, 8)
                                      else "Fall",
                    "d_lastdayinweekfl": 1 if weekday_idx == 6 else 0,
                    "d_lastdayinmonthfl": 1 if day == 28 else 0,
                    "d_holidayfl": 1 if (month == 12 and day == 25) else 0,
                    "d_weekdayfl": 1 if weekday_idx < 5 else 0,
                })

    import pyarrow as pa
    return pa.Table.from_pylist(dates)


def print_generation_summary(tables: dict[str, tuple[str, float]]):
    """Print a summary table of generated datasets."""
    print("\n" + "=" * 60)
    print(f"{'Table':<25} {'Format':<10} {'Size (MB)':>10}")
    print("-" * 60)
    total = 0.0
    for name, (filepath, size_mb) in tables.items():
        fmt = filepath.rsplit(".", 1)[-1]
        print(f"{name:<25} {fmt:<10} {size_mb:>10.1f}")
        total += size_mb
    print("-" * 60)
    print(f"{'TOTAL':<25} {'':10} {total:>10.1f}")
    print("=" * 60)
