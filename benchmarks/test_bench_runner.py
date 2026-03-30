#!/usr/bin/env python3
"""Tests for bench_runner.py — focused on timing instrumentation."""

import sys
import os

# Ensure benchmarks/ is importable
sys.path.insert(0, os.path.dirname(__file__))

from bench_runner import (
    _parse_timing_from_stderr,
    QueryResult,
    QuerySummary,
)
from statistics import median


# ---------------------------------------------------------------------------
# _parse_timing_from_stderr
# ---------------------------------------------------------------------------

def test_parse_timing_full_line():
    """Parse a canonical [TIMING] line with all common phases."""
    stderr = "[TIMING] query=Q1.1 total=5044ms parse=2ms plan=15ms execute=5025ms\n"
    result = _parse_timing_from_stderr(stderr)
    assert result["total"] == 5044.0
    assert result["parse"] == 2.0
    assert result["plan"] == 15.0
    assert result["execute"] == 5025.0
    # query= is non-numeric and must be absent
    assert "query" not in result


def test_parse_timing_all_phases():
    """Parse a line that includes all standard Apiary phases."""
    line = (
        "[TIMING] query=SELECT total=6000ms parse=1ms plan=10ms "
        "file_discovery=50ms metadata_read=20ms data_read=500ms execute=5419ms"
    )
    result = _parse_timing_from_stderr(line)
    assert result["file_discovery"] == 50.0
    assert result["metadata_read"] == 20.0
    assert result["data_read"] == 500.0
    assert result["execute"] == 5419.0


def test_parse_timing_no_timing_line():
    """Return empty dict when there is no [TIMING] line."""
    stderr = "Some log line\nAnother log line\n"
    result = _parse_timing_from_stderr(stderr)
    assert result == {}


def test_parse_timing_empty_stderr():
    result = _parse_timing_from_stderr("")
    assert result == {}


def test_parse_timing_zero_ms():
    """Zero-millisecond phases are still captured."""
    stderr = "[TIMING] query=trivial total=0ms parse=0ms plan=0ms execute=0ms\n"
    result = _parse_timing_from_stderr(stderr)
    assert result["total"] == 0.0
    assert result["parse"] == 0.0


def test_parse_timing_multiple_lines_uses_all():
    """Multiple [TIMING] lines (e.g., multi-query run) are all parsed."""
    stderr = (
        "[TIMING] query=Q1 total=100ms parse=1ms execute=99ms\n"
        "[TIMING] query=Q2 total=200ms parse=2ms execute=198ms\n"
    )
    result = _parse_timing_from_stderr(stderr)
    # Second line overwrites first for overlapping keys
    assert result["total"] == 200.0
    assert result["parse"] == 2.0
    assert result["execute"] == 198.0


def test_parse_timing_ignores_non_timing_lines():
    """Lines that are not [TIMING] prefixed are ignored."""
    stderr = (
        "INFO: starting query\n"
        "[TIMING] query=Q1 total=50ms parse=1ms execute=49ms\n"
        "INFO: query complete\n"
    )
    result = _parse_timing_from_stderr(stderr)
    assert result["total"] == 50.0
    assert "INFO" not in result


def test_parse_timing_missing_ms_suffix_skipped():
    """Tokens whose value cannot be parsed as a float are skipped gracefully."""
    stderr = "[TIMING] query=Q1 total=5044ms broken=notanumber\n"
    result = _parse_timing_from_stderr(stderr)
    assert result["total"] == 5044.0
    assert "broken" not in result


# ---------------------------------------------------------------------------
# QueryResult phase_timings field
# ---------------------------------------------------------------------------

def test_query_result_has_phase_timings():
    r = QueryResult(
        query_id="Q1.1",
        query_text="SELECT 1",
        run_number=1,
        wall_clock_ms=500.0,
        phase_timings={"parse": 2.0, "execute": 495.0},
    )
    assert r.phase_timings["parse"] == 2.0
    assert r.phase_timings["execute"] == 495.0


def test_query_result_default_phase_timings():
    r = QueryResult(
        query_id="Q1.1",
        query_text="SELECT 1",
        run_number=1,
        wall_clock_ms=500.0,
    )
    assert r.phase_timings == {}


# ---------------------------------------------------------------------------
# QuerySummary median_phase_ms field
# ---------------------------------------------------------------------------

def test_query_summary_has_median_phase_ms():
    s = QuerySummary(
        query_id="Q1.1",
        runs=3,
        median_ms=500.0,
        mean_ms=510.0,
        best_ms=480.0,
        worst_ms=550.0,
        rows_returned=1000,
        peak_memory_mb=0.0,
        all_times_ms=[480.0, 500.0, 550.0],
        median_phase_ms={"parse": 2.0, "execute": 495.0},
    )
    assert s.median_phase_ms["parse"] == 2.0
    assert s.median_phase_ms["execute"] == 495.0


def test_query_summary_default_median_phase_ms():
    s = QuerySummary(
        query_id="Q1.1",
        runs=3,
        median_ms=500.0,
        mean_ms=510.0,
        best_ms=480.0,
        worst_ms=550.0,
        rows_returned=1000,
        peak_memory_mb=0.0,
    )
    assert s.median_phase_ms == {}


# ---------------------------------------------------------------------------
# Phase aggregation logic (mirrors run_benchmark internals)
# ---------------------------------------------------------------------------

def test_phase_aggregation_excludes_query_and_total():
    """The 'query' and 'total' keys from [TIMING] are excluded from summaries."""
    run_timings = [
        {"query": "Q1.1", "total": 5044.0, "parse": 2.0, "execute": 5040.0},
        {"query": "Q1.1", "total": 5100.0, "parse": 3.0, "execute": 5095.0},
        {"query": "Q1.1", "total": 4980.0, "parse": 1.0, "execute": 4977.0},
    ]

    all_phases: dict[str, list[float]] = {}
    for timings in run_timings:
        for phase, ms in timings.items():
            if phase not in ("query", "total"):
                all_phases.setdefault(phase, []).append(ms)

    median_phase_ms = {
        phase: round(median(ms_list), 2)
        for phase, ms_list in all_phases.items()
        if ms_list
    }

    assert "query" not in median_phase_ms
    assert "total" not in median_phase_ms
    assert median_phase_ms["parse"] == 2.0    # median of [2, 3, 1]
    assert median_phase_ms["execute"] == 5040.0  # median of [5040, 5095, 4977]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    failed = 0
    for test_fn in tests:
        name = test_fn.__name__
        try:
            test_fn()
            print(f"  ✓ {name}")
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed out of {passed + failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
