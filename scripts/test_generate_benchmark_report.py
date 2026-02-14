#!/usr/bin/env python3
"""Tests for generate_benchmark_report.py trend analysis and history features."""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Ensure the scripts directory is importable
sys.path.insert(0, os.path.dirname(__file__))

from generate_benchmark_report import (
    compute_trend,
    trend_badge,
    load_history,
    append_history,
    format_number,
    generate_single_node_section,
    generate_multi_node_section,
    generate_trend_charts_section,
    MAX_HISTORY_ENTRIES,
)


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------

def _make_single_node_data(write_tp=100000, query_tp=200000, rows=1000):
    """Create a minimal single-node benchmark data dict."""
    return {
        "results": [
            {
                "name": "write_benchmark",
                "success": True,
                "metrics": {"rows": float(rows), "elapsed": 0.01, "throughput": write_tp},
            },
            {
                "name": "query_benchmark",
                "success": True,
                "metrics": {"rows_scanned": float(rows), "elapsed": 0.005, "throughput": query_tp},
            },
        ]
    }


def _make_multi_node_data(write_tp=50000, query_tp=80000, rows=5000):
    """Create a minimal multi-node benchmark data dict."""
    return {
        "num_nodes": 2,
        "results": [
            {
                "name": "distributed_write_benchmark",
                "success": True,
                "metrics": {"rows": float(rows), "throughput": write_tp, "elapsed": 0.1, "verified_nodes": 2},
            },
            {
                "name": "distributed_query_benchmark",
                "success": True,
                "metrics": {"rows_queried": float(rows), "throughput": query_tp, "avg_elapsed": 0.06, "nodes_alive": 2, "total_bees": 4},
            },
        ],
    }


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_compute_trend_improving():
    direction, pct = compute_trend(200, 100)
    assert direction == "improving"
    assert pct > 0


def test_compute_trend_regressing():
    direction, pct = compute_trend(80, 100)
    assert direction == "regressing"
    assert pct < 0


def test_compute_trend_stable():
    direction, pct = compute_trend(102, 100)
    assert direction == "stable"
    assert abs(pct) < 5.1


def test_compute_trend_no_previous():
    direction, pct = compute_trend(100, None)
    assert direction == "stable"
    assert pct == 0.0


def test_compute_trend_zero_previous():
    direction, pct = compute_trend(100, 0)
    assert direction == "stable"
    assert pct == 0.0


def test_trend_badge_improving():
    badge = trend_badge("improving", 15.3)
    assert "trend-improving" in badge
    assert "+15.3%" in badge


def test_trend_badge_regressing():
    badge = trend_badge("regressing", -10.5)
    assert "trend-regressing" in badge
    assert "-10.5%" in badge


def test_trend_badge_stable():
    badge = trend_badge("stable", 1.2)
    assert "trend-stable" in badge
    assert "stable" in badge


def test_load_history_missing_file():
    history = load_history("/nonexistent/path.json")
    assert history == []


def test_load_history_empty():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump([], f)
        f.flush()
        path = f.name
    try:
        history = load_history(path)
        assert history == []
    finally:
        os.unlink(path)


def test_load_history_list_format():
    entries = [{"timestamp": "2025-01-01", "run_number": "1", "commit_sha": "abc", "throughputs": {"write_1000": 100}}]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(entries, f)
        f.flush()
        path = f.name
    try:
        history = load_history(path)
        assert len(history) == 1
        assert history[0]["throughputs"]["write_1000"] == 100
    finally:
        os.unlink(path)


def test_load_history_dict_format():
    data = {"entries": [{"timestamp": "2025-01-01", "throughputs": {"write_1000": 50}}]}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        f.flush()
        path = f.name
    try:
        history = load_history(path)
        assert len(history) == 1
    finally:
        os.unlink(path)


def test_append_history_adds_entry():
    history = []
    sn = _make_single_node_data(write_tp=300000, query_tp=400000, rows=1000)
    updated = append_history(history, sn, None, "42", "deadbeef")
    assert len(updated) == 1
    assert updated[0]["run_number"] == "42"
    assert updated[0]["commit_sha"] == "deadbeef"
    assert updated[0]["throughputs"]["write_1000"] == 300000
    assert updated[0]["throughputs"]["query_1000"] == 400000


def test_append_history_includes_multinode():
    history = []
    mn = _make_multi_node_data(write_tp=60000, query_tp=90000, rows=5000)
    updated = append_history(history, None, mn, "1", "abc")
    assert "dist_write_5000" in updated[0]["throughputs"]
    assert "dist_query_5000" in updated[0]["throughputs"]
    assert updated[0]["throughputs"]["dist_write_5000"] == 60000


def test_append_history_caps_at_max():
    history = [{"timestamp": f"t{i}", "run_number": str(i), "commit_sha": "x", "throughputs": {}} for i in range(MAX_HISTORY_ENTRIES)]
    updated = append_history(history, _make_single_node_data(), None, "999", "fff")
    assert len(updated) == MAX_HISTORY_ENTRIES
    assert updated[-1]["run_number"] == "999"
    # Oldest entry should have been dropped
    assert updated[0]["run_number"] == "1"


def test_generate_single_node_section_with_trend():
    # Build a two-entry history so that previous throughput exists
    sn1 = _make_single_node_data(write_tp=100000, query_tp=200000)
    sn2 = _make_single_node_data(write_tp=150000, query_tp=180000)
    history = []
    history = append_history(history, sn1, None, "1", "aaa")
    history = append_history(history, sn2, None, "2", "bbb")

    html = generate_single_node_section(sn2, history)
    # Write throughput improved by 50%
    assert "trend-improving" in html
    # Query throughput dropped by 10%
    assert "trend-regressing" in html


def test_generate_single_node_section_no_history():
    sn = _make_single_node_data()
    history = append_history([], sn, None, "1", "x")
    html = generate_single_node_section(sn, history)
    # First run → all stable
    assert "trend-stable" in html
    assert "Single-Node Performance" in html


def test_generate_multi_node_section_with_trend():
    mn1 = _make_multi_node_data(write_tp=50000, query_tp=80000)
    mn2 = _make_multi_node_data(write_tp=30000, query_tp=120000)
    history = []
    history = append_history(history, None, mn1, "1", "a")
    history = append_history(history, None, mn2, "2", "b")

    html = generate_multi_node_section(mn2, history)
    assert "trend-regressing" in html  # write throughput dropped
    assert "trend-improving" in html   # query throughput improved


def test_generate_trend_charts_no_history():
    section, script = generate_trend_charts_section([])
    assert section == ""
    assert script == ""


def test_generate_trend_charts_single_entry():
    history = [{"timestamp": "t", "run_number": "1", "commit_sha": "x", "throughputs": {"write_1000": 100}}]
    section, script = generate_trend_charts_section(history)
    # Need at least 2 entries for charts
    assert section == ""
    assert script == ""


def test_generate_trend_charts_multiple_entries():
    history = [
        {"timestamp": "t1", "run_number": "1", "commit_sha": "aaa", "throughputs": {"write_1000": 100000, "query_1000": 200000}},
        {"timestamp": "t2", "run_number": "2", "commit_sha": "bbb", "throughputs": {"write_1000": 120000, "query_1000": 210000}},
        {"timestamp": "t3", "run_number": "3", "commit_sha": "ccc", "throughputs": {"write_1000": 110000, "query_1000": 205000}},
    ]
    section, script = generate_trend_charts_section(history)
    assert "Performance Trends" in section
    assert "trendChart" in section
    assert "Chart" in script
    assert "#1" in section or "#1" in script  # run labels
    assert "Write Throughput" in section
    assert "Query Throughput" in section


# ---------------------------------------------------------------------------
# Integration test: run the script end-to-end
# ---------------------------------------------------------------------------

def test_cli_generates_report_with_history():
    """Run the script via CLI and verify output files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write sample single-node data
        sn_path = os.path.join(tmpdir, "single.json")
        with open(sn_path, "w") as f:
            json.dump(_make_single_node_data(), f)

        # Create a pre-existing history with one entry
        hist_path = os.path.join(tmpdir, "benchmark_history.json")
        prev = _make_single_node_data(write_tp=90000, query_tp=190000)
        history = append_history([], prev, None, "1", "aaa")
        with open(hist_path, "w") as f:
            json.dump(history, f)

        out_html = os.path.join(tmpdir, "report", "index.html")

        script = os.path.join(os.path.dirname(__file__), "generate_benchmark_report.py")
        result = subprocess.run(
            [
                sys.executable, script,
                "--single-node", sn_path,
                "--history", hist_path,
                "--output", out_html,
                "--run-number", "2",
                "--commit-sha", "deadbeef123456",
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Script failed: {result.stderr}"

        # Verify HTML was generated
        assert os.path.exists(out_html)
        with open(out_html) as f:
            html = f.read()
        assert "Apiary Performance Benchmarks" in html
        assert "Performance Trends" in html
        assert "trendChart" in html
        assert "trend-" in html  # at least one trend badge

        # Verify history file was updated
        updated_hist = os.path.join(tmpdir, "report", "benchmark_history.json")
        assert os.path.exists(updated_hist)
        with open(updated_hist) as f:
            entries = json.load(f)
        assert len(entries) == 2
        assert entries[-1]["run_number"] == "2"


def test_cli_no_history_flag():
    """Script works without --history (first-ever run)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sn_path = os.path.join(tmpdir, "single.json")
        with open(sn_path, "w") as f:
            json.dump(_make_single_node_data(), f)

        out_html = os.path.join(tmpdir, "report", "index.html")
        script = os.path.join(os.path.dirname(__file__), "generate_benchmark_report.py")
        result = subprocess.run(
            [sys.executable, script, "--single-node", sn_path, "--output", out_html],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Script failed: {result.stderr}"
        assert os.path.exists(out_html)
        # History file should still be written (with 1 entry)
        hist = os.path.join(tmpdir, "report", "benchmark_history.json")
        assert os.path.exists(hist)
        with open(hist) as f:
            entries = json.load(f)
        assert len(entries) == 1


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
