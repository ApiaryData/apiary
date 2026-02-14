#!/usr/bin/env python3
"""Generate an HTML report from benchmark results with trend analysis."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Maximum number of historical entries to retain in benchmark_history.json
MAX_HISTORY_ENTRIES = 50


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Apiary Performance Benchmarks</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            color: #2c3e50;
            margin-bottom: 10px;
            font-size: 2.5em;
        }}
        
        .subtitle {{
            color: #7f8c8d;
            margin-bottom: 30px;
            font-size: 1.1em;
        }}
        
        .metadata {{
            background: #ecf0f1;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 30px;
        }}
        
        .metadata-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        
        .metadata-item {{
            display: flex;
            flex-direction: column;
        }}
        
        .metadata-label {{
            font-weight: 600;
            color: #7f8c8d;
            font-size: 0.9em;
            margin-bottom: 5px;
        }}
        
        .metadata-value {{
            color: #2c3e50;
            font-size: 1.1em;
        }}
        
        h2 {{
            color: #34495e;
            margin-top: 40px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #3498db;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 30px;
            background: white;
        }}
        
        thead {{
            background: #3498db;
            color: white;
        }}
        
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        
        th {{
            font-weight: 600;
        }}
        
        tbody tr:hover {{
            background: #f8f9fa;
        }}
        
        .number {{
            text-align: right;
            font-family: "Courier New", monospace;
        }}
        
        .status {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        
        .status-success {{
            background: #d4edda;
            color: #155724;
        }}
        
        .status-failure {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .trend {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 8px;
            font-size: 0.8em;
            font-weight: 600;
            white-space: nowrap;
        }}
        
        .trend-improving {{
            background: #d4edda;
            color: #155724;
        }}
        
        .trend-regressing {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .trend-stable {{
            background: #e2e3e5;
            color: #383d41;
        }}
        
        .chart-container {{
            position: relative;
            margin: 20px 0 30px 0;
            padding: 15px;
            background: #fafafa;
            border-radius: 8px;
            border: 1px solid #eee;
        }}
        
        .chart-container canvas {{
            max-height: 350px;
        }}
        
        .footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            text-align: center;
            color: #7f8c8d;
        }}
        
        .footer a {{
            color: #3498db;
            text-decoration: none;
        }}
        
        .footer a:hover {{
            text-decoration: underline;
        }}
        
        .section {{
            margin-bottom: 40px;
        }}
        
        .highlight {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 20px 0;
        }}
        
        .highlight h3 {{
            margin-bottom: 10px;
            color: #856404;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🐝 Apiary Performance Benchmarks</h1>
        <p class="subtitle">Continuous performance monitoring for distributed data processing</p>
        
        <div class="metadata">
            <div class="metadata-grid">
                <div class="metadata-item">
                    <span class="metadata-label">Last Updated</span>
                    <span class="metadata-value">{timestamp}</span>
                </div>
                <div class="metadata-item">
                    <span class="metadata-label">Run Number</span>
                    <span class="metadata-value">#{run_number}</span>
                </div>
                <div class="metadata-item">
                    <span class="metadata-label">Commit</span>
                    <span class="metadata-value">{commit_sha}</span>
                </div>
                <div class="metadata-item">
                    <span class="metadata-label">Platform</span>
                    <span class="metadata-value">{platform}</span>
                </div>
            </div>
        </div>
        
        {single_node_section}
        
        {trend_charts_section}
        
        {multi_node_section}
        
        <div class="footer">
            <p>Generated by <a href="https://github.com/ApiaryData/apiary">Apiary</a> benchmark suite</p>
            <p>View <a href="https://github.com/ApiaryData/apiary/actions">workflow runs</a> or check the <a href="https://github.com/ApiaryData/apiary">repository</a></p>
        </div>
    </div>
    {chart_scripts}
</body>
</html>
"""


def format_number(num, decimals=2):
    """Format a number with thousands separators."""
    if isinstance(num, (int, float)):
        if decimals == 0:
            return f"{int(num):,}"
        return f"{num:,.{decimals}f}"
    return str(num)


def compute_trend(current, previous):
    """Compute trend between current and previous throughput values.

    Returns a tuple of (direction, pct_change) where direction is one of
    'improving', 'regressing', or 'stable'.  A change of less than 5% in
    either direction is considered stable.
    """
    if previous is None or previous == 0:
        return ("stable", 0.0)
    pct = ((current - previous) / previous) * 100.0
    if pct > 5.0:
        return ("improving", pct)
    elif pct < -5.0:
        return ("regressing", pct)
    return ("stable", pct)


def trend_badge(direction, pct):
    """Return an HTML badge for a trend indicator."""
    if direction == "improving":
        return f'<span class="trend trend-improving">▲ +{pct:.1f}%</span>'
    elif direction == "regressing":
        return f'<span class="trend trend-regressing">▼ {pct:.1f}%</span>'
    return '<span class="trend trend-stable">● stable</span>'


def _extract_throughputs(data, bench_name, size_key):
    """Extract a dict mapping dataset-size → throughput from benchmark data."""
    if not data or "results" not in data:
        return {}
    out = {}
    for r in data["results"]:
        if r.get("name") == bench_name and r.get("success"):
            m = r.get("metrics", {})
            size = int(m.get(size_key, 0))
            if size:
                out[size] = m.get("throughput", 0)
    return out


def load_history(path):
    """Load benchmark history from a JSON file.

    Returns a list of entry dicts, each with 'timestamp', 'run_number',
    'commit_sha', and 'throughputs' (keyed by benchmark label).
    """
    if not path or not Path(path).exists():
        return []
    with open(path, "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return data.get("entries", [])


def append_history(history, single_node_data, multi_node_data, run_number, commit_sha):
    """Append the current run's throughput data to the history list.

    Returns the updated list (capped at MAX_HISTORY_ENTRIES).
    """
    throughputs = {}
    # Single-node
    for label, bench, key in [
        ("write", "write_benchmark", "rows"),
        ("query", "query_benchmark", "rows_scanned"),
    ]:
        for size, tp in _extract_throughputs(single_node_data, bench, key).items():
            throughputs[f"{label}_{size}"] = tp
    # Multi-node
    for label, bench, key in [
        ("dist_write", "distributed_write_benchmark", "rows"),
        ("dist_query", "distributed_query_benchmark", "rows_queried"),
    ]:
        for size, tp in _extract_throughputs(multi_node_data, bench, key).items():
            throughputs[f"{label}_{size}"] = tp

    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "run_number": str(run_number),
        "commit_sha": str(commit_sha),
        "throughputs": throughputs,
    }
    history.append(entry)
    return history[-MAX_HISTORY_ENTRIES:]


def _previous_throughputs(history):
    """Return the throughputs dict from the second-to-last history entry."""
    if len(history) < 2:
        return {}
    return history[-2].get("throughputs", {})


def generate_single_node_section(data, history):
    """Generate HTML for single-node benchmark results with trend badges."""
    if not data or 'results' not in data:
        return ""

    prev = _previous_throughputs(history)

    write_rows = []
    query_rows = []
    
    for result in data['results']:
        if not result.get('success'):
            continue
        
        metrics = result.get('metrics', {})
        
        if result['name'] == 'write_benchmark':
            rows = int(metrics.get('rows', 0))
            throughput = metrics.get('throughput', 0)
            elapsed = metrics.get('elapsed', 0)
            direction, pct = compute_trend(throughput, prev.get(f"write_{rows}"))
            badge = trend_badge(direction, pct)
            
            write_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)} {badge}</td>
                    <td class="number">{format_number(elapsed, 4)}</td>
                </tr>
            """)
        
        elif result['name'] == 'query_benchmark':
            rows = int(metrics.get('rows_scanned', 0))
            throughput = metrics.get('throughput', 0)
            elapsed = metrics.get('elapsed', 0)
            direction, pct = compute_trend(throughput, prev.get(f"query_{rows}"))
            badge = trend_badge(direction, pct)
            
            query_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)} {badge}</td>
                    <td class="number">{format_number(elapsed, 4)}</td>
                </tr>
            """)
    
    write_table = f"""
        <h3>Write Performance</h3>
        <table>
            <thead>
                <tr>
                    <th>Dataset Size</th>
                    <th>Throughput (rows/sec)</th>
                    <th>Elapsed Time (sec)</th>
                </tr>
            </thead>
            <tbody>
                {''.join(write_rows) if write_rows else '<tr><td colspan="3">No data</td></tr>'}
            </tbody>
        </table>
    """ if write_rows else ""
    
    query_table = f"""
        <h3>Query Performance (GROUP BY + AVG)</h3>
        <table>
            <thead>
                <tr>
                    <th>Dataset Size</th>
                    <th>Throughput (rows/sec)</th>
                    <th>Elapsed Time (sec)</th>
                </tr>
            </thead>
            <tbody>
                {''.join(query_rows) if query_rows else '<tr><td colspan="3">No data</td></tr>'}
            </tbody>
        </table>
    """ if query_rows else ""
    
    return f"""
        <div class="section">
            <h2>Single-Node Performance</h2>
            {write_table}
            {query_table}
        </div>
    """


def generate_multi_node_section(data, history):
    """Generate HTML for multi-node benchmark results with trend badges."""
    if not data or 'results' not in data:
        return ""

    prev = _previous_throughputs(history)

    write_rows = []
    query_rows = []
    
    num_nodes = data.get('num_nodes', 'N/A')
    
    for result in data['results']:
        if not result.get('success'):
            continue
        
        metrics = result.get('metrics', {})
        
        if result['name'] == 'distributed_write_benchmark':
            rows = int(metrics.get('rows', 0))
            throughput = metrics.get('throughput', 0)
            elapsed = metrics.get('elapsed', 0)
            verified = metrics.get('verified_nodes', 0)
            direction, pct = compute_trend(throughput, prev.get(f"dist_write_{rows}"))
            badge = trend_badge(direction, pct)
            
            write_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)} {badge}</td>
                    <td class="number">{format_number(elapsed, 4)}</td>
                    <td class="number">{verified}</td>
                </tr>
            """)
        
        elif result['name'] == 'distributed_query_benchmark':
            rows = int(metrics.get('rows_queried', 0))
            throughput = metrics.get('throughput', 0)
            elapsed = metrics.get('avg_elapsed', 0)
            nodes_alive = metrics.get('nodes_alive', 0)
            total_bees = metrics.get('total_bees', 0)
            direction, pct = compute_trend(throughput, prev.get(f"dist_query_{rows}"))
            badge = trend_badge(direction, pct)
            
            query_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)} {badge}</td>
                    <td class="number">{format_number(elapsed, 4)}</td>
                    <td class="number">{nodes_alive}</td>
                    <td class="number">{total_bees}</td>
                </tr>
            """)
    
    write_table = f"""
        <h3>Distributed Write Performance</h3>
        <table>
            <thead>
                <tr>
                    <th>Dataset Size</th>
                    <th>Throughput (rows/sec)</th>
                    <th>Elapsed Time (sec)</th>
                    <th>Verified Nodes</th>
                </tr>
            </thead>
            <tbody>
                {''.join(write_rows) if write_rows else '<tr><td colspan="4">No data</td></tr>'}
            </tbody>
        </table>
    """ if write_rows else ""
    
    query_table = f"""
        <h3>Distributed Query Performance</h3>
        <table>
            <thead>
                <tr>
                    <th>Dataset Size</th>
                    <th>Throughput (rows/sec)</th>
                    <th>Avg Elapsed Time (sec)</th>
                    <th>Nodes Alive</th>
                    <th>Total Bees</th>
                </tr>
            </thead>
            <tbody>
                {''.join(query_rows) if query_rows else '<tr><td colspan="5">No data</td></tr>'}
            </tbody>
        </table>
    """ if query_rows else ""
    
    highlight = ""
    if write_rows or query_rows:
        highlight = f"""
            <div class="highlight">
                <h3>Multi-Node Configuration</h3>
                <p><strong>Nodes:</strong> {num_nodes} Apiary nodes sharing MinIO object storage</p>
                <p><strong>Test:</strong> Data written on one node is immediately visible and queryable from all other nodes</p>
            </div>
        """
    
    return f"""
        <div class="section">
            <h2>Multi-Node Performance</h2>
            {highlight}
            {write_table}
            {query_table}
        </div>
    """ if (write_rows or query_rows) else ""


def generate_trend_charts_section(history):
    """Generate the HTML + JS for historical trend charts.

    Returns a tuple (section_html, script_html).  If there are fewer
    than 2 history entries no charts are rendered.
    """
    if len(history) < 2:
        return ("", "")

    # Collect all throughput keys that appear in the history
    all_keys = set()
    for entry in history:
        all_keys.update(entry.get("throughputs", {}).keys())
    if not all_keys:
        return ("", "")

    # Build labels (run numbers or timestamps)
    labels = []
    for e in history:
        run = e.get("run_number", "")
        sha = e.get("commit_sha", "")[:7] if e.get("commit_sha") else ""
        labels.append(f"#{run}" if run and run != "N/A" else sha or e.get("timestamp", "?"))

    # Separate write vs query series
    write_series = sorted(k for k in all_keys if k.startswith("write_"))
    query_series = sorted(k for k in all_keys if k.startswith("query_"))
    dist_write_series = sorted(k for k in all_keys if k.startswith("dist_write_"))
    dist_query_series = sorted(k for k in all_keys if k.startswith("dist_query_"))

    palette = [
        "rgb(52,152,219)",   # blue
        "rgb(46,204,113)",   # green
        "rgb(231,76,60)",    # red
        "rgb(155,89,182)",   # purple
        "rgb(241,196,15)",   # yellow
        "rgb(230,126,34)",   # orange
    ]

    def _build_datasets(series_keys):
        datasets = []
        for idx, key in enumerate(series_keys):
            color = palette[idx % len(palette)]
            data_points = []
            for entry in history:
                val = entry.get("throughputs", {}).get(key)
                data_points.append(val if val is not None else "null")
            # Pretty label: "write_1000" → "1,000 rows"
            size_part = key.split("_", 1)[-1] if "_" in key else key
            # For dist_ keys strip the extra prefix
            if key.startswith("dist_write_") or key.startswith("dist_query_"):
                size_part = key.split("_", 2)[-1]
            try:
                pretty = f"{int(size_part):,} rows"
            except ValueError:
                pretty = size_part
            datasets.append({
                "label": pretty,
                "data": data_points,
                "borderColor": color,
                "backgroundColor": color.replace("rgb", "rgba").replace(")", ",0.1)"),
                "tension": 0.3,
                "fill": True,
            })
        return datasets

    charts_html = []
    charts_js = []
    chart_id = 0

    chart_groups = [
        ("Write Throughput Over Time", write_series),
        ("Query Throughput Over Time", query_series),
        ("Distributed Write Throughput Over Time", dist_write_series),
        ("Distributed Query Throughput Over Time", dist_query_series),
    ]

    for title, series in chart_groups:
        if not series:
            continue
        cid = f"trendChart{chart_id}"
        chart_id += 1
        charts_html.append(
            f'<h3>{title}</h3>\n'
            f'<div class="chart-container"><canvas id="{cid}"></canvas></div>'
        )
        datasets = _build_datasets(series)
        # Build JS dataset array – need to emit null properly
        ds_js_parts = []
        for ds in datasets:
            data_str = ", ".join(str(v) for v in ds["data"])
            ds_js_parts.append(
                f'{{ label: "{ds["label"]}", '
                f'data: [{data_str}], '
                f'borderColor: "{ds["borderColor"]}", '
                f'backgroundColor: "{ds["backgroundColor"]}", '
                f'tension: {ds["tension"]}, fill: {str(ds["fill"]).lower()} }}'
            )
        labels_js = json.dumps(labels)
        charts_js.append(f"""
new Chart(document.getElementById('{cid}'), {{
    type: 'line',
    data: {{
        labels: {labels_js},
        datasets: [{", ".join(ds_js_parts)}]
    }},
    options: {{
        responsive: true,
        plugins: {{
            title: {{ display: false }},
            tooltip: {{
                callbacks: {{
                    label: function(ctx) {{
                        return ctx.dataset.label + ': ' + (ctx.parsed.y != null ? ctx.parsed.y.toLocaleString() + ' rows/sec' : 'N/A');
                    }}
                }}
            }}
        }},
        scales: {{
            y: {{
                beginAtZero: true,
                title: {{ display: true, text: 'Throughput (rows/sec)' }},
                ticks: {{
                    callback: function(v) {{ return v.toLocaleString(); }}
                }}
            }},
            x: {{
                title: {{ display: true, text: 'Run' }}
            }}
        }}
    }}
}});
""")

    if not charts_html:
        return ("", "")

    section = (
        '<div class="section">\n'
        '    <h2>Performance Trends</h2>\n'
        '    ' + '\n    '.join(charts_html) + '\n'
        '</div>'
    )
    script = '<script>\n' + '\n'.join(charts_js) + '\n</script>'
    return (section, script)


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report from benchmark results")
    parser.add_argument("--single-node", type=str, help="Path to single-node benchmark JSON")
    parser.add_argument("--multi-node", type=str, help="Path to multi-node benchmark JSON")
    parser.add_argument("--history", type=str, help="Path to benchmark history JSON")
    parser.add_argument("--output", type=str, required=True, help="Output HTML file path")
    parser.add_argument("--run-number", type=str, default="N/A", help="CI run number")
    parser.add_argument("--commit-sha", type=str, default="N/A", help="Git commit SHA")
    parser.add_argument("--platform", type=str, default="ubuntu-latest", help="Platform name")
    
    args = parser.parse_args()
    
    # Load benchmark data
    single_node_data = None
    multi_node_data = None
    
    if args.single_node and Path(args.single_node).exists():
        with open(args.single_node, 'r') as f:
            single_node_data = json.load(f)
    
    if args.multi_node and Path(args.multi_node).exists():
        with open(args.multi_node, 'r') as f:
            multi_node_data = json.load(f)
    
    # Load and update history
    history = load_history(args.history)
    history = append_history(
        history, single_node_data, multi_node_data,
        args.run_number, args.commit_sha,
    )

    # Generate sections
    single_node_section = generate_single_node_section(single_node_data, history)
    multi_node_section = generate_multi_node_section(multi_node_data, history)
    trend_charts_section, chart_scripts = generate_trend_charts_section(history)
    
    # Format timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Generate HTML
    html = HTML_TEMPLATE.format(
        timestamp=timestamp,
        run_number=args.run_number,
        commit_sha=args.commit_sha[:7] if args.commit_sha != "N/A" else "N/A",
        platform=args.platform,
        single_node_section=single_node_section,
        multi_node_section=multi_node_section,
        trend_charts_section=trend_charts_section,
        chart_scripts=chart_scripts,
    )
    
    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write(html)
    
    # Write updated history next to the HTML output
    history_out = output_path.parent / "benchmark_history.json"
    with open(history_out, "w") as f:
        json.dump(history, f, indent=2)
    
    print(f"Report generated: {output_path}", file=sys.stderr)
    print(f"History updated: {history_out} ({len(history)} entries)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
