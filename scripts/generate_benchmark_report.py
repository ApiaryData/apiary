#!/usr/bin/env python3
"""Generate an HTML report from benchmark results."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Apiary Performance Benchmarks</title>
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
        
        {multi_node_section}
        
        <div class="footer">
            <p>Generated by <a href="https://github.com/ApiaryData/apiary">Apiary</a> benchmark suite</p>
            <p>View <a href="https://github.com/ApiaryData/apiary/actions">workflow runs</a> or check the <a href="https://github.com/ApiaryData/apiary">repository</a></p>
        </div>
    </div>
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


def generate_single_node_section(data):
    """Generate HTML for single-node benchmark results."""
    if not data or 'results' not in data:
        return ""
    
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
            
            write_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)}</td>
                    <td class="number">{format_number(elapsed, 4)}</td>
                </tr>
            """)
        
        elif result['name'] == 'query_benchmark':
            rows = int(metrics.get('rows_scanned', 0))
            throughput = metrics.get('throughput', 0)
            elapsed = metrics.get('elapsed', 0)
            
            query_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)}</td>
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


def generate_multi_node_section(data):
    """Generate HTML for multi-node benchmark results."""
    if not data or 'results' not in data:
        return ""
    
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
            
            write_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)}</td>
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
            
            query_rows.append(f"""
                <tr>
                    <td class="number">{format_number(rows, 0)}</td>
                    <td class="number">{format_number(throughput, 2)}</td>
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


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report from benchmark results")
    parser.add_argument("--single-node", type=str, help="Path to single-node benchmark JSON")
    parser.add_argument("--multi-node", type=str, help="Path to multi-node benchmark JSON")
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
    
    # Generate sections
    single_node_section = generate_single_node_section(single_node_data)
    multi_node_section = generate_multi_node_section(multi_node_data)
    
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
    )
    
    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write(html)
    
    print(f"Report generated: {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
