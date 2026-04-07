#!/usr/bin/env python3
"""
plot_benchmark.py

Plots node resource metrics alongside client latency from a benchmark run.
Produces one PNG with subplots showing all nodes + client over time.

Usage:
    python3 plot_benchmark.py \
        --client  results/least_c46.csv \
        --nodes   results/metrics_node0.csv results/metrics_node1.csv \
                  results/metrics_node2.csv results/metrics_node3.csv \
                  results/metrics_node4.csv \
        --title   "LEAST vs Default — cluster46" \
        --output  plots/least_c46.png

Dependencies:
    pip install pandas matplotlib
"""

import argparse
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# ── Args ──────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser()
parser.add_argument('--client',  required=True,
                    help='Client CSV from trace_driver (latency timeseries)')
parser.add_argument('--nodes',   required=True, nargs='+',
                    help='Node metric CSVs (one per node)')
parser.add_argument('--title',   default='Benchmark Results',
                    help='Plot title')
parser.add_argument('--output',  default='benchmark.png',
                    help='Output PNG path')
parser.add_argument('--start',   default=None,
                    help='Crop start time HH:MM:SS (optional)')
parser.add_argument('--end',     default=None,
                    help='Crop end time HH:MM:SS (optional)')
args = parser.parse_args()

# ── Load data ─────────────────────────────────────────────────────────────────

def parse_time(ts_str, date_str=None):
    """Parse HH:MM:SS into a datetime using today's date."""
    today = date_str or datetime.now().strftime('%Y-%m-%d')
    return datetime.strptime(f'{today} {ts_str}', '%Y-%m-%d %H:%M:%S')

def load_csv(path):
    df = pd.read_csv(path)
    today = datetime.now().strftime('%Y-%m-%d')
    # handle both column names: client CSV uses 'time', node CSV uses 'timestamp'
    time_col = 'time' if 'time' in df.columns else 'timestamp'
    df['dt'] = df[time_col].apply(lambda t: parse_time(t, today))
    return df

print(f'Loading client CSV: {args.client}')
client = load_csv(args.client)

print(f'Loading {len(args.nodes)} node CSVs...')
nodes = []
for path in args.nodes:
    df = load_csv(path)
    node_name = df['node'].iloc[0]
    print(f'  {node_name}: {len(df)} rows  ({df["dt"].iloc[0].strftime("%H:%M:%S")} → {df["dt"].iloc[-1].strftime("%H:%M:%S")})')
    nodes.append((node_name, df))

# Sort nodes by name
nodes.sort(key=lambda x: x[0])

# Optional time crop
def crop(df, start, end):
    if start:
        s = parse_time(start)
        df = df[df['dt'] >= s]
    if end:
        e = parse_time(end)
        df = df[df['dt'] <= e]
    return df

client = crop(client, args.start, args.end)
nodes  = [(n, crop(df, args.start, args.end)) for n, df in nodes]

# ── Color palette ──────────────────────────────────────────────────────────────

NODE_COLORS = ['#2196F3', '#4CAF50', '#FF9800', '#E91E63', '#9C27B0',
               '#00BCD4', '#8BC34A', '#FF5722']

# ── Plot layout ───────────────────────────────────────────────────────────────
#
# Rows (top to bottom):
#   0 — Client: GET latency (mean, p99, p999)
#   1 — Client: throughput (ops/s)
#   2 — All nodes: CPU %
#   3 — All nodes: disk read MB/s
#   4 — All nodes: disk write MB/s
#   5 — All nodes: network rx MB/s
#   6 — All nodes: memory used GB (cgroup if available, else system)

N_ROWS = 7
fig, axes = plt.subplots(N_ROWS, 1,
                         figsize=(16, 4 * N_ROWS),
                         sharex=True)
fig.suptitle(args.title, fontsize=16, fontweight='bold', y=0.98)

# Time formatter
time_fmt = mdates.DateFormatter('%H:%M:%S')

def fmt_ax(ax, ylabel, title):
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_title(title, fontsize=10, loc='left', pad=3)
    ax.grid(True, alpha=0.3, linewidth=0.5)
    ax.legend(fontsize=8, loc='upper right', framealpha=0.8)
    ax.tick_params(axis='x', labelsize=8)
    ax.tick_params(axis='y', labelsize=8)

# ── Row 0: GET latency ────────────────────────────────────────────────────────

ax = axes[0]
if 'get_mean_us' in client.columns and client['get_count'].sum() > 0:
    # Only plot where GETs actually happened
    g = client[client['get_count'] > 0]
    ax.plot(g['dt'], g['get_mean_us'] / 1000, label='mean',  color='#2196F3', lw=1.5)
else:
    ax.text(0.5, 0.5, 'No GET data', transform=ax.transAxes, ha='center')

# Also try to plot p99 and p999 if the driver outputs them
# (the current driver CSV has mean only per interval; p-tiles are in final summary)
fmt_ax(ax, 'Latency (ms)', 'Client — GET latency')

# ── Row 1: Throughput ─────────────────────────────────────────────────────────

ax = axes[1]
ax.plot(client['dt'], client['ops_per_sec'],
        label='ops/s', color='#FF6F00', lw=1.5)
ax.fill_between(client['dt'], 0, client['ops_per_sec'],
                alpha=0.15, color='#FF6F00')
fmt_ax(ax, 'ops/s', 'Client — throughput')

# ── Row 2: CPU % ──────────────────────────────────────────────────────────────

ax = axes[2]
for i, (name, df) in enumerate(nodes):
    color = NODE_COLORS[i % len(NODE_COLORS)]
    ax.plot(df['dt'], df['cpu_pct'], label=name, color=color, lw=1.2, alpha=0.9)
ax.set_ylim(0, 100)
ax.axhline(100, color='red', lw=0.5, ls='--', alpha=0.5)
fmt_ax(ax, 'CPU %', 'Nodes — CPU utilization')

# ── Row 3: Disk reads ─────────────────────────────────────────────────────────

ax = axes[3]
for i, (name, df) in enumerate(nodes):
    color = NODE_COLORS[i % len(NODE_COLORS)]
    ax.plot(df['dt'], df['disk_read_mb_s'], label=name, color=color, lw=1.2, alpha=0.9)
fmt_ax(ax, 'MB/s', 'Nodes — disk reads')

# ── Row 4: Disk writes ────────────────────────────────────────────────────────

ax = axes[4]
for i, (name, df) in enumerate(nodes):
    color = NODE_COLORS[i % len(NODE_COLORS)]
    ax.plot(df['dt'], df['disk_write_mb_s'], label=name, color=color, lw=1.2, alpha=0.9)
fmt_ax(ax, 'MB/s', 'Nodes — disk writes')

# ── Row 5: Network rx ─────────────────────────────────────────────────────────

ax = axes[5]
for i, (name, df) in enumerate(nodes):
    color = NODE_COLORS[i % len(NODE_COLORS)]
    ax.plot(df['dt'], df['net_rx_mb_s'], label=name, color=color, lw=1.2, alpha=0.9)
fmt_ax(ax, 'MB/s', 'Nodes — network RX')

# ── Row 6: Memory ─────────────────────────────────────────────────────────────

ax = axes[6]
for i, (name, df) in enumerate(nodes):
    color = NODE_COLORS[i % len(NODE_COLORS)]
    # Use cgroup memory if available, fall back to system memory
    if 'cgroup_used_gb' in df.columns and df['cgroup_used_gb'].notna().any():
        mem_col = 'cgroup_used_gb'
        label   = f'{name} (cgroup)'
    else:
        mem_col = 'mem_used_gb'
        label   = name
    ax.plot(df['dt'], df[mem_col], label=label, color=color, lw=1.2, alpha=0.9)

# Draw cgroup limit line if available
for i, (name, df) in enumerate(nodes):
    if 'cgroup_limit_gb' in df.columns and df['cgroup_limit_gb'].notna().any():
        limit = df['cgroup_limit_gb'].dropna().iloc[0]
        ax.axhline(limit, color='red', lw=1.0, ls='--', alpha=0.6,
                   label=f'cgroup limit ({limit:.0f} GB)')
        break   # one line is enough, all nodes have same limit

fmt_ax(ax, 'GB', 'Nodes — memory used')

# ── X axis ────────────────────────────────────────────────────────────────────

axes[-1].xaxis.set_major_formatter(time_fmt)
axes[-1].set_xlabel('Wall clock time', fontsize=9)
plt.xticks(rotation=30, ha='right')

plt.tight_layout(rect=[0, 0, 1, 0.97])

# ── Save ──────────────────────────────────────────────────────────────────────

os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
plt.savefig(args.output, dpi=150, bbox_inches='tight')
print(f'\nSaved → {args.output}')
