#!/usr/bin/env python3
"""
plot_cpu_compare.py

Plots average CPU utilization across all nodes for two experiments on
the same graph, so you can directly compare LEAST vs default Cassandra.

Usage:
    python3 plot_cpu_compare.py \
        --exp1-nodes results/c46_least_node*.csv   --exp1-label "LEAST" \
        --exp2-nodes results/c46_default_node*.csv --exp2-label "Default" \
        --output     results/cpu_compare.png

You can add more metrics to compare by passing --metric (default: cpu_pct).
Valid metrics: cpu_pct, disk_read_mb_s, disk_write_mb_s,
               net_rx_mb_s, net_tx_mb_s, mem_used_gb, cgroup_used_gb
"""

import argparse
import glob
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# ── Args ──────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser()
parser.add_argument('--exp1-nodes', nargs='+', required=True,
                    help='Node CSVs for experiment 1 (e.g. results/c46_least_node*.csv)')
parser.add_argument('--exp1-label', default='Experiment 1',
                    help='Label for experiment 1')
parser.add_argument('--exp2-nodes', nargs='+', required=True,
                    help='Node CSVs for experiment 2')
parser.add_argument('--exp2-label', default='Experiment 2',
                    help='Label for experiment 2')
parser.add_argument('--metrics', nargs='+',
                    default=['cpu_pct', 'disk_read_mb_s', 'disk_write_mb_s'],
                    help='Metrics to plot (default: cpu_pct disk_read_mb_s disk_write_mb_s)')
parser.add_argument('--output', default='compare.png',
                    help='Output PNG path')
parser.add_argument('--title', default='LEAST vs Default Cassandra',
                    help='Plot title')
args = parser.parse_args()

# ── Helpers ───────────────────────────────────────────────────────────────────

METRIC_LABELS = {
    'cpu_pct':          'CPU %',
    'disk_read_mb_s':   'Disk Read MB/s',
    'disk_write_mb_s':  'Disk Write MB/s',
    'net_rx_mb_s':      'Network RX MB/s',
    'net_tx_mb_s':      'Network TX MB/s',
    'mem_used_gb':      'Memory Used GB',
    'cgroup_used_gb':   'Cgroup Memory GB',
}

def parse_time(ts):
    today = datetime.now().strftime('%Y-%m-%d')
    return datetime.strptime(f'{today} {ts}', '%Y-%m-%d %H:%M:%S')

def load_nodes(paths):
    """Load all node CSVs and return a single DataFrame with avg per timestamp."""
    dfs = []
    for path in sorted(paths):
        df = pd.read_csv(path)
        time_col = 'time' if 'time' in df.columns else 'timestamp'
        df['dt'] = df[time_col].apply(parse_time)
        dfs.append(df)

    if not dfs:
        raise ValueError(f"No CSVs found")

    # Concatenate all nodes, group by timestamp, take mean across nodes
    combined = pd.concat(dfs)
    numeric_cols = combined.select_dtypes(include='number').columns.tolist()
    avg = combined.groupby('dt')[numeric_cols].mean().reset_index()

    print(f"  Loaded {len(dfs)} nodes, {len(avg)} timestamps")
    return avg

# ── Load data ─────────────────────────────────────────────────────────────────

print(f"Loading {args.exp1_label}...")
exp1 = load_nodes(args.exp1_nodes)

print(f"Loading {args.exp2_label}...")
exp2 = load_nodes(args.exp2_nodes)

# ── Normalize time to elapsed seconds from each experiment's start ────────────
# This aligns the two experiments even if they started at different wall times.

exp1_start = exp1['dt'].iloc[0]
exp2_start = exp2['dt'].iloc[0]
exp1['elapsed'] = (exp1['dt'] - exp1_start).dt.total_seconds()
exp2['elapsed'] = (exp2['dt'] - exp2_start).dt.total_seconds()

# ── Plot ──────────────────────────────────────────────────────────────────────

n_metrics = len(args.metrics)
fig, axes = plt.subplots(n_metrics, 1,
                         figsize=(14, 4 * n_metrics),
                         sharex=True)

if n_metrics == 1:
    axes = [axes]

fig.suptitle(args.title, fontsize=14, fontweight='bold')

EXP1_COLOR = '#2196F3'   # blue  — LEAST
EXP2_COLOR = '#FF5722'   # red   — default

for ax, metric in zip(axes, args.metrics):
    label = METRIC_LABELS.get(metric, metric)

    if metric not in exp1.columns or metric not in exp2.columns:
        ax.text(0.5, 0.5, f'No data for {metric}',
                transform=ax.transAxes, ha='center', fontsize=11)
        ax.set_ylabel(label)
        continue

    ax.plot(exp1['elapsed'], exp1[metric],
            label=args.exp1_label, color=EXP1_COLOR, lw=1.5)
    ax.plot(exp2['elapsed'], exp2[metric],
            label=args.exp2_label, color=EXP2_COLOR, lw=1.5)

    # Interpolate exp2 onto exp1's time axis so fill_between gets equal-length arrays
    exp2_interp = np.interp(exp1['elapsed'], exp2['elapsed'], exp2[metric])
    ax.fill_between(exp1['elapsed'], exp1[metric], exp2_interp,
                    alpha=0.08, color='gray')

    if metric == 'cpu_pct':
        ax.set_ylim(0, 100)

    ax.set_ylabel(label, fontsize=10)
    ax.set_title(f'Avg across all nodes — {label}', fontsize=10, loc='left')
    ax.legend(fontsize=9, loc='upper right')
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='both', labelsize=9)

axes[-1].set_xlabel('Elapsed time (seconds)', fontsize=10)

plt.tight_layout()
plt.savefig(args.output, dpi=150, bbox_inches='tight')
print(f'\nSaved → {args.output}')
