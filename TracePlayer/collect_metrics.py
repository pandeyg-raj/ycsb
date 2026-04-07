#!/usr/bin/env python3
"""
collect_metrics.py

Collects per-second system metrics on a Cassandra node during a benchmark.
Run this on EACH node before starting the benchmark. It will write a CSV
file with one row per second until you stop it (Ctrl+C or kill).

Usage:
    python3 collect_metrics.py --node node0 --output metrics_node0.csv

Arguments:
    --node   NODE    node name/label for identification (e.g. node0, node1)
    --output PATH    output CSV file path

The script reads from /proc which is available on all Linux systems.
No external dependencies needed.

Typical workflow:
    # On each node (run in background before benchmark):
    python3 collect_metrics.py --node node0 --output /mydata/metrics_node0.csv &
    python3 collect_metrics.py --node node1 --output /mydata/metrics_node1.csv &
    ...

    # Run benchmark on client
    ./trace_driver --trace cluster46.filtered ...

    # After benchmark, kill collectors on all nodes
    pkill -f collect_metrics.py

    # scp all CSVs to client for plotting
    scp node1:/mydata/metrics_node1.csv ./results/
    ...
"""

import sys
import time
import os
import argparse
import signal

# ── Argument parsing ──────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description='Collect node metrics')
parser.add_argument('--node',   required=True, help='Node label (e.g. node0)')
parser.add_argument('--output', required=True, help='Output CSV path')
parser.add_argument('--interval', type=float, default=1.0,
                    help='Sample interval in seconds (default: 1.0)')
args = parser.parse_args()

# ── Signal handler ────────────────────────────────────────────────────────────

running = True
def stop(sig, frame):
    global running
    running = False
signal.signal(signal.SIGINT,  stop)
signal.signal(signal.SIGTERM, stop)

# ── /proc readers ─────────────────────────────────────────────────────────────

def read_cpu():
    """Read aggregate CPU stats from /proc/stat.
    Returns (user, nice, system, idle, iowait, irq, softirq, steal) ticks."""
    with open('/proc/stat') as f:
        line = f.readline()   # first line: 'cpu  ...'
    fields = line.split()
    # fields[0] = 'cpu', fields[1..] = tick counts
    vals = [int(x) for x in fields[1:8]]
    # user, nice, system, idle, iowait, irq, softirq
    return vals

def cpu_percent(prev, curr):
    """Compute CPU usage % between two /proc/stat snapshots."""
    prev_total = sum(prev)
    curr_total = sum(curr)
    prev_idle  = prev[3] + prev[4]   # idle + iowait
    curr_idle  = curr[3] + curr[4]
    total_diff = curr_total - prev_total
    idle_diff  = curr_idle  - prev_idle
    if total_diff == 0:
        return 0.0
    return (1.0 - idle_diff / total_diff) * 100.0

def read_disk():
    """Read disk stats from /proc/diskstats.
    Returns (read_sectors, write_sectors) summed across all non-loop devices."""
    read_s = write_s = 0
    with open('/proc/diskstats') as f:
        for line in f:
            fields = line.split()
            if len(fields) < 14:
                continue
            dev = fields[2]
            # skip loop devices, partitions (sdX only), dm devices
            # include: sda, sdb, nvme0n1, vda etc.
            if (dev.startswith('loop') or
                (dev.startswith('sd') and dev[-1].isdigit()) or
                (dev.startswith('nvme') and 'p' in dev)):
                continue
            read_s  += int(fields[5])    # sectors read
            write_s += int(fields[9])    # sectors written
    return read_s, write_s

def read_net():
    """Read network stats from /proc/net/dev.
    Returns (rx_bytes, tx_bytes) summed across non-loopback interfaces."""
    rx = tx = 0
    with open('/proc/net/dev') as f:
        lines = f.readlines()[2:]   # skip 2 header lines
    for line in lines:
        parts = line.split()
        if not parts:
            continue
        iface = parts[0].rstrip(':')
        if iface == 'lo':
            continue
        rx += int(parts[1])    # rx bytes
        tx += int(parts[9])    # tx bytes
    return rx, tx

def read_mem():
    """Read memory from /proc/meminfo.
    Returns (total_kb, available_kb)."""
    total = avail = 0
    with open('/proc/meminfo') as f:
        for line in f:
            if line.startswith('MemTotal:'):
                total = int(line.split()[1])
            elif line.startswith('MemAvailable:'):
                avail = int(line.split()[1])
    return total, avail

def read_cgroup_mem():
    """Try to read cgroup memory limit and usage.
    Returns (limit_bytes, usage_bytes) or (None, None) if unavailable."""
    # Try cgroup v2 first
    try:
        with open('/sys/fs/cgroup/memory.max') as f:
            limit = f.read().strip()
            limit = int(limit) if limit != 'max' else None
        with open('/sys/fs/cgroup/memory.current') as f:
            usage = int(f.read().strip())
        return limit, usage
    except FileNotFoundError:
        pass
    # Try cgroup v1
    try:
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes') as f:
            limit = int(f.read().strip())
            if limit > 10**15:   # sentinel "unlimited" value
                limit = None
        with open('/sys/fs/cgroup/memory/memory.usage_in_bytes') as f:
            usage = int(f.read().strip())
        return limit, usage
    except FileNotFoundError:
        pass
    return None, None

# ── Main collection loop ──────────────────────────────────────────────────────

SECTOR_SIZE = 512   # bytes per disk sector

print(f"Collecting metrics on {args.node} → {args.output}")
print(f"Interval: {args.interval}s   Press Ctrl+C to stop\n")

# Open output file
out = open(args.output, 'w')
out.write('timestamp,node,'
          'cpu_pct,'
          'disk_read_mb_s,disk_write_mb_s,'
          'net_rx_mb_s,net_tx_mb_s,'
          'mem_used_gb,mem_avail_gb,'
          'cgroup_used_gb,cgroup_limit_gb\n')
out.flush()

# Initial readings (for delta calculation)
prev_cpu  = read_cpu()
prev_disk = read_disk()
prev_net  = read_net()
prev_time = time.time()
time.sleep(args.interval)

rows = 0
while running:
    t_now  = time.time()
    dt     = t_now - prev_time

    # Timestamp in same format as trace driver CSV
    ts = time.strftime('%H:%M:%S', time.localtime(t_now))

    # CPU
    curr_cpu = read_cpu()
    cpu      = cpu_percent(prev_cpu, curr_cpu)

    # Disk (convert sector delta to MB/s)
    curr_disk = read_disk()
    dr_mbs = (curr_disk[0] - prev_disk[0]) * SECTOR_SIZE / dt / 1e6
    dw_mbs = (curr_disk[1] - prev_disk[1]) * SECTOR_SIZE / dt / 1e6

    # Network (convert byte delta to MB/s)
    curr_net = read_net()
    rx_mbs = (curr_net[0] - prev_net[0]) / dt / 1e6
    tx_mbs = (curr_net[1] - prev_net[1]) / dt / 1e6

    # Memory
    mem_total, mem_avail = read_mem()
    mem_used_gb  = (mem_total - mem_avail) / 1e6
    mem_avail_gb = mem_avail / 1e6

    # Cgroup memory
    cg_limit, cg_usage = read_cgroup_mem()
    cg_used_gb  = cg_usage  / 1e9 if cg_usage  is not None else ''
    cg_limit_gb = cg_limit  / 1e9 if cg_limit  is not None else ''

    out.write(f'{ts},{args.node},'
              f'{cpu:.1f},'
              f'{dr_mbs:.2f},{dw_mbs:.2f},'
              f'{rx_mbs:.2f},{tx_mbs:.2f},'
              f'{mem_used_gb:.2f},{mem_avail_gb:.2f},'
              f'{cg_used_gb},{cg_limit_gb}\n')
    out.flush()

    rows += 1
    if rows % 60 == 0:
        print(f'[{ts}] {args.node}: CPU={cpu:.1f}%  '
              f'disk r={dr_mbs:.1f} w={dw_mbs:.1f} MB/s  '
              f'net rx={rx_mbs:.1f} tx={tx_mbs:.1f} MB/s  '
              f'mem_used={mem_used_gb:.1f}GB')

    # Update previous readings
    prev_cpu  = curr_cpu
    prev_disk = curr_disk
    prev_net  = curr_net
    prev_time = t_now

    # Sleep until next sample
    elapsed = time.time() - t_now
    sleep_for = args.interval - elapsed
    if sleep_for > 0:
        time.sleep(sleep_for)

out.close()
print(f'\nDone. Wrote {rows} rows to {args.output}')
