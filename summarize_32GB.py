#!/usr/bin/env python3
"""Summarize a run_ycsb_32GB_compare.sh result folder.

Usage: python3 summarize_32GB.py <result_dir> <label>
  label = the system name used in the .scr file names (elect, elect_noec, ec, rep)

Raw .scr lines look like:  OP,timestamp_ms,latency_us[,key]
Writes <result_dir>/summary.txt and prints it.
"""
import os
import sys


def pct(sorted_vals, p):
    if not sorted_vals:
        return float('nan')
    k = min(len(sorted_vals) - 1, int(round(p / 100.0 * (len(sorted_vals) - 1))))
    return sorted_vals[k]


def parse_counters(path):
    """Return {node: sectors_read_bytes} from a counters_*.txt file."""
    d, node = {}, None
    if not os.path.exists(path):
        return d
    for line in open(path):
        p = line.split()
        if line.startswith('###'):
            node = p[1]
            continue
        if node and len(p) > 6 and p[2].startswith(('dm', 'nvme', 'sd')):
            try:
                d[node] = int(p[5]) * 512
            except ValueError:
                pass
    return d


def diskread_mb(before, after):
    b, a = parse_counters(before), parse_counters(after)
    return sum(a[n] - b.get(n, 0) for n in a) / 2**20 if a else float('nan')


def main():
    outdir, label = sys.argv[1], sys.argv[2]
    order = ["Warmup", "workloadC_warmup", "workloadB", "workloadD",
             "workloadA", "workloadC", "workloadE", "workloadF"]
    lines = [f"=== {label} | latency in ms (mean / p50 / p99) | disk read during workload ===",
             "    note: in A and B, YCSB labels updates as INSERT"]
    for w in order:
        path = os.path.join(outdir, f"{label}_{w}.scr")
        if not os.path.exists(path):
            continue
        lat = {}
        for line in open(path):
            p = line.strip().split(',')
            if len(p) < 3 or not p[2].strip().isdigit():
                continue
            lat.setdefault(p[0].strip(), []).append(int(p[2]) / 1000.0)
        dr = diskread_mb(os.path.join(outdir, f"counters_{w}_before.txt"),
                         os.path.join(outdir, f"counters_{w}_after.txt"))
        parts = []
        for op in sorted(lat):
            if op == "CLEANUP":
                continue
            v = sorted(lat[op])
            parts.append(f"{op} n={len(v)} {sum(v)/len(v):.2f}/{pct(v, 50):.2f}/{pct(v, 99):.2f}")
        drs = f"  disk_read={dr:.0f}MB" if dr == dr else ""
        lines.append(f"{w:18s} " + "  ".join(parts) + drs)
    out = "\n".join(lines)
    with open(os.path.join(outdir, "summary.txt"), "w") as f:
        f.write(out + "\n")
    print(out)


if __name__ == "__main__":
    main()
