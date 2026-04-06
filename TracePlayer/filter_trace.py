#!/usr/bin/env python3
# filter_trace.py
#
# Removes GET operations for keys that have no prior SET in the trace.
# Output trace is a drop-in replacement for the original.
#
# Usage:
#   python3 filter_trace.py cluster46.sort cluster46.filtered 3600 600
#
# Arguments:
#   input_trace   : original trace file
#   output_trace  : filtered output
#   bench_duration: benchmark window in seconds (e.g. 3600)
#   load_buffer   : extra seconds for load window (e.g. 600)

import sys

input_file  = sys.argv[1]
output_file = sys.argv[2]
bench_s     = int(sys.argv[3])
buffer_s    = int(sys.argv[4])
load_s      = bench_s + buffer_s

written_keys = set()   # keys that have been SET so far
kept   = 0
dropped= 0
total  = 0

print(f"Filtering: bench window={bench_s}s  load window={load_s}s")
print(f"Input : {input_file}")
print(f"Output: {output_file}")

with open(input_file) as fin, open(output_file, 'w') as fout:
    for line in fin:
        parts = line.split(',')
        if len(parts) < 6:
            continue

        ts  = int(parts[0])
        key = parts[1].strip()
        op  = parts[5].strip().lower()

        total += 1

        # past the load window — nothing more to include
        if ts > load_s:
            break

        if op in ('set', 'add', 'replace', 'cas'):
            written_keys.add(key)
            # include SET if within load window
            fout.write(line)
            kept += 1

        elif op in ('get', 'gets'):
            # only include GET if this key was SET earlier in the trace
            if key in written_keys:
                fout.write(line)
                kept += 1
            else:
                dropped += 1
        # other ops (delete, incr etc.) — skip

print(f"\nDone.")
print(f"  Total lines read : {total:,}")
print(f"  Lines kept       : {kept:,}")
print(f"  GETs dropped     : {dropped:,}  ({dropped/(dropped+kept)*100:.1f}% of output)")
print(f"  Output file      : {output_file}")
