#!/usr/bin/env python3
"""
filter_trace.py

Filters a Twitter Twemcache trace file for use with the LEAST trace driver.

Two filters applied:
  1. Remove GET operations for keys that have no prior SET in the trace
     (these keys existed in the live cache before recording started —
     they would return empty results and not exercise the read path).

  2. Remove SET operations whose value_size < min_value_size (default 100B),
     and remove all subsequent GET operations for those keys.
     Small values are not interesting for LEAST's erasure coding benefit.

Output trace is a drop-in replacement for the original — same format,
same column order, same timestamp ordering.

Usage:
    python3 filter_trace.py <input> <output> <bench_duration> <load_buffer> [min_value_size]

Arguments:
    input          : original trace file (e.g. cluster46.sort)
    output         : filtered output file (e.g. cluster46.filtered)
    bench_duration : benchmark window in seconds (e.g. 3600)
    load_buffer    : extra seconds beyond bench window for load (e.g. 600)
    min_value_size : minimum value size in bytes to keep (default: 100)

Example:
    python3 filter_trace.py cluster46.sort cluster46.filtered 3600 600 100

The output file covers timestamps 0..(bench_duration + load_buffer).
Run load phase against the full output file (--full-trace).
Run benchmark phase with --duration bench_duration.
"""

import sys

if len(sys.argv) < 5:
    print(__doc__)
    sys.exit(1)

input_file    = sys.argv[1]
output_file   = sys.argv[2]
bench_s       = int(sys.argv[3])
buffer_s      = int(sys.argv[4])
min_vsz       = int(sys.argv[5]) if len(sys.argv) > 5 else 100
load_s        = bench_s + buffer_s

SET_OPS = {'set', 'add', 'replace', 'cas'}
GET_OPS = {'get', 'gets'}

print(f"Input         : {input_file}")
print(f"Output        : {output_file}")
print(f"Bench window  : 0..{bench_s}s")
print(f"Load window   : 0..{load_s}s  (bench + {buffer_s}s buffer)")
print(f"Min value size: {min_vsz} bytes")
print()

# Keys with a valid SET (value_size >= min_vsz) seen so far
valid_keys = set()

# Counters for reporting
total_lines      = 0
kept             = 0
dropped_no_set   = 0   # GET with no prior SET
dropped_small    = 0   # SET/GET with value_size < min_vsz
dropped_window   = 0   # lines beyond load window (not written, just counted)

with open(input_file) as fin, open(output_file, 'w') as fout:
    for line in fin:
        parts = line.split(',')
        if len(parts) < 6:
            continue

        ts  = int(parts[0])
        key = parts[1].strip()
        vsz = int(parts[3]) if parts[3].strip().lstrip('-').isdigit() else 0
        op  = parts[5].strip().lower()

        total_lines += 1

        # past the load window — stop reading
        if ts > load_s:
            break

        if op in SET_OPS:
            if vsz < min_vsz:
                # small value — discard this key entirely
                # do NOT add to valid_keys so future GETs are also dropped
                dropped_small += 1
                continue

            # valid SET — record key and keep line
            valid_keys.add(key)
            fout.write(line)
            kept += 1

        elif op in GET_OPS:
            if key not in valid_keys:
                # either never SET, or SET was too small
                dropped_no_set += 1
                continue

            # GET for a key we know exists with sufficient value size
            fout.write(line)
            kept += 1

        # other ops (delete, incr, decr) — skip silently

print(f"Lines read    : {total_lines:>12,}")
print(f"Lines kept    : {kept:>12,}")
print()
print(f"Dropped — GET with no prior SET    : {dropped_no_set:>10,}")
print(f"Dropped — SET/GET value < {min_vsz}B     : {dropped_small:>10,}")
print()
print(f"Valid keys loaded into memory      : {len(valid_keys):>10,}")
print(f"Output file   : {output_file}")
