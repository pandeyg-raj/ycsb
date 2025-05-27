import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
from pathlib import Path
base_path = Path(__file__).parent
file_path = (base_path / "ycsb_results2/ec_iter_1Run10000Bytes_read95run.scr").resolve()
x =0;
def parse_raw_ycsb(file_path):
    read_latencies = []
    insert_latencies = []

    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) != 3:
                continue

            op = parts[0].strip()
            latency_str = parts[2].strip()

            # Only process exact operation names
            if op not in ("READ", "INSERT"):
                continue

            try:
                latency = int(latency_str)
            except ValueError:
                continue

            if op == "READ":
                read_latencies.append(latency)
            elif op == "INSERT":
                insert_latencies.append(latency)

    return read_latencies, insert_latencies

def compute_percentiles(data, label=""):
    if not data:
        print(f"--- {label} ---")
        print("No data available.\n")
        return

    percentiles = [50, 95, 99, 99.9]
    print(f"--- {label} ---")
    print(f"Average latency: {np.mean(data):.2f} us")
    for p in percentiles:
        print(f"{p}th percentile: {np.percentile(data, p):.2f} us")
    print()
  
# Example usage
read_lats, insert_lats = parse_raw_ycsb(file_path)
combined_lats = read_lats + insert_lats

compute_percentiles(read_lats, "READ")
compute_percentiles(insert_lats, "INSERT")
compute_percentiles(combined_lats, "READ+INSERT")
