import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
#import mplcursors
from pathlib import Path
import csv
import numpy as np
import plotly.graph_objects as go
base_path = Path(__file__).parent

file1 = (base_path / "./FinalExps/ycsb_results1KB/ec_iter_1Run1000Bytes_read100run.scr").resolve()
file2 = (base_path / "./FinalExps/ycsb_results1KB/ec_iter_2Run1000Bytes_read100run.scr").resolve()
file3 = (base_path / "./FinalExps/ycsb_results1KB/ec_iter_3Run1000Bytes_read100run.scr").resolve()
file4 = (base_path / "./FinalExps/ycsb_results1KB/ec_iter_4Run1000Bytes_read100run.scr").resolve()
file5 = (base_path / "./FinalExps/ycsb_results1KB/ec_iter_5Run1000Bytes_read100run.scr").resolve()

file6 = (base_path / "./FinalExps/ycsb_results1KB/rep_iter_1Run1000Bytes_read100run.scr").resolve()
file7 = (base_path / "./FinalExps/ycsb_results1KB/rep_iter_2Run1000Bytes_read100run.scr").resolve()
file8 = (base_path / "./FinalExps/ycsb_results1KB/rep_iter_3Run1000Bytes_read100run.scr").resolve()
file9 = (base_path / "./FinalExps/ycsb_results1KB/rep_iter_4Run1000Bytes_read100run.scr").resolve()
file10 = (base_path / "./FinalExps/ycsb_results1KB/rep_iter_5Run1000Bytes_read100run.scr").resolve()


# Define your groups and their files
group_files = {
    "EC": [file1, file2],# file3,file4,file5],
    "REP": [file6, file7]#, file8,file9,file10]
}

# Colors for groups — adjust or add more if needed
group_colors = {
    "EC": "blue",
    "REP": "red"
}

fig = go.Figure()

for group, files in group_files.items():
    color = group_colors.get(group, "black")  # fallback color
    for filename in files:
        latencies = []
        try:
            with open(filename, newline='') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if len(row) < 3:
                        continue
                    op = row[0].strip().upper()
                    # Optional: filter ops here if needed
                    if op not in ("READ", "INSERT"):
                        continue
                    try:
                        latency = float(row[2])
                        latencies.append(latency)
                    except ValueError:
                        continue
        except FileNotFoundError:
            print(f"Warning: File not found: {filename}")
            continue

        if not latencies:
            print(f"No valid data in {filename}")
            continue

        latencies = np.sort(latencies)
        cdf = np.arange(1, len(latencies) + 1) / len(latencies) * 100

        fig.add_trace(go.Scatter(
            x=latencies,
            y=cdf,
            mode='lines',
            name=f"{group} - {filename}",
            line=dict(color=color)
        ))

fig.update_layout(
    title="Latency CDFs by Group",
    xaxis_title="Latency (μs)",
    yaxis_title="Percentile (%)",
    yaxis=dict(range=[0, 100]),
    hovermode="x unified"
)

#fig.show()
fig.write_html("latency_cdf_plot.html")
print("done")
