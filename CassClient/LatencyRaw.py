import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
#import mplcursors
from pathlib import Path

base_path = Path(__file__).parent
print(base_path)
file_path = (base_path / "Cache__Hit_CacheReadPercent80readPercent90ObSize0.500000mb_TotalOp10000.csv").resolve()
df1 = pd.read_csv(file_path,comment='#',skipinitialspace=True)
print("mean is memtable",  df1.server_lat_us.mean())  # Same as df['field_A'].mean())
plt.scatter (df1["index"],df1.server_lat_us,color='green')


file_path = (base_path / "Cache__Miss_CacheReadPercent80readPercent90ObSize0.500000mb_TotalOp10000.csv").resolve()
df1 = pd.read_csv(file_path,comment='#',skipinitialspace=True)
print("mean is sstaabele",  df1.server_lat_us.mean())  # Same as df['field_A'].mean())
plt.scatter (df1["index"],df1.server_lat_us,color='red')


plt.legend(["request served from memtable only", "request served from sstable"], loc="upper right")
plt.xlabel("Latency (us)")
plt.ylabel("Probability")
plt.title("PMF request served from memtable and sstabel for 1MB  object")
print("done")
plt.show()
