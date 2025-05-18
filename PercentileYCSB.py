import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
#import mplcursors
from pathlib import Path

base_path = Path(__file__).parent
file_path = (base_path / "./Z_EcRead10KB50P_ThPool.data").resolve()
df = pd.read_csv(file_path,comment='#',skipinitialspace=True,dtype={"op": "string","timestamp_ms":"string","client_lat_us": int})

#changing to ms from us
print("mean is overall",  round(df.client_lat_us.mean()))  # Same as df['field_A'].mean())
print("mean is ",  round(df.groupby('op').client_lat_us.mean()))  # Same as df['field_A'].mean())
print("")

print("90th overall percentile is ",  round(df.client_lat_us.quantile(0.90)) )  # 90th percentile
print("90th percentile is ",  round(df.groupby('op').client_lat_us.quantile(0.90)) )  # 90th percentile
print("")

print("95th overall percentile is ",  round(df.client_lat_us.quantile(0.95)) )  # 95th percentile
print("95th percentile is ",  round(df.groupby('op').client_lat_us.quantile(0.95)) )  # 95th percentile
print("")

print("99th overall percentile is ",  round(df.client_lat_us.quantile(0.99)) )  # 99th percentile
print("99th percentile is ",  round(df.groupby('op').client_lat_us.quantile(0.99)) )  # 99th percentile
