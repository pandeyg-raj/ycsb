import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
#import mplcursors
from pathlib import Path

base_path = Path(__file__).parent
file_path = (base_path / "./ecRead4GB.data").resolve()
df = pd.read_csv(file_path,comment='#',skipinitialspace=True)

#changing to ms from us
print("mean is ",  round(df.client_lat_us.mean()/1000,2))  # Same as df['field_A'].mean())
print("median is ",   round(df.client_lat_us.median()/1000,2) )
print("90th percentile is ",  round(df.client_lat_us.quantile(0.90)/1000,3) )  # 90th percentile
print("95th percentile is ",  round(df.client_lat_us.quantile(0.95)/1000,3) )  # 95th percentile
print("96th percentile is ",  round(df.client_lat_us.quantile(0.96)/1000,3) )  # 95th percentile
print("97th percentile is ",  round(df.client_lat_us.quantile(0.97)/1000,3) )  # 95th percentile
print("98th percentile is ",  round(df.client_lat_us.quantile(0.98)/1000,3) )  # 95th percentile
print("99th percentile is ",  round(df.client_lat_us.quantile(0.99)/1000,3) )  # 99th percentile
print("done")
