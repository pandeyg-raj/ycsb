import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
#import mplcursors
from pathlib import Path
base_path = Path(__file__).parent

file_path = (base_path / "cache1MB.csv").resolve()
OnMB50 = pd.read_csv(file_path,comment='#',skipinitialspace=True)
OneMB50 = OnMB50.sort_values(by=['server_lat_us'])
OneMB50.insert(1, 'ID', range(1,  1+ len(OneMB50)))
OneMB50["ID"] = OneMB50["ID"] / len(OneMB50)
print(f"1MB90 red  server mean {OneMB50.server_lat_us.mean()}")
plt.plot( OneMB50.server_lat_us,OneMB50.ID,c='red')#,linestyle=':')


file_path = (base_path / "cacheHalfMB.csv").resolve()
OnMB50 = pd.read_csv(file_path,comment='#',skipinitialspace=True)
OneMB50 = OnMB50.sort_values(by=['server_lat_us'])
OneMB50.insert(1, 'ID', range(1,  1+ len(OneMB50)))
OneMB50["ID"] = OneMB50["ID"] / len(OneMB50)
print(f"HalfMB90 green  server mean {OneMB50.server_lat_us.mean()}")
plt.plot( OneMB50.server_lat_us,OneMB50.ID,c='green')#,linestyle=':')

plt.legend(["1MB Object", "Half MB Object"], loc="lower right")
plt.xlabel("Latency (us)")
plt.title("CDF")
print("done")
plt.show()

