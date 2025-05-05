import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from itertools import accumulate
#import mplcursors
from pathlib import Path
base_path = Path(__file__).parent

file_path = (base_path / "./ecRead4GB.data").resolve()
OnMB50 = pd.read_csv(file_path,comment='#',skipinitialspace=True)
OneMB50 = OnMB50.sort_values(by=['client_lat_us'])
OneMB50.insert(1, 'ID', range(1,  1+ len(OneMB50)))
OneMB50["ID"] = OneMB50["ID"] / len(OneMB50)
print(f"green  ec client mean {OneMB50.client_lat_us.mean()}")
plt.plot( OneMB50.client_lat_us,OneMB50.ID,c='green')#,linestyle=':')


file_path = (base_path / "./repRead4GB.data").resolve()
OnMB50 = pd.read_csv(file_path,comment='#',skipinitialspace=True)
OneMB50 = OnMB50.sort_values(by=['client_lat_us'])
OneMB50.insert(1, 'ID', range(1,  1+ len(OneMB50)))
OneMB50["ID"] = OneMB50["ID"] / len(OneMB50)
print(f"red  rep client mean {OneMB50.client_lat_us.mean()}")
plt.plot( OneMB50.client_lat_us,OneMB50.ID,c='red')#,linestyle=':')


plt.legend(["EC", "REP"], loc="lower right")
plt.xlabel("Latency (us)")
plt.title("CDF")
print("done")
#plt.show()
plt.savefig('cdf.pdf')

