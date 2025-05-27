import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

# rep
resultsrep = {
    "Read100": [21535, 1717, 1722, 1717, 1693 ],
    "Read95": [1716, 1696, 1714, 1674, 1707],
    "Read50": [1657, 1708, 1648, 1600, 1701]
}

#ec
resultsec = {
    "Read100": [1562, 1533, 1537, 1533, 1537],
    "Read95": [2066, 1579, 1567, 1563, 1568],
    "Read50": [1457,1493 ,1528,1555,1618 ]
}
results = resultsec

labels, means, margins = [], [], []

for label, values in results.items():
    mean = np.mean(values)
    std_err = stats.sem(values)
    std_dev = np.std(values, ddof=1)
    print(std_dev)
    t_crit = stats.t.ppf((1 + 0.95) / 2, df=len(values)-1)
    margin = t_crit * std_err
    labels.append(label)
    means.append(mean)
    margins.append(margin)

plt.bar(labels, means, yerr=margins, capsize=10, color='skyblue')
plt.ylabel("Latency (us)")
plt.title("YCSB: 95% CI for Average Latency")
plt.grid(True, axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig("aec.png")
