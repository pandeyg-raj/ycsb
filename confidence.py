import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

# rep
resultsrep = {
    "Read100": [1571, 1555,	1495, 1520,	1518 ],
    "Read95": [1539,	1501,	1544,	1556,	1594],
    "Read50": [1189,	1203,	1212,	1190,	1206]
}

#ec
resultsec = {
    "Read100": [943,	892,	899.00,	895,	898],
    "Read95": [869,	876,	898.00,	891,	887],
    "Read50": [829,	806,	836.00,	873,	850]
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
