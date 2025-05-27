import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import expon, kstest, probplot

# Load timestamps
timestamps = []
with open("tmp.log") as f:
    for line in f:
        if line.startswith("RequestStart,"):
            t = int(line.strip().split(",")[1])
            timestamps.append(t)

# Convert to seconds and compute inter-arrival times
timestamps = np.array(timestamps)
timestamps = np.sort(timestamps) / 1e9  # Convert from nanoseconds to seconds
inter_arrivals = np.diff(timestamps)

# Estimate lambda
mean_gap = np.mean(inter_arrivals)
lambda_hat = 1 / mean_gap
print(f"Mean inter-arrival time: {mean_gap:.6f} s")
print(f"Estimated λ: {lambda_hat:.2f} requests/sec")

# Plot histogram with exponential fit
plt.figure(figsize=(8, 5))
plt.hist(inter_arrivals, bins=50, density=True, alpha=0.6, label='Observed')
x = np.linspace(0, max(inter_arrivals), 100)
plt.plot(x, expon.pdf(x, scale=1/lambda_hat), 'r--', label='Exponential fit')
plt.title("Inter-arrival Times with Exponential Fit")
plt.xlabel("Inter-arrival Time (s)")
plt.ylabel("Probability Density")
plt.legend()
plt.grid()
plt.savefig("poissonhisto")

# K-S Test
ks_stat, p_value = kstest(inter_arrivals, 'expon', args=(0, 1/lambda_hat))
print(f"K-S statistic: {ks_stat:.4f}, p-value: {p_value:.4f}")
if p_value > 0.05:
    print("✅ Distribution is consistent with exponential (Poisson arrivals)")
else:
    print("❌ Distribution is NOT consistent with exponential")

# Q-Q Plot
plt.figure(figsize=(6, 6))
probplot(inter_arrivals, dist="expon", sparams=(0, 1/lambda_hat), plot=plt)
plt.title("Q-Q Plot for Exponential Distribution")
plt.grid()
plt.savefig("poissonQQ")
