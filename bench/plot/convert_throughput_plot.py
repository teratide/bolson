import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import argparse
from scipy import stats

parser = argparse.ArgumentParser(description='Plot throughput.')
parser.add_argument('csv', type=str, nargs=1)
parser.add_argument('pdf', type=str, nargs=1)
parser.add_argument('title', type=str, nargs=1)
args = parser.parse_args()
csv = args.csv[0]
pdf = args.pdf[0]
title = args.title[0]

# Prepare data

df = pd.read_csv(csv)

x_jsons = df['Number of JSONs'].values
x_total_size = df['Raw JSONs size'].values
y_time = df['Convert time'].values
y_tp = (x_total_size / df['Convert time']).values

# Plot
plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '15'})

background = '#ffffff'
colors = ['#1a74b2', '#d9a53f']

fig, ax = plt.subplots()

print(x_total_size)
print(y_time)
print(y_tp)

ax.plot(x_jsons, y_time, marker='o', color=colors[0], label='Time (s)')
ax.set_ylabel('Time (s)', color=colors[0])
ax.set_xscale('log', base=2)
ax.set_yscale('log', base=10)
ax.set_xticks(x_jsons)
ax.set_xlabel("Number of JSONs")

ax.grid(True)

ax2 = ax.twinx()
ax2.plot(x_jsons, y_tp * 1E-6, marker='s', color=colors[1], label='Throughput (MB/s)')
ax2.set_xticks(x_jsons)
ax2.set_xscale('log', base=2)
ax2.set_ylabel('Throughput (MB/s)', color=colors[1])

ax_x2 = ax.secondary_xaxis('top')
ax_x2.set_xscale('log', base=2)
ax_x2.set_xticks(x_jsons, minor=False)
ax_x2.set_xticklabels(np.around(x_total_size / 2 ** 20).astype(np.int), rotation=45, ha="left")
ax_x2.set_xlabel("Raw JSON size (MiB)")

ax.set_title(title)

fig.set_facecolor('white')
fig.tight_layout()

fig.savefig(pdf)
