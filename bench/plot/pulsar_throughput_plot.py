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

x_messages = df['Messages'].values
x_total_size = (df['Messages'] * df['Message size']).values
y_time = df['Time'].values
y_tp = (x_total_size / df['Time']).values

# Plot
plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '15'})

background = '#ffffff'
colors = ['#1a74b2', '#d9a53f']

print(x_total_size)
print(y_time)
print(y_tp)

with plt.rc_context({'axes.edgecolor': colors[0], 'ytick.color': colors[0]}):
    fig, ax = plt.subplots()
    ax.plot(x_messages, y_time, marker='o', color=colors[0], label='Time (s)')
    ax.set_ylabel('Time (s)', color=colors[0])
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=10)
    ax.set_xticks(x_messages)
    ax.set_xlabel("Number of messages")
    ax.grid(True)

with plt.rc_context({'ytick.color': colors[1]}):
    ax2 = ax.twinx()
    y_tp = y_tp * 1E-6
    ax2.plot(x_messages, y_tp, marker='s', color=colors[1], label='Throughput (MB/s)')
    ax2.set_xticks(x_messages)
    ax2.set_xscale('log', base=2)
    ax2.set_ylabel('Throughput (MB/s)', color=colors[1])
    ax2.set_ylim(0, 1.1*max(y_tp))

ax_x2 = ax.secondary_xaxis('top')
ax_x2.set_xscale('log', base=2)
ax_x2.set_xticks(x_messages, minor=False)
ax_x2.set_xticklabels(np.around(x_total_size / 2 ** 20).astype(np.int), rotation=45, ha="left")
ax_x2.set_xlabel("Total size (MiB)")

ax.set_title(title)

fig.set_facecolor('white')
fig.tight_layout()

fig.savefig(pdf)
