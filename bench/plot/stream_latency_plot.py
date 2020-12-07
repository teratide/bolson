import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
import math

# Parse arguments
parser = argparse.ArgumentParser(description='Plot queue latency.')
parser.add_argument('csv', type=str, nargs=1)
parser.add_argument('pdf', type=str, nargs=1)
parser.add_argument('title', type=str, nargs=1)
parser.add_argument('--violin', action="store_true")
args = parser.parse_args()
csv = args.csv[0]
pdf = args.pdf[0]
title = args.title[0]
violin = args.violin

# Prepare data
df = pd.read_csv(csv, index_col=0, skiprows=34)
n = len(df.index)
m = len(df.columns)

# Use microseconds
y = []
for col in df.columns:
    y.append(df[col])

# Plot settings
plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '15'})

# Plot stuff
cols = 3
fig, axs = plt.subplots(math.ceil(m/cols), cols, figsize=(5 * cols, 2 + 3 * (m/cols)))

for a in range(0, m):
    ay = int(a/cols)
    ax = a % cols
    if args.violin:
        sns.violinplot(ax=axs[ay][ax], x=y[a], bw=1e-1)
        #axs[ay][ax].set_xlim(0, max(y[m-1]))
        axs[ay][ax].set_xlabel('Time (s)')
    else:
        axs[ay][ax].scatter(label='', x=df.index, y=y[a], s=0.25)
        axs[ay][ax].set_xlabel('Sample \#')
        axs[ay][ax].axhline(y=y[a].mean(), linestyle='--')
        axs[ay][ax].set_ylabel('Time (s)')
        axs[ay][ax].set_ylim(1e-7, 2*max(y[m-1]))
        axs[ay][ax].set_yscale('log')

    axs[ay][ax].grid(which='both')
    axs[ay][ax].set_title(df.columns[a])

fig.suptitle(title)
fig.tight_layout()

fig.savefig(pdf)
