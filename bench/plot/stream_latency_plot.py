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
args = parser.parse_args()
csv = args.csv[0]
pdf = args.pdf[0]
title = args.title[0]

# Prepare data
df = pd.read_csv(csv, index_col=0, skiprows=34)
n = len(df.index)
m = len(df.columns)

# Use microseconds
y = []
for col in df.columns:
    y.append(df[col] * 1e6)

# Plot settings
plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '15'})

# Plot stuff
cols = 2
fig, axs = plt.subplots(math.ceil(m/cols), cols, figsize=(5 * cols, 2 + 3 * (m/cols)))

for a in range(0, m):
    ay = int(a/cols)
    ax = a % cols
    # sns.violinplot(ax=axs[a], x=y[a])
    axs[ay][ax].scatter(label='', x=df.index, y=y[a], s=0.25)
    axs[ay][ax].grid(which='both')
    axs[ay][ax].set_title(df.columns[a])
    axs[ay][ax].set_xlabel('Sample \#')
    axs[ay][ax].axhline(y=y[a].mean(), linestyle='--')
    axs[ay][ax].set_ylabel('Time ($\mu s$)')

fig.suptitle(title)
fig.tight_layout()

fig.savefig(pdf)
