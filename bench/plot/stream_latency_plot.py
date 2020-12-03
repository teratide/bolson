import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import argparse

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
df = pd.read_csv(csv, index_col=0)
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
fig, axs = plt.subplots(m, 1, figsize=(5, 2 + 2 * m))

for a in range(0, m):
    sns.violinplot(ax=axs[a], x=y[a])
    #axs[a].scatter(label='', x=df.index, y=y[a], s=0.25)
    #axs[a].grid(which='both')
    axs[a].set_title(df.columns[a])
    #axs[a].set_xlabel('JSON')
    #axs[a].axhline(y=y[a].mean(), linestyle='--')
    axs[a].set_xlabel('Latency ($\mu s$)')

fig.suptitle(title)
fig.tight_layout()

fig.savefig(pdf)
