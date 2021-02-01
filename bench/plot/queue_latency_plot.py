import pandas as pd
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

# Use microseconds
y_enq = df['Enqueue'] * 1e6
y_deq = df['Dequeue'] * 1e6

# Plot settings
plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '15'})

# Plot stuff
fig, ax = plt.subplots(2, 1, figsize=(5, 5))
ax[0].scatter(label='', x=df.index, y=y_enq, color='blue', s=1)
ax[1].scatter(label='', x=df.index, y=y_deq, color='red', s=1)

ax[0].grid(which='both')
ax[1].grid(which='both')

ax[0].set_ylabel('Enqueue time ($\mu s$)')
ax[0].set_xlabel('Experiment')
ax[1].set_ylabel('Dequeue time ($\mu s$)')
ax[1].set_xlabel('Experiment')

ax[0].axhline(y=y_enq.mean(), color='blue', linestyle='--')
ax[1].axhline(y=y_deq.mean(), color='red', linestyle='--')

fig.suptitle(title)
fig.tight_layout()

fig.savefig(pdf)
