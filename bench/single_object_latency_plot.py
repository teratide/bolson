import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import argparse

parser = argparse.ArgumentParser(description='Plot single-object latency.')
parser.add_argument('csv', type=str, nargs=1)
parser.add_argument('pdf', type=str, nargs=1)
parser.add_argument('title', type=str, nargs=1)
args = parser.parse_args()
csv = args.csv[0]
pdf = args.pdf[0]
title = args.title[0]

# Prepare data

df = pd.read_csv(csv)

y_lat = df['First latency']
y_conv = df['Avg. convert time']
y_pub = df['Avg. publish time']

# JSON to Arrow conversion is included in y_lat
y_total = y_lat + y_pub

num_rows = df.count()[0]
x = np.arange(0, num_rows)

# Plot

plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '15'})

background = '#f5ebdf'
colors = ['#000000', '#c0a040', '#007e80', '#e06055', '#3f5fbf', '#d275de', '#7bb144', '#888888']

fig = plt.figure(figsize=(16, 8))
ax_left = fig.add_subplot(1, 6, (1, 5))
ax_right = fig.add_subplot(1, 6, 6, sharey=ax_left)

ax_left.scatter(x=x, y=y_lat, s=2, c=colors[2], label='First TCP packet to Pulsar send()')
ax_left.scatter(x=x, y=y_conv, s=2, c=colors[3], label='JSON to Arrow')
ax_left.scatter(x=x, y=y_pub, s=2, c=colors[4], label='Pulsar send()')
ax_left.scatter(x=x, y=y_total, s=2, c=colors[1], label='Total')

sns.histplot(ax=ax_right, y=y_lat, color=colors[2])

ax_left.hlines(df["First latency"].mean(), 0, num_rows, color=colors)

ax_left.grid(which='both')
ax_right.grid(which='both')

ax_left.set_ylabel('Time (s)')
ax_right.set_ylabel(None)
ax_left.set_xlabel('Experiment no.')

ax_left.set_facecolor(background)
ax_right.set_facecolor(background)

leg = ax_left.legend()
for h in leg.legendHandles:
    h._sizes = [30]

ax_left.set_title(title)

fig.set_facecolor('white')
fig.tight_layout()

fig.savefig(pdf)
