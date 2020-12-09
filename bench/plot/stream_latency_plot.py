import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
import math

# Parse arguments
parser = argparse.ArgumentParser(description='Plot queue latency.')
parser.add_argument('lat', type=str, nargs=1)
parser.add_argument('log', type=str, nargs=1)
parser.add_argument('pdf', type=str, nargs=1)
parser.add_argument('title', type=str, nargs=1)
parser.add_argument('--violin', action="store_true")
args = parser.parse_args()
csv = args.lat[0]
log = args.log[0]
pdf = args.pdf[0]
title = args.title[0]
violin = args.violin

# Prepare latency data
df = pd.read_csv(csv, index_col=0)
# Number of sampled JSONs
n = len(df.index)
# Number of time points
m = len(df.columns)

y = []
for col in df.columns:
    y.append(df[col])


def get_tp(which):
    s_val = open(log, "r").readlines()
    s_val = filter(lambda l: which in l, s_val)
    s_val = map(lambda s: float(s.split(':')[1].split(which)[0]), s_val)
    s_val = list(s_val)
    return s_val


s_val_mjs = get_tp('MJ/s')

s_dsc = [
    "TCP client",
    "JSON Parsing",
    "Adding Seq. Nos.",
    "Combing batches",
    "Serializing",
    "Pulsar publish"
]

# Plot settings
#plt.rcParams.update({"text.usetex": True})
#plt.rcParams.update({"font.size": '15'})

# Set up figure and axes
cols = 3
fig, axs = plt.subplots(math.ceil((m + 1) / cols), cols, figsize=(5 * cols, 2 + 3 * ((m + 1) / cols)))

# Plot latencies
for a in range(0, m):
    ay = int(a / cols)
    ax = a % cols
    if args.violin:
        sns.violinplot(ax=axs[ay][ax], x=y[a], bw=2.5e-2)
        # axs[ay][ax].set_xlim(0, max(y[m-1]))
        axs[ay][ax].set_xlabel('Time ($s$)')
    else:
        axs[ay][ax].scatter(label='', x=df.index, y=y[a], s=0.25)
        axs[ay][ax].set_xlabel('Sample \#')
        axs[ay][ax].axhline(y=y[a].mean(), linestyle='--', color='red')
        axs[ay][ax].set_ylabel('Time ($s$)')
        # axs[ay][ax].set_ylim(1e-7, 2*max(y[m-1]))
        # axs[ay][ax].set_yscale('log')

    axs[ay][ax].grid(which='both')
    axs[ay][ax].set_title(df.columns[a])

# Plot throughput on last axis
ay = int(m / cols)
ax = m % cols
ys = range(0, len(s_dsc))
a = axs[ay][ax]
a.set_title("Average stage throughput")
a.barh(ys, s_val_mjs)
a.set_yticks(ys)
a.set_yticklabels(s_dsc)
a.set_xscale('log')
a.set_xlabel('Throughput (MJ/s)')
a.invert_yaxis()

# Finalize the figure and save
fig.suptitle(title)
fig.tight_layout()

fig.savefig(pdf)
