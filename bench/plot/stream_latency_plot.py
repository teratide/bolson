import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
import math
from quantiphy import Quantity

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

scale = 'μs'

y = []
for col in df.columns:
    y.append(df[col] * 1e6)


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

color = ['#97c3da',
         '#ffffff',
         '#f5c971',
         '#91856d',
         '#404040',
         '#609fbf',
         '#1a74b2']

# Makes it pretty but slow:
# plt.rcParams.update({"text.usetex": True})
# scale = '$\\mu s$'
# plt.rcParams.update({"font.size": '15'})

# Set up figure and axes
cols = 3
rows = math.ceil((m + 1) / cols) + int(args.violin)

axs = []
fig = plt.figure(figsize=(5 * cols, 2 + 3 * (m / cols)))

for i in range(0, m):
    axs.append(plt.subplot2grid((rows, cols), (int(i / cols), i % cols), 1, 1))

axs.append(plt.subplot2grid((rows, cols), (int(m / cols), 0), 1 + int(args.violin), cols))

# Plot latencies
for i in range(0, m):
    a = axs[i]

    bb = dict(fc='white', alpha=0.75, ec='white', lw=None)
    smean = 'Mean: {} {}'.format(str(round(y[i].mean(), 3)), scale)
    sstd = 'Stdev: {} {}'.format(str(round(y[i].std(), 3)), scale)
    q50 = '50-th: {} {}'.format(str(round(y[i].quantile(0.5), 3)), scale)
    q95 = '95-th: {} {}'.format(str(round(y[i].quantile(0.95), 3)), scale)
    q99 = '99-th: {} {}'.format(str(round(y[i].quantile(0.99), 3)), scale)
    smm = 'Min/max ({}): {} / {}'.format(scale, str(round(y[i].min(), 3)), str(round(y[i].max(), 3)))

    if args.violin:
        bw = y[i].min() / y[i].max()
        #sns.violinplot(ax=a, x=y[i], inner='box', bw=bw, color=color[0], linewidth=2)
        sns.boxenplot(ax=a, x=y[i], scale='area', linewidth=0)
        a.set_xlim(0, a.get_xlim()[1])
        a.set_xlabel('Time (' + scale + ')')
    else:
        a.scatter(label='', x=df.index, y=y[i], s=0.5, color=color[0])
        a.set_xlabel('Sample')
        a.axhline(y=y[i].mean(), linestyle='--', color='red')
        a.set_ylabel('Time (' + scale + ')')
        # axs[ay][ax].set_ylim(1e-7, 2*max(y[m-1]))
        # axs[ay][ax].set_yscale('log')

    a.set_ylim(a.get_ylim()[0], a.get_ylim()[1] * (1.1 + 0.2 * int(args.violin)))
    a.text(x=0.02, y=0.95, s=smm, transform=a.transAxes, ha='left', va='top', bbox=bb, fontsize=8)

    a.text(x=0.98, y=0.35, s=q50, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=8)
    a.text(x=0.98, y=0.24, s=q95, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=8)
    a.text(x=0.98, y=0.13, s=q99, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=8)

    a.text(x=0.98, y=0.95, s=smean, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=8)
    a.text(x=0.98, y=0.84, s=sstd, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=8)

    a.grid(which='both')
    a.set_title(df.columns[i], fontweight='bold')

# Plot throughput on last axis
ys = range(0, len(s_dsc))
a = axs[m]
a.set_title("Average stage throughput", fontweight='bold')
a.grid(which='both')
a.barh(ys, s_val_mjs, color=color[4])
a.set_yticks(ys)
a.set_yticklabels(s_dsc)
a.set_xscale('log')
a.set_xlabel('Throughput (MJ/s)')
a.invert_yaxis()

# Finalize the figure and save
fig.suptitle(title, fontsize=15, fontweight='bold')
fig.tight_layout()

fig.savefig(pdf)
