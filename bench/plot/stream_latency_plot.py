import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import argparse
import math
import numpy as np
from quantiphy import Quantity

# Parse arguments
parser = argparse.ArgumentParser(description='Plot queue latency.')
parser.add_argument('lat', type=str, nargs=1)
parser.add_argument('log', type=str, nargs=1)
parser.add_argument('pdf', type=str, nargs=1)
parser.add_argument('title', type=str, nargs=1)
parser.add_argument('--fpga_lat', type=str)
parser.add_argument('--fpga_log', type=str)
parser.add_argument('--boxen', action="store_true")
args = parser.parse_args()
csv = args.lat[0]
log = args.log[0]
pdf = args.pdf[0]
title = args.title[0]
boxen = args.boxen
fpga_csv = args.fpga_lat
fpga_log = args.fpga_log

fpga = True if fpga_csv and fpga_log else False

# Prepare latency data
df = pd.read_csv(csv, index_col=0)

if fpga:
    fpga_df = pd.read_csv(fpga_csv, index_col=0)
else:
    fpga_df = None

# Number of sampled JSONs
n = len(df.index)
# Number of time points
m = len(df.columns)

scale = 's'


def get_tp(file, which):
    s_val = open(file, "r").readlines()
    s_val = filter(lambda l: which in l, s_val)
    s_val = map(lambda s: float(s.split(':')[1].split(which)[0]), s_val)
    s_val = list(s_val)
    return s_val


s_val_mjs = get_tp(log, 'MJ/s')
if fpga:
    fpga_s_val_mjs = get_tp(fpga_log, 'MJ/s')

s_dsc = ["TCP client",
         "JSON Parsing",
         "Adding Seq. Nos.",
         "Combing batches",
         "Serializing",
         "Pulsar publish"]

# Plot settings
color = ['#97c3da', '#ffffff', '#f5c971', '#91856d', '#404040', '#609fbf', '#1a74b2']

# Makes it pretty but slow:
# plt.rcParams.update({"text.usetex": True})
# scale = '$\\mu s$'
# plt.rcParams.update({"font.size": '15'})

# Set up figure and axes
cols = 4
rows = math.ceil(m / cols) + 1

axs = []
fig = plt.figure(figsize=(5 * cols, 2 + 3.5 * (m / cols)))

for i in range(0, m):
    axs.append(plt.subplot2grid((rows, cols), (int(i / cols), i % cols), 1, 1))

axs.append(plt.subplot2grid((rows, cols), (int(m / cols), 0), 1 + int(args.boxen), cols))

# Plot latencies
y = []
fy = []
for col in df.columns:
    y.append(df[col])
    if fpga:
        fy.append(fpga_df[col])

for im in range(0, m):
    a = axs[im]

    # Generate strings with statistics
    bb = dict(fc='white', alpha=0.75, ec='white', lw=None)
    sms = 'μ: {} σ: {}'.format(Quantity(y[im].mean(), scale), Quantity(y[im].std(), scale))
    pct = '50-th: {}, 95-th: {}, 99-th: {}'.format(Quantity(y[im].quantile(0.5), scale),
                                                   Quantity(y[im].quantile(0.95), scale),
                                                   Quantity(y[im].quantile(0.99), scale))
    smm = 'min-max: {} - {}'.format(Quantity(y[im].min(), scale), Quantity(y[im].max(), scale))

    if fpga:
        fbb = dict(fc='white', alpha=0.75, ec='white', lw=None)
        fsms = 'μ: {} σ: {} min: {} max: {}'.format(Quantity(y[im].mean(), scale),
                                                    Quantity(y[im].std(), scale),
                                                    Quantity(fy[im].min(), scale),
                                                    Quantity(fy[im].max(), scale))
        fpct = '50-th: {}, 95-th: {}, 99-th: {}'.format(Quantity(y[im].quantile(0.5), scale),
                                                        Quantity(y[im].quantile(0.95), scale),
                                                        Quantity(y[im].quantile(0.99), scale))

    if boxen:
        if fpga:
            sns.boxenplot(ax=a, data=[y[im], fy[im]], scale='area', linewidth=0, orient='horizontal')
        else:
            sns.boxenplot(ax=a, data=y[im], scale='area', linewidth=0, orient='horizontal')
        a.set_xlim(0, a.get_xlim()[1])
        a.set_xlabel('Time (' + scale + ')')
        if fpga:
            a.set_yticklabels(['CPU', 'FPGA'])
    else:
        a.scatter(label='', x=df.index, y=y[im], s=0.5, c=color[0])
        a.axhline(y=y[im].mean(), linestyle='--', c=color[0])
        if fpga:
            a.scatter(label='', x=df.index, y=fy[im], s=0.5, c=color[2])
            a.axhline(y=y[im].mean(), linestyle='--', c=color[2])
        a.set_xlabel('Sample')
        a.set_ylabel('Time (' + scale + ')')
        a.set_xlim(0, n)

    # Make space for text.
    yspace = a.get_ylim()[1] * (0.2 + 0.2 * int(boxen) + 0.3 * int(fpga))
    fs = 7
    a.set_ylim(a.get_ylim()[0] - yspace, a.get_ylim()[1] + yspace)
    a.text(x=0.98, y=0.10, s=sms, transform=a.transAxes, ha='right', va='bottom', bbox=bb, fontsize=fs)
    a.text(x=0.98, y=0.035, s=pct, transform=a.transAxes, ha='right', va='bottom', bbox=bb, fontsize=fs)
    if fpga:
        a.text(x=0.98, y=0.965, s=fsms, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=fs)
        a.text(x=0.98, y=0.90, s=fpct, transform=a.transAxes, ha='right', va='top', bbox=bb, fontsize=fs)

    a.grid(which='both')
    xmax = a.get_xlim()[1]
    xt = a.get_xticks()
    xl = list(map(lambda t: Quantity(t, scale).render(form='sia', show_units=False), xt))
    a.set_xticks(xt)
    a.set_xticklabels(xl)

    a.set_title(df.columns[im], fontweight='bold')

# Plot throughput on last axis
ys = np.arange(0, 2 * len(s_dsc), 2)
at = axs[m]
at.set_title("Average stage throughput", fontweight='bold')
at.grid(which='both')
at.barh(ys - 0.5, s_val_mjs, label='CPU', linewidth=1, edgecolor='#222222')
if fpga:
    at.barh(ys + 0.5, fpga_s_val_mjs, label='FPGA', linewidth=1, edgecolor='#222222')
at.set_yticks(ys)
at.set_yticklabels(s_dsc)
at.set_xscale('log')
at.set_xlabel('Throughput (MJ/s)')
at.invert_yaxis()
if fpga:
    at.legend()
at.set_axisbelow(True)

# Finalize the figure and save
fig.suptitle(title, fontsize=15, fontweight='bold')
fig.tight_layout()

fig.savefig(pdf)
