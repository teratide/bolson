import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import webcolors as wc
import argparse
import numpy as np

parser = argparse.ArgumentParser(description='Plot throughput.')
parser.add_argument('--csv', nargs='+', help='<Required> Set flag', required=True)
parser.add_argument('--log', nargs='+', help='<Required> Set flag', required=True)
parser.add_argument('--pdf', type=str, required=True)
parser.add_argument('--title', type=str)
args = parser.parse_args()

if len(args.csv) != len(args.log):
    sys.exit('Need same amount of csv and log files.')


def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def prepare(csv, log):
    # Extract options and throughput from log.
    with open(log) as log:
        log = [x.strip() for x in log.readlines()]

    (num_msg, msg_size) = [int(s) for s in log[1].split() if s.isdigit()]
    num_prod = [int(s) for s in log[8].split() if s.isdigit()][0]
    throughput = round([float(s) for s in log[11].split() if is_float(s)][0], 2)

    # Prepare latency data
    df = pd.read_csv(csv)
    df = df * 1e-9

    return df, num_msg, msg_size, num_prod, throughput


def get_color(hex_str):
    tup = [x / 255.0 for x in tuple(wc.hex_to_rgb(hex_str))]
    tup.append(1.0)
    return tup


blue = get_color("#609fbf")
yellow = get_color("#f5c971")


def add_plot(ax, data, x_range):
    (df, num_msg, msg_size, num_prod, throughput) = data
    sns.boxenplot(ax=ax, data=df, scale='area', saturation=1, linewidth=0, orient='horizontal', color=yellow)
    # zorder fix
    # plt.setp(ax.lines, zorder=100)
    # plt.setp(ax.collections, zorder=100, label="")
    # sns.stripplot(ax=ax, data=df, size=2, alpha=1, jitter=0.5, orient='horizontal', color=blue)

    ax.set_xscale('log')
    ax.set_xlim(min(1e-3, x_range[0]), max(1e0, x_range[1]))


num_plots = len(args.csv)
print("Plotting {} files.".format(num_plots))

fig, axs = plt.subplots(ncols=2, figsize=(10, 1.25 * num_plots), facecolor='white')

x_min = 0
x_max = 0
all_df = []
all_tp = []
for i in range(0, num_plots):
    print("Reading " + args.csv[i] + " and " + args.log[i])
    (df, num_msg, msg_size, num_prod, throughput) = prepare(args.csv[i], args.log[i])
    txt = "\n".join(["Messages: {}",
                     "Size/msg: {} B",
                     "Producers: {}",
                     "Throughput: {} MB/s"]).format(num_msg, msg_size, num_prod, throughput)
    x_min = max(x_min, min(df['Publish']))
    x_max = max(x_max, max(df['Publish']))
    all_df.append(df.rename(columns={"Publish": txt}))
    all_tp.append(throughput)

all_df = pd.concat(all_df, axis=1)

print(all_df)

add_plot(axs[0], (all_df, 0, 0, 0, 0), (x_min, x_max))
axs[0].grid(True)
axs[0].set_xlabel("Latency (s)")

tp_yticks = 0.5 + np.arange(0, num_plots)
axs[1].barh(y=tp_yticks, width=all_tp, color=blue)
axs[1].grid(True)
axs[1].set_yticks(tp_yticks)
axs[1].set_yticklabels([])
axs[1].set_ylim(0, num_plots)
axs[1].set_xlabel("Throughput (MB/s)")
plt.gca().invert_yaxis()

fig.suptitle(args.title)
fig.tight_layout()
fig.savefig(args.pdf)
