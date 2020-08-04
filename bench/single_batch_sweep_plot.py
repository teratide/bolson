import matplotlib.pyplot as plt
import pandas as pd
import math

plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '14'})

background = '#f5ebdf'
line_colors = ['#000000', '#c0a040', '#007e80', '#e06055', '#3f5fbf', '#d275de', '#7bb144', '#888888']
markers = ['o', 's', 'd', 'v', '^', '<', '>']


def plot_throughput():
    df = pd.read_csv("single_batch_sweep_result.csv")
    x = df['Arrow IPC messages total size (GiB)']
    y = df[['Load JSON (GB/s)', 'Parse JSON (GB/s)',
            'Convert to Arrow RecordBatches (in) (GB/s)', 'Convert to Arrow RecordBatches (out) (GB/s)',
            'Write Arrow IPC messages (GB/s)', 'Publish IPC messages in Pulsar (GB/s)']]

    # Create figs and plot
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(1, 1, 1)

    ax.set_title('JSON $\\rightarrow$ Arrow $\\rightarrow$ Pulsar', fontweight="bold", fontsize=20)

    for (i, (name, data)) in enumerate(y.iteritems()):
        ax.plot(x, data, color=line_colors[i], marker=markers[i], markersize=8)
        i = i + 1

    ax.set_xlabel(x.name)
    ax.set_xscale('log', base=2)
    ax.set_xticks([2 ** n for n in range(8, math.ceil(math.log2(x.max()) / 2 + 1) * 2, 2)])

    ax.set_ylabel('GB/s')
    ax.set_ylim([-0.1, 1.1 * y.max().max()])

    ax.grid(which='both')
    ax.set_facecolor(background)

    fig.set_facecolor('white')
    fig.tight_layout()

    legend_names = [name.replace(' (GB/s)', '') for name in y.columns]
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.15, box.width, box.height * 0.85])
    fig.legend(legend_names,
               loc='upper center',
               bbox_to_anchor=(0.5, 0.15),
               ncol=3,
               fancybox=False,
               frameon=False,
               prop={'size': 11})

    fig.savefig("single_batch_sweep_throughput.png")


def plot_latency():
    df = pd.read_csv("single_batch_sweep_result.csv")
    x = df['Arrow IPC messages total size (GiB)']
    y = df[['Load JSON (s)', 'Parse JSON (s)', 'Convert to Arrow RecordBatches (in) (s)',
            'Convert to Arrow RecordBatches (out) (s)',
            'Write Arrow IPC messages (s)', 'Publish IPC messages in Pulsar (s)']]

    # Create figs and plot
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(1, 1, 1)

    ax.set_title('JSON $\\rightarrow$ Arrow $\\rightarrow$ Pulsar', fontweight="bold", fontsize=20)

    for (i, (name, data)) in enumerate(y.iteritems()):
        ax.plot(x, data, color=line_colors[i], marker=markers[i], markersize=8)
        i = i + 1

    fig.savefig("single_batch_sweep_latency.png")

plot_throughput()
plot_latency()
