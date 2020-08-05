import matplotlib.pyplot as plt
import pandas as pd
import math
from textwrap import wrap

plt.rcParams.update({"text.usetex": True})
plt.rcParams.update({"font.size": '12'})

background = '#f5ebdf'
colors = ['#000000', '#c0a040', '#007e80', '#e06055', '#3f5fbf', '#d275de', '#7bb144', '#888888']
markers = ['o', 's', 'd', 'v', '^', '<', '>']


def plot_throughput():
    df = pd.read_csv("single_batch_sweep_result.csv")
    x = df['Arrow IPC messages total size (B)']
    y = df[['Load JSON (GB/s)', 'Parse JSON (GB/s)',
            'Convert to Arrow RecordBatches (in) (GB/s)', 'Convert to Arrow RecordBatches (out) (GB/s)',
            'Write Arrow IPC messages (GB/s)', 'Publish IPC messages in Pulsar (GB/s)']]

    # Create figs and plot
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(1, 1, 1)

    ax.set_title('JSON $\\rightarrow$ Arrow $\\rightarrow$ Pulsar', fontweight="bold", fontsize=20)

    for (i, (name, data)) in enumerate(y.iteritems()):
        ax.plot(x, data, color=colors[i], marker=markers[i], markersize=8)
        i = i + 1

    ax.set_xlabel(x.name)
    ax.set_xscale('log', base=2)
    ax.set_xticks([2 ** n for n in
                   range(math.ceil(math.log2(x.min()) / 2 - 1) * 2, math.ceil(math.log2(x.max()) / 2 + 1) * 2, 2)])

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
    b = df['Arrow IPC messages avg. size (B)']
    t = df['Number of tweets']
    x = t  # df['Number of RecordBatches']
    y = df[['Load JSON (s)',
            'Parse JSON (s)',
            'Convert to Arrow RecordBatches (out) (s)',
            'Write Arrow IPC messages (s)',  # ]].div(t, axis='index')
            'Publish IPC messages in Pulsar (s)']].div(t, axis='index')

    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    rows = y.shape[0]

    # Create figs and plot
    fig = plt.figure(figsize=(10, 8))
    axes = fig.subplots(nrows=6, sharex=True)  # , gridspec_kw={'wspace': 0, 'hspace': 0.05})

    for i in range(0, rows):
        for (j, value) in enumerate(y.iloc[i].values):
            axes[j].bar(x[i], value, width=0.4 * x[i], color=colors[j])
            axes[j].text(x[i], value, '{:.2e}'.format(value),
                         ha='left', va='bottom', rotation=30,
                         fontsize='xx-small', fontstyle='oblique')

        axes[5].bar(x[i], b[i], width=0.4 * x[i], color=colors[5])
        axes[5].text(x[i], b[i], '{:.2e}'.format(b[i]),
                     ha='left', va='bottom', rotation=30,
                     fontsize='xx-small', fontstyle='oblique')

    for (j, ax) in enumerate(axes[:5]):
        axes[j].set_ylabel('\n'.join(wrap('Avg. ' + y.columns[j], 11)), rotation=0, ha='right', va='center', fontsize='x-small')
        ax.set_xscale('log', base=2)
        ax.set_xlim([1 / 2, x.max() * 2])
        ax.set_xticks([2 ** x for x in range(0, int(math.ceil(math.log2(x.max() * 2))))])
        ax.set_yscale('log', base=10)
        ax.set_yticks([10 ** (-x) for x in range(1, 7)])
        ax.set_ylim([10 ** (-7), 1])

        ax.grid(which='both')
        ax.set_facecolor(background)

    axes[5].set_yscale('log', base=2)
    axes[5].set_yticks([2 ** x for x in range(10, 26, 2)])
    axes[5].set_ylim([b.min()/2, 16 * b.max()])
    axes[5].set_ylabel('\n'.join(wrap(b.name, 16)), rotation=0, ha='right', va='center', fontsize='x-small')

    axes[5].grid(which='both')
    axes[5].set_facecolor(background)

    axes[-1].set_xlabel(x.name)

    fig.set_facecolor('white')

    fig.tight_layout()
    fig.subplots_adjust(hspace=0.05)

    fig.savefig("single_batch_sweep_latency.png")


plot_throughput()
plot_latency()
