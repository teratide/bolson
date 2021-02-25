#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import webcolors as wc
import argparse

parser = argparse.ArgumentParser(description='Plot throughput.')
parser.add_argument('-i', '--inputs', nargs='+', help='<Required> Set flag', required=True)
args = parser.parse_args()
print(args.inputs)


def prepare(file, suffix, per_json=False):
    df = pd.read_csv(file)

    num_batches = len(df.index)

    json_count = df['Last'] - df['First'] + 1
    num_jsons = sum(json_count)

    mean = json_count.mean()
    stdev = json_count.std()

    # This is costly
    if per_json:
        df['Seq'] = df.apply(lambda x: range(x['First'], x['Last'] + 1), axis=1)
        df = df.explode('Seq')
        df.sort_values(by='Seq')

    # Convert to seconds
    df = df[['Parse', 'Resize', 'Serialize', 'Publish']] * 1e-9
    df['Total'] = df['Parse'] + df['Resize'] + df['Serialize'] + df['Publish']

    avg_latency = df.mul(json_count, axis=0)
    avg_latency = avg_latency.sum() / num_jsons

    df = df.add_suffix(" " + suffix)

    print("{}".format(file))
    print("  Num batches      : {}".format(num_batches))
    print("  Size mean        : {}".format(mean))
    print("  Size stdev       : {}".format(stdev))

    return df, num_batches, mean, stdev, avg_latency


def concat(dfs):
    df = dfs[0]
    for i in range(1, len(list)):
        df = pd.concat([dfs[i - 1], dfs[i]], axis=1)
    df = df.reindex(['Parse', 'Parse',
                     'Resize', 'Resize',
                     'Serialize', 'Serialize',
                     'Publish', 'Publish',
                     'Total', 'Total'], axis=1)
    return df


def get_color(hex_str):
    tup = [x / 255.0 for x in tuple(wc.hex_to_rgb(hex_str))]
    tup.append(1.0)
    return tup


def plot_latency(files, title, plot_all_data_points=True, latency_per_json=False):
    all = []

    [all.append(prepare(file, file, latency_per_json)) for file in files]

    df = concat(all)

    fig, ax = plt.subplots(figsize=(10, 5), facecolor='white')

    blue = get_color("#609fbf")
    yellow = get_color("#f5c971")
    colors = [blue, yellow, blue, yellow, blue, yellow, blue, yellow]

    left = 0.5e-7
    right = 0.1e3

    sns.boxenplot(ax=ax,
                  data=df,
                  scale='area',
                  saturation=1,
                  linewidth=0,
                  orient='horizontal',
                  palette=colors,
                  )

    # Draw some lines to separate measurements
    for i in range(0, len(df.columns)):
        ax.axhline(y=i + 0.5, color='gray', linewidth=1, linestyle='--')

    for i in range(1, len(df.columns), 2):
        ax.axhline(y=i + 0.5, color='black', linewidth=2)

    box_text = 'Batches\n(N, size mean, size stdev)\nCPU: {:3}, {:3}, {:3}\nFPGA: {:3}, {:3}, {:3}'
    box_text = box_text.format(nb_cpu,
                               round(bs_mean_cpu),
                               round(bs_stdev_cpu),
                               nb_fpga,
                               round(bs_mean_fpga),
                               round(bs_stdev_fpga))
    # plot batch sizes
    plt.text(x=right * 0.75, y=-0.3, ha='right', va='top', bbox=dict(facecolor='white', linestyle='--'),
             s=box_text, zorder=100)

    if plot_all_data_points:
        # Workaround because zorder doesnt seem to work on sns
        plt.setp(ax.lines, zorder=100)
        plt.setp(ax.collections, zorder=100, label="")

        a = 0.01 + 0.99 / np.log2((nb_cpu + nb_fpga) / 2)

        sns.stripplot(ax=ax,
                      data=df,
                      size=1,
                      alpha=a,
                      jitter=0.5,
                      orient='horizontal',
                      palette=colors,
                      )
    ax.set_xscale('log')
    ax.grid()
    ax.set_xticks([10 ** e for e in range(-7, 3)])
    ax.set_xlim(left, right)
    ax.set_xlabel('Time (s)')
    ax.set_title(title)


plot_latency("cpu.csv",
             "fpga.csv",
             "Title", True, False)
