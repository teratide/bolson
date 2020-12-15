import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import argparse
from scipy import stats

parser = argparse.ArgumentParser(description='Plot throughput.')
parser.add_argument('log_cpu', type=str, nargs=1)
parser.add_argument('log_fpga', type=str, nargs=1)
parser.add_argument('pdf', type=str, nargs=1)
parser.add_argument('title', type=str, nargs=1)
args = parser.parse_args()
log_cpu = args.log_cpu[0]
log_fpga = args.log_fpga[0]
pdf = args.pdf[0]
title = args.title[0]


# Prepare data
def get_tp(file, which):
    s_val = open(file, "r").readlines()
    s_val = filter(lambda l: which in l, s_val)
    s_val = map(lambda s: float(s.split(':')[1].split(which)[0]), s_val)
    s_val = list(s_val)
    return s_val


t_cpu = get_tp(log_cpu, 'MB/s')
t_fpga = get_tp(log_fpga, 'MB/s')

s_dsc = ["Generation",
         "End-to-end conversion (in)",
         "End-to-end conversion (out)",
         "Parsing",
         "Resizing",
         "Serializing",
         "Enqueueing"]

# Plot
plt.rcParams.update({"text.usetex": True})
#plt.rcParams.update({"font.size": '15'})

background = '#ffffff'
colors = ['#1a74b2', '#d9a53f']

fig, ax = plt.subplots()

x = np.arange(1, len(s_dsc) + 1)
x_cpu = x - 0.5
x_fpga = x + 0.5

print(x_cpu)
print(t_cpu)

ax.barh(0, t_cpu[3])
ax.barh(1, t_fpga[3])
ax.set_yticks([0, 1])
ax.set_yticklabels(["CPU\n(32 threads)", "FPGA\n(1 parser instance)"])
ax.set_xlabel("Throughput (MB/s)")
ax.grid(True)
ax.set_title(title, fontsize=16)

fig.set_facecolor('white')
fig.tight_layout()

fig.savefig(pdf)
