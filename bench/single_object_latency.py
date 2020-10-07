import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description='Measure single-object latency N times.')
parser.add_argument('N', type=int, nargs=1)
args = parser.parse_args()

N = args.N[0] + 1

bolson = '../release/bolson'

with open('single_object_latency.csv', 'w') as f:
    f.write("TCP Received,"
            "Converted,"
            "IPC bytes out,"
            "Avg. bytes/msg,"
            "Avg. convert time,"
            "Avg. thread time,"
            "IPC messages,"
            "Avg. publish time,"
            "Publish thread time,"
            "First latency\n")
    f.flush()

    for i in range(0, N):
        # Run bolson
        process = subprocess.run([bolson, 'stream', '-s'], stdout=f)

    f.close()
