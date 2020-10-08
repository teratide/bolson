import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description='Measure single-object latency N times.')
parser.add_argument('schema', type=str, nargs=1)
parser.add_argument('csv', type=str, nargs=1)
parser.add_argument('num_jsons', type=int, nargs=1)
args = parser.parse_args()
num_jsons = args.num_jsons[0]
schema = args.schema[0]
csv = args.csv[0]

bolson = '../release/bolson'

with open(csv, 'w') as f:
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

    for i in range(0, num_jsons + 1):
        # Run bolson
        command = [bolson, 'stream', '-c', schema]
        process = subprocess.run(command, stdout=f)

    f.close()
