import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description='Measure single-object latency N times.')
parser.add_argument('schema', type=str, nargs=1)
parser.add_argument('csv', type=str, nargs=1)
parser.add_argument('log2_max_jsons', type=int, nargs=1)
parser.add_argument('threads', type=int, nargs=1)
args = parser.parse_args()
log2_max_jsons = args.log2_max_jsons[0]
schema = args.schema[0]
csv = args.csv[0]
threads = args.threads[0]

bolson = '../release/bolson'

with open(csv, 'w') as f:
    f.write("Number of JSONs,"
            "Raw JSONs size,"
            "JSON queue size,"
            "JSON queue fill time,"
            "IPC size,"
            "Convert time,"
            "Number of threads\n")
    f.flush()

    for i in range(0, log2_max_jsons + 1):
        # Run bolson
        command = [bolson, 'bench', '-c', 'convert', '--num-jsons', str(2 ** i), '--threads', str(threads), schema]
        process = subprocess.run(command, stdout=f)

    f.close()
