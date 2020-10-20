import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description='Measure single-object latency N times.')
parser.add_argument('csv', type=str, nargs=1)
parser.add_argument('log2_max_messages', type=int, nargs=1)
parser.add_argument('message_size', type=int, nargs=1)
args = parser.parse_args()
log2_max_messages = args.log2_max_messages[0]
csv = args.csv[0]
message_size = args.message_size[0]

bolson = '../release/bolson'

with open(csv, 'w') as f:
    f.write("Messages,"
            "Message size,"
            "Time\n")
    f.flush()

    for i in range(0, log2_max_messages + 1):
        # Run bolson
        command = [bolson, 'bench', '-c', 'pulsar', '--num-messages', str(2 ** i), '--message-size', str(message_size)]
        process = subprocess.run(command, stdout=f)

    f.close()
