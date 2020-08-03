import subprocess
import os
import argparse

parser = argparse.ArgumentParser(description='Sweep over a batches with tweet counts 2^0 .. 2^N')
parser.add_argument('N', type=int, nargs=1)
args = parser.parse_args()

N = args.N[0] + 1

tweetgen = '../release/cpp/tweetgen/tweetgen'
flitter = '../release/cpp/flitter/flitter'

with open('single_batch_sweep_result.csv', 'w') as f:
    f.write("Load JSON (s),"
            "Load JSON (GB/s),"
            "Parse JSON (s),"
            "Parse JSON (GB/s),"
            "Convert to Arrow (in) (s),"
            "Convert to Arrow (in) (GB/s),"
            "Convert to Arrow (out) (s),"
            "Convert to Arrow (out) (GB/s),"
            "Write Arrow IPC message (s),"
            "Write Arrow IPC message (GB/s),"
            "Publish IPC message in Pulsar (s),"
            "Publish IPC message in Pulsar (GB/s),"
            "Number of tweets,"
            "JSON File size (B),"
            "Arrow RecordBatch size (B),"
            "Arrow IPC message size (B)\n"
            )
    f.flush()

    for i in range(0, N):
        num_tweets = 2 ** i
        json_file = 'random_{:08d}.json'.format(num_tweets)

        # Run tweetgen
        subprocess.run([tweetgen, '-s', '42', '-o', json_file, '-n', str(num_tweets)])

        # Run flitter
        process = subprocess.run([flitter, '-s', json_file], stdout=f)

        # Remove file
        os.remove(json_file)

    f.close()
