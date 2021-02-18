import subprocess
import os
import time

root = './data/pulsar'
bolson = '../release/bolson'

os.makedirs(root, exist_ok=True)

for p in range(0, 5 + 1):
    for n in range(0, 10 + 1):
        for s in range(0, 22 + 1):
            with open("{}/p{}_n{}_s{}.log".format(root, p, n, s), 'w') as log:
                # Run bolson bench pulsar
                command = [bolson, 'bench', 'pulsar',
                           '-n', str(2 ** n),
                           '-s', str(2 ** s),
                           '--pulsar-producers', str(2 ** p),
                           '--pulsar-url', 'pulsar://localhost:6650/',
                           '--pulsar-topic', 'non-persistent://public/default/bolson',
                           '-l', "{}/p{}_n{}_s{}.csv".format(root, p, n, s)]
                print(' '.join(command))
                process = subprocess.run(command, stdout=log)
                log.close()
