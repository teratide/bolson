import pulsar
import pyarrow

client = pulsar.Client('pulsar://pulsar:6650')
consumer = client.subscribe('bolson', subscription_name='my-sub', initial_position=pulsar.InitialPosition.Earliest)

schema = pyarrow.schema([
    pyarrow.field("voltage", pyarrow.list_(
        pyarrow.field("item", pyarrow.uint64(), False)
    ), False)
])

while True:
    msg = consumer.receive()
    consumer.acknowledge(msg)
    rb = pyarrow.ipc.read_record_batch(msg.data(), schema)
    print(rb.to_pydict())

client.close()
