# Usage

This chapter describes how to use Bolson.

## Requirements

To use Bolson, you need the following:

- A source of JSON data that acts as a TCP server. Bolson operates as long as the connection is alive.
- An [Apache Pulsar](https://pulsar.apache.org) broker.
- An [Apache Arrow](https://arrow.apache.org) [Schema](#arrow-schema)
  - Unless fixed schema parsing implementations are used, such as specific FPGA implementations.

### Arrow Schema

For Bolson to read Arrow Schemas, they need to be serialized to a file using
Arrow's built-in schema serialization facility.

An example of how to define and serialize a schema in Python:

```python
import pyarrow

schema = pyarrow.schema([pyarrow.field("field", pyarrow.uint64())])
pyarrow.output_stream("example.as").write(schema.serialize())
```

### JSON data source

If you do not have a JSON data source that can act as a TCP server, it is
possible to generate random JSON data using a companion project of Bolson named [Illex](https://github.com/teratide/illex).

Illex is used throughout this example.

The requirements for a JSON data source acting as a TCP are simple. When Bolson
connects, the source can start sending data, without any additional protocol,
with the exception that each JSON object must be terminated by the newline
character `'\n'`.

### Pulsar broker

If you do not have a Pulsar broker, it can easily be spawned locally using
[Docker](https://docker.com):

```bash
docker run -it --rm -p 6650:6650 -p 8080:8080 apachepulsar/pulsar bin/pulsar standalone
```

## Subcommands

Bolson knows two subcommands, `stream` and `bench`.

- **Stream**: Convert JSONs and publish them to Pulsar in a streaming fashion.
- **Bench**: Run micro-benchmarks of specific components of Bolson.

### Stream

```
Produce Pulsar messages from a JSON TCP stream.
Usage: bolson stream [OPTIONS] [input]

Positionals:
  input TEXT:FILE                                 Serialized Arrow schema file for records to convert to.

Options:
  -h,--help                                       Print this help message and exit
  --latency TEXT                                  Enable batch latency measurements and write to supplied file.
  --metrics TEXT                                  Write metrics to supplied file.
  --max-rows UINT=1024                            Maximum number of rows per RecordBatch.
  --max-ipc UINT=5232640                          Maximum size of IPC messages in bytes.
  --threads UINT=1                                Number of threads to use for conversion.
  -p,--parser ENUM:value in {arrow->0,opae-battery->1,opae-trip->2} OR {0,1,2}=0
                                                  Parser implementation. OPAE parsers have fixed schema and ignore schema supplied to -i.
  -i,--input TEXT:FILE                            Serialized Arrow schema file for records to convert to.
  --arrow-buf-cap UINT=16777216                   Arrow input buffer capacity.
  --arrow-seq-col=0                               Arrow parser, retain ordering information by adding a sequence number column.
  --battery-afu-id TEXT                           OPAE "battery status" AFU ID. If not supplied, it is derived from number of parser instances.
  --battery-num-parsers UINT=8                    OPAE "battery status" number of parser instances.
  --battery-seq-col=0                             OPAE "battery status" parser, retain ordering information by adding a sequence number column.
  --trip-afu-id TEXT                              OPAE "trip report" AFU ID. If not supplied, it is derived from number of parser instances.
  --trip-num-parsers UINT=4                       OPAE "trip report" number of parser instances.
  -u,--pulsar-url TEXT=pulsar://localhost:6650/   Pulsar broker service URL.
  -t,--pulsar-topic TEXT=non-persistent://public/default/bolson
                                                  Pulsar topic.
  --pulsar-max-msg-size UINT=5232640
  --pulsar-producers UINT=1                       Number of concurrent Pulsar producers.
  --pulsar-batch                                  Enable batching Pulsar producer(s).
  --pulsar-batch-max-messages UINT=1000           Pulsar batching max. messages.
  --pulsar-batch-max-bytes UINT=131072            Pulsar batching max. bytes.
  --pulsar-batch-max-delay UINT=10                Pulsar batching max. delay (ms).
  --host TEXT=localhost                           JSON source TCP server hostname.
  --port UINT=10197                               JSON source TCP server port.

```

### Bench

```
Run micro-benchmarks on isolated pipeline stages.
Usage: bolson bench [OPTIONS] SUBCOMMAND

Options:
  -h,--help                                       Print this help message and exit

Subcommands:
  client                                          Run TCP client interface microbenchmark.
  convert                                         Run JSON to Arrow IPC convert microbenchmark.
  queue                                           Run queue microbenchmark.
  pulsar                                          Run Pulsar publishing microbenchmark.

```
