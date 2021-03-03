# Usage

This chapter describes how to use Bolson.

## Requirements

To use Bolson, you need the following:

- A source of JSON data that acts as a TCP server. Bolson will operate as long
  as the connection is alive.
- A Pulsar broker.
- An [Arrow Schema](#arrow-schema)
    - Unless fixed schema parsing implementations are used, such as specific
      FPGA implementations.

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
possible to generate random JSON data using a companion project of Bolson named
[Illex](https://github.com/teratide/illex).

We will use Illex throughout this example.

The requirements for a JSON data source acting as a TCP are simple. When Bolson
connects, the source can start sending data, without any additional protocol,
with the exception that each JSON object must be terminated by the newline
character `'\n'`.

### Pulsar broker

If you do not have a Pulsar broker, it can easily be spawned locally using
Docker:

```bash
docker run -it --rm -p 6650:6650 -p 8080:8080 apachepulsar/pulsar bin/pulsar standalone
```

## Subcommands

Bolson knows two subcommands, `stream` and `bench`.

- **Stream**: Convert JSONs and publish them to Pulsar in a streaming fashion.
- **Bench**: Run micro-benchmarks of specific components of Bolson.

### Stream

### Bench
