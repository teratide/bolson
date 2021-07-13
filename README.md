# Bolson

A JSON to Arrow IPC converter and Pulsar publishing tool.

- [Documentation](https://teratide.github.io/bolson/)
- [API Documentation](https://teratide.github.io/bolson/api/)

## Goal

### Functionality

- Bolson receives newline-separated JSONs over a TCP connection.
- The JSONs are converted to [Arrow](https://arrow.apache.org) RecordBatches.
- The Arrow RecordBatches are serialized to an Arrow IPC message.
- The IPC messages are published to a Pulsar broker.

### Performance

- The implementation aims to achieve high throughput and low latency.
- The implementation allows using FPGA accelerators for more performance.

## Build

To build Bolson, make sure your system adheres to the following requirements:

- Toolchain:
    - CMake 3.14+
    - A C++17 compiler.
- Dependencies:
    - [Arrow 3.0.0](https://github.com/apache/arrow)
        - When building from source, run `cmake` with `-DARROW_JSON=ON` and `-DARROW_COMPUTE=ON`.
    - [Pulsar 2.7.0](https://github.com/apache/pulsar)

Build Bolson as follows:

```bash
git clone https://github.com/teratide/bolson.git
cd bolson
mkdir build
cd build
cmake ..
make
```

## Usage

There are two subcommands, `stream` and `bench`.

More detailed options can be found by running:

```
bolson --help <subcommand>
```

## FPGA-accelerated parsing

To enable FPGA-accelerated parsing, continue to read [here](doc/src/fpga.md).

## FAQ

- Why is it called Bolson?
    - The name is inspired by the "Bolson Pupfish", which sounds a bit like "
      JSON publish". It's a working title.
