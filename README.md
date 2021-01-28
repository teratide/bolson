# Bolson

A JSON stream to Arrow IPC to Pulsar conversion and publish tool.

# Install

* Requirements
    * To build:
        - CMake 3.14+
        - A C++17 compiler.
    * Dependencies:
        - [Arrow 3.0.0](https://github.com/apache/arrow)
            - When building from source, run `cmake` with `-DARROW_JSON ON`.
        - [Pulsar 2.7.0](https://github.com/apache/pulsar)

## Build & run

```bash
git clone https://github.com/teratide/bolson.git
cd bolson
mkdir build
cd build
cmake ..
make
./bolson
```

# Usage

There are two subcommands, `file` and `stream`.

Both subcommand require an Arrow schema to be supplied as the first positional
argument, or through `-i` or `--input`.

More detailed options can be found by running:

```
bolson --help <subcommand>
```

The name is inspired by the "Bolson Pupfish", which sounds a bit like "JSON
publish".