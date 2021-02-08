# FPGA accelerated parsing

By default, Bolson parses and converts JSONs using Arrow's built-in JSON parser.
Bolson can also run with FPGA-accelerated parsing enabled for specific, hard
coded schemas.

## Fletcher/Intel OPAE

### Prerequisites

* A system set up according to the setup instructions for [Fletcher OPAE].
* The correct bitstream for a specific Arrow schema & parser implementation.

### Flash the bitstream

Make sure to first flash the bitstream. From the [Fletcher OPAE] guide:

Start the FPGA development environment container for the Intel Acceleration
Stack (IAS).

```bash
cd path/to/bitstream
docker run -it --rm --privileged -v `pwd`:/src:ro ias:1.2.1
```

From the IAS container, program the FPGA with the bitstream and exit the
container:

```bash
fpgaconf bitstream.gbs
exit
```

### Enable huge pages

The current implementation of Fletcher OPAE based accelerators requires huge
pages to be enabled.

On a CentOS system, they can be enabled by root users as follows:

```bash
sudo su
echo 32 | tee /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages
exit
```

### Run Bolson with FPGA-accelerated parser implementation

Run Bolson with the `-p` or `--parser` option followed by the name of the
implementation of to select which FPGA-accelerated parser implementation to use
for the respective subcommand (`stream` or `bench convert`).

To see which implementations are available, run Bolson
with `<subcommand> --help`.

Example:

```bash
./bolson bench convert path/to/schema.as -p opae-battery --threads 8
```

[Fletcher OPAE]: https://teratide.github.io/fletcher-opae