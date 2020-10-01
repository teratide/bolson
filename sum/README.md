# Build

Check the `README.md` in the `hw` and `sw` for build instructions.

# Simulation using OPAE ASE

Build the docker image using the `Dockerfile` in this repository:

```
docker build -t ias:1.2.1 - < Dockerfile
```

Start a container and mount the example directory:

```
docker run -it --rm --name ias -v `pwd`:/src:ro ias:1.2.1
```

Create the simulation environment:

```
afu_sim_setup -s hw/sources.txt /sim
cd /sim
make
```

Start the simulator:

```
make sim
```

Start another shell in your container:

```
docker exec -it ias bash
```

Build the host application:

```
mkdir -p /build
cd /build
cmake3 /src/sw
make
```

Check if the simulator is ready and run your host application:

```
export ASE_WORKDIR=/sim/work
./sum /src/hw/example.rb
```

The host application should output `-6`.

# Synthesis

Start a container and mount the example directory:

```
docker run -it --rm --name ias -v `pwd`:/src:ro ias:1.2.1
```

Create the synthesis environment and generate the bistream.

```
afu_synth_setup -s hw/sources.txt /synth
cd /synth
${OPAE_PLATFORM_ROOT}/bin/run.sh
```

Run the bistream through `PACSign`:

```

```

Copy the generated (unsigned) bitstream to your host system.

```
docker cp ias:/synth/sum_unsigned.gbs .
```

# Run on hardware

Make sure the intel-fpga driver is installed:

```
lsmod | grep fpga
```

If not please install the driver:

```
sudo yum -y localinstall https://github.com/OPAE/opae-sdk/releases/download/1.4.0-1/opae-intel-fpga-driver-2.0.4-2.x86_64.rpm
```

Start a privileged container to access the FPGA:

```
docker run -it --rm --privileged --name ias -v `pwd`:/src:ro ias:1.2.1
```

Flash the bitstream:

```
fpgaconf sum_unsigned.gbs
```

Build the host application (release build type):

```
mkdir -p /build
cd /build
cmake3 -DCMAKE_BUILD_TYPE=release /src/sw
make
```

Run the host application using the FPGA:

```
./sum /src/hw/example.rb
```
