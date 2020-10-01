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

# Run on hardware
