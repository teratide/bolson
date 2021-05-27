# Battery Status JSON parser accelerator with N instances on Intel OPAE

This folder contains the sources for an accelerator for the "battery status" schema:
```
voltage: list<uint64>
```

# Generate sources

Run the generation script, inserting the desired number of parser instances `N`.
```bash
gen_battery_status.py N
```

Eight parser instances should be enough to saturate the practically achievable interface bandwidth of the Intel PAC,
which is a bit below 5 GB/s.

# Build FPGA image

Set up the development environment as explained in 
[the Fletcher OPAE docs](https://teratide.github.io/fletcher-opae/dev-env-setup.html).

Start the development container from `bolson/hardware`:

```bash
docker run -it --rm --name ias --net=host -v `pwd`:/src:ro ias:1.2.1
```

From the container, now navigate to the project hardware directory, initialize AFU synthesis in `/synth`, and run it.
```bash
cd opae/battery_status_n/hw
afu_synth_setup -s sources.txt /synth
cd /synth
${OPAE_PLATFORM_ROOT}/bin/run.sh
```
