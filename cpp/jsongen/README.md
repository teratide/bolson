# An incredibly simple random JSON generator.
Generates random JSON-formatted objects based on Arrow schemas.

## Install

### Requirements
- CMake 3.14+
- A C++17 compiler.

### Build
```bash
git clone https://github.com/teratide/flitter.git
cd flitter/cpp/jsongen
mkdir build && cd build
cmake ..
make
make install
```

### Usage

There are two subcommands, `file` and `stream`.

Both subcommand require an Arrow schema to be supplied as the first positional
argument, or through `-i` or `--input`.

Other than that:

| Option       | Description                          |
|--------------|--------------------------------------|
| `-pretty`    | to pretty-print JSON output.         |
| `--seed INT` | to set a seed for random generators. |
| `-s INT`     | same as `--seed`                     |

More detailed options can be found by running:
```
jsongen help <subcommand>
```

#### Usage for `file`
`todo`

#### Usage for `stream`
`todo`

### Example

After running [tripreport.py](examples/tripreport.py) in the examples folder,
you could run: 

```bash
./jsongen file tripreport.as -s 0 --pretty
```

Without supplying an output file with `-o`, the output is written to stdout.
This results in the following output:
```
{
    "timestamp": "2005-09-09T11:59:06-10:00",
    "timezone": 883,
    "vin": 16852243674679352615,
    "odometer": 997,
    "hypermiling": false,
    "avgspeed": 156,
    "sec_in_band": [3403, 893, 2225, 78, 162, 2332, 1473, 2587, 3446, 178, 997, 2403],
    "miles_in_time_range": [3376, 2553, 2146, 919, 2241, 1044, 1079, 3751, 1665, 2062, 46, 2868, 375, 3305, 4109, 3319, 627, 3523, 2225, 357, 1653, 2757, 3477, 3549],
    "const_speed_miles_in_band": [4175, 2541, 2841, 157, 2922, 651, 315, 2484, 2696, 165, 1366, 958],
    "vary_speed_miles_in_band": [2502, 155, 1516, 1208, 2229, 1850, 4032, 3225, 2704, 2064, 484, 3073],
    "sec_decel": [722, 2549, 547, 3468, 844, 3064, 2710, 1515, 763, 2972],
    "sec_accel": [2580, 3830, 792, 2407, 2425, 3305, 2985, 1920, 3889, 909],
    "braking": [2541, 13, 3533, 59, 116, 134],
    "accel": [1780, 228, 1267, 2389, 437, 871],
    "orientation": false,
    "small_speed_var": [1254, 3048, 377, 754, 1745, 3666, 2820, 3303, 2558, 1308, 2795, 941, 2049],
    "large_speed_var": [3702, 931, 2040, 3388, 2575, 881, 1821, 3675, 2080, 3973, 4132, 3965, 4166],
    "accel_decel": 1148,
    "speed_changes": 1932
}
```
