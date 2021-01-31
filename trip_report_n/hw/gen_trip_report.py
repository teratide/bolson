import os
import argparse
import pyarrow as pa
from pyfletchgen.lib import fletchgen

import vhdmmio

parser = argparse.ArgumentParser(description="Generate a kernel with N parser instances.")
parser.add_argument("parsers", type=int, help="Number of parser instances.")
args = parser.parse_args()

assert 16 >= args.parsers > 0

KERNEL_NAME = 'trip-report'

VHDMMIO_YAML = """
metadata:
  name: mmio
entity:
  bus-flatten:  yes
  bus-prefix:   mmio_
  clock-name:   kcd_clk
  reset-name:   kcd_reset
features:
  bus-width:    64
  optimize:     yes
interface:
  flatten:      yes
fields:
  - address: 0b0---
    name: AFU_DHF
    behavior: constant
    value: 17293826967149215744 # [63:60]: 1 && [40]: 1
  - address: 0b1---
    name: AFU_ID_L
    behavior: constant
    value: {afuidint} # check battery_status.json
  - address: 0b10---
    name: AFU_ID_H
    behavior: constant
    value: 11287216594519869704 # check battery_status.json
  - address: 0b11---
    name: DFH_RSVD0
    behavior: constant
    value: 0
  - address: 0b100---
    name: DFH_RSVD1
    behavior: constant
    value: 0
{fletchgen}
"""


def emphasize(s):
    """Like print(), but emphasizes the line using ANSI escape sequences."""
    print('\n\033[1m{}\033[0m'.format(s))


def input_schema(idx):
    return pa.schema([
        pa.field("input", pa.uint8(), False).with_metadata({b"fletcher_epc": b"8"})
    ]).with_metadata({
        b"fletcher_mode": b"read",
        b"fletcher_name": "input_{:02}".format(idx).encode('ascii')
    })


output_schema = pa.schema([pa.field("timestamp", pa.utf8(), False).with_metadata({
       b'fletcher_epc': b'1'
    }),
    pa.field("tag", pa.uint8(), False),
    pa.field("timezone", pa.uint64(), False),
    pa.field("vin", pa.uint64(), False),
    pa.field("odometer", pa.uint64(), False),
    pa.field("hypermiling", pa.uint8(), False),
    pa.field("avgspeed", pa.uint64(), False),
    pa.field("sec_in_band", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("miles_in_time_range", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("const_speed_miles_in_band", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("vary_speed_miles_in_band", pa.list_(pa.field("item",pa.uint64(), False)), False),
    pa.field("sec_decel", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("sec_accel", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("braking", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("accel", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("orientation", pa.uint8(), False),
    pa.field("small_speed_var", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("large_speed_var", pa.list_(pa.field("item", pa.uint64(), False)), False),
    pa.field("accel_decel", pa.uint64(), False),
    pa.field("speed_changes", pa.uint64(), False)

    ]).with_metadata({
       b'fletcher_mode': b'write',
       b'fletcher_name': b'output'
    })

def generate_schema_files(num_parsers):
    files = []
    file_out = "schemas/out_.as"
    pa.output_stream(file_out).write(output_schema.serialize())
    files.append(file_out)
    for i in range(0, num_parsers):
        file_in = "schemas/in_{:02}.as".format(i)
        schema_in = input_schema(i)
        pa.output_stream(file_in).write(schema_in.serialize())
        files.append(file_in)
    return files

emphasize("Generating schemas...")

# prepare output folder for schemas
if not os.path.exists('schemas'):
    os.makedirs('schemas')

schema_files = generate_schema_files(args.parsers)

# prepare registers
registers = [['c:32:parser_{:02}_tag'.format(i)]
             for i in range(0, args.parsers)]
registers = [item for sublist in registers for item in sublist]  # flatten list

emphasize("Running fletchgen...")

fletchgen(
    '-i', *schema_files,
    '-n', KERNEL_NAME,
    '--regs', *registers,
    '-l', 'vhdl',
    '--mmio64',
    '--mmio-offset', str(64),
    '--static-vhdl',
    '--axi'
)

emphasize("Generating kernel source...")
# TODO

emphasize("Re-running vhdmmio...")

base_afu_id = 0xb79b5c89b4ef5e00
base_afu_id += args.parsers

with open("fletchgen.mmio.yaml") as f:
    fletchgen_yaml_part = f.readlines()[18:]

vhdmmio_source = VHDMMIO_YAML.format(afuidint=base_afu_id, fletchgen=''.join(fletchgen_yaml_part))
vhdmmio_file = "{}.mmio.yml".format(KERNEL_NAME)

with open(vhdmmio_file, 'w') as f:
    f.write(vhdmmio_source)

vhdmmio.run_cli(['-V', 'vhdl', '-P', 'vhdl', vhdmmio_file])
