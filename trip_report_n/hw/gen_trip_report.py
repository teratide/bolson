import os
import argparse
from string import Template

import pyarrow as pa
from pyfletchgen.lib import fletchgen

import vhdmmio

parser = argparse.ArgumentParser(description="Generate a kernel with N parser instances.")
parser.add_argument("parsers", type=int, help="Number of parser instances.")
args = parser.parse_args()

assert 16 >= args.parsers > 0

parsers = args.parsers

KERNEL_NAME = 'trip_report'

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
    value: {afuidint} # check trip-report.json
  - address: 0b10---
    name: AFU_ID_H
    behavior: constant
    value: 11287216594519869704 # check trip_report.json
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

OPAE_JSON = """{{
  "version": 1,
  "afu-image": {{
    "power": 0,
    "clock-frequency-high": "auto",
    "clock-frequency-low": "auto",
    "afu-top-interface": {{
      "name": "ofs_plat_afu"
    }},
    "accelerator-clusters": [
      {{
        "name": "trip-report",
        "total-contexts": 1,
        "accelerator-type-uuid": "9ca43fb0-c340-4908-b79b-5c89b4ef5e{n:02X}"
      }}
    ]
  }}
}}
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


hw_output_schema = pa.schema([pa.field("timestamp", pa.utf8(), False).with_metadata({
    b'fletcher_epc': b'1'
}),
    pa.field("tag", pa.uint64(), False),
    pa.field("timezone", pa.uint64(), False),
    pa.field("vin", pa.uint64(), False),
    pa.field("odometer", pa.uint64(), False),
    pa.field("hypermiling", pa.uint8(), False),
    pa.field("avgspeed", pa.uint64(), False),
    pa.field("sec_in_band", pa.uint64(), False),
    pa.field("miles_in_time_range", pa.uint64(), False),
    pa.field("const_speed_miles_in_band", pa.uint64(), False),
    pa.field("vary_speed_miles_in_band", pa.uint64(), False),
    pa.field("sec_decel", pa.uint64(), False),
    pa.field("sec_accel", pa.uint64(), False),
    pa.field("braking", pa.uint64(), False),
    pa.field("accel", pa.uint64(), False),
    pa.field("orientation", pa.uint8(), False),
    pa.field("small_speed_var", pa.uint64(), False),
    pa.field("large_speed_var", pa.uint64(), False),
    pa.field("accel_decel", pa.uint64(), False),
    pa.field("speed_changes", pa.uint64(), False)

]).with_metadata({
    b'fletcher_mode': b'write',
    b'fletcher_name': b'output'
})

sw_output_schema = pa.schema([pa.field("timestamp", pa.utf8(), False).with_metadata({
    b'fletcher_epc': b'1'
    }),
    pa.field("tag", pa.uint64(), False),
    pa.field("timezone", pa.uint64(), False),
    pa.field("vin", pa.uint64(), False),
    pa.field("odometer", pa.uint64(), False),
    pa.field("hypermiling", pa.uint8(), False),
    pa.field("avgspeed", pa.uint64(), False),
    pa.field("sec_in_band", pa.list_(pa.field("item", pa.uint64(), False), 12), False),
    pa.field("miles_in_time_range", pa.list_(pa.field("item", pa.uint64(), False), 24), False),
    pa.field("const_speed_miles_in_band", pa.list_(pa.field("item", pa.uint64(), False), 12), False),
    pa.field("vary_speed_miles_in_band", pa.list_(pa.field("item",pa.uint64(), False), 12), False),
    pa.field("sec_decel", pa.list_(pa.field("item", pa.uint64(), False), 10), False),
    pa.field("sec_accel", pa.list_(pa.field("item", pa.uint64(), False), 10), False),
    pa.field("braking", pa.list_(pa.field("item", pa.uint64(), False), 6), False),
    pa.field("accel", pa.list_(pa.field("item", pa.uint64(), False), 6), False),
    pa.field("orientation", pa.uint8(), False),
    pa.field("small_speed_var", pa.list_(pa.field("item", pa.uint64(), False), 13), False),
    pa.field("large_speed_var", pa.list_(pa.field("item", pa.uint64(), False), 13), False),
    pa.field("accel_decel", pa.uint64(), False),
    pa.field("speed_changes", pa.uint64(), False)

    ]).with_metadata({
    b'fletcher_mode': b'write',
    b'fletcher_name': b'output'
    })


def generate_schema_files(num_parsers):
    files = []
    file_out_hw = "schemas/out_.as"
    pa.output_stream(file_out_hw).write(hw_output_schema.serialize())
    files.append(file_out_hw)
    file_out_sw = "schemas/out_sw.as"
    pa.output_stream(file_out_sw).write(sw_output_schema.serialize())
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
registers = [['c:32:parser_{:02}_tag'.format(i),
              's:32:parser_{:02}_consumed_bytes'.format(i)]
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

INPUT_PORTS = ""
for i in range(0, args.parsers):
    INPUT_PORTS = INPUT_PORTS + """\
    input_{idx:02}_input_valid                          : in std_logic;
    input_{idx:02}_input_ready                          : out std_logic;
    input_{idx:02}_input_dvalid                         : in std_logic;
    input_{idx:02}_input_last                           : in std_logic;
    input_{idx:02}_input                                : in std_logic_vector(63 downto 0);
    input_{idx:02}_input_count                          : in std_logic_vector(3 downto 0);
    input_{idx:02}_input_unl_valid                      : in std_logic;
    input_{idx:02}_input_unl_ready                      : out std_logic;
    input_{idx:02}_input_unl_tag                        : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    input_{idx:02}_input_cmd_valid                      : out std_logic;
    input_{idx:02}_input_cmd_ready                      : in std_logic;
    input_{idx:02}_input_cmd_firstIdx                   : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    input_{idx:02}_input_cmd_lastIdx                    : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    input_{idx:02}_input_cmd_tag                        : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    input_{idx:02}_firstidx                             : in  std_logic_vector(31 downto 0);
    input_{idx:02}_lastidx                              : in  std_logic_vector(31 downto 0);
    
""".format(idx=i)

STRB_SENSITIVITY_LIST=""
for i in range(0, args.parsers):
    if i != args.parsers-1:
        STRB_SENSITIVITY_LIST = STRB_SENSITIVITY_LIST + """\
        input_{idx:02}_input_dvalid,
        input_{idx:02}_input_count,
""".format(idx=i)
    else:
        STRB_SENSITIVITY_LIST = STRB_SENSITIVITY_LIST + """\
        input_{idx:02}_input_dvalid,
        input_{idx:02}_input_count
""".format(idx=i)

TYDI_STRB = """\
tydi_strb: process (
{strb_sensitivity_list})
    begin
""".format(strb_sensitivity_list=STRB_SENSITIVITY_LIST)
for i in range(0, args.parsers):
    TYDI_STRB = TYDI_STRB + """\
      for i in EPC-1 downto 0 loop
        if unsigned(input_{idx:02}_input_count) = 0 or i < unsigned(input_{idx:02}_input_count) then
          in_strb(EPC*{idx} + i) <= input_{idx:02}_input_dvalid;
        else
          in_strb(EPC*{idx} + i) <= '0';
        end if;
      end loop;
""".format(idx=i)
TYDI_STRB = TYDI_STRB + """\
end process;
"""


LAST_SENSITIVITY_LIST=""
for i in range(0, args.parsers):
    if i != args.parsers-1:
        LAST_SENSITIVITY_LIST = LAST_SENSITIVITY_LIST + """\
        input_{idx:02}_input_last,
""".format(idx=i)
    else:
        LAST_SENSITIVITY_LIST = LAST_SENSITIVITY_LIST + """\
        input_{idx:02}_input_last
""".format(idx=i)

TYDI_LAST = """\
tydi_last: process (
{last_sensitivity_list})
    begin
    -- all records are currently sent in one transfer, so there's no difference
    -- between the two dimensions going into the parser.
""".format(last_sensitivity_list=LAST_SENSITIVITY_LIST)

for i in range(0, args.parsers):
    TYDI_LAST = TYDI_LAST + """\
    in_last(EPC * 2 * ({idx}+1)-1 downto EPC * 2 * {idx}) <= (others => '0');
    in_last(EPC * 2 * ({idx}+1) - 2) <= input_{idx:02}_input_last;
    in_last(EPC * 2 * ({idx}+1) - 1) <= input_{idx:02}_input_last;
    
""".format(idx=i)
TYDI_LAST = TYDI_LAST + """\
end process;
"""

SYNC_IN_UNL_MAP = ""
for i in range(0, args.parsers):
    SYNC_IN_UNL_MAP = SYNC_IN_UNL_MAP + """\
      in_valid({idx})             => input_{idx:02}_input_unl_valid,
""".format(idx=i)
for i in range(0, args.parsers):
    if i != args.parsers - 1:
        SYNC_IN_UNL_MAP = SYNC_IN_UNL_MAP + """\
      in_ready({idx})             => input_{idx:02}_input_unl_ready,
""".format(idx=i)
    else:
        SYNC_IN_UNL_MAP = SYNC_IN_UNL_MAP + """\
      in_ready({idx})             => input_{idx:02}_input_unl_ready
""".format(idx=i)

SYNC_IN_UNL = """\
sync_in_unl: StreamSync
    generic map (
      NUM_INPUTS              => {num_parsers},
      NUM_OUTPUTS             => 1
    )
    port map (
      clk                     => kcd_clk,
      reset                   => kcd_reset,
      
      out_valid(0)            => in_unl_valid,
      out_ready(0)            => in_unl_ready,  
{sync_in_unl_map}  
  );
""".format(num_parsers=args.parsers, sync_in_unl_map=SYNC_IN_UNL_MAP)

SYNC_IN_CMD_MAP = ""
for i in range(0, args.parsers):
    SYNC_IN_CMD_MAP = SYNC_IN_CMD_MAP + """\
      out_valid({idx})            => input_{idx:02}_input_cmd_valid,
""".format(idx=i)
for i in range(0, args.parsers):
    if i != args.parsers - 1:
        SYNC_IN_CMD_MAP = SYNC_IN_CMD_MAP + """\
      out_ready({idx})            => input_{idx:02}_input_cmd_ready,
""".format(idx=i)
    else:
        SYNC_IN_CMD_MAP = SYNC_IN_CMD_MAP + """\
      out_ready({idx})            => input_{idx:02}_input_cmd_ready
""".format(idx=i)

SYNC_IN_CMD = """\
sync_in_cmd: StreamSync
    generic map (
      NUM_INPUTS              => 1,
      NUM_OUTPUTS             => {num_parsers}
    )
    port map (
      clk                     => kcd_clk,
      reset                   => kcd_reset,
      
      in_valid(0)             => in_cmd_valid,
      in_ready(0)             => in_cmd_ready,
{sync_in_cmd_map}  
  );
""".format(num_parsers=args.parsers, sync_in_cmd_map=SYNC_IN_CMD_MAP)

READ_REQ_DEFAULTS = ""
for i in range(0, args.parsers):
    READ_REQ_DEFAULTS = READ_REQ_DEFAULTS + """\
  input_{idx:02}_input_cmd_firstIdx                         <= input_{idx:02}_firstidx;
  input_{idx:02}_input_cmd_lastIdx                          <= input_{idx:02}_lastidx;
  input_{idx:02}_input_cmd_tag                              <= (others => '0');
    
""".format(idx=i)

INST_IN_VALID = ""
for i in range(0, args.parsers):
    INST_IN_VALID = INST_IN_VALID + """\
    in_valid({idx})                                 => input_{idx:02}_input_valid,
""".format(idx=i)

INST_IN_READY = ""
for i in range(0, args.parsers):
    INST_IN_READY = INST_IN_READY + """\
    in_ready({idx})                                 => input_{idx:02}_input_ready_s,
""".format(idx=i)

INST_IN_DATA= ""
for i in range(0, args.parsers):
    INST_IN_DATA = INST_IN_DATA + """\
    in_data(8*EPC*({idx}+1)-1 downto 8*EPC*{idx})       => input_{idx:02}_input,
""".format(idx=i)

TAG_CFG= ""
for i in range(0, args.parsers):
    if i != args.parsers-1:
        TAG_CFG = TAG_CFG + """\
    tag_CFG(32*({idx}+1)-1 downto 32*{idx})             => parser_{idx:02}_tag,
""".format(idx=i)
    else:
        TAG_CFG = TAG_CFG + """\
    tag_CFG(32*({idx}+1)-1 downto 32*{idx})             => parser_{idx:02}_tag
""".format(idx=i)

MMIO=""
for i in range(0, args.parsers):
    MMIO = MMIO + """\
    parser_{idx:02}_tag                                 : in  std_logic_vector(31 downto 0);
    parser_{idx:02}_consumed_bytes                      : out std_logic_vector(31 downto 0);
""".format(idx=i)

INPUT_READY_COPIES=""
for i in range(0, args.parsers):
    INPUT_READY_COPIES = INPUT_READY_COPIES + """\
  signal input_{idx:02}_input_ready_s                 : std_logic;
""".format(idx=i)

INPUT_READY_ASSIGNMENTS=""
for i in range(0, args.parsers):
    INPUT_READY_ASSIGNMENTS = INPUT_READY_ASSIGNMENTS + """\
  input_{idx:02}_input_ready  <= input_{idx:02}_input_ready_s;
""".format(idx=i)


BYTE_COUNTER_SIGNALS=""
for i in range(0, args.parsers):
    BYTE_COUNTER_SIGNALS = BYTE_COUNTER_SIGNALS + """\
  signal byte_count_{idx:02}                    : unsigned(31 downto 0);
""".format(idx=i)

BYTE_COUNTERS=""
for i in range(0, args.parsers):
    BYTE_COUNTERS = BYTE_COUNTERS + """\
  byte_counter_{idx:02}: process (kcd_clk)
    is
  begin
    if rising_edge(kcd_clk) then
      if input_{idx:02}_input_valid = '1' and input_{idx:02}_input_ready_s = '1' then
        byte_count_{idx:02} <= byte_count_{idx:02} + unsigned(input_{idx:02}_input_count);
      end if;
      if kcd_reset = '1' or reset = '1' then
        byte_count_{idx:02} <= (others => '0');
      end if;
    end if;
  end process;
  parser_{idx:02}_consumed_bytes <= std_logic_vector(byte_count_{idx:02});
""".format(idx=i)

emphasize("Generating kernel source...")
template = open('template/kernel.tmpl')
# src = Template(template.read())
kernel = template.read().format(input_ports=INPUT_PORTS, mmio=MMIO, tydi_strb=TYDI_STRB, tydi_last=TYDI_LAST,
                                read_req_defaults=READ_REQ_DEFAULTS, sync_in_unl=SYNC_IN_UNL, sync_in_cmd=SYNC_IN_CMD,
                                in_valid=INST_IN_VALID, in_ready=INST_IN_READY, in_data=INST_IN_DATA, tag_cfg=TAG_CFG,
                                num_parsers=args.parsers, byte_counter_signals=BYTE_COUNTER_SIGNALS, byte_counters=BYTE_COUNTERS,
                                input_ready_copies=INPUT_READY_COPIES, input_ready_assignments=INPUT_READY_ASSIGNMENTS)
# Write the kernel source
kernel_file = 'vhdl/{}.gen.vhd'.format(KERNEL_NAME)
with open(kernel_file, 'w') as f:
    f.write(kernel)

print("Wrote kernel source to: " + kernel_file)

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

emphasize("Generating OPAE JSON...")
opae_json_file = "{}.json".format(KERNEL_NAME)
opae_json_source = OPAE_JSON.format(n=args.parsers)

with open(opae_json_file, 'w') as f:
    f.write(opae_json_source)

emphasize("Generating OPAE source list ...")

# Create list of recordbatch readers and writers
rbrs = '\n'.join(["vhdl/trip_report_input_{:02}.gen.vhd".format(i) for i in range(0, args.parsers)])
rbws = "\nvhdl/trip_report_output.gen.vhd"

template = open('template/sources.tmpl')
opae_json_file = "{}.json".format(KERNEL_NAME)

opae_sources_source = template.read().format(rbrs=rbrs, rbws=rbws)
opae_sources_file = "sources.txt"

with open(opae_sources_file, 'w') as f:
    f.write(opae_sources_source)


