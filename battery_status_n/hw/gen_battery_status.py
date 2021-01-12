import os
import argparse
import pyarrow as pa
from pyfletchgen.lib import fletchgen

import vhdmmio

parser = argparse.ArgumentParser(description="Generate a kernel with N parser instances.")
parser.add_argument("parsers", type=int, help="Number of parser instances.")
args = parser.parse_args()

assert 16 >= args.parsers > 0

KERNEL_NAME = 'battery_status'


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


def output_schema(idx):
    return pa.schema([
        pa.field("voltage", pa.list_(pa.field("item", pa.uint64(), False)), False)
    ]).with_metadata({
        b"fletcher_mode": b"write",
        b"fletcher_name": "output_{:02}".format(idx).encode('ascii')
    })


def generate_schema_files(num_parsers):
    files = []
    for i in range(0, num_parsers):
        file_in = "schemas/in_{:02}.as".format(i)
        file_out = "schemas/out_{:02}.as".format(i)
        schema_in = input_schema(i)
        schema_out = output_schema(i)
        pa.output_stream(file_in).write(schema_in.serialize())
        pa.output_stream(file_out).write(schema_out.serialize())
        files.append(file_in)
        files.append(file_out)

    return files


KERNEL_VHD = """library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

entity {kernel_name} is
  generic (
    INDEX_WIDTH : integer := 32;
    TAG_WIDTH   : integer := 1
  );
  port (
    kcd_clk                     : in std_logic;
    kcd_reset                   : in std_logic;
{ports}
    start                       : in std_logic;
    stop                        : in std_logic;
    reset                       : in std_logic;
    idle                        : out std_logic;
    busy                        : out std_logic;
    done                        : out std_logic;
    result                      : out std_logic_vector(63 downto 0)
  );
end entity;

architecture Implementation of {kernel_name} is
begin
{inst}
end architecture;
"""

SUBKERNEL_INST_VHD = """  {kernel_name}_{idx:02}_inst: entity work.{kernel_name}_sub
    generic map (
      INDEX_WIDTH => INDEX_WIDTH,
      TAG_WIDTH   => TAG_WIDTH
    )
    port map (
      kcd_clk                     => kcd_clk,
      kcd_reset                   => kcd_reset,
      input_input_valid           => input_{idx:02}_input_valid,
      input_input_ready           => input_{idx:02}_input_ready,
      input_input_dvalid          => input_{idx:02}_input_dvalid,
      input_input_last            => input_{idx:02}_input_last,
      input_input                 => input_{idx:02}_input,
      input_input_count           => input_{idx:02}_input_count,
      input_input_unl_valid       => input_{idx:02}_input_unl_valid,
      input_input_unl_ready       => input_{idx:02}_input_unl_ready,
      input_input_unl_tag         => input_{idx:02}_input_unl_tag,
      input_input_cmd_valid       => input_{idx:02}_input_cmd_valid,
      input_input_cmd_ready       => input_{idx:02}_input_cmd_ready,
      input_input_cmd_firstIdx    => input_{idx:02}_input_cmd_firstIdx,
      input_input_cmd_lastIdx     => input_{idx:02}_input_cmd_lastIdx,
      input_input_cmd_tag         => input_{idx:02}_input_cmd_tag,
      output_voltage_valid        => output_{idx:02}_voltage_valid,
      output_voltage_ready        => output_{idx:02}_voltage_ready,
      output_voltage_dvalid       => output_{idx:02}_voltage_dvalid,
      output_voltage_last         => output_{idx:02}_voltage_last,
      output_voltage_length       => output_{idx:02}_voltage_length,
      output_voltage_count        => output_{idx:02}_voltage_count,
      output_voltage_item_valid   => output_{idx:02}_voltage_item_valid,
      output_voltage_item_ready   => output_{idx:02}_voltage_item_ready,
      output_voltage_item_dvalid  => output_{idx:02}_voltage_item_dvalid,
      output_voltage_item_last    => output_{idx:02}_voltage_item_last,
      output_voltage_item         => output_{idx:02}_voltage_item,
      output_voltage_item_count   => output_{idx:02}_voltage_item_count,
      output_voltage_unl_valid    => output_{idx:02}_voltage_unl_valid,
      output_voltage_unl_ready    => output_{idx:02}_voltage_unl_ready,
      output_voltage_unl_tag      => output_{idx:02}_voltage_unl_tag,
      output_voltage_cmd_valid    => output_{idx:02}_voltage_cmd_valid,
      output_voltage_cmd_ready    => output_{idx:02}_voltage_cmd_ready,
      output_voltage_cmd_firstIdx => output_{idx:02}_voltage_cmd_firstIdx,
      output_voltage_cmd_lastIdx  => output_{idx:02}_voltage_cmd_lastIdx,
      output_voltage_cmd_tag      => output_{idx:02}_voltage_cmd_tag,
      start                       => parser_{idx:02}_control(0),
      stop                        => parser_{idx:02}_control(1),
      reset                       => parser_{idx:02}_control(2),
      idle                        => parser_{idx:02}_status(0),
      busy                        => parser_{idx:02}_status(1),
      done                        => parser_{idx:02}_status(2),
      result                      => parser_{idx:02}_rows,
      input_firstidx              => input_{idx:02}_firstidx,
      input_lastidx               => input_{idx:02}_lastidx,
      output_firstidx             => output_{idx:02}_firstidx,
      output_lastidx              => output_{idx:02}_lastidx
    );
"""

KERNEL_PORTS_VHD = """    input_{idx:02}_input_valid           : in std_logic;
    input_{idx:02}_input_ready           : out std_logic;
    input_{idx:02}_input_dvalid          : in std_logic;
    input_{idx:02}_input_last            : in std_logic;
    input_{idx:02}_input                 : in std_logic_vector(63 downto 0);
    input_{idx:02}_input_count           : in std_logic_vector(3 downto 0);
    input_{idx:02}_input_unl_valid       : in std_logic;
    input_{idx:02}_input_unl_ready       : out std_logic;
    input_{idx:02}_input_unl_tag         : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    input_{idx:02}_input_cmd_valid       : out std_logic;
    input_{idx:02}_input_cmd_ready       : in std_logic;
    input_{idx:02}_input_cmd_firstIdx    : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    input_{idx:02}_input_cmd_lastIdx     : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    input_{idx:02}_input_cmd_tag         : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_{idx:02}_voltage_valid        : out std_logic;
    output_{idx:02}_voltage_ready        : in std_logic;
    output_{idx:02}_voltage_dvalid       : out std_logic;
    output_{idx:02}_voltage_last         : out std_logic;
    output_{idx:02}_voltage_length       : out std_logic_vector(31 downto 0);
    output_{idx:02}_voltage_count        : out std_logic_vector(0 downto 0);
    output_{idx:02}_voltage_item_valid   : out std_logic;
    output_{idx:02}_voltage_item_ready   : in std_logic;
    output_{idx:02}_voltage_item_dvalid  : out std_logic;
    output_{idx:02}_voltage_item_last    : out std_logic;
    output_{idx:02}_voltage_item         : out std_logic_vector(63 downto 0);
    output_{idx:02}_voltage_item_count   : out std_logic_vector(0 downto 0);
    output_{idx:02}_voltage_unl_valid    : in std_logic;
    output_{idx:02}_voltage_unl_ready    : out std_logic;
    output_{idx:02}_voltage_unl_tag      : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_{idx:02}_voltage_cmd_valid    : out std_logic;
    output_{idx:02}_voltage_cmd_ready    : in std_logic;
    output_{idx:02}_voltage_cmd_firstIdx : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_{idx:02}_voltage_cmd_lastIdx  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_{idx:02}_voltage_cmd_tag      : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    input_{idx:02}_firstidx              : in std_logic_vector(31 downto 0);
    input_{idx:02}_lastidx               : in std_logic_vector(31 downto 0);
    output_{idx:02}_firstidx             : in std_logic_vector(31 downto 0);
    output_{idx:02}_lastidx              : in std_logic_vector(31 downto 0);
    parser_{idx:02}_control              : in std_logic_vector(31 downto 0);
    parser_{idx:02}_status               : out std_logic_vector(31 downto 0);
    parser_{idx:02}_rows                 : out std_logic_vector(63 downto 0);
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
        "name": "battery_status",
        "total-contexts": 1,
        "accelerator-type-uuid": "9ca43fb0-c340-4908-b79b-5c89b4ef5ee{n:1X}"
      }}
    ]
  }}
}}
"""

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

OPAE_SOURCES = """battery_status.json

vhdl/support/vhlib/util/UtilMisc_pkg.vhd
vhdl/support/vhlib/util/UtilInt_pkg.vhd
vhdl/support/vhlib/util/UtilConv_pkg.vhd
vhdl/support/vhlib/util/UtilStr_pkg.vhd
vhdl/support/vhlib/util/UtilRam1R1W.vhd
vhdl/support/vhlib/util/UtilRam_pkg.vhd

vhdl/support/vhlib/stream/Stream_pkg.vhd
vhdl/support/vhlib/stream/StreamPrefixSum.vhd
vhdl/support/vhlib/stream/StreamPipelineControl.vhd
vhdl/support/vhlib/stream/StreamPipelineBarrel.vhd
vhdl/support/vhlib/stream/StreamReshaper.vhd
vhdl/support/vhlib/stream/StreamElementCounter.vhd
vhdl/support/vhlib/stream/StreamGearbox.vhd
vhdl/support/vhlib/stream/StreamNormalizer.vhd
vhdl/support/vhlib/stream/StreamArb.vhd
vhdl/support/vhlib/stream/StreamGearboxSerializer.vhd
vhdl/support/vhlib/stream/StreamSync.vhd
vhdl/support/vhlib/stream/StreamGearboxParallelizer.vhd
vhdl/support/vhlib/stream/StreamSlice.vhd
vhdl/support/vhlib/stream/StreamFIFOCounter.vhd
vhdl/support/vhlib/stream/StreamFIFO.vhd
vhdl/support/vhlib/stream/StreamBuffer.vhd

vhdl/support/interconnect/Interconnect_pkg.vhd
vhdl/support/interconnect/BusWriteArbiterVec.vhd
vhdl/support/interconnect/BusReadArbiterVec.vhd
vhdl/support/interconnect/BusReadBuffer.vhd
vhdl/support/interconnect/BusWriteBuffer.vhd

vhdl/support/buffers/Buffer_pkg.vhd
vhdl/support/buffers/BufferReaderRespCtrl.vhd
vhdl/support/buffers/BufferReaderResp.vhd
vhdl/support/buffers/BufferReaderPost.vhd
vhdl/support/buffers/BufferReaderCmdGenBusReq.vhd
vhdl/support/buffers/BufferReaderCmd.vhd
vhdl/support/buffers/BufferReader.vhd
vhdl/support/buffers/BufferWriterPrePadder.vhd
vhdl/support/buffers/BufferWriterPreCmdGen.vhd
vhdl/support/buffers/BufferWriterPre.vhd
vhdl/support/buffers/BufferWriterCmdGenBusReq.vhd
vhdl/support/buffers/BufferWriter.vhd

vhdl/support/arrays/ArrayConfigParse_pkg.vhd
vhdl/support/arrays/ArrayConfig_pkg.vhd
vhdl/support/arrays/Array_pkg.vhd
vhdl/support/arrays/ArrayWriterListSync.vhd
vhdl/support/arrays/ArrayWriterListPrim.vhd
vhdl/support/arrays/ArrayWriterLevel.vhd
vhdl/support/arrays/ArrayWriterArb.vhd
vhdl/support/arrays/ArrayWriter.vhd
vhdl/support/arrays/ArrayReaderStruct.vhd
vhdl/support/arrays/ArrayReaderNull.vhd
vhdl/support/arrays/ArrayReaderListPrim.vhd
vhdl/support/arrays/ArrayReaderUnlockCombine.vhd
vhdl/support/arrays/ArrayReaderListSyncDecoder.vhd
vhdl/support/arrays/ArrayReaderListSync.vhd
vhdl/support/arrays/ArrayReaderList.vhd
vhdl/support/arrays/ArrayReaderLevel.vhd
vhdl/support/arrays/ArrayReaderArb.vhd
vhdl/support/arrays/ArrayReader.vhd
vhdl/support/arrays/ArrayCmdCtrlMerger.vhd

vhdl/support/arrow/Arrow_pkg.vhd

vhdl/support/axi/Axi_pkg.vhd
vhdl/support/axi/AxiReadConverter.vhd
vhdl/support/axi/AxiWriteConverter.vhd

vhdl/vhdmmio_pkg.gen.vhd
vhdl/mmio_pkg.gen.vhd
vhdl/mmio.gen.vhd

{rbws}
{rbrs}

vhdl/json/Json_pkg.vhd
vhdl/json/component/JsonRecordParser.vhd
vhdl/json/component/JsonArrayParser.vhd
vhdl/json/component/IntParser.vhd

vhdl/battery_status_sub.vhd
vhdl/battery_status.gen.vhd
vhdl/battery_status_Nucleus.gen.vhd
vhdl/battery_status_Mantle.gen.vhd

vhdl/AxiTop.vhd

opae/AxiWriteFenceGenerator.vhd
opae/OpaeAxiTop.vhd
opae/ofs_plat_afu.sv
"""

emphasize("Generating schemas...")

# prepare output folder for schemas
if not os.path.exists('schemas'):
    os.makedirs('schemas')

schema_files = generate_schema_files(args.parsers)

# prepare registers
registers = [['c:32:parser_{:02}_control'.format(i),
              's:32:parser_{:02}_status'.format(i),
              's:64:parser_{:02}_rows'.format(i)]
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
    '--static-vhdl'
)

emphasize("Generating kernel source...")

all_inst = "\n".join([SUBKERNEL_INST_VHD.format(idx=i,
                                                kernel_name=KERNEL_NAME)
                      for i in range(0, args.parsers)])
all_ports = "".join([KERNEL_PORTS_VHD.format(idx=i)
                     for i in range(0, args.parsers)])

kernel_source = KERNEL_VHD.format(n=args.parsers,
                                  kernel_name=KERNEL_NAME,
                                  ports=all_ports,
                                  inst=all_inst)

# Write the kernel source
kernel_file = 'vhdl/{}.gen.vhd'.format(KERNEL_NAME)
with open(kernel_file, 'w') as f:
    f.write(kernel_source)

print("Wrote kernel source to: " + kernel_file)

# Write the vhdmmio YAML

emphasize("Re-running vhdmmio...")

base_afu_id = 0xb79b5c89b4ef5ee0
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
rbrs = '\n'.join(["vhdl/battery_status_input_{:02}.gen.vhd".format(i) for i in range(0, args.parsers)])
rbws = '\n'.join(["vhdl/battery_status_output_{:02}.gen.vhd".format(i) for i in range(0, args.parsers)])

opae_sources_source = OPAE_SOURCES.format(rbrs=rbrs, rbws=rbws)
opae_sources_file = "sources.txt"

with open(opae_sources_file, 'w') as f:
    f.write(opae_sources_source)
