import os
import argparse
import pyarrow as pa
from pyfletchgen.lib import fletchgen

parser = argparse.ArgumentParser(description="Generate a kernel with N parser instances.")
parser.add_argument("parsers", type=int, help="Number of parser instances.")
args = parser.parse_args()

assert 32 >= args.parsers > 0

kernel_name = 'battery_status'


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

fletchgen(
    '-i', *schema_files,
    '-n', kernel_name,
    '--regs', *registers,
    '-l', 'vhdl'
)

KERNEL_VHD = """library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

library work;
use work.battery_status_pkg.all;
use work.Stream_pkg.all;

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
    result                      : out std_logic_vector(63 downto 0);
    ext_platform_complete_req   : out std_logic;
    ext_platform_complete_ack   : in std_logic
  );
end entity;

architecture Implementation of {kernel_name} is
  signal int_platform_complete_req : std_logic_vector({n}-1 downto 0);
  signal int_platform_complete_ack : std_logic_vector({n}-1 downto 0); 
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
      output_lastidx              => output_{idx:02}_lastidx,
      ext_platform_complete_req   => int_platform_complete_req({idx}),
      ext_platform_complete_ack   => int_platform_complete_ack({idx})
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

all_inst = "\n".join([SUBKERNEL_INST_VHD.format(idx=i,
                                                kernel_name=kernel_name)
                      for i in range(0, args.parsers)])
all_ports = "".join([KERNEL_PORTS_VHD.format(idx=i)
                     for i in range(0, args.parsers)])

kernel_source = KERNEL_VHD.format(n=args.parsers,
                                  kernel_name=kernel_name,
                                  ports=all_ports,
                                  inst=all_inst)

# Write the kernel source
with open('vhdl/{}.gen.vhd'.format(kernel_name), 'w') as f:
    f.write(kernel_source)
