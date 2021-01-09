library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.UtilInt_pkg.all;
use work.trip_report_util_pkg.all;

entity D2ListToVecs is
  generic (
      EPC                      : natural := 1;
      DATA_WIDTH               : natural := 8;
      LENGTH_WIDTH             : natural := 8
      );
  port (
      clk                      : in  std_logic;
      reset                    : in  std_logic;

      -- Input stream
      in_valid                 : in  std_logic;
      in_ready                 : out std_logic;
      in_data                  : in  std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
      in_last                  : in  std_logic_vector(1 downto 0) := (others => '0');
      in_count                 : in  std_logic_vector(log2ceil(EPC+1)-1 downto 0) := std_logic_vector(to_unsigned(1, log2ceil(EPC+1)));
      in_dvalid                : in  std_logic := '1';

      -- Element stream
      out_valid                : out std_logic;
      out_ready                : in  std_logic;
      out_data                 : out std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
      out_count                : out std_logic_vector(log2ceil(EPC+1)-1 downto 0);
      out_dvalid               : out std_logic;
      out_last                 : out std_logic;

      -- Length stream
      length_valid             : out std_logic;
      length_ready             : in  std_logic;
      length_data              : out std_logic_vector(LENGTH_WIDTH-1 downto 0);
      length_dvalid            : out std_logic;
      length_count             : out std_logic_vector(0 downto 0);
      length_last              : out std_logic
  );
end entity;


architecture Implementation of D2ListToVecs is
begin
    convert_proc : process (clk) is
      type input_holding_reg_type is record
        valid         : std_logic;
        dvalid        : std_logic;
        data          : std_logic_vector(DATA_WIDTH*EPC downto 0);
        count         : std_logic_vector(log2ceil(EPC)-1 downto 0);
        last          : std_logic_vector(1 downto 0);
      end record;
      variable i : input_holding_reg_type;
    
      type count_output_holding_reg_type is record
        valid  : std_logic;
        dvalid : std_logic;
        data   : std_logic_vector(LENGTH_WIDTH-1 downto 0);
        last   : std_logic;
      end record;
      variable oc : count_output_holding_reg_type;
    
      type element_output_holding_reg_type is record
        valid  : std_logic;
        dvalid : std_logic;
        data   : std_logic_vector(DATA_WIDTH*EPC downto 0);
        count  : std_logic_vector(log2ceil(EPC)-1 downto 0);
        last   : std_logic;
      end record;
      variable oe    : element_output_holding_reg_type;
    
      -- Current element count for length stream.
      variable count : unsigned(LENGTH_WIDTH-1 downto 0) := (others => '0');
    
    begin
      if rising_edge(clk) then
    
        if i.valid = '0' then
          i.valid         := in_valid;
          i.dvalid        := in_dvalid;
          i.count         := in_count;
          i.data          := in_data;
          i.last          := in_last;
        end if;
    
        if length_ready = '1' then
          oc.valid := '0';
        end if;
    
        if out_ready = '1' then
          oe.valid := '0';
        end if;
    
        if i.valid = '1' and oc.valid = '0' and oe.valid = '0' then
          oe.dvalid := '0';
          oe.last   := '0';
          oe.count  := (others => '0');
          oc.dvalid := '0';
          oc.last   := '0';
    
          -- If the data is valid, forward it and increment the element counter.
          if i.dvalid = '1' then
            oe.dvalid := '1';
            oe.data   := i.data;
            oe.count  := i.count;
            count     := count + unsigned(in_count);
          end if;
    
          -- If this is the end of the inner dimension, send last to the
          -- element stream and send the element count to the length stream.
          if i.last(0) = '1' then
            oc.dvalid := '1';
            oc.data   := std_logic_vector(count);
            count     := (others => '0');
          end if;
    
          -- If this is the last list, send last to the length
          -- stream.
          if i.last(1) = '1' then
            oe.last := '1';
            oc.last := '1';
          end if;
    
          oe.valid := i.dvalid or i.last(1);
          oc.valid := or_reduce(i.last);
    
          -- clear holding register
          i.valid  := '0';
    
        end if;
    
        if reset = '1' then
          i.valid  := '0';
          oc.valid := '0';
          oe.valid := '0';
          count    := (others => '0');
        end if;
    
        in_ready           <= not i.valid;
    
        length_valid       <= oc.valid;
        length_dvalid      <= oc.dvalid;
        length_last        <= oc.last;
        length_data        <= oc.data;
        length_count       <= "1";
    
        out_valid          <= oe.valid;
        out_dvalid         <= oe.dvalid;
        out_last           <= oe.last;
        out_data           <= oe.data;
        out_count          <= oe.count;
    
      end if;
  end process;
end architecture;
