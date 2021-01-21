library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.UtilInt_pkg.all;
use work.trip_report_util_pkg.all;

entity DropEmpty is
  generic (
    EPC                      : natural := 1;
    DATA_WIDTH               : natural := 8;
    DIMENSIONALITY           : natural := 1
    );
port (
    clk                      : in  std_logic;
    reset                    : in  std_logic;

    -- Input stream
    in_valid                 : in  std_logic;
    in_ready                 : out std_logic;
    in_data                  : in  std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
    in_dvalid                : in  std_logic := '1';
    in_last                  : in  std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');

    -- Output stream
    out_valid                : out std_logic;
    out_ready                : in  std_logic;
    out_data                 : out std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
    out_dvalid               : out std_logic := '1';
    out_last                 : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0')
);
end entity;


architecture Implementation of DropEmpty is
  begin
    clk_proc: process (clk) is
  
      type stream_type is record
        valid  : std_logic;
        data   : std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
        last   : std_logic_vector(DIMENSIONALITY-1 downto 0);
        dvalid : std_logic;
      end record;
  
      variable i : stream_type;
      variable o : stream_type;

      variable ir : std_logic := '0';

  
    begin
      if rising_edge(clk) then
  
        if to_x01(ir) = '1' then
          i.valid  := in_valid;
          i.data   := in_data;
          i.last   := in_last;
          i.dvalid := in_dvalid;
        end if;
  
        if to_x01(out_ready) = '1' then
          o.valid := '0';
        end if;

        if to_x01(i.valid) = '1' and to_x01(o.valid) /= '1' then
          o := i;
          if i.dvalid /= '1' and or_reduce(i.last) /= '1' then
            o.valid := '0';
          end if;
          i.valid := '0';
        end if;
  
        -- Handle reset.
        if to_x01(reset) /= '0' then
          ir      := '0';
          i.valid := '0';
          o.valid := '0';
        end if;
  
        -- Forward output holding register.
        ir         := not i.valid and not reset;
        in_ready   <= ir and not reset;
        out_valid  <= o.valid;
        out_data   <= o.data;
        out_last   <= o.last;
        out_dvalid <= o.dvalid;
      end if;
    end process;
end architecture;