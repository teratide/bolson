library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;
use work.Json_pkg.all;

entity PacketArbiter is
  generic (
      DATA_WIDTH            : natural := 8;
      NUM_INPUTS            : natural;
      INDEX_WIDTH           : natural;
      DIMENSIONALITY        : natural := 1
      );
  port (
      clk                   : in  std_logic;
      reset                 : in  std_logic;

      in_valid              : in  std_logic_vector(NUM_INPUTS-1 downto 0);
      in_ready              : out std_logic_vector(NUM_INPUTS-1 downto 0);
      in_data               : in  std_logic_vector(DATA_WIDTH*NUM_INPUTS-1 downto 0);
      in_last               : in  std_logic_vector(NUM_INPUTS*DIMENSIONALITY-1 downto 0) := (others => '0');
      in_strb               : in  std_logic_vector(NUM_INPUTS-1 downto 0) := (others => '1');

      out_valid             : out std_logic;
      out_ready             : in  std_logic;
      out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
      out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
      out_strb              : out std_logic := '1';

      cmd_valid             : in  std_logic;
      cmd_ready             : out std_logic;
      cmd_index             : in  std_logic_vector(INDEX_WIDTH-1 downto 0)

  );
end entity;

architecture behavioral of PacketArbiter is

  signal out_last_s             : std_logic_vector(DIMENSIONALITY-1 downto 0);
  signal out_valid_s            : std_logic := '0';
  signal lock                   : std_logic := '0';
  signal index                  : std_logic_vector(INDEX_WIDTH-1 downto 0) := (others => '0');

  begin

    cmd_proc: process (clk) is
      variable cv     : std_logic := '0';
      variable cr     : std_logic := '0';
      variable lock_v : std_logic := '0';
    begin 

      if rising_edge(clk) then
        
        -- Latch index.
        if to_x01(cr) = '1' then 
          cv    := cmd_valid;
          index <= cmd_index;
        end if;

        -- Lock on new command.
        if to_x01(cv) = '1' then
          cv     := '0';
          lock_v := '1';
        end if;

        -- Unlock if we received the last transfer in the outermost dimension.
        if out_valid_s = '1' and out_ready = '1' then
          lock_v := not out_last_s(out_last_s'left);
        end if;

        -- Only be ready for a command when a previous one is not in progress.
        cr := not cv and not lock_v and not reset;
        cmd_ready <= cr;
        lock <= lock_v;

      end if;

      -- Handle reset.
      if reset = '1' then
        index  <= (others => '0');
        lock   <= '0';
      end if;

    end process;

    -- Output ready demux
    inp_mux_proc: process(in_data, in_last, in_strb, lock) is
        variable idx : integer range 0 to 2**INDEX_WIDTH-1;
    begin
        idx := to_integer(unsigned(index));
        out_data    <= in_data(DATA_WIDTH*(idx+1)-1 downto DATA_WIDTH*idx);
        out_last_s  <= in_last(DIMENSIONALITY*(idx+1) - 1  downto DIMENSIONALITY*idx);
        out_strb    <= in_strb(idx);

        -- Pass through transfers when we're in a locked state.
        if lock = '1' then
          out_valid_s <= in_valid(idx);
        else
          out_valid_s <= '0';
        end if;
    end process;

    -- Output ready demux
    rdy_demux_proc: process(out_ready, index, lock) is
      begin
      for idx in 0 to NUM_INPUTS-1 loop
        if idx = to_integer(unsigned(index)) and lock = '1' then
          in_ready(idx) <= out_ready;
        else
          in_ready(idx) <= '0';
        end if;
      end loop;
    end process;

    out_valid <= out_valid_s;
    out_last  <= out_last_s;
  end architecture;
