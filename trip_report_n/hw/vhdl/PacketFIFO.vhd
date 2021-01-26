library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;
use work.Json_pkg.all;

entity PacketFIFO is
  generic (
      DATA_WIDTH            : natural;
      DEPTH                 : natural;
      PKT_COUNT_WIDTH       : natural := 8;
      DIMENSIONALITY        : natural := 1
      );
  port (
      clk                   : in  std_logic;
      reset                 : in  std_logic;

      in_valid              : in  std_logic;
      in_ready              : out std_logic;
      in_data               : in  std_logic_vector(DATA_WIDTH-1 downto 0);
      in_last               : in  std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
      in_strb               : in  std_logic := '1';

      out_valid             : out std_logic;
      out_ready             : in  std_logic;
      out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
      out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
      out_strb              : out std_logic := '1';

      packet_valid          : out std_logic;
      packet_ready          : in  std_logic;
      packet_count          : out std_logic_vector(PKT_COUNT_WIDTH-1 downto 0)

  );
end entity;

architecture implementation of PacketFIFO is

  -- Data bits, last bits, strb
  constant BUFF_WIDTH          : integer := DATA_WIDTH + DIMENSIONALITY + 1 ;

  signal buff_in_data          : std_logic_vector(BUFF_WIDTH-1 downto 0);
  signal buff_out_data         : std_logic_vector(BUFF_WIDTH-1 downto 0);

  begin

    -- Pack and unpack buffer data
    buff_in_data(DATA_WIDTH-1 downto 0)                           <= in_data;
    buff_in_data(DATA_WIDTH + DIMENSIONALITY-1 downto DATA_WIDTH) <= in_last;
    buff_in_data(DATA_WIDTH + DIMENSIONALITY)                     <= in_strb;

    out_data <= buff_out_data(DATA_WIDTH-1 downto 0);
    out_last <= buff_out_data(DATA_WIDTH + DIMENSIONALITY-1 downto DATA_WIDTH);
    out_strb <= buff_out_data(DATA_WIDTH + DIMENSIONALITY);

    buff_i: StreamBuffer
    generic map (
      DATA_WIDTH                => BUFF_WIDTH,
      MIN_DEPTH                 => DEPTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => in_valid,
      in_ready                  => in_ready,
      in_data                   => buff_in_data,
      out_valid                 => out_valid,
      out_ready                 => out_ready,
      out_data                  => buff_out_data
    );

    pkt_cntr_proc: process (clk) is
      variable cnt : unsigned(PKT_COUNT_WIDTH-1 downto 0) := (others => '0');
      variable ov  : std_logic := '0';
    begin 

      if rising_edge(clk) then

        -- Increase the packet counter when the last element of a sequence
        -- has been handshaked on the element FIFO input.
        if in_ready = '1' and in_valid = '1' and in_last(in_last'left) = '1' then
          cnt := cnt + 1;
        end if;
        
        -- Decrese the packet counter on packet count stream handshakes.
        if to_x01(ov) = '1' and packet_ready = '1' then
          cnt := cnt - 1;
        end if;
        
        -- When the counter is > 0 there's a full packet in the FIFO,
        -- so set the packet count stream output to valid.
        if cnt > 0 then
          ov := '1';
        else
          ov := '0';
        end if;
        packet_valid <= ov;
        packet_count <= std_logic_vector(cnt);
      end if;

      -- Handle reset.
      if reset = '1' then
        cnt := (others => '0');
        ov  := '0';
      end if;

    end process;
  end architecture;
