library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;

entity PacketFIFO is
  generic (
      DATA_WIDTH            : natural;
      DEPTH                 : natural;
      PKT_COUNT_WIDTH       : natural := 8;
      DIMENSIONALITY        : natural := 1;
      PKT_LAST              : natural := 1;
      TX_LAST               : natural := 1
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
      packet_count          : out std_logic_vector(PKT_COUNT_WIDTH-1 downto 0);
      packet_last           : out std_logic := '0'
  );
end entity;

architecture Implementation of PacketFIFO is

  -- Data bits, last bits, strb
  constant BUFF_WIDTH          : integer := DATA_WIDTH + DIMENSIONALITY + 1 ;

  signal pkt_buff_in_data      : std_logic_vector(BUFF_WIDTH-1 downto 0);
  signal pkt_buff_out_data     : std_logic_vector(BUFF_WIDTH-1 downto 0);

  signal tx_last_buff_in_data  : std_logic;
  signal tx_last_buff_out_data : std_logic;
  
  signal pkt_buff_valid        : std_logic;
  signal pkt_buff_ready        : std_logic;
  signal tx_last_buff_in_valid : std_logic;
  signal tx_last_buff_valid    : std_logic;
  signal tx_last_buff_ready    : std_logic;

  signal packet_valid_s        : std_logic;
  signal in_ready_s            : std_logic;

  begin

    -- Pack and unpack buffer data
    pkt_buff_in_data(DATA_WIDTH-1 downto 0)                           <= in_data;
    pkt_buff_in_data(DATA_WIDTH + DIMENSIONALITY-1 downto DATA_WIDTH) <= in_last;
    pkt_buff_in_data(DATA_WIDTH + DIMENSIONALITY)                     <= in_strb;

    out_data <= pkt_buff_out_data(DATA_WIDTH-1 downto 0);
    out_last <= pkt_buff_out_data(DATA_WIDTH + DIMENSIONALITY-1 downto DATA_WIDTH);
    out_strb <= pkt_buff_out_data(DATA_WIDTH + DIMENSIONALITY);

    tx_last_buff_in_data  <= in_last(TX_LAST);
    packet_last <= tx_last_buff_out_data;

    sync_in: StreamSync
      generic map (
        NUM_INPUTS              => 1,
        NUM_OUTPUTS             => 2
      )
      port map (
        clk                     => clk,
        reset                   => reset,

        in_valid(0)             => in_valid,
        in_ready(0)             => in_ready_s,

        out_valid(0)            => pkt_buff_valid,
        out_valid(1)            => tx_last_buff_in_valid,

        out_ready(0)            => pkt_buff_ready,
        out_ready(1)            => tx_last_buff_ready
    );

    pkt_buff_i: StreamBuffer
    generic map (
      DATA_WIDTH                => BUFF_WIDTH,
      MIN_DEPTH                 => DEPTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => pkt_buff_valid,
      in_ready                  => pkt_buff_ready,
      in_data                   => pkt_buff_in_data,
      out_valid                 => out_valid,
      out_ready                 => out_ready,
      out_data                  => pkt_buff_out_data
    );

    tx_last_buff_valid <= tx_last_buff_in_valid and (in_last(PKT_LAST) or in_last(TX_LAST));

    tx_last_buff_i: StreamBuffer
    generic map (
      DATA_WIDTH                => 1,
      MIN_DEPTH                 => DEPTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => tx_last_buff_valid,
      in_ready                  => tx_last_buff_ready,
      in_data(0)                => tx_last_buff_in_data,
      out_valid                 => packet_valid_s,
      out_ready                 => packet_ready,
      out_data(0)               => tx_last_buff_out_data
    );

    packet_valid <= packet_valid_s;
    in_ready <= in_ready_s;

    pkt_cntr_proc: process (clk) is
      variable cnt      : unsigned(PKT_COUNT_WIDTH-1 downto 0) := (others => '0');
      variable ov       : std_logic := '0';
      variable ol       : std_logic := '0';
      variable last_pkt : unsigned(PKT_COUNT_WIDTH-1 downto 0) := (others => '0');
    begin 

      if rising_edge(clk) then

        -- Increase the packet counter when the last element of a sequence
        -- has been handshaked on the element FIFO input.
        if in_ready_s = '1' and in_valid = '1' and in_last(PKT_LAST) = '1' then
          cnt := cnt + 1;
        end if;

        
        -- Decrese the packet counter on packet count stream handshakes.
        if packet_valid_s = '1' and packet_ready = '1' then
          if cnt > 0 then
            cnt := cnt - 1;
          end if;
        end if;
        packet_count <= std_logic_vector(cnt);
      end if;

      -- Handle reset.
      if reset = '1' then
        cnt      := (others => '0');
        ov       := '0';
      end if;

    end process;
  end architecture;
