library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

library work;
use work.TestCase_pkg.all;
use work.Stream_pkg.all;
use work.ClockGen_pkg.all;
use work.StreamSource_pkg.all;
use work.StreamSink_pkg.all;
use work.UtilInt_pkg.all;
use work.trip_report_util_pkg.all;
use work.TestCase_pkg.all;

entity ArbiterController_tc is
end ArbiterController_tc;

architecture test_case of ArbiterController_tc is

  signal clk                    : std_logic;
  signal reset                  : std_logic;
      
  constant INDEX_WIDTH          : integer := 2;
  constant TAG_WIDTH            : integer := 2;
  constant DEPTH                : integer := 20;
  constant PKT_COUNT_WIDTH      : integer := 2;
      
  signal in_a_valid             : std_logic;
  signal in_a_ready             : std_logic;
  signal in_a_strb              : std_logic;
  signal in_a_last              : std_logic;
  signal in_a_data              : std_logic_vector(7 downto 0);
      
  signal in_b_valid             : std_logic;
  signal in_b_ready             : std_logic;
  signal in_b_strb              : std_logic;
  signal in_b_last              : std_logic;
  signal in_b_data              : std_logic_vector(7 downto 0);
      
  signal cmd_valid              : std_logic;
  signal cmd_ready              : std_logic;
  signal cmd_index              : std_logic_vector(INDEX_WIDTH-1 downto 0);
      
  signal a_packet_valid         : std_logic;
  signal a_packet_ready         : std_logic;
      
  signal b_packet_valid         : std_logic;
  signal b_packet_ready         : std_logic;
      
  signal arb_ready              : std_logic;
  signal arb_valid              : std_logic;
  signal arb_data               : std_logic_vector(7 downto 0);
  signal arb_last               : std_logic;
  signal arb_strb               : std_logic;
      
  signal out_ready              : std_logic;
  signal out_valid              : std_logic;
  signal out_data               : std_logic_vector(7 downto 0);
  signal out_last               : std_logic;
  signal out_strb               : std_logic;
      
  signal fifo_a_ready           : std_logic;
  signal fifo_a_valid           : std_logic;
  signal fifo_a_data            : std_logic_vector(7 downto 0);
  signal fifo_a_last            : std_logic;
  signal fifo_a_strb            : std_logic;
      
  signal fifo_b_ready           : std_logic;
  signal fifo_b_valid           : std_logic;
  signal fifo_b_data            : std_logic_vector(7 downto 0);
  signal fifo_b_last            : std_logic;
  signal fifo_b_strb            : std_logic;
      
  signal tag_valid              : std_logic;
  signal tag_ready              : std_logic;
  signal tag                    : std_logic_vector(TAG_WIDTH-1 downto 0);


begin

  clkgen: ClockGen_mdl
    port map (
      clk                       => clk,
      reset                     => reset
    );

  src_a_inst: StreamSource_mdl
    generic map (
      NAME                      => "src_a",
      ELEMENT_WIDTH             => 8,
      COUNT_MAX                 => 1,
      COUNT_WIDTH               => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      valid                     => fifo_a_valid,
      ready                     => fifo_a_ready,
      dvalid                    => fifo_a_strb,
      last                      => fifo_a_last,
      data                      => fifo_a_data
    );

    src_b_inst: StreamSource_mdl
    generic map (
      NAME                      => "src_b",
      ELEMENT_WIDTH             => 8,
      COUNT_MAX                 => 1,
      COUNT_WIDTH               => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      valid                     => fifo_b_valid,
      ready                     => fifo_b_ready,
      dvalid                    => fifo_b_strb,
      last                      => fifo_b_last,
      data                      => fifo_b_data
    );

    dut: ArbiterController
    generic map (
      NUM_INPUTS                => 2,
      INDEX_WIDTH               => INDEX_WIDTH,
      TAG_WIDTH                 => TAG_WIDTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      pkt_valid(0)              => a_packet_valid,
      pkt_valid(1)              => b_packet_valid,

      pkt_ready(0)              => a_packet_ready,
      pkt_ready(1)              => b_packet_ready,

      cmd_valid                 => cmd_valid,
      cmd_ready                 => cmd_ready,
      cmd_index                 => cmd_index,

      tag_valid                 => tag_valid,
      tag_ready                 => tag_ready,
      tag                       => tag,

      tag_cfg                   => "0100"
    );
    
    arb: PacketArbiter
    generic map (
      NUM_INPUTS                => 2,
      INDEX_WIDTH               => INDEX_WIDTH,
      DATA_WIDTH                => 8,
      DIMENSIONALITY            => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      in_valid(0)               => in_a_valid,
      in_valid(1)               => in_b_valid,
      in_ready(0)               => in_a_ready,
      in_ready(1)               => in_b_ready,
      in_data(7 downto 0)       => in_a_data,
      in_data(15 downto 8)      => in_b_data,
      in_last(0)                => in_a_last,
      in_last(1)                => in_b_last,
      in_strb(0)                => in_a_strb,
      in_strb(1)                => in_b_strb,

      out_data                  => arb_data,
      out_valid                 => arb_valid,
      out_ready                 => arb_ready,
      out_strb                  => arb_strb,
      out_last(0)               => arb_last,

      cmd_valid                 => cmd_valid,
      cmd_ready                 => cmd_ready,
      cmd_index                 => cmd_index
    );

    pkt_fifo_a: PacketFIFO
    generic map (
      DEPTH                     => DEPTH,
      PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
      DATA_WIDTH                => 8,
      DIMENSIONALITY            => 1,
      PKT_LAST                  => 0,
      TX_LAST                   => 0
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      in_valid                  => fifo_a_valid,
      in_ready                  => fifo_a_ready,
      in_data                   => fifo_a_data,
      in_last(0)                => fifo_a_last,
      in_strb                   => fifo_a_strb,

      out_data                  => in_a_data,
      out_valid                 => in_a_valid,
      out_ready                 => in_a_ready,
      out_strb                  => in_a_strb,
      out_last(0)               => in_a_last,

      packet_valid              => a_packet_valid,
      packet_ready              => a_packet_ready
    );

    pkt_fifo_b: PacketFIFO
    generic map (
      DEPTH                     => DEPTH,
      PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
      DATA_WIDTH                => 8,
      DIMENSIONALITY            => 1,
      PKT_LAST                  => 0,
      TX_LAST                   => 0
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      in_valid                  => fifo_b_valid,
      in_ready                  => fifo_b_ready,
      in_data                   => fifo_b_data,
      in_last(0)                => fifo_b_last,
      in_strb                   => fifo_b_strb,

      out_data                  => in_b_data,
      out_valid                 => in_b_valid,
      out_ready                 => in_b_ready,
      out_strb                  => in_b_strb,
      out_last(0)               => in_b_last,

      packet_valid              => b_packet_valid,
      packet_ready              => b_packet_ready
    );

    out_sink_inst: StreamSink_mdl
    generic map (
      NAME                      => "out_sink",
      ELEMENT_WIDTH             => 8,
      COUNT_MAX                 => 1,
      COUNT_WIDTH               => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      valid                     => arb_valid,
      ready                     => arb_ready,
      data                      => arb_data,
      dvalid                    => arb_strb,
      last                      => arb_last
    );

    tag_sink_inst: StreamSink_mdl
    generic map (
      NAME                      => "tag_sink",
      ELEMENT_WIDTH             => TAG_WIDTH,
      COUNT_MAX                 => 1,
      COUNT_WIDTH               => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      valid                     => tag_valid,
      ready                     => tag_ready,
      data                      => tag,
      dvalid                    => '1'
    );
    

  random_tc: process is
    variable src_a        : streamsource_type;
    variable src_b        : streamsource_type;
    variable src_cmd      : streamsource_type;
    variable out_sink     : streamsink_type;
    variable tag_sink     : streamsink_type;

  begin
    tc_open("StreamSerializer", "test");
    src_a.initialize("src_a");
    src_b.initialize("src_b");
    out_sink.initialize("out_sink");
    tag_sink.initialize("tag_sink");

    src_a.push_str("SRCA");
    src_a.transmit;

    src_b.push_str("SRCB");
    src_b.transmit;


    out_sink.unblock;
    tag_sink.unblock;

    tc_wait_for(20 us);

    tc_check(out_sink.pq_ready, true);
    tc_check(out_sink.pq_get_str, "SRCA");
    out_sink.cq_next;
    tc_check(out_sink.pq_get_str, "SRCB");

    tc_check(tag_sink.pq_ready, true);
    tc_check(tag_sink.cq_get_d_nat, 0, "tag: 0");
    tag_sink.cq_next;
    tc_check(tag_sink.cq_get_d_nat, 1, "tag: 1");
    
    tc_pass;

    wait;
  end process;

end test_case;