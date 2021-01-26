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

entity PacketArbiter_tc is
end PacketArbiter_tc;

architecture test_case of PacketArbiter_tc is

  signal clk              : std_logic;
  signal reset            : std_logic;

  constant INDEX_WIDTH    : integer := 2;

  signal in_a_valid       : std_logic;
  signal in_a_ready       : std_logic;
  signal in_a_dvalid      : std_logic;
  signal in_a_last        : std_logic;
  signal in_a_data        : std_logic_vector(7 downto 0);

  signal in_b_valid       : std_logic;
  signal in_b_ready       : std_logic;
  signal in_b_dvalid      : std_logic;
  signal in_b_last        : std_logic;
  signal in_b_data        : std_logic_vector(7 downto 0);

  signal cmd_valid        : std_logic;
  signal cmd_ready        : std_logic;
  signal cmd_index        : std_logic_vector(INDEX_WIDTH-1 downto 0);

  signal out_ready        : std_logic;
  signal out_valid        : std_logic;
  signal out_data         : std_logic_vector(7 downto 0);
  signal out_last         : std_logic;
  signal out_strb         : std_logic;


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
      valid                     => in_a_valid,
      ready                     => in_a_ready,
      dvalid                    => in_a_dvalid,
      last                      => in_a_last,
      data                      => in_a_data
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
      valid                     => in_b_valid,
      ready                     => in_b_ready,
      dvalid                    => in_b_dvalid,
      last                      => in_b_last,
      data                      => in_b_data
    );

    src_cmd_inst: StreamSource_mdl
    generic map (
      NAME                      => "src_cmd",
      ELEMENT_WIDTH             => INDEX_WIDTH,
      COUNT_MAX                 => 1,
      COUNT_WIDTH               => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      valid                     => cmd_valid,
      ready                     => cmd_ready,
      data                      => cmd_index
    );

    

    dut: PacketArbiter
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
      in_strb(0)                => in_a_dvalid,
      in_strb(1)                => in_b_dvalid,

      out_data                  => out_data,
      out_valid                 => out_valid,
      out_ready                 => out_ready,
      out_strb                  => out_strb,
      out_last(0)               => out_last,

      cmd_valid                 => cmd_valid,
      cmd_ready                 => cmd_ready,
      cmd_index                 => cmd_index
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
      valid                     => out_valid,
      ready                     => out_ready,
      data                      => out_data,
      dvalid                    => out_strb,
      last                      => out_last
    );

    

  random_tc: process is
    variable src_a        : streamsource_type;
    variable src_b        : streamsource_type;
    variable src_cmd      : streamsource_type;
    variable out_sink     : streamsink_type;

  begin
    tc_open("StreamSerializer", "test");
    src_a.initialize("src_a");
    src_b.initialize("src_b");
    src_cmd.initialize("src_cmd");
    out_sink.initialize("out_sink");

    src_a.push_str("SRCA");
    src_a.transmit;

    src_b.push_str("SRCB");
    src_b.transmit;

    src_cmd.push_int(0);
    src_cmd.push_int(1);
    src_cmd.transmit;

    out_sink.unblock;

    tc_wait_for(20 us);

    tc_check(out_sink.pq_ready, true);
    tc_check(out_sink.pq_get_str, "SRCA");
    out_sink.cq_next;
    tc_check(out_sink.pq_get_str, "SRCB");
    
    tc_pass;

    wait;
  end process;

end test_case;