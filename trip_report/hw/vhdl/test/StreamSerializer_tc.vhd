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

entity StreamSerializer_tc is
end StreamSerializer_tc;

architecture test_case of StreamSerializer_tc is

  signal clk              : std_logic;
  signal reset            : std_logic;

  constant EPC            : integer := 3;

  signal in_valid         : std_logic;
  signal in_ready         : std_logic;
  signal in_dvalid        : std_logic;
  signal in_last          : std_logic;
  signal in_data          : std_logic_vector(EPC*8-1 downto 0);
  signal in_count         : std_logic_vector(log2ceil(EPC+1)-1 downto 0) := std_logic_vector(to_unsigned(1, log2ceil(EPC+1)));
  signal in_strb          : std_logic_vector(EPC-1 downto 0);
  signal in_endi          : std_logic_vector(log2ceil(EPC+1)-1 downto 0) := (others => '1');
  signal in_stai          : std_logic_vector(log2ceil(EPC+1)-1 downto 0) := (others => '0');


  signal out_ready        : std_logic;
  signal out_valid        : std_logic;
  signal out_data         : std_logic_vector(7 downto 0);
  signal out_last         : std_logic;
  signal out_strb         : std_logic;

  signal adv_last         : std_logic_vector(EPC-1 downto 0);

begin

  clkgen: ClockGen_mdl
    port map (
      clk                       => clk,
      reset                     => reset
    );

  in_source: StreamSource_mdl
    generic map (
      NAME                      => "a",
      ELEMENT_WIDTH             => 8,
      COUNT_MAX                 => EPC,
      COUNT_WIDTH               => log2ceil(EPC+1)
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      valid                     => in_valid,
      ready                     => in_ready,
      dvalid                    => in_dvalid,
      last                      => in_last,
      data                      => in_data,
      count                     => in_count
    );

    
    in_strb <= element_mask(in_count, in_dvalid, EPC); 
    in_endi <= std_logic_vector(unsigned(in_count) - 1);

    adv_last(EPC-1 downto 0) <=  std_logic_vector(shift_left(resize(unsigned'("0" & in_last),EPC), to_integer((unsigned(in_endi)))));
    
    dut: StreamSerializer
    generic map (
      EPC                       => EPC,
      DATA_WIDTH                => 8,
      DIMENSIONALITY            => 1
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => in_valid,
      in_ready                  => in_ready,
      in_data                   => in_data,
      in_last                   => adv_last,
      in_strb                   => in_strb,
      out_data                  => out_data,
      out_valid                 => out_valid,
      out_ready                 => out_ready,
      out_strb                  => out_strb,
      out_last(0)               => out_last
    );

    out_sink: StreamSink_mdl
    generic map (
      NAME                      => "b",
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
      --dvalid                    => out_strb,
      last                      => out_last
    );

    

  random_tc: process is
    variable a        : streamsource_type;
    variable b        : streamsink_type;

  begin
    tc_open("StreamSerializer", "test");
    a.initialize("a");
    b.initialize("b");

    b.set_valid_cyc(0, 10);
    b.set_total_cyc(0, 10);

    a.push_str("All is well");
    a.transmit;

    b.unblock;

    tc_wait_for(20 us);

    tc_check(b.pq_ready, true);
    tc_check(b.pq_get_str, "All is well");
    
    tc_pass;

    wait;
  end process;

end test_case;