-- Generated using vhdMMIO 0.0.3 (https://github.com/abs-tudelft/vhdmmio)

library ieee;
use ieee.std_logic_1164.all;
use ieee.std_logic_misc.all;
use ieee.numeric_std.all;

library work;
use work.vhdmmio_pkg.all;

package mmio_pkg is

  -- Component declaration for mmio.
  component mmio is
    generic (

      -- Interface for field AFU_ID_L: AFU_ID_L.
      F_AFU_ID_L_RESET_DATA : std_logic_vector(63 downto 0) := (others => '0');

      -- Interface for field AFU_ID_H: AFU_ID_H.
      F_AFU_ID_H_RESET_DATA : std_logic_vector(63 downto 0) := (others => '0')

    );
    port (

      -- Clock sensitive to the rising edge and synchronous, active-high reset.
      kcd_clk : in std_logic;
      kcd_reset : in std_logic := '0';

      -- Interface for field start: start.
      f_start_data : out std_logic := '0';

      -- Interface for field stop: stop.
      f_stop_data : out std_logic := '0';

      -- Interface for field reset: reset.
      f_reset_data : out std_logic := '0';

      -- Interface for field idle: idle.
      f_idle_write_data : in std_logic := '0';

      -- Interface for field busy: busy.
      f_busy_write_data : in std_logic := '0';

      -- Interface for field done: done.
      f_done_write_data : in std_logic := '0';

      -- Interface for field result: result.
      f_result_write_data : in std_logic_vector(63 downto 0) := (others => '0');

      -- Interface for field ExampleBatch_firstidx: ExampleBatch_firstidx.
      f_ExampleBatch_firstidx_data : out std_logic_vector(31 downto 0)
          := (others => '0');

      -- Interface for field ExampleBatch_lastidx: ExampleBatch_lastidx.
      f_ExampleBatch_lastidx_data : out std_logic_vector(31 downto 0)
          := (others => '0');

      -- Interface for field ExampleBatch_number_values:
      -- ExampleBatch_number_values.
      f_ExampleBatch_number_values_data : out std_logic_vector(63 downto 0)
          := (others => '0');

      -- Interface for field Profile_enable: Profile_enable.
      f_Profile_enable_data : out std_logic := '0';

      -- Interface for field Profile_clear: Profile_clear.
      f_Profile_clear_data : out std_logic := '0';

      -- AXI4-lite + interrupt request bus to the master.
      mmio_awvalid : in  std_logic := '0';
      mmio_awready : out std_logic := '1';
      mmio_awaddr  : in  std_logic_vector(31 downto 0) := X"00000000";
      mmio_awprot  : in  std_logic_vector(2 downto 0) := "000";
      mmio_wvalid  : in  std_logic := '0';
      mmio_wready  : out std_logic := '1';
      mmio_wdata   : in  std_logic_vector(63 downto 0) := (others => '0');
      mmio_wstrb   : in  std_logic_vector(7 downto 0) := (others => '0');
      mmio_bvalid  : out std_logic := '0';
      mmio_bready  : in  std_logic := '1';
      mmio_bresp   : out std_logic_vector(1 downto 0) := "00";
      mmio_arvalid : in  std_logic := '0';
      mmio_arready : out std_logic := '1';
      mmio_araddr  : in  std_logic_vector(31 downto 0) := X"00000000";
      mmio_arprot  : in  std_logic_vector(2 downto 0) := "000";
      mmio_rvalid  : out std_logic := '0';
      mmio_rready  : in  std_logic := '1';
      mmio_rdata   : out std_logic_vector(63 downto 0) := (others => '0');
      mmio_rresp   : out std_logic_vector(1 downto 0) := "00";
      mmio_uirq    : out std_logic := '0'

    );
  end component;

end package mmio_pkg;
