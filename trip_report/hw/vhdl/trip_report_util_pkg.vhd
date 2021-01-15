library ieee;
use ieee.std_logic_1164.all;
use ieee.std_logic_misc.all;
use ieee.numeric_std.all;

library work;
use work.UtilInt_pkg.all;


package trip_report_util_pkg is
  component StreamSerializer is
    generic (
        EPC                   : natural := 1;
        DATA_WIDTH            : natural := 8;
        DIMENSIONALITY        : natural := 1
        );
    port (
        clk                   : in  std_logic;
        reset                 : in  std_logic;
  
        -- Stream(
        --     Bits(DATA_WIDTH),
        --     t=EPC,
        --     c=8
        -- )
        in_valid              : in  std_logic;
        in_ready              : out std_logic;
        in_data               : in  std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
        in_last               : in  std_logic_vector(DIMENSIONALITY*EPC-1 downto 0) := (others => '0');
        in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
        in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
        in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');
  
        -- Stream(
        --     Bits(DATA_WIDTH),
        --     c=2
        -- )
        out_valid             : out std_logic;
        out_ready             : in  std_logic;
        out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
        out_strb              : out std_logic;
        out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0)
    );
  end component;

  component D2ListToVecs is
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
  end component;

  component DropEmpty is
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
  end component;
end trip_report_util_pkg;