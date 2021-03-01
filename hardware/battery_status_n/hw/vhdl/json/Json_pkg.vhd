library ieee;
use ieee.std_logic_1164.all;
use ieee.std_logic_misc.all;
use ieee.numeric_std.all;

library work;
use work.UtilInt_pkg.all;


package Json_pkg is
    component JsonRecordParser is
        generic (
          EPC                   : natural := 1;
          OUTER_NESTING_LEVEL   : natural := 1;
          INNER_NESTING_LEVEL   : natural := 1;
          END_REQ_EN            : boolean := false
            );
        port (
            clk                   : in  std_logic;
            reset                 : in  std_logic;

            -- Stream(
            --     Bits(8),
            --     t=EPC,
            --     d=OUTER_NESTING_LEVEL+1,
            --     c=8
            -- )
            in_valid              : in  std_logic;
            in_ready              : out std_logic;
            in_data               : in  std_logic_vector(8*EPC-1 downto 0);
            in_last               : in  std_logic_vector((OUTER_NESTING_LEVEL+1)*EPC-1 downto 0) := (others => '0');
            in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
            in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
            in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');

            end_req               : in  std_logic := '0';
            end_ack               : out std_logic;


            -- Stream(
            --     Bits(8),
            --     t=EPC,
            --     d=OUTER_NESTING_LEVEL+2,
            --     c=8
            -- )
            out_valid             : out std_logic;
            out_ready             : in  std_logic;
            out_data              : out std_logic_vector(8*EPC + EPC-1 downto 0);
            out_last              : out std_logic_vector((OUTER_NESTING_LEVEL+2)*EPC-1 downto 0) := (others => '0');
            out_stai              : out std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
            out_endi              : out std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
            out_strb              : out std_logic_vector(EPC-1 downto 0) := (others => '1')

        );
    end component;

    component JsonArrayParser is
        generic (
            EPC                   : natural := 1;
            OUTER_NESTING_LEVEL   : natural := 1;
            INNER_NESTING_LEVEL   : natural := 0;
            ELEMENT_COUNTER_BW    : natural := 4
            );
        port (
            clk                   : in  std_logic;
            reset                 : in  std_logic;
      
            -- Stream(
            --     Bits(8),
            --     t=EPC,
            --     d=OUTER_NESTING_LEVEL+1,
            --     c=8
            -- )
            in_valid              : in  std_logic;
            in_ready              : out std_logic;
            in_data               : in  std_logic_vector(8*EPC-1 downto 0);
            in_last               : in  std_logic_vector((OUTER_NESTING_LEVEL+1)*EPC-1 downto 0) := (others => '0');
            in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
            in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
            in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');
      
            -- Stream(
            --     Bits(8),
            --     t=EPC,
            --     d=OUTER_NESTING_LEVEL+2,
            --     c=8
            -- )
            out_valid             : out std_logic;
            out_ready             : in  std_logic;
            out_data              : out std_logic_vector(8*EPC-1 downto 0);
            out_last              : out std_logic_vector((OUTER_NESTING_LEVEL+2)*EPC-1 downto 0) := (others => '0');
            out_stai              : out std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
            out_endi              : out std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
            out_strb              : out std_logic_vector(EPC-1 downto 0) := (others => '1')


            -- out_count_valid       : out std_logic;
            -- out_count_ready       : in  std_logic := '1';
            -- out_count_data        : out std_logic_vector(ELEMENT_COUNTER_BW-1 downto 0)
      
        );
      end component;

    component KeyFilter is
      generic (
        EPC                   : natural := 1;
        OUTER_NESTING_LEVEL   : natural := 1;
        DLY_COMP_BUFF_DEPTH   : natural := 5
        );
      port (
        clk                   : in  std_logic;
        reset                 : in  std_logic;
  
        in_valid              : in  std_logic;
        in_ready              : out std_logic;
        in_data               : in  std_logic_vector(8*EPC+EPC-1 downto 0);
        in_last               : in  std_logic_vector((OUTER_NESTING_LEVEL+1)*EPC-1 downto 0) := (others => '0');
        in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
        in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
        in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');
  
        matcher_str_valid     : out std_logic;
        matcher_str_ready     : in  std_logic;
        matcher_str_data      : out std_logic_vector(EPC*8-1 downto 0);
        matcher_str_mask      : out std_logic_vector(EPC-1 downto 0);
        matcher_str_last      : out std_logic_vector(EPC-1 downto 0);
  
        matcher_match_valid   : in  std_logic;
        matcher_match_ready   : out std_logic;
        matcher_match         : in  std_logic_vector(EPC-1 downto 0);
  
        out_valid             : out std_logic;
        out_ready             : in  std_logic;
        out_data              : out std_logic_vector(EPC*8-1 downto 0);
        out_last              : out std_logic_vector((OUTER_NESTING_LEVEL+1)*EPC-1 downto 0) := (others => '0');
        out_stai              : out std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
        out_endi              : out std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
        out_strb              : out std_logic_vector(EPC-1 downto 0) := (others => '1')
      );
    end component;

    component BooleanParser is
      generic (
        EPC                   : natural := 1;
        NESTING_LEVEL         : natural := 1
        );
      port (
          clk                   : in  std_logic;
          reset                 : in  std_logic;
    
          -- Stream(
          --     Bits(8),
          --     t=EPC,
          --     d=NESTING_LEVEL+1,
          --     c=8
          -- )
          in_valid              : in  std_logic;
          in_ready              : out std_logic;
          in_data               : in  std_logic_vector(8*EPC-1 downto 0);
          in_last               : in  std_logic_vector((NESTING_LEVEL+1)*EPC-1 downto 0) := (others => '0');
          in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
          in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
          in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');
    
          -- Stream(
          --     Bits(1),
          --     d=NESTING_LEVEL,
          --     c=8
          -- )
          out_valid             : out std_logic;
          out_ready             : in  std_logic;
          out_data              : out std_logic;
          out_strb              : out std_logic;
          out_last              : out std_logic_vector(NESTING_LEVEL-1 downto 0)
      );
    end component;

    component IntParser is
      generic (
          EPC                   : natural := 1;
          NESTING_LEVEL         : natural := 1;
          BITWIDTH              : natural := 8;
          SIGNED                : boolean := false; -- Signed is not supported yet!
          PIPELINE_STAGES       : natural := 1
          );
      port (
          clk                   : in  std_logic;
          reset                 : in  std_logic;
    
          -- Stream(
          --     Bits(8),
          --     t=EPC,
          --     d=NESTING_LEVEL+1,
          --     c=8
          -- )
          in_valid              : in  std_logic;
          in_ready              : out std_logic;
          in_data               : in  std_logic_vector(8*EPC-1 downto 0);
          in_last               : in  std_logic_vector((NESTING_LEVEL+1)*EPC-1 downto 0) := (others => '0');
          in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
          in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
          in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');
    
          -- Stream(
          --     Bits(BITWIDTH),
          --     d=NESTING_LEVEL,
          --     c=2
          -- )
          out_valid             : out std_logic;
          out_ready             : in  std_logic;
          out_data              : out std_logic_vector(BITWIDTH-1 downto 0);
          out_strb              : out std_logic;
          out_last              : out std_logic_vector(NESTING_LEVEL-1 downto 0)
      );
    end component;
end Json_pkg;



