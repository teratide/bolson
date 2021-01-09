library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;


library work;
use work.UtilInt_pkg.all;
use work.trip_report_util_pkg.all;


entity StreamSerializer is
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
end entity;

architecture behavioral of StreamSerializer is
    begin
      clk_proc: process (clk) is
    
        -- Input holding register.
        type in_type is record
          data  : std_logic_vector(DATA_WIDTH-1 downto 0);
          last  : std_logic_vector(DIMENSIONALITY-1 downto 0);
          strb  : std_logic;
        end record;
    
        type in_array is array (natural range <>) of in_type;

        variable id : in_array(0 to EPC-1);
        variable iv : std_logic := '0';
        variable ir : std_logic := '0';
    
        variable od : std_logic_vector(DATA_WIDTH-1 downto 0);
        variable ov : std_logic := '0';
        variable os : std_logic := '1';
        variable ol : std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
    
      begin
        if rising_edge(clk) then
    
          -- Latch input holding register if we said we would.
          if to_x01(ir) = '1' then
            iv := in_valid;
            for idx in 0 to EPC-1 loop
              id(idx).data := in_data(DATA_WIDTH*(idx+1)-1 downto DATA_WIDTH*idx);
              id(idx).last := in_last(DIMENSIONALITY*(idx+1)-1 downto DIMENSIONALITY*idx);
              if idx < unsigned(in_stai) then
                id(idx).strb := '0';
              elsif idx > unsigned(in_endi) then
                id(idx).strb := '0';
              else
                id(idx).strb := in_strb(idx);
              end if;
            end loop;
          end if;
    
          -- Clear output holding register if transfer was accepted.
          if to_x01(out_ready) = '1' then
            ov := '0';
          end if;

          -- Do processing when both registers are ready.
          os := '0';
          if to_x01(iv) = '1' then
            for idx in 0 to EPC-1 loop
              if to_x01(ov) /= '1' then
                if or_reduce(id(idx).last) = '1' then
                  ol := id(idx).last;
                  ov := '1';
                  id(idx).last := (others => '0');
                end if;
                if to_x01(id(idx).strb) = '1' then
                  od := id(idx).data;
                  ov := '1';
                  os := '1';
                  id(idx).strb := '0';
                end if;
             end if;
            end loop;

            iv := '0';
            for idx in id'range loop
              if id(idx).strb = '1' or or_reduce(id(idx).last) = '1' then
                iv := '1';
              end if;
            end loop;
          end if;
    
          -- Handle reset.
          if to_x01(reset) /= '0' then
            ir    := '0';
            ov    := '0';
          end if;
    
          -- Forward output holding register.
          ir := not iv and not reset;
          in_ready <= ir and not reset;
          out_valid <= to_x01(ov);
          out_data <= od;
          out_last <= ol;
          out_strb <= os;
        end if;
      end process;
    end architecture;