library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;


library work;
use work.UtilInt_pkg.all;
use work.Json_pkg.all;

-- TODO

entity BooleanParser is
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
end entity;

architecture behavioral of BooleanParser is
    begin
      clk_proc: process (clk) is
        constant IDXW : natural := log2ceil(EPC);
    
        -- Input holding register.
        type in_type is record
          data  : std_logic_vector(7 downto 0);
          last  : std_logic_vector(NESTING_LEVEL downto 0);
          empty : std_logic;
          strb  : std_logic;
        end record;
    
        type in_array is array (natural range <>) of in_type;
        variable id : in_array(0 to EPC-1);
        variable iv : std_logic := '0';
        variable ir : std_logic := '0';
    
        variable ov : std_logic := '0';
        variable oe : std_logic := '1';
        variable ol : std_logic_vector(NESTING_LEVEL-1 downto 0) := (others => '0');

        variable val : boolean;
    
      begin
        if rising_edge(clk) then
    
          -- Latch input holding register if we said we would.
          if to_x01(ir) = '1' then
            iv := in_valid;
            for idx in 0 to EPC-1 loop
              id(idx).data := in_data(8*idx+7 downto 8*idx);
              id(idx).last := in_last((NESTING_LEVEL+1)*(idx+1)-1 downto (NESTING_LEVEL+1)*idx);
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

          ol := (others => '0');
          oe := '1';

          -- Do processing when both registers are ready.
          if to_x01(iv) = '1' then
            for idx in 0 to EPC-1 loop
              -- Element-wise processing only when the lane is valid.

              if to_x01(id(idx).strb) = '1' and to_x01(ov) /= '1' then

                ol := ol or id(idx).last(NESTING_LEVEL downto 1);
                  case id(idx).data is
                    when X"66" => -- 'f'
                        ov := '1';
                        oe := '0';
                        val:= false;
                    when X"46" => -- 'F'
                        ov := '1';
                        oe := '0';
                        val:= false;
                    when X"74" => -- 't'
                        ov := '1';
                        oe := '0';    
                        val:= true;
                    when X"54" => -- 'T'
                        ov := '1';
                        oe := '0';
                        val:= true;
                    when others =>
                        ov := '0';
                  end case;
                id(idx).strb := '0';
             end if;

            end loop;

            iv := '0';
            for idx in id'range loop
              if id(idx).strb = '1' then
                iv := '1';
              end if;
            end loop;
          end if;

          if or_reduce(ol) and not iv then
            ov := '1';
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
          out_data <= '1' when val else '0';
          out_last <= ol;
          out_strb <= not oe;
        end if;
      end process;
    end architecture;