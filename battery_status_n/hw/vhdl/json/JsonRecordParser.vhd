library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.or_reduce;

library work;
use work.UtilInt_pkg.all;
use work.Json_pkg.all;


entity JsonRecordParser is
  generic (
      EPC                   : natural := 1;
      OUTER_NESTING_LEVEL   : natural := 1;
      INNER_NESTING_LEVEL   : natural := 0;
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
end entity;

architecture behavioral of JsonRecordParser is
  constant OUT_VEC_WIDTH : integer := 8*EPC + EPC;
  constant OUT_DATA_STAI : integer := 0;
  constant OUT_DATA_ENDI : integer := EPC*8-1;
  CONSTANT OUT_TAG_STAI  : integer := EPC*8;
  CONSTANT OUT_TAG_ENDI  : integer := EPC*8+EPC-1;
begin
  clk_proc: process (clk) is
    constant IDXW : natural := log2ceil(EPC);

    -- Input holding register.
    type in_type is record
      data  : std_logic_vector(7 downto 0);
      last  : std_logic_vector(OUTER_NESTING_LEVEL-1 downto 0);
      strb  : std_logic;
    end record;

    type in_array is array (natural range <>) of in_type;
    variable id : in_array(0 to EPC-1);
    variable iv : std_logic := '0';
    variable ir : std_logic := '0';

    -- Output holding register.
    type out_type is record
      data  : std_logic_vector(7 downto 0);
      tag   : std_logic;
      last  : std_logic_vector(OUTER_NESTING_LEVEL+1 downto 0);
      strb  : std_logic;
    end record;

    type out_array is array (natural range <>) of out_type;
    variable od : out_array(0 to EPC-1);
    variable ov : std_logic := '0';


    variable idx_int : unsigned(log2ceil(EPC)-1 downto 0);

    variable end_req_i : std_logic;
    variable end_ack_i : std_logic;

    -- Enumeration type for our state machine.
    type state_t is (STATE_IDLE,
                     STATE_RECORD,
                     STATE_KEY, 
                     STATE_VALUE);

    -- State variable
    variable state : state_t;

    variable nesting_level_th : std_logic_vector(INNER_NESTING_LEVEL downto 0) := (others => '0');
    variable nesting_inner    : std_logic_vector(INNER_NESTING_LEVEL downto 1) := (others => '0');

    variable nesting_origo    : std_logic;

  begin

    if END_REQ_EN then
      assert OUTER_NESTING_LEVEL = 0 Report "When END_REQ_EN is asserted," &
                                             "OUTER_NESTING_LEVEL needs to be set to 1" &
                                             "in JsonRecordParser." 
      severity Failure;
    end if;


    if rising_edge(clk) then

      -- Latch input holding register if we said we would.
      if to_x01(ir) = '1' then
        iv := in_valid;
        end_req_i := end_req;
        for idx in 0 to EPC-1 loop
          id(idx).data  := in_data(8*idx+7 downto 8*idx);
          id(idx).last  := in_last((OUTER_NESTING_LEVEL+1)*(idx+1)-1 downto (OUTER_NESTING_LEVEL+1)*(idx)+1);
          id(idx).strb  := in_strb(idx);
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
      if to_x01(iv) = '1' and to_x01(ov) = '0' then
        for idx in 0 to EPC-1 loop

          -- Default behavior.
          od(idx).data                                  := id(idx).data;
          od(idx).tag                                   := '0';
          od(idx).last(OUTER_NESTING_LEVEL+1 downto 0)  := id(idx).last & "00";
          od(idx).strb                                  := '0';
          end_ack_i                                     := '0';
          
          idx_int := to_unsigned(idx, idx_int'length);

          -- Element-wise processing only when the lane is valid.
          if to_x01(id(idx).strb) = '1' then


            -- Keep track of nesting.
            case id(idx).data is
              when X"7B" => -- '{'
                nesting_level_th := nesting_level_th(nesting_level_th'high-1 downto 0) & '1';
              when X"5B" => -- '['
                nesting_level_th := nesting_level_th(nesting_level_th'high-1 downto 0) & '1';
              when X"7D" => -- '}'
                nesting_level_th := '0' & nesting_level_th(nesting_level_th'high downto 1);
              when X"5D" => -- ']'
                nesting_level_th := '0' & nesting_level_th(nesting_level_th'high downto 1);
              when others =>
                nesting_level_th := nesting_level_th;
            end case;

            nesting_inner := nesting_level_th(nesting_level_th'high downto 1);
            nesting_origo := not or_reduce(nesting_inner);

            case state is
              when STATE_IDLE =>
                od(idx).strb    := '0';
                case id(idx).data is
                  when X"7B" => -- '{'
                    state := STATE_RECORD;
                  when others =>
                    state := STATE_IDLE;
                end case;

              when STATE_RECORD =>
                od(idx).strb    := '0';
                case id(idx).data is
                  when X"22" => -- '"'
                    state := STATE_KEY;
                  when X"3A" => -- ':'
                    state := STATE_VALUE;
                  when X"7D" => -- '}'
                    od(idx).last(1) := '1';
                    ov              := '1'; 
                    if end_req_i = '1' then
                      end_ack_i := '1';
                    end if;
                    state := STATE_IDLE;
                  when others =>
                    state := STATE_RECORD;
                end case;
                
              when STATE_KEY =>
                od(idx).tag := '0';
                od(idx).strb := '1';
                case id(idx).data is
                  when X"22" => -- '"'
                    state := STATE_RECORD;
                    od(idx).last(0) := '1';
                    od(idx).strb   := '0';
                  when others =>
                    ov := '1';
                    state := STATE_KEY;
                end case;

              when STATE_VALUE =>
                od(idx).tag := '1';
                od(idx).strb := '1';
                case id(idx).data is
                  when X"2C" => -- ','
                    if nesting_origo = '1' then
                      state := STATE_RECORD;
                      od(idx).last(0) := '1';
                      od(idx).strb   := '0';
                    end if;
                  when X"7D" => -- '}'
                    if nesting_origo = '1' then
                      state := STATE_IDLE;   
                      od(idx).last(0) := '1';
                      od(idx).last(1) := '1';
                      od(idx).strb   := '0';     
                      if end_req_i = '1' then
                        end_ack_i := '1';
                        od(idx).last(2) := '1';
                      end if;
                    end if;
                  when others =>
                    state := STATE_VALUE;
                end case;
            end case;
          end if;
          -- Clear state upon any last, to prevent broken elements from messing
          -- up everything.
          if id(idx).last(0) /= '0' then
            state := STATE_IDLE;
            nesting_level_th := (others => '0');
            if end_req_i = '1' then
              end_ack_i := '1';
            end if;
          end if;
        end loop;
        ov := '1';
        iv := '0';
      end if;

      -- Handle reset.
      if to_x01(reset) /= '0' then
        ir    := '0';
        ov    := '0';
        state := STATE_IDLE;
        nesting_level_th := (others => '0');
      end if;

      -- Forward output holding register.
      out_valid <= to_x01(ov);
      ir := not iv and not reset;
      in_ready <= ir;
      end_ack <= end_req_i;
      for idx in 0 to EPC-1 loop
        --Pack output data
        out_data(OUT_DATA_STAI+8*idx+7 downto OUT_DATA_STAI+8*idx) <= od(idx).data;
        out_data(OUT_TAG_STAI+idx) <= od(idx).tag;
        out_last((OUTER_NESTING_LEVEL+2)*(idx+1)-1 downto (OUTER_NESTING_LEVEL+2)*idx) <= od(idx).last;
        out_stai <= (others => '0');--std_logic_vector(stai);
        out_endi <= (others => '1');--std_logic_vector(endi);
        out_strb(idx) <= od(idx).strb;
      end loop;
    end if;
  end process;
end architecture;