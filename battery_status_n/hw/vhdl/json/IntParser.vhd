library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.UtilInt_pkg.all;
use work.Stream_pkg.all;
use work.Json_pkg.all;

entity IntParser is
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
      out_strb              : out  std_logic;
      out_last              : out std_logic_vector(NESTING_LEVEL-1 downto 0)

  );
end entity;

architecture behavioral of IntParser is

    -- Input holding register.
    type in_type is record
      data  : std_logic_vector(7 downto 0);
      last  : std_logic_vector(NESTING_LEVEL downto 0);
      strb  : std_logic;
    end record;

    type dd_stage_t is record
      bcd   : std_logic_vector(BITWIDTH+(BITWIDTH-4)/3-1 downto 0);
      bin   : std_logic_vector(BITWIDTH-1 downto 0);
      --ready : std_logic;
      --valid : std_logic;
      empty : std_logic;
      last  : std_logic_vector(NESTING_LEVEL-1 downto 0);
    end record;

    constant BCD_WIDTH    : integer := BITWIDTH+(BITWIDTH-4)/3;
    constant SLICE_WIDTH  : integer := BCD_WIDTH + BITWIDTH + NESTING_LEVEL + 1;

    constant DD_BCD_STAI  : integer := 0;
    constant DD_BCD_ENDI  : integer := BCD_WIDTH-1;
    constant DD_BIN_STAI  : integer := BCD_WIDTH;
    constant DD_BIN_ENDI  : integer := BITWIDTH + BCD_WIDTH-1;
    constant DD_LAST_STAI : integer := BITWIDTH + BCD_WIDTH;
    constant DD_LAST_ENDI : integer := BITWIDTH + BCD_WIDTH + NESTING_LEVEL -1;
    CONSTANT DD_EMPTY_I   : integer := BITWIDTH + BCD_WIDTH + NESTING_LEVEL;


    constant dd_stage_t_init : dd_stage_t := (  bcd => (others => '0'),
                                                bin => (others => '0'),
                                                empty => '1',
                                                last => (others => '0'));

    signal dd_in_s  : dd_stage_t := dd_stage_t_init;
    signal dd_out_s : dd_stage_t := dd_stage_t_init;

    signal dd_in_valid_s : std_logic;
    signal dd_in_ready   : std_logic;

    type slice_handshake_t        is array (0 to PIPELINE_STAGES) of std_logic;
    type slice_data_t             is array (0 to PIPELINE_STAGES) of std_logic_vector(SLICE_WIDTH-1 downto 0);


    signal slice_data_in    : slice_data_t;
    signal slice_data_out   : slice_data_t;
    signal slice_ready      : slice_handshake_t;
    signal slice_valid      : slice_handshake_t;

    signal bcd    : std_logic_vector(BCD_WIDTH-1 downto 0);

    type stage_data_t is array (0 to PIPELINE_STAGES) of dd_stage_t;

    signal dd_stage_data_in : stage_data_t := (others => dd_stage_t_init);
    signal dd_stage_data_out : stage_data_t := (others => dd_stage_t_init);
    

    procedure dd_stage (
        signal    i         : in  dd_stage_t;
        signal    o         : out dd_stage_t;
        constant  BW        : in natural;
        constant  STEPS     : in natural
      ) is
        variable bcd_shr : std_logic_vector(BW+(BW-4)/3-1 downto 0) := (others => '0');
        variable bin_shr : std_logic_vector(BW-1 downto 0) := (others => '0');
    begin
      -- Use the double-dabble alogorithm to convert BCD to binary.
      bcd_shr := i.bcd;
      bin_shr := i.bin;
      for j in 0 to STEPS-1 loop
        bin_shr := bcd_shr(0) & bin_shr(bin_shr'left downto 1);
        bcd_shr := '0' & bcd_shr(bcd_shr'high downto 1);
        for idx in 0 to (BW+(BW-4)/3)/4-1 loop
          if to_01(unsigned(bcd_shr(idx*4+3 downto idx*4))) >= 8 then
            bcd_shr(idx*4+3 downto idx*4) := std_logic_vector(unsigned(unsigned(bcd_shr(idx*4+3 downto idx*4)) - 3));
          end if;
        end loop;
      end loop;
      o.bcd   <= bcd_shr;
      o.bin   <= bin_shr;
      o.last  <= i.last;
      o.empty <= i.empty;
    end procedure;

    begin
        in_stage: process (clk) is
          
          type in_array is array (natural range <>) of in_type;
          variable id   : in_array(0 to EPC-1);
          variable stai : unsigned(log2ceil(EPC)-1 downto 0);
          variable iv   : std_logic := '0';
          variable ir   : std_logic := '0';
    
          variable in_shr  : std_logic_vector(BITWIDTH+(BITWIDTH-4)/3-1 downto 0) := (others => '0');

          variable dd_in  : dd_stage_t := dd_stage_t_init;

          variable dd_in_valid   : std_logic;

      begin

        assert BITWIDTH mod PIPELINE_STAGES = 0 Report "BITWIDTH mod PIPELINE_STAGES needs to be 0 in IntParser!"
        severity Failure;

        if rising_edge(clk) then
          -- Latch input holding register if we said we would.
          if to_x01(ir) = '1' then
            iv := in_valid;
            if to_x01(iv) = '1'then
              for idx in 0 to EPC-1 loop
                id(idx).data := in_data(8*idx+7 downto 8*idx);
                id(idx).last := in_last((NESTING_LEVEL+1)*(idx+1)-1 downto (NESTING_LEVEL+1)*idx);
                stai := unsigned(in_stai);
                id(idx).strb := in_strb(idx);
                if idx < unsigned(in_stai) then
                  id(idx).strb := '0';
                elsif idx > unsigned(in_endi) then
                  id(idx).strb := '0';
                else
                  id(idx).strb := in_strb(idx);
                end if;
              end loop;
            end if;
          end if;
    
          -- Clear output holding register if transfer was accepted.
          if to_x01(dd_in_ready) = '1' then
            if dd_in_valid = '1' then
              dd_in := dd_stage_t_init;
            end if;
            dd_in_valid := '0';
          end if;

          

          for idx in 0 to EPC-1 loop
            if to_x01(iv) = '1' and to_x01(dd_in_valid) = '0' then

              dd_in.last := dd_in.last or id(idx).last(NESTING_LEVEL downto 1);
              id(idx).last(NESTING_LEVEL downto 1) := (others => '0');

              if to_x01(id(idx).strb) = '1' or id(idx).last(0) /= '0' then

                if id(idx).data(7 downto 4) = X"3" then
                  in_shr := in_shr(in_shr'high-4 downto 0) & id(idx).data(3 downto 0);
                end if;

                if id(idx).last(0) /= '0'  then
                  id(idx).last(0) := '0';
                  dd_in.bcd       := in_shr;
                  in_shr          := (others => '0');
                  dd_in.empty     := '0';
                  dd_in_valid     := '1';
                end if;
              end if;
              id(idx).strb := '0';
            end if;
          end loop;

          if to_x01(iv) = '1'then
            iv := '0';
            for lane in id'range loop
              if id(lane).strb = '1' or or_reduce(id(lane).last(NESTING_LEVEL downto 1)) /= '0' then
                iv := '1';
              end if;
            end loop;
          end if;


          if or_reduce(dd_in.last) = '1' then
            dd_in_valid := '1';
          end if;

          -- Handle reset.
          if to_x01(reset) /= '0' then
            iv            := '0';
            dd_in         := dd_stage_t_init;
            dd_in_valid   := '0';
            in_shr        := (others => '0');
          end if;
    
          -- Assign input ready and forward data to the next stage.
          ir        := not iv;
          in_ready      <= ir;
          dd_in_s       <= dd_in;
          dd_in_valid_s <= dd_in_valid;
          bcd <= dd_in.bcd;

        end if;
      end process;

      -- Interfacing
      dd_stage_data_in(0) <= dd_in_s;

      dd_in_ready <= slice_ready(0);
      slice_valid(0) <= dd_in_valid_s;

      stage_gen: for i in 0 to PIPELINE_STAGES-1  generate
        dd_stage(dd_stage_data_in(i),
                dd_stage_data_out(i),
                BITWIDTH,
                BITWIDTH/PIPELINE_STAGES);

        -- Pack slice data vector
        slice_data_in(i)(DD_BCD_ENDI   downto DD_BCD_STAI)     <= dd_stage_data_out(i).bcd;
        slice_data_in(i)(DD_BIN_ENDI   downto DD_BIN_STAI)     <= dd_stage_data_out(i).bin;
        slice_data_in(i)(DD_LAST_ENDI  downto DD_LAST_STAI)    <= dd_stage_data_out(i).last;
        slice_data_in(i)(DD_EMPTY_I)                           <= dd_stage_data_out(i).empty;

        -- Unpack slice data vector
        dd_stage_data_in(i+1).bcd   <= slice_data_out(i)(DD_BCD_ENDI   downto DD_BCD_STAI);
        dd_stage_data_in(i+1).bin   <= slice_data_out(i)(DD_BIN_ENDI   downto DD_BIN_STAI);
        dd_stage_data_in(i+1).last  <= slice_data_out(i)(DD_LAST_ENDI  downto DD_LAST_STAI);
        dd_stage_data_in(i+1).empty <= slice_data_out(i)(DD_EMPTY_I);
      end generate stage_gen;

      gen_slices : for i in 0 to PIPELINE_STAGES-1 generate
      slice : StreamSlice 
        generic map (
          DATA_WIDTH          => SLICE_WIDTH
        )
        port map(
          clk          => clk,
          reset        => reset,

          in_valid     => slice_valid(i),
          in_ready     => slice_ready(i),

          in_data      => slice_data_in(i),
          out_data     => slice_data_out(i),

          out_valid    => slice_valid(i+1),
          out_ready    => slice_ready(i+1)
        );
      end generate;

      
      -- Interfacing
      slice_ready(PIPELINE_STAGES) <= out_ready;
      out_valid <= slice_valid(PIPELINE_STAGES);
      out_data  <= dd_stage_data_in(PIPELINE_STAGES).bin;
      out_last  <= dd_stage_data_in(PIPELINE_STAGES).last;
      out_strb  <= not dd_stage_data_in(PIPELINE_STAGES).empty;
      
    end architecture;