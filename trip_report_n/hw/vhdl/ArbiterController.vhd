library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;

entity ArbiterController is
  generic (
      NUM_INPUTS            : natural;
      INDEX_WIDTH           : natural;
      TAG_WIDTH             : natural := 8
      );
  port (
      clk                   : in  std_logic;
      reset                 : in  std_logic;

      pkt_valid             : in  std_logic_vector(NUM_INPUTS-1 downto 0);
      pkt_ready             : out std_logic_vector(NUM_INPUTS-1 downto 0);

      last_pkt_valid        : in  std_logic;
      last_pkt_ready        : out std_logic;

      cmd_valid             : out std_logic;
      cmd_ready             : in  std_logic;
      cmd_index             : out std_logic_vector(INDEX_WIDTH-1 downto 0);

      tag_valid             : out std_logic;
      tag_ready             : in  std_logic;
      tag                   : out std_logic_vector(TAG_WIDTH-1 downto 0);
      tag_strb              : out std_logic;
      tag_last              : out std_logic;

      tag_cfg               : in  std_logic_vector(NUM_INPUTS*TAG_WIDTH-1 downto 0)
  );
end entity;

architecture Implementation of ArbiterController is
  begin
    cntrl_proc: process (clk) is
      -- Tag valid.
      variable tv          : std_logic;
      -- Last packet valid.
      variable lpv         : std_logic;
      -- Last packet ready.
      variable lpr         : std_logic;
      -- Tag strobe.
      variable ts          : std_logic;
      -- Tag last.
      variable tl          : std_logic;
      -- Command valid.
      variable cv          : std_logic;
      -- Previous source index,
      variable index_r     : std_logic_vector(INDEX_WIDTH-1 downto 0);
      -- Current source index.
      variable index       : std_logic_vector(INDEX_WIDTH-1 downto 0);
      -- Selected tag.
      variable tag_v       : std_logic_vector(TAG_WIDTH-1 downto 0);

      variable pkt_ready_v : std_logic_vector(NUM_INPUTS-1 downto 0); 

    begin 
      if rising_edge(clk) then

        if cmd_ready = '1' then
          cv := '0';
        end if;

        if tag_ready = '1' then
          tv := '0';
        end if;

        if to_x01(lpr) = '1' then
          lpv := last_pkt_valid;
        end if;

        pkt_ready_v  := (others => '0');

        -- Select the next index (RR)
        if to_x01(cv) /= '1' and to_x01(tv) /= '1'then
          ts := '0';

          -- Priority init.
          for idx in NUM_INPUTS-1 downto 0 loop
            if pkt_valid(idx) = '1' then
              index          := std_logic_vector(to_unsigned(idx, INDEX_WIDTH));
            end if;
          end loop;

          --Round-robin arbitration.
          for idx in NUM_INPUTS-1 downto 0 loop
            if pkt_valid(idx) = '1' then
              if idx > to_integer(unsigned(index_r)) then
                index  := std_logic_vector(to_unsigned(idx, INDEX_WIDTH));
              end if;
            end if;
          end loop;

          for idx in NUM_INPUTS-1 downto 0 loop
            if idx = to_integer(unsigned(index)) and pkt_valid(to_integer(unsigned(index))) = '1' then
              pkt_ready_v(idx) := '1';
            else
              pkt_ready_v(idx) := '0';
            end if;
          end loop;


          if pkt_valid(to_integer(unsigned(index))) = '1' then
            cv             := '1';
            tv             := '1';
            ts             := '1';
            tl             := '0';
            tag_v          := tag_cfg(TAG_WIDTH*(to_integer(unsigned(index))+1)-1 downto TAG_WIDTH*to_integer(unsigned(index)));
          end if;

          if to_x01(lpv) = '1' then
            lpv := '0';
            tv  := '1';
            tl  := '1'; 
          end if;

        end if;

        -- Handle reset.
        if reset = '1' then
          index       := (others => '0');
          index_r     := (others => '0');
          cv          := '0';
          tv          := '0';
        end if;

        index_r        := index;
        cmd_index      <= index;
        tag            <= tag_v;
        cmd_valid      <= cv and not reset;
        tag_valid      <= tv and not reset;
        tag_strb       <= ts;
        lpr            := not lpv;
        last_pkt_ready <= lpr and not reset;
        pkt_ready      <= pkt_ready_v;
        tag_last       <= tl;
        tag_strb       <= ts; 
      end if;
    end process;
  end architecture;
