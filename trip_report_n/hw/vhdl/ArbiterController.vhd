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
      pkt_last              : in  std_logic_vector(NUM_INPUTS-1 downto 0) := (others => '0');

      cmd_valid             : out std_logic;
      cmd_ready             : in  std_logic;
      cmd_index             : out std_logic_vector(INDEX_WIDTH-1 downto 0);
      cmd_last              : out std_logic;

      tag_valid             : out std_logic;
      tag_ready             : in  std_logic;
      tag                   : out std_logic_vector(TAG_WIDTH-1 downto 0);
      tag_last              : out std_logic;

      tag_cfg               : in  std_logic_vector(NUM_INPUTS*TAG_WIDTH-1 downto 0)
  );
end entity;

architecture Implementation of ArbiterController is
    signal out_valid : std_logic;
    signal out_ready : std_logic;
  begin

    sync_out: StreamSync
      generic map (
        NUM_INPUTS              => 1,
        NUM_OUTPUTS             => 2
      )
      port map (
        clk                     => clk,
        reset                   => reset,

        in_valid(0)             => out_valid,
        in_ready(0)             => out_ready,

        out_valid(0)            => cmd_valid,
        out_valid(1)            => tag_valid,

        out_ready(0)            => cmd_ready,
        out_ready(1)            => tag_ready
    );

    cntrl_proc: process (clk) is
      variable ov       : std_logic := '0';
      variable ol       : std_logic := '0';
      variable cl       : std_logic := '0';
      variable index_r  : std_logic_vector(INDEX_WIDTH-1 downto 0) := (others => '0');
      variable index    : std_logic_vector(INDEX_WIDTH-1 downto 0) := (others => '0');
      variable tag_v    : std_logic_vector(TAG_WIDTH-1 downto 0);
      variable last_pkt : std_logic_vector(INDEX_WIDTH-1 downto 0) := (others => '0');
    begin 

      if rising_edge(clk) then

        if out_ready = '1' then
          ov := '0';
          ol := '0';
          if to_x01(ol) = '1' then
            last_pkt := (others => '0');
          end if;
        end if;

        pkt_ready <= (others => '0');

        -- Select the next index (RR)
        if to_x01(ov) /= '1' then
          -- Priority init.
          for idx in NUM_INPUTS-1 downto 0 loop
            if pkt_valid(idx) = '1' then
              index         := std_logic_vector(to_unsigned(idx, INDEX_WIDTH));
              ov            := '1';
              tag_v         := tag_cfg(TAG_WIDTH*(idx+1)-1 downto TAG_WIDTH*idx);
              last_pkt(idx) := last_pkt(idx) or pkt_last(idx);
              cl            := pkt_last(idx);
              ol            := and_reduce(last_pkt);
            end if;
          end loop;
          --Round-robin arbitration.
          for idx in NUM_INPUTS-1 downto 0 loop
            if pkt_valid(idx) = '1' then
              if idx > to_integer(unsigned(index_r)) then
                index := std_logic_vector(to_unsigned(idx, INDEX_WIDTH));
                ov            := '1';
                tag_v         := tag_cfg(TAG_WIDTH*(idx+1)-1 downto TAG_WIDTH*idx);
                last_pkt(idx) := last_pkt(idx) or pkt_last(idx);
                cl            := pkt_last(idx);
                ol            := and_reduce(last_pkt);
              end if;
            end if;
          end loop;
          if pkt_valid(to_integer(unsigned(index))) = '1' then
            pkt_ready(to_integer(unsigned(index))) <= '1';
          end if;
        end if;

        -- Handle reset.
        if reset = '1' then
          index     := (others => '0');
          index_r   := (others => '0');
          last_pkt  := (others => '0');
          ov        := '0';
        end if;

        index_r   := index;
        cmd_index <= index;
        tag       <= tag_v;
        out_valid <= ov;
        tag_last  <= ol;
        cmd_last  <= cl;
      end if;
    end process;
  end architecture;
