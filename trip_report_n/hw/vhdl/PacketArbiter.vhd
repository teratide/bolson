library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;

entity PacketArbiter is
  generic (
      DATA_WIDTH            : natural := 8;
      NUM_INPUTS            : natural;
      INDEX_WIDTH           : natural;
      DIMENSIONALITY        : natural := 1
      );
  port (
      clk                   : in  std_logic;
      reset                 : in  std_logic;

      in_valid              : in  std_logic_vector(NUM_INPUTS-1 downto 0);
      in_ready              : out std_logic_vector(NUM_INPUTS-1 downto 0);
      in_data               : in  std_logic_vector(DATA_WIDTH*NUM_INPUTS-1 downto 0);
      in_last               : in  std_logic_vector(NUM_INPUTS*DIMENSIONALITY-1 downto 0) := (others => '0');
      in_strb               : in  std_logic_vector(NUM_INPUTS-1 downto 0) := (others => '1');

      in_enable_valid       : in  std_logic := '1';
      in_enable_ready       : out std_logic;
      in_enable             : in  std_logic_vector(NUM_INPUTS-1 downto 0) := (others => '1');

      out_valid             : out std_logic;
      out_ready             : in  std_logic;
      out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
      out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
      out_strb              : out std_logic;

      cmd_valid             : in  std_logic;
      cmd_ready             : out std_logic;
      cmd_index             : in  std_logic_vector(INDEX_WIDTH-1 downto 0);

      last_packet_valid     : out std_logic;
      last_packet_ready     : in  std_logic

  );
end entity;

architecture Implementation of PacketArbiter is

  -- Packet and trsnfer closing 'last' indices.
  constant PKT_LAST : natural := imax(DIMENSIONALITY-2, 0);
  constant TX_LAST  : natural := imax(DIMENSIONALITY-1, 0);

  -- Internal copies ot output signals.
  signal out_last_s             : std_logic_vector(DIMENSIONALITY-1 downto 0);
  signal out_valid_s            : std_logic;
  signal in_ready_s             : std_logic_vector(NUM_INPUTS-1 downto 0);

  -- Whether the current command is in progress.
  signal lock                   : std_logic;

  -- The selected input index by the last handshaked command.
  signal index                  : std_logic_vector(INDEX_WIDTH-1 downto 0);

  -- Counter for keeping track of how many sources have handshaked their last 
  -- packet in a transfer.
  signal last_pkt_cntr_s        : unsigned(INDEX_WIDTH downto 0);

  -- Signals that the currect packet is going the be the last packet globally,
  -- so we will need to close the transfer appropritely and validate the last_packet stream.
  signal glob_last_pkt_s        : std_logic;

  -- The transfer closing last (TX_LAST) may come after the packet closing last (PKT_LAST), in a separate cycle.
  -- Since PKT_LAST unlocks the arbiter, we need a way to pass the element that closes the whole transfer.
  signal outstanding_last       : std_logic;

  -- When a source sends a TX_LAST, we need to increment the last_pkt_cntr counter, but
  -- in case it's not the last packet globally, we need to swallow that TX_LAST.
  signal silent_last            : std_logic_vector(NUM_INPUTS-1 downto 0);

  -- Number of inputs that we expect a transfer from.
  signal inp_en_popcnt          : unsigned(INDEX_WIDTH-1 downto 0);
  signal inp_en_popcnt_v        : std_logic;

  begin

    cmd_proc: process (clk) is
      variable cv            : std_logic;
      variable cr            : std_logic;
      variable iev           : std_logic;
      variable ier           : std_logic;
      variable last_pkt_v    : std_logic;
      variable lock_v        : std_logic;
      variable last_pkt_cntr : unsigned(INDEX_WIDTH downto 0) := (others => '0');
      variable ie_popcnt     : unsigned(INDEX_WIDTH-1 downto 0);
      variable idx           : std_logic_vector(INDEX_WIDTH-1 downto 0);
    begin 

      if rising_edge(clk) then

        if last_packet_ready = '1' then
          if last_pkt_v = '1' then
            last_pkt_v    := '0';
            last_pkt_cntr := (others => '0');
            iev           := '0';
          end if;
        end if;
        
        -- Latch command.
        if to_x01(cr) = '1' then 
          cv          := cmd_valid;
        end if;

        -- Lock on new command.
        if to_x01(cv) = '1' and last_pkt_v = '0' then
          if lock = '0' then
            idx         := cmd_index;
          end if;
          lock_v      := '1';
        end if;

        if to_x01(ier) =  '1' then
          iev          := in_enable_valid;
          ie_popcnt    := (others => '0');
          for i in 0 to NUM_INPUTS-1 loop  
            if(in_enable(i) = '1') then 
              ie_popcnt := ie_popcnt + 1;
            end if;
          end loop;
        end if;

        for idx in 0 to NUM_INPUTS-1 loop
          if in_ready_s(idx) = '1' and in_valid(idx) = '1' and in_last(DIMENSIONALITY*idx + TX_LAST) = '1' then
            last_pkt_cntr := last_pkt_cntr + 1;
          end if;
        end loop;

        if last_pkt_cntr = inp_en_popcnt and iev = '1' then
          last_pkt_v := '1';
        end if;

        if out_valid_s = '1' and out_ready = '1' then
          if out_last_s(PKT_LAST) = '1' then
            lock_v       := '0';
            cv           := '0';
          end if;
        end if;

        -- Handle reset.
        if reset = '1' then
          cv            := '0';
          idx           := (others => '0');
          lock_v        := '0';
          last_pkt_cntr := (others => '0');
          last_pkt_v    := '0';
          iev           := '0';
        end if;

        cr                 := (not cv) and (not lock_v) and (not reset);
        cmd_ready          <= cr and not reset;
        lock               <= lock_v and not reset;
        index              <= idx;
        last_pkt_cntr_s    <= last_pkt_cntr;
        last_packet_valid  <= last_pkt_v and not reset;
        inp_en_popcnt      <= ie_popcnt;
        inp_en_popcnt_v    <= iev;
        ier                := not iev;
        in_enable_ready    <= ier and not reset;
      end if;
    end process;

    global_last_packet_proc: process(last_pkt_cntr_s,inp_en_popcnt_v) is
      begin
        if last_pkt_cntr_s = inp_en_popcnt-1 and inp_en_popcnt_v = '1' then
          glob_last_pkt_s <= '1';
        else
          glob_last_pkt_s <= '0';
        end if;
    end process;

    outstanding_last_proc : process(glob_last_pkt_s, in_last, index) is
        variable idx : integer range 0 to 2**INDEX_WIDTH-1;
      begin
        idx := to_integer(unsigned(index));
        outstanding_last <= glob_last_pkt_s 
                            and in_last(DIMENSIONALITY*idx+TX_LAST)
                            and not in_last(DIMENSIONALITY*idx+PKT_LAST);
    end process;

    silent_last_proc : process(glob_last_pkt_s, in_last, in_strb) is
      variable idx : integer range 0 to 2**INDEX_WIDTH-1;
    begin
        for idx in 0 to NUM_INPUTS-1 loop
          silent_last(idx) <= (not glob_last_pkt_s) 
                              and (not in_strb(idx))
                              and (not in_last(DIMENSIONALITY*idx+PKT_LAST))
                              and in_last(DIMENSIONALITY*idx+TX_LAST);
        end loop;
    end process;

    -- Input mux
    inp_mux_proc: process(index, in_data, in_valid, in_last, in_strb, lock, glob_last_pkt_s, outstanding_last) is
        variable idx : integer range 0 to 2**INDEX_WIDTH-1;
    begin
        idx := to_integer(unsigned(index));
        out_data    <= in_data(DATA_WIDTH*(idx+1)-1 downto DATA_WIDTH*idx);
        out_last_s  <= in_last(DIMENSIONALITY*(idx+1) - 1  downto DIMENSIONALITY*idx);
        out_strb    <= in_strb(idx);

        if lock = '1' or outstanding_last = '1' then
          out_valid_s <= in_valid(idx);
        else
          out_valid_s <= '0';
        end if;
    end process;


    -- Output ready demux
    rdy_demux_proc: process(out_ready, index, lock, silent_last, outstanding_last, in_valid) is
      begin
      for idx in 0 to NUM_INPUTS-1 loop
        if idx = to_integer(unsigned(index)) and (lock = '1' or outstanding_last = '1') then
          in_ready_s(idx) <= out_ready;
        elsif in_valid(idx) ='1' and silent_last(idx) = '1' then
          in_ready_s(idx) <= '1';
        else
          in_ready_s(idx) <= '0';
        end if;
      end loop;
    end process;

    in_ready <= in_ready_s;
    out_valid <= out_valid_s;
    out_last(PKT_LAST downto 0)  <= out_last_s(PKT_LAST  downto 0);
    out_last(TX_LAST)  <= out_last_s(TX_LAST) and glob_last_pkt_s;
  end architecture;
