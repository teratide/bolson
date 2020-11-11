library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

-- sometimes you just need some dingen and extreme prejudice
-- life really do be like that sometimes - JvS

-- When placed in capture mode, engine waits for data & trig_mask to equal
-- trig_cmp, and then starts capturing. Data capture starts one cycle prior
-- to the trigger, so you can see the cycle leading up to it. Then, every
-- time the data signals selected by capt_mask toggle, the entire data
-- vector is written to memory, along with a counter value indicating how
-- long its been since the previous capture. When this counter overflows,
-- data capture is forced regardless of capt_mask.
entity dingen_vhd is
  generic (
    -- your mileage may vary if you change these;

    -- number of signals to trace
    DATA_WIDTH  : natural := 52;

    -- 2**this cycles of inactivity per record; also this + DATA_WIDTH is the
    -- size of the control and status register interface
    COUNT_WIDTH : natural := 12;

    -- log2 of the depth of the capture memory, must be less than the other two
    -- parameters
    DEPTH_LOG2  : natural := 10

  );
  port (

    -- if you don't know what this is, you have bigger problems
    clk     : in std_logic;

    -- tie to bullshit under test
    data    : in std_logic_vector(DATA_WIDTH - 1 downto 0);

    -- tie to MMIO registers that hopefully do work; status reflects what is
    -- asked via control Some Time After (TM) control is modified
    control : in std_logic_vector(DATA_WIDTH + COUNT_WIDTH - 1 downto 0);
    status  : out std_logic_vector(DATA_WIDTH + COUNT_WIDTH - 1 downto 0)

    -- control & status values:
    --
    -- |                  control                   |                  status                   | what                    |
    -- | COUNT_WIDTH MSBs | DATA_WIDTH LSBs         | COUNT_WIDTH MSBs      | DATA_WIDTH LSBs   |                         |
    -- |------------------|-------------------------|-----------------------|-------------------|-------------------------|
    -- | 000.....         | -                       | 0                     | Input signals     | Idle, extreme prejudice |
    -- | 00100...         | New value for capt_mask | 0                     | Register check    | Write capt_mask         |
    -- | 00101...         | New value for trig_mask | 0                     | Register check    | Write trig_mask         |
    -- | 00110...         | New value for trig_cmp  | 0                     | Register check    | Write trig_cmp          |
    -- | 00111...         | -                       | 0                     | Register check    | Extreme prejudice       |
    -- | 01......         | Address to read         | Cycles since prev - 1 | New (masked) data | Waveform readout        |
    -- | 1.......         | -                       | Done & current addr.  | Input signals     | Capture                 |
    --
    -- When placed in capture mode, engine waits for

  );
end entity;

architecture panic of dingen_vhd is

  -- Control vector split into useful pieces, also registered for timing.
  signal ctrl_cmd   : unsigned(COUNT_WIDTH - 1 downto 0);
  signal ctrl_data  : std_logic_vector(DATA_WIDTH - 1 downto 0);

  -- Control registers.
  signal capt_mask  : std_logic_vector(DATA_WIDTH - 1 downto 0);
  signal trig_mask  : std_logic_vector(DATA_WIDTH - 1 downto 0);
  signal trig_cmp   : std_logic_vector(DATA_WIDTH - 1 downto 0);

  -- Datapath.
  signal data_r     : std_logic_vector(DATA_WIDTH - 1 downto 0);
  signal data_rr    : std_logic_vector(DATA_WIDTH - 1 downto 0);
  signal data_final : std_logic_vector(DATA_WIDTH - 1 downto 0);
  signal trigger    : std_logic;
  signal data_edge  : std_logic;

  -- Capture engine registers.
  signal armed      : std_logic;
  signal done       : std_logic;
  signal counter    : unsigned(COUNT_WIDTH - 1 downto 0);

  -- Storage memory.
  type mem_type is array (natural range <>) of std_logic_vector(DATA_WIDTH + COUNT_WIDTH - 1 downto 0);
  signal mem        : mem_type(0 to 2 ** DEPTH_LOG2 - 1);
  signal address    : unsigned(DEPTH_LOG2 - 1 downto 0);
  signal write_data : std_logic_vector(DATA_WIDTH + COUNT_WIDTH - 1 downto 0);
  signal write_ena  : std_logic;
  signal read_data  : std_logic_vector(DATA_WIDTH + COUNT_WIDTH - 1 downto 0);

begin

  ctrl_proc : process (clk) is
  begin
    if rising_edge(clk) then

      -- Input registers to avoid timing kokolores and datapath.
      ctrl_cmd  <= unsigned(control(DATA_WIDTH + COUNT_WIDTH - 1 downto DATA_WIDTH));
      ctrl_data <= control(DATA_WIDTH - 1 downto 0);
      data_r    <= data;
      --
      if (data_r and trig_mask) = trig_cmp then
        trigger <= '1';
      else
        trigger <= '0';
      end if;
      data_rr    <= data_r;
      --
      data_final <= data_rr;
      --
      if (data_final and capt_mask) /= (data_rr and capt_mask) then
        data_edge <= '1';
      else
        data_edge <= '0';
      end if;

      -- Write control signals for memory.
      write_ena                                                  <= '0';
      write_data(DATA_WIDTH + COUNT_WIDTH - 1 downto DATA_WIDTH) <= std_logic_vector(counter);
      write_data(DATA_WIDTH - 1 downto 0)                        <= data_final;

      if ctrl_cmd(COUNT_WIDTH - 1) = '1' then

        -- Armed/capture/complete mode.
        if done = '0' then
          if trigger = '1' then
            armed     <= '1';
            write_ena <= '1';
          end if;
          if armed = '1' then
            counter <= counter + 1;
            if data_edge = '1' or ((not counter) = 0) then
              write_ena <= '1';
              counter   <= (others => '0');
              address   <= address + 1;
              if (not address) = 0 then
                done <= '1';
              end if;
            end if;
          else
            address <= (others => '0');
          end if;
        end if;

        -- Read out done, next write address, and current signals.
        status                                                <= (others => done);
        status(DEPTH_LOG2 + DATA_WIDTH - 1 downto DATA_WIDTH) <= std_logic_vector(address);
        status(DATA_WIDTH - 1 downto 0)                       <= data_r;

      else

        -- Reset capture engine.
        armed   <= '0';
        done    <= '0';
        counter <= (others => '0');

        -- Assign address to the LSBs of the data part of the control register
        -- for readout.
        address <= unsigned(ctrl_data(DEPTH_LOG2 - 1 downto 0));

        if ctrl_cmd(COUNT_WIDTH - 2) = '1' then

          -- Readout mode.
          status <= read_data;

        elsif ctrl_cmd(COUNT_WIDTH - 3) = '1' then

          -- Control register write mode.
          if ctrl_cmd(COUNT_WIDTH - 4 downto COUNT_WIDTH - 5) = "00" then
            capt_mask <= ctrl_data;
          elsif ctrl_cmd(COUNT_WIDTH - 4 downto COUNT_WIDTH - 5) = "01" then
            trig_mask <= ctrl_data;
          elsif ctrl_cmd(COUNT_WIDTH - 4 downto COUNT_WIDTH - 5) = "10" then
            trig_cmp <= ctrl_data;
          end if;
          -- case ctrl_cmd(COUNT_WIDTH - 4 downto COUNT_WIDTH - 5) is
          --   when "00"   => capt_mask <= ctrl_data;
          --   when "01"   => trig_mask <= ctrl_data;
          --   when "10"   => trig_cmp  <= ctrl_data;
          --   when others => null;
          -- end case;

          -- Readout register checksum.
          status                          <= (others => '0');
          status(DATA_WIDTH - 1 downto 0) <= capt_mask xor trig_mask xor trig_cmp;

        else

          -- Readout current signals.
          status                          <= (others => '0');
          status(DATA_WIDTH - 1 downto 0) <= data_r;

        end if;
      end if;
    end if;
  end process;

  mem_proc : process (clk) is
  begin
    if rising_edge(clk) then
      if write_ena = '1' then
        mem(to_integer(address)) <= write_data;
      else
        read_data <= mem(to_integer(address));
      end if;
    end if;
  end process;

end architecture;