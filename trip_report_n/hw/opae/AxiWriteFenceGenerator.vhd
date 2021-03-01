-- Copyright 2018 Delft University of Technology
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;
use work.UtilConv_pkg.all;

entity WriteFenceGenerator is
  generic (
    BUS_ADDR_WIDTH  : natural := 32;
    BUS_DATA_WIDTH  : natural := 32;
    BUS_LEN_WIDTH   : natural := 8;
    BUS_WUSER_WIDTH : natural := 8
  );
  port (
    clk           :  in std_logic;
    reset         :  in std_logic;

    ---------------------------------------------------------------------------
    -- AXI4 master to OPAE shell
    ---------------------------------------------------------------------------
    -- Write sync is accomplished by sending a write fence request, which
    -- consists of:
    --  - a write request with:
    --     - addr: 0
    --     - len: 0
    --     - size: full bus word
    --     - user: 2 (bit 1 set)
    --  - a single write transfer with:
    --     - data: 0
    --     - strb: 0
    --     - last: 1
    -- resulting in a write response acknowledgement when synchronization
    -- completes.

    -- Write address channel.
    m_axi_awvalid : out std_logic;
    m_axi_awready : in  std_logic;
    m_axi_awaddr  : out std_logic_vector(BUS_ADDR_WIDTH-1 downto 0);
    m_axi_awlen   : out std_logic_vector(BUS_LEN_WIDTH-1 downto 0);
    m_axi_awsize  : out std_logic_vector(2 downto 0);
    m_axi_awuser  : out std_logic_vector(BUS_WUSER_WIDTH-1 downto 0);

    -- Write data channel.
    m_axi_wvalid  : out std_logic;
    m_axi_wready  : in  std_logic;
    m_axi_wdata   : out std_logic_vector(BUS_DATA_WIDTH-1 downto 0);
    m_axi_wlast   : out std_logic;
    m_axi_wstrb   : out std_logic_vector((BUS_DATA_WIDTH/8)-1 downto 0);

    -- Write response channel.
    m_axi_bvalid  : in  std_logic;
    m_axi_bready  : out std_logic;
    m_axi_bresp   : in  std_logic_vector(1 downto 0);

    -- Read address channel.
    m_axi_arvalid : out std_logic;
    m_axi_arready : in  std_logic;
    m_axi_araddr  : out std_logic_vector(BUS_ADDR_WIDTH-1 downto 0);
    m_axi_arlen   : out std_logic_vector(BUS_LEN_WIDTH-1 downto 0);
    m_axi_arsize  : out std_logic_vector(2 downto 0);

    -- Read data channel.
    m_axi_rvalid  : in  std_logic;
    m_axi_rready  : out std_logic;
    m_axi_rdata   : in  std_logic_vector(BUS_DATA_WIDTH-1 downto 0);
    m_axi_rresp   : in  std_logic_vector(1 downto 0);
    m_axi_rlast   : in  std_logic;

    ---------------------------------------------------------------------------
    -- AXI4 slave for Fletcher to access
    ---------------------------------------------------------------------------
    -- Fletcher requests write synchronization by setting its single awuser bit
    -- to 1 for the last write in a sequence. Unlike OPAE, this request may
    -- also contain valid write data.

    -- Write address channel.
    s_axi_awvalid : in  std_logic;
    s_axi_awready : out std_logic;
    s_axi_awaddr  : in  std_logic_vector(BUS_ADDR_WIDTH-1 downto 0);
    s_axi_awlen   : in  std_logic_vector(BUS_LEN_WIDTH-1 downto 0);
    s_axi_awsize  : in  std_logic_vector(2 downto 0);
    s_axi_awuser  : in  std_logic_vector(0 downto 0);

    -- Write data channel.
    s_axi_wvalid  : in  std_logic;
    s_axi_wready  : out std_logic;
    s_axi_wdata   : in  std_logic_vector(BUS_DATA_WIDTH-1 downto 0);
    s_axi_wstrb   : in  std_logic_vector((BUS_DATA_WIDTH/8)-1 downto 0);
    s_axi_wlast   : in  std_logic;

    -- Write response channel.
    s_axi_bvalid  : out std_logic;
    s_axi_bready  : in  std_logic;
    s_axi_bresp   : out std_logic_vector(1 downto 0);

    -- Read address channel.
    s_axi_arvalid : in  std_logic;
    s_axi_arready : out std_logic;
    s_axi_araddr  : in  std_logic_vector(BUS_ADDR_WIDTH-1 downto 0);
    s_axi_arlen   : in  std_logic_vector(BUS_LEN_WIDTH-1 downto 0);
    s_axi_arsize  : in  std_logic_vector(2 downto 0);

    -- Read data channel.
    s_axi_rvalid  : out std_logic;
    s_axi_rready  : in  std_logic;
    s_axi_rdata   : out std_logic_vector(BUS_DATA_WIDTH-1 downto 0);
    s_axi_rresp   : out std_logic_vector(1 downto 0);
    s_axi_rlast   : out std_logic

  );
end entity;

architecture behavioral of WriteFenceGenerator is

  type aw_type is record
    valid : std_logic;
    addr  : std_logic_vector(BUS_ADDR_WIDTH-1 downto 0);
    len   : std_logic_vector(BUS_LEN_WIDTH-1 downto 0);
    size  : std_logic_vector(2 downto 0);
    user  : std_logic_vector(BUS_WUSER_WIDTH-1 downto 0);
  end record;

  type w_type is record
    valid : std_logic;
    data  : std_logic_vector(BUS_DATA_WIDTH-1 downto 0);
    strb  : std_logic_vector(BUS_DATA_WIDTH/8-1 downto 0);
    last  : std_logic;
  end record;

  type b_type is record
    valid : std_logic;
    resp  : std_logic_vector(1 downto 0);
  end record;

  type marker_type is record
    valid : std_logic;
    mark  : std_logic;
  end record;

  type ready_type is record
    ready : std_logic;
  end record;

  -- Address channel to data channel fence marker stream.
  signal a2d  : marker_type;
  signal d2a  : ready_type;

  -- Address channel to outstanding request buffer fence marker stream.
  signal a2b  : marker_type;
  signal b2a  : ready_type;

  -- Outstanding request buffer to response channel fence marker stream.
  signal b2r  : marker_type;
  signal r2b  : ready_type;

begin

  -- The read channels need no modification.
  m_axi_arvalid <= s_axi_arvalid;
  s_axi_arready <= m_axi_arready;
  m_axi_araddr  <= s_axi_araddr;
  m_axi_arlen   <= s_axi_arlen;
  m_axi_arsize  <= s_axi_arsize;
  s_axi_rvalid  <= m_axi_rvalid;
  m_axi_rready  <= s_axi_rready;
  s_axi_rdata   <= m_axi_rdata;
  s_axi_rresp   <= m_axi_rresp;
  s_axi_rlast   <= m_axi_rlast;

  aw_proc: process (clk) is
    variable s              : aw_type;
    variable m              : aw_type;
    variable d              : marker_type;
    variable r              : marker_type;
    variable fence_pending  : std_logic;
  begin
    if rising_edge(clk) then

      -- Stream handshake boilerplate.
      if s.valid = '0' then
        s.valid   := s_axi_awvalid;
        s.addr    := s_axi_awaddr;
        s.len     := s_axi_awlen;
        s.size    := s_axi_awsize;
        s.user(0) := s_axi_awuser(0);
      end if;
      if m_axi_awready = '1' then
        m.valid := '0';
      end if;
      if d2a.ready = '1' then
        d.valid := '0';
      end if;
      if b2a.ready = '1' then
        r.valid := '0';
      end if;

      -- If a fence operation is pending and the master output holding register
      -- is ready, send the fence before handling the next input.
      if fence_pending = '1' and m.valid = '0' then
        m.addr  := (others => '0');
        m.len   := (others => '0');
        m.size  := slv(u(log2ceil(BUS_DATA_WIDTH/8), 3));
        m.user  := (1 => '1', others => '0');
        m.valid := '1';
        fence_pending := '0';
      end if;

      -- Propagate write requests and distribute the fence flag.
      if s.valid = '1' and m.valid = '0' and d.valid = '0' and r.valid = '0' then
        m       := s;
        m.user  := (others => '0');
        d.mark  := s.user(0);
        d.valid := '1';
        r.mark  := s.user(0);
        r.valid := '1';
        fence_pending := s.user(0);
        s.valid := '0';
      end if;

      -- Handle reset.
      if reset = '1' then
        s.valid := '0';
        m.valid := '0';
        d.valid := '0';
        r.valid := '0';
        fence_pending := '0';
      end if;

      -- Assign outputs.
      s_axi_awready <= not s.valid;
      m_axi_awvalid <= m.valid;
      m_axi_awaddr  <= m.addr;
      m_axi_awlen   <= m.len;
      m_axi_awsize  <= m.size;
      m_axi_awuser  <= m.user;
      a2d           <= d;
      a2b           <= r;

    end if;
  end process;

  w_proc: process (clk) is
    variable s              : w_type;
    variable m              : w_type;
    variable f              : marker_type;
    variable fence_pending  : std_logic;
  begin
    if rising_edge(clk) then

      -- Stream handshake boilerplate.
      if s.valid = '0' then
        s.valid   := s_axi_wvalid;
        s.data    := s_axi_wdata;
        s.strb    := s_axi_wstrb;
        s.last    := s_axi_wlast;
      end if;
      if m_axi_wready = '1' then
        m.valid := '0';
      end if;
      if f.valid = '0' then
        f := a2d;
      end if;

      -- If a fence operation is pending and the master output holding register
      -- is ready, send the dummy data for the fence before handling the next
      -- input.
      if fence_pending = '1' and m.valid = '0' then
        m.data  := (others => '0');
        m.strb  := (others => '0');
        m.last  := '1';
        m.valid := '1';
        fence_pending := '0';
      end if;

      -- Propagate write data. We only wait for and use the fence marker stream
      -- when the last flag is set, allowing the data burst to start before the
      -- marker stream is up to speed.
      if s.valid = '1' and m.valid = '0' and (s.last = '0' or f.valid = '1') then
        if s.last = '1' then
          fence_pending := f.mark;
          f.valid := '0';
        end if;
        m := s;
        s.valid := '0';
      end if;

      -- Handle reset.
      if reset = '1' then
        s.valid := '0';
        m.valid := '0';
        f.valid := '0';
        fence_pending := '0';
      end if;

      -- Assign outputs.
      s_axi_wready  <= not s.valid;
      m_axi_wvalid  <= m.valid;
      m_axi_wdata   <= m.data;
      m_axi_wstrb   <= m.strb;
      m_axi_wlast   <= m.last;
      d2a.ready     <= not f.valid;

    end if;
  end process;

  outst_req_fifo: StreamFIFO
    generic map (
      DEPTH_LOG2  => 16,
      DATA_WIDTH  => 1
    )
    port map (
      in_clk      => clk,
      in_reset    => reset,
      in_valid    => a2b.valid,
      in_ready    => b2a.ready,
      in_data(0)  => a2b.mark,

      out_clk     => clk,
      out_reset   => reset,
      out_valid   => b2r.valid,
      out_ready   => r2b.ready,
      out_data(0) => b2r.mark
    );

  b_proc: process (clk) is
    variable s              : b_type;
    variable m              : b_type;
    variable f              : marker_type;
    variable fence_pending  : std_logic;
    variable resp           : std_logic_vector(1 downto 0);
  begin
    if rising_edge(clk) then

      -- Stream handshake boilerplate.
      if s_axi_bready = '1' then
        s.valid := '0';
      end if;
      if m.valid = '0' then
        m.valid := m_axi_bvalid;
        m.resp  := m_axi_bresp;
      end if;
      if f.valid = '0' then
        f := b2r;
      end if;

      -- If a fence operation is pending, propagate the response without
      -- popping the marker stream.
      if fence_pending = '1' and s.valid = '0' and m.valid = '1' then
        s := m;
        if resp /= "00" then
          s.resp := resp;
        end if;
        m.valid := '0';
        fence_pending := '0';
        resp := "00";
      end if;

      -- Pop incoming responses along with the marker stream. If the marker
      -- is set, the response belongs to a data write request that Fletcher
      -- sent along with a synchronization request. In that case we DON'T
      -- want to propagate this response; we want to propagate the response of
      -- the fence instead, to ensure both are complete when Fletcher receives
      -- the response.
      if s.valid = '0' and m.valid = '1' and f.valid = '1' then
        if f.mark = '1' then
          fence_pending := '1';
          resp := s.resp;
        else
          s := m;
        end if;
        f.valid := '0';
        m.valid := '0';
      end if;

      -- Handle reset.
      if reset = '1' then
        s.valid := '0';
        m.valid := '0';
        f.valid := '0';
        fence_pending := '0';
        resp := "00";
      end if;

      -- Assign outputs.
      s_axi_bvalid  <= s.valid;
      s_axi_bresp   <= s.resp;
      m_axi_bready  <= not m.valid;
      r2b.ready     <= not f.valid;

    end if;
  end process;

end architecture;
