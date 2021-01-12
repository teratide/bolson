-- Copyright 2021 Teratide B.V.
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

-- Intel OPAE top-level wrapper for Fletcher AXI top-level wrapper
entity OpaeAxiTop is
  generic (
    M_AXI_ADDR_WIDTH  : natural := 48;
    M_AXI_LEN_WIDTH   : natural := 8;
    M_AXI_DATA_WIDTH  : natural := 512;
    M_AXI_ID_WIDTH    : natural := 8;
    M_AXI_WUSER_WIDTH : natural := 8;
    M_AXI_RUSER_WIDTH : natural := 8;
    S_AXI_ADDR_WIDTH  : natural := 18;
    S_AXI_DATA_WIDTH  : natural := 64;
    S_AXI_RID_WIDTH   : natural := 10;
    S_AXI_WID_WIDTH   : natural := 1;
    S_AXI_WUSER_WIDTH : natural := 1;
    S_AXI_RUSER_WIDTH : natural := 1
  );
  port (

    -- Rising-edge-sensitive clock and asynchronous active-low reset.
    clk               : in  std_logic;
    areset_n          : in  std_logic;

    ---------------------------------------------------------------------------
    -- AXI4 master to memory interface
    ---------------------------------------------------------------------------
    -- Write address channel.
    m_axi_awvalid     : out std_logic;
    m_axi_awready     : in  std_logic;
    m_axi_awid        : out std_logic_vector(M_AXI_ID_WIDTH-1 downto 0);
    m_axi_awaddr      : out std_logic_vector(M_AXI_ADDR_WIDTH-1 downto 0);
    m_axi_awlen       : out std_logic_vector(M_AXI_LEN_WIDTH-1 downto 0);
    m_axi_awsize      : out std_logic_vector(2 downto 0);
    m_axi_awburst     : out std_logic_vector(1 downto 0);
    m_axi_awlock      : out std_logic;
    m_axi_awcache     : out std_logic_vector(3 downto 0);
    m_axi_awprot      : out std_logic_vector(2 downto 0);
    m_axi_awuser      : out std_logic_vector(M_AXI_WUSER_WIDTH-1 downto 0);
    m_axi_awqos       : out std_logic_vector(3 downto 0);
    m_axi_awregion    : out std_logic_vector(3 downto 0);

    -- Write data channel.
    m_axi_wvalid      : out std_logic;
    m_axi_wready      : in  std_logic;
    m_axi_wdata       : out std_logic_vector(M_AXI_DATA_WIDTH-1 downto 0);
    m_axi_wstrb       : out std_logic_vector((M_AXI_DATA_WIDTH/8)-1 downto 0);
    m_axi_wlast       : out std_logic;
    m_axi_wuser       : out std_logic_vector(M_AXI_WUSER_WIDTH-1 downto 0);

    -- Write response channel.
    m_axi_bvalid      : in  std_logic;
    m_axi_bready      : out std_logic;
    m_axi_bid         : in  std_logic_vector(M_AXI_ID_WIDTH-1 downto 0);
    m_axi_bresp       : in  std_logic_vector(1 downto 0);
    m_axi_buser       : in  std_logic_vector(M_AXI_WUSER_WIDTH-1 downto 0);

    -- Read address channel.
    m_axi_arvalid     : out std_logic;
    m_axi_arready     : in  std_logic;
    m_axi_arid        : out std_logic_vector(M_AXI_ID_WIDTH-1 downto 0);
    m_axi_araddr      : out std_logic_vector(M_AXI_ADDR_WIDTH-1 downto 0);
    m_axi_arlen       : out std_logic_vector(M_AXI_LEN_WIDTH-1 downto 0);
    m_axi_arsize      : out std_logic_vector(2 downto 0);
    m_axi_arburst     : out std_logic_vector(1 downto 0);
    m_axi_arlock      : out std_logic;
    m_axi_arcache     : out std_logic_vector(3 downto 0);
    m_axi_arprot      : out std_logic_vector(2 downto 0);
    m_axi_aruser      : out std_logic_vector(M_AXI_RUSER_WIDTH-1 downto 0);
    m_axi_arqos       : out std_logic_vector(3 downto 0);
    m_axi_arregion    : out std_logic_vector(3 downto 0);

    -- Read data/response channel.
    m_axi_rvalid      : in  std_logic;
    m_axi_rready      : out std_logic;
    m_axi_rid         : in  std_logic_vector(M_AXI_ID_WIDTH-1 downto 0);
    m_axi_rdata       : in  std_logic_vector(M_AXI_DATA_WIDTH-1 downto 0);
    m_axi_rresp       : in  std_logic_vector(1 downto 0);
    m_axi_ruser       : in  std_logic_vector(M_AXI_RUSER_WIDTH-1 downto 0);
    m_axi_rlast       : in  std_logic;

    ---------------------------------------------------------------------------
    -- AXI4 slave for MMIO access
    ---------------------------------------------------------------------------
    -- Write address channel.
    s_axi_awvalid     : in  std_logic;
    s_axi_awready     : out std_logic;
    s_axi_awid        : in  std_logic_vector(S_AXI_WID_WIDTH-1 downto 0);
    s_axi_awaddr      : in  std_logic_vector(S_AXI_ADDR_WIDTH-1 downto 0);
    s_axi_awsize      : in  std_logic_vector(2 downto 0);
    s_axi_awprot      : in  std_logic_vector(2 downto 0);
    s_axi_awuser      : in  std_logic_vector(S_AXI_WUSER_WIDTH-1 downto 0);

    -- Write data channel.
    s_axi_wvalid      : in  std_logic;
    s_axi_wready      : out std_logic;
    s_axi_wdata       : in  std_logic_vector(S_AXI_DATA_WIDTH-1 downto 0);
    s_axi_wstrb       : in  std_logic_vector((S_AXI_DATA_WIDTH/8)-1 downto 0);
    s_axi_wuser       : in  std_logic_vector(S_AXI_WUSER_WIDTH-1 downto 0);

    -- Write response channel.
    s_axi_bvalid      : out std_logic;
    s_axi_bready      : in  std_logic;
    s_axi_bid         : out std_logic_vector(S_AXI_WID_WIDTH-1 downto 0);
    s_axi_bresp       : out std_logic_vector(1 downto 0);
    s_axi_buser       : out std_logic_vector(S_AXI_WUSER_WIDTH-1 downto 0);

    -- Read address channel.
    s_axi_arvalid     : in  std_logic;
    s_axi_arready     : out std_logic;
    s_axi_arid        : in  std_logic_vector(S_AXI_RID_WIDTH-1 downto 0);
    s_axi_araddr      : in  std_logic_vector(S_AXI_ADDR_WIDTH-1 downto 0);
    s_axi_arsize      : in  std_logic_vector(2 downto 0);
    s_axi_arprot      : in  std_logic_vector(2 downto 0);
    s_axi_aruser      : in  std_logic_vector(S_AXI_RUSER_WIDTH-1 downto 0);

    -- Read data/response channel.
    s_axi_rvalid      : out std_logic;
    s_axi_rready      : in  std_logic;
    s_axi_rid         : out std_logic_vector(S_AXI_RID_WIDTH-1 downto 0);
    s_axi_rdata       : out std_logic_vector(S_AXI_DATA_WIDTH-1 downto 0);
    s_axi_rresp       : out std_logic_vector(1 downto 0);
    s_axi_ruser       : out std_logic_vector(S_AXI_RUSER_WIDTH-1 downto 0)

  );
end OpaeAxiTop;

architecture Behavorial of OpaeAxiTop is

  -- Reset synchronizer signals.
  signal reset_sync   : std_logic_vector(2 downto 0);
  signal reset        : std_logic;

  -- Memory interface bus as generated by Fletcher.
  signal flm_awvalid  : std_logic;
  signal flm_awready  : std_logic;
  signal flm_awaddr   : std_logic_vector(63 downto 0);
  signal flm_awlen    : std_logic_vector(7 downto 0);
  signal flm_awsize   : std_logic_vector(2 downto 0);
  signal flm_awuser   : std_logic_vector(0 downto 0);
  signal flm_wvalid   : std_logic;
  signal flm_wready   : std_logic;
  signal flm_wdata    : std_logic_vector(M_AXI_DATA_WIDTH-1 downto 0);
  signal flm_wstrb    : std_logic_vector((M_AXI_DATA_WIDTH/8)-1 downto 0);
  signal flm_wlast    : std_logic;
  signal flm_bvalid   : std_logic;
  signal flm_bready   : std_logic;
  signal flm_bresp    : std_logic_vector(1 downto 0);
  signal flm_arvalid  : std_logic;
  signal flm_arready  : std_logic;
  signal flm_araddr   : std_logic_vector(63 downto 0);
  signal flm_arlen    : std_logic_vector(7 downto 0);
  signal flm_arsize   : std_logic_vector(2 downto 0);
  signal flm_rvalid   : std_logic;
  signal flm_rready   : std_logic;
  signal flm_rdata    : std_logic_vector(M_AXI_DATA_WIDTH-1 downto 0);
  signal flm_rresp    : std_logic_vector(1 downto 0);
  signal flm_rlast    : std_logic;

  -- Memory interface bus as modified by the write fence generator.
  signal wrf_arvalid  : std_logic;
  signal wrf_arready  : std_logic;
  signal wrf_araddr   : std_logic_vector(63 downto 0);
  signal wrf_arlen    : std_logic_vector(7 downto 0);
  signal wrf_arsize   : std_logic_vector(2 downto 0);
  signal wrf_rvalid   : std_logic;
  signal wrf_rready   : std_logic;
  signal wrf_rdata    : std_logic_vector(M_AXI_DATA_WIDTH-1 downto 0);
  signal wrf_rresp    : std_logic_vector(1 downto 0);
  signal wrf_rlast    : std_logic;
  signal wrf_awvalid  : std_logic;
  signal wrf_awready  : std_logic;
  signal wrf_awaddr   : std_logic_vector(63 downto 0);
  signal wrf_awlen    : std_logic_vector(7 downto 0);
  signal wrf_awsize   : std_logic_vector(2 downto 0);
  signal wrf_awuser   : std_logic_vector(M_AXI_WUSER_WIDTH-1 downto 0);
  signal wrf_wvalid   : std_logic;
  signal wrf_wready   : std_logic;
  signal wrf_wdata    : std_logic_vector(M_AXI_DATA_WIDTH-1 downto 0);
  signal wrf_wlast    : std_logic;
  signal wrf_wstrb    : std_logic_vector((M_AXI_DATA_WIDTH/8)-1 downto 0);
  signal wrf_bvalid   : std_logic;
  signal wrf_bready   : std_logic;
  signal wrf_bresp    : std_logic_vector(1 downto 0);

  -- MMIO interface bus as generated by Fletcher.
  signal fls_awvalid  : std_logic;
  signal fls_awready  : std_logic;
  signal fls_awaddr   : std_logic_vector(31 downto 0);
  signal fls_wvalid   : std_logic;
  signal fls_wready   : std_logic;
  signal fls_wdata    : std_logic_vector(S_AXI_DATA_WIDTH-1 downto 0);
  signal fls_wstrb    : std_logic_vector((S_AXI_DATA_WIDTH/8)-1 downto 0);
  signal fls_bvalid   : std_logic;
  signal fls_bready   : std_logic;
  signal fls_bresp    : std_logic_vector(1 downto 0);
  signal fls_arvalid  : std_logic;
  signal fls_arready  : std_logic;
  signal fls_araddr   : std_logic_vector(31 downto 0);
  signal fls_rvalid   : std_logic;
  signal fls_rready   : std_logic;
  signal fls_rdata    : std_logic_vector(S_AXI_DATA_WIDTH-1 downto 0);
  signal fls_rresp    : std_logic_vector(1 downto 0);

  -- MMIO bus ID buffer signals.
  signal buf_awvalid  : std_logic;
  signal buf_awready  : std_logic;
  signal buf_awmeta   : std_logic_vector(S_AXI_WID_WIDTH+S_AXI_WUSER_WIDTH-1 downto 0);
  signal buf_bvalid   : std_logic;
  signal buf_bready   : std_logic;
  signal buf_bmeta    : std_logic_vector(S_AXI_WID_WIDTH+S_AXI_WUSER_WIDTH-1 downto 0);
  signal buf_arvalid  : std_logic;
  signal buf_arready  : std_logic;
  signal buf_armeta   : std_logic_vector(S_AXI_RID_WIDTH+S_AXI_RUSER_WIDTH-1 downto 0);
  signal buf_rvalid   : std_logic;
  signal buf_rready   : std_logic;
  signal buf_rmeta    : std_logic_vector(S_AXI_RID_WIDTH+S_AXI_RUSER_WIDTH-1 downto 0);

begin

  -----------------------------------------------------------------------------
  -- Instantiate the Fletchgen-generated design
  -----------------------------------------------------------------------------
  fletcher_inst: entity work.AxiTop
    generic map (
      REG_WIDTH       => S_AXI_DATA_WIDTH,
      BUS_DATA_WIDTH  => M_AXI_DATA_WIDTH
    )
    port map (

      -- Kernel clock domain.
      kcd_clk         => clk,
      kcd_reset       => reset,

      -- Bus clock domain.
      bcd_clk         => clk,
      bcd_reset       => reset,

      -- AXI4 master as Host Memory Interface.
      m_axi_awvalid   => flm_awvalid,
      m_axi_awready   => flm_awready,
      m_axi_awaddr    => flm_awaddr,
      m_axi_awlen     => flm_awlen,
      m_axi_awsize    => flm_awsize,
      m_axi_awuser    => flm_awuser,
      m_axi_wvalid    => flm_wvalid,
      m_axi_wready    => flm_wready,
      m_axi_wdata     => flm_wdata,
      m_axi_wstrb     => flm_wstrb,
      m_axi_wlast     => flm_wlast,
      m_axi_bvalid    => flm_bvalid,
      m_axi_bready    => flm_bready,
      m_axi_bresp     => flm_bresp,
      m_axi_arvalid   => flm_arvalid,
      m_axi_arready   => flm_arready,
      m_axi_araddr    => flm_araddr,
      m_axi_arlen     => flm_arlen,
      m_axi_arsize    => flm_arsize,
      m_axi_rvalid    => flm_rvalid,
      m_axi_rready    => flm_rready,
      m_axi_rdata     => flm_rdata,
      m_axi_rresp     => flm_rresp,
      m_axi_rlast     => flm_rlast,

      -- AXI4-lite Slave as MMIO interface.
      s_axi_awvalid   => fls_awvalid,
      s_axi_awready   => fls_awready,
      s_axi_awaddr    => fls_awaddr,
      s_axi_wvalid    => fls_wvalid,
      s_axi_wready    => fls_wready,
      s_axi_wdata     => fls_wdata,
      s_axi_wstrb     => fls_wstrb,
      s_axi_bvalid    => fls_bvalid,
      s_axi_bready    => fls_bready,
      s_axi_bresp     => fls_bresp,
      s_axi_arvalid   => fls_arvalid,
      s_axi_arready   => fls_arready,
      s_axi_araddr    => fls_araddr,
      s_axi_rvalid    => fls_rvalid,
      s_axi_rready    => fls_rready,
      s_axi_rdata     => fls_rdata,
      s_axi_rresp     => fls_rresp

    );

  -----------------------------------------------------------------------------
  -- Convert clock/reset signal conventions
  -----------------------------------------------------------------------------
  reset_sync_proc: process (clk, areset_n) is
  begin
    if areset_n = '1' then
      if rising_edge(clk) then
        reset_sync <= reset_sync(1 downto 0) & "0";
      end if;
    else
      reset_sync <= (others => '1');
    end if;
  end process;

  reset <= reset_sync(2);

  -----------------------------------------------------------------------------
  -- Convert memory access bus signal conventions
  -----------------------------------------------------------------------------
  -- Instantiate write fence generator.
  fence_gen_inst: entity work.WriteFenceGenerator
    generic map (
      BUS_ADDR_WIDTH  => 64,
      BUS_DATA_WIDTH  => M_AXI_DATA_WIDTH,
      BUS_LEN_WIDTH   => 8,
      BUS_WUSER_WIDTH => M_AXI_WUSER_WIDTH
    )
    port map (

      -- Bus clock domain.
      clk           => clk,
      reset         => reset,

      -- AXI4 master to OPAE shell.
      m_axi_awvalid => wrf_awvalid,
      m_axi_awready => wrf_awready,
      m_axi_awaddr  => wrf_awaddr,
      m_axi_awlen   => wrf_awlen,
      m_axi_awsize  => wrf_awsize,
      m_axi_awuser  => wrf_awuser,
      m_axi_wvalid  => wrf_wvalid,
      m_axi_wready  => wrf_wready,
      m_axi_wdata   => wrf_wdata,
      m_axi_wlast   => wrf_wlast,
      m_axi_wstrb   => wrf_wstrb,
      m_axi_bvalid  => wrf_bvalid,
      m_axi_bready  => wrf_bready,
      m_axi_bresp   => wrf_bresp,
      m_axi_arvalid => wrf_arvalid,
      m_axi_arready => wrf_arready,
      m_axi_araddr  => wrf_araddr,
      m_axi_arlen   => wrf_arlen,
      m_axi_arsize  => wrf_arsize,
      m_axi_rvalid  => wrf_rvalid,
      m_axi_rready  => wrf_rready,
      m_axi_rdata   => wrf_rdata,
      m_axi_rresp   => wrf_rresp,
      m_axi_rlast   => wrf_rlast,

      -- AXI4 slave for Fletcher to access
      s_axi_awvalid => flm_awvalid,
      s_axi_awready => flm_awready,
      s_axi_awaddr  => flm_awaddr,
      s_axi_awlen   => flm_awlen,
      s_axi_awsize  => flm_awsize,
      s_axi_awuser  => flm_awuser,
      s_axi_wvalid  => flm_wvalid,
      s_axi_wready  => flm_wready,
      s_axi_wdata   => flm_wdata,
      s_axi_wstrb   => flm_wstrb,
      s_axi_wlast   => flm_wlast,
      s_axi_bvalid  => flm_bvalid,
      s_axi_bready  => flm_bready,
      s_axi_bresp   => flm_bresp,
      s_axi_arvalid => flm_arvalid,
      s_axi_arready => flm_arready,
      s_axi_araddr  => flm_araddr,
      s_axi_arlen   => flm_arlen,
      s_axi_arsize  => flm_arsize,
      s_axi_rvalid  => flm_rvalid,
      s_axi_rready  => flm_rready,
      s_axi_rdata   => flm_rdata,
      s_axi_rresp   => flm_rresp,
      s_axi_rlast   => flm_rlast

    );

  -- Convert between Fletcher and OPAE signal conventions for memory access
  -- bus.
  m_axi_awvalid     <= wrf_awvalid;
  wrf_awready       <= m_axi_awready;
  m_axi_awid        <= (others => '0');
  m_axi_awaddr      <= std_logic_vector(resize(unsigned(wrf_awaddr), M_AXI_ADDR_WIDTH));
  m_axi_awlen       <= std_logic_vector(resize(unsigned(wrf_awlen), M_AXI_LEN_WIDTH));
  m_axi_awsize      <= wrf_awsize;
  m_axi_awburst     <= "01";
  m_axi_awlock      <= '0';
  m_axi_awcache     <= "0000";
  m_axi_awprot      <= "000";
  m_axi_awuser      <= wrf_awuser;
  m_axi_awqos       <= "0000";
  m_axi_awregion    <= "0000";

  m_axi_wvalid      <= wrf_wvalid;
  wrf_wready        <= m_axi_wready;
  m_axi_wdata       <= wrf_wdata;
  m_axi_wstrb       <= wrf_wstrb;
  m_axi_wlast       <= wrf_wlast;
  m_axi_wuser       <= (others => '0');

  wrf_bvalid        <= m_axi_bvalid;
  m_axi_bready      <= wrf_wready;
  wrf_bresp         <= m_axi_bresp;

  m_axi_arvalid     <= wrf_arvalid;
  wrf_arready       <= m_axi_arready;
  m_axi_arid        <= (others => '0');
  m_axi_araddr      <= std_logic_vector(resize(unsigned(wrf_araddr), M_AXI_ADDR_WIDTH));
  m_axi_arlen       <= std_logic_vector(resize(unsigned(wrf_arlen), M_AXI_LEN_WIDTH));
  m_axi_arsize      <= wrf_arsize;
  m_axi_arburst     <= "01";
  m_axi_arlock      <= '0';
  m_axi_arcache     <= "0000";
  m_axi_arprot      <= "000";
  m_axi_aruser      <= (others => '0');
  m_axi_arqos       <= "0000";
  m_axi_arregion    <= "0000";

  wrf_rvalid        <= m_axi_rvalid;
  m_axi_rready      <= wrf_rready;
  wrf_rdata         <= m_axi_rdata;
  wrf_rresp         <= m_axi_rresp;
  wrf_rlast         <= m_axi_rlast;

  -----------------------------------------------------------------------------
  -- Convert MMIO access bus signal conventions
  -----------------------------------------------------------------------------
  -- MMIO bus ID field buffers. OPAE uses ID tags on the MMIO bus, which the
  -- MMIO slave must propagate from request to response. Fletcher doesn't
  -- have the means to do that, so we have to do it using some buffering here.
  buf_aw_sync: StreamSync
    generic map (
      NUM_INPUTS    => 1,
      NUM_OUTPUTS   => 2
    )
    port map (
      clk           => clk,
      reset         => reset,
      in_valid(0)   => s_axi_awvalid,
      in_ready(0)   => s_axi_awready,
      out_valid(1)  => fls_awvalid,
      out_valid(0)  => buf_awvalid,
      out_ready(1)  => fls_awready,
      out_ready(0)  => buf_awready
    );

  buf_w_inst: StreamBuffer
    generic map (
      MIN_DEPTH     => 4,
      DATA_WIDTH    => S_AXI_WID_WIDTH + S_AXI_WUSER_WIDTH
    )
    port map (
      clk           => clk,
      reset         => reset,
      in_valid      => buf_awvalid,
      in_ready      => buf_awready,
      in_data       => buf_awmeta,
      out_valid     => buf_bvalid,
      out_ready     => buf_bready,
      out_data      => buf_bmeta
    );

  buf_b_sync: StreamSync
    generic map (
      NUM_INPUTS    => 2,
      NUM_OUTPUTS   => 1
    )
    port map (
      clk           => clk,
      reset         => reset,
      in_valid(1)   => fls_bvalid,
      in_valid(0)   => buf_bvalid,
      in_ready(1)   => fls_bready,
      in_ready(0)   => buf_bready,
      out_valid(0)  => s_axi_bvalid,
      out_ready(0)  => s_axi_bready
    );

  buf_ar_sync: StreamSync
    generic map (
      NUM_INPUTS    => 1,
      NUM_OUTPUTS   => 2
    )
    port map (
      clk           => clk,
      reset         => reset,
      in_valid(0)   => s_axi_arvalid,
      in_ready(0)   => s_axi_arready,
      out_valid(1)  => fls_arvalid,
      out_valid(0)  => buf_arvalid,
      out_ready(1)  => fls_arready,
      out_ready(0)  => buf_arready
    );

  buf_r_inst: StreamBuffer
    generic map (
      MIN_DEPTH     => 4,
      DATA_WIDTH    => S_AXI_RID_WIDTH + S_AXI_RUSER_WIDTH
    )
    port map (
      clk           => clk,
      reset         => reset,
      in_valid      => buf_arvalid,
      in_ready      => buf_arready,
      in_data       => buf_armeta,
      out_valid     => buf_rvalid,
      out_ready     => buf_rready,
      out_data      => buf_rmeta
    );

  buf_r_sync: StreamSync
    generic map (
      NUM_INPUTS    => 2,
      NUM_OUTPUTS   => 1
    )
    port map (
      clk           => clk,
      reset         => reset,
      in_valid(1)   => fls_rvalid,
      in_valid(0)   => buf_rvalid,
      in_ready(1)   => fls_rready,
      in_ready(0)   => buf_rready,
      out_valid(0)  => s_axi_rvalid,
      out_ready(0)  => s_axi_rready
    );

  fls_awaddr        <= std_logic_vector(resize(unsigned(s_axi_awaddr), 32));
  buf_awmeta        <= s_axi_awuser & s_axi_awid;

  fls_wvalid        <= s_axi_wvalid;
  s_axi_wready      <= fls_wready;
  fls_wdata         <= s_axi_wdata;
  fls_wstrb         <= s_axi_wstrb;

  s_axi_bid         <= buf_bmeta(S_AXI_WID_WIDTH-1 downto 0);
  s_axi_bresp       <= fls_bresp;
  s_axi_buser       <= buf_bmeta(S_AXI_WUSER_WIDTH+S_AXI_WID_WIDTH-1 downto S_AXI_WID_WIDTH);

  fls_araddr        <= std_logic_vector(resize(unsigned(s_axi_araddr), 32));
  buf_armeta        <= s_axi_aruser & s_axi_arid;

  s_axi_rid         <= buf_rmeta(S_AXI_RID_WIDTH-1 downto 0);
  s_axi_rdata       <= fls_rdata;
  s_axi_rresp       <= fls_rresp;
  s_axi_ruser       <= buf_rmeta(S_AXI_RUSER_WIDTH+S_AXI_RID_WIDTH-1 downto S_AXI_RID_WIDTH);

end architecture;
