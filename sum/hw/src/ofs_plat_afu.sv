`include "ofs_plat_if.vh"
`include "afu_json_info.vh"

module ofs_plat_afu (ofs_plat_if plat_ifc);

  // AXI-MM host channel
  ofs_plat_axi_mem_if
    #(
      // Memory interface parameters
      `HOST_CHAN_AXI_MEM_PARAMS,
      // Log traffic (simulation)
       .LOG_CLASS(ofs_plat_log_pkg::HOST_CHAN)
      )
      host_mem();
  
  // AXI-lite for MMIO
  ofs_plat_axi_mem_lite_if
    #(
      // Data bus width
      `HOST_CHAN_AXI_MMIO_PARAMS(64),
      // Log traffic (simulation)
      .LOG_CLASS(ofs_plat_log_pkg::HOST_CHAN)
      )
      mmio64_to_afu();
  
  // Map primary host interface to AXI-MM. This includes the OPAE-managed MMIO 
  // connection.
  ofs_plat_host_chan_as_axi_mem_with_mmio primary_axi 
    (
      .to_fiu(plat_ifc.host_chan.ports[0]),
      .host_mem_to_afu(host_mem),
      .mmio_to_afu(mmio64_to_afu),
      .afu_clk(),
      .afu_reset_n()
      );
    
  // Tie off unused ports
  ofs_plat_if_tie_off_unused
    #(
      // Channel group 0 port 0 is connected
      .HOST_CHAN_IN_USE_MASK(1)
      )
      tie_off(plat_ifc);

  logic clk;
  assign clk = host_mem.clk;
  // Flip reset
  logic reset;
  assign reset = !host_mem.reset_n;

  wire [63:0] m_axi_araddr;
  wire [63:0] m_axi_awaddr;

  // Add zeros to address to match default Fletcher top-level bus addr width
  wire [31:0] s_axi_awaddr;
  assign s_axi_awaddr = {14'b0, mmio64_to_afu.aw.addr};
  wire [31:0] s_axi_araddr;
  assign s_axi_araddr = {14'b0, mmio64_to_afu.ar.addr};

  AxiTop
  #(
    .AFU_ACCEL_UUID(`AFU_ACCEL_UUID)
  ) 
  axi_top
  (
    .kcd_clk(clk),
    .kcd_reset(reset),
    .bcd_clk(clk),
    .bcd_reset(reset),
    .m_axi_araddr(m_axi_araddr),
    .m_axi_arlen(host_mem.ar.len),
    .m_axi_arvalid(host_mem.arvalid),
    .m_axi_arready(host_mem.arready),
    .m_axi_arsize(host_mem.ar.size),
    .m_axi_rdata(host_mem.r.data),
    .m_axi_rresp(host_mem.r.resp),
    .m_axi_rlast(host_mem.r.last),
    .m_axi_rvalid(host_mem.rvalid),
    .m_axi_rready(host_mem.rready),
    .m_axi_awvalid(host_mem.awvalid),
    .m_axi_awready(host_mem.awready),
    .m_axi_awaddr(m_axi_awaddr),
    .m_axi_awlen(host_mem.aw.len),
    .m_axi_awsize(host_mem.aw.size),
    .m_axi_wvalid(host_mem.wvalid),
    .m_axi_wready(host_mem.wready),
    .m_axi_wdata(host_mem.w.data),
    .m_axi_wlast(host_mem.w.last),
    .m_axi_wstrb(host_mem.w.strb),
    .s_axi_awvalid(mmio64_to_afu.awvalid),
    .s_axi_awready(mmio64_to_afu.awready),
    .s_axi_awaddr(s_axi_awaddr),
    .s_axi_wvalid(mmio64_to_afu.wvalid),
    .s_axi_wready(mmio64_to_afu.wready),
    .s_axi_wdata(mmio64_to_afu.w.data),
    .s_axi_wstrb(mmio64_to_afu.w.strb),
    .s_axi_bvalid(mmio64_to_afu.bvalid),
    .s_axi_bready(mmio64_to_afu.bready),
    .s_axi_bresp(mmio64_to_afu.b.resp),
    .s_axi_arvalid(mmio64_to_afu.arvalid),
    .s_axi_arready(mmio64_to_afu.arready),
    .s_axi_araddr(s_axi_araddr),
    .s_axi_rvalid(mmio64_to_afu.rvalid),
    .s_axi_rready(mmio64_to_afu.rready),
    .s_axi_rdata(mmio64_to_afu.r.data),
    .s_axi_rresp(mmio64_to_afu.r.resp)
  );

  // TODO(mb): add FIFO and sync for MMIO ID tags
  always_ff @(posedge clk)
    begin
        if (mmio64_to_afu.arvalid)
        begin
            mmio64_to_afu.r.id <= mmio64_to_afu.ar.id;
        end
    end

  // TODO(mb): add ports for these interfaces in Fletcher
  assign host_mem.ar.addr = m_axi_araddr[47:0];
  assign host_mem.aw.addr = m_axi_awaddr[47:0];

  assign mmio64_to_afu.b.id = mmio64_to_afu.aw.id;
  assign mmio64_to_afu.b.user = ~0;
  assign mmio64_to_afu.r.user = ~0; 

  assign host_mem.ar.id = ~0;
  assign host_mem.ar.prot = ~0;
  assign host_mem.ar.user = ~0;

  assign host_mem.aw.id = ~0;
  assign host_mem.aw.prot = ~0;
  assign host_mem.aw.user = ~0;

  assign host_mem.w.user = ~0;

  assign host_mem.bready = 1'b1;

endmodule
