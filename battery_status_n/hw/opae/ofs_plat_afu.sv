// Copyright 2021 Teratide B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  ofs_plat_host_chan_as_axi_mem_with_mmio 
    #(
      .ADD_CLOCK_CROSSING(1)
    )
  primary_axi
    (
      .to_fiu(plat_ifc.host_chan.ports[0]),
      .host_mem_to_afu(host_mem),
      .mmio_to_afu(mmio64_to_afu),
      .afu_clk(plat_ifc.clocks.uClk_usr.clk),
      .afu_reset_n(plat_ifc.clocks.uClk_usr.reset_n)
      );
    
  // Tie off unused ports
  ofs_plat_if_tie_off_unused
    #(
      // Channel group 0 port 0 is connected
      .HOST_CHAN_IN_USE_MASK(1)
      )
      tie_off(plat_ifc);

  OpaeAxiTop opae_axi_top
  (
    .clk(mmio64_to_afu.clk),
    .areset_n(mmio64_to_afu.reset_n),

    .m_axi_awvalid(host_mem.awvalid),
    .m_axi_awready(host_mem.awready),
    .m_axi_awid(host_mem.aw.id),
    .m_axi_awaddr(host_mem.aw.addr),
    .m_axi_awlen(host_mem.aw.len),
    .m_axi_awsize(host_mem.aw.size),
    .m_axi_awburst(host_mem.aw.burst),
    .m_axi_awlock(host_mem.aw.lock),
    .m_axi_awcache(host_mem.aw.cache),
    .m_axi_awprot(host_mem.aw.prot),
    .m_axi_awuser(host_mem.aw.user),
    .m_axi_awqos(host_mem.aw.qos),
    .m_axi_awregion(host_mem.aw.region),

    .m_axi_wvalid(host_mem.wvalid),
    .m_axi_wready(host_mem.wready),
    .m_axi_wdata(host_mem.w.data),
    .m_axi_wstrb(host_mem.w.strb),
    .m_axi_wlast(host_mem.w.last),
    .m_axi_wuser(host_mem.w.user),

    .m_axi_bready(host_mem.bready),
    .m_axi_bvalid(host_mem.bvalid),
    .m_axi_bid(host_mem.b.id),
    .m_axi_bresp(host_mem.b.resp),
    .m_axi_buser(host_mem.b.user),

    .m_axi_arvalid(host_mem.arvalid),
    .m_axi_arready(host_mem.arready),
    .m_axi_arid(host_mem.ar.id),
    .m_axi_araddr(host_mem.ar.addr),
    .m_axi_arlen(host_mem.ar.len),
    .m_axi_arsize(host_mem.ar.size),
    .m_axi_arburst(host_mem.ar.burst),
    .m_axi_arlock(host_mem.ar.lock),
    .m_axi_arcache(host_mem.ar.cache),
    .m_axi_arprot(host_mem.ar.prot),
    .m_axi_aruser(host_mem.ar.user),
    .m_axi_arqos(host_mem.ar.qos),
    .m_axi_arregion(host_mem.ar.region),

    .m_axi_rvalid(host_mem.rvalid),
    .m_axi_rready(host_mem.rready),
    .m_axi_rid(host_mem.r.id),
    .m_axi_rdata(host_mem.r.data),
    .m_axi_rresp(host_mem.r.resp),
    .m_axi_ruser(host_mem.r.user),
    .m_axi_rlast(host_mem.r.last),

    .s_axi_awvalid(mmio64_to_afu.awvalid),
    .s_axi_awready(mmio64_to_afu.awready),
    .s_axi_awid(mmio64_to_afu.aw.id),
    .s_axi_awaddr(mmio64_to_afu.aw.addr),
    .s_axi_awsize(mmio64_to_afu.aw.size),
    .s_axi_awprot(mmio64_to_afu.aw.prot),
    .s_axi_awuser(mmio64_to_afu.aw.user),

    .s_axi_wvalid(mmio64_to_afu.wvalid),
    .s_axi_wready(mmio64_to_afu.wready),
    .s_axi_wdata(mmio64_to_afu.w.data),
    .s_axi_wstrb(mmio64_to_afu.w.strb),
    .s_axi_wuser(mmio64_to_afu.w.user),

    .s_axi_bvalid(mmio64_to_afu.bvalid),
    .s_axi_bready(mmio64_to_afu.bready),
    .s_axi_bid(mmio64_to_afu.b.id),
    .s_axi_bresp(mmio64_to_afu.b.resp),
    .s_axi_buser(mmio64_to_afu.b.user),

    .s_axi_arvalid(mmio64_to_afu.arvalid),
    .s_axi_arready(mmio64_to_afu.arready),
    .s_axi_arid(mmio64_to_afu.ar.id),
    .s_axi_araddr(mmio64_to_afu.ar.addr),
    .s_axi_arsize(mmio64_to_afu.ar.size),
    .s_axi_arprot(mmio64_to_afu.ar.prot),
    .s_axi_aruser(mmio64_to_afu.ar.user),

    .s_axi_rvalid(mmio64_to_afu.rvalid),
    .s_axi_rready(mmio64_to_afu.rready),
    .s_axi_rid(mmio64_to_afu.r.id),
    .s_axi_rdata(mmio64_to_afu.r.data),
    .s_axi_rresp(mmio64_to_afu.r.resp),
    .s_axi_ruser(mmio64_to_afu.r.user)
  );

endmodule
