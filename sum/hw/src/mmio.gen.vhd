-- Generated using vhdMMIO 0.0.3 (https://github.com/abs-tudelft/vhdmmio)

library ieee;
use ieee.std_logic_1164.all;
use ieee.std_logic_misc.all;
use ieee.numeric_std.all;

library work;
use work.vhdmmio_pkg.all;
use work.mmio_pkg.all;

entity mmio is
  generic (

    -- Interface for field AFU_ID_L: AFU_ID_L.
    F_AFU_ID_L_RESET_DATA : std_logic_vector(63 downto 0) := (others => '0');

    -- Interface for field AFU_ID_H: AFU_ID_H.
    F_AFU_ID_H_RESET_DATA : std_logic_vector(63 downto 0) := (others => '0')

  );
  port (

    -- Clock sensitive to the rising edge and synchronous, active-high reset.
    kcd_clk : in std_logic;
    kcd_reset : in std_logic := '0';

    -- Interface for field start: start.
    f_start_data : out std_logic := '0';

    -- Interface for field stop: stop.
    f_stop_data : out std_logic := '0';

    -- Interface for field reset: reset.
    f_reset_data : out std_logic := '0';

    -- Interface for field idle: idle.
    f_idle_write_data : in std_logic := '0';

    -- Interface for field busy: busy.
    f_busy_write_data : in std_logic := '0';

    -- Interface for field done: done.
    f_done_write_data : in std_logic := '0';

    -- Interface for field result: result.
    f_result_write_data : in std_logic_vector(63 downto 0) := (others => '0');

    -- Interface for field ExampleBatch_firstidx: ExampleBatch_firstidx.
    f_ExampleBatch_firstidx_data : out std_logic_vector(31 downto 0)
        := (others => '0');

    -- Interface for field ExampleBatch_lastidx: ExampleBatch_lastidx.
    f_ExampleBatch_lastidx_data : out std_logic_vector(31 downto 0)
        := (others => '0');

    -- Interface for field ExampleBatch_number_values:
    -- ExampleBatch_number_values.
    f_ExampleBatch_number_values_data : out std_logic_vector(63 downto 0)
        := (others => '0');

    -- Interface for field Profile_enable: Profile_enable.
    f_Profile_enable_data : out std_logic := '0';

    -- Interface for field Profile_clear: Profile_clear.
    f_Profile_clear_data : out std_logic := '0';

    -- AXI4-lite + interrupt request bus to the master.
    mmio_awvalid : in  std_logic := '0';
    mmio_awready : out std_logic := '1';
    mmio_awaddr  : in  std_logic_vector(31 downto 0) := X"00000000";
    mmio_awprot  : in  std_logic_vector(2 downto 0) := "000";
    mmio_wvalid  : in  std_logic := '0';
    mmio_wready  : out std_logic := '1';
    mmio_wdata   : in  std_logic_vector(63 downto 0) := (others => '0');
    mmio_wstrb   : in  std_logic_vector(7 downto 0) := (others => '0');
    mmio_bvalid  : out std_logic := '0';
    mmio_bready  : in  std_logic := '1';
    mmio_bresp   : out std_logic_vector(1 downto 0) := "00";
    mmio_arvalid : in  std_logic := '0';
    mmio_arready : out std_logic := '1';
    mmio_araddr  : in  std_logic_vector(31 downto 0) := X"00000000";
    mmio_arprot  : in  std_logic_vector(2 downto 0) := "000";
    mmio_rvalid  : out std_logic := '0';
    mmio_rready  : in  std_logic := '1';
    mmio_rdata   : out std_logic_vector(63 downto 0) := (others => '0');
    mmio_rresp   : out std_logic_vector(1 downto 0) := "00";
    mmio_uirq    : out std_logic := '0'

  );
end mmio;

architecture behavioral of mmio is
begin
  reg_proc: process (kcd_clk) is

    -- Convenience function for unsigned accumulation with differing vector
    -- widths.
    procedure accum_add(
      accum: inout std_logic_vector;
      addend: std_logic_vector) is
    begin
      accum := std_logic_vector(
        unsigned(accum) + resize(unsigned(addend), accum'length));
    end procedure accum_add;

    -- Convenience function for unsigned subtraction with differing vector
    -- widths.
    procedure accum_sub(
      accum: inout std_logic_vector;
      addend: std_logic_vector) is
    begin
      accum := std_logic_vector(
        unsigned(accum) - resize(unsigned(addend), accum'length));
    end procedure accum_sub;

    -- Internal alias for the reset input.
    variable reset : std_logic;

    -- Bus response output register.
    variable bus_v : axi4l64_s2m_type := AXI4L64_S2M_RESET; -- reg

    -- Holding registers for the AXI4-lite request channels. Having these
    -- allows us to make the accompanying ready signals register outputs
    -- without sacrificing a cycle's worth of delay for every transaction.
    variable awl : axi4la_type := AXI4LA_RESET; -- reg
    variable wl  : axi4lw64_type := AXI4LW64_RESET; -- reg
    variable arl : axi4la_type := AXI4LA_RESET; -- reg

    -- Request flags for the register logic. When asserted, a request is
    -- present in awl/wl/arl, and the response can be returned immediately.
    -- This is used by simple registers.
    variable w_req : boolean := false;
    variable r_req : boolean := false;

    -- As above, but asserted when there is a request that can NOT be returned
    -- immediately for whatever reason, but CAN be started already if deferral
    -- is supported by the targeted block. Abbreviation for lookahead request.
    -- Note that *_lreq implies *_req.
    variable w_lreq : boolean := false;
    variable r_lreq : boolean := false;

    -- Request signals. w_strb is a validity bit for each data bit; it actually
    -- always has byte granularity but encoding it this way makes the code a
    -- lot nicer (and it should be optimized to the same thing by any sane
    -- synthesizer).
    variable w_addr : std_logic_vector(31 downto 0);
    variable w_data : std_logic_vector(63 downto 0) := (others => '0');
    variable w_strb : std_logic_vector(63 downto 0) := (others => '0');
    constant w_prot : std_logic_vector(2 downto 0) := (others => '0');
    variable r_addr : std_logic_vector(31 downto 0);
    constant r_prot : std_logic_vector(2 downto 0) := (others => '0');

    -- Logical write data holding registers. For multi-word registers, write
    -- data is held in w_hold and w_hstb until the last subregister is written,
    -- at which point their entire contents are written at once.
    variable w_hold : std_logic_vector(63 downto 0) := (others => '0'); -- reg
    variable w_hstb : std_logic_vector(63 downto 0) := (others => '0'); -- reg

    -- Between the first and last access to a multiword register, the multi
    -- bit will be set. If it is set while a request with a different *_prot is
    -- received, the interrupting request is rejected if it is A) non-secure
    -- while the interrupted request is secure or B) unprivileged while the
    -- interrupted request is privileged. If it is not rejected, previously
    -- buffered data is cleared and masked. Within the same security level, it
    -- is up to the bus master to not mess up its own access pattern. The last
    -- access to a multiword register clears the bit; for the read end r_hold
    -- is also cleared in this case to prevent data leaks.
    variable w_multi : std_logic := '0'; -- reg
    variable r_multi : std_logic := '0'; -- reg

    -- Response flags. When *_req is set and *_addr matches a register, it must
    -- set at least one of these flags; when *_rreq is set and *_rtag matches a
    -- register, it must also set at least one of these, except it cannot set
    -- *_defer. A decode error can be generated by intentionally NOT setting
    -- any of these flags, but this should only be done by registers that
    -- contain only one field (usually, these would be AXI-lite passthrough
    -- "registers"). The action taken by the non-register-specific logic is as
    -- follows (priority decoder):
    --
    --  - if *_defer is set, push *_dtag into the deferal FIFO;
    --  - if *_block is set, do nothing;
    --  - otherwise, if *_nack is set, send a slave error response;
    --  - otherwise, if *_ack is set, send a positive response;
    --  - otherwise, send a decode error response.
    --
    -- In addition to the above, the request stream(s) will be handshaked if
    -- *_req was set and a response is sent or the response is deferred.
    -- Likewise, the deferal FIFO will be popped if *_rreq was set and a
    -- response is sent.
    --
    -- The valid states can be summarized as follows:
    --
    -- .----------------------------------------------------------------------------------.
    -- | req | lreq | rreq || ack | nack | block | defer || request | response | defer    |
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  0  |  0   |  0   ||  0  |  0   |   0   |   0   ||         |          |          | Idle.
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  0  |  0   |  1   ||  0  |  0   |   0   |   0   ||         | dec_err  | pop      | Completing
    -- |  0  |  0   |  1   ||  1  |  0   |   0   |   0   ||         | ack      | pop      | previous,
    -- |  0  |  0   |  1   ||  -  |  1   |   0   |   0   ||         | slv_err  | pop      | no
    -- |  0  |  0   |  1   ||  -  |  -   |   1   |   0   ||         |          |          | lookahead.
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  1  |  0   |  0   ||  0  |  0   |   0   |   0   || accept  | dec_err  |          | Responding
    -- |  1  |  0   |  0   ||  1  |  0   |   0   |   0   || accept  | ack      |          | immediately
    -- |  1  |  0   |  0   ||  -  |  1   |   0   |   0   || accept  | slv_err  |          | to incoming
    -- |  1  |  0   |  0   ||  -  |  -   |   1   |   0   ||         |          |          | request.
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  1  |  0   |  0   ||  0  |  0   |   0   |   1   || accept  |          | push     | Deferring.
    -- |  0  |  1   |  0   ||  0  |  0   |   0   |   1   || accept  |          | push     | Deferring.
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  0  |  1   |  1   ||  0  |  0   |   0   |   0   ||         | dec_err  | pop      | Completing
    -- |  0  |  1   |  1   ||  1  |  0   |   0   |   0   ||         | ack      | pop      | previous,
    -- |  0  |  1   |  1   ||  -  |  1   |   0   |   0   ||         | slv_err  | pop      | ignoring
    -- |  0  |  1   |  1   ||  -  |  -   |   1   |   0   ||         |          |          | lookahead.
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  0  |  1   |  1   ||  0  |  0   |   0   |   1   || accept  | dec_err  | pop+push | Completing
    -- |  0  |  1   |  1   ||  1  |  0   |   0   |   1   || accept  | ack      | pop+push | previous,
    -- |  0  |  1   |  1   ||  -  |  1   |   0   |   1   || accept  | slv_err  | pop+push | deferring
    -- |  0  |  1   |  1   ||  -  |  -   |   1   |   1   || accept  |          | push     | lookahead.
    -- '----------------------------------------------------------------------------------'
    --
    -- This can be simplified to the following:
    --
    -- .----------------------------------------------------------------------------------.
    -- | req | lreq | rreq || ack | nack | block | defer || request | response | defer    |
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  -  |  -   |  -   ||  -  |  -   |   1   |   -   ||         |          |          |
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  -  |  -   |  1   ||  -  |  1   |   0   |   -   ||         | slv_err  | pop      |
    -- |  1  |  -   |  0   ||  -  |  1   |   0   |   -   || accept  | slv_err  |          |
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  -  |  -   |  1   ||  1  |  0   |   0   |   -   ||         | ack      | pop      |
    -- |  1  |  -   |  0   ||  1  |  0   |   0   |   -   || accept  | ack      |          |
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  -  |  -   |  1   ||  0  |  0   |   0   |   -   ||         | dec_err  | pop      |
    -- |  1  |  -   |  0   ||  0  |  0   |   0   |   -   || accept  | dec_err  |          |
    -- |-----+------+------||-----+------+-------+-------||---------+----------+----------|
    -- |  -  |  -   |  -   ||  -  |  -   |   -   |   1   || accept  |          | push     |
    -- '----------------------------------------------------------------------------------'
    --
    variable w_block : boolean := false;
    variable r_block : boolean := false;
    variable w_nack  : boolean := false;
    variable r_nack  : boolean := false;
    variable w_ack   : boolean := false;
    variable r_ack   : boolean := false;

    -- Logical read data holding register. This is set when r_ack is set during
    -- an access to the first physical register of a logical register for all
    -- fields in the logical register.
    variable r_hold  : std_logic_vector(63 downto 0) := (others => '0'); -- reg

    -- Physical read data. This is taken from r_hold based on which physical
    -- subregister is being read.
    variable r_data  : std_logic_vector(63 downto 0);

    -- Subaddress variables, used to index within large fields like memories and
    -- AXI passthroughs.
    variable subaddr_none         : std_logic_vector(0 downto 0);

    -- Private declarations for field AFU_DHF: AFU_DHF.
    type f_AFU_DHF_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_AFU_DHF_R_RESET : f_AFU_DHF_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_AFU_DHF_r_array is array (natural range <>) of f_AFU_DHF_r_type;
    variable f_AFU_DHF_r : f_AFU_DHF_r_array(0 to 0)
        := (others => F_AFU_DHF_R_RESET);

    -- Private declarations for field AFU_ID_L: AFU_ID_L.
    type f_AFU_ID_L_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_AFU_ID_L_R_RESET : f_AFU_ID_L_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_AFU_ID_L_r_array is array (natural range <>) of f_AFU_ID_L_r_type;
    variable f_AFU_ID_L_r : f_AFU_ID_L_r_array(0 to 0)
        := (others => F_AFU_ID_L_R_RESET);

    -- Private declarations for field AFU_ID_H: AFU_ID_H.
    type f_AFU_ID_H_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_AFU_ID_H_R_RESET : f_AFU_ID_H_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_AFU_ID_H_r_array is array (natural range <>) of f_AFU_ID_H_r_type;
    variable f_AFU_ID_H_r : f_AFU_ID_H_r_array(0 to 0)
        := (others => F_AFU_ID_H_R_RESET);

    -- Private declarations for field DFH_RSVD0: DFH_RSVD0.
    type f_DFH_RSVD0_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_DFH_RSVD0_R_RESET : f_DFH_RSVD0_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_DFH_RSVD0_r_array is array (natural range <>) of f_DFH_RSVD0_r_type;
    variable f_DFH_RSVD0_r : f_DFH_RSVD0_r_array(0 to 0)
        := (others => F_DFH_RSVD0_R_RESET);

    -- Private declarations for field DFH_RSVD1: DFH_RSVD1.
    type f_DFH_RSVD1_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_DFH_RSVD1_R_RESET : f_DFH_RSVD1_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_DFH_RSVD1_r_array is array (natural range <>) of f_DFH_RSVD1_r_type;
    variable f_DFH_RSVD1_r : f_DFH_RSVD1_r_array(0 to 0)
        := (others => F_DFH_RSVD1_R_RESET);

    -- Private declarations for field start: start.
    type f_start_r_type is record
      d : std_logic;
      v : std_logic;
      inval : std_logic;
    end record;
    constant F_START_R_RESET : f_start_r_type := (
      d => '0',
      v => '0',
      inval => '0'
    );
    type f_start_r_array is array (natural range <>) of f_start_r_type;
    variable f_start_r : f_start_r_array(0 to 0) := (others => F_START_R_RESET);

    -- Private declarations for field stop: stop.
    type f_stop_r_type is record
      d : std_logic;
      v : std_logic;
      inval : std_logic;
    end record;
    constant F_STOP_R_RESET : f_stop_r_type := (
      d => '0',
      v => '0',
      inval => '0'
    );
    type f_stop_r_array is array (natural range <>) of f_stop_r_type;
    variable f_stop_r : f_stop_r_array(0 to 0) := (others => F_STOP_R_RESET);

    -- Private declarations for field reset: reset.
    type f_reset_r_type is record
      d : std_logic;
      v : std_logic;
      inval : std_logic;
    end record;
    constant F_RESET_R_RESET : f_reset_r_type := (
      d => '0',
      v => '0',
      inval => '0'
    );
    type f_reset_r_array is array (natural range <>) of f_reset_r_type;
    variable f_reset_r : f_reset_r_array(0 to 0) := (others => F_RESET_R_RESET);

    -- Private declarations for field idle: idle.
    type f_idle_r_type is record
      d : std_logic;
      v : std_logic;
    end record;
    constant F_IDLE_R_RESET : f_idle_r_type := (
      d => '0',
      v => '0'
    );
    type f_idle_r_array is array (natural range <>) of f_idle_r_type;
    variable f_idle_r : f_idle_r_array(0 to 0) := (others => F_IDLE_R_RESET);

    -- Private declarations for field busy: busy.
    type f_busy_r_type is record
      d : std_logic;
      v : std_logic;
    end record;
    constant F_BUSY_R_RESET : f_busy_r_type := (
      d => '0',
      v => '0'
    );
    type f_busy_r_array is array (natural range <>) of f_busy_r_type;
    variable f_busy_r : f_busy_r_array(0 to 0) := (others => F_BUSY_R_RESET);

    -- Private declarations for field done: done.
    type f_done_r_type is record
      d : std_logic;
      v : std_logic;
    end record;
    constant F_DONE_R_RESET : f_done_r_type := (
      d => '0',
      v => '0'
    );
    type f_done_r_array is array (natural range <>) of f_done_r_type;
    variable f_done_r : f_done_r_array(0 to 0) := (others => F_DONE_R_RESET);

    -- Private declarations for field result: result.
    type f_result_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_RESULT_R_RESET : f_result_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_result_r_array is array (natural range <>) of f_result_r_type;
    variable f_result_r : f_result_r_array(0 to 0)
        := (others => F_RESULT_R_RESET);

    -- Private declarations for field ExampleBatch_firstidx:
    -- ExampleBatch_firstidx.
    type f_ExampleBatch_firstidx_r_type is record
      d : std_logic_vector(31 downto 0);
      v : std_logic;
    end record;
    constant F_EXAMPLEBATCH_FIRSTIDX_R_RESET : f_ExampleBatch_firstidx_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_ExampleBatch_firstidx_r_array is array (natural range <>) of f_ExampleBatch_firstidx_r_type;
    variable f_ExampleBatch_firstidx_r : f_ExampleBatch_firstidx_r_array(0 to 0)
        := (others => F_EXAMPLEBATCH_FIRSTIDX_R_RESET);

    -- Private declarations for field ExampleBatch_lastidx:
    -- ExampleBatch_lastidx.
    type f_ExampleBatch_lastidx_r_type is record
      d : std_logic_vector(31 downto 0);
      v : std_logic;
    end record;
    constant F_EXAMPLEBATCH_LASTIDX_R_RESET : f_ExampleBatch_lastidx_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_ExampleBatch_lastidx_r_array is array (natural range <>) of f_ExampleBatch_lastidx_r_type;
    variable f_ExampleBatch_lastidx_r : f_ExampleBatch_lastidx_r_array(0 to 0)
        := (others => F_EXAMPLEBATCH_LASTIDX_R_RESET);

    -- Private declarations for field ExampleBatch_number_values:
    -- ExampleBatch_number_values.
    type f_ExampleBatch_number_values_r_type is record
      d : std_logic_vector(63 downto 0);
      v : std_logic;
    end record;
    constant F_EXAMPLEBATCH_NUMBER_VALUES_R_RESET : f_ExampleBatch_number_values_r_type := (
      d => (others => '0'),
      v => '0'
    );
    type f_ExampleBatch_number_values_r_array is array (natural range <>) of f_ExampleBatch_number_values_r_type;
    variable f_ExampleBatch_number_values_r : f_ExampleBatch_number_values_r_array(0 to 0)
        := (others => F_EXAMPLEBATCH_NUMBER_VALUES_R_RESET);

    -- Private declarations for field Profile_enable: Profile_enable.
    type f_Profile_enable_r_type is record
      d : std_logic;
      v : std_logic;
    end record;
    constant F_PROFILE_ENABLE_R_RESET : f_Profile_enable_r_type := (
      d => '0',
      v => '0'
    );
    type f_Profile_enable_r_array is array (natural range <>) of f_Profile_enable_r_type;
    variable f_Profile_enable_r : f_Profile_enable_r_array(0 to 0)
        := (others => F_PROFILE_ENABLE_R_RESET);

    -- Private declarations for field Profile_clear: Profile_clear.
    type f_Profile_clear_r_type is record
      d : std_logic;
      v : std_logic;
      inval : std_logic;
    end record;
    constant F_PROFILE_CLEAR_R_RESET : f_Profile_clear_r_type := (
      d => '0',
      v => '0',
      inval => '0'
    );
    type f_Profile_clear_r_array is array (natural range <>) of f_Profile_clear_r_type;
    variable f_Profile_clear_r : f_Profile_clear_r_array(0 to 0)
        := (others => F_PROFILE_CLEAR_R_RESET);

    -- Temporary variables for the field templates.
    variable tmp_data    : std_logic;
    variable tmp_strb    : std_logic;
    variable tmp_data32  : std_logic_vector(31 downto 0);
    variable tmp_strb32  : std_logic_vector(31 downto 0);
    variable tmp_data64  : std_logic_vector(63 downto 0);
    variable tmp_strb64  : std_logic_vector(63 downto 0);

  begin
    if rising_edge(kcd_clk) then

      -- Reset variables that shouldn't become registers to default values.
      reset   := kcd_reset;
      w_req   := false;
      r_req   := false;
      w_lreq  := false;
      r_lreq  := false;
      w_addr  := (others => '0');
      w_data  := (others => '0');
      w_strb  := (others => '0');
      r_addr  := (others => '0');
      w_block := false;
      r_block := false;
      w_nack  := false;
      r_nack  := false;
      w_ack   := false;
      r_ack   := false;
      r_data  := (others => '0');

      -------------------------------------------------------------------------
      -- Finish up the previous cycle
      -------------------------------------------------------------------------
      -- Invalidate responses that were acknowledged by the master in the
      -- previous cycle.
      if mmio_bready = '1' then
        bus_v.b.valid := '0';
      end if;
      if mmio_rready = '1' then
        bus_v.r.valid := '0';
      end if;

      -- If we indicated to the master that we were ready for a transaction on
      -- any of the incoming channels, we must latch any incoming requests. If
      -- we're ready but there is no incoming request this becomes don't-care.
      if bus_v.aw.ready = '1' then
        awl.valid := mmio_awvalid;
        awl.addr  := mmio_awaddr;
        awl.prot  := mmio_awprot;
      end if;
      if bus_v.w.ready = '1' then
        wl.valid := mmio_wvalid;
        wl.data  := mmio_wdata;
        wl.strb  := mmio_wstrb;
      end if;
      if bus_v.ar.ready = '1' then
        arl.valid := mmio_arvalid;
        arl.addr  := mmio_araddr;
        arl.prot  := mmio_arprot;
      end if;

      -------------------------------------------------------------------------
      -- Handle interrupts
      -------------------------------------------------------------------------
      -- No incoming interrupts; request signal is always released.
      bus_v.u.irq := '0';

      -------------------------------------------------------------------------
      -- Handle MMIO fields
      -------------------------------------------------------------------------
      -- We're ready for a write/read when all the respective channels (or
      -- their holding registers) are ready/waiting for us.
      if awl.valid = '1' and wl.valid = '1' then
        if bus_v.b.valid = '0' then
          w_req := true; -- Request valid and response register empty.
        else
          w_lreq := true; -- Request valid, but response register is busy.
        end if;
      end if;
      if arl.valid = '1' then
        if bus_v.r.valid = '0' then
          r_req := true; -- Request valid and response register empty.
        else
          r_lreq := true; -- Request valid, but response register is busy.
        end if;
      end if;

      -- Capture request inputs into more consistently named variables.
      w_addr := awl.addr;
      for b in w_strb'range loop
        w_strb(b) := wl.strb(b / 8);
      end loop;
      w_data := wl.data and w_strb;
      r_addr := arl.addr;

      -------------------------------------------------------------------------
      -- Generated field logic
      -------------------------------------------------------------------------

      -- Pre-bus logic for field start: start.

      -- Handle post-write invalidation for field start one cycle after the
      -- write occurs.
      if f_start_r((0)).inval = '1' then
        f_start_r((0)).d := '0';
        f_start_r((0)).v := '0';
      end if;
      f_start_r((0)).inval := '0';

      -- Pre-bus logic for field stop: stop.

      -- Handle post-write invalidation for field stop one cycle after the write
      -- occurs.
      if f_stop_r((0)).inval = '1' then
        f_stop_r((0)).d := '0';
        f_stop_r((0)).v := '0';
      end if;
      f_stop_r((0)).inval := '0';

      -- Pre-bus logic for field reset: reset.

      -- Handle post-write invalidation for field reset one cycle after the
      -- write occurs.
      if f_reset_r((0)).inval = '1' then
        f_reset_r((0)).d := '0';
        f_reset_r((0)).v := '0';
      end if;
      f_reset_r((0)).inval := '0';

      -- Pre-bus logic for field idle: idle.

      -- Handle hardware write for field idle: status.
      f_idle_r((0)).d := f_idle_write_data;
      f_idle_r((0)).v := '1';

      -- Pre-bus logic for field busy: busy.

      -- Handle hardware write for field busy: status.
      f_busy_r((0)).d := f_busy_write_data;
      f_busy_r((0)).v := '1';

      -- Pre-bus logic for field done: done.

      -- Handle hardware write for field done: status.
      f_done_r((0)).d := f_done_write_data;
      f_done_r((0)).v := '1';

      -- Pre-bus logic for field result: result.

      -- Handle hardware write for field result: status.
      f_result_r((0)).d := f_result_write_data;
      f_result_r((0)).v := '1';

      -- Pre-bus logic for field Profile_clear: Profile_clear.

      -- Handle post-write invalidation for field Profile_clear one cycle after
      -- the write occurs.
      if f_Profile_clear_r((0)).inval = '1' then
        f_Profile_clear_r((0)).d := '0';
        f_Profile_clear_r((0)).v := '0';
      end if;
      f_Profile_clear_r((0)).inval := '0';

      -------------------------------------------------------------------------
      -- Bus read logic
      -------------------------------------------------------------------------

      -- Construct the subaddresses for read mode.
      subaddr_none(0) := '0';

      -- Read address decoder.
      case r_addr(6 downto 3) is
        when "0000" =>
          -- r_addr = 00000000000000000000000000000---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field AFU_DHF: AFU_DHF.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_AFU_DHF_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block AFU_DHF_reg: block containing bits 63..0 of
          -- register `AFU_DHF_reg` (`AFU_DHF`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "0001" =>
          -- r_addr = 00000000000000000000000000001---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field AFU_ID_L: AFU_ID_L.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_AFU_ID_L_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block AFU_ID_L_reg: block containing bits 63..0 of
          -- register `AFU_ID_L_reg` (`AFU_ID_L`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "0010" =>
          -- r_addr = 00000000000000000000000000010---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field AFU_ID_H: AFU_ID_H.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_AFU_ID_H_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block AFU_ID_H_reg: block containing bits 63..0 of
          -- register `AFU_ID_H_reg` (`AFU_ID_H`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "0011" =>
          -- r_addr = 00000000000000000000000000011---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field DFH_RSVD0: DFH_RSVD0.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_DFH_RSVD0_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block DFH_RSVD0_reg: block containing bits 63..0 of
          -- register `DFH_RSVD0_reg` (`DFH_RSVD0`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "0100" =>
          -- r_addr = 00000000000000000000000000100---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field DFH_RSVD1: DFH_RSVD1.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_DFH_RSVD1_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block DFH_RSVD1_reg: block containing bits 63..0 of
          -- register `DFH_RSVD1_reg` (`DFH_RSVD1`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "1000" =>
          -- r_addr = 00000000000000000000000001000---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field idle: idle.

          if r_req then
            tmp_data := r_hold(32);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data := f_idle_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(32) := tmp_data;
          end if;

          -- Read logic for field busy: busy.

          if r_req then
            tmp_data := r_hold(33);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data := f_busy_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(33) := tmp_data;
          end if;

          -- Read logic for field done: done.

          if r_req then
            tmp_data := r_hold(34);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data := f_done_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(34) := tmp_data;
          end if;

          -- Read logic for block idle_reg: block containing bits 63..0 of
          -- register `idle_reg` (`IDLE`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "1001" =>
          -- r_addr = 00000000000000000000000001001---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field result: result.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_result_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block result_reg: block containing bits 63..0 of
          -- register `result_reg` (`RESULT`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "1010" =>
          -- r_addr = 00000000000000000000000001010---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field ExampleBatch_firstidx: ExampleBatch_firstidx.

          if r_req then
            tmp_data32 := r_hold(31 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data32 := f_ExampleBatch_firstidx_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(31 downto 0) := tmp_data32;
          end if;

          -- Read logic for field ExampleBatch_lastidx: ExampleBatch_lastidx.

          if r_req then
            tmp_data32 := r_hold(63 downto 32);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data32 := f_ExampleBatch_lastidx_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 32) := tmp_data32;
          end if;

          -- Read logic for block ExampleBatch_firstidx_reg: block containing
          -- bits 63..0 of register `ExampleBatch_firstidx_reg`
          -- (`EXAMPLEBATCH_FIRSTIDX`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when "1011" =>
          -- r_addr = 00000000000000000000000001011---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field ExampleBatch_number_values:
          -- ExampleBatch_number_values.

          if r_req then
            tmp_data64 := r_hold(63 downto 0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data64 := f_ExampleBatch_number_values_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(63 downto 0) := tmp_data64;
          end if;

          -- Read logic for block ExampleBatch_number_values_reg: block
          -- containing bits 63..0 of register `ExampleBatch_number_values_reg`
          -- (`EXAMPLEBATCH_NUMBER_VALUES`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

        when others => -- "1100"
          -- r_addr = 00000000000000000000000001100---

          if r_req then

            -- Clear holding register location prior to read.
            r_hold := (others => '0');

          end if;

          -- Read logic for field Profile_enable: Profile_enable.

          if r_req then
            tmp_data := r_hold(0);
          end if;
          if r_req then

            -- Regular access logic. Read mode: enabled.
            tmp_data := f_Profile_enable_r((0)).d;
            r_ack := true;

          end if;
          if r_req then
            r_hold(0) := tmp_data;
          end if;

          -- Read logic for block Profile_enable_reg: block containing bits
          -- 63..0 of register `Profile_enable_reg` (`PROFILE_ENABLE`).
          if r_req then

            r_data := r_hold(63 downto 0);
            r_multi := '0';

          end if;

      end case;

      -------------------------------------------------------------------------
      -- Bus write logic
      -------------------------------------------------------------------------

      -- Construct the subaddresses for write mode.
      subaddr_none(0) := '0';

      -- Write address decoder.
      case w_addr(5 downto 3) is
        when "000" =>
          -- w_addr = 00000000000000000000000001000---

          -- Write logic for block idle_reg: block containing bits 63..0 of
          -- register `idle_reg` (`IDLE`).
          if w_req or w_lreq then
            w_hold(63 downto 0) := w_data;
            w_hstb(63 downto 0) := w_strb;
            w_multi := '0';
          end if;

          -- Write logic for field start: start.

          tmp_data := w_hold(0);
          tmp_strb := w_hstb(0);
          if w_req then

            -- Regular access logic. Write mode: enabled.

            f_start_r((0)).d := tmp_data;
            w_ack := true;

            -- Handle post-write operation: invalidate.
            f_start_r((0)).v := '1';
            f_start_r((0)).inval := '1';

          end if;

          -- Write logic for field stop: stop.

          tmp_data := w_hold(1);
          tmp_strb := w_hstb(1);
          if w_req then

            -- Regular access logic. Write mode: enabled.

            f_stop_r((0)).d := tmp_data;
            w_ack := true;

            -- Handle post-write operation: invalidate.
            f_stop_r((0)).v := '1';
            f_stop_r((0)).inval := '1';

          end if;

          -- Write logic for field reset: reset.

          tmp_data := w_hold(2);
          tmp_strb := w_hstb(2);
          if w_req then

            -- Regular access logic. Write mode: enabled.

            f_reset_r((0)).d := tmp_data;
            w_ack := true;

            -- Handle post-write operation: invalidate.
            f_reset_r((0)).v := '1';
            f_reset_r((0)).inval := '1';

          end if;

        when "010" =>
          -- w_addr = 00000000000000000000000001010---

          -- Write logic for block ExampleBatch_firstidx_reg: block containing
          -- bits 63..0 of register `ExampleBatch_firstidx_reg`
          -- (`EXAMPLEBATCH_FIRSTIDX`).
          if w_req or w_lreq then
            w_hold(63 downto 0) := w_data;
            w_hstb(63 downto 0) := w_strb;
            w_multi := '0';
          end if;

          -- Write logic for field ExampleBatch_firstidx: ExampleBatch_firstidx.

          tmp_data32 := w_hold(31 downto 0);
          tmp_strb32 := w_hstb(31 downto 0);
          if w_req then

            -- Regular access logic. Write mode: masked.

            f_ExampleBatch_firstidx_r((0)).d
                := (f_ExampleBatch_firstidx_r((0)).d and not tmp_strb32)
                or tmp_data32;
            w_ack := true;

          end if;

          -- Write logic for field ExampleBatch_lastidx: ExampleBatch_lastidx.

          tmp_data32 := w_hold(63 downto 32);
          tmp_strb32 := w_hstb(63 downto 32);
          if w_req then

            -- Regular access logic. Write mode: masked.

            f_ExampleBatch_lastidx_r((0)).d
                := (f_ExampleBatch_lastidx_r((0)).d and not tmp_strb32)
                or tmp_data32;
            w_ack := true;

          end if;

        when "011" =>
          -- w_addr = 00000000000000000000000001011---

          -- Write logic for block ExampleBatch_number_values_reg: block
          -- containing bits 63..0 of register `ExampleBatch_number_values_reg`
          -- (`EXAMPLEBATCH_NUMBER_VALUES`).
          if w_req or w_lreq then
            w_hold(63 downto 0) := w_data;
            w_hstb(63 downto 0) := w_strb;
            w_multi := '0';
          end if;

          -- Write logic for field ExampleBatch_number_values:
          -- ExampleBatch_number_values.

          tmp_data64 := w_hold(63 downto 0);
          tmp_strb64 := w_hstb(63 downto 0);
          if w_req then

            -- Regular access logic. Write mode: masked.

            f_ExampleBatch_number_values_r((0)).d
                := (f_ExampleBatch_number_values_r((0)).d and not tmp_strb64)
                or tmp_data64;
            w_ack := true;

          end if;

        when others => -- "100"
          -- w_addr = 00000000000000000000000001100---

          -- Write logic for block Profile_enable_reg: block containing bits
          -- 63..0 of register `Profile_enable_reg` (`PROFILE_ENABLE`).
          if w_req or w_lreq then
            w_hold(63 downto 0) := w_data;
            w_hstb(63 downto 0) := w_strb;
            w_multi := '0';
          end if;

          -- Write logic for field Profile_enable: Profile_enable.

          tmp_data := w_hold(0);
          tmp_strb := w_hstb(0);
          if w_req then

            -- Regular access logic. Write mode: masked.

            f_Profile_enable_r((0)).d
                := (f_Profile_enable_r((0)).d and not tmp_strb) or tmp_data;
            w_ack := true;

          end if;

          -- Write logic for field Profile_clear: Profile_clear.

          tmp_data := w_hold(32);
          tmp_strb := w_hstb(32);
          if w_req then

            -- Regular access logic. Write mode: enabled.

            f_Profile_clear_r((0)).d := tmp_data;
            w_ack := true;

            -- Handle post-write operation: invalidate.
            f_Profile_clear_r((0)).v := '1';
            f_Profile_clear_r((0)).inval := '1';

          end if;

      end case;

      -------------------------------------------------------------------------
      -- Generated field logic
      -------------------------------------------------------------------------

      -- Post-bus logic for field AFU_DHF: AFU_DHF.

      -- Handle reset for field AFU_DHF.
      if reset = '1' then
        f_AFU_DHF_r((0)).d
            := "1111000000000000000001000000000000000000000000000000000000000000";
        f_AFU_DHF_r((0)).v := '1';
      end if;

      -- Post-bus logic for field AFU_ID_L: AFU_ID_L.

      -- Handle reset for field AFU_ID_L.
      if reset = '1' then
        f_AFU_ID_L_r((0)).d := F_AFU_ID_L_RESET_DATA;
        f_AFU_ID_L_r((0)).v := '1';
      end if;

      -- Post-bus logic for field AFU_ID_H: AFU_ID_H.

      -- Handle reset for field AFU_ID_H.
      if reset = '1' then
        f_AFU_ID_H_r((0)).d := F_AFU_ID_H_RESET_DATA;
        f_AFU_ID_H_r((0)).v := '1';
      end if;

      -- Post-bus logic for field DFH_RSVD0: DFH_RSVD0.

      -- Handle reset for field DFH_RSVD0.
      if reset = '1' then
        f_DFH_RSVD0_r((0)).d
            := "0000000000000000000000000000000000000000000000000000000000000000";
        f_DFH_RSVD0_r((0)).v := '1';
      end if;

      -- Post-bus logic for field DFH_RSVD1: DFH_RSVD1.

      -- Handle reset for field DFH_RSVD1.
      if reset = '1' then
        f_DFH_RSVD1_r((0)).d
            := "0000000000000000000000000000000000000000000000000000000000000000";
        f_DFH_RSVD1_r((0)).v := '1';
      end if;

      -- Post-bus logic for field start: start.

      -- Handle reset for field start.
      if reset = '1' then
        f_start_r((0)).d := '0';
        f_start_r((0)).v := '1';
        f_start_r((0)).inval := '0';
      end if;
      -- Assign the read outputs for field start.
      f_start_data <= f_start_r((0)).d;

      -- Post-bus logic for field stop: stop.

      -- Handle reset for field stop.
      if reset = '1' then
        f_stop_r((0)).d := '0';
        f_stop_r((0)).v := '1';
        f_stop_r((0)).inval := '0';
      end if;
      -- Assign the read outputs for field stop.
      f_stop_data <= f_stop_r((0)).d;

      -- Post-bus logic for field reset: reset.

      -- Handle reset for field reset.
      if reset = '1' then
        f_reset_r((0)).d := '0';
        f_reset_r((0)).v := '1';
        f_reset_r((0)).inval := '0';
      end if;
      -- Assign the read outputs for field reset.
      f_reset_data <= f_reset_r((0)).d;

      -- Post-bus logic for field ExampleBatch_firstidx: ExampleBatch_firstidx.

      -- Handle reset for field ExampleBatch_firstidx.
      if reset = '1' then
        f_ExampleBatch_firstidx_r((0)).d := (others => '0');
        f_ExampleBatch_firstidx_r((0)).v := '0';
      end if;
      -- Assign the read outputs for field ExampleBatch_firstidx.
      f_ExampleBatch_firstidx_data <= f_ExampleBatch_firstidx_r((0)).d;

      -- Post-bus logic for field ExampleBatch_lastidx: ExampleBatch_lastidx.

      -- Handle reset for field ExampleBatch_lastidx.
      if reset = '1' then
        f_ExampleBatch_lastidx_r((0)).d := (others => '0');
        f_ExampleBatch_lastidx_r((0)).v := '0';
      end if;
      -- Assign the read outputs for field ExampleBatch_lastidx.
      f_ExampleBatch_lastidx_data <= f_ExampleBatch_lastidx_r((0)).d;

      -- Post-bus logic for field ExampleBatch_number_values:
      -- ExampleBatch_number_values.

      -- Handle reset for field ExampleBatch_number_values.
      if reset = '1' then
        f_ExampleBatch_number_values_r((0)).d := (others => '0');
        f_ExampleBatch_number_values_r((0)).v := '0';
      end if;
      -- Assign the read outputs for field ExampleBatch_number_values.
      f_ExampleBatch_number_values_data <= f_ExampleBatch_number_values_r((0)).d;

      -- Post-bus logic for field Profile_enable: Profile_enable.

      -- Handle reset for field Profile_enable.
      if reset = '1' then
        f_Profile_enable_r((0)).d := '0';
        f_Profile_enable_r((0)).v := '0';
      end if;
      -- Assign the read outputs for field Profile_enable.
      f_Profile_enable_data <= f_Profile_enable_r((0)).d;

      -- Post-bus logic for field Profile_clear: Profile_clear.

      -- Handle reset for field Profile_clear.
      if reset = '1' then
        f_Profile_clear_r((0)).d := '0';
        f_Profile_clear_r((0)).v := '1';
        f_Profile_clear_r((0)).inval := '0';
      end if;
      -- Assign the read outputs for field Profile_clear.
      f_Profile_clear_data <= f_Profile_clear_r((0)).d;

      -------------------------------------------------------------------------
      -- Boilerplate bus access logic
      -------------------------------------------------------------------------
      -- Perform the write action dictated by the field logic.
      if w_req and not w_block then

        -- Accept write requests by invalidating the request holding
        -- registers.
        awl.valid := '0';
        wl.valid := '0';

        -- Send the appropriate write response.
        bus_v.b.valid := '1';
        if w_nack then
          bus_v.b.resp := AXI4L_RESP_SLVERR;
        elsif w_ack then
          bus_v.b.resp := AXI4L_RESP_OKAY;
        else
          bus_v.b.resp := AXI4L_RESP_DECERR;
        end if;

      end if;

      -- Perform the read action dictated by the field logic.
      if r_req and not r_block then

        -- Accept read requests by invalidating the request holding
        -- registers.
        arl.valid := '0';

        -- Send the appropriate read response.
        bus_v.r.valid := '1';
        if r_nack then
          bus_v.r.resp := AXI4L_RESP_SLVERR;
        elsif r_ack then
          bus_v.r.resp := AXI4L_RESP_OKAY;
          bus_v.r.data := r_data;
        else
          bus_v.r.resp := AXI4L_RESP_DECERR;
        end if;

      end if;

      -- If we're at the end of a multi-word write, clear the write strobe
      -- holding register to prevent previously written data from leaking into
      -- later partial writes.
      if w_multi = '0' then
        w_hstb := (others => '0');
      end if;

      -- Mark the incoming channels as ready when their respective holding
      -- registers are empty.
      bus_v.aw.ready := not awl.valid;
      bus_v.w.ready := not wl.valid;
      bus_v.ar.ready := not arl.valid;

      -------------------------------------------------------------------------
      -- Handle AXI4-lite bus reset
      -------------------------------------------------------------------------
      -- Reset overrides everything, so it comes last. Note that field
      -- registers are *not* reset here; this would complicate code generation.
      -- Instead, the generated field logic blocks include reset logic for the
      -- field-specific registers.
      if reset = '1' then
        bus_v      := AXI4L64_S2M_RESET;
        awl        := AXI4LA_RESET;
        wl         := AXI4LW64_RESET;
        arl        := AXI4LA_RESET;
        w_hstb     := (others => '0');
        w_hold     := (others => '0');
        w_multi    := '0';
        r_multi    := '0';
        r_hold     := (others => '0');
      end if;

      mmio_awready <= bus_v.aw.ready;
      mmio_wready  <= bus_v.w.ready;
      mmio_bvalid  <= bus_v.b.valid;
      mmio_bresp   <= bus_v.b.resp;
      mmio_arready <= bus_v.ar.ready;
      mmio_rvalid  <= bus_v.r.valid;
      mmio_rdata   <= bus_v.r.data;
      mmio_rresp   <= bus_v.r.resp;
      mmio_uirq    <= bus_v.u.irq;

    end if;
  end process;
end behavioral;
