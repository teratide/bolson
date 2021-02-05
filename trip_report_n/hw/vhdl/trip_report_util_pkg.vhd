library ieee;
use ieee.std_logic_1164.all;
use ieee.std_logic_misc.all;
use ieee.numeric_std.all;

library work;
use work.UtilInt_pkg.all;


package trip_report_util_pkg is
  component StreamSerializer is
    generic (
        EPC                   : natural := 1;
        DATA_WIDTH            : natural := 8;
        DIMENSIONALITY        : natural := 1
        );
    port (
        clk                   : in  std_logic;
        reset                 : in  std_logic;
  
        -- Stream(
        --     Bits(DATA_WIDTH),
        --     t=EPC,
        --     c=8
        -- )
        in_valid              : in  std_logic;
        in_ready              : out std_logic;
        in_data               : in  std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
        in_last               : in  std_logic_vector(DIMENSIONALITY*EPC-1 downto 0) := (others => '0');
        in_stai               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '0');
        in_endi               : in  std_logic_vector(log2ceil(EPC)-1 downto 0) := (others => '1');
        in_strb               : in  std_logic_vector(EPC-1 downto 0) := (others => '1');
  
        -- Stream(
        --     Bits(DATA_WIDTH),
        --     c=2
        -- )
        out_valid             : out std_logic;
        out_ready             : in  std_logic;
        out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
        out_strb              : out std_logic;
        out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0)
    );
  end component;

  component D2ListToVecs is
    generic (
      EPC                      : natural := 1;
      DATA_WIDTH               : natural := 8;
      LENGTH_WIDTH             : natural := 8
      );
  port (
      clk                      : in  std_logic;
      reset                    : in  std_logic;

      -- Input stream
      in_valid                 : in  std_logic;
      in_ready                 : out std_logic;
      in_data                  : in  std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
      in_last                  : in  std_logic_vector(1 downto 0) := (others => '0');
      in_count                 : in  std_logic_vector(log2ceil(EPC+1)-1 downto 0) := std_logic_vector(to_unsigned(1, log2ceil(EPC+1)));
      in_dvalid                : in  std_logic := '1';

      -- Element stream
      out_valid                : out std_logic;
      out_ready                : in  std_logic;
      out_data                 : out std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
      out_count                : out std_logic_vector(log2ceil(EPC+1)-1 downto 0);
      out_dvalid               : out std_logic;
      out_last                 : out std_logic;

      -- Length stream
      length_valid             : out std_logic;
      length_ready             : in  std_logic;
      length_data              : out std_logic_vector(LENGTH_WIDTH-1 downto 0);
      length_dvalid            : out std_logic;
      length_count             : out std_logic_vector(0 downto 0);
      length_last              : out std_logic
  );
  end component;

  component DropEmpty is
    generic (
      EPC                      : natural := 1;
      DATA_WIDTH               : natural := 8;
      DIMENSIONALITY           : natural := 1
      );
  port (
      clk                      : in  std_logic;
      reset                    : in  std_logic;

      -- Input stream
      in_valid                 : in  std_logic;
      in_ready                 : out std_logic;
      in_data                  : in  std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
      in_dvalid                : in  std_logic := '1';
      in_last                  : in  std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');

      -- Output stream
      out_valid                : out std_logic;
      out_ready                : in  std_logic;
      out_data                 : out std_logic_vector(DATA_WIDTH*EPC-1 downto 0);
      out_dvalid               : out std_logic := '1';
      out_last                 : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0')
  );
  end component;

  component PacketArbiter is
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
  
        out_valid             : out std_logic;
        out_ready             : in  std_logic;
        out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
        out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
        out_strb              : out std_logic := '1';
  
        cmd_valid             : in  std_logic;
        cmd_ready             : out std_logic;
        cmd_index             : in  std_logic_vector(INDEX_WIDTH-1 downto 0);
        cmd_last              : in  std_logic_vector(1 downto 0) := (others => '0')
    );
  end component;

  component PacketFIFO is
    generic (
        DATA_WIDTH            : natural;
        DEPTH                 : natural;
        PKT_COUNT_WIDTH       : natural := 8;
        DIMENSIONALITY        : natural := 1
        );
    port (
        clk                   : in  std_logic;
        reset                 : in  std_logic;
  
        in_valid              : in  std_logic;
        in_ready              : out std_logic;
        in_data               : in  std_logic_vector(DATA_WIDTH-1 downto 0);
        in_last               : in  std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
        in_strb               : in  std_logic := '1';
  
        out_valid             : out std_logic;
        out_ready             : in  std_logic;
        out_data              : out std_logic_vector(DATA_WIDTH-1 downto 0);
        out_last              : out std_logic_vector(DIMENSIONALITY-1 downto 0) := (others => '0');
        out_strb              : out std_logic := '1';
  
        packet_valid          : out std_logic;
        packet_ready          : in  std_logic;
        packet_count          : out std_logic_vector(PKT_COUNT_WIDTH-1 downto 0);
        packet_last           : out std_logic := '0'
    );
  end component;

  component ArbiterController is
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
        cmd_last              : out std_logic_vector(1 downto 0);
  
        tag_valid             : out std_logic;
        tag_ready             : in  std_logic;
        tag                   : out std_logic_vector(TAG_WIDTH-1 downto 0);
        tag_last              : out std_logic;
  
        tag_cfg               : in std_logic_vector(NUM_INPUTS*TAG_WIDTH-1 downto 0)
    );
  end component;

  component trip_report_sub is
    generic (
      EPC                                              : natural := 8;
      TAG_WIDTH                                        : natural := 1;
      NUM_PARSERS                                      : natural := 1;
          
      -- 
      -- INTEGER FIELDS
      --
      TIMEZONE_INT_WIDTH                               : natural := 16;
      TIMEZONE_INT_P_PIPELINE_STAGES                   : natural := 1;
      TIMEZONE_BUFFER_D                                : natural := 1;
  
      VIN_INT_WIDTH                                    : natural := 16;
      VIN_INT_P_PIPELINE_STAGES                        : natural := 1;
      VIN_BUFFER_D                                     : natural := 1;
  
      ODOMETER_INT_WIDTH                               : natural := 16;
      ODOMETER_INT_P_PIPELINE_STAGES                   : natural := 1;
      ODOMETER_BUFFER_D                                : natural := 1;
  
      AVGSPEED_INT_WIDTH                               : natural := 16;
      AVGSPEED_INT_P_PIPELINE_STAGES                   : natural := 1;
      AVGSPEED_BUFFER_D                                : natural := 1;
  
      ACCEL_DECEL_INT_WIDTH                            : natural := 16;
      ACCEL_DECEL_INT_P_PIPELINE_STAGES                : natural := 1;
      ACCEL_DECEL_BUFFER_D                             : natural := 1;
  
      SPEED_CHANGES_INT_WIDTH                          : natural := 16;
      SPEED_CHANGES_INT_P_PIPELINE_STAGES              : natural := 1;
      SPEED_CHANGES_BUFFER_D                           : natural := 1;
  
      -- 
      -- BOOLEAN FIELDS
      --
      HYPERMILING_BUFFER_D                              : natural := 1;
      ORIENTATION_BUFFER_D                              : natural := 1;
  
      -- 
      -- INTEGER ARRAY FIELDS
      --
      SEC_IN_BAND_INT_WIDTH                             : natural := 16;
      SEC_IN_BAND_INT_P_PIPELINE_STAGES                 : natural := 1;
      SEC_IN_BAND_BUFFER_D                              : natural := 1;
  
      MILES_IN_TIME_RANGE_INT_WIDTH                     : natural := 16;
      MILES_IN_TIME_RANGE_INT_P_PIPELINE_STAGES         : natural := 1; 
      MILES_IN_TIME_RANGE_BUFFER_D                      : natural := 1; 
  
  
      CONST_SPEED_MILES_IN_BAND_INT_WIDTH               : natural := 16;
      CONST_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES   : natural := 1; 
      CONST_SPEED_MILES_IN_BAND_BUFFER_D                : natural := 1; 
  
  
      VARY_SPEED_MILES_IN_BAND_INT_WIDTH                : natural := 16;
      VARY_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES    : natural := 1; 
      VARY_SPEED_MILES_IN_BAND_BUFFER_D                 : natural := 1; 
  
  
      SEC_DECEL_INT_WIDTH                               : natural := 16;
      SEC_DECEL_INT_P_PIPELINE_STAGES                   : natural := 1; 
      SEC_DECEL_BUFFER_D                                : natural := 1; 
                    
                    
      SEC_ACCEL_INT_WIDTH                               : natural := 16;
      SEC_ACCEL_INT_P_PIPELINE_STAGES                   : natural := 1; 
      SEC_ACCEL_BUFFER_D                                : natural := 1; 
                    
                    
      BRAKING_INT_WIDTH                                 : natural := 16;
      BRAKING_INT_P_PIPELINE_STAGES                     : natural := 1; 
      BRAKING_BUFFER_D                                  : natural := 1; 
  
  
      ACCEL_INT_WIDTH                                   : natural := 16;
      ACCEL_INT_P_PIPELINE_STAGES                       : natural := 1; 
      ACCEL_BUFFER_D                                    : natural := 1; 
  
  
      SMALL_SPEED_VAR_INT_WIDTH                         : natural := 16;
      SMALL_SPEED_VAR_INT_P_PIPELINE_STAGES             : natural := 1; 
      SMALL_SPEED_VAR_BUFFER_D                          : natural := 1; 
  
  
      LARGE_SPEED_VAR_INT_WIDTH                         : natural := 16;
      LARGE_SPEED_VAR_INT_P_PIPELINE_STAGES             : natural := 1; 
      LARGE_SPEED_VAR_BUFFER_D                          : natural := 1;
  
      -- 
      -- STRING FIELDS
      --
      TIMESTAMP_BUFFER_D                          : natural := 1;
  
      END_REQ_EN                                  : boolean := false
      );              
      port (              
      clk                                         : in  std_logic;
      reset                                       : in  std_logic;
      
      in_valid                                    : in  std_logic_vector(NUM_PARSERS-1 downto 0);
      in_ready                                    : out std_logic_vector(NUM_PARSERS-1 downto 0);
      in_data                                     : in  std_logic_vector(8*EPC*NUM_PARSERS-1 downto 0);
      in_last                                     : in  std_logic_vector(2*EPC*NUM_PARSERS-1 downto 0);
      in_stai                                     : in  std_logic_vector(log2ceil(EPC)*NUM_PARSERS-1 downto 0) := (others => '0');
      in_endi                                     : in  std_logic_vector(log2ceil(EPC)*NUM_PARSERS-1 downto 0) := (others => '1');
      in_strb                                     : in  std_logic_vector(EPC*NUM_PARSERS-1 downto 0);
      
      end_req                                     : in  std_logic := '0';
      end_ack                                     : out std_logic;
      
      timezone_valid                              : out std_logic;
      timezone_ready                              : in  std_logic;
      timezone_strb                               : out std_logic;
      timezone_data                               : out std_logic_vector(TIMEZONE_INT_WIDTH-1 downto 0);
      timezone_last                               : out std_logic_vector(1 downto 0);
  
      --    
      -- INTEGER FIELDS   
      --    
      vin_valid                                   : out std_logic;
      vin_ready                                   : in  std_logic;
      vin_data                                    : out std_logic_vector(VIN_INT_WIDTH-1 downto 0);
      vin_strb                                    : out std_logic;
      vin_last                                    : out std_logic_vector(1 downto 0);
          
      odometer_valid                              : out std_logic;
      odometer_ready                              : in  std_logic;
      odometer_data                               : out std_logic_vector(ODOMETER_INT_WIDTH-1 downto 0);
      odometer_strb                               : out std_logic;
      odometer_last                               : out std_logic_vector(1 downto 0);
  
      avgspeed_valid                              : out std_logic;
      avgspeed_ready                              : in  std_logic;
      avgspeed_data                               : out std_logic_vector(AVGSPEED_INT_WIDTH-1 downto 0);
      avgspeed_strb                               : out std_logic;
      avgspeed_last                               : out std_logic_vector(1 downto 0);
  
      accel_decel_valid                           : out std_logic;
      accel_decel_ready                           : in  std_logic;
      accel_decel_data                            : out std_logic_vector(ACCEL_DECEL_INT_WIDTH-1 downto 0);
      accel_decel_strb                            : out std_logic;
      accel_decel_last                            : out std_logic_vector(1 downto 0);
  
      speed_changes_valid                         : out std_logic;
      speed_changes_ready                         : in  std_logic;
      speed_changes_data                          : out std_logic_vector(SPEED_CHANGES_INT_WIDTH-1 downto 0);
      speed_changes_strb                          : out std_logic;
      speed_changes_last                          : out std_logic_vector(1 downto 0);
  
      --    
      -- BOOLEAN FIELDS   
      --    
      hypermiling_valid                           : out std_logic;
      hypermiling_ready                           : in  std_logic;
      hypermiling_data                            : out std_logic_vector(0 downto 0);
      hypermiling_strb                            : out std_logic;
      hypermiling_last                            : out std_logic_vector(1 downto 0);
  
      orientation_valid                           : out std_logic;
      orientation_ready                           : in  std_logic;
      orientation_data                            : out std_logic_vector(0 downto 0);
      orientation_strb                            : out std_logic;
      orientation_last                            : out std_logic_vector(1 downto 0);
  
      --    
      -- INTEGER ARRAY FIELDS   
      --    
      sec_in_band_valid                           : out std_logic;
      sec_in_band_ready                           : in  std_logic;
      sec_in_band_data                            : out std_logic_vector(SEC_IN_BAND_INT_WIDTH-1 downto 0);
      sec_in_band_strb                            : out std_logic;
      sec_in_band_last                            : out std_logic_vector(2 downto 0);
  
      miles_in_time_range_valid                   : out std_logic;
      miles_in_time_range_ready                   : in  std_logic;
      miles_in_time_range_data                    : out std_logic_vector(MILES_IN_TIME_RANGE_INT_WIDTH-1 downto 0);
      miles_in_time_range_strb                    : out std_logic;
      miles_in_time_range_last                    : out std_logic_vector(2 downto 0);
  
  
      const_speed_miles_in_band_valid             : out std_logic;
      const_speed_miles_in_band_ready             : in  std_logic;
      const_speed_miles_in_band_data              : out std_logic_vector(CONST_SPEED_MILES_IN_BAND_INT_WIDTH-1 downto 0);
      const_speed_miles_in_band_strb              : out std_logic;
      const_speed_miles_in_band_last              : out std_logic_vector(2 downto 0);
  
  
      vary_speed_miles_in_band_valid              : out std_logic;
      vary_speed_miles_in_band_ready              : in  std_logic;
      vary_speed_miles_in_band_data               : out std_logic_vector(VARY_SPEED_MILES_IN_BAND_INT_WIDTH-1 downto 0);
      vary_speed_miles_in_band_strb               : out std_logic;
      vary_speed_miles_in_band_last               : out std_logic_vector(2 downto 0);
  
  
      sec_decel_valid                             : out std_logic;
      sec_decel_ready                             : in  std_logic;
      sec_decel_data                              : out std_logic_vector(SEC_DECEL_INT_WIDTH-1 downto 0);
      sec_decel_strb                              : out std_logic;
      sec_decel_last                              : out std_logic_vector(2 downto 0);
        
        
      sec_accel_valid                             : out std_logic;
      sec_accel_ready                             : in  std_logic;
      sec_accel_data                              : out std_logic_vector(SEC_ACCEL_INT_WIDTH-1 downto 0);
      sec_accel_strb                              : out std_logic;
      sec_accel_last                              : out std_logic_vector(2 downto 0);
        
        
      braking_valid                               : out std_logic;
      braking_ready                               : in  std_logic;
      braking_data                                : out std_logic_vector(BRAKING_INT_WIDTH-1 downto 0);
      braking_strb                                : out std_logic;
      braking_last                                : out std_logic_vector(2 downto 0);
  
  
      accel_valid                                 : out std_logic;
      accel_ready                                 : in  std_logic;
      accel_data                                  : out std_logic_vector(ACCEL_INT_WIDTH-1 downto 0);
      accel_strb                                  : out std_logic;
      accel_last                                  : out std_logic_vector(2 downto 0);
  
  
      small_speed_var_valid                       : out std_logic;
      small_speed_var_ready                       : in  std_logic;
      small_speed_var_data                        : out std_logic_vector(SMALL_SPEED_VAR_INT_WIDTH-1 downto 0);
      small_speed_var_strb                        : out std_logic;
      small_speed_var_last                        : out std_logic_vector(2 downto 0);
  
  
      large_speed_var_valid                       : out std_logic;
      large_speed_var_ready                       : in  std_logic;
      large_speed_var_data                        : out std_logic_vector(LARGE_SPEED_VAR_INT_WIDTH-1 downto 0);
      large_speed_var_strb                        : out std_logic;
      large_speed_var_last                        : out std_logic_vector(2 downto 0);
  
      --    
      -- STRING FIELDS   
      -- 
      timestamp_valid                             : out std_logic;
      timestamp_ready                             : in  std_logic;
      timestamp_data                              : out std_logic_vector(7 downto 0);
      timestamp_last                              : out std_logic_vector(2 downto 0);
      timestamp_strb                              : out std_logic;
      
      --    
      -- TAG STREAM   
      -- 
      tag_valid                                   : out std_logic;
      tag_ready                                   : in  std_logic;
      tag                                         : out std_logic_vector(TAG_WIDTH-1 downto 0);
      tag_last                                    : out std_logic;
      
      tag_cfg                                     : in std_logic_vector(TAG_WIDTH*NUM_PARSERS-1 downto 0)
    );
  end component;
  
end trip_report_util_pkg;