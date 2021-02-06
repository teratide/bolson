
library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use ieee.std_logic_misc.all;

library work;
use work.Stream_pkg.all;
use work.UtilInt_pkg.all;
use work.Json_pkg.all;
use work.trip_report_pkg.all;
use work.trip_report_util_pkg.all;
use work.tr_field_pkg.all;

entity trip_report_sub is
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
    tag_strb                                    : out std_logic;
    tag_last                                    : out std_logic;
    
    tag_cfg                                     : in std_logic_vector(TAG_WIDTH*NUM_PARSERS-1 downto 0)
  );
end trip_report_sub;

architecture Implementation of trip_report_sub is
    
  constant INDEX_WIDTH : natural := log2ceil(NUM_PARSERS+1);
  constant PKT_COUNT_WIDTH : natural := 2;

  --
  -- Packet FIFO depths
  --
  constant TIMEZONE_FIFO_DEPTH : natural := 3;
  constant TIMESTAMP_FIFO_DEPTH : natural := 40;
  constant VIN_FIFO_DEPTH : natural := 3;
  constant ODOMETER_FIFO_DEPTH : natural := 3;
  constant AVGSPEED_FIFO_DEPTH : natural := 3;
  constant ACCEL_DECEL_FIFO_DEPTH : natural := 3;
  constant SPEED_CHANGES_FIFO_DEPTH : natural := 3;
  constant HYPERMILING_FIFO_DEPTH : natural := 3;
  constant ORIENTATION_FIFO_DEPTH : natural := 3;
  constant SEC_IN_BAND_FIFO_DEPTH : natural := 15;
  constant MILES_IN_TIME_RANGE_FIFO_DEPTH : natural := 27;
  constant CONST_SPEED_MILES_IN_BAND_FIFO_DEPTH : natural := 15;
  constant VARY_SPEED_MILES_IN_BAND_FIFO_DEPTH : natural := 15;
  constant SEC_DECEL_FIFO_DEPTH : natural := 13;
  constant SEC_ACCEL_FIFO_DEPTH : natural := 13;
  constant BRAKING_FIFO_DEPTH : natural := 9;
  constant ACCEL_FIFO_DEPTH : natural := 9;
  constant SMALL_SPEED_VAR_FIFO_DEPTH : natural := 16;
  constant LARGE_SPEED_VAR_FIFO_DEPTH : natural := 16;

    
  --
  -- Field: timezone
  --
  
  -- Parser <-> FIFO
  signal timezone_valid_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timezone_ready_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timezone_data_f                         : std_logic_vector(TIMEZONE_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal timezone_strb_f                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timezone_last_f                         : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal timezone_valid_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timezone_ready_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timezone_data_a                         : std_logic_vector(TIMEZONE_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal timezone_strb_a                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timezone_last_a                         : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: vin
  --
  
  -- Parser <-> FIFO
  signal vin_valid_f                             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vin_ready_f                             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vin_data_f                              : std_logic_vector(VIN_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal vin_strb_f                              : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vin_last_f                              : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal vin_valid_a                             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vin_ready_a                             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vin_data_a                              : std_logic_vector(VIN_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal vin_strb_a                              : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vin_last_a                              : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: odometer
  --
  
  -- Parser <-> FIFO
  signal odometer_valid_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal odometer_ready_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal odometer_data_f                         : std_logic_vector(ODOMETER_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal odometer_strb_f                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal odometer_last_f                         : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal odometer_valid_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal odometer_ready_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal odometer_data_a                         : std_logic_vector(ODOMETER_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal odometer_strb_a                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal odometer_last_a                         : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: avgspeed
  --
  
  -- Parser <-> FIFO
  signal avgspeed_valid_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal avgspeed_ready_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal avgspeed_data_f                         : std_logic_vector(AVGSPEED_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal avgspeed_strb_f                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal avgspeed_last_f                         : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal avgspeed_valid_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal avgspeed_ready_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal avgspeed_data_a                         : std_logic_vector(AVGSPEED_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal avgspeed_strb_a                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal avgspeed_last_a                         : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: accel_decel
  --
  
  -- Parser <-> FIFO
  signal accel_decel_valid_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_decel_ready_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_decel_data_f                      : std_logic_vector(ACCEL_DECEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal accel_decel_strb_f                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_decel_last_f                      : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal accel_decel_valid_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_decel_ready_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_decel_data_a                      : std_logic_vector(ACCEL_DECEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal accel_decel_strb_a                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_decel_last_a                      : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: speed_changes
  --
  
  -- Parser <-> FIFO
  signal speed_changes_valid_f                   : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal speed_changes_ready_f                   : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal speed_changes_data_f                    : std_logic_vector(SPEED_CHANGES_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal speed_changes_strb_f                    : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal speed_changes_last_f                    : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal speed_changes_valid_a                   : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal speed_changes_ready_a                   : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal speed_changes_data_a                    : std_logic_vector(SPEED_CHANGES_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal speed_changes_strb_a                    : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal speed_changes_last_a                    : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: hypermiling
  --
  
  -- Parser <-> FIFO
  signal hypermiling_valid_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_ready_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_data_f                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_strb_f                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_last_f                      : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal hypermiling_valid_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_ready_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_data_a                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_strb_a                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal hypermiling_last_a                      : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: orientation
  --
  
  -- Parser <-> FIFO
  signal orientation_valid_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_ready_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_data_f                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_strb_f                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_last_f                      : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal orientation_valid_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_ready_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_data_a                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_strb_a                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal orientation_last_a                      : std_logic_vector(2*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: sec_in_band
  --
  
  -- Parser <-> FIFO
  signal sec_in_band_valid_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_in_band_ready_f                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_in_band_data_f                      : std_logic_vector(SEC_IN_BAND_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal sec_in_band_strb_f                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_in_band_last_f                      : std_logic_vector(3*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal sec_in_band_valid_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_in_band_ready_a                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_in_band_data_a                      : std_logic_vector(SEC_IN_BAND_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal sec_in_band_strb_a                      : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_in_band_last_a                      : std_logic_vector(3*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: miles_in_time_range
  --
  
  -- Parser <-> FIFO
  signal miles_in_time_range_valid_f             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_ready_f             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_data_f              : std_logic_vector(MILES_IN_TIME_RANGE_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_strb_f              : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_last_f              : std_logic_vector(3*NUM_PARSERS-1 downto 0);
  
  -- FIFO <-> arbiter
  signal miles_in_time_range_valid_a             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_ready_a             : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_data_a              : std_logic_vector(MILES_IN_TIME_RANGE_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_strb_a              : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal miles_in_time_range_last_a              : std_logic_vector(3*NUM_PARSERS-1 downto 0);
  
  --
  -- Field: const_speed_miles_in_band
  --
  
  -- Parser <-> FIFO
  signal const_speed_miles_in_band_valid_f       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_ready_f       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_data_f        : std_logic_vector(CONST_SPEED_MILES_IN_BAND_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_strb_f        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_last_f        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal const_speed_miles_in_band_valid_a       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_ready_a       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_data_a        : std_logic_vector(CONST_SPEED_MILES_IN_BAND_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_strb_a        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal const_speed_miles_in_band_last_a        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: vary_speed_miles_in_band 
  -- 
   
  -- Parser <-> FIFO 
  signal vary_speed_miles_in_band_valid_f        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_ready_f        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_data_f         : std_logic_vector(VARY_SPEED_MILES_IN_BAND_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_strb_f         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_last_f         : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal vary_speed_miles_in_band_valid_a        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_ready_a        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_data_a         : std_logic_vector(VARY_SPEED_MILES_IN_BAND_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_strb_a         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal vary_speed_miles_in_band_last_a         : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: sec_decel 
  -- 
   
  -- Parser <-> FIFO 
  signal sec_decel_valid_f                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_decel_ready_f                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_decel_data_f                        : std_logic_vector(SEC_DECEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal sec_decel_strb_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_decel_last_f                        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal sec_decel_valid_a                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_decel_ready_a                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_decel_data_a                        : std_logic_vector(SEC_DECEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal sec_decel_strb_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_decel_last_a                        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: sec_accel 
  -- 
   
  -- Parser <-> FIFO 
  signal sec_accel_valid_f                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_accel_ready_f                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_accel_data_f                        : std_logic_vector(SEC_ACCEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal sec_accel_strb_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_accel_last_f                        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal sec_accel_valid_a                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_accel_ready_a                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_accel_data_a                        : std_logic_vector(SEC_ACCEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal sec_accel_strb_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal sec_accel_last_a                        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: braking 
  -- 
   
  -- Parser <-> FIFO 
  signal braking_valid_f                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal braking_ready_f                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal braking_data_f                          : std_logic_vector(BRAKING_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal braking_strb_f                          : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal braking_last_f                          : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal braking_valid_a                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal braking_ready_a                         : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal braking_data_a                          : std_logic_vector(BRAKING_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal braking_strb_a                          : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal braking_last_a                          : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: accel 
  -- 
   
  -- Parser <-> FIFO 
  signal accel_valid_f                           : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_ready_f                           : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_data_f                            : std_logic_vector(ACCEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal accel_strb_f                            : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_last_f                            : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal accel_valid_a                           : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_ready_a                           : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_data_a                            : std_logic_vector(ACCEL_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal accel_strb_a                            : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal accel_last_a                            : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: small_speed_var 
  -- 
   
  -- Parser <-> FIFO 
  signal small_speed_var_valid_f                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal small_speed_var_ready_f                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal small_speed_var_data_f                  : std_logic_vector(SMALL_SPEED_VAR_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal small_speed_var_strb_f                  : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal small_speed_var_last_f                  : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal small_speed_var_valid_a                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal small_speed_var_ready_a                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal small_speed_var_data_a                  : std_logic_vector(SMALL_SPEED_VAR_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal small_speed_var_strb_a                  : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal small_speed_var_last_a                  : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: large_speed_var 
  -- 
   
  -- Parser <-> FIFO 
  signal large_speed_var_valid_f                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal large_speed_var_ready_f                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal large_speed_var_data_f                  : std_logic_vector(LARGE_SPEED_VAR_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal large_speed_var_strb_f                  : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal large_speed_var_last_f                  : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal large_speed_var_valid_a                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal large_speed_var_ready_a                 : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal large_speed_var_data_a                  : std_logic_vector(LARGE_SPEED_VAR_INT_WIDTH*NUM_PARSERS-1 downto 0);
  signal large_speed_var_strb_a                  : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal large_speed_var_last_a                  : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- 
  -- Field: timestamp 
  -- 
   
  -- Parser <-> FIFO 
  signal timestamp_valid_f                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_ready_f                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_data_f                        : std_logic_vector(8*NUM_PARSERS-1 downto 0);
  signal timestamp_strb_f                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_last_f                        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
   
  -- FIFO <-> arbiter 
  signal timestamp_valid_a                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_ready_a                       : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_data_a                        : std_logic_vector(8*NUM_PARSERS-1 downto 0);
  signal timestamp_strb_a                        : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_last_a                        : std_logic_vector(3*NUM_PARSERS-1 downto 0);
     
  signal timestamp_ser_valid                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_ser_ready                     : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal timestamp_ser_data                      : std_logic_vector(8*EPC*NUM_PARSERS-1 downto 0);
  signal timestamp_ser_last                      : std_logic_vector(3*EPC*NUM_PARSERS-1 downto 0);
  signal timestamp_ser_strb                      : std_logic_vector(EPC*NUM_PARSERS-1 downto 0);

  type field_cntrl_array_t is array (0 to NUM_PARSERS-1) of std_logic_vector(19-1 downto 0);
  signal pkt_valid                        : field_cntrl_array_t;
  signal pkt_ready                        : field_cntrl_array_t;
  
  signal cmd_valid                        : std_logic_vector(19-1 downto 0);
  signal cmd_ready                        : std_logic_vector(19-1 downto 0);
  
  signal last_pkt_valid                   : std_logic_vector(19-1 downto 0);
  signal last_pkt_ready                   : std_logic_vector(19-1 downto 0);
  
  signal cmd_valid_sync                   : std_logic;
  signal cmd_ready_sync                   : std_logic;
  signal cmd_index                        : std_logic_vector(INDEX_WIDTH-1 downto 0);
  
  signal pkt_valid_sync                   : std_logic_vector(NUM_PARSERS-1 downto 0);
  signal pkt_ready_sync                   : std_logic_vector(NUM_PARSERS-1 downto 0);
  
  signal last_pkt_valid_sync              : std_logic;
  signal last_pkt_ready_sync              : std_logic;

begin
    

  gen_parsers : for p in 0 to NUM_PARSERS-1 generate
    parser : TripReportParser
      generic map(
        EPC                                              => EPC,

        -- 
        -- INTEGER FIELDS
        --
        TIMEZONE_INT_WIDTH                               => TIMEZONE_INT_WIDTH,
        TIMEZONE_INT_P_PIPELINE_STAGES                   => 4,
        TIMEZONE_BUFFER_D                                => 1,

        VIN_INT_WIDTH                                    => VIN_INT_WIDTH,
        VIN_INT_P_PIPELINE_STAGES                        => 4,
        VIN_BUFFER_D                                     => 1,

        ODOMETER_INT_WIDTH                               => ODOMETER_INT_WIDTH,
        ODOMETER_INT_P_PIPELINE_STAGES                   => 4,
        ODOMETER_BUFFER_D                                => 1,

        AVGSPEED_INT_WIDTH                               => AVGSPEED_INT_WIDTH,
        AVGSPEED_INT_P_PIPELINE_STAGES                   => 4,
        AVGSPEED_BUFFER_D                                => 1,

        ACCEL_DECEL_INT_WIDTH                            => ACCEL_DECEL_INT_WIDTH,
        ACCEL_DECEL_INT_P_PIPELINE_STAGES                => 4,
        ACCEL_DECEL_BUFFER_D                             => 1,

        SPEED_CHANGES_INT_WIDTH                          => SPEED_CHANGES_INT_WIDTH,
        SPEED_CHANGES_INT_P_PIPELINE_STAGES              => 4,
        SPEED_CHANGES_BUFFER_D                           => 1,

        -- 
        -- BOOLEAN FIELDS
        --
        HYPERMILING_BUFFER_D                             => 1,
        ORIENTATION_BUFFER_D                             => 1,

        -- 
        -- INTEGER ARRAY FIELDS
        --
        SEC_IN_BAND_INT_WIDTH                            => SEC_IN_BAND_INT_WIDTH,
        SEC_IN_BAND_INT_P_PIPELINE_STAGES                => 4,
        SEC_IN_BAND_BUFFER_D                             => 12,

        MILES_IN_TIME_RANGE_INT_WIDTH                    => MILES_IN_TIME_RANGE_INT_WIDTH,
        MILES_IN_TIME_RANGE_INT_P_PIPELINE_STAGES        => 4,
        MILES_IN_TIME_RANGE_BUFFER_D                     => 24,

        CONST_SPEED_MILES_IN_BAND_INT_WIDTH              => CONST_SPEED_MILES_IN_BAND_INT_WIDTH,
        CONST_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES  => 4,
        CONST_SPEED_MILES_IN_BAND_BUFFER_D               => 12,

        VARY_SPEED_MILES_IN_BAND_INT_WIDTH               => VARY_SPEED_MILES_IN_BAND_INT_WIDTH,
        VARY_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES   => 4,
        VARY_SPEED_MILES_IN_BAND_BUFFER_D                => 12,

        SEC_DECEL_INT_WIDTH                              => SEC_DECEL_INT_WIDTH,
        SEC_DECEL_INT_P_PIPELINE_STAGES                  => 4,
        SEC_DECEL_BUFFER_D                               => 10,

        SEC_ACCEL_INT_WIDTH                              => SEC_ACCEL_INT_WIDTH,
        SEC_ACCEL_INT_P_PIPELINE_STAGES                  => 4,
        SEC_ACCEL_BUFFER_D                               => 10,

        BRAKING_INT_WIDTH                                => BRAKING_INT_WIDTH,
        BRAKING_INT_P_PIPELINE_STAGES                    => 4,
        BRAKING_BUFFER_D                                 => 13,

        ACCEL_INT_WIDTH                                  => ACCEL_INT_WIDTH,
        ACCEL_INT_P_PIPELINE_STAGES                      => 4,
        ACCEL_BUFFER_D                                   => 6,

        SMALL_SPEED_VAR_INT_WIDTH                        => SMALL_SPEED_VAR_INT_WIDTH,
        SMALL_SPEED_VAR_INT_P_PIPELINE_STAGES            => 4,
        SMALL_SPEED_VAR_BUFFER_D                         => 13,

        LARGE_SPEED_VAR_INT_WIDTH                        => LARGE_SPEED_VAR_INT_WIDTH,
        LARGE_SPEED_VAR_INT_P_PIPELINE_STAGES            => 4,
        LARGE_SPEED_VAR_BUFFER_D                         => 13,

        -- 
        -- STRING FIELDS
        --
        TIMESTAMP_BUFFER_D                               => 1,
        END_REQ_EN                                       => false
      )
      port map(
        clk                                         => clk,
        reset                                       => reset,

        in_valid                                    => in_valid(p),
        in_ready                                    => in_ready(p),
        in_data                                     => in_data(8*EPC*(p+1)-1 downto 8*EPC*p),
        in_last                                     => in_last(2*EPC*(p+1)-1 downto 2*EPC*p),
        in_stai                                     => (others => '0'),
        in_endi                                     => (others => '1'),
        in_strb                                     => in_strb(EPC*(p+1)-1 downto EPC*p),

        end_req                                     => '0',
        end_ack                                     => open,

         
        timezone_valid                               => timezone_valid_f(p),
        timezone_ready                               => timezone_ready_f(p),
        timezone_data                                => timezone_data_f(TIMEZONE_INT_WIDTH*(p+1)-1 downto TIMEZONE_INT_WIDTH*p),
        timezone_strb                                => timezone_strb_f(p),
        timezone_last                                => timezone_last_f(2*(p+1)-1 downto 2*p),
     
        vin_valid                                    => vin_valid_f(p),
        vin_ready                                    => vin_ready_f(p),
        vin_data                                     => vin_data_f(VIN_INT_WIDTH*(p+1)-1 downto VIN_INT_WIDTH*p),
        vin_strb                                     => vin_strb_f(p),
        vin_last                                     => vin_last_f(2*(p+1)-1 downto 2*p),
     
        odometer_valid                               => odometer_valid_f(p),
        odometer_ready                               => odometer_ready_f(p),
        odometer_data                                => odometer_data_f(ODOMETER_INT_WIDTH*(p+1)-1 downto ODOMETER_INT_WIDTH*p),
        odometer_strb                                => odometer_strb_f(p),
        odometer_last                                => odometer_last_f(2*(p+1)-1 downto 2*p),
     
        avgspeed_valid                               => avgspeed_valid_f(p),
        avgspeed_ready                               => avgspeed_ready_f(p),
        avgspeed_data                                => avgspeed_data_f(AVGSPEED_INT_WIDTH*(p+1)-1 downto AVGSPEED_INT_WIDTH*p),
        avgspeed_strb                                => avgspeed_strb_f(p),
        avgspeed_last                                => avgspeed_last_f(2*(p+1)-1 downto 2*p),
     
        accel_decel_valid                            => accel_decel_valid_f(p),
        accel_decel_ready                            => accel_decel_ready_f(p),
        accel_decel_data                             => accel_decel_data_f(ACCEL_DECEL_INT_WIDTH*(p+1)-1 downto ACCEL_DECEL_INT_WIDTH*p),
        accel_decel_strb                             => accel_decel_strb_f(p),
        accel_decel_last                             => accel_decel_last_f(2*(p+1)-1 downto 2*p),
     
        speed_changes_valid                          => speed_changes_valid_f(p),
        speed_changes_ready                          => speed_changes_ready_f(p),
        speed_changes_data                           => speed_changes_data_f(SPEED_CHANGES_INT_WIDTH*(p+1)-1 downto SPEED_CHANGES_INT_WIDTH*p),
        speed_changes_strb                           => speed_changes_strb_f(p),
        speed_changes_last                           => speed_changes_last_f(2*(p+1)-1 downto 2*p),
     
        hypermiling_valid                            => hypermiling_valid_f(p),
        hypermiling_ready                            => hypermiling_ready_f(p),
        hypermiling_data                             => hypermiling_data_f(p),
        hypermiling_strb                             => hypermiling_strb_f(p),
        hypermiling_last                             => hypermiling_last_f(2*(p+1)-1 downto 2*p),
     
        orientation_valid                            => orientation_valid_f(p),
        orientation_ready                            => orientation_ready_f(p),
        orientation_data                             => orientation_data_f(p),
        orientation_strb                             => orientation_strb_f(p),
        orientation_last                             => orientation_last_f(2*(p+1)-1 downto 2*p),
     
        sec_in_band_valid                            => sec_in_band_valid_f(p),
        sec_in_band_ready                            => sec_in_band_ready_f(p),
        sec_in_band_data                             => sec_in_band_data_f(SEC_IN_BAND_INT_WIDTH*(p+1)-1 downto SEC_IN_BAND_INT_WIDTH*p),
        sec_in_band_strb                             => sec_in_band_strb_f(p),
        sec_in_band_last                             => sec_in_band_last_f(3*(p+1)-1 downto 3*p),
     
        miles_in_time_range_valid                    => miles_in_time_range_valid_f(p),
        miles_in_time_range_ready                    => miles_in_time_range_ready_f(p),
        miles_in_time_range_data                     => miles_in_time_range_data_f(MILES_IN_TIME_RANGE_INT_WIDTH*(p+1)-1 downto MILES_IN_TIME_RANGE_INT_WIDTH*p),
        miles_in_time_range_strb                     => miles_in_time_range_strb_f(p),
        miles_in_time_range_last                     => miles_in_time_range_last_f(3*(p+1)-1 downto 3*p),
     
        const_speed_miles_in_band_valid              => const_speed_miles_in_band_valid_f(p),
        const_speed_miles_in_band_ready              => const_speed_miles_in_band_ready_f(p),
        const_speed_miles_in_band_data               => const_speed_miles_in_band_data_f(CONST_SPEED_MILES_IN_BAND_INT_WIDTH*(p+1)-1 downto CONST_SPEED_MILES_IN_BAND_INT_WIDTH*p),
        const_speed_miles_in_band_strb               => const_speed_miles_in_band_strb_f(p),
        const_speed_miles_in_band_last               => const_speed_miles_in_band_last_f(3*(p+1)-1 downto 3*p),
     
        vary_speed_miles_in_band_valid               => vary_speed_miles_in_band_valid_f(p),
        vary_speed_miles_in_band_ready               => vary_speed_miles_in_band_ready_f(p),
        vary_speed_miles_in_band_data                => vary_speed_miles_in_band_data_f(VARY_SPEED_MILES_IN_BAND_INT_WIDTH*(p+1)-1 downto VARY_SPEED_MILES_IN_BAND_INT_WIDTH*p),
        vary_speed_miles_in_band_strb                => vary_speed_miles_in_band_strb_f(p),
        vary_speed_miles_in_band_last                => vary_speed_miles_in_band_last_f(3*(p+1)-1 downto 3*p),
     
        sec_decel_valid                              => sec_decel_valid_f(p),
        sec_decel_ready                              => sec_decel_ready_f(p),
        sec_decel_data                               => sec_decel_data_f(SEC_DECEL_INT_WIDTH*(p+1)-1 downto SEC_DECEL_INT_WIDTH*p),
        sec_decel_strb                               => sec_decel_strb_f(p),
        sec_decel_last                               => sec_decel_last_f(3*(p+1)-1 downto 3*p),
     
        sec_accel_valid                              => sec_accel_valid_f(p),
        sec_accel_ready                              => sec_accel_ready_f(p),
        sec_accel_data                               => sec_accel_data_f(SEC_ACCEL_INT_WIDTH*(p+1)-1 downto SEC_ACCEL_INT_WIDTH*p),
        sec_accel_strb                               => sec_accel_strb_f(p),
        sec_accel_last                               => sec_accel_last_f(3*(p+1)-1 downto 3*p),
     
        braking_valid                                => braking_valid_f(p),
        braking_ready                                => braking_ready_f(p),
        braking_data                                 => braking_data_f(BRAKING_INT_WIDTH*(p+1)-1 downto BRAKING_INT_WIDTH*p),
        braking_strb                                 => braking_strb_f(p),
        braking_last                                 => braking_last_f(3*(p+1)-1 downto 3*p),
     
        accel_valid                                  => accel_valid_f(p),
        accel_ready                                  => accel_ready_f(p),
        accel_data                                   => accel_data_f(ACCEL_INT_WIDTH*(p+1)-1 downto ACCEL_INT_WIDTH*p),
        accel_strb                                   => accel_strb_f(p),
        accel_last                                   => accel_last_f(3*(p+1)-1 downto 3*p),
     
        small_speed_var_valid                        => small_speed_var_valid_f(p),
        small_speed_var_ready                        => small_speed_var_ready_f(p),
        small_speed_var_data                         => small_speed_var_data_f(SMALL_SPEED_VAR_INT_WIDTH*(p+1)-1 downto SMALL_SPEED_VAR_INT_WIDTH*p),
        small_speed_var_strb                         => small_speed_var_strb_f(p),
        small_speed_var_last                         => small_speed_var_last_f(3*(p+1)-1 downto 3*p),
     
        large_speed_var_valid                        => large_speed_var_valid_f(p),
        large_speed_var_ready                        => large_speed_var_ready_f(p),
        large_speed_var_data                         => large_speed_var_data_f(LARGE_SPEED_VAR_INT_WIDTH*(p+1)-1 downto LARGE_SPEED_VAR_INT_WIDTH*p),
        large_speed_var_strb                         => large_speed_var_strb_f(p),
        large_speed_var_last                         => large_speed_var_last_f(3*(p+1)-1 downto 3*p),
     
        timestamp_valid                              => timestamp_ser_valid(p),
        timestamp_ready                              => timestamp_ser_ready(p),
        timestamp_data                               => timestamp_ser_data(8*EPC*(p+1)-1 downto 8*EPC*p),
        timestamp_strb                               => timestamp_ser_strb(EPC*(p+1)-1 downto EPC*p),
        timestamp_last                               => timestamp_ser_last(3*EPC*(p+1)-1 downto 3*EPC*p)

      );
  end generate;

  timezone_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    timezone_fifo: PacketFIFO
      generic map (
        DEPTH                     => TIMEZONE_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => TIMEZONE_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => timezone_valid_f(p),
        in_ready                  => timezone_ready_f(p),
        in_data                   => timezone_data_f(TIMEZONE_INT_WIDTH*(p+1)-1 downto TIMEZONE_INT_WIDTH*p),
        in_last                   => timezone_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => timezone_strb_f(p),
      
        out_valid                 => timezone_valid_a(p),
        out_ready                 => timezone_ready_a(p),
        out_data                  => timezone_data_a(TIMEZONE_INT_WIDTH*(p+1)-1 downto TIMEZONE_INT_WIDTH*p),
        out_last                  => timezone_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => timezone_strb_a(p),
      
        packet_valid              => pkt_valid(p)(0),
        packet_ready              => pkt_ready(p)(0)
      );
  end generate;

  vin_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    vin_fifo: PacketFIFO
      generic map (
        DEPTH                     => VIN_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => VIN_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => vin_valid_f(p),
        in_ready                  => vin_ready_f(p),
        in_data                   => vin_data_f(VIN_INT_WIDTH*(p+1)-1 downto VIN_INT_WIDTH*p),
        in_last                   => vin_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => vin_strb_f(p),
      
        out_valid                 => vin_valid_a(p),
        out_ready                 => vin_ready_a(p),
        out_data                  => vin_data_a(VIN_INT_WIDTH*(p+1)-1 downto VIN_INT_WIDTH*p),
        out_last                  => vin_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => vin_strb_a(p),
      
        packet_valid              => pkt_valid(p)(1),
        packet_ready              => pkt_ready(p)(1)
      );
  end generate;

  odometer_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    odometer_fifo: PacketFIFO
      generic map (
        DEPTH                     => ODOMETER_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => ODOMETER_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => odometer_valid_f(p),
        in_ready                  => odometer_ready_f(p),
        in_data                   => odometer_data_f(ODOMETER_INT_WIDTH*(p+1)-1 downto ODOMETER_INT_WIDTH*p),
        in_last                   => odometer_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => odometer_strb_f(p),
      
        out_valid                 => odometer_valid_a(p),
        out_ready                 => odometer_ready_a(p),
        out_data                  => odometer_data_a(ODOMETER_INT_WIDTH*(p+1)-1 downto ODOMETER_INT_WIDTH*p),
        out_last                  => odometer_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => odometer_strb_a(p),
      
        packet_valid              => pkt_valid(p)(2),
        packet_ready              => pkt_ready(p)(2)
      );
  end generate;

  avgspeed_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    avgspeed_fifo: PacketFIFO
      generic map (
        DEPTH                     => AVGSPEED_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => AVGSPEED_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => avgspeed_valid_f(p),
        in_ready                  => avgspeed_ready_f(p),
        in_data                   => avgspeed_data_f(AVGSPEED_INT_WIDTH*(p+1)-1 downto AVGSPEED_INT_WIDTH*p),
        in_last                   => avgspeed_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => avgspeed_strb_f(p),
      
        out_valid                 => avgspeed_valid_a(p),
        out_ready                 => avgspeed_ready_a(p),
        out_data                  => avgspeed_data_a(AVGSPEED_INT_WIDTH*(p+1)-1 downto AVGSPEED_INT_WIDTH*p),
        out_last                  => avgspeed_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => avgspeed_strb_a(p),
      
        packet_valid              => pkt_valid(p)(3),
        packet_ready              => pkt_ready(p)(3)
      );
  end generate;

  accel_decel_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    accel_decel_fifo: PacketFIFO
      generic map (
        DEPTH                     => ACCEL_DECEL_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => ACCEL_DECEL_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => accel_decel_valid_f(p),
        in_ready                  => accel_decel_ready_f(p),
        in_data                   => accel_decel_data_f(ACCEL_DECEL_INT_WIDTH*(p+1)-1 downto ACCEL_DECEL_INT_WIDTH*p),
        in_last                   => accel_decel_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => accel_decel_strb_f(p),
      
        out_valid                 => accel_decel_valid_a(p),
        out_ready                 => accel_decel_ready_a(p),
        out_data                  => accel_decel_data_a(ACCEL_DECEL_INT_WIDTH*(p+1)-1 downto ACCEL_DECEL_INT_WIDTH*p),
        out_last                  => accel_decel_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => accel_decel_strb_a(p),
      
        packet_valid              => pkt_valid(p)(4),
        packet_ready              => pkt_ready(p)(4)
      );
  end generate;

  speed_changes_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    speed_changes_fifo: PacketFIFO
      generic map (
        DEPTH                     => SPEED_CHANGES_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => SPEED_CHANGES_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => speed_changes_valid_f(p),
        in_ready                  => speed_changes_ready_f(p),
        in_data                   => speed_changes_data_f(SPEED_CHANGES_INT_WIDTH*(p+1)-1 downto SPEED_CHANGES_INT_WIDTH*p),
        in_last                   => speed_changes_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => speed_changes_strb_f(p),
      
        out_valid                 => speed_changes_valid_a(p),
        out_ready                 => speed_changes_ready_a(p),
        out_data                  => speed_changes_data_a(SPEED_CHANGES_INT_WIDTH*(p+1)-1 downto SPEED_CHANGES_INT_WIDTH*p),
        out_last                  => speed_changes_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => speed_changes_strb_a(p),
      
        packet_valid              => pkt_valid(p)(5),
        packet_ready              => pkt_ready(p)(5)
      );
  end generate;

  hypermiling_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    hypermiling_fifo: PacketFIFO
      generic map (
        DEPTH                     => HYPERMILING_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => 1,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => hypermiling_valid_f(p),
        in_ready                  => hypermiling_ready_f(p),
        in_data                   => hypermiling_data_f(1*(p+1)-1 downto 1*p),
        in_last                   => hypermiling_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => hypermiling_strb_f(p),
      
        out_valid                 => hypermiling_valid_a(p),
        out_ready                 => hypermiling_ready_a(p),
        out_data                  => hypermiling_data_a(1*(p+1)-1 downto 1*p),
        out_last                  => hypermiling_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => hypermiling_strb_a(p),
      
        packet_valid              => pkt_valid(p)(6),
        packet_ready              => pkt_ready(p)(6)
      );
  end generate;

  orientation_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    orientation_fifo: PacketFIFO
      generic map (
        DEPTH                     => ORIENTATION_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => 1,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => orientation_valid_f(p),
        in_ready                  => orientation_ready_f(p),
        in_data                   => orientation_data_f(1*(p+1)-1 downto 1*p),
        in_last                   => orientation_last_f(2*(p+1)-1 downto 2*p),
        in_strb                   => orientation_strb_f(p),
      
        out_valid                 => orientation_valid_a(p),
        out_ready                 => orientation_ready_a(p),
        out_data                  => orientation_data_a(1*(p+1)-1 downto 1*p),
        out_last                  => orientation_last_a(2*(p+1)-1 downto 2*p),
        out_strb                  => orientation_strb_a(p),
      
        packet_valid              => pkt_valid(p)(7),
        packet_ready              => pkt_ready(p)(7)
      );
  end generate;

  sec_in_band_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    sec_in_band_fifo: PacketFIFO
      generic map (
        DEPTH                     => SEC_IN_BAND_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => SEC_IN_BAND_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => sec_in_band_valid_f(p),
        in_ready                  => sec_in_band_ready_f(p),
        in_data                   => sec_in_band_data_f(SEC_IN_BAND_INT_WIDTH*(p+1)-1 downto SEC_IN_BAND_INT_WIDTH*p),
        in_last                   => sec_in_band_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => sec_in_band_strb_f(p),
      
        out_valid                 => sec_in_band_valid_a(p),
        out_ready                 => sec_in_band_ready_a(p),
        out_data                  => sec_in_band_data_a(SEC_IN_BAND_INT_WIDTH*(p+1)-1 downto SEC_IN_BAND_INT_WIDTH*p),
        out_last                  => sec_in_band_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => sec_in_band_strb_a(p),
      
        packet_valid              => pkt_valid(p)(8),
        packet_ready              => pkt_ready(p)(8)
      );
  end generate;

  miles_in_time_range_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    miles_in_time_range_fifo: PacketFIFO
      generic map (
        DEPTH                     => MILES_IN_TIME_RANGE_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => MILES_IN_TIME_RANGE_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => miles_in_time_range_valid_f(p),
        in_ready                  => miles_in_time_range_ready_f(p),
        in_data                   => miles_in_time_range_data_f(MILES_IN_TIME_RANGE_INT_WIDTH*(p+1)-1 downto MILES_IN_TIME_RANGE_INT_WIDTH*p),
        in_last                   => miles_in_time_range_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => miles_in_time_range_strb_f(p),
      
        out_valid                 => miles_in_time_range_valid_a(p),
        out_ready                 => miles_in_time_range_ready_a(p),
        out_data                  => miles_in_time_range_data_a(MILES_IN_TIME_RANGE_INT_WIDTH*(p+1)-1 downto MILES_IN_TIME_RANGE_INT_WIDTH*p),
        out_last                  => miles_in_time_range_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => miles_in_time_range_strb_a(p),
      
        packet_valid              => pkt_valid(p)(9),
        packet_ready              => pkt_ready(p)(9)
      );
  end generate;

  const_speed_miles_in_band_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    const_speed_miles_in_band_fifo: PacketFIFO
      generic map (
        DEPTH                     => CONST_SPEED_MILES_IN_BAND_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => CONST_SPEED_MILES_IN_BAND_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => const_speed_miles_in_band_valid_f(p),
        in_ready                  => const_speed_miles_in_band_ready_f(p),
        in_data                   => const_speed_miles_in_band_data_f(CONST_SPEED_MILES_IN_BAND_INT_WIDTH*(p+1)-1 downto CONST_SPEED_MILES_IN_BAND_INT_WIDTH*p),
        in_last                   => const_speed_miles_in_band_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => const_speed_miles_in_band_strb_f(p),
      
        out_valid                 => const_speed_miles_in_band_valid_a(p),
        out_ready                 => const_speed_miles_in_band_ready_a(p),
        out_data                  => const_speed_miles_in_band_data_a(CONST_SPEED_MILES_IN_BAND_INT_WIDTH*(p+1)-1 downto CONST_SPEED_MILES_IN_BAND_INT_WIDTH*p),
        out_last                  => const_speed_miles_in_band_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => const_speed_miles_in_band_strb_a(p),
      
        packet_valid              => pkt_valid(p)(10),
        packet_ready              => pkt_ready(p)(10)
      );
  end generate;

  vary_speed_miles_in_band_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    vary_speed_miles_in_band_fifo: PacketFIFO
      generic map (
        DEPTH                     => VARY_SPEED_MILES_IN_BAND_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => VARY_SPEED_MILES_IN_BAND_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => vary_speed_miles_in_band_valid_f(p),
        in_ready                  => vary_speed_miles_in_band_ready_f(p),
        in_data                   => vary_speed_miles_in_band_data_f(VARY_SPEED_MILES_IN_BAND_INT_WIDTH*(p+1)-1 downto VARY_SPEED_MILES_IN_BAND_INT_WIDTH*p),
        in_last                   => vary_speed_miles_in_band_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => vary_speed_miles_in_band_strb_f(p),
      
        out_valid                 => vary_speed_miles_in_band_valid_a(p),
        out_ready                 => vary_speed_miles_in_band_ready_a(p),
        out_data                  => vary_speed_miles_in_band_data_a(VARY_SPEED_MILES_IN_BAND_INT_WIDTH*(p+1)-1 downto VARY_SPEED_MILES_IN_BAND_INT_WIDTH*p),
        out_last                  => vary_speed_miles_in_band_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => vary_speed_miles_in_band_strb_a(p),
      
        packet_valid              => pkt_valid(p)(11),
        packet_ready              => pkt_ready(p)(11)
      );
  end generate;

  sec_decel_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    sec_decel_fifo: PacketFIFO
      generic map (
        DEPTH                     => SEC_DECEL_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => SEC_DECEL_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => sec_decel_valid_f(p),
        in_ready                  => sec_decel_ready_f(p),
        in_data                   => sec_decel_data_f(SEC_DECEL_INT_WIDTH*(p+1)-1 downto SEC_DECEL_INT_WIDTH*p),
        in_last                   => sec_decel_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => sec_decel_strb_f(p),
      
        out_valid                 => sec_decel_valid_a(p),
        out_ready                 => sec_decel_ready_a(p),
        out_data                  => sec_decel_data_a(SEC_DECEL_INT_WIDTH*(p+1)-1 downto SEC_DECEL_INT_WIDTH*p),
        out_last                  => sec_decel_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => sec_decel_strb_a(p),
      
        packet_valid              => pkt_valid(p)(12),
        packet_ready              => pkt_ready(p)(12)
      );
  end generate;

  sec_accel_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    sec_accel_fifo: PacketFIFO
      generic map (
        DEPTH                     => SEC_ACCEL_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => SEC_ACCEL_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => sec_accel_valid_f(p),
        in_ready                  => sec_accel_ready_f(p),
        in_data                   => sec_accel_data_f(SEC_ACCEL_INT_WIDTH*(p+1)-1 downto SEC_ACCEL_INT_WIDTH*p),
        in_last                   => sec_accel_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => sec_accel_strb_f(p),
      
        out_valid                 => sec_accel_valid_a(p),
        out_ready                 => sec_accel_ready_a(p),
        out_data                  => sec_accel_data_a(SEC_ACCEL_INT_WIDTH*(p+1)-1 downto SEC_ACCEL_INT_WIDTH*p),
        out_last                  => sec_accel_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => sec_accel_strb_a(p),
      
        packet_valid              => pkt_valid(p)(13),
        packet_ready              => pkt_ready(p)(13)
      );
  end generate;

  braking_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    braking_fifo: PacketFIFO
      generic map (
        DEPTH                     => BRAKING_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => BRAKING_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => braking_valid_f(p),
        in_ready                  => braking_ready_f(p),
        in_data                   => braking_data_f(BRAKING_INT_WIDTH*(p+1)-1 downto BRAKING_INT_WIDTH*p),
        in_last                   => braking_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => braking_strb_f(p),
      
        out_valid                 => braking_valid_a(p),
        out_ready                 => braking_ready_a(p),
        out_data                  => braking_data_a(BRAKING_INT_WIDTH*(p+1)-1 downto BRAKING_INT_WIDTH*p),
        out_last                  => braking_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => braking_strb_a(p),
      
        packet_valid              => pkt_valid(p)(14),
        packet_ready              => pkt_ready(p)(14)
      );
  end generate;

  accel_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    accel_fifo: PacketFIFO
      generic map (
        DEPTH                     => ACCEL_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => ACCEL_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => accel_valid_f(p),
        in_ready                  => accel_ready_f(p),
        in_data                   => accel_data_f(ACCEL_INT_WIDTH*(p+1)-1 downto ACCEL_INT_WIDTH*p),
        in_last                   => accel_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => accel_strb_f(p),
      
        out_valid                 => accel_valid_a(p),
        out_ready                 => accel_ready_a(p),
        out_data                  => accel_data_a(ACCEL_INT_WIDTH*(p+1)-1 downto ACCEL_INT_WIDTH*p),
        out_last                  => accel_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => accel_strb_a(p),
      
        packet_valid              => pkt_valid(p)(15),
        packet_ready              => pkt_ready(p)(15)
      );
  end generate;

  small_speed_var_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    small_speed_var_fifo: PacketFIFO
      generic map (
        DEPTH                     => SMALL_SPEED_VAR_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => SMALL_SPEED_VAR_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => small_speed_var_valid_f(p),
        in_ready                  => small_speed_var_ready_f(p),
        in_data                   => small_speed_var_data_f(SMALL_SPEED_VAR_INT_WIDTH*(p+1)-1 downto SMALL_SPEED_VAR_INT_WIDTH*p),
        in_last                   => small_speed_var_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => small_speed_var_strb_f(p),
      
        out_valid                 => small_speed_var_valid_a(p),
        out_ready                 => small_speed_var_ready_a(p),
        out_data                  => small_speed_var_data_a(SMALL_SPEED_VAR_INT_WIDTH*(p+1)-1 downto SMALL_SPEED_VAR_INT_WIDTH*p),
        out_last                  => small_speed_var_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => small_speed_var_strb_a(p),
      
        packet_valid              => pkt_valid(p)(16),
        packet_ready              => pkt_ready(p)(16)
      );
  end generate;

  large_speed_var_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    large_speed_var_fifo: PacketFIFO
      generic map (
        DEPTH                     => LARGE_SPEED_VAR_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => LARGE_SPEED_VAR_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => large_speed_var_valid_f(p),
        in_ready                  => large_speed_var_ready_f(p),
        in_data                   => large_speed_var_data_f(LARGE_SPEED_VAR_INT_WIDTH*(p+1)-1 downto LARGE_SPEED_VAR_INT_WIDTH*p),
        in_last                   => large_speed_var_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => large_speed_var_strb_f(p),
      
        out_valid                 => large_speed_var_valid_a(p),
        out_ready                 => large_speed_var_ready_a(p),
        out_data                  => large_speed_var_data_a(LARGE_SPEED_VAR_INT_WIDTH*(p+1)-1 downto LARGE_SPEED_VAR_INT_WIDTH*p),
        out_last                  => large_speed_var_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => large_speed_var_strb_a(p),
      
        packet_valid              => pkt_valid(p)(17),
        packet_ready              => pkt_ready(p)(17)
      );
  end generate;

  timestamp_gen_fifos : for p in 0 to NUM_PARSERS-1 generate
    timestamp_fifo: PacketFIFO
      generic map (
        DEPTH                     => TIMESTAMP_FIFO_DEPTH,
        PKT_COUNT_WIDTH           => PKT_COUNT_WIDTH,
        DATA_WIDTH                => 8,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
      
        in_valid                  => timestamp_valid_f(p),
        in_ready                  => timestamp_ready_f(p),
        in_data                   => timestamp_data_f(8*(p+1)-1 downto 8*p),
        in_last                   => timestamp_last_f(3*(p+1)-1 downto 3*p),
        in_strb                   => timestamp_strb_f(p),
      
        out_valid                 => timestamp_valid_a(p),
        out_ready                 => timestamp_ready_a(p),
        out_data                  => timestamp_data_a(8*(p+1)-1 downto 8*p),
        out_last                  => timestamp_last_a(3*(p+1)-1 downto 3*p),
        out_strb                  => timestamp_strb_a(p),
      
        packet_valid              => pkt_valid(p)(18),
        packet_ready              => pkt_ready(p)(18)
      );
  end generate;


    timezone_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => TIMEZONE_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => timezone_valid_a,
        in_ready                  => timezone_ready_a,
        in_data                   => timezone_data_a,
        in_strb                   => timezone_strb_a,
        in_last                   => timezone_last_a,
    
        out_valid                 => timezone_valid,
        out_ready                 => timezone_ready,
        out_data                  => timezone_data,
        out_strb                  => timezone_strb,
        out_last                  => timezone_last,
    
        cmd_valid                 => cmd_valid(0),
        cmd_ready                 => cmd_ready(0),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(0),
        last_packet_ready         => last_pkt_ready(0)

      );

    vin_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => VIN_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => vin_valid_a,
        in_ready                  => vin_ready_a,
        in_data                   => vin_data_a,
        in_strb                   => vin_strb_a,
        in_last                   => vin_last_a,
    
        out_valid                 => vin_valid,
        out_ready                 => vin_ready,
        out_data                  => vin_data,
        out_strb                  => vin_strb,
        out_last                  => vin_last,
    
        cmd_valid                 => cmd_valid(1),
        cmd_ready                 => cmd_ready(1),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(1),
        last_packet_ready         => last_pkt_ready(1)

      );

    odometer_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => ODOMETER_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => odometer_valid_a,
        in_ready                  => odometer_ready_a,
        in_data                   => odometer_data_a,
        in_strb                   => odometer_strb_a,
        in_last                   => odometer_last_a,
    
        out_valid                 => odometer_valid,
        out_ready                 => odometer_ready,
        out_data                  => odometer_data,
        out_strb                  => odometer_strb,
        out_last                  => odometer_last,
    
        cmd_valid                 => cmd_valid(2),
        cmd_ready                 => cmd_ready(2),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(2),
        last_packet_ready         => last_pkt_ready(2)

      );

    avgspeed_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => AVGSPEED_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => avgspeed_valid_a,
        in_ready                  => avgspeed_ready_a,
        in_data                   => avgspeed_data_a,
        in_strb                   => avgspeed_strb_a,
        in_last                   => avgspeed_last_a,
    
        out_valid                 => avgspeed_valid,
        out_ready                 => avgspeed_ready,
        out_data                  => avgspeed_data,
        out_strb                  => avgspeed_strb,
        out_last                  => avgspeed_last,
    
        cmd_valid                 => cmd_valid(3),
        cmd_ready                 => cmd_ready(3),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(3),
        last_packet_ready         => last_pkt_ready(3)

      );

    accel_decel_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => ACCEL_DECEL_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => accel_decel_valid_a,
        in_ready                  => accel_decel_ready_a,
        in_data                   => accel_decel_data_a,
        in_strb                   => accel_decel_strb_a,
        in_last                   => accel_decel_last_a,
    
        out_valid                 => accel_decel_valid,
        out_ready                 => accel_decel_ready,
        out_data                  => accel_decel_data,
        out_strb                  => accel_decel_strb,
        out_last                  => accel_decel_last,
    
        cmd_valid                 => cmd_valid(4),
        cmd_ready                 => cmd_ready(4),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(4),
        last_packet_ready         => last_pkt_ready(4)

      );

    speed_changes_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => SPEED_CHANGES_INT_WIDTH,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => speed_changes_valid_a,
        in_ready                  => speed_changes_ready_a,
        in_data                   => speed_changes_data_a,
        in_strb                   => speed_changes_strb_a,
        in_last                   => speed_changes_last_a,
    
        out_valid                 => speed_changes_valid,
        out_ready                 => speed_changes_ready,
        out_data                  => speed_changes_data,
        out_strb                  => speed_changes_strb,
        out_last                  => speed_changes_last,
    
        cmd_valid                 => cmd_valid(5),
        cmd_ready                 => cmd_ready(5),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(5),
        last_packet_ready         => last_pkt_ready(5)

      );

    hypermiling_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => 1,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => hypermiling_valid_a,
        in_ready                  => hypermiling_ready_a,
        in_data                   => hypermiling_data_a,
        in_strb                   => hypermiling_strb_a,
        in_last                   => hypermiling_last_a,
    
        out_valid                 => hypermiling_valid,
        out_ready                 => hypermiling_ready,
        out_data                  => hypermiling_data,
        out_strb                  => hypermiling_strb,
        out_last                  => hypermiling_last,
    
        cmd_valid                 => cmd_valid(6),
        cmd_ready                 => cmd_ready(6),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(6),
        last_packet_ready         => last_pkt_ready(6)

      );

    orientation_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => 1,
        DIMENSIONALITY            => 2
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => orientation_valid_a,
        in_ready                  => orientation_ready_a,
        in_data                   => orientation_data_a,
        in_strb                   => orientation_strb_a,
        in_last                   => orientation_last_a,
    
        out_valid                 => orientation_valid,
        out_ready                 => orientation_ready,
        out_data                  => orientation_data,
        out_strb                  => orientation_strb,
        out_last                  => orientation_last,
    
        cmd_valid                 => cmd_valid(7),
        cmd_ready                 => cmd_ready(7),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(7),
        last_packet_ready         => last_pkt_ready(7)

      );

    sec_in_band_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => SEC_IN_BAND_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => sec_in_band_valid_a,
        in_ready                  => sec_in_band_ready_a,
        in_data                   => sec_in_band_data_a,
        in_strb                   => sec_in_band_strb_a,
        in_last                   => sec_in_band_last_a,
    
        out_valid                 => sec_in_band_valid,
        out_ready                 => sec_in_band_ready,
        out_data                  => sec_in_band_data,
        out_strb                  => sec_in_band_strb,
        out_last                  => sec_in_band_last,
    
        cmd_valid                 => cmd_valid(8),
        cmd_ready                 => cmd_ready(8),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(8),
        last_packet_ready         => last_pkt_ready(8)

      );

    miles_in_time_range_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => MILES_IN_TIME_RANGE_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => miles_in_time_range_valid_a,
        in_ready                  => miles_in_time_range_ready_a,
        in_data                   => miles_in_time_range_data_a,
        in_strb                   => miles_in_time_range_strb_a,
        in_last                   => miles_in_time_range_last_a,
    
        out_valid                 => miles_in_time_range_valid,
        out_ready                 => miles_in_time_range_ready,
        out_data                  => miles_in_time_range_data,
        out_strb                  => miles_in_time_range_strb,
        out_last                  => miles_in_time_range_last,
    
        cmd_valid                 => cmd_valid(9),
        cmd_ready                 => cmd_ready(9),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(9),
        last_packet_ready         => last_pkt_ready(9)

      );

    const_speed_miles_in_band_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => CONST_SPEED_MILES_IN_BAND_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => const_speed_miles_in_band_valid_a,
        in_ready                  => const_speed_miles_in_band_ready_a,
        in_data                   => const_speed_miles_in_band_data_a,
        in_strb                   => const_speed_miles_in_band_strb_a,
        in_last                   => const_speed_miles_in_band_last_a,
    
        out_valid                 => const_speed_miles_in_band_valid,
        out_ready                 => const_speed_miles_in_band_ready,
        out_data                  => const_speed_miles_in_band_data,
        out_strb                  => const_speed_miles_in_band_strb,
        out_last                  => const_speed_miles_in_band_last,
    
        cmd_valid                 => cmd_valid(10),
        cmd_ready                 => cmd_ready(10),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(10),
        last_packet_ready         => last_pkt_ready(10)

      );

    vary_speed_miles_in_band_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => VARY_SPEED_MILES_IN_BAND_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => vary_speed_miles_in_band_valid_a,
        in_ready                  => vary_speed_miles_in_band_ready_a,
        in_data                   => vary_speed_miles_in_band_data_a,
        in_strb                   => vary_speed_miles_in_band_strb_a,
        in_last                   => vary_speed_miles_in_band_last_a,
    
        out_valid                 => vary_speed_miles_in_band_valid,
        out_ready                 => vary_speed_miles_in_band_ready,
        out_data                  => vary_speed_miles_in_band_data,
        out_strb                  => vary_speed_miles_in_band_strb,
        out_last                  => vary_speed_miles_in_band_last,
    
        cmd_valid                 => cmd_valid(11),
        cmd_ready                 => cmd_ready(11),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(11),
        last_packet_ready         => last_pkt_ready(11)

      );

    sec_decel_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => SEC_DECEL_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => sec_decel_valid_a,
        in_ready                  => sec_decel_ready_a,
        in_data                   => sec_decel_data_a,
        in_strb                   => sec_decel_strb_a,
        in_last                   => sec_decel_last_a,
    
        out_valid                 => sec_decel_valid,
        out_ready                 => sec_decel_ready,
        out_data                  => sec_decel_data,
        out_strb                  => sec_decel_strb,
        out_last                  => sec_decel_last,
    
        cmd_valid                 => cmd_valid(12),
        cmd_ready                 => cmd_ready(12),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(12),
        last_packet_ready         => last_pkt_ready(12)

      );

    sec_accel_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => SEC_ACCEL_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => sec_accel_valid_a,
        in_ready                  => sec_accel_ready_a,
        in_data                   => sec_accel_data_a,
        in_strb                   => sec_accel_strb_a,
        in_last                   => sec_accel_last_a,
    
        out_valid                 => sec_accel_valid,
        out_ready                 => sec_accel_ready,
        out_data                  => sec_accel_data,
        out_strb                  => sec_accel_strb,
        out_last                  => sec_accel_last,
    
        cmd_valid                 => cmd_valid(13),
        cmd_ready                 => cmd_ready(13),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(13),
        last_packet_ready         => last_pkt_ready(13)

      );

    braking_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => BRAKING_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => braking_valid_a,
        in_ready                  => braking_ready_a,
        in_data                   => braking_data_a,
        in_strb                   => braking_strb_a,
        in_last                   => braking_last_a,
    
        out_valid                 => braking_valid,
        out_ready                 => braking_ready,
        out_data                  => braking_data,
        out_strb                  => braking_strb,
        out_last                  => braking_last,
    
        cmd_valid                 => cmd_valid(14),
        cmd_ready                 => cmd_ready(14),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(14),
        last_packet_ready         => last_pkt_ready(14)

      );

    accel_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => ACCEL_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => accel_valid_a,
        in_ready                  => accel_ready_a,
        in_data                   => accel_data_a,
        in_strb                   => accel_strb_a,
        in_last                   => accel_last_a,
    
        out_valid                 => accel_valid,
        out_ready                 => accel_ready,
        out_data                  => accel_data,
        out_strb                  => accel_strb,
        out_last                  => accel_last,
    
        cmd_valid                 => cmd_valid(15),
        cmd_ready                 => cmd_ready(15),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(15),
        last_packet_ready         => last_pkt_ready(15)

      );

    small_speed_var_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => SMALL_SPEED_VAR_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => small_speed_var_valid_a,
        in_ready                  => small_speed_var_ready_a,
        in_data                   => small_speed_var_data_a,
        in_strb                   => small_speed_var_strb_a,
        in_last                   => small_speed_var_last_a,
    
        out_valid                 => small_speed_var_valid,
        out_ready                 => small_speed_var_ready,
        out_data                  => small_speed_var_data,
        out_strb                  => small_speed_var_strb,
        out_last                  => small_speed_var_last,
    
        cmd_valid                 => cmd_valid(16),
        cmd_ready                 => cmd_ready(16),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(16),
        last_packet_ready         => last_pkt_ready(16)

      );

    large_speed_var_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => LARGE_SPEED_VAR_INT_WIDTH,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => large_speed_var_valid_a,
        in_ready                  => large_speed_var_ready_a,
        in_data                   => large_speed_var_data_a,
        in_strb                   => large_speed_var_strb_a,
        in_last                   => large_speed_var_last_a,
    
        out_valid                 => large_speed_var_valid,
        out_ready                 => large_speed_var_ready,
        out_data                  => large_speed_var_data,
        out_strb                  => large_speed_var_strb,
        out_last                  => large_speed_var_last,
    
        cmd_valid                 => cmd_valid(17),
        cmd_ready                 => cmd_ready(17),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(17),
        last_packet_ready         => last_pkt_ready(17)

      );

    timestamp_arb: PacketArbiter
      generic map (
        NUM_INPUTS                => NUM_PARSERS,
        INDEX_WIDTH               => INDEX_WIDTH,
        DATA_WIDTH                => 8,
        DIMENSIONALITY            => 3
      )
      port map (
        clk                       => clk,
        reset                     => reset,
    
        in_valid                  => timestamp_valid_a,
        in_ready                  => timestamp_ready_a,
        in_data                   => timestamp_data_a,
        in_strb                   => timestamp_strb_a,
        in_last                   => timestamp_last_a,
    
        out_valid                 => timestamp_valid,
        out_ready                 => timestamp_ready,
        out_data                  => timestamp_data,
        out_strb                  => timestamp_strb,
        out_last                  => timestamp_last,
    
        cmd_valid                 => cmd_valid(18),
        cmd_ready                 => cmd_ready(18),
        cmd_index                 => cmd_index,
        
        last_packet_valid         => last_pkt_valid(18),
        last_packet_ready         => last_pkt_ready(18)

      );

  gen_timestamp_ser : for p in 0 to NUM_PARSERS-1 generate
    timestamp_serializer : StreamSerializer
      generic map(
        EPC             => EPC,
        DATA_WIDTH      => 8,
        DIMENSIONALITY  => 3 
      ) 
      port map( 
        clk             => clk,
        reset           => reset,
    
        in_valid        => timestamp_ser_valid(p),
        in_ready        => timestamp_ser_ready(p),
        in_data         => timestamp_ser_data(8*EPC*(p+1)-1 downto 8*EPC*p),
        in_strb         => timestamp_ser_strb(EPC*(p+1)-1 downto EPC*p), 
        in_last         => timestamp_ser_last(3*EPC*(p+1)-1 downto 3*EPC*p), 
    
        out_valid       => timestamp_valid_f(p),
        out_ready       => timestamp_ready_f(p),
        out_data        => timestamp_data_f(8*(p+1)-1 downto 8*p),
        out_strb        => timestamp_strb_f(p), 
        out_last        => timestamp_last_f(3*(p+1)-1 downto 3*p) 
      );
  end generate;
  
  cmd_sync: StreamSync
    generic map (
      NUM_INPUTS              => 1,
      NUM_OUTPUTS             => 19
    )
    port map (
      clk                     => clk,
      reset                   => reset,
      in_valid(0)             => cmd_valid_sync,
      in_ready(0)             => cmd_ready_sync,
      out_valid               => cmd_valid,
      out_ready               => cmd_ready
    );
      
  last_pkt_sync: StreamSync
  generic map (
    NUM_INPUTS              => 19,
    NUM_OUTPUTS             => 1
  )
  port map (
    clk                     => clk,
    reset                   => reset,
    in_valid                => last_pkt_valid,
    in_ready                => last_pkt_ready,
    out_valid(0)            => last_pkt_valid_sync,
    out_ready(0)            => last_pkt_ready_sync
  );
      
  gen_pkt_sync : for p in 0 to NUM_PARSERS-1 generate
    pkt_sync: StreamSync
      generic map (
        NUM_INPUTS              => 19,
        NUM_OUTPUTS             => 1
      )
      port map (
        clk                     => clk,
        reset                   => reset,
        in_valid                => pkt_valid(p),
        in_ready                => pkt_ready(p),
        out_valid(0)            => pkt_valid_sync(p),
        out_ready(0)            => pkt_ready_sync(p)
      );
  end generate;
  
  arb_cntrl: ArbiterController
    generic map (
      NUM_INPUTS                => NUM_PARSERS,
      INDEX_WIDTH               => INDEX_WIDTH,
      TAG_WIDTH                 => TAG_WIDTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      pkt_valid                 => pkt_valid_sync,
      pkt_ready                 => pkt_ready_sync,

      cmd_valid                 => cmd_valid_sync,
      cmd_ready                 => cmd_ready_sync,
      cmd_index                 => cmd_index,

      last_pkt_valid            => last_pkt_valid_sync,
      last_pkt_ready            => last_pkt_ready_sync,

      tag_valid                 => tag_valid,
      tag_ready                 => tag_ready,
      tag                       => tag,
      tag_strb                  => tag_strb,
      tag_last                  => tag_last,

      tag_cfg                   => tag_cfg
    );

end architecture;