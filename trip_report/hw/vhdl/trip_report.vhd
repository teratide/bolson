library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use work.trip_report_pkg.all;

entity trip_report is
  generic (
    INDEX_WIDTH : integer := 32;
    TAG_WIDTH   : integer := 1
  );
  port (
    kcd_clk                                       : in std_logic;
    kcd_reset                                     : in std_logic;
    input_input_valid                             : in std_logic;
    input_input_ready                             : out std_logic;
    input_input_dvalid                            : in std_logic;
    input_input_last                              : in std_logic;
    input_input                                   : in std_logic_vector(63 downto 0);
    input_input_count                             : in std_logic_vector(3 downto 0);
    input_input_unl_valid                         : in std_logic;
    input_input_unl_ready                         : out std_logic;
    input_input_unl_tag                           : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    input_input_cmd_valid                         : out std_logic;
    input_input_cmd_ready                         : in std_logic;
    input_input_cmd_firstIdx                      : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    input_input_cmd_lastIdx                       : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    input_input_cmd_tag                           : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_timestamp_valid                        : out std_logic;
    output_timestamp_ready                        : in std_logic;
    output_timestamp_dvalid                       : out std_logic;
    output_timestamp_last                         : out std_logic;
    output_timestamp_length                       : out std_logic_vector(31 downto 0);
    output_timestamp_count                        : out std_logic_vector(0 downto 0);
    output_timestamp_chars_valid                  : out std_logic;
    output_timestamp_chars_ready                  : in std_logic;
    output_timestamp_chars_dvalid                 : out std_logic;
    output_timestamp_chars_last                   : out std_logic;
    output_timestamp_chars                        : out std_logic_vector(7 downto 0);
    output_timestamp_chars_count                  : out std_logic_vector(0 downto 0);
    output_timezone_valid                         : out std_logic;
    output_timezone_ready                         : in std_logic;
    output_timezone_dvalid                        : out std_logic;
    output_timezone_last                          : out std_logic;
    output_timezone                               : out std_logic_vector(63 downto 0);
    output_vin_valid                              : out std_logic;
    output_vin_ready                              : in std_logic;
    output_vin_dvalid                             : out std_logic;
    output_vin_last                               : out std_logic;
    output_vin                                    : out std_logic_vector(63 downto 0);
    output_odometer_valid                         : out std_logic;
    output_odometer_ready                         : in std_logic;
    output_odometer_dvalid                        : out std_logic;
    output_odometer_last                          : out std_logic;
    output_odometer                               : out std_logic_vector(63 downto 0);
    output_hypermiling_valid                      : out std_logic;
    output_hypermiling_ready                      : in std_logic;
    output_hypermiling_dvalid                     : out std_logic;
    output_hypermiling_last                       : out std_logic;
    output_hypermiling                            : out std_logic_vector(7 downto 0);
    output_avgspeed_valid                         : out std_logic;
    output_avgspeed_ready                         : in std_logic;
    output_avgspeed_dvalid                        : out std_logic;
    output_avgspeed_last                          : out std_logic;
    output_avgspeed                               : out std_logic_vector(63 downto 0);
    output_sec_in_band_valid                      : out std_logic;
    output_sec_in_band_ready                      : in std_logic;
    output_sec_in_band_dvalid                     : out std_logic;
    output_sec_in_band_last                       : out std_logic;
    output_sec_in_band                            : out std_logic_vector(63 downto 0);
    output_miles_in_time_range_valid              : out std_logic;
    output_miles_in_time_range_ready              : in std_logic;
    output_miles_in_time_range_dvalid             : out std_logic;
    output_miles_in_time_range_last               : out std_logic;
    output_miles_in_time_range                    : out std_logic_vector(63 downto 0);
    output_const_speed_miles_in_band_valid        : out std_logic;
    output_const_speed_miles_in_band_ready        : in std_logic;
    output_const_speed_miles_in_band_dvalid       : out std_logic;
    output_const_speed_miles_in_band_last         : out std_logic;
    output_const_speed_miles_in_band              : out std_logic_vector(63 downto 0);
    output_vary_speed_miles_in_band_valid         : out std_logic;
    output_vary_speed_miles_in_band_ready         : in std_logic;
    output_vary_speed_miles_in_band_dvalid        : out std_logic;
    output_vary_speed_miles_in_band_last          : out std_logic;
    output_vary_speed_miles_in_band               : out std_logic_vector(63 downto 0);
    output_sec_decel_valid                        : out std_logic;
    output_sec_decel_ready                        : in std_logic;
    output_sec_decel_dvalid                       : out std_logic;
    output_sec_decel_last                         : out std_logic;
    output_sec_decel                              : out std_logic_vector(63 downto 0);
    output_sec_accel_valid                        : out std_logic;
    output_sec_accel_ready                        : in std_logic;
    output_sec_accel_dvalid                       : out std_logic;
    output_sec_accel_last                         : out std_logic;
    output_sec_accel                              : out std_logic_vector(63 downto 0);
    output_braking_valid                          : out std_logic;
    output_braking_ready                          : in std_logic;
    output_braking_dvalid                         : out std_logic;
    output_braking_last                           : out std_logic;
    output_braking                                : out std_logic_vector(63 downto 0);
    output_accel_valid                            : out std_logic;
    output_accel_ready                            : in std_logic;
    output_accel_dvalid                           : out std_logic;
    output_accel_last                             : out std_logic;
    output_accel                                  : out std_logic_vector(63 downto 0);
    output_orientation_valid                      : out std_logic;
    output_orientation_ready                      : in std_logic;
    output_orientation_dvalid                     : out std_logic;
    output_orientation_last                       : out std_logic;
    output_orientation                            : out std_logic_vector(7 downto 0);
    output_small_speed_var_valid                  : out std_logic;
    output_small_speed_var_ready                  : in std_logic;
    output_small_speed_var_dvalid                 : out std_logic;
    output_small_speed_var_last                   : out std_logic;
    output_small_speed_var                        : out std_logic_vector(63 downto 0);
    output_large_speed_var_valid                  : out std_logic;
    output_large_speed_var_ready                  : in std_logic;
    output_large_speed_var_dvalid                 : out std_logic;
    output_large_speed_var_last                   : out std_logic;
    output_large_speed_var                        : out std_logic_vector(63 downto 0);
    output_accel_decel_valid                      : out std_logic;
    output_accel_decel_ready                      : in std_logic;
    output_accel_decel_dvalid                     : out std_logic;
    output_accel_decel_last                       : out std_logic;
    output_accel_decel                            : out std_logic_vector(63 downto 0);
    output_speed_changes_valid                    : out std_logic;
    output_speed_changes_ready                    : in std_logic;
    output_speed_changes_dvalid                   : out std_logic;
    output_speed_changes_last                     : out std_logic;
    output_speed_changes                          : out std_logic_vector(63 downto 0);
    output_timestamp_unl_valid                    : in std_logic;
    output_timestamp_unl_ready                    : out std_logic;
    output_timestamp_unl_tag                      : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_timezone_unl_valid                     : in std_logic;
    output_timezone_unl_ready                     : out std_logic;
    output_timezone_unl_tag                       : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_vin_unl_valid                          : in std_logic;
    output_vin_unl_ready                          : out std_logic;
    output_vin_unl_tag                            : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_odometer_unl_valid                     : in std_logic;
    output_odometer_unl_ready                     : out std_logic;
    output_odometer_unl_tag                       : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_hypermiling_unl_valid                  : in std_logic;
    output_hypermiling_unl_ready                  : out std_logic;
    output_hypermiling_unl_tag                    : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_avgspeed_unl_valid                     : in std_logic;
    output_avgspeed_unl_ready                     : out std_logic;
    output_avgspeed_unl_tag                       : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_sec_in_band_unl_valid                  : in std_logic;
    output_sec_in_band_unl_ready                  : out std_logic;
    output_sec_in_band_unl_tag                    : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_miles_in_time_range_unl_valid          : in std_logic;
    output_miles_in_time_range_unl_ready          : out std_logic;
    output_miles_in_time_range_unl_tag            : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_const_speed_miles_in_band_unl_valid    : in std_logic;
    output_const_speed_miles_in_band_unl_ready    : out std_logic;
    output_const_speed_miles_in_band_unl_tag      : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_vary_speed_miles_in_band_unl_valid     : in std_logic;
    output_vary_speed_miles_in_band_unl_ready     : out std_logic;
    output_vary_speed_miles_in_band_unl_tag       : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_sec_decel_unl_valid                    : in std_logic;
    output_sec_decel_unl_ready                    : out std_logic;
    output_sec_decel_unl_tag                      : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_sec_accel_unl_valid                    : in std_logic;
    output_sec_accel_unl_ready                    : out std_logic;
    output_sec_accel_unl_tag                      : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_braking_unl_valid                      : in std_logic;
    output_braking_unl_ready                      : out std_logic;
    output_braking_unl_tag                        : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_accel_unl_valid                        : in std_logic;
    output_accel_unl_ready                        : out std_logic;
    output_accel_unl_tag                          : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_orientation_unl_valid                  : in std_logic;
    output_orientation_unl_ready                  : out std_logic;
    output_orientation_unl_tag                    : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_small_speed_var_unl_valid              : in std_logic;
    output_small_speed_var_unl_ready              : out std_logic;
    output_small_speed_var_unl_tag                : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_large_speed_var_unl_valid              : in std_logic;
    output_large_speed_var_unl_ready              : out std_logic;
    output_large_speed_var_unl_tag                : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_accel_decel_unl_valid                  : in std_logic;
    output_accel_decel_unl_ready                  : out std_logic;
    output_accel_decel_unl_tag                    : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_speed_changes_unl_valid                : in std_logic;
    output_speed_changes_unl_ready                : out std_logic;
    output_speed_changes_unl_tag                  : in std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_timestamp_cmd_valid                    : out std_logic;
    output_timestamp_cmd_ready                    : in std_logic;
    output_timestamp_cmd_firstIdx                 : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_timestamp_cmd_lastIdx                  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_timestamp_cmd_tag                      : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_timezone_cmd_valid                     : out std_logic;
    output_timezone_cmd_ready                     : in std_logic;
    output_timezone_cmd_firstIdx                  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_timezone_cmd_lastIdx                   : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_timezone_cmd_tag                       : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_vin_cmd_valid                          : out std_logic;
    output_vin_cmd_ready                          : in std_logic;
    output_vin_cmd_firstIdx                       : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_vin_cmd_lastIdx                        : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_vin_cmd_tag                            : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_odometer_cmd_valid                     : out std_logic;
    output_odometer_cmd_ready                     : in std_logic;
    output_odometer_cmd_firstIdx                  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_odometer_cmd_lastIdx                   : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_odometer_cmd_tag                       : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_hypermiling_cmd_valid                  : out std_logic;
    output_hypermiling_cmd_ready                  : in std_logic;
    output_hypermiling_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_hypermiling_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_hypermiling_cmd_tag                    : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_avgspeed_cmd_valid                     : out std_logic;
    output_avgspeed_cmd_ready                     : in std_logic;
    output_avgspeed_cmd_firstIdx                  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_avgspeed_cmd_lastIdx                   : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_avgspeed_cmd_tag                       : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_sec_in_band_cmd_valid                  : out std_logic;
    output_sec_in_band_cmd_ready                  : in std_logic;
    output_sec_in_band_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_sec_in_band_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_sec_in_band_cmd_tag                    : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_miles_in_time_range_cmd_valid          : out std_logic;
    output_miles_in_time_range_cmd_ready          : in std_logic;
    output_miles_in_time_range_cmd_firstIdx       : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_miles_in_time_range_cmd_lastIdx        : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_miles_in_time_range_cmd_tag            : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_const_speed_miles_in_band_cmd_valid    : out std_logic;
    output_const_speed_miles_in_band_cmd_ready    : in std_logic;
    output_const_speed_miles_in_band_cmd_firstIdx : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_const_speed_miles_in_band_cmd_lastIdx  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_const_speed_miles_in_band_cmd_tag      : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_vary_speed_miles_in_band_cmd_valid     : out std_logic;
    output_vary_speed_miles_in_band_cmd_ready     : in std_logic;
    output_vary_speed_miles_in_band_cmd_firstIdx  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_vary_speed_miles_in_band_cmd_lastIdx   : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_vary_speed_miles_in_band_cmd_tag       : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_sec_decel_cmd_valid                    : out std_logic;
    output_sec_decel_cmd_ready                    : in std_logic;
    output_sec_decel_cmd_firstIdx                 : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_sec_decel_cmd_lastIdx                  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_sec_decel_cmd_tag                      : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_sec_accel_cmd_valid                    : out std_logic;
    output_sec_accel_cmd_ready                    : in std_logic;
    output_sec_accel_cmd_firstIdx                 : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_sec_accel_cmd_lastIdx                  : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_sec_accel_cmd_tag                      : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_braking_cmd_valid                      : out std_logic;
    output_braking_cmd_ready                      : in std_logic;
    output_braking_cmd_firstIdx                   : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_braking_cmd_lastIdx                    : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_braking_cmd_tag                        : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_accel_cmd_valid                        : out std_logic;
    output_accel_cmd_ready                        : in std_logic;
    output_accel_cmd_firstIdx                     : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_accel_cmd_lastIdx                      : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_accel_cmd_tag                          : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_orientation_cmd_valid                  : out std_logic;
    output_orientation_cmd_ready                  : in std_logic;
    output_orientation_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_orientation_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_orientation_cmd_tag                    : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_small_speed_var_cmd_valid              : out std_logic;
    output_small_speed_var_cmd_ready              : in std_logic;
    output_small_speed_var_cmd_firstIdx           : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_small_speed_var_cmd_lastIdx            : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_small_speed_var_cmd_tag                : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_large_speed_var_cmd_valid              : out std_logic;
    output_large_speed_var_cmd_ready              : in std_logic;
    output_large_speed_var_cmd_firstIdx           : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_large_speed_var_cmd_lastIdx            : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_large_speed_var_cmd_tag                : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_accel_decel_cmd_valid                  : out std_logic;
    output_accel_decel_cmd_ready                  : in std_logic;
    output_accel_decel_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_accel_decel_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_accel_decel_cmd_tag                    : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    output_speed_changes_cmd_valid                : out std_logic;
    output_speed_changes_cmd_ready                : in std_logic;
    output_speed_changes_cmd_firstIdx             : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_speed_changes_cmd_lastIdx              : out std_logic_vector(INDEX_WIDTH - 1 downto 0);
    output_speed_changes_cmd_tag                  : out std_logic_vector(TAG_WIDTH - 1 downto 0);
    start                                         : in std_logic;
    stop                                          : in std_logic;
    reset                                         : in std_logic;
    idle                                          : out std_logic;
    busy                                          : out std_logic;
    done                                          : out std_logic;
    result                                        : out std_logic_vector(63 downto 0);
    input_firstidx                                : in std_logic_vector(31 downto 0);
    input_lastidx                                 : in std_logic_vector(31 downto 0);
    output_firstidx                               : in std_logic_vector(31 downto 0);
    output_lastidx                                : in std_logic_vector(31 downto 0);
    ext_platform_complete_req                     : out std_logic;
    ext_platform_complete_ack                     : in std_logic
  );
end entity;

architecture Implementation of trip_report is
begin

  trip_report_parser : TripReportParser
  generic map(
    EPC                                    => 8,

    -- 
    -- INTEGER FIELDS
    --
    TIMEZONE_INT_WIDTH                     => 16,
    TIMEZONE_INT_P_PIPELINE_STAGES         => 1,
    TIMEZONE_BUFFER_D                      => 1,

    VIN_INT_WIDTH                          => 16,
    VIN_INT_P_PIPELINE_STAGES              => 1,
    VIN_BUFFER_D                           => 1,

    ODOMETER_INT_WIDTH                     => 16,
    ODOMETER_INT_P_PIPELINE_STAGES         => 1,
    ODOMETER_BUFFER_D                      => 1,

    AVGSPEED_INT_WIDTH                    => 16,
    AVGSPEED_INT_P_PIPELINE_STAGES        => 1,
    AVGSPEED_BUFFER_D                     => 1,

    ACCEL_DECEL_INT_WIDTH                    => 16,
    ACCEL_DECEL_INT_P_PIPELINE_STAGES        => 1,
    ACCEL_DECEL_BUFFER_D                     => 1,

    SPEED_CHANGES_INT_WIDTH                    => 16,
    SPEED_CHANGES_INT_P_PIPELINE_STAGES        => 1,
    SPEED_CHANGES_BUFFER_D                     => 1,

    -- 
    -- BOOLEAN FIELDS
    --
    HYPERMILING_BUFFER_D                  => 1,
    ORIENTATION_BUFFER_D                   => 1,

    -- 
    -- INTEGER ARRAY FIELDS
    --
    SEC_IN_BAND_INT_WIDTH                    => 16,
    SEC_IN_BAND_INT_P_PIPELINE_STAGES        => 1,
    SEC_IN_BAND_BUFFER_D                     => 1,

    MILES_IN_TIME_RANGE_INT_WIDTH                => 16,
    MILES_IN_TIME_RANGE_INT_P_PIPELINE_STAGES    => 1,
    MILES_IN_TIME_RANGE_BUFFER_D                 => 1,
    CONST_SPEED_MILES_IN_BAND_INT_WIDTH             => 16,
    CONST_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES => 1,
    CONST_SPEED_MILES_IN_BAND_BUFFER_D              => 1,
    VARY_SPEED_MILES_IN_BAND_INT_WIDTH               => 16,
    VARY_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES   => 1,
    VARY_SPEED_MILES_IN_BAND_BUFFER_D                => 1,
    SEC_DECEL_INT_WIDTH                => 16,
    SEC_DECEL_INT_P_PIPELINE_STAGES    => 1,
    SEC_DECEL_BUFFER_D                 => 1,
    SEC_ACCEL_INT_WIDTH                => 16,
    SEC_ACCEL_INT_P_PIPELINE_STAGES    => 1,
    SEC_ACCEL_BUFFER_D                 => 1,
    BRAKING_INT_WIDTH                  => 16,
    BRAKING_INT_P_PIPELINE_STAGES      => 1,
    BRAKING_BUFFER_D                   => 1,
    ACCEL_INT_WIDTH                => 16,
    ACCEL_INT_P_PIPELINE_STAGES    => 1,
    ACCEL_BUFFER_D                 => 1,
    SMALL_SPEED_VAR_INT_WIDTH                => 16,
    SMALL_SPEED_VAR_INT_P_PIPELINE_STAGES    => 1,
    SMALL_SPEED_VAR_BUFFER_D                 => 1,
    LARGE_SPEED_VAR_INT_WIDTH                => 16,
    LARGE_SPEED_VAR_INT_P_PIPELINE_STAGES    => 1,
    LARGE_SPEED_VAR_BUFFER_D                 => 1,

    -- 
    -- STRING FIELDS
    --
    TIMESTAMP_BUFFER_D                     => 1,

    END_REQ_EN                             => false
  )
  port map(
    clk                    => open,
    reset                  => open,

    in_valid               => open,
    in_ready               => open,
    in_data                => open,
    in_last                => open,
    in_stai                => open,
    in_endi                => open,
    in_strb                => open,

    end_req                => open,
    end_ack                => open,

    timezone_valid         => open,
    timezone_ready         => open,
    timezone_data          => open,
    timezone_strb          => open,
    timezone_last          => open,

    --    
    -- INTEGER FIELDS   
    --    
    vin_valid              => output_vin_valid,
    vin_ready              => output_vin_ready,
    vin_data               => output_vin,
    vin_strb               => output_vin_dvalid,
    vin_last(0)            => output_vin_last,

    odometer_valid         => open,
    odometer_ready         => open,
    odometer_data          => open,
    odometer_strb          => open,
    odometer_last          => open,

    avgspeed_valid        => open,
    avgspeed_ready        => open,
    avgspeed_data         => open,
    avgspeed_strb         => open,
    avgspeed_last         => open,

    accel_decel_valid        => open,
    accel_decel_ready        => open,
    accel_decel_data         => open,
    accel_decel_strb         => open,
    accel_decel_last         => open,

    speed_changes_valid        => open,
    speed_changes_ready        => open,
    speed_changes_data         => open,
    speed_changes_strb         => open,
    speed_changes_last         => open,

    --    
    -- BOOLEAN FIELDS   
    --    
    hypermiling_valid     => open,
    hypermiling_ready     => open,
    hypermiling_data      => open,
    hypermiling_strb      => open,
    hypermiling_last      => open,

    orientation_valid      => open,
    orientation_ready      => open,
    orientation_data       => open,
    orientation_strb       => open,
    orientation_last       => open,

    --    
    -- INTEGER ARRAY FIELDS   
    --    
    sec_in_band_valid        => open,
    sec_in_band_ready        => open,
    sec_in_band_data         => open,
    sec_in_band_strb         => open,
    sec_in_band_last         => open,

    miles_in_time_range_valid    => open,
    miles_in_time_range_ready    => open,
    miles_in_time_range_data     => open,
    miles_in_time_range_strb     => open,
    miles_in_time_range_last     => open,
    const_speed_miles_in_band_valid => open,
    const_speed_miles_in_band_ready => open,
    const_speed_miles_in_band_data  => open,
    const_speed_miles_in_band_strb  => open,
    const_speed_miles_in_band_last  => open,
    vary_speed_miles_in_band_valid   => open,
    vary_speed_miles_in_band_ready   => open,
    vary_speed_miles_in_band_data    => open,
    vary_speed_miles_in_band_strb    => open,
    vary_speed_miles_in_band_last    => open,
    sec_decel_valid    => open,
    sec_decel_ready    => open,
    sec_decel_data     => open,
    sec_decel_strb     => open,
    sec_decel_last     => open,
    sec_accel_valid    => open,
    sec_accel_ready    => open,
    sec_accel_data     => open,
    sec_accel_strb     => open,
    sec_accel_last     => open,
    braking_valid      => open,
    braking_ready      => open,
    braking_data       => open,
    braking_strb       => open,
    braking_last       => open,
    accel_valid    => open,
    accel_ready    => open,
    accel_data     => open,
    accel_strb     => open,
    accel_last     => open,
    small_speed_var_valid    => open,
    small_speed_var_ready    => open,
    small_speed_var_data     => open,
    small_speed_var_strb     => open,
    small_speed_var_last     => open,
    large_speed_var_valid    => open,
    large_speed_var_ready    => open,
    large_speed_var_data     => open,
    large_speed_var_strb     => open,
    large_speed_var_last     => open,

    --    
    -- STRING FIELDS   
    -- 
    timestamp_valid        => open,
    timestamp_ready        => open,
    timestamp_data         => open,
    timestamp_last         => open,
    timestamp_stai         => open,
    timestamp_endi         => open,
    timestamp_strb         => open
  );



  
end architecture;