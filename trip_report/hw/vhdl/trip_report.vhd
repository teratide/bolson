library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;
use work.Stream_pkg.all;
use work.trip_report_pkg.all;
use work.trip_report_util_pkg.all;


entity trip_report is
  generic (
    INDEX_WIDTH : integer := 32;
    TAG_WIDTH   : integer := 1
  );
  port (
    kcd_clk                                       : in  std_logic;
    kcd_reset                                     : in  std_logic;
    input_input_valid                             : in  std_logic;
    input_input_ready                             : out std_logic;
    input_input_dvalid                            : in  std_logic;
    input_input_last                              : in  std_logic;
    input_input                                   : in  std_logic_vector(63 downto 0);
    input_input_count                             : in  std_logic_vector(3 downto 0);
    input_input_unl_valid                         : in  std_logic;
    input_input_unl_ready                         : out std_logic;
    input_input_unl_tag                           : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    input_input_cmd_valid                         : out std_logic;
    input_input_cmd_ready                         : in  std_logic;
    input_input_cmd_firstIdx                      : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    input_input_cmd_lastIdx                       : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    input_input_cmd_tag                           : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_timestamp_valid                        : out std_logic;
    output_timestamp_ready                        : in  std_logic;
    output_timestamp_dvalid                       : out std_logic;
    output_timestamp_last                         : out std_logic;
    output_timestamp_length                       : out std_logic_vector(31 downto 0);
    output_timestamp_count                        : out std_logic_vector(0 downto 0);
    output_timestamp_chars_valid                  : out std_logic;
    output_timestamp_chars_ready                  : in  std_logic;
    output_timestamp_chars_dvalid                 : out std_logic;
    output_timestamp_chars_last                   : out std_logic;
    output_timestamp_chars                        : out std_logic_vector(7 downto 0);
    output_timestamp_chars_count                  : out std_logic_vector(0 downto 0);
    output_timezone_valid                         : out std_logic;
    output_timezone_ready                         : in  std_logic;
    output_timezone_dvalid                        : out std_logic;
    output_timezone_last                          : out std_logic;
    output_timezone                               : out std_logic_vector(63 downto 0);
    output_vin_valid                              : out std_logic;
    output_vin_ready                              : in  std_logic;
    output_vin_dvalid                             : out std_logic;
    output_vin_last                               : out std_logic;
    output_vin                                    : out std_logic_vector(63 downto 0);
    output_odometer_valid                         : out std_logic;
    output_odometer_ready                         : in  std_logic;
    output_odometer_dvalid                        : out std_logic;
    output_odometer_last                          : out std_logic;
    output_odometer                               : out std_logic_vector(63 downto 0);
    output_hypermiling_valid                      : out std_logic;
    output_hypermiling_ready                      : in  std_logic;
    output_hypermiling_dvalid                     : out std_logic;
    output_hypermiling_last                       : out std_logic;
    output_hypermiling                            : out std_logic_vector(7 downto 0);
    output_avgspeed_valid                         : out std_logic;
    output_avgspeed_ready                         : in  std_logic;
    output_avgspeed_dvalid                        : out std_logic;
    output_avgspeed_last                          : out std_logic;
    output_avgspeed                               : out std_logic_vector(63 downto 0);
    output_sec_in_band_valid                      : out std_logic;
    output_sec_in_band_ready                      : in  std_logic;
    output_sec_in_band_dvalid                     : out std_logic;
    output_sec_in_band_last                       : out std_logic;
    output_sec_in_band_length                     : out std_logic_vector(31 downto 0);
    output_sec_in_band_count                      : out std_logic_vector(0 downto 0);
    output_sec_in_band_item_valid                 : out std_logic;
    output_sec_in_band_item_ready                 : in  std_logic;
    output_sec_in_band_item_dvalid                : out std_logic;
    output_sec_in_band_item_last                  : out std_logic;
    output_sec_in_band_item                       : out std_logic_vector(63 downto 0);
    output_sec_in_band_item_count                 : out std_logic_vector(0 downto 0);
    output_miles_in_time_range_valid              : out std_logic;
    output_miles_in_time_range_ready              : in  std_logic;
    output_miles_in_time_range_dvalid             : out std_logic;
    output_miles_in_time_range_last               : out std_logic;
    output_miles_in_time_range_length             : out std_logic_vector(31 downto 0);
    output_miles_in_time_range_count              : out std_logic_vector(0 downto 0);
    output_miles_in_time_range_item_valid         : out std_logic;
    output_miles_in_time_range_item_ready         : in  std_logic;
    output_miles_in_time_range_item_dvalid        : out std_logic;
    output_miles_in_time_range_item_last          : out std_logic;
    output_miles_in_time_range_item               : out std_logic_vector(63 downto 0);
    output_miles_in_time_range_item_count         : out std_logic_vector(0 downto 0);
    output_const_speed_miles_in_band_valid        : out std_logic;
    output_const_speed_miles_in_band_ready        : in  std_logic;
    output_const_speed_miles_in_band_dvalid       : out std_logic;
    output_const_speed_miles_in_band_last         : out std_logic;
    output_const_speed_miles_in_band_length       : out std_logic_vector(31 downto 0);
    output_const_speed_miles_in_band_count        : out std_logic_vector(0 downto 0);
    output_const_speed_miles_in_band_item_valid   : out std_logic;
    output_const_speed_miles_in_band_item_ready   : in  std_logic;
    output_const_speed_miles_in_band_item_dvalid  : out std_logic;
    output_const_speed_miles_in_band_item_last    : out std_logic;
    output_const_speed_miles_in_band_item         : out std_logic_vector(63 downto 0);
    output_const_speed_miles_in_band_item_count   : out std_logic_vector(0 downto 0);
    output_vary_speed_miles_in_band_valid         : out std_logic;
    output_vary_speed_miles_in_band_ready         : in  std_logic;
    output_vary_speed_miles_in_band_dvalid        : out std_logic;
    output_vary_speed_miles_in_band_last          : out std_logic;
    output_vary_speed_miles_in_band_length        : out std_logic_vector(31 downto 0);
    output_vary_speed_miles_in_band_count         : out std_logic_vector(0 downto 0);
    output_vary_speed_miles_in_band_item_valid    : out std_logic;
    output_vary_speed_miles_in_band_item_ready    : in  std_logic;
    output_vary_speed_miles_in_band_item_dvalid   : out std_logic;
    output_vary_speed_miles_in_band_item_last     : out std_logic;
    output_vary_speed_miles_in_band_item          : out std_logic_vector(63 downto 0);
    output_vary_speed_miles_in_band_item_count    : out std_logic_vector(0 downto 0);
    output_sec_decel_valid                        : out std_logic;
    output_sec_decel_ready                        : in  std_logic;
    output_sec_decel_dvalid                       : out std_logic;
    output_sec_decel_last                         : out std_logic;
    output_sec_decel_length                       : out std_logic_vector(31 downto 0);
    output_sec_decel_count                        : out std_logic_vector(0 downto 0);
    output_sec_decel_item_valid                   : out std_logic;
    output_sec_decel_item_ready                   : in  std_logic;
    output_sec_decel_item_dvalid                  : out std_logic;
    output_sec_decel_item_last                    : out std_logic;
    output_sec_decel_item                         : out std_logic_vector(63 downto 0);
    output_sec_decel_item_count                   : out std_logic_vector(0 downto 0);
    output_sec_accel_valid                        : out std_logic;
    output_sec_accel_ready                        : in  std_logic;
    output_sec_accel_dvalid                       : out std_logic;
    output_sec_accel_last                         : out std_logic;
    output_sec_accel_length                       : out std_logic_vector(31 downto 0);
    output_sec_accel_count                        : out std_logic_vector(0 downto 0);
    output_sec_accel_item_valid                   : out std_logic;
    output_sec_accel_item_ready                   : in  std_logic;
    output_sec_accel_item_dvalid                  : out std_logic;
    output_sec_accel_item_last                    : out std_logic;
    output_sec_accel_item                         : out std_logic_vector(63 downto 0);
    output_sec_accel_item_count                   : out std_logic_vector(0 downto 0);
    output_braking_valid                          : out std_logic;
    output_braking_ready                          : in  std_logic;
    output_braking_dvalid                         : out std_logic;
    output_braking_last                           : out std_logic;
    output_braking_length                         : out std_logic_vector(31 downto 0);
    output_braking_count                          : out std_logic_vector(0 downto 0);
    output_braking_item_valid                     : out std_logic;
    output_braking_item_ready                     : in  std_logic;
    output_braking_item_dvalid                    : out std_logic;
    output_braking_item_last                      : out std_logic;
    output_braking_item                           : out std_logic_vector(63 downto 0);
    output_braking_item_count                     : out std_logic_vector(0 downto 0);
    output_accel_valid                            : out std_logic;
    output_accel_ready                            : in  std_logic;
    output_accel_dvalid                           : out std_logic;
    output_accel_last                             : out std_logic;
    output_accel_length                           : out std_logic_vector(31 downto 0);
    output_accel_count                            : out std_logic_vector(0 downto 0);
    output_accel_item_valid                       : out std_logic;
    output_accel_item_ready                       : in  std_logic;
    output_accel_item_dvalid                      : out std_logic;
    output_accel_item_last                        : out std_logic;
    output_accel_item                             : out std_logic_vector(63 downto 0);
    output_accel_item_count                       : out std_logic_vector(0 downto 0);
    output_orientation_valid                      : out std_logic;
    output_orientation_ready                      : in  std_logic;
    output_orientation_dvalid                     : out std_logic;
    output_orientation_last                       : out std_logic;
    output_orientation                            : out std_logic_vector(7 downto 0);
    output_small_speed_var_valid                  : out std_logic;
    output_small_speed_var_ready                  : in  std_logic;
    output_small_speed_var_dvalid                 : out std_logic;
    output_small_speed_var_last                   : out std_logic;
    output_small_speed_var_length                 : out std_logic_vector(31 downto 0);
    output_small_speed_var_count                  : out std_logic_vector(0 downto 0);
    output_small_speed_var_item_valid             : out std_logic;
    output_small_speed_var_item_ready             : in  std_logic;
    output_small_speed_var_item_dvalid            : out std_logic;
    output_small_speed_var_item_last              : out std_logic;
    output_small_speed_var_item                   : out std_logic_vector(63 downto 0);
    output_small_speed_var_item_count             : out std_logic_vector(0 downto 0);
    output_large_speed_var_valid                  : out std_logic;
    output_large_speed_var_ready                  : in  std_logic;
    output_large_speed_var_dvalid                 : out std_logic;
    output_large_speed_var_last                   : out std_logic;
    output_large_speed_var_length                 : out std_logic_vector(31 downto 0);
    output_large_speed_var_count                  : out std_logic_vector(0 downto 0);
    output_large_speed_var_item_valid             : out std_logic;
    output_large_speed_var_item_ready             : in  std_logic;
    output_large_speed_var_item_dvalid            : out std_logic;
    output_large_speed_var_item_last              : out std_logic;
    output_large_speed_var_item                   : out std_logic_vector(63 downto 0);
    output_large_speed_var_item_count             : out std_logic_vector(0 downto 0);
    output_accel_decel_valid                      : out std_logic;
    output_accel_decel_ready                      : in  std_logic;
    output_accel_decel_dvalid                     : out std_logic;
    output_accel_decel_last                       : out std_logic;
    output_accel_decel                            : out std_logic_vector(63 downto 0);
    output_speed_changes_valid                    : out std_logic;
    output_speed_changes_ready                    : in  std_logic;
    output_speed_changes_dvalid                   : out std_logic;
    output_speed_changes_last                     : out std_logic;
    output_speed_changes                          : out std_logic_vector(63 downto 0);
    output_timestamp_unl_valid                    : in  std_logic;
    output_timestamp_unl_ready                    : out std_logic;
    output_timestamp_unl_tag                      : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_timezone_unl_valid                     : in  std_logic;
    output_timezone_unl_ready                     : out std_logic;
    output_timezone_unl_tag                       : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_vin_unl_valid                          : in  std_logic;
    output_vin_unl_ready                          : out std_logic;
    output_vin_unl_tag                            : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_odometer_unl_valid                     : in  std_logic;
    output_odometer_unl_ready                     : out std_logic;
    output_odometer_unl_tag                       : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_hypermiling_unl_valid                  : in  std_logic;
    output_hypermiling_unl_ready                  : out std_logic;
    output_hypermiling_unl_tag                    : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_avgspeed_unl_valid                     : in  std_logic;
    output_avgspeed_unl_ready                     : out std_logic;
    output_avgspeed_unl_tag                       : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_sec_in_band_unl_valid                  : in  std_logic;
    output_sec_in_band_unl_ready                  : out std_logic;
    output_sec_in_band_unl_tag                    : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_miles_in_time_range_unl_valid          : in  std_logic;
    output_miles_in_time_range_unl_ready          : out std_logic;
    output_miles_in_time_range_unl_tag            : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_const_speed_miles_in_band_unl_valid    : in  std_logic;
    output_const_speed_miles_in_band_unl_ready    : out std_logic;
    output_const_speed_miles_in_band_unl_tag      : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_vary_speed_miles_in_band_unl_valid     : in  std_logic;
    output_vary_speed_miles_in_band_unl_ready     : out std_logic;
    output_vary_speed_miles_in_band_unl_tag       : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_sec_decel_unl_valid                    : in  std_logic;
    output_sec_decel_unl_ready                    : out std_logic;
    output_sec_decel_unl_tag                      : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_sec_accel_unl_valid                    : in  std_logic;
    output_sec_accel_unl_ready                    : out std_logic;
    output_sec_accel_unl_tag                      : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_braking_unl_valid                      : in  std_logic;
    output_braking_unl_ready                      : out std_logic;
    output_braking_unl_tag                        : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_accel_unl_valid                        : in  std_logic;
    output_accel_unl_ready                        : out std_logic;
    output_accel_unl_tag                          : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_orientation_unl_valid                  : in  std_logic;
    output_orientation_unl_ready                  : out std_logic;
    output_orientation_unl_tag                    : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_small_speed_var_unl_valid              : in  std_logic;
    output_small_speed_var_unl_ready              : out std_logic;
    output_small_speed_var_unl_tag                : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_large_speed_var_unl_valid              : in  std_logic;
    output_large_speed_var_unl_ready              : out std_logic;
    output_large_speed_var_unl_tag                : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_accel_decel_unl_valid                  : in  std_logic;
    output_accel_decel_unl_ready                  : out std_logic;
    output_accel_decel_unl_tag                    : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_speed_changes_unl_valid                : in  std_logic;
    output_speed_changes_unl_ready                : out std_logic;
    output_speed_changes_unl_tag                  : in  std_logic_vector(TAG_WIDTH-1 downto 0);
    output_timestamp_cmd_valid                    : out std_logic;
    output_timestamp_cmd_ready                    : in  std_logic;
    output_timestamp_cmd_firstIdx                 : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_timestamp_cmd_lastIdx                  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_timestamp_cmd_tag                      : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_timezone_cmd_valid                     : out std_logic;
    output_timezone_cmd_ready                     : in  std_logic;
    output_timezone_cmd_firstIdx                  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_timezone_cmd_lastIdx                   : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_timezone_cmd_tag                       : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_vin_cmd_valid                          : out std_logic;
    output_vin_cmd_ready                          : in  std_logic;
    output_vin_cmd_firstIdx                       : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_vin_cmd_lastIdx                        : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_vin_cmd_tag                            : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_odometer_cmd_valid                     : out std_logic;
    output_odometer_cmd_ready                     : in  std_logic;
    output_odometer_cmd_firstIdx                  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_odometer_cmd_lastIdx                   : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_odometer_cmd_tag                       : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_hypermiling_cmd_valid                  : out std_logic;
    output_hypermiling_cmd_ready                  : in  std_logic;
    output_hypermiling_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_hypermiling_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_hypermiling_cmd_tag                    : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_avgspeed_cmd_valid                     : out std_logic;
    output_avgspeed_cmd_ready                     : in  std_logic;
    output_avgspeed_cmd_firstIdx                  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_avgspeed_cmd_lastIdx                   : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_avgspeed_cmd_tag                       : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_sec_in_band_cmd_valid                  : out std_logic;
    output_sec_in_band_cmd_ready                  : in  std_logic;
    output_sec_in_band_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_sec_in_band_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_sec_in_band_cmd_tag                    : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_miles_in_time_range_cmd_valid          : out std_logic;
    output_miles_in_time_range_cmd_ready          : in  std_logic;
    output_miles_in_time_range_cmd_firstIdx       : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_miles_in_time_range_cmd_lastIdx        : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_miles_in_time_range_cmd_tag            : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_const_speed_miles_in_band_cmd_valid    : out std_logic;
    output_const_speed_miles_in_band_cmd_ready    : in  std_logic;
    output_const_speed_miles_in_band_cmd_firstIdx : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_const_speed_miles_in_band_cmd_lastIdx  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_const_speed_miles_in_band_cmd_tag      : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_vary_speed_miles_in_band_cmd_valid     : out std_logic;
    output_vary_speed_miles_in_band_cmd_ready     : in  std_logic;
    output_vary_speed_miles_in_band_cmd_firstIdx  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_vary_speed_miles_in_band_cmd_lastIdx   : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_vary_speed_miles_in_band_cmd_tag       : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_sec_decel_cmd_valid                    : out std_logic;
    output_sec_decel_cmd_ready                    : in  std_logic;
    output_sec_decel_cmd_firstIdx                 : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_sec_decel_cmd_lastIdx                  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_sec_decel_cmd_tag                      : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_sec_accel_cmd_valid                    : out std_logic;
    output_sec_accel_cmd_ready                    : in  std_logic;
    output_sec_accel_cmd_firstIdx                 : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_sec_accel_cmd_lastIdx                  : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_sec_accel_cmd_tag                      : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_braking_cmd_valid                      : out std_logic;
    output_braking_cmd_ready                      : in  std_logic;
    output_braking_cmd_firstIdx                   : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_braking_cmd_lastIdx                    : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_braking_cmd_tag                        : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_accel_cmd_valid                        : out std_logic;
    output_accel_cmd_ready                        : in  std_logic;
    output_accel_cmd_firstIdx                     : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_accel_cmd_lastIdx                      : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_accel_cmd_tag                          : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_orientation_cmd_valid                  : out std_logic;
    output_orientation_cmd_ready                  : in  std_logic;
    output_orientation_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_orientation_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_orientation_cmd_tag                    : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_small_speed_var_cmd_valid              : out std_logic;
    output_small_speed_var_cmd_ready              : in  std_logic;
    output_small_speed_var_cmd_firstIdx           : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_small_speed_var_cmd_lastIdx            : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_small_speed_var_cmd_tag                : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_large_speed_var_cmd_valid              : out std_logic;
    output_large_speed_var_cmd_ready              : in  std_logic;
    output_large_speed_var_cmd_firstIdx           : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_large_speed_var_cmd_lastIdx            : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_large_speed_var_cmd_tag                : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_accel_decel_cmd_valid                  : out std_logic;
    output_accel_decel_cmd_ready                  : in  std_logic;
    output_accel_decel_cmd_firstIdx               : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_accel_decel_cmd_lastIdx                : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_accel_decel_cmd_tag                    : out std_logic_vector(TAG_WIDTH-1 downto 0);
    output_speed_changes_cmd_valid                : out std_logic;
    output_speed_changes_cmd_ready                : in  std_logic;
    output_speed_changes_cmd_firstIdx             : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_speed_changes_cmd_lastIdx              : out std_logic_vector(INDEX_WIDTH-1 downto 0);
    output_speed_changes_cmd_tag                  : out std_logic_vector(TAG_WIDTH-1 downto 0);
    start                                         : in  std_logic;
    stop                                          : in  std_logic;
    reset                                         : in  std_logic;
    idle                                          : out std_logic;
    busy                                          : out std_logic;
    done                                          : out std_logic;
    result                                        : out std_logic_vector(63 downto 0);
    input_firstidx                                : in  std_logic_vector(31 downto 0);
    input_lastidx                                 : in  std_logic_vector(31 downto 0);
    output_firstidx                               : in  std_logic_vector(31 downto 0);
    output_lastidx                                : in  std_logic_vector(31 downto 0);
    ext_platform_complete_req                     : out std_logic;
    ext_platform_complete_ack                     : in  std_logic
  );
end entity;

architecture Implementation of trip_report is
  -- elements per cycle of input stream
  -- matches the value set in the schema (generate.py)
  constant EPC : natural := 8;

  -- 
    -- INTEGER FIELDS
    --
  constant TIMEZONE_INT_WIDTH                   :natural := 64;
  constant VIN_INT_WIDTH                        :natural := 64;
  constant ODOMETER_INT_WIDTH                   :natural := 64;
  constant AVGSPEED_INT_WIDTH                   :natural := 64;
  constant ACCEL_DECEL_INT_WIDTH                :natural := 64;
  constant SPEED_CHANGES_INT_WIDTH              :natural := 64;
  constant SEC_IN_BAND_INT_WIDTH                :natural := 64;
  constant MILES_IN_TIME_RANGE_INT_WIDTH        :natural := 64;
  constant CONST_SPEED_MILES_IN_BAND_INT_WIDTH  :natural := 64;
  constant VARY_SPEED_MILES_IN_BAND_INT_WIDTH   :natural := 64;
  constant SEC_DECEL_INT_WIDTH                  :natural := 64;
  constant SEC_ACCEL_INT_WIDTH                  :natural := 64;
  constant BRAKING_INT_WIDTH                    :natural := 64;
  constant ACCEL_INT_WIDTH                      :natural := 64;
  constant SMALL_SPEED_VAR_INT_WIDTH            :natural := 64;
  constant LARGE_SPEED_VAR_INT_WIDTH            :natural := 64;

  signal sec_in_band_valid                : std_logic;
  signal sec_in_band_ready                : std_logic;
  signal sec_in_band_data                 : std_logic_vector(SEC_IN_BAND_INT_WIDTH-1 downto 0);
  signal sec_in_band_strb                 : std_logic;
  signal sec_in_band_last                 : std_logic_vector(2 downto 0);

  signal miles_in_time_range_valid        : std_logic;
  signal miles_in_time_range_ready        : std_logic;
  signal miles_in_time_range_data         : std_logic_vector(MILES_IN_TIME_RANGE_INT_WIDTH-1 downto 0);
  signal miles_in_time_range_strb         : std_logic;
  signal miles_in_time_range_last         : std_logic_vector(2 downto 0);

  signal const_speed_miles_in_band_valid  : std_logic;
  signal const_speed_miles_in_band_ready  : std_logic;
  signal const_speed_miles_in_band_data   : std_logic_vector(CONST_SPEED_MILES_IN_BAND_INT_WIDTH-1 downto 0);
  signal const_speed_miles_in_band_strb   : std_logic;
  signal const_speed_miles_in_band_last   : std_logic_vector(2 downto 0);

  signal vary_speed_miles_in_band_valid   : std_logic;
  signal vary_speed_miles_in_band_ready   : std_logic;
  signal vary_speed_miles_in_band_data    : std_logic_vector(VARY_SPEED_MILES_IN_BAND_INT_WIDTH-1 downto 0);
  signal vary_speed_miles_in_band_strb    : std_logic;
  signal vary_speed_miles_in_band_last    : std_logic_vector(2 downto 0);

  signal sec_decel_valid                  : std_logic;
  signal sec_decel_ready                  : std_logic;
  signal sec_decel_data                   : std_logic_vector(SEC_DECEL_INT_WIDTH-1 downto 0);
  signal sec_decel_strb                   : std_logic;
  signal sec_decel_last                   : std_logic_vector(2 downto 0);

  signal sec_accel_valid                  : std_logic;
  signal sec_accel_ready                  : std_logic;
  signal sec_accel_data                   : std_logic_vector(SEC_ACCEL_INT_WIDTH-1 downto 0);
  signal sec_accel_strb                   : std_logic;
  signal sec_accel_last                   : std_logic_vector(2 downto 0);

  signal braking_valid                    : std_logic;
  signal braking_ready                    : std_logic;
  signal braking_data                     : std_logic_vector(BRAKING_INT_WIDTH-1 downto 0);
  signal braking_strb                     : std_logic;
  signal braking_last                     : std_logic_vector(2 downto 0);

  signal accel_valid                      : std_logic;
  signal accel_ready                      : std_logic;
  signal accel_data                       : std_logic_vector(ACCEL_INT_WIDTH-1 downto 0);
  signal accel_strb                       : std_logic;
  signal accel_last                       : std_logic_vector(2 downto 0);

  signal small_speed_var_valid            : std_logic;
  signal small_speed_var_ready            : std_logic;
  signal small_speed_var_data             : std_logic_vector(SMALL_SPEED_VAR_INT_WIDTH-1 downto 0);
  signal small_speed_var_strb             : std_logic;
  signal small_speed_var_last             : std_logic_vector(2 downto 0);

  signal large_speed_var_valid            : std_logic;
  signal large_speed_var_ready            : std_logic;
  signal large_speed_var_data             : std_logic_vector(LARGE_SPEED_VAR_INT_WIDTH-1 downto 0);
  signal large_speed_var_strb             : std_logic;
  signal large_speed_var_last             : std_logic_vector(2 downto 0);

  -- Integer and boolean field last signals
  signal timezone_last                    : std_logic_vector(1 downto 0);
  signal vin_last                         : std_logic_vector(1 downto 0);
  signal odometer_last                    : std_logic_vector(1 downto 0);
  signal avgspeed_last                    : std_logic_vector(1 downto 0);
  signal accel_decel_last                 : std_logic_vector(1 downto 0);
  signal speed_changes_last               : std_logic_vector(1 downto 0);
  signal hypermiling_last                 : std_logic_vector(1 downto 0);
  signal orientation_last                 : std_logic_vector(1 downto 0);


  signal timestamp_valid                  : std_logic;
  signal timestamp_ready                  : std_logic;
  signal timestamp_data                   : std_logic_vector(8*EPC-1 downto 0);
  signal timestamp_last                   : std_logic_vector(3*EPC-1 downto 0);
  signal timestamp_strb                   : std_logic_vector(EPC-1 downto 0);

  signal timestamp_ser_valid              : std_logic;
  signal timestamp_ser_ready              : std_logic;
  signal timestamp_ser_data               : std_logic_vector(7 downto 0);
  signal timestamp_ser_last               : std_logic_vector(2 downto 0);
  signal timestamp_ser_strb               : std_logic;

  signal hypermiling_data                 : std_logic;
  signal orientation_data                 : std_logic;


  signal int_input_input_ready            : std_logic;

  signal in_strb                          : std_logic_vector(EPC - 1 downto 0);
  signal int_in_last                      : std_logic_vector(2 * EPC - 1 downto 0);

  signal record_counter                   : unsigned(63 downto 0);

  signal cmd_valid                        : std_logic;
  signal cmd_ready                        : std_logic;
  signal unl_valid                        : std_logic;
  signal unl_ready                        : std_logic;



  type state_t is (
    STATE_IDLE,         -- idle
    STATE_REQ_READ,     -- send read request
    STATE_REQ_WRITE,    -- send write request
    STATE_UNLOCK_READ,  -- unlock read
    STATE_UNLOCK_WRITE, -- unlock write
    STATE_FENCE,        -- write fence
    STATE_DONE          -- done
  );

  -- state signals
  signal state, state_next     : state_t;


begin

  tydi_strb : process (input_input_dvalid, input_input_count)
  begin
    in_strb <= (others => '0');
    for i in in_strb'range loop
      if unsigned(input_input_count) = 0 or i < unsigned(input_input_count) then
        in_strb(i) <= input_input_dvalid;
      end if;
    end loop;
  end process;

  int_in_last_proc : process (input_input_last)
  begin
    int_in_last              <= (others => '0');
    -- all records are currently sent in one transfer, so there's no difference
    -- between the two dimensions going into the parser.
    int_in_last(EPC * 2 - 2) <= input_input_last;
    int_in_last(EPC * 2 - 1) <= input_input_last;
  end process;

  counter : process (kcd_clk)
    is
  begin
    if rising_edge(kcd_clk) then
      if timestamp_ser_valid = '1' and timestamp_ser_ready = '1' and timestamp_ser_last(1) = '1' then
        record_counter <= record_counter + 1;
      end if;
      if kcd_reset = '1' or reset = '1' then
        record_counter <= (others => '0');
      end if;
    end if;
  end process;

  result <= std_logic_vector(record_counter);




  -- write request defaults
  output_timezone_cmd_firstIdx                  <= output_firstidx;
  output_timezone_cmd_lastIdx                   <= output_lastidx;
  output_timezone_cmd_tag                       <= (others => '0');
  
  output_vin_cmd_firstIdx                       <= output_firstidx;
  output_vin_cmd_lastIdx                        <= output_lastidx;
  output_vin_cmd_tag                            <= (others => '0');
  
  output_odometer_cmd_firstIdx                  <= output_firstidx;
  output_odometer_cmd_lastIdx                   <= output_lastidx;
  output_odometer_cmd_tag                       <= (others => '0');
  
  output_avgspeed_cmd_firstIdx                  <= output_firstidx;
  output_avgspeed_cmd_lastIdx                   <= output_lastidx;
  output_avgspeed_cmd_tag                       <= (others => '0');
  
  output_accel_decel_cmd_firstIdx               <= output_firstidx;
  output_accel_decel_cmd_lastIdx                <= output_lastidx;
  output_accel_decel_cmd_tag                    <= (others => '0');
  
  output_speed_changes_cmd_firstIdx             <= output_firstidx;
  output_speed_changes_cmd_lastIdx              <= output_lastidx;
  output_speed_changes_cmd_tag                  <= (others => '0');
  
  output_sec_in_band_cmd_firstIdx               <= output_firstidx;
  output_sec_in_band_cmd_lastIdx                <= output_lastidx;
  output_sec_in_band_cmd_tag                    <= (others => '0');
  
  output_miles_in_time_range_cmd_firstIdx       <= output_firstidx;
  output_miles_in_time_range_cmd_lastIdx        <= output_lastidx;
  output_miles_in_time_range_cmd_tag            <= (others => '0');
  
  output_const_speed_miles_in_band_cmd_firstIdx <= output_firstidx;
  output_const_speed_miles_in_band_cmd_lastIdx  <= output_lastidx;
  output_const_speed_miles_in_band_cmd_tag      <= (others => '0');
  
  output_vary_speed_miles_in_band_cmd_firstIdx  <= output_firstidx;
  output_vary_speed_miles_in_band_cmd_lastIdx   <= output_lastidx;
  output_vary_speed_miles_in_band_cmd_tag       <= (others => '0');
  
  output_sec_decel_cmd_firstIdx                 <= output_firstidx;
  output_sec_decel_cmd_lastIdx                  <= output_lastidx;
  output_sec_decel_cmd_tag                      <= (others => '0');
  
  output_sec_accel_cmd_firstIdx                 <= output_firstidx;
  output_sec_accel_cmd_lastIdx                  <= output_lastidx;
  output_sec_accel_cmd_tag                      <= (others => '0');
  
  output_braking_cmd_firstIdx                   <= output_firstidx;
  output_braking_cmd_lastIdx                    <= output_lastidx;
  output_braking_cmd_tag                        <= (others => '0');
  
  output_accel_cmd_firstIdx                     <= output_firstidx;
  output_accel_cmd_lastIdx                      <= output_lastidx;
  output_accel_cmd_tag                          <= (others => '0');
  
  output_small_speed_var_cmd_firstIdx           <= output_firstidx;
  output_small_speed_var_cmd_lastIdx            <= output_lastidx;
  output_small_speed_var_cmd_tag                <= (others => '0');
  
  output_large_speed_var_cmd_firstIdx           <= output_firstidx;
  output_large_speed_var_cmd_lastIdx            <= output_lastidx;
  output_large_speed_var_cmd_tag                <= (others => '0');
  
  output_hypermiling_cmd_firstIdx               <= output_firstidx;
  output_hypermiling_cmd_lastIdx                <= output_lastidx;
  output_hypermiling_cmd_tag                    <= (others => '0');
  
  output_orientation_cmd_firstIdx               <= output_firstidx;
  output_orientation_cmd_lastIdx                <= output_lastidx;
  output_orientation_cmd_tag                    <= (others => '0');
  
  output_timestamp_cmd_firstIdx                 <= output_firstidx;
  output_timestamp_cmd_lastIdx                  <= output_lastidx;
  output_timestamp_cmd_tag                      <= (others => '0');
  

  sync_unl: StreamSync
    generic map (
      NUM_INPUTS              => 19,
      NUM_OUTPUTS             => 1
    )
    port map (
      clk                     => kcd_clk,
      reset                   => kcd_reset,

      out_valid(0)            => unl_valid,
      out_ready(0)            => unl_ready,

      in_valid(0)            => output_timezone_unl_valid,
      in_valid(1)            => output_vin_unl_valid,
      in_valid(2)            => output_odometer_unl_valid,
      in_valid(3)            => output_avgspeed_unl_valid,
      in_valid(4)            => output_accel_decel_unl_valid,
      in_valid(5)            => output_speed_changes_unl_valid,
      in_valid(6)            => output_sec_in_band_unl_valid,
      in_valid(7)            => output_miles_in_time_range_unl_valid,
      in_valid(8)            => output_const_speed_miles_in_band_unl_valid,
      in_valid(9)            => output_vary_speed_miles_in_band_unl_valid,
      in_valid(10)           => output_sec_decel_unl_valid,
      in_valid(11)           => output_sec_accel_unl_valid,
      in_valid(12)           => output_braking_unl_valid,
      in_valid(13)           => output_accel_unl_valid,
      in_valid(14)           => output_small_speed_var_unl_valid,
      in_valid(15)           => output_large_speed_var_unl_valid,
      in_valid(16)           => output_hypermiling_unl_valid,
      in_valid(17)           => output_orientation_unl_valid,
      in_valid(18)           => output_timestamp_unl_valid,

      in_ready(0)            => output_timezone_unl_ready,
      in_ready(1)            => output_vin_unl_ready,
      in_ready(2)            => output_odometer_unl_ready,
      in_ready(3)            => output_avgspeed_unl_ready,
      in_ready(4)            => output_accel_decel_unl_ready,
      in_ready(5)            => output_speed_changes_unl_ready,
      in_ready(6)            => output_sec_in_band_unl_ready,
      in_ready(7)            => output_miles_in_time_range_unl_ready,
      in_ready(8)            => output_const_speed_miles_in_band_unl_ready,
      in_ready(9)            => output_vary_speed_miles_in_band_unl_ready,
      in_ready(10)           => output_sec_decel_unl_ready,
      in_ready(11)           => output_sec_accel_unl_ready,
      in_ready(12)           => output_braking_unl_ready,
      in_ready(13)           => output_accel_unl_ready,
      in_ready(14)           => output_small_speed_var_unl_ready,
      in_ready(15)           => output_large_speed_var_unl_ready,
      in_ready(16)           => output_hypermiling_unl_ready,
      in_ready(17)           => output_orientation_unl_ready,
      in_ready(18)           => output_timestamp_unl_ready
  );



  sync_cmd: StreamSync
    generic map (
      NUM_INPUTS              => 1,
      NUM_OUTPUTS             => 19
    )
    port map (
      clk                     => kcd_clk,
      reset                   => kcd_reset,

      in_valid(0)             => cmd_valid,
      in_ready(0)             => cmd_ready,

      out_valid(0)            => output_timezone_cmd_valid,
      out_valid(1)            => output_vin_cmd_valid,
      out_valid(2)            => output_odometer_cmd_valid,
      out_valid(3)            => output_avgspeed_cmd_valid,
      out_valid(4)            => output_accel_decel_cmd_valid,
      out_valid(5)            => output_speed_changes_cmd_valid,
      out_valid(6)            => output_sec_in_band_cmd_valid,
      out_valid(7)            => output_miles_in_time_range_cmd_valid,
      out_valid(8)            => output_const_speed_miles_in_band_cmd_valid,
      out_valid(9)            => output_vary_speed_miles_in_band_cmd_valid,
      out_valid(10)           => output_sec_decel_cmd_valid,
      out_valid(11)           => output_sec_accel_cmd_valid,
      out_valid(12)           => output_braking_cmd_valid,
      out_valid(13)           => output_accel_cmd_valid,
      out_valid(14)           => output_small_speed_var_cmd_valid,
      out_valid(15)           => output_large_speed_var_cmd_valid,
      out_valid(16)           => output_hypermiling_cmd_valid,
      out_valid(17)           => output_orientation_cmd_valid,
      out_valid(18)           => output_timestamp_cmd_valid,

      out_ready(0)            => output_timezone_cmd_ready,
      out_ready(1)            => output_vin_cmd_ready,
      out_ready(2)            => output_odometer_cmd_ready,
      out_ready(3)            => output_avgspeed_cmd_ready,
      out_ready(4)            => output_accel_decel_cmd_ready,
      out_ready(5)            => output_speed_changes_cmd_ready,
      out_ready(6)            => output_sec_in_band_cmd_ready,
      out_ready(7)            => output_miles_in_time_range_cmd_ready,
      out_ready(8)            => output_const_speed_miles_in_band_cmd_ready,
      out_ready(9)            => output_vary_speed_miles_in_band_cmd_ready,
      out_ready(10)           => output_sec_decel_cmd_ready,
      out_ready(11)           => output_sec_accel_cmd_ready,
      out_ready(12)           => output_braking_cmd_ready,
      out_ready(13)           => output_accel_cmd_ready,
      out_ready(14)           => output_small_speed_var_cmd_ready,
      out_ready(15)           => output_large_speed_var_cmd_ready,
      out_ready(16)           => output_hypermiling_cmd_ready,
      out_ready(17)           => output_orientation_cmd_ready,
      out_ready(18)           => output_timestamp_cmd_ready
  );

  comb : process (
    start,
    reset,
    state,
    input_firstidx,
    input_lastidx,
    int_input_input_ready,
    input_input_cmd_ready,
    cmd_ready,
    input_input_unl_valid,
    unl_valid
    ) is
  begin

    -- read request defaults
    input_input_cmd_valid       <= '0';
    input_input_cmd_firstIdx    <= input_firstidx;
    input_input_cmd_lastIdx     <= input_lastidx;
    input_input_cmd_tag         <= (others => '0');
    input_input_unl_ready       <= '0';

    cmd_valid                   <= '0';
    unl_ready                   <= '0';

    -- next state is the same if not changed
    state_next                  <= state;

    -- internal signal
    input_input_ready           <= int_input_input_ready;

    ext_platform_complete_req   <= '0';

    case state is

        -- wait for start signal
      when STATE_IDLE =>
        done <= '0';
        busy <= '0';
        idle <= '1';

        if start = '1' then
          state_next <= STATE_REQ_READ;
        end if;

        -- send read request
      when STATE_REQ_READ =>
        done                  <= '0';
        busy                  <= '1';
        idle                  <= '0';

        input_input_cmd_valid <= '1';

        -- handshake
        if input_input_cmd_ready = '1' then
          state_next <= STATE_REQ_WRITE;
        end if;

        -- send write request
      when STATE_REQ_WRITE =>
        done                     <= '0';
        busy                     <= '1';
        idle                     <= '0';

        cmd_valid <= '1';

        -- handshake
        if cmd_ready = '1' then
          state_next <= STATE_UNLOCK_READ;
        end if;

        -- unlock read
      when STATE_UNLOCK_READ =>
        done <= '0';
        busy <= '1';
        idle <= '0';

        if input_input_unl_valid = '1' then
          input_input_unl_ready  <= '1';
          state_next <= STATE_UNLOCK_WRITE;
        end if;

        -- unlock write
      when STATE_UNLOCK_WRITE =>
        done <= '0';
        busy <= '1';
        idle <= '0';

        if unl_valid = '1' then
          unl_ready  <= '1';
          state_next <= STATE_DONE;
        end if;


      when STATE_FENCE =>
        done                      <= '0';
        busy                      <= '1';
        idle                      <= '0';

        ext_platform_complete_req <= '1';
        if ext_platform_complete_ack = '1' then
          state_next <= STATE_DONE;
        end if;

        -- wait for kernel reset
      when STATE_DONE =>
        done <= '1';
        busy <= '0';
        idle <= '1';

        if reset = '1' then
          state_next <= STATE_IDLE;
        end if;

    end case;

  end process;

  seq : process (kcd_clk)
  begin
    if rising_edge(kcd_clk) then
      state <= state_next;

      if kcd_reset = '1' then
        state <= STATE_IDLE;
      end if;

    end if;
  end process;


  trip_report_parser : TripReportParser
  generic map(
    EPC                                              => 8,

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
    SEC_IN_BAND_BUFFER_D                             => 1,

    MILES_IN_TIME_RANGE_INT_WIDTH                    => MILES_IN_TIME_RANGE_INT_WIDTH,
    MILES_IN_TIME_RANGE_INT_P_PIPELINE_STAGES        => 4,
    MILES_IN_TIME_RANGE_BUFFER_D                     => 1,

    CONST_SPEED_MILES_IN_BAND_INT_WIDTH              => CONST_SPEED_MILES_IN_BAND_INT_WIDTH,
    CONST_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES  => 4,
    CONST_SPEED_MILES_IN_BAND_BUFFER_D               => 1,

    VARY_SPEED_MILES_IN_BAND_INT_WIDTH               => VARY_SPEED_MILES_IN_BAND_INT_WIDTH,
    VARY_SPEED_MILES_IN_BAND_INT_P_PIPELINE_STAGES   => 4,
    VARY_SPEED_MILES_IN_BAND_BUFFER_D                => 1,

    SEC_DECEL_INT_WIDTH                              => SEC_DECEL_INT_WIDTH,
    SEC_DECEL_INT_P_PIPELINE_STAGES                  => 4,
    SEC_DECEL_BUFFER_D                               => 1,

    SEC_ACCEL_INT_WIDTH                              => SEC_ACCEL_INT_WIDTH,
    SEC_ACCEL_INT_P_PIPELINE_STAGES                  => 4,
    SEC_ACCEL_BUFFER_D                               => 1,

    BRAKING_INT_WIDTH                                => BRAKING_INT_WIDTH,
    BRAKING_INT_P_PIPELINE_STAGES                    => 4,
    BRAKING_BUFFER_D                                 => 1,

    ACCEL_INT_WIDTH                                  => ACCEL_INT_WIDTH,
    ACCEL_INT_P_PIPELINE_STAGES                      => 4,
    ACCEL_BUFFER_D                                   => 1,

    SMALL_SPEED_VAR_INT_WIDTH                        => SMALL_SPEED_VAR_INT_WIDTH,
    SMALL_SPEED_VAR_INT_P_PIPELINE_STAGES            => 4,
    SMALL_SPEED_VAR_BUFFER_D                         => 1,

    LARGE_SPEED_VAR_INT_WIDTH                        => LARGE_SPEED_VAR_INT_WIDTH,
    LARGE_SPEED_VAR_INT_P_PIPELINE_STAGES            => 4,
    LARGE_SPEED_VAR_BUFFER_D                         => 1,

    -- 
    -- STRING FIELDS
    --
    TIMESTAMP_BUFFER_D                               => 1,
    END_REQ_EN                                       => false
  )
  port map(
    clk                                         => kcd_clk,
    reset                                       => kcd_reset,

    in_valid                                    => input_input_valid,
    in_ready                                    => int_input_input_ready,
    in_data                                     => input_input,
    in_last                                     => int_in_last,
    in_stai                                     => (others => '0'),
    in_endi                                     => (others => '1'),
    in_strb                                     => in_strb,

    end_req                                     => '0',
    end_ack                                     => open,

    --                
    -- INTEGER FIELDS               
    --    
    timezone_valid                              => output_timezone_valid,
    timezone_ready                              => output_timezone_ready,
    timezone_data                               => output_timezone,
    timezone_strb                               => output_timezone_dvalid,
    timezone_last                               => timezone_last,
            
    vin_valid                                   => output_vin_valid,
    vin_ready                                   => output_vin_ready,
    vin_data                                    => output_vin,
    vin_strb                                    => output_vin_dvalid,
    vin_last                                    => vin_last,

    odometer_valid                              => output_odometer_valid,
    odometer_ready                              => output_odometer_ready,
    odometer_data                               => output_odometer,
    odometer_strb                               => output_odometer_dvalid,
    odometer_last                               => odometer_last,

    avgspeed_valid                              => output_avgspeed_valid,
    avgspeed_ready                              => output_avgspeed_ready,
    avgspeed_data                               => output_avgspeed,
    avgspeed_strb                               => output_avgspeed_dvalid,
    avgspeed_last                               => avgspeed_last,

    accel_decel_valid                           => output_accel_decel_valid,
    accel_decel_ready                           => output_accel_decel_ready,
    accel_decel_data                            => output_accel_decel,
    accel_decel_strb                            => output_accel_decel_dvalid,
    accel_decel_last                            => accel_decel_last,

    speed_changes_valid                         => output_speed_changes_valid,
    speed_changes_ready                         => output_speed_changes_ready,
    speed_changes_data                          => output_speed_changes,
    speed_changes_strb                          => output_speed_changes_dvalid,
    speed_changes_last                          => speed_changes_last,

    --                
    -- BOOLEAN FIELDS               
    --                
    hypermiling_valid                           => output_hypermiling_valid,
    hypermiling_ready                           => output_hypermiling_ready,
    hypermiling_data                            => hypermiling_data,
    hypermiling_strb                            => output_hypermiling_dvalid,
    hypermiling_last                            => hypermiling_last,

    orientation_valid                           => output_orientation_valid,
    orientation_ready                           => output_orientation_ready,
    orientation_data                            => orientation_data,
    orientation_strb                            => output_orientation_dvalid,
    orientation_last                            => orientation_last,

    --                
    -- INTEGER ARRAY FIELDS               
    --                
    sec_in_band_valid                           => sec_in_band_valid,
    sec_in_band_ready                           => sec_in_band_ready,
    sec_in_band_data                            => sec_in_band_data,
    sec_in_band_strb                            => sec_in_band_strb, 
    sec_in_band_last                            => sec_in_band_last, 

    miles_in_time_range_valid                   => miles_in_time_range_valid,
    miles_in_time_range_ready                   => miles_in_time_range_ready,
    miles_in_time_range_data                    => miles_in_time_range_data, 
    miles_in_time_range_strb                    => miles_in_time_range_strb, 
    miles_in_time_range_last                    => miles_in_time_range_last, 

    const_speed_miles_in_band_valid             => const_speed_miles_in_band_valid,
    const_speed_miles_in_band_ready             => const_speed_miles_in_band_ready,
    const_speed_miles_in_band_data              => const_speed_miles_in_band_data, 
    const_speed_miles_in_band_strb              => const_speed_miles_in_band_strb, 
    const_speed_miles_in_band_last              => const_speed_miles_in_band_last, 

    vary_speed_miles_in_band_valid              => vary_speed_miles_in_band_valid,
    vary_speed_miles_in_band_ready              => vary_speed_miles_in_band_ready,
    vary_speed_miles_in_band_data               => vary_speed_miles_in_band_data,
    vary_speed_miles_in_band_strb               => vary_speed_miles_in_band_strb,
    vary_speed_miles_in_band_last               => vary_speed_miles_in_band_last,

    sec_decel_valid                             => sec_decel_valid,
    sec_decel_ready                             => sec_decel_ready,
    sec_decel_data                              => sec_decel_data,
    sec_decel_strb                              => sec_decel_strb,
    sec_decel_last                              => sec_decel_last,

    sec_accel_valid                             => sec_accel_valid,
    sec_accel_ready                             => sec_accel_ready,
    sec_accel_data                              => sec_accel_data,
    sec_accel_strb                              => sec_accel_strb,
    sec_accel_last                              => sec_accel_last,

    braking_valid                               => braking_valid,
    braking_ready                               => braking_ready,
    braking_data                                => braking_data,
    braking_strb                                => braking_strb,
    braking_last                                => braking_last,

    accel_valid                                 => accel_valid,
    accel_ready                                 => accel_ready,
    accel_data                                  => accel_data,
    accel_strb                                  => accel_strb,
    accel_last                                  => accel_last,

    small_speed_var_valid                       => small_speed_var_valid,
    small_speed_var_ready                       => small_speed_var_ready,
    small_speed_var_data                        => small_speed_var_data,
    small_speed_var_strb                        => small_speed_var_strb,
    small_speed_var_last                        => small_speed_var_last,

    large_speed_var_valid                       => large_speed_var_valid,
    large_speed_var_ready                       => large_speed_var_ready,
    large_speed_var_data                        => large_speed_var_data,
    large_speed_var_strb                        => large_speed_var_strb,
    large_speed_var_last                        => large_speed_var_last,

    --                
    -- STRING FIELDS              
    --            
    timestamp_valid                             => timestamp_valid,
    timestamp_ready                             => timestamp_ready,
    timestamp_data                              => timestamp_data, 
    timestamp_last                              => timestamp_last, 
    timestamp_strb                              => timestamp_strb
  );

  -- Some interfacing

  --
  -- Last signals
  --

  -- Integer + boolean fields
  output_timezone_last      <= timezone_last(1);
  output_vin_last           <= vin_last(1);
  output_odometer_last      <= odometer_last(1);
  output_avgspeed_last      <= avgspeed_last(1);
  output_accel_decel_last   <= accel_decel_last(1);
  output_speed_changes_last <= speed_changes_last(1);
  output_hypermiling_last   <= hypermiling_last(1);
  output_orientation_last   <= orientation_last(1);

  -- Integer array fields


  --
  -- Data conversion for boolean fields
  --
  output_orientation <= "0000000" & orientation_data;
  output_hypermiling <= "0000000" & hypermiling_data;

  --output_orientation <= std_logic_vector(resize(unsigned'("0" & orientation_data), 64));
  --output_hypermiling <= std_logic_vector(resize(unsigned'("0" & hypermiling_data), 64));


  --
  -- Converters for the array fields
  --
  sec_in_band_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => SEC_IN_BAND_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => sec_in_band_valid,
    in_ready      => sec_in_band_ready,
    in_data       => sec_in_band_data,
    in_dvalid     => sec_in_band_strb,
    in_last       => sec_in_band_last(2 downto 1),

    out_valid     => output_sec_in_band_item_valid,
    out_ready     => output_sec_in_band_item_ready,
    out_data      => output_sec_in_band_item,
    out_count     => output_sec_in_band_item_count,
    out_dvalid    => output_sec_in_band_item_dvalid,
    out_last      => output_sec_in_band_item_last,

    length_valid  => output_sec_in_band_valid,
    length_ready  => output_sec_in_band_ready,
    length_data   => output_sec_in_band_length,
    length_last   => output_sec_in_band_last,
    length_dvalid => output_sec_in_band_dvalid,
    length_count  => output_sec_in_band_count
  );

miles_in_time_range_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => MILES_IN_TIME_RANGE_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => miles_in_time_range_valid,
    in_ready      => miles_in_time_range_ready,
    in_data       => miles_in_time_range_data,
    in_dvalid     => miles_in_time_range_strb,
    in_last       => miles_in_time_range_last(2 downto 1),

    out_valid     => output_miles_in_time_range_item_valid,
    out_ready     => output_miles_in_time_range_item_ready,
    out_data      => output_miles_in_time_range_item,
    out_count     => output_miles_in_time_range_item_count,
    out_dvalid    => output_miles_in_time_range_item_dvalid,
    out_last      => output_miles_in_time_range_item_last,

    length_valid  => output_miles_in_time_range_valid,
    length_ready  => output_miles_in_time_range_ready,
    length_data   => output_miles_in_time_range_length,
    length_last   => output_miles_in_time_range_last,
    length_dvalid => output_miles_in_time_range_dvalid,
    length_count  => output_miles_in_time_range_count
  );

const_speed_miles_in_band_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => CONST_SPEED_MILES_IN_BAND_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => const_speed_miles_in_band_valid,
    in_ready      => const_speed_miles_in_band_ready,
    in_data       => const_speed_miles_in_band_data,
    in_dvalid     => const_speed_miles_in_band_strb,
    in_last       => const_speed_miles_in_band_last(2 downto 1),

    out_valid     => output_const_speed_miles_in_band_item_valid,
    out_ready     => output_const_speed_miles_in_band_item_ready,
    out_data      => output_const_speed_miles_in_band_item,
    out_count     => output_const_speed_miles_in_band_item_count,
    out_dvalid    => output_const_speed_miles_in_band_item_dvalid,
    out_last      => output_const_speed_miles_in_band_item_last,

    length_valid  => output_const_speed_miles_in_band_valid,
    length_ready  => output_const_speed_miles_in_band_ready,
    length_data   => output_const_speed_miles_in_band_length,
    length_last   => output_const_speed_miles_in_band_last,
    length_dvalid => output_const_speed_miles_in_band_dvalid,
    length_count  => output_const_speed_miles_in_band_count
  );

vary_speed_miles_in_band_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => VARY_SPEED_MILES_IN_BAND_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => vary_speed_miles_in_band_valid,
    in_ready      => vary_speed_miles_in_band_ready,
    in_data       => vary_speed_miles_in_band_data,
    in_dvalid     => vary_speed_miles_in_band_strb,
    in_last       => vary_speed_miles_in_band_last(2 downto 1),

    out_valid     => output_vary_speed_miles_in_band_item_valid,
    out_ready     => output_vary_speed_miles_in_band_item_ready,
    out_data      => output_vary_speed_miles_in_band_item,
    out_count     => output_vary_speed_miles_in_band_item_count,
    out_dvalid    => output_vary_speed_miles_in_band_item_dvalid,
    out_last      => output_vary_speed_miles_in_band_item_last,

    length_valid  => output_vary_speed_miles_in_band_valid,
    length_ready  => output_vary_speed_miles_in_band_ready,
    length_data   => output_vary_speed_miles_in_band_length,
    length_last   => output_vary_speed_miles_in_band_last,
    length_dvalid => output_vary_speed_miles_in_band_dvalid,
    length_count  => output_vary_speed_miles_in_band_count
  );

sec_decel_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => SEC_DECEL_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => sec_decel_valid,
    in_ready      => sec_decel_ready,
    in_data       => sec_decel_data,
    in_dvalid     => sec_decel_strb,
    in_last       => sec_decel_last(2 downto 1),

    out_valid     => output_sec_decel_item_valid,
    out_ready     => output_sec_decel_item_ready,
    out_data      => output_sec_decel_item,
    out_count     => output_sec_decel_item_count,
    out_dvalid    => output_sec_decel_item_dvalid,
    out_last      => output_sec_decel_item_last,

    length_valid  => output_sec_decel_valid,
    length_ready  => output_sec_decel_ready,
    length_data   => output_sec_decel_length,
    length_last   => output_sec_decel_last,
    length_dvalid => output_sec_decel_dvalid,
    length_count  => output_sec_decel_count
  );

sec_accel_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => SEC_ACCEL_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => sec_accel_valid,
    in_ready      => sec_accel_ready,
    in_data       => sec_accel_data,
    in_dvalid     => sec_accel_strb,
    in_last       => sec_accel_last(2 downto 1),

    out_valid     => output_sec_accel_item_valid,
    out_ready     => output_sec_accel_item_ready,
    out_data      => output_sec_accel_item,
    out_count     => output_sec_accel_item_count,
    out_dvalid    => output_sec_accel_item_dvalid,
    out_last      => output_sec_accel_item_last,

    length_valid  => output_sec_accel_valid,
    length_ready  => output_sec_accel_ready,
    length_data   => output_sec_accel_length,
    length_last   => output_sec_accel_last,
    length_dvalid => output_sec_accel_dvalid,
    length_count  => output_sec_accel_count
  );

braking_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => BRAKING_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => braking_valid,
    in_ready      => braking_ready,
    in_data       => braking_data,
    in_dvalid     => braking_strb,
    in_last       => braking_last(2 downto 1),

    out_valid     => output_braking_item_valid,
    out_ready     => output_braking_item_ready,
    out_data      => output_braking_item,
    out_count     => output_braking_item_count,
    out_dvalid    => output_braking_item_dvalid,
    out_last      => output_braking_item_last,

    length_valid  => output_braking_valid,
    length_ready  => output_braking_ready,
    length_data   => output_braking_length,
    length_last   => output_braking_last,
    length_dvalid => output_braking_dvalid,
    length_count  => output_braking_count
  );

accel_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => ACCEL_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => accel_valid,
    in_ready      => accel_ready,
    in_data       => accel_data,
    in_dvalid     => accel_strb,
    in_last       => accel_last(2 downto 1),

    out_valid     => output_accel_item_valid,
    out_ready     => output_accel_item_ready,
    out_data      => output_accel_item,
    out_count     => output_accel_item_count,
    out_dvalid    => output_accel_item_dvalid,
    out_last      => output_accel_item_last,

    length_valid  => output_accel_valid,
    length_ready  => output_accel_ready,
    length_data   => output_accel_length,
    length_last   => output_accel_last,
    length_dvalid => output_accel_dvalid,
    length_count  => output_accel_count
  );

small_speed_var_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => SMALL_SPEED_VAR_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => small_speed_var_valid,
    in_ready      => small_speed_var_ready,
    in_data       => small_speed_var_data,
    in_dvalid     => small_speed_var_strb,
    in_last       => small_speed_var_last(2 downto 1),

    out_valid     => output_small_speed_var_item_valid,
    out_ready     => output_small_speed_var_item_ready,
    out_data      => output_small_speed_var_item,
    out_count     => output_small_speed_var_item_count,
    out_dvalid    => output_small_speed_var_item_dvalid,
    out_last      => output_small_speed_var_item_last,

    length_valid  => output_small_speed_var_valid,
    length_ready  => output_small_speed_var_ready,
    length_data   => output_small_speed_var_length,
    length_last   => output_small_speed_var_last,
    length_dvalid => output_small_speed_var_dvalid,
    length_count  => output_small_speed_var_count
  );

large_speed_var_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => LARGE_SPEED_VAR_INT_WIDTH,
    LENGTH_WIDTH  => INDEX_WIDTH
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => large_speed_var_valid,
    in_ready      => large_speed_var_ready,
    in_data       => large_speed_var_data,
    in_dvalid     => large_speed_var_strb,
    in_last       => large_speed_var_last(2 downto 1),

    out_valid     => output_large_speed_var_item_valid,
    out_ready     => output_large_speed_var_item_ready,
    out_data      => output_large_speed_var_item,
    out_count     => output_large_speed_var_item_count,
    out_dvalid    => output_large_speed_var_item_dvalid,
    out_last      => output_large_speed_var_item_last,

    length_valid  => output_large_speed_var_valid,
    length_ready  => output_large_speed_var_ready,
    length_data   => output_large_speed_var_length,
    length_last   => output_large_speed_var_last,
    length_dvalid => output_large_speed_var_dvalid,
    length_count  => output_large_speed_var_count
  );


  -- String field conversion.
  -- Consists of two steps: serialize the c=8 tydi stream to EPC=1,
  -- then convert to char and length streams.
  timestamp_serializer : StreamSerializer
  generic map(
    EPC             => EPC,
    DATA_WIDTH      => 8,
    DIMENSIONALITY  => 3 
  ) 
  port map( 
    clk             => kcd_clk,
    reset           => kcd_reset,

    in_valid        => timestamp_valid,
    in_ready        => timestamp_ready,
    in_data         => timestamp_data,
    in_strb         => timestamp_strb, 
    in_last         => timestamp_last, 

    out_valid       => timestamp_ser_valid,
    out_ready       => timestamp_ser_ready,
    out_data        => timestamp_ser_data,
    out_strb        => timestamp_ser_strb, 
    out_last        => timestamp_ser_last 
  );

  timestamp_converter : D2ListToVecs
  generic map(
    EPC           => 1,
    DATA_WIDTH    => 8,
    LENGTH_WIDTH  => INDEX_WIDTH 
  )
  port map(
    clk           => kcd_clk,
    reset         => kcd_reset,

    in_valid      => timestamp_ser_valid,
    in_ready      => timestamp_ser_ready,
    in_data       => timestamp_ser_data,
    in_dvalid     => timestamp_ser_strb,
    in_last       => timestamp_ser_last(2 downto 1),

    out_valid     => output_timestamp_chars_valid, 
    out_ready     => output_timestamp_chars_ready,
    out_data      => output_timestamp_chars,
    out_count     => output_timestamp_chars_count,
    out_dvalid    => output_timestamp_chars_dvalid,
    out_last      => output_timestamp_chars_last,  

    length_valid  => output_timestamp_valid,
    length_ready  => output_timestamp_ready,
    length_dvalid => output_timestamp_dvalid,
    length_last   => output_timestamp_last,
    length_data   => output_timestamp_length,
    length_count  => output_timestamp_count
  );
end architecture;