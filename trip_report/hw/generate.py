import pyarrow as pa

input_schema = pa.schema([
    pa.field("input", pa.uint8(), False).with_metadata({b'fletcher_epc': b'8'})
]).with_metadata({
    b'fletcher_mode': b'read',
    b'fletcher_name': b'input'
})

pa.output_stream("in.as").write(input_schema.serialize())

test_rec= """{
    "timestamp": "2005-09-09T11:59:06-10:00",
    "timezone": 883,
    "vin": 16852243674679352615,
    "odometer": 997,
    "hypermiling": false,
    "avgspeed": 156,
    "sec_in_band": [3403, 893, 2225, 78, 162, 2332, 1473, 2587, 3446, 178, 997, 2403],
    "miles_in_time_range_range": [3376, 2553, 2146, 919, 2241, 1044, 1079, 3751, 1665, 2062, 46, 2868, 375, 3305, 4109, 3319, 627, 3523, 2225, 357, 1653, 2757, 3477, 3549],
    "const_speed_miles_in_band": [4175, 2541, 2841, 157, 2922, 651, 315, 2484, 2696, 165, 1366, 958],
    "vary_speed_miles_in_band": [2502, 155, 1516, 1208, 2229, 1850, 4032, 3225, 2704, 2064, 484, 3073],
    "sec_decel": [722, 2549, 547, 3468, 844, 3064, 2710, 1515, 763, 2972],
    "sec_accel": [2580, 3830, 792, 2407, 2425, 3305, 2985, 1920, 3889, 909],
    "braking": [2541, 13, 3533, 59, 116, 134],
    "accel": [1780, 228, 1267, 2389, 437, 871],
    "orientation": false,
    "small_speed_var": [1254, 3048, 377, 754, 1745, 3666, 2820, 3303, 2558, 1308, 2795, 941, 2049],
    "large_speed_var": [3702, 931, 2040, 3388, 2575, 881, 1821, 3675, 2080, 3973, 4132, 3965, 4166],
    "accel_decel": 1148,
    "speed_changes": 1932
}"""

with pa.RecordBatchFileWriter('in.rb', input_schema) as writer:
    writer.write(
        pa.RecordBatch.from_arrays(
            [pa.array(
                [byte for byte in test_rec.encode()], pa.uint8())],
            schema=input_schema)
    )

output_schema = pa.schema([pa.field("timestamp", pa.utf8(), False).with_metadata({
                               b'fletcher_epc': b'1'
                           }),
                           pa.field("timezone", pa.uint64(), False),
                           pa.field("vin", pa.uint64(), False),
                           pa.field("odometer", pa.uint64(), False),
                           pa.field("hypermiling", pa.uint8(), False),
                           pa.field("avgspeed", pa.uint64(), False),
                           pa.field("sec_in_band", pa.uint64(), False),
                           pa.field("miles_in_time_range", pa.uint64(), False),
                           pa.field("const_speed_miles_in_band",
                                    pa.uint64(), False),
                           pa.field("vary_speed_miles_in_band",
                                    pa.uint64(), False),
                           pa.field("sec_decel", pa.uint64(), False),
                           pa.field("sec_accel", pa.uint64(), False),
                           pa.field("braking", pa.uint64(), False),
                           pa.field("accel", pa.uint64(), False),
                           pa.field("orientation", pa.uint8(), False),
                           pa.field("small_speed_var", pa.uint64(), False),
                           pa.field("large_speed_var", pa.uint64(), False),
                           pa.field("accel_decel", pa.uint64(), False),
                           pa.field("speed_changes", pa.uint64(), False)
                           ]).with_metadata({
                               b'fletcher_mode': b'write',
                               b'fletcher_name': b'output'
                           })

pa.output_stream("out.as").write(output_schema.serialize())
