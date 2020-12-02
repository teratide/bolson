import pyarrow as pa

input_schema = pa.schema([
    pa.field("input", pa.uint8(), False).with_metadata({b'fletcher_epc': b'8'})
]).with_metadata({
    b'fletcher_mode': b'read',
    b'fletcher_name': b'input'
})

pa.output_stream("in.as").write(input_schema.serialize())

output_schema = pa.schema([pa.field("timestamp", pa.utf8(), False),
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
