import pyarrow as pa

schema_fields = [pa.field("timestamp", pa.utf8(), False),
                 pa.field("timezone", pa.uint64(), False),
                 pa.field("vin", pa.uint64(), False),
                 pa.field("odometer", pa.uint64(), False),
                 pa.field("hypermiling", pa.bool_(), False),
                 pa.field("avgspeed", pa.uint64(), False),
                 pa.field("sec_in_band", pa.list_(pa.field("item", pa.uint64(), False), 12), False),
                 pa.field("miles_in_time_range", pa.list_(pa.field("item", pa.uint64(), False), 24), False),
                 pa.field("const_speed_miles_in_band", pa.list_(pa.field("item", pa.uint64(), False), 12), False),
                 pa.field("vary_speed_miles_in_band", pa.list_(pa.field("item", pa.uint64(), False), 12), False),
                 pa.field("sec_decel", pa.list_(pa.field("item", pa.uint64(), False), 10), False),
                 pa.field("sec_accel", pa.list_(pa.field("item", pa.uint64(), False), 10), False),
                 pa.field("braking", pa.list_(pa.field("item", pa.uint64(), False), 6), False),
                 pa.field("accel", pa.list_(pa.field("item", pa.uint64(), False), 6), False),
                 pa.field("orientation", pa.bool_(), False),
                 pa.field("small_speed_var", pa.list_(pa.field("item", pa.uint64(), False), 13), False),
                 pa.field("large_speed_var", pa.list_(pa.field("item", pa.uint64(), False), 13), False),
                 pa.field("accel_decel", pa.uint64(), False),
                 pa.field("speed_changes", pa.uint64(), False)
                 ]

schema = pa.schema(schema_fields)
serialized_schema = schema.serialize()
pa.output_stream('tripreport.as').write(serialized_schema)
