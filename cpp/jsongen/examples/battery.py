import pyarrow as pa

schema_fields = [pa.field("voltage",
                          pa.list_(pa.field("item", pa.uint64(), False)
                                   .with_metadata({"JSONGEN_MIN": "0", "JSONGEN_MAX": "2047"}),
                                   12), False)]

schema = pa.schema(schema_fields)
serialized_schema = schema.serialize()
pa.output_stream('battery.as').write(serialized_schema)
