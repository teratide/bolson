import pyarrow as pa

input_schema = pa.schema([
    pa.field("input", pa.uint8(), False).with_metadata(
        {b'fletcher_epc': b'8'})
])

pa.output_stream("in_1.as").write(input_schema.with_metadata({
    b'fletcher_mode': b'read',
    b'fletcher_name': b'input_1'
}).serialize())

pa.output_stream("in_2.as").write(input_schema.with_metadata({
    b'fletcher_mode': b'read',
    b'fletcher_name': b'input_2'
}).serialize())

with pa.RecordBatchFileWriter('in_1.rb', input_schema) as writer:
    writer.write(
        pa.RecordBatch.from_arrays(
            [pa.array(
                [byte for byte in '{"voltage": [1,2]}{"voltage": [4, 42]}'.encode()], pa.uint8())],
            schema=input_schema.with_metadata({
                b'fletcher_mode': b'read',
                b'fletcher_name': b'input_1'
            })
        ))

with pa.RecordBatchFileWriter('in_2.rb', input_schema) as writer:
    writer.write(
        pa.RecordBatch.from_arrays(
            [pa.array(
                [byte for byte in '{"voltage": [9,10]}'.encode()], pa.uint8())],
            schema=input_schema.with_metadata({
                b'fletcher_mode': b'read',
                b'fletcher_name': b'input_2'
            })
        )
    )

output_schema = pa.schema([
    pa.field("voltage", pa.list_(
        pa.field("item", pa.uint64(), False).with_metadata(
            {"illex_MIN": "0", "illex_MAX": "2047"})
    ), False).with_metadata(
        {"illex_MIN_LENGTH": "1", "illex_MAX_LENGTH": "16"}
    )
])

pa.output_stream("out_1.as").write(output_schema.with_metadata({
    b'fletcher_mode': b'write',
    b'fletcher_name': b'output_1'
}).serialize())
pa.output_stream("out_2.as").write(output_schema.with_metadata({
    b'fletcher_mode': b'write',
    b'fletcher_name': b'output_2'
}).serialize())
