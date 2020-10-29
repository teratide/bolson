import pyarrow as pa

input_schema = pa.schema([
    pa.field("input", pa.uint8(), False).with_metadata({b'fletcher_epc': b'8'})
]).with_metadata({
    b'fletcher_mode': b'read',
    b'fletcher_name': b'input'
})

with pa.RecordBatchFileWriter('input.rb', input_schema) as writer:
    writer.write(
        pa.RecordBatch.from_arrays(
            [pa.array(
                [byte for byte in '{"voltage": [1, 2, 3, 4,5,6,7,8,9,10]}'.encode()], pa.uint8())],
            schema=input_schema)
    )

output_schema = pa.schema([
    pa.field("voltage", pa.list_(
        pa.field("item", pa.uint64(), False)
    ), False)
]).with_metadata({
    b'fletcher_mode': b'write',
    b'fletcher_name': b'output'
})

pa.output_stream("output.as").write(output_schema.serialize())

with pa.RecordBatchFileWriter('output.rb', output_schema) as writer:
    writer.write(
        pa.RecordBatch.from_arrays([
            pa.array(
                [
                    [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
                ], pa.list_(pa.uint64()))
        ], schema=output_schema)
    )
