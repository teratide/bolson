import pyarrow as pa

input_schema = pa.schema([
    pa.field("input", pa.uint8(), False).with_metadata({b'fletcher_epc': b'8'})
]).with_metadata({
    b'fletcher_mode': b'read',
    b'fletcher_name': b'input'
})

pa.output_stream("in.as").write(input_schema.serialize())

with pa.RecordBatchFileWriter('in.rb', input_schema) as writer:
    writer.write(
        pa.RecordBatch.from_arrays(
            [pa.array(
                [byte for byte in '{"voltage":[1128,1213,1850,429,1770,1683,1483,478,545,1555,867,1495,1398,1380,1753,438]}\n'.encode()], pa.uint8())],
            schema=input_schema)
    )

output_schema = pa.schema([
    pa.field("voltage", pa.list_(
        pa.field("item", pa.uint64(), False).with_metadata(
            {"illex_MIN": "0", "illex_MAX": "2047"})
    ), False).with_metadata(
        {"illex_MIN_LENGTH": "1", "illex_MAX_LENGTH": "16"}
    )
]).with_metadata({
    b'fletcher_mode': b'write',
    b'fletcher_name': b'output'
})

pa.output_stream("out.as").write(output_schema.serialize())
