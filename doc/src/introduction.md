# Introduction

Bolson is a tool that parses JSONs, converts them to records in an Apache Arrow
formatted RecordBatch, serializes the batch to an Arrow IPC message, and 
publishes the message to a Pulsar topic.

```dot process Overview
digraph {
  rankdir=LR; 
  Source -> Bolson [label="Raw JSON data"]
  Bolson -> Pulsar [label="Arrow IPC message"]
}
```