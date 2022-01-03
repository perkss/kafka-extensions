# Watermarks

A bit of fun and not meant for production use, a way to understand the concept of watermarks and how other stream
processing systems have implemented them and applying these approaches to Kafka Streams.

Based on
the [Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/)
design for Watermark Kafka source. This transformer will assign and emit periodic watermarks based on all partitions of
a stream. Each transformer initialised by Kafka Streams for each partition will record its watermark per event and then
combine the watermark minimum and add to a transformed values header at intervals.

## Design

### Periodic Emit

The watermark approach for Kafka Streams has been designed to use the periodic emit approach, where the combined
watermark across the source partitions are all combined at a user defined periodic interval.

This reduces latency of calculating on every single input message. The watermark is always emitted as a header in each
record though but will only update on the periodic emit call.

## Limitations

### Multi Instance

* This will not work fully in a multi instant stream setup at this time as the watermark is only tracked across tasks on
  a single instance and the state is not shared across instances.
* Investigate into options here, global state store, https communication? 