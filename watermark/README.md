# Watermarks

A bit of fun and not meant for production use, a way to understand the concept of watermarks and how other stream
processing systems have implemented them and applying these approaches to Kafka Streams.

Based on the Flink design for Watermark Kafka source. This transformer will assign and emit periodic watermarks based on
all partitions of a stream. Each transformer initialised by Kafka Streams for each partition will record its watermark
per event and then combine the watermark minimum and add to a transformed values header at intervals.

## Limitations

* This will not work fully in a multi instant stream setup at this time as the watermark is only tracked across tasks on
  a single instance and the state is not shared across instances. 