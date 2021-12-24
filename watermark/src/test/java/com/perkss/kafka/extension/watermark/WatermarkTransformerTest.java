package com.perkss.kafka.extension.watermark;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.perkss.kafka.extension.watermark.WatermarkTransformer.WATERMARK_HEADER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

// TODO
// Builder same name throw exception
// reused builder
// topology with idleness? TODO we can mark idleness if the stream time does not move on periodic emits
// multiple partitions transformer test
class WatermarkTransformerTest {

    private final long minimumWatermark = Long.MIN_VALUE;

    @Test
    void topologyTest() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        builder.stream("input-topic", Consumed.with(stringSerde, stringSerde))
                .transformValues(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .named("name"))
                .to("output-topic", Produced.with(stringSerde, stringSerde));
        Topology topology = builder.build();
        TopologyTestDriver testDriver = new TopologyTestDriver(topology);

        long inputTime = System.currentTimeMillis();
        long firstWatermark = inputTime - 1;

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), stringSerde.serializer());
        inputTopic.pipeInput("A", "FirstMessage", inputTime);

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", stringSerde.deserializer(), stringSerde.deserializer());

        TestRecord<String, String> output = outputTopic.readRecord();

        // No watermark set on first message until the periodic emit is set
        assertThat(ByteUtils.bytesToLong(output.getHeaders().lastHeader(WATERMARK_HEADER).value()), equalTo(minimumWatermark));
        assertThat(output.value(), equalTo("FirstMessage"));
        assertThat(output.key(), equalTo("A"));

        long inputTimeAdvanced5Seconds = inputTime + Duration.ofSeconds(5).toMillis();
        long secondWatermark = inputTimeAdvanced5Seconds - 1;

        inputTopic.pipeInput("B", "FirstMessage", inputTimeAdvanced5Seconds);
        TestRecord<String, String> output2 = outputTopic.readRecord();
        assertThat(ByteUtils.bytesToLong(output2.getHeaders().lastHeader(WATERMARK_HEADER).value()), equalTo(firstWatermark));
        assertThat(output2.value(), equalTo("FirstMessage"));
        assertThat(output2.key(), equalTo("B"));

        long inputTimeAdvanced6Seconds = inputTime + Duration.ofSeconds(6).toMillis();

        inputTopic.pipeInput("A", "SecondMessage", inputTimeAdvanced6Seconds);
        TestRecord<String, String> output3 = outputTopic.readRecord();
        assertThat(ByteUtils.bytesToLong(output3.getHeaders().lastHeader(WATERMARK_HEADER).value()), equalTo(secondWatermark));
        assertThat(output3.value(), equalTo("SecondMessage"));
        assertThat(output3.key(), equalTo("A"));
    }

    @Test
    void transform() {
        final MockProcessorContext context = new MockProcessorContext();

        Long timestamp = System.currentTimeMillis();
        long currentStreamTime = System.currentTimeMillis();
        long currentSystemTime = System.currentTimeMillis();
        context.setRecordTimestamp(timestamp);
        context.setCurrentStreamTimeMs(currentStreamTime);
        context.setCurrentSystemTimeMs(currentSystemTime);
        context.setPartition(1);
        context.setHeaders(new RecordHeaders());

        ValueTransformer<String, String> transformer = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .named("Source")
                .get();

        transformer.init(context);

        String firstInput = "Hello";
        assertEquals(firstInput, transformer.transform(firstInput));

        // No watermark is set to begin with should it default?
        assertThat(ByteUtils.bytesToLong(context.headers().lastHeader(WATERMARK_HEADER).value()), equalTo(minimumWatermark));

        ((WatermarkTransformer<String>) transformer).onPeriodicEmit();

        String secondInput = "HelloWithWatermark";
        assertEquals(secondInput, transformer.transform(secondInput));

        assertEquals(timestamp, context.timestamp());
        assertEquals(timestamp - 1, ByteUtils.bytesToLong(context.headers().lastHeader(WATERMARK_HEADER).value()));
    }

    @Test
    void transformTwoPartitions() {
        final MockProcessorContext contextPartition1 = new MockProcessorContext();

        Long timestamp = System.currentTimeMillis();
        long currentStreamTime = System.currentTimeMillis();
        long currentSystemTime = System.currentTimeMillis();
        contextPartition1.setRecordTimestamp(timestamp);
        contextPartition1.setCurrentStreamTimeMs(currentStreamTime);
        contextPartition1.setCurrentSystemTimeMs(currentSystemTime);
        contextPartition1.setPartition(1);
        contextPartition1.setHeaders(new RecordHeaders());

        final MockProcessorContext contextPartition2 = new MockProcessorContext();

        Long timestamp2 = System.currentTimeMillis() + 1000;
        long currentStreamTime2 = System.currentTimeMillis();
        long currentSystemTime2 = System.currentTimeMillis();
        contextPartition2.setRecordTimestamp(timestamp2);
        contextPartition2.setCurrentStreamTimeMs(currentStreamTime2);
        contextPartition2.setCurrentSystemTimeMs(currentSystemTime2);
        contextPartition2.setPartition(0);
        contextPartition2.setHeaders(new RecordHeaders());

        ValueTransformer<String, String> transformer = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .named("Source")
                .get();

        transformer.init(contextPartition1);

        transformer.transform("Hello");

        ((WatermarkTransformer<String>) transformer).onPeriodicEmit();

        transformer.transform("HelloWithWatermark");

        // assert the forwarded records and header for watermark
        assertEquals(timestamp, contextPartition1.timestamp());
        //assertEquals(currentStreamTime, context.currentStreamTimeMs());
        //assertEquals(currentSystemTime, context.currentSystemTimeMs());
        assertEquals(timestamp - 1, ByteUtils.bytesToLong(contextPartition1.headers().lastHeader(WATERMARK_HEADER).value()));


        transformer.init(contextPartition2);

        transformer.transform("Hello2");

        ((WatermarkTransformer<String>) transformer).onPeriodicEmit();

        transformer.transform("HelloWithWatermark2");

        // assert the forwarded records and header for watermark
        // TODO this does not emit watermark as it has not changed
        assertEquals(timestamp2, contextPartition2.timestamp());
        //assertEquals(currentStreamTime, context.currentStreamTimeMs());
        //assertEquals(currentSystemTime, context.currentSystemTimeMs());
        //  assertNotNull(ByteUtils.bytesToLong(contextPartition2.headers().lastHeader("watermark").value()));


        contextPartition1.setRecordTimestamp(timestamp + 15000);

        transformer.transform("Hello1Part2");

        ((WatermarkTransformer<String>) transformer).onPeriodicEmit();

        transformer.transform("HelloWithWatermark3");

        // assert the forwarded records and header for watermark
        assertEquals(timestamp + 15000, contextPartition1.timestamp());
        //assertEquals(currentStreamTime, context.currentStreamTimeMs());
        //assertEquals(currentSystemTime, context.currentSystemTimeMs());
        assertEquals(timestamp - 1, ByteUtils.bytesToLong(contextPartition1.headers().lastHeader(WATERMARK_HEADER).value()));


    }

}