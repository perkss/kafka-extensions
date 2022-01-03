package com.perkss.kafka.extension.watermark;

import com.perkss.kafka.extension.watermark.strategy.WatermarkStrategy;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.perkss.kafka.extension.watermark.WatermarkTransformer.WATERMARK_HEADER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class WatermarkTransformerTopologyTest {

    private final long minimumWatermark = Long.MIN_VALUE;
    private final Serde<String> stringSerde = Serdes.String();
    private StreamsBuilder builder;

    @BeforeEach
    void setup() {
        builder = new StreamsBuilder();
    }

    @Test
    void topologyEmitsWatermarksAsHeaders() {
        builder.stream("input-topic", Consumed.with(stringSerde, stringSerde))
                .transformValues(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withIdleness(Duration.ofMillis(500))
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
    void topologyEmitsWatermarksAsHeadersDetectsIdleness() {

    }

}