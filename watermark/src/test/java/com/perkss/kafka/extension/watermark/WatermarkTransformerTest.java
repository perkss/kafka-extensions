package com.perkss.kafka.extension.watermark;

import com.perkss.kafka.extension.watermark.strategy.WatermarkStrategy;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.perkss.kafka.extension.watermark.WatermarkTransformer.WATERMARK_HEADER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WatermarkTransformerTest {

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
        context.setTopic("source");

        ValueTransformer<String, String> transformer = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .named("Source")
                .get();

        transformer.init(context);

        String firstInput = "Hello";
        assertEquals(firstInput, transformer.transform(firstInput));

        // No watermark is set so is the default minimum value
        long minimumWatermark = Long.MIN_VALUE;
        assertThat(ByteUtils.bytesToLong(context.headers().lastHeader(WATERMARK_HEADER).value()), equalTo(minimumWatermark));

        ((WatermarkTransformer<String>) transformer).onPeriodicCalculate();

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
        contextPartition1.setTopic("source");

        final MockProcessorContext contextPartition2 = new MockProcessorContext();

        Long timestamp2 = System.currentTimeMillis() + 1000;
        long currentStreamTime2 = System.currentTimeMillis();
        long currentSystemTime2 = System.currentTimeMillis();
        contextPartition2.setRecordTimestamp(timestamp2);
        contextPartition2.setCurrentStreamTimeMs(currentStreamTime2);
        contextPartition2.setCurrentSystemTimeMs(currentSystemTime2);
        contextPartition2.setPartition(0);
        contextPartition2.setTopic("source");
        contextPartition2.setHeaders(new RecordHeaders());

        ValueTransformer<String, String> transformer = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .named("Source")
                .get();

        transformer.init(contextPartition1);

        transformer.transform("Hello");

        ((WatermarkTransformer<String>) transformer).onPeriodicCalculate();

        transformer.transform("HelloWithWatermark");

        // assert the forwarded records and header for watermark
        assertEquals(timestamp, contextPartition1.timestamp());
        //assertEquals(currentStreamTime, context.currentStreamTimeMs());
        //assertEquals(currentSystemTime, context.currentSystemTimeMs());
        assertEquals(timestamp - 1, ByteUtils.bytesToLong(contextPartition1.headers().lastHeader(WATERMARK_HEADER).value()));


        transformer.init(contextPartition2);

        transformer.transform("Hello2");

        ((WatermarkTransformer<String>) transformer).onPeriodicCalculate();

        transformer.transform("HelloWithWatermark2");

        assertEquals(timestamp2, contextPartition2.timestamp());

        contextPartition1.setRecordTimestamp(timestamp + 15000);

        transformer.transform("Hello1Part2");

        ((WatermarkTransformer<String>) transformer).onPeriodicCalculate();

        transformer.transform("HelloWithWatermark3");

        // assert the forwarded records and header for watermark
        assertEquals(timestamp + 15000, contextPartition1.timestamp());
        assertEquals(timestamp - 1, ByteUtils.bytesToLong(contextPartition1.headers().lastHeader(WATERMARK_HEADER).value()));
    }

}