package com.perkss.kafka.extension.watermark.generator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BoundedOutOfOrdernessWatermarksTest {

    private TestingWatermarkOutput output;

    @BeforeEach
    void setup() {
        output = new TestingWatermarkOutput();
    }

    @Test
    void watermarkBeforeRecords() {
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onPeriodicEmit(output);

        assertNotNull(output.lastWatermark());
        assertEquals(Long.MIN_VALUE, output.lastWatermark().getTimestamp());
    }

    @Test
    void watermarkAfterEvent() {
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onEvent(new Object(), 122511L, output);
        watermarks.onPeriodicEmit(output);

        assertEquals(122500L, output.lastWatermark().getTimestamp());
    }

    @Test
    void watermarkAfterNonMonotonousEvents() {
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onEvent(new Object(), 12545L, output);
        watermarks.onEvent(new Object(), 12500L, output);
        watermarks.onEvent(new Object(), 12540L, output);
        watermarks.onEvent(new Object(), 12580L, output);
        watermarks.onPeriodicEmit(output);

        assertEquals(12569L, output.lastWatermark().getTimestamp());
    }

    @Test
    void repeatedProbe() {
        final BoundedOutOfOrdernessWatermarks<Object> watermarks =
                new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(10));

        watermarks.onEvent(new Object(), 723456L, new TestingWatermarkOutput());
        watermarks.onPeriodicEmit(new TestingWatermarkOutput());

        watermarks.onPeriodicEmit(output);

        assertEquals(723445L, output.lastWatermark().getTimestamp());
    }

}