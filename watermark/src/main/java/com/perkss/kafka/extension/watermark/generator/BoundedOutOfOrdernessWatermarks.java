package com.perkss.kafka.extension.watermark.generator;

import com.perkss.kafka.extension.watermark.Watermark;
import com.perkss.kafka.extension.watermark.WatermarkOutput;

import java.time.Duration;
import java.util.Objects;

/**
 * In Kafka Streams upstream sources may have delays on specific partitions.
 *
 * @param <T>
 */
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    /**
     * The maximum out-of-orderness that this watermark generator assumes.
     */
    private final long outOfOrdernessMillis;

    /**
     * The maximum timestamp encountered so far.
     */
    private long maxTimestamp;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        if (maxOutOfOrderness == null || maxOutOfOrderness.isNegative()) {
            throw new IllegalArgumentException("Out of orderness must be zero or a positive duration");
        }

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // We subtract 1 as there is potential for matching timestamps to still be received
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoundedOutOfOrdernessWatermarks<?> that = (BoundedOutOfOrdernessWatermarks<?>) o;
        return outOfOrdernessMillis == that.outOfOrdernessMillis && maxTimestamp == that.maxTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(outOfOrdernessMillis, maxTimestamp);
    }
}
