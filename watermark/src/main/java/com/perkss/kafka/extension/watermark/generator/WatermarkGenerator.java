package com.perkss.kafka.extension.watermark.generator;

import com.perkss.kafka.extension.watermark.WatermarkOutput;

public interface WatermarkGenerator<T> {

    /**
     * Called for every event, allows the watermark generator to examine and remember the
     * event timestamps, or to emit a watermark based on the event itself.
     */
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /**
     * Called periodically, and might emit a new watermark, or not.
     *
     * <p>The interval in which this method is called and Watermarks are generated
     * depends on WatermarkTransformer#periodicEmit.
     */
    void onPeriodicEmit(WatermarkOutput output);
}

