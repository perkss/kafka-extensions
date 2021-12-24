package com.perkss.kafka.extension.watermark;

/**
 * Tracks the watermark and idleness of a specific partition of a task
 */
public interface WatermarkOutput {

    /**
     * Emits the given watermark
     */
    void emitWatermark(Watermark watermark);

    /**
     * Marks this output as idle.
     * TODO how can we handle this scenario alert all tasks related?
     */
    void markIdle();

}
