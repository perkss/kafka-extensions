package com.perkss.kafka.extension.watermark;

public interface WatermarkOutput {

    /**
     * Emits the given watermark.
     *
     * <p>Emitting a watermark also implicitly marks the stream as <i>active</i>, ending
     * previously marked idleness.
     */
    void emitWatermark(Watermark watermark);

    /**
     * Marks this output as idle, meaning that downstream operations do not
     * wait for watermarks from this output.
     *
     * <p>An output becomes active again as soon as the next watermark is emitted.
     */
    void markIdle();

}
