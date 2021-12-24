package com.perkss.kafka.extension.watermark;

public class SourceContextWatermarkOutputAdapter<T> implements WatermarkOutput {

    private boolean idle = false;

    public SourceContextWatermarkOutputAdapter() {
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        // TODO change it to use this as a boolean to emit or not?
    }

    // This output has gone idle and downstream events should punctuate or soemthing
    @Override
    //source.markAsTemporarilyIdle
    /**
     * Marks the source to be temporarily idle. This tells the system that this source will
     * temporarily stop emitting records and watermarks for an indefinite amount of time. This
     * is only relevant when running on {@link TimeCharacteristic#IngestionTime} and
     * {@link TimeCharacteristic#EventTime}, allowing downstream tasks to advance their
     * watermarks without the need to wait for watermarks from this source while it is idle.
     *
     * <p>Source functions should make a best effort to call this method as soon as they
     * acknowledge themselves to be idle. The system will consider the source to resume activity
     * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T, long)},
     * or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or watermarks from the source.
     */
    public void markIdle() {
        idle = true;
    }
}
