package com.perkss.kafka.extension.watermark.generator;

import com.perkss.kafka.extension.watermark.Watermark;
import com.perkss.kafka.extension.watermark.WatermarkOutput;

final class TestingWatermarkOutput implements WatermarkOutput {

    private Watermark lastWatermark;

    private boolean isIdle;

    @Override
    public void emitWatermark(Watermark watermark) {
        lastWatermark = watermark;
        isIdle = false;
    }

    @Override
    public void markIdle() {
        isIdle = true;
    }

    public Watermark lastWatermark() {
        return lastWatermark;
    }

    public boolean isIdle() {
        return isIdle;
    }
}
