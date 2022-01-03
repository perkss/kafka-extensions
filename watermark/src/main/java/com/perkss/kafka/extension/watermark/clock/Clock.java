package com.perkss.kafka.extension.watermark.clock;

public abstract class Clock {

    /**
     * Gets the current relative time, in nanoseconds.
     */
    public abstract long relativeTimeNanos();
}

