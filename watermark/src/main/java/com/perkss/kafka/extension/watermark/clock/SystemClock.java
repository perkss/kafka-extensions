package com.perkss.kafka.extension.watermark.clock;

public final class SystemClock extends Clock {

    private static final SystemClock INSTANCE = new SystemClock();

    public static SystemClock getInstance() {
        return INSTANCE;
    }

    @Override
    public long relativeTimeNanos() {
        return System.nanoTime();
    }

    private SystemClock() {
    }
}

