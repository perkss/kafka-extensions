package com.perkss.kafka.extension.watermark.clock;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class ManualClock extends Clock {

    private final AtomicLong currentTime;

    public ManualClock() {
        this(0);
    }

    public ManualClock(long startTime) {
        this.currentTime = new AtomicLong(startTime);
    }

    @Override
    public long relativeTimeNanos() {
        return currentTime.get();
    }

    public void advanceTime(Duration duration) {
        currentTime.addAndGet(duration.toNanos());
    }
}

