package com.perkss.kafka.extension.watermark.generator;

import com.perkss.kafka.extension.watermark.WatermarkOutput;
import com.perkss.kafka.extension.watermark.clock.Clock;
import com.perkss.kafka.extension.watermark.clock.SystemClock;

import java.time.Duration;

public class WatermarksWithIdleness<T> implements WatermarkGenerator<T> {

    private final WatermarkGenerator<T> watermarks;

    private final IdlenessTimer idlenessTimer;

    /**
     * Creates a new WatermarksWithIdleness generator to the given generator idleness
     * detection with the given timeout.
     *
     * @param watermarks  The original watermark generator.
     * @param idleTimeout The timeout for the idleness detection.
     */
    public WatermarksWithIdleness(WatermarkGenerator<T> watermarks, Duration idleTimeout) {
        this(watermarks, idleTimeout, SystemClock.getInstance());
    }

    WatermarksWithIdleness(WatermarkGenerator<T> watermarks, Duration idleTimeout, Clock clock) {
        this.watermarks = watermarks;
        this.idlenessTimer = new IdlenessTimer(clock, idleTimeout);
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        watermarks.onEvent(event, eventTimestamp, output);
        idlenessTimer.activity();
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (idlenessTimer.checkIfIdle()) {
            output.markIdle();
        } else {
            watermarks.onPeriodicEmit(output);
        }
    }

    static final class IdlenessTimer {

        /**
         * The clock used to measure elapsed time.
         */
        private final Clock clock;

        /**
         * Counter to detect change. No problem if it overflows.
         */
        private long counter;

        /**
         * The value of the counter at the last activity check.
         */
        private long lastCounter;

        /**
         * The first time (relative to {@link Clock#relativeTimeNanos()}) when the activity
         * check found that no activity happened since the last check.
         * Special value: 0 = no timer.
         */
        private long startOfInactivityNanos;

        /**
         * The duration before the output is marked as idle.
         */
        private final long maxIdleTimeNanos;

        IdlenessTimer(Clock clock, Duration idleTimeout) {
            this.clock = clock;

            long idleNanos;
            try {
                idleNanos = idleTimeout.toNanos();
            } catch (ArithmeticException ignored) {
                idleNanos = Long.MAX_VALUE;
            }

            this.maxIdleTimeNanos = idleNanos;
        }

        public void activity() {
            counter++;
        }

        public boolean checkIfIdle() {
            // If the counter has moved it is not idle
            // set the last check and restart inactivity
            if (counter != lastCounter) {
                lastCounter = counter;
                startOfInactivityNanos = 0L;
                return false;
            } else {
                // It has not been set yet set
                if (startOfInactivityNanos == 0L) {
                    startOfInactivityNanos = clock.relativeTimeNanos();
                    return false;
                } else {
                    // Or it has been inactive so check against allowed non-activeness
                    return clock.relativeTimeNanos() - startOfInactivityNanos > maxIdleTimeNanos;
                }
            }
        }
    }
}

