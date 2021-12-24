package com.perkss.kafka.extension.watermark.generator;

import com.perkss.kafka.extension.watermark.clock.ManualClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarksWithIdlenessTest {

    @Test
    void initiallyActive() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final WatermarksWithIdleness.IdlenessTimer timer = new WatermarksWithIdleness.IdlenessTimer(clock, Duration.ofMillis(10));

        assertFalse(timer.checkIfIdle());
    }

    @Test
    void idleWithoutEvents() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final WatermarksWithIdleness.IdlenessTimer timer = new WatermarksWithIdleness.IdlenessTimer(clock, Duration.ofMillis(10));
        timer.checkIfIdle(); // start timer

        clock.advanceTime(Duration.ofMillis(11));
        assertTrue(timer.checkIfIdle());
    }

    @Test
    void repeatedIdleChecks() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final WatermarksWithIdleness.IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        assertTrue(timer.checkIfIdle());
        clock.advanceTime(Duration.ofMillis(100));
        assertTrue(timer.checkIfIdle());
    }

    @Test
    void activeAfterIdleness() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final WatermarksWithIdleness.IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(10));

        timer.activity();
        assertFalse(timer.checkIfIdle());
    }

    @Test
    void idleActiveIdle() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final WatermarksWithIdleness.IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(152));

        timer.activity();
        assertFalse(timer.checkIfIdle());

        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(153));
        assertTrue(timer.checkIfIdle());
    }

    private static WatermarksWithIdleness.IdlenessTimer createTimerAndMakeIdle(ManualClock clock, Duration idleTimeout) {
        final WatermarksWithIdleness.IdlenessTimer timer = new WatermarksWithIdleness.IdlenessTimer(clock, idleTimeout);

        timer.checkIfIdle();
        clock.advanceTime(Duration.ofMillis(idleTimeout.toMillis() + 1));
        assertTrue(timer.checkIfIdle());

        return timer;
    }

}