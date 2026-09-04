package com.sstlfsj.disruptor.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScheduledTaskSpecTest {

    private static final ContextCallable<String> TASK = context -> "done";

    @Test
    void defaultsToOneShotWithEmptyContextAndImmediateTrigger() {
        ScheduledTaskSpec<String> spec = ScheduledTaskSpec.<String>builder()
                .task(TASK)
                .build();

        assertEquals(ScheduleMode.ONE_SHOT, spec.scheduleMode());
        assertEquals(Duration.ZERO, spec.triggerAfter());
        assertEquals(TaskContext.empty(), spec.context());
        assertNull(spec.period());
        assertNull(spec.dynamicDelay());
    }

    @Test
    void acceptsEachSchedulingModeOnlyWithItsOwnConfiguration() {
        assertEquals(ScheduleMode.ONE_SHOT, spec(ScheduleMode.ONE_SHOT, null, null).scheduleMode());
        assertEquals(ScheduleMode.FIXED_RATE,
                spec(ScheduleMode.FIXED_RATE, Duration.ofMillis(1), null).scheduleMode());
        assertEquals(ScheduleMode.FIXED_DELAY,
                spec(ScheduleMode.FIXED_DELAY, Duration.ofMillis(1), null).scheduleMode());
        assertEquals(ScheduleMode.DYNAMIC_DELAY,
                spec(ScheduleMode.DYNAMIC_DELAY, null, lastRun -> Duration.ofMillis(1)).scheduleMode());

        assertThrows(IllegalArgumentException.class,
                () -> spec(ScheduleMode.ONE_SHOT, Duration.ofMillis(1), null));
        assertThrows(IllegalArgumentException.class,
                () -> spec(ScheduleMode.ONE_SHOT, null, lastRun -> Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> spec(ScheduleMode.FIXED_RATE, Duration.ofMillis(1), lastRun -> Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> spec(ScheduleMode.DYNAMIC_DELAY, Duration.ofMillis(1), lastRun -> Duration.ZERO));
        assertThrows(NullPointerException.class,
                () -> spec(ScheduleMode.DYNAMIC_DELAY, null, null));
    }

    @Test
    void rejectsInvalidDurationsAndExecutionLimit() {
        assertThrows(IllegalArgumentException.class, () -> ScheduledTaskSpec.<String>builder()
                .task(TASK).triggerAfter(Duration.ofNanos(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> ScheduledTaskSpec.<String>builder()
                .task(TASK).expiresAfter(Duration.ofNanos(-1)).build());
        assertThrows(IllegalArgumentException.class,
                () -> spec(ScheduleMode.FIXED_RATE, Duration.ZERO, null));
        assertThrows(IllegalArgumentException.class,
                () -> spec(ScheduleMode.FIXED_DELAY, Duration.ofNanos(-1), null));
        assertThrows(IllegalArgumentException.class, () -> ScheduledTaskSpec.<String>builder()
                .task(TASK).maxExecutions(0).build());
        assertThrows(IllegalArgumentException.class, () -> ScheduledTaskSpec.<String>builder()
                .task(TASK).maxExecutions(-1).build());
    }

    private static ScheduledTaskSpec<String> spec(
            ScheduleMode mode,
            Duration period,
            DynamicDelay dynamicDelay) {
        return ScheduledTaskSpec.<String>builder()
                .task(TASK)
                .scheduleMode(mode)
                .period(period)
                .dynamicDelay(dynamicDelay)
                .build();
    }
}
