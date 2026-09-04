package com.sstlfsj.disruptor.concurrent;

/** 任务的唯一调度模式。 */
public enum ScheduleMode {
    ONE_SHOT,
    FIXED_RATE,
    FIXED_DELAY,
    DYNAMIC_DELAY;

    public boolean isPeriodic() {
        return this != ONE_SHOT;
    }
}
