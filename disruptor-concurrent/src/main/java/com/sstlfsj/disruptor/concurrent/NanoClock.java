package com.sstlfsj.disruptor.concurrent;

/** 可注入的单调纳秒时钟。 */
@FunctionalInterface
public interface NanoClock {

    NanoClock SYSTEM = System::nanoTime;

    long nanoTime();

    static NanoClock system() {
        return SYSTEM;
    }
}
