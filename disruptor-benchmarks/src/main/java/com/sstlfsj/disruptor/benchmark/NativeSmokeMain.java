package com.sstlfsj.disruptor.benchmark;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineSpec;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** GraalVM native-image 冒烟入口。 */
public final class NativeSmokeMain {

    private static final EventTranslatorOneArg<SmokeEvent, Long> TRANSLATOR =
            (event, sequence, value) -> event.value = value;

    private NativeSmokeMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch consumed = new CountDownLatch(1);
        PipelineSpec<SmokeEvent> spec = PipelineSpec.builder(
                        "native-smoke", SmokeEvent.class, SmokeEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                    if (event.value == 42L) {
                        consumed.countDown();
                    }
                }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();

        runtime.start();
        try {
            runtime.require("native-smoke", SmokeEvent.class).publishEvent(TRANSLATOR, 42L);
            if (!consumed.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("native smoke 消费超时");
            }
        } finally {
            runtime.shutdown();
        }
    }

    public static final class SmokeEvent {
        private long value;
    }
}
