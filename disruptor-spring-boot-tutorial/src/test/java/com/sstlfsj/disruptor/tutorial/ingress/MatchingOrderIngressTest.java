package com.sstlfsj.disruptor.tutorial.ingress;

import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.autoconfigure.DisruptorLifecycle;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineSpec;
import com.sstlfsj.disruptor.tutorial.dto.PlaceOrderRequest;
import com.sstlfsj.disruptor.tutorial.match.Side;
import com.sstlfsj.disruptor.tutorial.pipeline.OrderEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class MatchingOrderIngressTest {

    @Test
    void serializesConcurrentCallersAndStopsBeforeRuntime() throws Exception {
        int orderCount = 16;
        CountDownLatch consumed = new CountDownLatch(orderCount);
        Set<Long> consumedOrderIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
        PipelineSpec<OrderEvent> spec = PipelineSpec.builder(
                        "matching", OrderEvent.class, OrderEvent::new)
                .bufferSize(64)
                .producerType(ProducerType.SINGLE)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                    consumedOrderIds.add(event.getOrderId());
                    consumed.countDown();
                }))
                .build();
        DisruptorRuntime runtime = DisruptorRuntime.builder().add(spec).build();
        DisruptorLifecycle runtimeLifecycle = new DisruptorLifecycle(runtime, -100);
        MatchingOrderIngress ingress = new MatchingOrderIngress(runtime, runtimeLifecycle);
        PlaceOrderRequest request = new PlaceOrderRequest(
                "TEST", Side.BUY, new BigDecimal("100"), BigDecimal.ONE);

        runtimeLifecycle.start();
        ingress.start();
        assertThat(ingress.getPhase()).isEqualTo(-99);

        ExecutorService callers = Executors.newFixedThreadPool(orderCount);
        List<Future<OptionalLong>> futures = new ArrayList<>();
        try {
            for (int index = 0; index < orderCount; index++) {
                futures.add(callers.submit(() -> ingress.tryPublish(request)));
            }

            Set<Long> acceptedOrderIds = new HashSet<>();
            for (Future<OptionalLong> future : futures) {
                OptionalLong result = future.get(2, TimeUnit.SECONDS);
                assertThat(result).isPresent();
                acceptedOrderIds.add(result.getAsLong());
            }
            assertThat(acceptedOrderIds).hasSize(orderCount);
            assertThat(consumed.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(consumedOrderIds).containsExactlyInAnyOrderElementsOf(acceptedOrderIds);
        } finally {
            callers.shutdownNow();
            assertThat(callers.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }

        AtomicBoolean stopped = new AtomicBoolean();
        ingress.stop(() -> stopped.set(true));

        assertThat(stopped).isTrue();
        assertThat(ingress.isRunning()).isFalse();
        assertThat(ingress.tryPublish(request)).isEmpty();

        runtimeLifecycle.stop();
        assertThat(runtime.isRunning()).isFalse();
    }
}
