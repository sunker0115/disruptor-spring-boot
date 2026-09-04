package com.sstlfsj.disruptor.concurrent;

import com.sstlfsj.disruptor.core.ShutdownDeadline;
import com.sstlfsj.disruptor.core.ShutdownMode;
import com.sstlfsj.disruptor.core.SupervisedLifecycle;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLoopGroupTest {

    @Test
    void selectionIsStableRoundRobinAndChildrenExposeTheirSingleParent() {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("workers", 3, 8)
                .build();
        List<EventLoop> children = children(group);

        for (int key : List.of(Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE)) {
            EventLoop expected = children.get(Math.floorMod(key, children.size()));
            assertSame(expected, group.select(key));
            assertSame(expected, group.select(key));
        }
        assertSame(children.get(0), group.next());
        assertSame(children.get(1), group.next());
        assertSame(children.get(2), group.next());
        assertSame(children.get(0), group.next());
        children.forEach(child -> assertSame(group, child.parent()));
        Iterator<EventLoop> iterator = group.iterator();
        iterator.next();
        assertThrows(UnsupportedOperationException.class, iterator::remove);

        group.shutdownNow();
    }

    @Test
    void groupGateRejectsChildrenBeforeAllModulesStartAndAfterShutdownBegins()
            throws Exception {
        CountDownLatch secondModuleEntered = new CountDownLatch(1);
        CountDownLatch releaseSecondModule = new CountDownLatch(1);
        DisruptorEventLoopGroup group = EventLoopGroupBuilder.builder(
                        "gated",
                        2,
                        (parent, childIndex) -> {
                            EventLoopBuilder<DisruptorEventLoop> builder =
                                    EventLoopBuilder.bounded("gated-" + childIndex, 8);
                            if (childIndex == 1) {
                                builder.module(new EventLoopModule() {
                                    @Override
                                    public void onStart(EventLoop loop) throws Exception {
                                        secondModuleEntered.countDown();
                                        releaseSecondModule.await();
                                    }
                                });
                            }
                            return builder.build();
                        })
                .build();
        EventLoop child = group.select(0);

        assertFalse(group.snapshot().acceptingTasks());
        assertFalse(child.tryExecute(() -> { }));
        CompletableFuture<Void> startup = group.start().toCompletableFuture();
        assertTrue(secondModuleEntered.await(2, TimeUnit.SECONDS));
        assertFalse(startup.isDone());
        assertFalse(child.tryExecute(() -> { }));
        assertFalse(child.snapshot().acceptingTasks());
        releaseSecondModule.countDown();
        startup.get(2, TimeUnit.SECONDS);

        assertEquals(SupervisedLifecycle.RUNNING, group.snapshot().lifecycle());
        CountDownLatch executed = new CountDownLatch(1);
        assertTrue(child.tryExecute(executed::countDown));
        assertTrue(executed.await(2, TimeUnit.SECONDS));
        group.shutdown();
        assertFalse(child.tryExecute(() -> { }));
        assertFalse(child.snapshot().acceptingTasks());
        assertThrows(RejectedExecutionException.class, () -> child.execute(() -> { }));
        group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void childLifecycleCanOnlyBeDrivenByItsOwnerGroup() throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("ownership", 2, 8)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        EventLoop child = group.select(1);

        assertThrows(ChildLifecycleOwnershipException.class, child::start);
        assertThrows(ChildLifecycleOwnershipException.class, child::shutdown);
        assertThrows(ChildLifecycleOwnershipException.class, child::shutdownNow);
        assertThrows(ChildLifecycleOwnershipException.class,
                () -> child.requestShutdown(
                        ShutdownMode.IMMEDIATE,
                        ShutdownDeadline.after(Duration.ofSeconds(1))));
        assertThrows(ChildLifecycleOwnershipException.class, child::close);
        assertEquals(SupervisedLifecycle.RUNNING, group.snapshot().lifecycle());
        assertTrue(group.snapshot().acceptingTasks());

        group.shutdown();
        group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void groupExecutorMethodsChooseAChildAtSubmissionTime() throws Exception {
        DisruptorEventLoopGroup group = EventLoopGroupBuilder
                .bounded("executor-group", 2, 8)
                .build();
        group.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
        List<String> threads = new ArrayList<>();

        group.submit(() -> threads.add(Thread.currentThread().getName()))
                .get(2, TimeUnit.SECONDS);
        group.schedule(() -> threads.add(Thread.currentThread().getName()),
                        0, TimeUnit.NANOSECONDS)
                .get(2, TimeUnit.SECONDS);

        assertEquals(2, threads.size());
        assertTrue(threads.get(0).contains("-0-worker"));
        assertTrue(threads.get(1).contains("-1-worker"));
        group.shutdown();
        group.termination().toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    @Test
    void factoryReceivesStableParentAndIndexAndUnboundedBuilderKeepsOneKernelModel() {
        List<EventLoopGroup> parents = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();
        DisruptorEventLoopGroup custom = EventLoopGroupBuilder.builder(
                        "factory-contract",
                        3,
                        (parent, childIndex) -> {
                            parents.add(parent);
                            indexes.add(childIndex);
                            return EventLoopBuilder
                                    .unbounded("factory-contract-" + childIndex, 8)
                                    .build();
                        })
                .build();

        assertEquals(List.of(custom, custom, custom), parents);
        assertEquals(List.of(0, 1, 2), indexes);
        assertTrue(children(custom).stream().allMatch(
                child -> child.snapshot().capacityMode() == CapacityMode.UNBOUNDED));
        custom.shutdownNow();

        DisruptorEventLoopGroup unbounded = EventLoopGroupBuilder
                .unbounded("unbounded-group", 2, 8)
                .build();
        assertTrue(children(unbounded).stream().allMatch(
                child -> child.snapshot().capacityMode() == CapacityMode.UNBOUNDED));
        unbounded.shutdownNow();
    }

    private static List<EventLoop> children(EventLoopGroup group) {
        List<EventLoop> children = new ArrayList<>();
        group.forEach(children::add);
        return List.copyOf(children);
    }
}
