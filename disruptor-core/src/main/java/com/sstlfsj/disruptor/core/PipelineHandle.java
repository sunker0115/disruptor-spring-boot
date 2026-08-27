package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 一条已构建管道的发布入口。生命周期由 {@link DisruptorRuntime} 独占管理。
 *
 * @param <E> 事件类型
 */
public interface PipelineHandle<E> {

    String name();

    Class<E> eventType();

    /**
     * 受管阻塞发布。关闭开始后拒绝新事件，已经进入的发布会被本次优雅关闭纳入排空边界。
     */
    void publishEvent(EventTranslator<E> translator);

    /** 受管非阻塞发布；仅在管道运行且 RingBuffer 有容量时返回 {@code true}。 */
    boolean tryPublishEvent(EventTranslator<E> translator);

    <A> void publishEvent(EventTranslatorOneArg<E, A> translator, A arg0);

    <A> boolean tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0);

    <A, B> void publishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1);

    <A, B> boolean tryPublishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1);

    <A, B, C> void publishEvent(EventTranslatorThreeArg<E, A, B, C> translator,
                                A arg0, B arg1, C arg2);

    <A, B, C> boolean tryPublishEvent(EventTranslatorThreeArg<E, A, B, C> translator,
                                     A arg0, B arg1, C arg2);

    /**
     * 原生零代理逃生口。调用方必须在 Runtime 关闭前自行停止所有通过该对象发布事件的生产者；
     * 与关闭并发的原生发布不享受受管发布的准入和排空保证。
     */
    RingBuffer<E> unsafeRingBuffer();

    /**
     * 便捷阻塞发布：领取下一个槽位、用 {@code filler} 原地填充预分配事件后发布；
     * RingBuffer 满时阻塞直到有空位。
     *
     * <p>注意：{@code filler} 会被包装为一次性 {@code EventTranslator} lambda，每次调用产生一次
     * 该 lambda 的分配。追求零分配的极致低延迟路径请使用静态
     * {@code EventTranslator}。
     */
    default void publish(Consumer<E> filler) {
        Objects.requireNonNull(filler, "filler 不能为空");
        publishEvent((event, sequence) -> filler.accept(event));
    }

    /**
     * 便捷非阻塞发布（背压入口）：RingBuffer 有空位时填充并发布返回 {@code true}；
     * 满时立即返回 {@code false}，不阻塞。分配权衡同 {@link #publish(Consumer)}。
     */
    default boolean tryPublish(Consumer<E> filler) {
        Objects.requireNonNull(filler, "filler 不能为空");
        return tryPublishEvent((event, sequence) -> filler.accept(event));
    }

    /** 剩余可写槽位数；{@code bufferSize - remaining()} 为近似积压量。 */
    default long remaining() {
        return unsafeRingBuffer().remainingCapacity();
    }
}
