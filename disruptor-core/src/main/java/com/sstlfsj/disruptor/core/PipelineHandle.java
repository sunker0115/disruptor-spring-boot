package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 一条已构建管道的发布与观察入口，不拥有生命周期。
 *
 * @param <E> 事件类型
 */
public interface PipelineHandle<E> {

    String name();

    Class<E> eventType();

    /** 返回所属管道当前的统一事实快照。 */
    PipelineSnapshot snapshot();

    /**
     * 受管非阻塞发布。RingBuffer 无容量时立即返回
     * {@link PublicationResult#CAPACITY_EXHAUSTED}。
     *
     * <p>translator 只在成功取得容量后执行；其异常按 LMAX 原语义透传。</p>
     */
    PublicationResult tryPublishEvent(EventTranslator<E> translator);

    /**
     * 受管有界、可中断发布。超时时间从调用时起算；零超时仍执行一次
     * 立即尝试，无容量则返回 {@link PublicationResult#TIMED_OUT}。
     *
     * <p>translator 只在成功取得容量后执行；其异常按 LMAX 原语义透传。</p>
     *
     * @throws IllegalArgumentException timeout 为负数
     * @throws InterruptedException 等待容量时线程被中断
     */
    PublicationResult publishEvent(EventTranslator<E> translator, Duration timeout)
            throws InterruptedException;

    <A> PublicationResult tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0);

    <A> PublicationResult publishEvent(
            EventTranslatorOneArg<E, A> translator, A arg0, Duration timeout)
            throws InterruptedException;

    <A, B> PublicationResult tryPublishEvent(
            EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1);

    <A, B> PublicationResult publishEvent(
            EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1, Duration timeout)
            throws InterruptedException;

    <A, B, C> PublicationResult tryPublishEvent(
            EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2);

    <A, B, C> PublicationResult publishEvent(
            EventTranslatorThreeArg<E, A, B, C> translator,
            A arg0, B arg1, C arg2, Duration timeout) throws InterruptedException;

    /**
     * 原生零代理逃生口。调用方必须在 Runtime 关闭前自行停止所有通过该对象发布事件的生产者；
     * 与关闭并发的原生发布不享受受管发布的准入和排空保证。
     */
    RingBuffer<E> unsafeRingBuffer();

    /**
     * 便捷有界发布：用 {@code filler} 原地填充预分配事件。
     *
     * <p>注意：{@code filler} 会被包装为一次性 {@code EventTranslator} lambda，每次调用产生一次
     * 该 lambda 的分配。追求零分配的极致低延迟路径请使用静态
     * {@code EventTranslator}。
     */
    default PublicationResult publish(Consumer<E> filler, Duration timeout)
            throws InterruptedException {
        Objects.requireNonNull(filler, "filler 不能为空");
        return publishEvent((event, sequence) -> filler.accept(event), timeout);
    }

    /**
     * 便捷非阻塞发布（背压入口）。分配权衡同 {@link #publish(Consumer, Duration)}。
     */
    default PublicationResult tryPublish(Consumer<E> filler) {
        Objects.requireNonNull(filler, "filler 不能为空");
        return tryPublishEvent((event, sequence) -> filler.accept(event));
    }

    /** 剩余可写槽位数；{@code bufferSize - remaining()} 为近似积压量。 */
    default long remaining() {
        return unsafeRingBuffer().remainingCapacity();
    }
}
