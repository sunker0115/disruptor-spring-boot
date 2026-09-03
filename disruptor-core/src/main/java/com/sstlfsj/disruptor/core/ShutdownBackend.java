package com.sstlfsj.disruptor.core;

/**
 * 队列后端映射到统一受监督关闭状态机的内部非阻塞阶段协议。
 *
 * <p>所有方法只由 {@link WorkerSupervisor} 的命名虚拟控制线程调用，严格串行且不持有 supervisor
 * 状态锁。实现必须有界且非阻塞：不得等待 sequence 或 {@link java.util.concurrent.Future}，不得
 * {@link Thread#join()} worker，不得调用业务 handler、module 或其它用户代码。</p>
 *
 * <p>{@link #beginQuiesce()} 至多调用一次；{@link #isDrained()} 只在
 * {@link PipelineLifecycle#QUIESCING} 期间调用，可以被重复探测；
 * {@link #stop(ShutdownMode)} 对 {@link ShutdownMode#GRACEFUL} 和
 * {@link ShutdownMode#IMMEDIATE} 各至多调用一次。graceful stop 后若 worker 未在 deadline 前退出，
 * supervisor 可以继续串行调用一次 immediate stop。任一阶段抛出的首个异常会成为 supervisor 首因，
 * 并把关闭模式单调升级为 immediate。</p>
 */
public interface ShutdownBackend {

    /** 开始拒绝新工作并进入排空准备；不得执行排空等待。 */
    void beginQuiesce() throws Throwable;

    /** 执行一次非阻塞排空探测。 */
    boolean isDrained() throws Throwable;

    /** 应用指定停止模式；不得等待 worker 终止。 */
    void stop(ShutdownMode mode) throws Throwable;
}
