# Commons 能力事实矩阵

本文固定比较 `/Users/sunke/dev/ai-project/commons` 的 commit `5c831c06`，范围是 Java `Disruptor` 与 `Commons-Concurrent` 两个模块。结论描述业务与运行时能力，不承诺 `cn.wjybxx.*` 二进制、源码或自研 Future API 兼容。

分类只有四种：

- **覆盖**：本项目存在直接对应的运行时能力；
- **增强**：覆盖场景，并修正或扩展参考行为；
- **项目级替代**：需求由本项目既有边界或 JDK/Spring 标准能力承担，不复制参考抽象；
- **不复制**：能力不属于本项目最终边界，明确不是待补缺口。

## Disruptor 模块

| 参考能力 | Commons 源码证据 | 本项目落点与证据 | 判定 |
| --- | --- | --- | --- |
| 单/多生产者有界 RingBuffer、批量 claim/publish | `ProducerBarrier`、`RingBufferSequencer`、`SingleProducerSequencer`、`MultiProducerSequencer` | `disruptor-core` 直接暴露 LMAX 4.0 原生 topology 与 `unsafeRingBuffer()`；`NativeCapabilitiesTest` 覆盖 translator、processor、rewind 等原生能力 | 覆盖；使用上游 API，不复制 fork 类型 |
| 消费屏障、依赖序列、alert | `ConsumerBarrier`、`SingleConsumerBarrier`、`MultiConsumerBarrier` | LMAX `SequenceBarrier`、原生 `EventProcessor` 与 topology；`DisruptorPipelineTest`、`NativeCapabilitiesTest` | 项目级替代 |
| Blocking、BusySpin、Sleeping、Yielding 与 timeout wait | 同名 `WaitStrategy` 实现、`SequenceBlocker` | core 的 wait-strategy 工厂保留 LMAX 策略；构造参数型策略由 `PipelineSpec.waitStrategy(...)` 提供 | 覆盖；不复制 `SequenceBlocker` |
| 有界事件拓扑、用户事件和原始 sequence | `EventSequencer`、`RingBufferEventSequencer`、`EventHandler` | `PipelineSpec.topology` 接收原生 `Disruptor<E>`，运行时不包装业务 handler | 覆盖 |
| 分段 MPSC 无界 event sequencer | `MpUnboundedBuffer`、`MpUnboundedBufferSequencer`、`MpUnboundedEventSequencer` | `UnboundedTaskQueue` 作为 EventLoop 任务后端，与 bounded 后端共享唯一 kernel；分段 typed 槽 + 绝对 publication 代际 + CAS 段链 + 单消费者 head 回收 + 固定容量原子池，超 `maxPooledSegments` 才释放 GC；scanner 仅在 kernel 静默期读取；`TaskQueueContractTest`、`UnboundedEventLoopTest` 覆盖段回收、代际、MPSC 扩段、双 scanner 与 shutdown 交错 | 项目级覆盖；不公开通用无界事件总线 |
| 生产者/消费者屏障作为独立低层扩展 API | `ProducerBarrier`、`ConsumerBarrier`、`Sequencer` | core 保留 LMAX 原生 `RingBuffer`、barrier 与自定义 processor 逃生口；concurrent 只公开任务执行边界 | 项目级替代；不兼容 fork API |

## Commons-Concurrent 模块

| 参考能力或行为 | Commons 源码证据 | 本项目落点与验证证据 | 判定 |
| --- | --- | --- | --- |
| EventLoop、JDK Executor/Scheduler | `IEventLoop`、`AbstractEventLoop`、`DisruptorEventLoop` | `EventLoop`、`SupervisedScheduledExecutor`；`EventLoopExecutorContractTest`、`EventLoopSchedulingTest` | 覆盖，并增加监督终止 |
| 固定 EventLoopGroup、chooser、affinity key | `DefaultEventLoopGroup` 构造 child 及 `select()/select(int)` | `DisruptorEventLoopGroup.next/select`；`EventLoopGroupTest` | 覆盖；child owner 更严格 |
| one-shot、fixed-rate、fixed-delay | `ScheduledTaskBuilder.setOnlyOnce/setFixedRate/setFixedDelay` | JDK 方法与 `ScheduledTaskSpec`；`EventLoopSchedulingTest` | 覆盖 |
| dynamic-delay | `ScheduledTaskBuilder.SCHEDULE_DYNAMIC_DELAY` 是 private，builder 无入口；`ScheduledPromiseTask.setNextRunTime` 仅区分 fixed-rate/其它 | `ScheduleMode.DYNAMIC_DELAY` + `DynamicDelay`；`dynamicDelayReceivesLastRunAndFailureCanContinueUntilCountLimit` | 增强；参考 Java 运行时没有独立动态计算路径 |
| priority | `ScheduledTaskBuilder.priority` 可设置，但 `AbstractEventLoop.schedule(builder)` 未复制到任务，`ScheduledPromiseTask.compareToExplicitly` 不读取 | timer 排序 `trigger → priority → acceptedSequence`；`sameTriggerUsesPriorityThenAcceptedSequence` | 增强 |
| timeout、次数限制、周期异常继续 | `setTimeout`、`setCountLimit`、`TaskOptions.CAUGHT_EXCEPTION` | `expiresAfter`、`maxExecutions`、`continueOnFailure`；`EventLoopSchedulingTest` | 覆盖；不用位标志 |
| CancelToken、原因、监听、解注册 | `CancelTokenSource`、`ICancelToken`、`IRegistration` | `CancellationSource/Token/Registration`；`CancellationSourceTest` | 覆盖，并明确晚注册、线程与物理解注册 |
| `cancelAfter` 默认调度器 | `CancelTokenSource.cancelAfter` 使用字段 `delayer`，也有显式 executor 重载 | `CancellationSource.cancelAfter(reason, delay, scheduler)` 强制显式 scheduler | 项目级替代；不保留隐藏全局状态 |
| task context | `TaskBuilder` 的 `ctx` 与 Function/Consumer 重载 | 不可变 typed `TaskContext` + `ContextCallable`；`TaskContextTest`、示例 `OrderEventLoopService` | 覆盖；不依赖 ThreadLocal |
| 同 loop 阻塞保护 | `AbstractEventLoop.throwIfInEventLoop` 用于 await/invoke | Future/get、await、invoke、close guard；`EventLoopBlockingGuardTest` | 覆盖 |
| 首次提交 lazy start | `DisruptorEventLoop.publishTask` 在 `ST_UNSTARTED` 调 `ensureThreadStarted()` | 只有显式 `start()` 完成后才开放 gate；Spring 聚合器统一启动 | 不复制；避免 module 未就绪时 accepted |
| parent 引用 | `AbstractEventLoop.parent` | `EventLoop.parent()`；`EventLoopGroupTest` | 覆盖 |
| EventLoopFactory | `EventLoopGroupBuilder.eventLoopFactory`、`DefaultEventLoopGroup.newChild` | `EventLoopFactory(parent, childIndex)`；`EventLoopGroupTest` | 覆盖 |
| `TaskOptions.LOCAL_ORDER` 同 loop 绕入口 | `DisruptorEventLoop.execute` 直接交给 scheduler helper | 所有任务统一 accepted sequence；`zeroDelayScheduleAcceptedBeforeExecuteAlwaysRunsFirst` | 不复制；绕入口会破坏全序与 bounded 容量口径 |
| IEventLoopAgent 主循环阶段 | `IEventLoopAgent.checkMainLoop/beforeMainLoop/afterMainLoop/customUpdate` | module start/stop + opportunistic update；固定 tick 用显式周期调度 | 项目级替代；不宣称 tick 等价 |
| early/update/late module phases | `AbstractEventLoop` 三份 module 列表、`DisruptorEventLoop.updateModules` | 在一个业务调度任务内部显式编排阶段 | 项目级替代 |
| ComponentId、依赖解析、数组索引 | `IEventLoopModule.GLOBAL`、`AbstractEventLoop.indexedModuleList/getComponent` | Spring/应用 DI 与业务自己的索引 | 不复制通用组件框架 |
| module 运行时不可增删 | `AbstractEventLoop` 构造时 `List.copyOf` | builder 构建时冻结 module 列表；`EventLoopModuleTest`、`ModuleLifecycleTest` | 覆盖 |
| 用户事件、handler、原始 sequence | `IAgentEvent`、`IAgentEventHandler`、`DisruptorEventLoop.Worker` | `disruptor-core` 原生 topology 与 `unsafeRingBuffer()` | 项目级覆盖；不把业务事件塞入 Runnable 槽 |
| 自研 IFuture/IPromise/ICompletionStage | `IFuture`、`IPromise`、`Promise` | JDK `Future`、`ScheduledFuture`、`CompletionStage`、`CompletableFuture` | 项目级替代；不兼容 API |
| Future 只读视图、转发与组合器 | `IFuture.asReadonly`、`Promise` | 只读 termination stage 与 JDK CompletionStage 组合 | 项目级替代 |
| 立即执行器、Executor 适配 | `ImmediateExecutor`、`ExecutorServiceAdapter` | `Runnable::run` 与 JDK `ExecutorService` | 标准 API 替代 |
| stackless cancel/timeout、FutureLogger | `BetterCancellationException`、`FutureLogger` | 标准 `CancellationException`、显式 `CancellationReason` 与 SLF4J handler | 项目级替代；不复制异常类型 |
| 有界 RingBuffer 与无界 event sequencer | `RingBufferEventSequencer`、`MpUnboundedEventSequencer` | `BoundedTaskQueue`/`UnboundedTaskQueue` 两后端、唯一 `EventLoopKernel`；`EventLoopBackendContractTest` | 覆盖运行场景 |
| `shutdownNow()` 固定返回空列表 | `DisruptorEventLoop.shutdownNow` 第 394–398 行 | 冻结 `claimedCursor` 后 `queue.scanOrdinaryUnstarted` 扫描 ordinary 槽 + `registry.scanForShutdown` 扫描 tracked index，按 ticket 升序 CAS 返回未开始原始 Runnable，running 不返回并 `cancel(true)`；`EventLoopShutdownTest` | 增强，符合 JDK 契约 |
| 自定义拒绝策略 | `RejectedExecutionHandlers` | `RejectedExecutionException` 与非抛出 `tryExecute` | 有意收窄；不提供 CallerRuns 或静默丢弃 |
| 任务对象池 | `ScheduledPromiseTask.POOL` | 无需对象池：预分配 typed ring 槽即任务记录，ordinary 热路径零堆分配（`docs/disruptor-concurrent-verification.md` GC 实测 bounded 0.004、unbounded 1.607 B/op）；Future/记录/index 仅用于 `submit/schedule` | 不复制原池化实现；池化动机已由槽位原生重写消解，且保留统一入口与关闭/所有权契约 |
| WatcherMgr | `WatcherMgr`、`SimpleWatcherMgr` | module 内集合、取消监听或 JDK Flow | 项目级替代 |
| GlobalEventLoop | `GlobalEventLoop` | Spring Bean 或应用显式 owner | 不复制隐藏单例 |
| Group 失败收敛与共享 deadline | `DefaultEventLoopGroup` 聚合 child termination，但没有组级首因/deadline 协议 | `GroupLifecycleCoordinator`；`EventLoopGroupShutdownTest` 覆盖 child fail-stop、deadline 身份和真实终止 | 增强 |
| 启动回滚与精确终止事实 | `runningFuture/terminationFuture`，module stop 中异常主要记录日志 | `WorkerSupervisor`、module 逆序回滚、suppressed failure、注册封口和真实线程退出；`WorkerSupervisorTest`、`ModuleLifecycleTest` | 增强 |
| Spring 生命周期、健康和指标 | 参考模块不负责 Spring Boot | 三个独立条件自动配置；`DisruptorConcurrentAutoConfigurationTest` 及 lifecycle/health/metrics 测试 | 本项目增强 |

## 结论

本项目完整承担两个参考模块在当前工程中的目标场景：原生事件拓扑由 `disruptor-core` 保留 LMAX 4.0 能力，单线程任务执行与调度由 `disruptor-concurrent` 承担。dynamic-delay、priority、`shutdownNow` 返回值、Group fail-stop、共享 deadline、精确启动回滚和 Spring 运维集成强于参考实现。该结论只针对能力与契约。性能上，槽位原生重写后 ordinary 热路径已实测零堆分配（bounded 0.004、unbounded 1.607 B/op，与 Commons 同级）；本轮又移除 unbounded 数据面的显式段锁。本机 JDK 21 同参数两轮复测：unbounded 相对自身 bounded 约 82%（差距约 18%，上一轮为 78%/21.7%），相对 Commons 仍约 64%，未达 80% 门槛；MPSC 2/4/8 producer 下 unbounded/bounded 为 89%–105%，无结构性退化。单线程残余差距是跨段绝对序列账本与逐槽所有权契约的固有成本。功能覆盖不表示吞吐持平。

未复制项集中在 Commons 自身生态抽象：自研 Future API、ComponentId/Agent phases、WatcherMgr、LOCAL_ORDER 快路、任务池和 GlobalEventLoop。它们分别由 JDK/Spring/业务显式编排替代，或因破坏本项目全序、容量和所有权不变量而明确排除。因此“完整覆盖”指场景能力闭合，不表示包名、类型或调用点兼容。
