# disruptor-concurrent 设计

## 定位与边界

`disruptor-concurrent` 是本项目最终架构中的单线程执行与调度模块。它建立在 `disruptor-core` 的监督生命周期之上，提供有界和显式无界的 `EventLoop`、固定 `EventLoopGroup`、JDK 21 `ScheduledExecutorService` 契约、高级调度、协作取消、显式任务上下文以及 Spring Boot 运维集成。

它以 `/Users/sunke/dev/ai-project/commons/java/Commons-Concurrent` 和 `/Users/sunke/dev/ai-project/commons/java/Disruptor` 的 Java 源码为能力参照，但不复制其包名、类型体系、位字段任务选项、组件框架或自研 Future/Promise。项目不承诺二进制兼容、源码兼容或逐类型 API 兼容；承诺的是本文件明确列出的能力覆盖、项目级替代和增强。

最终依赖方向如下：

```text
LMAX Disruptor 4.0 ───────────────┐
                                  ▼
                           disruptor-core
                                  │
                 ┌────────────────┴───────────────┐
                 ▼                                ▼
       disruptor-concurrent          disruptor-spring-boot-autoconfigure
                 │                                ▲
                 └──────── optional ──────────────┘
                                                  │
                                                  ▼
                                  disruptor-spring-boot-starter
```

`disruptor-concurrent` 直接依赖 LMAX，因为有界 `TaskQueue` 自身使用 LMAX RingBuffer；不能只依赖 core 的传递依赖。它同时直接依赖 `disruptor-core`、SLF4J，并将 Lombok 声明为 optional。模块不依赖 Spring。

现有 starter 不传递 concurrent。应用显式添加 `disruptor-concurrent` 并声明根 `EventLoop` 或 `EventLoopGroup` Bean 后，现有 autoconfigure 模块才条件装配生命周期、健康和指标。

## 设计目标

- 以标准 JDK 21 `ScheduledExecutorService` 作为基础契约，而不是再造一套 Future/Promise。
- 单个 EventLoop 严格单线程；默认有界，显式选择无界。
- 有界容量限制覆盖正在准入、入口队列、timer heap 和正在执行的全部 accepted task，不能让延迟任务绕过容量。
- 有界与无界实现共享唯一 `EventLoopKernel`、监督器、Future、timer、取消、module 和关闭实现，只替换 `TaskQueue`。
- 标准 `shutdown()` 和 `shutdownNow()` 精确遵守 JDK 契约；Spring、Group 和运维使用额外的共享有界 deadline 协议。
- 定时任务不会被持续入口流量饿死，入口任务也不会被已到期 timer 无限饿死。
- 每个 accepted task 在真实清理完成前都可追踪，Future 最终恰好进入一个终态。
- Group 明确拥有 child 生命周期；任何 child 故障触发整个 Group fail-stop，不静默重映射 affinity。
- 用事实矩阵说明相对 Commons 的覆盖、替代、增强和有意不复制项，不用“完全兼容”掩盖差异。

## core 通用监督契约

### 通用命名

core 当前的 `PipelineLifecycle` 实际已被 `WorkerSupervisor`、worker 快照和未来 EventLoop 共同使用，名称却把通用状态机限定成管道概念。最终架构将它直接重构为：

```java
public enum SupervisedLifecycle {
    NEW,
    STARTING,
    RUNNING,
    QUIESCING,
    STOPPING,
    TERMINATED
}
```

`WorkerSupervisor`、`WorkerSnapshot`、`PipelineSnapshot`、`ManagedPipeline`、`DisruptorRuntime` 和全部测试统一使用新名称。旧类型直接删除，不保留别名、桥接或 deprecated 兼容层。

### 无界 deadline

`ShutdownDeadline` 增加显式单例 `unbounded()`。它不是把绝对值伪装成 `Long.MAX_VALUE`，而是一个独立语义：

- `isBounded()` 为 `false`；
- `isExpired()` 永远为 `false`；
- `remainingNanos()` 返回 `Long.MAX_VALUE`；
- 不受 `System.nanoTime()` 回绕或进程运行时长影响；
- 同一次关闭会话仍冻结首次传入的对象，后续请求只允许将模式从 graceful 升级为 immediate，不能替换 deadline。

`after(Duration)` 继续表示基于单调时钟的有界绝对 deadline。标准 Executor 关闭使用 `unbounded()`；应用编排使用显式有界对象。两种语义不能由一个“默认超时”混合表达。

## 公共模型

### SupervisedScheduledExecutor

concurrent 提供建立在 core 生命周期类型之上的通用受监督执行器边界，loop 和 group 都实现它：

```java
public interface SupervisedScheduledExecutor<S>
        extends ScheduledExecutorService, AutoCloseable {

    String name();

    CompletionStage<Void> start();

    void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline);

    CompletionStage<S> termination();

    S snapshot();
}
```

`start()` 完成表示整个对象已经可以接收任务，不只是线程已创建。`termination()` 是只读 stage，并且只在全部所属 worker 真实退出、内部引用和 Future 已清理后完成。`requestShutdown` 是 Spring、Group 和运维编排入口；JDK 的 `shutdown/shutdownNow` 仍按其自身契约工作。

### EventLoop

```java
public interface EventLoop
        extends SupervisedScheduledExecutor<EventLoopSnapshot> {

    boolean inEventLoop();

    EventLoopGroup parent();

    boolean tryExecute(Runnable command);

    <V> EventLoopScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec);
}
```

`parent()` 对独立 loop 返回 `null`，对 Group child 返回其唯一 owner。`DisruptorEventLoop` 是默认有界实现，`UnboundedEventLoop` 是显式无界实现。两者的公共行为只有容量拒绝不同。

公开 builder 配置名称、线程工厂、入口批次上限、timer 批次上限、任务异常处理器、module，以及有界容量或无界 segment 大小。生产者固定按多生产者正确性实现，不向业务暴露可能被误用的 single-producer 开关。

### EventLoopGroup

`EventLoopGroup` 同样实现 `SupervisedScheduledExecutor<EventLoopGroupSnapshot>` 和 `Iterable<EventLoop>`：

- child 数量构建后固定；
- `next()` 轮询选择；
- `select(int affinityKey)` 在本次 Group 生命周期内稳定选择；
- Group 的 `execute/submit/schedule` 在提交时选择一个 child，单 child 有序，跨 child 无序；
- `EventLoopFactory` 创建带 `parent` 和稳定 `childIndex` 的 child；
- Group 在全部 child 启动成功前不开放准入；
- 任一 child 启动失败或基础设施失败，Group 锁存首因并 fail-stop 全部 children；
- child 不允许独立 `start/shutdown/shutdownNow/requestShutdown`。这些生命周期操作由 owner Group 独占，直接调用抛出明确的所有权异常；任务提交、查询和 Future 不受影响；
- Group 终止只在全部 child 的真实 termination 完成后提交。

Group 的标准 `shutdown()` 向全部 child 广播同一个 unbounded graceful deadline；`shutdownNow()` 聚合每个 child 通过 registry CAS 取得的未开始任务，按 `childIndex asc -> child acceptedSequence asc` 返回。不同 child 没有可比较的全局 accepted sequence，因此不虚构跨 child 提交顺序。

Group 自己维护聚合生命周期和唯一关闭会话，不把没有 worker 的 Group 塞进 `WorkerSupervisor`。其启动回滚、广播 token、首次 deadline、模式升级、首因和迟到 child 事实遵循 `DisruptorRuntime` 已验证的会话不变量，但不复用 Runtime 的私有类型。

### 快照

`EventLoopSnapshot` 至少包含：名称、`SupervisedLifecycle`、是否接收任务、容量模式、容量上限、outstanding、入口 pending、timer pending、executing、completed、failed、cancelled、shutdownNow returned、首因、关闭模式以及完整 `WorkerSnapshot`。

`EventLoopGroupSnapshot` 至少包含：名称、生命周期、是否接收任务、child 数量、聚合 outstanding/执行/终态计数、首因、关闭模式和按 child index 固定排序的 `EventLoopSnapshot`。

`EventLoopScheduledFuture<V>` 扩展 `ScheduledFuture<V>` 并提供 `ScheduledTaskSnapshot snapshot()`。任务快照包含 accepted sequence、调度模式、当前 trigger、可选 expires、priority、已执行次数、最大次数、是否已开始、Future 终态、取消原因和最后一次失败。快照只报告事实，不根据队列长度猜测状态。

## 任务与调度契约

### ScheduledTaskSpec

`ScheduledTaskSpec<V>` 是不可变、类型安全的高级调度定义。它完整表达：

- `ContextCallable<V>` 与显式 `TaskContext`；
- 初次 `triggerAfter`；
- one-shot、fixed-rate、fixed-delay 或 dynamic-delay，四者互斥；
- 可选 `expiresAfter`；
- 可选 `maxExecutions`；
- 周期任务失败后是否 `continueOnFailure`；
- 同 trigger 的 `priority`；
- 可选 `CancellationToken`。

`ContextCallable<V>` 直接接收 `TaskContext`，不依赖 ThreadLocal：

```java
@FunctionalInterface
public interface ContextCallable<V> {
    V call(TaskContext context) throws Exception;
}
```

`TaskContext` 是不可变 typed-key map。它不自动捕获 MDC、安全上下文或任意 ThreadLocal；需要传播的值必须在提交点显式放入。标准 `Runnable/Callable` 仍按 JDK 方式执行，只有高级 spec 走 `ContextCallable`。

时间语义全部基于可注入 `NanoClock`：

- 标准 JDK `schedule` 将非正 delay 规范化为立即触发；高级 spec 拒绝负 `triggerAfter/expiresAfter`；
- trigger 和 expires 都在任务成功 accepted 的同一个时钟快照上换算为绝对纳秒值；
- 到达 expires 时尚未开始的任务不再执行并以 `EXPIRED` 取消；执行结束后到达 expires 的周期任务不再重排；
- fixed-rate 的下一 trigger 基于前一次逻辑 trigger 加 period，落后时允许追赶，但受 timer 批次公平限制；
- fixed-delay 的下一 trigger 基于本次调用真实结束时间加 delay；
- dynamic-delay 在每次调用后于 loop 线程执行 `DynamicDelay.nextDelay(lastRunSnapshot)`，结果必须非负；计算器抛异常、返回 `null` 或返回负值都会让 Future 异常完成；
- `maxExecutions` 必须为正，达到上限后以 `MAX_EXECUTIONS` 原因结束周期调度；
- `continueOnFailure=false` 时第一次用户异常使 Future 异常完成；为 `true` 时记录失败、交给任务异常处理器并继续计算下一 trigger；
- priority 只打破完全相同 trigger 的平局，不越过更早 trigger，也不抢占已开始任务。

one-shot Future 成功时持有返回值；周期 Future 在失败、取消、expires、次数上限或 shutdown 前保持未完成。expires 和次数上限采用带明确原因的取消终态，与“任务自然返回一个结果”区分。

### 接受顺序与 schedule(0)

每个成功提交获得唯一、单调 `acceptedSequence`。有界后端使用 RingBuffer claim sequence，无界后端使用其队列 reservation sequence；失败的尝试不算 accepted。

入口中存放内部 task envelope，登记定时任务本身也必须经过入口。worker 遇到一个已经到期的 schedule envelope 时，立即结束当前入口批次，先回到 timer 阶段执行它，因此：

```text
schedule(task, 0) accepted at sequence N
execute(other)    accepted at sequence N + 1
```

`task` 必须先于 `other` 开始。多个完全相同 trigger 的 timer 按 `priority desc -> acceptedSequence asc` 排序；普通任务按队列 accepted sequence 执行。

### timer 批次公平

worker 循环固定为：

1. 处理 cancellation mailbox；
2. 最多执行 `maxTimerBatchSize` 个到期 timer；
3. 若入口非空，至少处理一个、最多处理 `maxCommandBatchSize` 个 envelope；遇到新登记且已到期的 timer 时提前结束本批；
4. 调用一次 module opportunistic update；
5. 重新读取单调时钟并重复；
6. 两侧都为空时，park 到下一 trigger；没有 timer 时无限 park。

这样，持续普通流量至多延后 timer 一个 command 批次；持续到期 timer 至多延后入口一个 timer 批次。每次成功发布、取消、关闭和更早 timer 登记都 `unpark(worker)`。实现依赖 park permit 消除“检查为空后、park 前发布”的丢唤醒窗口，不做固定毫秒轮询。

## 取消、Future 与阻塞保护

### CancellationSource

取消源只允许第一次取消成功并冻结 `CancellationReason`。监听器使用可关闭注册句柄，契约如下：

- 未取消时登记的同步监听器由赢得 `cancel` 的线程执行；异步监听器由显式 executor 执行；
- 取消后晚注册的同步监听器在注册线程内立即执行，异步监听器立即提交到指定 executor；
- `close/unregister` 与 cancel 线性化：成功解注册保证监听器不会开始；若监听器已取得执行权则返回未解注册，允许它完成；
- 监听器在触发或成功解注册后从双向链表物理移除，未取消 token 不会永久保留已关闭句柄；
- 每个监听器至多执行一次；
- 一个监听器异常不能阻止后续监听器。异常交给 `CancellationListenerExceptionHandler`，默认通过 SLF4J 记录；
- 异步 executor 拒绝也进入同一异常处理器，不回滚已冻结的取消事实。

`Future.cancel`、token cancel、expires、次数上限、shutdown 和 shutdownNow 最终汇合到同一个 Future 终态 CAS。物理任务状态与 Future 终态分开：运行中 Future 可以已经取消，但有界 outstanding permit 必须等用户调用真实返回后才能释放。

`cancelAfter` 不依赖隐藏全局 EventLoop。`CancellationSource.cancelAfter(reason, delay, scheduler)` 要求调用方显式传入 scheduler，并返回可取消的登记句柄；源提前取消时自动撤销尚未触发的 timer。

### 同 loop 阻塞保护

当结果尚未完成时，在所属 EventLoop 线程调用以下阻塞 API 必须立即抛出 `BlockingOperationException`：

- EventLoop 返回 Future 的 `get()` 与 timed `get()`；
- `awaitTermination`；
- 会等待内部 Future 的 `invokeAll/invokeAny`；
- `close()` 的等待路径。

结果已经完成时 `get()` 可直接读取。该保护防止确定性自锁，不试图检测两个 EventLoop 之间的环形等待。

## EventLoopKernel 与两种队列

### 单一 kernel

`DisruptorEventLoop` 和 `UnboundedEventLoop` 都只是同一个 `EventLoopKernel` 的公共门面。kernel 唯一拥有：

- core `WorkerSupervisor` 和单一 worker；
- `TaskAdmissionGate`；
- `AcceptedTaskRegistry`；
- Future 状态、timer heap、cancellation mailbox；
- module 生命周期；
- graceful/immediate shutdown 与快照计数。

`TaskQueue` 是唯一可替换边界，只负责多生产者 reservation/publish、单消费者 poll、唤醒所需的可见性、容量查询和消费后引用清理。queue 不持有 lifecycle、Future、timer、module 或 shutdown 策略。

### TaskAdmissionGate

准入顺序为：

1. 检查 owner Group gate 和本地 gate；
2. 取得 active-admission token；
3. 有界模式取得 outstanding permit；
4. 从 TaskQueue reserve sequence；
5. 以该 sequence 将 `AcceptedTask` 登记到 registry；
6. publish 只包含内部引用的 envelope，并以发布成功作为 accepted 线性化点；
7. 释放 active-admission token，表示该次内部准入流程已经返回；
8. 任一步失败都发布必要 tombstone、撤销 registry、归还 permit，并明确拒绝。

gate 关闭后不再产生新 token；关闭控制路径等现有 token 清零后才冻结 accepted 集。这一等待只覆盖固定的内部登记/发布步骤，不执行用户代码。

有界 permit 的生命周期覆盖 task 从第 3 步到物理清理完成：入口队列、timer heap、正在执行都占用同一 permit。取消运行中任务不会提前释放 permit。因而最多只存在配置容量个 accepted-but-not-cleaned task，延迟一年触发的 timer 也占容量。

### AcceptedTaskRegistry

registry 保存所有 accepted 且尚未物理清理的 task，以 accepted sequence 为身份。物理状态为 `WAITING/RUNNING/CANCELLED_WAITING/RETURNED/TERMINAL`：普通取消把 WAITING 变为 CANCELLED_WAITING；shutdownNow 只有从 WAITING 取得 RETURNED 所有权才向调用方返回；两者都要等 worker 从 queue/timer 物理移除后才进入 TERMINAL。周期任务在每次执行后只在同一记录内从 RUNNING 返回 WAITING，不重复申请容量。

`shutdownNow()` 关闭 gate 并等待 active-admission token 清零后，对 registry 中每个记录执行 CAS：

- `WAITING -> RETURNED` 成功：取消 Future并按 accepted sequence 加入返回列表；queue/timer 后续物理跳过该记录，转 TERMINAL 时才释放 permit；
- 已是 `RUNNING`：不进入返回列表，执行 `cancel(true)` 并中断 worker；真实返回后才转 TERMINAL 和释放 permit；
- 已是 `CANCELLED_WAITING/RETURNED/TERMINAL`：忽略。

返回元素不能是内部 envelope。来自 `execute/submit(Runnable)/schedule(Runnable)` 的记录返回调用方传入的原始 `Runnable`；来源只有 `Callable` 时返回提交时创建并交给 JDK Future 语义的 `RunnableFuture`。同一 task 最多返回一次。

### 两种 TaskQueue

`BoundedTaskQueue` 使用 LMAX 多生产者 RingBuffer，容量为 2 的幂，并由 outstanding permit 保证 reserve 不会因 timer 已离开入口队列而错误放大总容量。单消费者取出后先清空槽位引用，再推进 gating sequence。

`UnboundedTaskQueue` 使用分段 MPSC reservation/publish 队列。segment 为固定 2 的幂；生产者按全局 sequence 定位 segment/offset，单消费者严格按连续 sequence 前进，跨过整段后清引用并回收前序 segment。它没有 outstanding permit，但仍使用同一个 admission gate 和 registry。快照必须报告 pending 与已分配 segment 数，并明确无界内存风险。

两种队列运行同一组参数化 queue contract 和 EventLoop contract。禁止在无界实现中复制 kernel，禁止为了 LMAX 名称让无界队列伪装成固定 RingBuffer。

## 精确启动与失败顺序

独立 EventLoop 的启动顺序固定如下：

1. 构造阶段完成配置校验、queue/gate/registry/kernel 创建；通过用户 ThreadFactory 创建尚未启动的 worker，并把 `supervisor.supervise(workerBody)` 作为唯一 Runnable；
2. worker 在 `NEW` 时登记到 supervisor；gate 保持关闭；
3. 第一个 `start()` 将 supervisor 推进到 `STARTING`，调用 `Thread.start()`，并在 `finally` 中封口 worker 登记；
4. supervised wrapper 完成 worker 入场；worker 等待 supervisor 的“全部已登记 worker 已入场”事实；
5. worker 按声明顺序执行 module `onStart`；
6. 全部 module 成功后调用 `markRunning()`；独立 loop 打开 gate，Group child 等 owner Group 打开父 gate；
7. 完成公开 `start()` stage，然后进入执行循环。

线程创建失败发生在构造期，构造直接失败且没有半成品对象。`Thread.start()`、worker 入场或 module 启动失败均锁存基础设施首因，关闭 gate，停止已经成功启动的 module，进入 immediate fail-stop。并发 shutdown 可以在任一步获胜；启动方仍必须封口，启动 stage 与关闭 stage 各自只有一个结果。

Group 并行请求全部 child start，待全部 child 报告 RUNNING 后一次性打开 Group gate并完成 Group start。任一失败时，先关闭 Group gate，再用一个共享有界 rollback deadline 同时请求全部 child immediate；Group 保留原启动失败为首因，child 停止失败按发生顺序 suppressed，且等待全部 child 真实终止。

## module 定位

`EventLoopModule` 只提供三个 hook：

```java
public interface EventLoopModule {
    default void onStart(EventLoop loop) throws Exception {}
    default void onUpdate(EventLoop loop, long nowNanos) throws Exception {}
    default void onStop(EventLoop loop) throws Exception {}
}
```

- `onStart` 按声明顺序执行；只有成功完成 start 的 module 才进入 started 集；
- `onStop` 对 started 集逆序执行且每个恰好一次；
- start/update 异常属于基础设施故障并 fail-stop；
- stop 异常不阻断后续 stop。若已有首因则作为 suppressed，否则第一个 stop 异常成为首因，其余 suppressed；
- `onUpdate` 只在每轮 timer/command 批次后 opportunistic 调用，空闲期间没有频率保证。

这不是 Commons `IEventLoopAgent` 的完整 tick 等价物。Commons 源码还包含 `before/afterEventLoopStart`、`before/afterMainLoop`、`checkMainLoop`、`customUpdate`、`before/afterEventLoopShutdown`，module 还有 early/update/late 三阶段和 ComponentId 索引。本项目不复制该游戏/仿真组件框架。需要稳定周期 tick 的应用必须使用 `ScheduledTaskSpec.fixedRate` 显式安排；需要三阶段 tick 时由一个已调度任务在自身内部按明确顺序调用业务组件。

## JDK 21 关闭契约

### shutdown

`shutdown()` 立即关闭新任务准入并以 `ShutdownDeadline.unbounded()` 请求 graceful：

- 已 accepted 普通任务继续执行；
- 已 accepted 的一次性 delayed task 保留，即使尚未到期；
- 已 accepted 周期任务取消，不再开始新的周期；若当前正在执行，不中断本次调用，但不再重排；
- 只有所有保留任务完成、Future 终态化、队列/timer/registry 清理、module stop 和 worker 真实退出后才 terminated。

因此一个一年后才触发的一次性任务可以让 `shutdown()` 等待一年。这是标准有序关闭语义，不用项目默认超时偷偷改变。需要有界运维窗口时必须调用 `requestShutdown(GRACEFUL, ShutdownDeadline.after(timeout))`。

### shutdownNow

`shutdownNow()` 关闭 gate、等待内部 admission token 归还、按 accepted registry CAS 收集未开始任务，随后以 immediate 模式停止：

- 返回所有在其线性化点尚未开始且被本次调用成功取得所有权的原始 Runnable，按 accepted sequence 排序；
- 未开始 Future 进入取消终态；
- 正在执行任务不返回，收到 `cancel(true)` 和 worker interrupt；
- Java 不能强制停止忽略中断的用户代码，因此 `shutdownNow()` 只发起 best-effort 停止，`isTerminated/termination` 仍等待线程真实退出；
- 调用本身不等待用户任务退出。

这比参考 `DisruptorEventLoop.shutdownNow()` 固定返回空列表更符合 JDK 21 契约。registry CAS 使调用线程无需并发消费 MPSC 队列，也不会破坏单消费者所有权。

### 显式有界关闭

`requestShutdown(mode, sharedDeadline)` 用于 Spring、Group 和运维：

- graceful 在 deadline 前遵守与 `shutdown()` 相同的保留集合；
- deadline 到期后锁存 timeout 事实并单调升级 immediate，取消未开始任务、请求中断运行任务；
- deadline 到期不伪造 terminated，真实 worker 未退出时 termination 不完成；
- 同一 Group/Spring 广播必须把同一个 `ShutdownDeadline` 对象传给所有根对象，不能按 child 重算 timeout；
- 后续 immediate 可升级模式，但不能替换首次 deadline 或结果首因。

## Commons 源码能力矩阵

最终事实矩阵见[Commons 能力事实矩阵](../../commons-capability-matrix.md)。该文档固定参考 commit `5c831c06`，分别审计 Java `Disruptor` 与 `Commons-Concurrent`，并为每一项记录参考源码、本项目落点和测试证据。

其中 dynamic-delay、priority、`shutdownNow` 返回值、Group fail-stop、共享 deadline 与精确启动回滚属于本项目增强。自研 Future、ComponentId/Agent phases、WatcherMgr、LOCAL_ORDER、任务池和 GlobalEventLoop 是标准 API 替代或明确不复制的边界，不是待补缺口。性能只由 JMH 结果陈述，不从功能矩阵推导。

## Spring Boot 4.1 集成

自动配置按 classpath 能力拆成三类，避免 optional 类型缺失造成类加载失败：

1. `DisruptorConcurrentAutoConfiguration` 只依赖 concurrent 与 Spring Context，管理显式根 Bean 生命周期；
2. `DisruptorConcurrentHealthAutoConfiguration` 同时以 concurrent 和 Boot 4.1 `org.springframework.boot.health.contributor.HealthIndicator` 为条件，生产 health contributor；
3. `DisruptorConcurrentMetricsAutoConfiguration` 同时以 concurrent 和 Micrometer `MeterRegistry` 为条件，生产 `MeterBinder`。

autoconfigure POM 将 `disruptor-concurrent`、`spring-boot-health` 和 `micrometer-core` 都声明为 optional；测试依赖提供真实 Boot 4.1 health/metrics 自动配置。不得把 concurrent 变成 starter 的强制传递依赖。

生命周期聚合器只管理容器中的根 `EventLoop`/`EventLoopGroup` Bean，不重复管理 Group children，也不自动创建 GlobalEventLoop。启动按 Spring 排序请求并等待全部 start stage；关闭时创建一个 `ShutdownDeadline.after(configuredTimeout)` 并广播给全部根对象。

`SmartLifecycle.stop(Runnable callback)` 使用异步聚合：先同步完成全部关闭请求，再监听全部真实 termination，最后在 `finally` 语义下恰好调用一次 callback。deadline 到期或 termination 异常不能提前调用 callback；仍有 worker 存活就不算 Spring 组件已停止。

健康语义：基础设施 failure 为 `DOWN`；正常 RUNNING 且 gate 开放为 `UP`；NEW、STARTING、QUIESCING、STOPPING 和无故障 TERMINATED 为 `OUT_OF_SERVICE`。任务业务失败计数不单独把基础设施判为 DOWN。

指标至少包含：

- `disruptor.eventloop.accepting`；
- `disruptor.eventloop.worker.registered/started/alive`；
- `disruptor.eventloop.tasks.outstanding/pending/executing/completed/failed/cancelled/returned`；
- `disruptor.eventloop.scheduled.pending`；
- `disruptor.eventloop.queue.remaining`，仅 bounded；
- `disruptor.eventloop.queue.segments`，仅 unbounded。

所有指标在采集时读取 snapshot，不进入提交或执行热路径。

## 验证策略

时间测试注入手动 `NanoClock`；只有 park/unpark、中断、真实线程终止和多生产者压力使用真实时间。测试必须覆盖：

- core 通用 lifecycle 重命名后现有 pipeline/runtime 全部不变量不退化；
- bounded/unbounded 同一 queue contract、同一 EventLoop contract 和同一 kernel 类型；
- 多生产者 accepted sequence、无丢失、无重复、槽位/segment 引用清理；
- 有界 outstanding 同时覆盖入口、timer 和 executing；
- `execute` 与 `schedule(0)` 顺序、同 trigger priority、两侧批次公平；
- 四种 schedule、expires、max executions、continue failure 与快照；
- cancellation 早/晚注册、解注册、同步/异步线程、监听异常、重复取消和 Future 唯一终态；
- 同 loop Future/get、await、invoke/close 阻塞保护；
- 精确 startup、并发 shutdown、module 部分启动回滚、suppressed stop failure；
- `shutdown()` 保留未来 one-shot delayed、取消 periodic 且使用 unbounded deadline；
- `shutdownNow()` registry CAS 返回原始未开始 Runnable，不返回 running task；
- bounded deadline 升级 immediate 但不伪造 termination；
- Group gate、固定 affinity、child 生命周期所有权、child fail-stop、共享 deadline 和真实聚合终止；
- Boot 4.1 lifecycle、health、metrics 三组条件装配及 callback 时序；
- Commons 能力矩阵中的每个“覆盖/替代/增强/不复制”都有源码、测试、示例或明确文档证据；
- JMH 对比原生 LMAX、core managed publish、bounded/unbounded EventLoop、JDK 单线程 executor，并在可独立构建参考仓时增加 Commons profile；不设置机器敏感硬阈值。

## 交付顺序

实施以十个可独立保持 reactor 绿色的纵向切片完成：

1. core 通用监督命名与无界 deadline；
2. concurrent 模块和完整公共契约；
3. 任务基元、取消、显式上下文和 module；
4. admission gate、accepted registry 与两种 TaskQueue 契约；
5. bounded 完整 executor、scheduler 和 JDK shutdown；
6. unbounded 门面复用全部 kernel 契约；
7. Group 所有权、选择、fail-stop 与聚合终止；
8. Boot 4.1 生命周期、健康和指标；
9. 使用示例与 Commons 事实矩阵；
10. JMH、全仓回归与逐条完成审计。
