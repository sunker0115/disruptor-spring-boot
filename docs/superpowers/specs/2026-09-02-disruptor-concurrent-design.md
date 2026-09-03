# disruptor-concurrent 设计

## 定位

`disruptor-concurrent` 是构建在受监督 `disruptor-core` 之上的有界单线程执行与调度模块。它对标 `Commons-Concurrent` 的行为能力，但不追求包名、类型名或二进制兼容，也不复制其自研 Future 体系。

最终依赖方向：

```text
LMAX Disruptor 4.0
        │
        ▼
disruptor-core
        │
        ▼
disruptor-concurrent

disruptor-core ───────────────┐
disruptor-concurrent(optional)├─> disruptor-spring-boot-autoconfigure -> starter
                              ┘
```

现有 starter 不强制传递 concurrent。用户显式添加 `disruptor-concurrent` 后，现有自动配置模块通过条件装配管理其 Spring 生命周期、健康和指标。若未来并发配置规模独立增长，再拆专属 starter，不在本次提前增加模块。

## 目标

- 提供标准 `ScheduledExecutorService`，可直接用于 Spring、JDK 和 `CompletableFuture`。
- 单个 EventLoop 严格单线程，默认有界、显式无界，并对有界容量给出明确背压和拒绝原因。
- 普通任务、定时登记和用户命令共享一个 `TaskQueue` 全序入口。
- 定时任务在持续普通任务流量下不饥饿，空闲时不固定周期轮询。
- 支持固定 EventLoopGroup、轮询选择和稳定 affinity-key 选择。
- 提供协作取消、取消原因、截止时间、执行次数、优先级和上下文传播的类型安全 API。
- 提供模块化启动、更新、停止 hook，覆盖 Commons 的 Agent/Module 业务场景。
- 有界和无界实现共享同一个 `EventLoopKernel`、worker 和 core `WorkerSupervisor`，只替换 `TaskQueue` 后端。
- 复用 core 的生命周期、首因、共享绝对截止时间、动态 worker 入场、终止与快照，不复制第二套监督状态机。
- 以功能矩阵、并发测试和 JMH 证明相对 Commons 没有未声明退化。

## 方案比较

| 方案 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- |
| concurrent 直接创建 LMAX RingBuffer 并自建状态机 | 实现自由 | 复制 core 的关闭、故障和发布竞态，长期漂移 | 不采用 |
| 包装 `ScheduledThreadPoolExecutor` | 标准 API 快速 | 定时队列无界，任务执行绕过 RingBuffer，模块名和性能目标失真 | 不采用 |
| 共享 EventLoopKernel + core 统一监督器 + 可替换 TaskQueue | Future、timer、module、shutdown 和 Group 只有一套语义，有界仍保留 LMAX RingBuffer 顺序与背压 | 需要 core 先完成分阶段监督生命周期 | 采用 |

## 公共 API

### EventLoop

公共 `EventLoop` 接口扩展 `ScheduledExecutorService`、`AutoCloseable`；`DisruptorEventLoop` 是默认有界实现，`UnboundedEventLoop` 是显式无界实现。公共接口增加：

```java
String name();
boolean inEventLoop();
CompletionStage<Void> start();
CompletionStage<EventLoopSnapshot> termination();
EventLoopSnapshot snapshot();
boolean tryExecute(Runnable command);
<V> ScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec);
```

标准 `execute/submit/invokeAll/invokeAny/schedule/scheduleAtFixedRate/scheduleWithFixedDelay` 保持 JDK 契约。`start()` 完成表示消费者已进入循环，而不是仅创建线程。`termination()` 返回只读 stage。

Builder 配置：名称、bufferSize、maxBatchSize、shutdownTimeout、ThreadFactory、任务异常处理器、容量模式和模块列表。Executor 固定为多生产者语义；`SINGLE` 只允许作为经过验证的内部优化，不能成为会被业务误用的普通配置。

### EventLoopGroup

`DisruptorEventLoopGroup` 实现 `ScheduledExecutorService`、`Iterable<EventLoop>`、`AutoCloseable`：

- 大小构建后固定；
- `next()` 轮询选择；
- `select(int affinityKey)` 在本次 Group 生命周期内稳定选择；
- Group 自身提交和调度委派给选中的 child；
- 单 child 内有序，跨 child 明确无序；
- 任一 child 发生基础设施故障，Group 锁存首因并 fail-stop，不能静默重映射 affinity key；
- Group 关闭创建一个 `ShutdownDeadline`，先向全部 child 传递相同绝对 deadline 并同时请求停止，再聚合真实终止；任一 child 未真实退出时 Group 不能提交 terminated。
- Group 应复用 core Runtime 的单会话不变量与协调模式；但在 child 故障 fail-stop、Executor 关闭结果和 Runtime 启动回滚的失败传播完全同构前，不直接复用 Runtime 私有会话类型。

### 高级任务语义

不使用 `TaskOptions` 位字段，改为类型安全对象：

- `CancellationToken/CancellationSource`：协作取消、取消原因和监听；
- `TaskContext`：显式上下文载体，不隐式依赖 ThreadLocal；
- `ScheduledTaskSpec<V>`：一次、固定频率、固定延迟、动态延迟；
- 可选截止时间、最大执行次数、同 deadline 优先级、异常后是否继续；
- `ScheduledFuture` 仍是标准 Future，扩展状态通过只读 `TaskSnapshot` 查询。

普通 Future 使用 JDK `FutureTask/CompletableFuture` 语义，不实现另一套 `IFuture/IPromise/ICompletionStage`。需要异步组合时使用 `CompletableFuture.runAsync/supplyAsync(..., eventLoop)`。

### 模块与用户事件

提供轻量 `EventLoopModule`：

```text
onStart（声明顺序）
onUpdate（每批任务后或定时唤醒）
onStop（启动成功模块的逆序）
```

模块启动失败属于基础设施故障并触发 fail-stop。模块更新异常默认触发 fail-stop，不以日志吞掉破坏状态一致性的错误。

Commons 的自定义用户事件和原始 sequence 发布由项目整体能力覆盖：需要强类型事件和 handler 拓扑时使用 `disruptor-core`；concurrent 不在 `Runnable` 任务槽内再实现一套事件总线。两者可通过 EventLoop 提交任务或管道 handler 发布命令组合。

## 内部执行模型

### 共享 EventLoopKernel

`DisruptorEventLoop` 和 `UnboundedEventLoop` 都是同一个 `EventLoopKernel` 的门面。kernel 唯一拥有：

- 标准 Executor/Future 的接受、执行、失败和取消终态；
- timer heap、cancellation mailbox 和调度顺序；
- module 的启动、更新、逆序停止；
- core `WorkerSupervisor`、单一 worker 和 lifecycle snapshot；
- graceful/immediate shutdown、首因和 termination。

`TaskQueue` 是唯一可替换边界，只负责 claim/offer、单消费者 poll、容量和引用清理。任何 TaskQueue 都不得拥有第二套生命周期、Future 或 module 状态。

EventLoop 启动时登记单一 worker，进入 `STARTING` 后启动并封口；worker 完成 module `onStart` 后才能提交 `RUNNING`。NEW 下意外启动的 worker 不执行循环；worker 在 `STARTING/RUNNING/QUIESCING` 提前退出都由 supervisor 视为基础设施故障。

### 有界路径

每个 EventLoop 内部是一条 `TaskSlot` 管道：

```java
final class TaskSlot {
    Runnable command;
}
```

唯一消费者是基于 LMAX `EventPoller` 的专用 `EventProcessor`。poller sequence 注册为 gating sequence。处理完成后必须先清空 `TaskSlot.command`，再推进 sequence，防止对象跨 RingBuffer 周期滞留。

发布成功后任务才算 accepted。translator 只赋值引用，不执行可能失败的业务逻辑。

### 容量模式

- `BOUNDED`：默认，`EventLoopKernel` 使用固定大小 LMAX RingBuffer `TaskQueue`；满时立即拒绝。它不嵌套应用事件 `DisruptorPipeline`，避免形成第二层生命周期。
- `UNBOUNDED`：显式选择，同一个 `EventLoopKernel` 使用分段 MPSC `TaskQueue`；它不伪装成 LMAX 固定 RingBuffer，也不进入默认 starter 配置。

两种后端实现同一个内部 `TaskQueue` 契约，并运行同一组 EventLoop 生命周期、Future、timer、module、shutdown 和 Group 契约测试。无界模式必须按块回收已消费引用，并提供当前已分配块数和待处理任务数指标。它只解决突发容量，不取消内存无限增长风险；文档和健康详情必须显式标注。

### 调度与顺序

定时任务登记也先进入任务队列，由 EventLoop 线程加入本地索引最小堆。排序键为：

```text
deadlineNanos 升序 → priority 降序 → acceptedSequence 升序
```

priority 只打破相同 deadline 的平局，不允许越过更早 deadline 或已经开始执行的普通任务。

循环规则：

1. 执行已经到期且此前已登记的 timer；
2. 最多处理 `maxBatchSize` 条普通/登记命令；
3. 再检查 timer 和模块 update；
4. 无任务时 park 到下一 deadline；没有 deadline 时无限 park。

每次成功发布、新取消请求和关闭请求都 `LockSupport.unpark(worker)`。unpark 许可允许先于 park，避免“检查为空后、park 前发布”的丢唤醒竞态。禁止固定 1ms 轮询。

`schedule(0)` 在自己的队列 sequence 处登记，到期后先于后续 sequence 执行。持续普通流量下，timer 最多被一个 `maxBatchSize` 批次延后。

外部取消只修改 Future 原子状态并进入 cancellation mailbox，然后 unpark。定时堆只由 loop 线程删除；索引堆保证删除 O(log n)。mailbox 中每个已接受定时任务最多存在一个取消节点，因此内存受已接受任务数量约束。

## 拒绝与异常

- `execute` 永不阻塞，也永不在提交线程内联执行；
- 未启动、容量满、关闭中、失败分别映射到包含具体原因的 `RejectedExecutionException`；
- loop 内重入提交若容量满同样拒绝，绝不阻塞自身；
- 不提供 `CallerRuns` 和静默 discard；两者破坏线程封闭或 Future 终态；
- `tryExecute` 只用布尔值表达容量/状态拒绝，详细原因从快照读取；
- `submit/schedule` 的用户任务异常由 Future 捕获；
- 裸 `execute` 的用户任务 `Throwable` 交给任务异常处理器，默认 SLF4J 记录并继续；
- 只有逃出用户任务边界的 processor、队列、模块生命周期异常才属于基础设施故障，并交给 core supervisor fail-stop。

## 关闭语义

EventLoop 使用 core `ShutdownBackend` 的非阻塞阶段协议：

- `beginQuiesce()` 只 unpark worker；
- worker 在 `QUIESCING` 中处理关闭前已接受的任务、取消未来 timer 和周期任务，并在所有 Future 已终态化后发布 `drained`；
- `isDrained()` 只读取该状态，不清队列、不等待 worker；
- `stop(GRACEFUL)` unpark worker 进入退出 `finally`；
- `stop(IMMEDIATE)` 发布强停标志并 unpark，supervisor 同时 interrupt worker。

所有阶段动作由 supervisor 控制线程串行调用，绝不并发重入。Future 终态化、队列引用清理和 module stop 由 EventLoop worker 在退出前完成；supervisor `join` 确认线程真实死亡后才完成 termination。

`shutdown()`：

- 立即关闭新任务准入；
- 执行关闭前已经接受的普通任务和已经到期的一次性任务；
- 取消尚未到期及所有周期任务；
- 排空队列、清理引用，然后终止 processor。

`shutdownNow()`：

- 立即停止领取新任务；
- 取消所有未开始 Future；
- 清理有界和无界队列引用；
- 返回空列表；pending 队列只允许 EventLoop worker 消费，调用线程不能为收集返回值并发破坏单消费者所有权。该取舍与 Commons `DisruptorEventLoop.shutdownNow()` 一致；
- 正在执行的任务收到 `cancel(true)` 和线程中断；任务退出后清除任务中断位，关闭控制只依赖原子状态与 unpark。

任何已接受任务在终止前必须进入成功、失败或取消终态。基础设施故障同样完成所有未完成 Future，不能留下永久等待者。

`shutdown()` 和 `shutdownNow()` 都只推进状态、冻结首次 `ShutdownDeadline` 并唤醒控制路径，调用方立即返回。graceful 固定经过 `RUNNING → QUIESCING → STOPPING → TERMINATED`；立即关闭、故障或 deadline 到期单调进入 `STOPPING`。deadline 到期只锁存首因并升级 immediate，不在 worker 存活时完成 termination。

`EventLoopSnapshot` 包含 worker 登记是否封口、registered/started/alive 计数。`RUNNING` 蕴含单一 worker 已登记封口、启动且存活；`gracefulTermination` 只有 EventLoop 曾进入 `RUNNING`、graceful drain 已提交、graceful stop 已应用、最终仍为 graceful、无基础设施故障、单一 worker 已真实退出时成立。

## 与 Commons 的能力审计

| Commons 能力 | 本项目落点 | 结论 |
| --- | --- | --- |
| EventLoop / EventLoopGroup / chooser | concurrent 标准 Executor + next/select | 覆盖 |
| 一次、fixed-rate、fixed-delay、动态延迟 | `ScheduledTaskSpec` | 覆盖 |
| 超时、次数、优先级、异常后继续 | 类型安全调度选项 | 覆盖 |
| CancelToken 与取消原因 | `CancellationToken/Source` | 覆盖 |
| Context | `TaskContext` | 覆盖 |
| Agent/Module/tick update | `EventLoopModule` | 等价覆盖，不复制组件框架 |
| IFuture/IPromise/组合器 | JDK Future、CompletionStage、CompletableFuture | 行为覆盖，不兼容 API |
| Executor 适配、立即执行器 | JDK `ExecutorService`、`Runnable::run` | 标准 API 覆盖 |
| Future 转发、只读视图、结果持有器 | CompletionStage 只读视图与标准 Future | 标准 API 覆盖 |
| FutureLogger 与 stackless 异常 | 任务异常处理器、标准取消/超时异常 | 行为覆盖，不复制异常类型 |
| 用户事件与原始 sequence | disruptor-core 原生 topology/unsafe RingBuffer | 项目级覆盖 |
| 有界/无界队列 | 两种显式 `TaskQueue` 后端 | 覆盖 |
| 任务池 | 默认不池化；以 JMH/GC 数据决定可选池化 | 暂不宣称性能等价 |
| WatcherMgr | 线程封闭场景使用模块内集合；跨线程监听使用取消监听或 JDK Flow | 项目级等价，不增加通用观察者框架 |
| GlobalEventLoop | 由 Spring Bean 或应用单例管理 | 等价覆盖，拒绝隐藏全局状态 |
| 动态 Group | Commons 本身也是固定 children 构建模型 | 覆盖实际能力 |

在基准完成前，不宣称吞吐或分配优于 Commons。若标准 Future 或监督准入造成显著退化，优先优化内部任务表示和批量发布，不改变公共契约。

## Spring 集成

用户显式添加 concurrent 依赖并声明 `DisruptorEventLoop` 或 `DisruptorEventLoopGroup` Bean。自动配置提供一个 `SmartLifecycle` 聚合器：按 bean 顺序启动，关闭时创建一个 `ShutdownDeadline` 并向所有 bean 广播，再聚合真实终止；不会自动创建隐藏的全局 EventLoop。

新增指标至少包括：

- `disruptor.eventloop.healthy`
- `disruptor.eventloop.worker.registered`
- `disruptor.eventloop.worker.started`
- `disruptor.eventloop.worker.alive`
- `disruptor.eventloop.queue.pending`
- `disruptor.eventloop.queue.remaining`
- `disruptor.eventloop.scheduled.pending`
- `disruptor.eventloop.tasks.completed`
- `disruptor.eventloop.tasks.failed`
- `disruptor.eventloop.tasks.cancelled`

健康状态由 core 快照与任务积压组合派生，任务异常计数本身不把 EventLoop 判为 `DOWN`；基础设施故障才判定为 `DOWN`。

## 验证

- 多生产者按成功 claim sequence 执行；
- `execute` 与 `schedule(0)` 交错顺序；
- timer 在持续 backlog 下不饥饿；
- fixed-rate、fixed-delay、动态延迟、次数和截止时间；
- 取消登记、触发、执行、重复调度之间的竞态；
- loop 内满队列重入不死锁；
- shutdown/publish、shutdown/cancel、故障/终止竞态；
- shutdown 和 shutdownNow 的 Future 终态及引用清理；
- bounded/unbounded 共享同一个 kernel、supervisor 和快照不变量；
- graceful 的 `QUIESCING → STOPPING → TERMINATED`、deadline immediate 升级和真实线程终止；
- 任务异常不击穿，基础设施异常 fail-stop；
- 模块启动回滚、update 故障和逆序停止；
- Group affinity 稳定、child 故障和聚合终止；
- 有界/无界 TaskQueue 契约一致性及无界块回收；
- 虚假唤醒和 lost wakeup 压测；
- Spring 生命周期、健康和 Micrometer 指标测试；
- JMH 对比原生 RingBuffer、EventLoop、Commons EventLoop 与 JDK 单线程执行器，不设置机器敏感的硬阈值。

时间相关单测注入 `NanoClock`，用手动时钟确定性推进；仅保留少量真实线程集成测试验证 park/unpark 和中断。

## 交付顺序

1. core 单管道监督、终止 stage、发布结果；
2. concurrent 模块骨架与有界 EventLoop；
3. 标准 submit 与 ScheduledExecutorService；
4. 高级调度、取消、上下文和模块 hook；
5. EventLoopGroup；
6. 显式无界后端；
7. Spring 生命周期、健康和指标；
8. 文档、示例、全仓回归和 JMH。
