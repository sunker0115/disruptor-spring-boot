# 受监督 Disruptor 管道设计

## 背景

当前 `disruptor-core` 已经托管命名管道、发布准入和有界关闭，但运行时状态只代表生命周期，不代表消费者健康。默认 `HALT` 策略导致消费者退出后，Runtime 仍可能保持 `RUNNING`；不可中断的阻塞发布还可能在消费者停止推进后永久等待。

本次重构以 LMAX Disruptor 4.0 为唯一有界 RingBuffer 实现，吸收 `cn.wjybxx.commons.disruptor` 在可中断生产者等待、消费者关闭后释放生产者和显式屏障方面解决的问题，但不复制其独立 Sequencer、Barrier 或 EventLoop 实现。

## 目标

- 单条管道是可独立启动、监督、关闭和观察的生命周期单元。
- 消费者运行期异常退出时，整条管道 fail-stop，其他命名管道不受影响。
- 生命周期与健康分轴，首个基础设施故障永久锁存到终止快照。
- 受管发布提供非阻塞和有界、可中断等待，不再提供无限阻塞入口。
- 关闭请求可以从消费线程内安全发起，不自等待、不自 `join`。
- 所有后端共享绝对关闭截止时间、worker 监督和真实终止语义。
- `DisruptorRuntime` 只聚合多条管道，不再拥有另一套管道状态判断。
- Spring 健康和指标只读取统一快照，不在发布或消费热路径埋点。

## 非目标

- 不重写 LMAX Sequencer、SequenceBarrier、RingBuffer 或原生 topology DSL。
- 不包装业务 EventHandler，不实现自动重试、补偿或消费者自动重启。
- 不承诺绕过 `unsafeRingBuffer()` 的外部发布与 Runtime 并发关闭时无损。
- 不把 `maxBatchSize` 错误抽象成公平调度参数；它仍是原生处理器的批边界配置。

## 方案比较

| 方案 | 架构边界与优点 | 正确性与代价 | 结论 |
| --- | --- | --- | --- |
| 统一监督器 + 非阻塞分阶段后端协议 | core 统一生命周期、首因、deadline、worker 与终止；LMAX 和 EventLoop 只适配后端动作 | 单锁线性化，后端动作由单控制线程串行，可确定性测试 | 采用 |
| passive worker 记录器 + 后端各自协调关闭 | 后端实现最自由 | LMAX 与 EventLoop 必须重复模式升级、deadline 和终止提交，长期必然漂移 | 不采用 |
| 阻塞排空线程 + 独立 watchdog | 可保留阻塞回调 | graceful 与 force 要么并发破坏后端线程封闭，要么仍被阻塞；还会泄漏不可强杀线程 | 不采用 |

## 最终模型

### 类型与职责

- `WorkerSupervisor`：core 提供的统一受监督生命周期控制器；管理生命周期、worker 登记与入场、首因、关闭模式、绝对截止时间和真实线程终止。
- `ShutdownBackend`：仅供项目内部后端实现的非阻塞阶段协议；不暴露任意用户回调。
- `ShutdownDeadline`：基于 `System.nanoTime()` 的不可变绝对截止时间；Runtime 和 Group 创建一次并传给所有 child。
- `DisruptorPipeline<E>`：组合 `WorkerSupervisor` 和 LMAX `ShutdownBackend` 的单管道生命周期单元。
- `PipelineHandle<E>`：发布入口与快照读取，不拥有生命周期。
- `PipelineSnapshot`：不可变状态事实源。
- `DisruptorRuntime`：构建并聚合多条 `DisruptorPipeline`，负责批量启动、回滚、共享截止时间和真实终止聚合。

后端阶段协议固定为：

```java
public interface ShutdownBackend {
    void beginQuiesce() throws Throwable;
    boolean isDrained() throws Throwable;
    void stop(ShutdownMode mode) throws Throwable;
}
```

约束如下：

- 三个方法只能由 supervisor 的命名虚拟控制线程调用，且绝不在状态锁内调用。
- 后端调用严格串行，不允许并发重入。
- 方法只做有界、非阻塞的内部动作，不得等待 sequence、等待 Future、`join` worker 或调用业务 handler/module。
- `beginQuiesce()` 至多一次；`isDrained()` 只在 `QUIESCING` 期间调用；`stop(GRACEFUL)` 和 `stop(IMMEDIATE)` 各至多一次。
- graceful stop 后若 worker 未在 deadline 前退出，可以继续串行调用一次 immediate stop。
- 后端动作抛出的首个异常进入 supervisor 首因，随后单调升级为 `IMMEDIATE`。
- `isDrained()` 返回 true 后必须回到状态锁重新确认仍为同一次 `GRACEFUL + QUIESCING`，才能提交 drain 和 `STOPPING`；并发 immediate、故障或 deadline 永远优先。

supervisor 配置一个默认 `shutdownTimeout`，只用于没有上层协调者的独立关闭或自主故障。`requestShutdown(mode, deadline)` 只在线性化点关闭准入、推进状态、冻结首次 deadline，并启动或唤醒控制线程；它不执行排空、后端 stop、worker 中断或线程等待。worker 自主故障使用默认 timeout 生成 deadline；Runtime/Group 在 child 尚未开始关闭时传入共享 deadline。并发请求以状态锁内第一个线性化的 deadline 为准，后续不得覆盖或延长。消费线程发起关闭时不会等待自身。

### 状态、健康与 worker 入场

生命周期固定为：

```text
NEW --beginStart--> STARTING --markRunning--> RUNNING

RUNNING --GRACEFUL--> QUIESCING --drained commit--> STOPPING
QUIESCING --失败/超时/IMMEDIATE--> STOPPING
NEW/STARTING/RUNNING --失败或 IMMEDIATE--> STOPPING
NEW/STARTING --GRACEFUL--> STOPPING

STOPPING --后端停止动作完成且全部已启动线程真实退出--> TERMINATED
```

`QUIESCING` 不得直接进入 `TERMINATED`。控制线程必须先在状态锁内提交 `STOPPING`，再调用后端 stop；`TERMINATED` 只能在所有已启动 worker 完成 `join` 后提交。

worker 使用动态登记与封口，不在构建 supervisor 时猜测 LMAX topology 的消费者数量：

1. `markStarting()` 后允许托管 ThreadFactory 登记 worker；预先构造的 EventLoop worker 也可在 `NEW` 登记。
2. 后端完成全部线程创建和启动后调用 `sealWorkers()`；封口后的登记数成为本生命周期的预期 worker 数。
3. wrapper 入场时记录 `startedWorkers` 和 `aliveWorkers`，并完成只读 `workersStarted()` 信号。
4. `markRunning()` 只允许在登记已封口、worker 数大于 0、全部登记 worker 已入场且仍存活时执行。
5. wrapper 只允许在 `STARTING/RUNNING` 入场；在 `NEW/STOPPING/TERMINATED` 不执行用户 runnable。每个线程只能经历一次 `REGISTERED → ALIVE → EXITED`。
6. worker 在 `STARTING/RUNNING/QUIESCING` 正常或异常提前退出都属于基础设施故障并触发 fail-stop；只有 `STOPPING` 中的普通退出是预期退出。

健康由快照派生，不增加可独立漂移的状态机：

- `NEW/STARTING`：`STARTING`；
- `RUNNING` 且登记已封口、全部已启动 worker 存活、无故障：`HEALTHY`；
- `RUNNING` 但 worker 数量不足或已有故障：`UNHEALTHY`；
- `QUIESCING/STOPPING`：`OUT_OF_SERVICE`；
- `TERMINATED`：`TERMINATED`。

故障与生命周期分轴。关闭期间不得清除、包装掉或用后续异常覆盖首个故障。

### 启动协议

LMAX 管道的启动顺序是：

1. supervisor 进入 `STARTING`；
2. 调用 `Disruptor.start()`，由托管 ThreadFactory 动态登记并启动全部 processor 线程；
3. `Disruptor.start()` 返回后封口 worker 登记；
4. 等待全部 worker 实际入场，再提交 `RUNNING` 和启动完成信号；
5. `ManagedPipeline` 用同一个 lifecycle lock 线性化启动完成、关闭和发布准入；发布 gate 只允许 `NEW → OPEN → CLOSED`，关闭一旦胜出就永不重新打开；
6. 启动前关闭通过 `markStarting + sealWorkers` 显式封口零 worker，启动 stage 异常完成，后续重复 `start()` 返回同一个失败 stage；
7. 任一步失败都锁存原始首因并使用同一 lifecycle 进入 `IMMEDIATE` 回滚。

这样支持 LMAX 任意原生 topology，不依赖反射读取 ConsumerRepository，也不把线程已创建误认为线程已运行。

### 发布协议

删除无限阻塞的受管 `publishEvent(...)`。0 至 3 参数 translator 统一提供：

```java
PublicationResult tryPublishEvent(...);
PublicationResult publishEvent(..., Duration timeout) throws InterruptedException;
```

结果固定为：`PUBLISHED`、`CAPACITY_EXHAUSTED`、`TIMED_OUT`、`NOT_RUNNING`、`PIPELINE_FAILED`。

有界等待通过反复调用 LMAX `tryPublishEvent` 实现，绝不手工 claim。等待循环必须检查中断、截止时间、生命周期和管道故障；关闭或故障必须唤醒等待者。translator 只会在成功取得容量后执行，仍遵守“translator 不得抛出业务异常”的 LMAX 约束。

`unsafeRingBuffer()` 保留完整 LMAX API，包括批量、varargs、原始 sequence claim/publish。名称明确表示它绕过受管生命周期。

### LMAX 关闭映射

`ManagedPipeline` 不调用可能阻塞排空的 LMAX `Disruptor.shutdown(timeout)`，也不再提供自带等待的 `shutdown/haltNow` 实现。其 `ShutdownBackend` 映射为：

- `beginQuiesce()`：唤醒受管发布等待者；生命周期状态已经拒绝新发布。
- `isDrained()`：先等待已获准 publisher 数归零，只在归零后捕获一次固定 cursor；所有叶子 gating sequence 到达目标后返回 true。
- `stop(GRACEFUL)`：排空提交后调用非阻塞 `disruptor.halt()`。
- `stop(IMMEDIATE)`：直接调用 `disruptor.halt()`；控制线程等待 halt/alert 返回后再中断仍存活 worker。

优雅关闭固定经过：关闭准入 → 等在途发布 → 捕获 cursor → 等叶子 sequence → `STOPPING` → halt → join。故障或立即停止不等待 gating sequence。

### deadline、首因与真实终止

`ShutdownDeadline.after(Duration)` 使用饱和加法生成绝对 `deadlineNanos`。第一次关闭请求或自主故障冻结 deadline，重复请求和 `IMMEDIATE` 升级不得延长。独立管道/EventLoop 使用自己的默认 shutdown timeout；Runtime/Group 在首次请求前创建一个共享 deadline 并传给全部 child。

控制线程只做非阻塞 drain probe 和有界等待，因此 deadline 不会被后端排空回调卡住。外部 `IMMEDIATE` 请求在线性化点直接关闭准入、把 `QUIESCING` 升级为 `STOPPING` 并唤醒控制线程；控制线程随后先串行执行 immediate stop，再中断仍存活 worker。即使 stop 抛错，中断也在其 `finally` 路径执行。这样 LMAX 会先通过 halt/alert 结束正常等待，不会把监督器主动中断误报为消费故障。

deadline 到期只做三件事：若尚无首因则记录 `WorkerTerminationTimeoutException`、把模式升级为 `IMMEDIATE`、由控制线程先强制停止后端再中断 worker。它不是终止捷径。抗中断 worker 仍存活时生命周期保持 `STOPPING`，`termination()` 不完成；控制线程继续等待实际线程死亡。只有后端停止动作已经执行、所有实际启动的 worker 完成 `join`，才能在状态锁内提交 `TERMINATED` 和终止快照。

worker 退出前的引用清理、Future 终态化和模块 stop 属于 worker 自己的 `finally`。因此线程真实退出自然蕴含后端 cleanup 已完成，supervisor 不另设会阻塞的 cleanup 回调。

`gracefulTermination` 只有同时满足以下事实才为 true：曾成功进入 `RUNNING`、graceful drain 已提交、已执行 graceful stop、最终模式仍为 `GRACEFUL`、无故障、全部登记 worker 都曾入场且已真实退出。

### Runtime 聚合

Runtime 为一次关闭创建一个 `ShutdownDeadline`，在 `shutdown/halt` 及其异步变体返回 stage 前同步向所有管道广播相同 deadline 的关闭请求，再由协调线程聚合等待；不能为每条管道重新计算 timeout。并发 `GRACEFUL/IMMEDIATE` 请求单调升级，每个调用返回前都已广播自身要求的模式。到共同 deadline 时，对尚未终止的管道使用原 deadline 升级 `IMMEDIATE` 并返回或抛出聚合结果，后台继续等待 child 的真实 termination。

Runtime 自身只有在所有 child 真正终止后才能提交终止状态。启动失败回滚也使用同一个绝对 deadline；不得在仍有 child 处于 `STOPPING` 时把 Runtime 标记为已终止。

### Spring 可观测性

现有指标继续保留，并新增：

- `disruptor.runtime.healthy`
- `disruptor.pipeline.healthy`
- `disruptor.pipeline.consumers.total`
- `disruptor.pipeline.consumers.started`
- `disruptor.pipeline.consumers.alive`

指标标签不包含异常类型或消息，避免高基数。Actuator 健康状态映射为：正常 `UP`，锁存故障 `DOWN`，未运行或关闭中 `OUT_OF_SERVICE`。具体故障只出现在健康详情和一次性日志中。

## 快照不变量

- `0 <= aliveWorkers <= startedWorkers <= registeredWorkers`，封口后 `expectedWorkers == registeredWorkers`。
- `RUNNING` 蕴含登记已封口、worker 数大于 0、全部登记 worker 已启动且存活。
- `TERMINATED` 蕴含存活 worker/consumer 数为 0，且所有实际启动线程已经完成 `join`。
- `gracefulTermination` 蕴含 `TERMINATED + GRACEFUL + 无故障 + reachedRunning + drainCommitted + gracefulStopApplied + startedWorkers == registeredWorkers + aliveWorkers == 0`。
- 公开 `PipelineSnapshot` 的优雅终止还蕴含不再接收发布、created/started 数与封口 worker 数一致。
- 所有队列后端共享同一个 supervisor 状态、首因、deadline 和终止契约。
- 已接受发布要么完成发布并纳入关闭目标，要么调用方收到明确非成功结果。
- 关闭、失败或中断后，没有受管发布者永久等待容量。
- 消费线程永远不等待自身终止。
- Runtime 关闭耗时受单一总截止时间约束，不随管道数量线性叠加。
- 快照、健康和指标不得改变 RingBuffer 热路径。

## 验证

- NEW worker 不执行用户任务，未全部入场不能进入 RUNNING；
- worker 在 STARTING、RUNNING、QUIESCING 提前退出均 fail-stop；
- drain 一直 pending 时，外部立即升级和 deadline 强制升级都不依赖回调返回；
- 后端阶段动作不并发，每个模式至多应用一次；
- `QUIESCING → STOPPING → TERMINATED` 顺序和中间状态可观察；
- 抗中断 worker 退出前 termination 不完成；
- LMAX 在途 publisher、固定 cursor、叶子 gating 和 halt 顺序；
- RingBuffer 满时非阻塞、超时、中断、关闭和故障发布；
- 多管道启动失败回滚、共享截止时间、首因和真实终止聚合；
- 快照、Micrometer 和 Actuator 状态一致性；
- 原生 topology、rewind、自定义 processor 与 `maxBatchSize` 回归。

## 参考

- LMAX Disruptor 4.0.0 `BatchEventProcessor.halt()`、`Disruptor.shutdown()`、`MultiProducerSequencer` 与官方用户文档；
- Apache Log4j2 `AsyncLoggerDisruptor` 的发布停止与关闭顺序；
- Agrona `AgentRunner` 的线程生命周期和异常处理边界；
- `cn.wjybxx.commons.disruptor` 的 `ProducerBarrier`、`ConsumerBarrier.alert()`、可中断 claim 和 gating barrier 移除；
- `Commons-Concurrent` 的 `DisruptorEventLoop` 分阶段关闭与 worker-owned cleanup。
