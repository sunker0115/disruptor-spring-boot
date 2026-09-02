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
- `DisruptorRuntime` 只聚合多条管道，不再拥有另一套管道状态判断。
- Spring 健康和指标只读取统一快照，不在发布或消费热路径埋点。

## 非目标

- 不重写 LMAX Sequencer、SequenceBarrier、RingBuffer 或原生 topology DSL。
- 不包装业务 EventHandler，不实现自动重试、补偿或消费者自动重启。
- 不承诺绕过 `unsafeRingBuffer()` 的外部发布与 Runtime 并发关闭时无损。
- 不把 `maxBatchSize` 错误抽象成公平调度参数；它仍是原生处理器的批边界配置。

## 方案比较

| 方案 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- |
| 在现有 `ManagedPipeline` 局部增加线程存活判断 | 改动少 | 生命周期、故障、发布、指标继续各自判断，无法支持独立 EventLoop 关闭 | 不采用 |
| 直接引入 `cn.wjybxx.commons.disruptor` | 获得可中断 barrier 和无界实现 | 替换 LMAX 语义、生态和 topology，形成第二套底座 | 不采用 |
| 把单条 LMAX 管道提升为受监督生命周期单元 | 保留 LMAX，统一关闭、故障和观察模型，可供 concurrent 复用 | 需要重构 core | 采用 |

## 最终模型

### 类型与职责

- `WorkerSupervisor`：core 内可供同仓模块复用的线程监督 SPI，只管理状态、首因、停止请求与终止信号，不感知任务队列。
- `DisruptorPipeline<E>`：组合 `WorkerSupervisor` 的单条 LMAX 管道生命周期控制器，内部实现保持不可替换。
- `PipelineHandle<E>`：发布入口与快照读取，不拥有生命周期。
- `PipelineSnapshot`：不可变状态事实源。
- `DisruptorRuntime`：构建并聚合多条 `DisruptorPipeline`，负责批量启动、回滚和总截止时间等待。

核心接口语义：

```java
public interface DisruptorPipeline<E> {
    PipelineHandle<E> handle();
    PipelineSnapshot snapshot();
    void start();
    void requestShutdown(ShutdownMode mode);
    CompletionStage<PipelineSnapshot> termination();
}
```

`requestShutdown` 只发出幂等请求，不阻塞调用线程。真正的排空、halt 和 join 由 JDK 21 虚拟控制线程执行；该线程只存在于关闭或故障冷路径。`DisruptorRuntime.shutdown()` 先向所有管道发出请求，再在同一个 Runtime 级截止时间内等待各自 `termination()`。

`WorkerSupervisor` 不是通用线程池框架。它只服务于本仓库中两种已经确定的单线程消费者：LMAX 管道 processor，以及 concurrent 显式无界后端的 worker。这样无界后端无需伪装成 LMAX RingBuffer，也不会复制第二套生命周期状态机。

### 状态与健康

生命周期固定为：

```text
NEW → STARTING → RUNNING → QUIESCING → STOPPING → TERMINATED
```

健康由快照派生，不增加可独立漂移的状态机：

- `NEW/STARTING`：`STARTING`；
- `RUNNING` 且全部已创建消费者线程存活、无故障：`HEALTHY`；
- `RUNNING` 但消费者数量不足或已有故障：`UNHEALTHY`；
- `QUIESCING/STOPPING`：`OUT_OF_SERVICE`；
- `TERMINATED`：`TERMINATED`。

`PipelineSnapshot` 至少包含：管道名、生命周期、健康、是否接受发布、预期/已创建/存活消费者数、RingBuffer 容量和近似积压、首个故障、是否优雅终止。

`PipelineSnapshot.builder()` 只接收除 `health` 外的状态事实字段；`build()` 内部派生健康状态，再通过 canonical constructor 保持所有不变量校验。

故障与生命周期分轴。关闭期间不得清除、包装掉或用后续异常覆盖首个故障。

### 消费者监督

托管 `ThreadFactory` 包装每个 LMAX 消费线程：

1. 线程进入时登记存活；
2. 正常退出时注销；
3. `RUNNING` 期间退出，无论由未捕获异常还是 processor 意外返回，都触发该管道 fail-stop；
4. 捕获到的异常原样重新抛出，让用户配置的 `UncaughtExceptionHandler` 仍能收到原始异常；
5. 故障回调只请求停止，不在当前消费者线程中等待终止。

一个消费者失败后：立即关闭发布准入、锁存首个故障、halt 其余 processor、唤醒容量等待者，并在控制线程中等待其他消费线程退出。不会停止 Runtime 中无关的其他管道。

### 发布协议

删除无限阻塞的受管 `publishEvent(...)`。0 至 3 参数 translator 统一提供：

```java
PublicationResult tryPublishEvent(...);
PublicationResult publishEvent(..., Duration timeout) throws InterruptedException;
```

结果固定为：

- `PUBLISHED`：事件已经发布；
- `CAPACITY_EXHAUSTED`：非阻塞尝试时容量不足；
- `TIMED_OUT`：有界等待到期；
- `NOT_RUNNING`：未启动或已开始关闭；
- `PIPELINE_FAILED`：管道已锁存故障。

有界等待通过反复调用 LMAX `tryPublishEvent` 实现，绝不手工 claim。等待循环必须检查中断、截止时间、发布准入和管道故障；关闭或故障必须唤醒等待者。translator 只会在成功取得容量后执行，仍遵守“translator 不得抛出业务异常”的 LMAX 约束。

`unsafeRingBuffer()` 保留完整 LMAX API，包括批量、varargs、原始 sequence claim/publish。名称继续明确表示它绕过受管生命周期，而不是 RingBuffer 本身线程不安全。

### 关闭协议

优雅关闭：

1. CAS 进入 `QUIESCING`，拒绝新发布并唤醒等待者；
2. 等待已经获准的发布完成；
3. 捕获固定目标 cursor；
4. 等待所有叶子 gating sequence 越过目标；
5. 进入 `STOPPING`，调用 LMAX `halt()`；
6. 中断并等待仍存活线程；
7. 完成终止快照与 `termination()`。

故障或立即停止不等待无法推进的 gating sequence，直接 halt、唤醒和 join。关闭 deadline 只用于判定优雅等待或普通 join 已经超时：到期时锁存首个 `WorkerTerminationTimeoutException`、把请求模式单调升级为 `IMMEDIATE`、中断 worker，并由同一个控制线程串行执行一次 `StopAction(IMMEDIATE)`；deadline 不因升级而延长。

超时不是终止捷径。抗中断 worker 仍存活时，生命周期保持 `STOPPING`，`termination()` 不完成；控制线程在 deadline 后通过线程终止通知继续等待，不做忙轮询。只有所有已经进入监督范围的 worker 实际退出，才能在同一状态锁内提交 `TERMINATED` 和终止快照。因此 `TERMINATED` 永远蕴含存活 worker 数为 0，该语义同时适用于后续 `EventLoop` 的 `awaitTermination`。线程退出、停止异常和超时都聚合到终止结果，但首个故障保持主因。

### Spring 可观测性

现有指标继续保留，并新增：

- `disruptor.runtime.healthy`
- `disruptor.pipeline.healthy`
- `disruptor.pipeline.consumers.total`
- `disruptor.pipeline.consumers.alive`

指标标签不包含异常类型或消息，避免高基数。Actuator 健康状态映射为：正常 `UP`，锁存故障 `DOWN`，未运行或关闭中 `OUT_OF_SERVICE`。具体故障只出现在健康详情和一次性日志中。

## 关键不变量

- 管道只有一个生命周期与故障事实源。
- 所有队列后端共享同一个 `WorkerSupervisor` 状态、故障和终止契约。
- `RUNNING + HEALTHY` 必须意味着所有预期消费者均存活。
- 已接受发布要么完成发布并纳入关闭目标，要么调用方收到明确非成功结果。
- 关闭、失败或中断后，没有受管发布者永久等待容量。
- 消费线程永远不等待自身终止。
- deadline 到期只触发首因和 `IMMEDIATE` 升级，不得在 worker 存活时完成 `termination()`。
- `TERMINATED` 快照中的存活 worker 数必须为 0。
- Runtime 关闭耗时受单一总截止时间约束，不随管道数量线性叠加。
- 快照、健康和指标不得改变 RingBuffer 热路径。

## 验证

- 消费者正常、异常和无异常提前退出的监督测试；
- 多消费者中一个失败后整条管道 fail-stop；
- RingBuffer 满时非阻塞、超时、中断、关闭和故障发布测试；
- 发布与 quiesce 竞态测试；
- 关闭从消费线程发起时不死锁；
- 启动失败回滚、共享截止时间和终止主因测试；
- 快照、Micrometer 和 Actuator 状态一致性测试；
- 原生 topology、rewind、自定义 processor 与 `maxBatchSize` 回归测试。

## 参考

- LMAX Disruptor 4.0.0 `MultiProducerSequencer`、`BatchEventProcessorBuilder` 与官方用户文档；
- Apache Log4j2 `AsyncLoggerDisruptor` 的发布停止与关闭顺序；
- Agrona `AgentRunner` 的线程生命周期和异常处理边界；
- `cn.wjybxx.commons.disruptor` 的 `ProducerBarrier`、可中断 claim 和消费者屏障设计。
