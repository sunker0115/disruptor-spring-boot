# disruptor-spring-boot 架构设计

本文描述当前 `1.0.0` 开发版本的实现边界、核心决策和验证策略，面向维护者与贡献者。使用方式和运行注意事项见[项目 README](../README.md)。

## 技术基线

| 组件 | 当前版本 |
| --- | --- |
| JDK | 21+ |
| Spring Boot | 4.1.0 |
| LMAX Disruptor | 4.0.0 |
| 构建工具 | Maven |

版本表记录当前仓库实际编译和测试基线。支持其它版本组合前，需要补充兼容性测试，不能只根据依赖解析成功作出承诺。

## 目标与非目标

### 目标

- 引入 Starter 并声明 `PipelineSpec` Bean 后即可由 Spring 托管管道；
- 保留 LMAX Disruptor 的原生拓扑、处理器和扩展能力；
- 统一命名注册、配置合并、发布准入、启动回滚和有界关闭；
- 提供与管道共享监督内核的有界/无界 EventLoop、调度与固定 Group；
- 受管发布不使用反射或动态代理，高级场景保留原生零代理逃生口；
- 纯 Java 与 Spring Boot 使用相同的 core/concurrent 运行时。

### 非目标

- 不实现另一套阶段注解或 DAG DSL；
- 不提供持久化、跨进程传输、自动重试、失败补偿或消费者自动恢复；
- 不推断业务事件字段，也不自动清理复用槽位；
- 不把生命周期状态包装成消费者健康状态；
- 不兼容 Commons 自研 Future、组件框架或二进制 API；
- 不创建隐藏的全局 EventLoop；
- 不在核心模块中引入 Spring 或 Micrometer。

Maven Central 发布、签名、CI、许可证和社区治理属于开源工程，不属于运行时架构，但在公开发布前必须单独完成。

## 核心决策

采用 `PipelineSpec<E> + DisruptorTopology<E> + PipelineHandle<E> + ManagedPipeline<E> + DisruptorRuntime`。业务在构建期使用原生拓扑 API，运行期默认通过受管发布入口进入 RingBuffer。

| 方案 | 原生能力 | Starter 提供的价值 | 长期成本 | 结论 |
| --- | --- | --- | --- | --- |
| 自研阶段注解和 DAG | 需要持续复制 handler、rewind、processor 和异常 API，容易遗漏 | 高 | 高 | 不采用 |
| 业务直接创建 `Disruptor<E>` Bean | 完整 | 生命周期、命名和配置样板仍由业务承担 | 低 | 不采用 |
| Starter 构建实例，业务配置原生 topology | 完整 | 托管基础设施并保留原生语义 | 低 | 采用 |

这个选择与 Disruptor 自身“预分配 RingBuffer、显式消费者依赖图”的模型保持一致。Starter 不重写 `EventProcessor`，只补齐上游刻意交给调用方负责的发布停止、排空边界和线程终止协议。

### 关闭方案对比

| 方案 | 能否解决启动后立即关闭 | 能否阻止关闭期发布 | 是否等待线程退出 | 结论 |
| --- | --- | --- | --- | --- |
| 等待消费者报告 `isRunning()` 后再返回 `start()` | 仅覆盖启动竞态 | 否 | 否 | 不采用；消费者后续异常退出仍会破坏关闭 |
| 直接调用 LMAX `Disruptor.shutdown()` | 否；上游会忽略尚未运行的消费者 | 否；上游明确要求调用前停止发布 | 否 | 不采用 |
| 只比较 cursor 与最小 gating sequence | 是 | 否 | 否 | 不采用；仍缺少完整关闭边界 |
| 受管发布准入 + 固定目标游标 + gating 排空 + halt + join | 是 | 受管入口可以 | 是 | 采用 |

[LMAX 4.0.0 `Disruptor.shutdown()`](https://github.com/LMAX-Exchange/disruptor/blob/4.0.0/src/main/java/com/lmax/disruptor/dsl/Disruptor.java) 明确要求调用前停止发布，其 backlog 检查还会通过 [`ConsumerRepository`](https://github.com/LMAX-Exchange/disruptor/blob/4.0.0/src/main/java/com/lmax/disruptor/dsl/ConsumerRepository.java) 排除未报告运行的消费者。[Apache Log4j2](https://github.com/apache/logging-log4j2/blob/2.x/log4j-core/src/main/java/org/apache/logging/log4j/core/async/AsyncLoggerDisruptor.java) 的成熟集成同样先关闭发布入口，再排空并调用上游关闭。本项目保留 `unsafeRingBuffer()` 作为高级逃生口，但不为绕过受管入口的并发发布作无损关闭承诺。

## 模块与依赖边界

```text
                       业务代码
                 ┌────────┴────────┐
                 │                 │
       PipelineSpec / topology   EventLoop / Group
                 │                 │
                 ▼                 ▼
          disruptor-core ◀── disruptor-concurrent
                 │          （有界/无界同一 kernel）
                 └────────┬────────┘
                          ▼
          disruptor-spring-boot-autoconfigure
          ├─ core Runtime 生命周期与配置
          ├─ concurrent 根 owner 生命周期
          └─ 可选 Health / Micrometer 快照适配
                          │
                          ▼
          disruptor-spring-boot-starter
          （不传递 disruptor-concurrent）
```

`disruptor-core` 提供 `SupervisedLifecycle`、`WorkerSupervisor`、`ShutdownDeadline` 以及管道运行时；`disruptor-concurrent` 直接依赖 core、LMAX 与 SLF4J，不依赖 Spring。有界与无界只替换 `TaskQueue` 后端，准入、注册表、调度、取消、生命周期与 worker 循环只有一个 `EventLoopKernel` 实现。autoconfigure 将 concurrent、Boot Health 和 Micrometer 都保持为 optional；Starter 不强制引入 concurrent。示例、教程和基准模块不进入 Starter 的传递依赖。

## 核心模型

### PipelineSpec

`PipelineSpec<E>` 是单条管道的强类型定义。

必要字段：

- `name`：全局唯一且不能为空白；
- `eventType`：用于运行时类型校验；
- `eventFactory`：显式预分配事件，不要求无参构造；
- `topology`：接收原生 `Disruptor<E>`。

可选显式覆盖：

- `bufferSize`；
- `ProducerType`；
- `Supplier<? extends WaitStrategy>`；
- `ThreadFactory`；
- `ExceptionHandler<? super E>`；

等待策略使用工厂而不是共享实例，保证每条管道获得独立对象。`PipelineSpec` 的显式配置是代码级权威源，优先于外部属性。

### PipelineHandle

`PipelineHandle<E>` 公开：

- 管道名与事件类型；
- 0 至 3 参数 translator 的受管 `publishEvent/tryPublishEvent`；
- `publish(...)`、`tryPublish(...)` 和 `remaining()` 便捷方法；
- 显式命名的原生逃生口 `unsafeRingBuffer()`。

#### 发布所有权模型

`ProducerType.SINGLE/MULTI` 决定 RingBuffer 的生产者并发模型；受管发布/外部托管发布决定生产者生命周期由谁负责。两者是正交维度：

| 发布所有权 | SINGLE | MULTI | 关闭责任 |
| --- | --- | --- | --- |
| Runtime 受管 | volatile 双标志握手 | 原子计数准入 | Runtime 关闭入口、等待在途发布并排空 |
| 应用外部托管 | 原生零代理 | 原生零代理 | 应用先停止生产者，Runtime 再排空 |

受管发布通过无锁准入协议记录在途发布。Runtime 进入 `QUIESCING` 后拒绝新发布，等待已获准发布完成后才捕获排空目标，因此“已经 claim 但 translator 尚未返回”的槽位不会被漏掉。`SINGLE` 利用其至多一个发布者的上游契约，通过两个 volatile 状态完成准入与关闭握手；`MULTI` 使用原子计数精确记录并发发布者。两条路径提供相同的关闭语义。

外部托管发布通过 `unsafeRingBuffer()` 使用完整原生 API，不经过准入协议。应用必须遵守：

```text
停止外部入口 → 等待全部生产线程退出 → DisruptorRuntime.shutdown()
```

同一管道可以混用两种入口，但 Runtime 只能证明受管发布的关闭边界。只要存在原生发布者，整条管道的无损关闭就以“原生生产者已停止”为前置条件；Runtime 无法从 cursor 判断一个已经通过业务检查但尚未 claim 的原生发布者。

撮合教程展示外部托管发布的完整边界：并发 HTTP 请求先提交到容量等于 RingBuffer 的有界单线程入口，只有该线程调用 `unsafeRingBuffer().tryPublishEvent(...)`，因此 `ProducerType.SINGLE` 的所有权契约不依赖 Web 容器线程模型。请求等待该次发布完成后返回：`202` 表示已经入环，`429` 表示确定未入环。入口实现 `SmartLifecycle`，phase 比 `DisruptorLifecycle` 大 1；Spring 启动时 Runtime 先就绪，停止时入口先拒绝新请求并排空已接收的发布任务，随后 Runtime 才捕获游标并排空消费者。入口队列与 RingBuffer 都是有界的，不使用 `CallerRunsPolicy`，避免请求线程在过载时越权成为第二个生产者。

不增加 `publication-mode` 配置。配置开关会让同一个 `publishEvent()` 在不同环境下具有不同关闭语义，代码审查无法从调用点判断责任归属。显式的 `publishEvent/tryPublishEvent` 与 `unsafeRingBuffer()` 让性能选择和生命周期责任同时出现在代码中。

句柄不复制 `EventSink` 的批量与 varargs 重载。需要这些能力或完全零代理时使用 `unsafeRingBuffer()`。运行期不公开原生 `Disruptor<E>`，防止业务绕过 Runtime 操作消费者生命周期；只开放 RingBuffer 不会允许调用方自行 `start()`、`halt()` 或 `shutdown()`。

### DisruptorRuntime

Runtime 使用名称索引管道。同一事件类型允许对应多条管道：

- `require(name, type)` 同时校验名称与事件类型；
- `unique(type)` 只在该类型唯一时返回句柄，零匹配或多匹配都失败。

状态机为：

```text
NEW → STARTING → RUNNING → QUIESCING → STOPPING → TERMINATED
 │                 │                         ▲
 └──── halt() ─────┴─────────────────────────┘

TERMINATED ── start() ──> IllegalStateException
```

`isRunning()` 只表示 Runtime 处于 `RUNNING` 状态，不探测每个消费者线程是否存活。

生命周期不变量：

- 处于 `RUNNING` 时重复 `start()` 幂等，重复 `shutdown()` 或 `halt()` 也幂等；
- 停止后不允许重新启动，因为 LMAX Disruptor 本身不支持重启；
- topology 不得自行调用 `start()`；
- 启动失败时，逆序 `halt` 所有已经尝试启动的管道，包括部分启动的当前管道；
- `shutdown()` 先关闭所有受管发布入口并等待在途发布，再为每条管道捕获固定目标游标；
- 正常关闭按启动逆序逐条排空；排空只比较最小 gating sequence 与目标游标，不依赖消费者 `isRunning()`；
- 排空完成后调用上游 `halt()`，并等待 Runtime 跟踪的消费线程真正退出；
- 全部管道共享一个 Runtime 级截止时间，关闭耗时不随管道数量线性累加；
- 单条管道排空超时或停止失败时强制 `halt`、中断仍存活线程，并继续关闭其它管道；
- Runtime 最终进入 `TERMINATED`；只要存在未排空、停止失败或线程未退出，就抛出聚合 `DisruptorShutdownException`；
- `isRunning()` 在 `QUIESCING` 开始时立即变为 `false`，且状态读取不被长时间关闭过程阻塞；它仍不是消费者健康检查。

“排空”只保证消费链末端 gating sequence 越过目标游标。自定义 processor 若提前推进 sequence，或 handler 内部再启动 fire-and-forget 副作用，这些外部工作不属于 Runtime 能证明完成的范围。

命名管道是彼此独立的生命周期单元，关闭协议不推断跨管道依赖。需要保证级联处理完整性的阶段应放在同一条 Disruptor topology 中；handler 向另一条受管管道继续发布的场景必须由应用先停止上游并自行编排关闭顺序，否则全局进入 `QUIESCING` 后下游发布会被拒绝。

### 监督生命周期内核

管道、EventLoop 与 Group 使用同一组阶段：`NEW → STARTING → RUNNING → QUIESCING → STOPPING → TERMINATED`。`start()` 成功的含义是启动登记已经封口、全部 worker 与 module 已完成启动、内部启动 token 已归零且公开 gate 已开放，不以“线程已创建”冒充 ready。

`ShutdownDeadline` 是同一次关闭广播的身份对象。正常提交要求启动落定、全部 child stage 完成且 token 清零；超时提交只等待已经接受广播的参与者归还 token，随后冻结原 outcome。迟到 rollback 只补充会话事实并继续后台收敛，不能重开广播、替换首因或伪造 termination。

### EventLoop 与 Group

`EventLoop` 同时实现 JDK `ScheduledExecutorService` 和项目的 `SupervisedScheduledExecutor`。bounded 后端用 LMAX RingBuffer，容量口径是尚未物理清理的全部 accepted 任务，包括入口、timer 和 executing；unbounded 后端使用分段 MPSC 队列并在消费后清除引用。两者共享 accepted sequence，因此立即任务和零延迟任务保持一个本地全序，timer 与 command 通过有限批次互相让行。

高级 `ScheduledTaskSpec` 以不可变字段表达 one-shot、fixed-rate、fixed-delay、dynamic-delay、expires、max executions、continue-on-failure、priority、显式 `TaskContext` 与 `CancellationToken`。Future 终态和物理任务状态分离：取消可以先完成 Future，但 bounded permit 只在任务引用从 queue/timer/执行路径真正清除后释放。

`EventLoopGroup` 拥有固定 child 集合，提供轮询 `next()` 与稳定 `select(affinityKey)`。Group gate 先于 child gate 获取准入 token，使组级关闭能冻结新任务并等待在途选择完成。child 生命周期只能由 Group 操作；任一 child 基础设施失败会以同一 deadline fail-stop 全组，Group termination 只在全部 child 真实终止后完成。

标准 `shutdown()` 使用无界 graceful deadline：保留已接受的一次性延迟任务，取消周期任务。`shutdownNow()` 通过 accepted registry CAS 返回本次取得所有权的未开始原始任务，并尽力中断 running task；它不等待用户任务退出。Spring、Group 与运维使用显式共享的有界 deadline，到期后单调升级 immediate，但仍等待线程真实退出。

## 配置模型

```yaml
disruptor:
  enabled: true
  lifecycle-phase: -2147483648
  shutdown-timeout: 10s
  defaults:
    buffer-size: 1024
    producer-type: MULTI
    wait-strategy: BLOCKING
    daemon-threads: false
    error-strategy: HALT
  pipelines:
    matching:
      producer-type: SINGLE
      buffer-size: 65536
  metrics:
    enabled: true
```

配置只在 Spring 自动配置边界合并：

```text
core 安全默认值
  < disruptor.defaults
  < disruptor.pipelines.<name>
  < PipelineSpec 显式值
```

如果希望某个参数由不同环境调整，`PipelineSpec` 中必须保持未设置，让外部属性提供该值。topology、事件类型和事件工厂只能在代码中定义。

默认管道设置为 `MULTI + BLOCKING + 1024 + 非 daemon + HALT`，Runtime 默认关闭总预算为 `10s`。配置层只提供无参数等待策略预设；需要构造参数或自定义实现时由 `PipelineSpec` 提供原生工厂。

`shutdown-timeout` 是 Runtime 级总预算，不进入命名管道覆盖和 `PipelineSpec`。把它设为每管道配置会导致 N 条管道最坏关闭 N 倍时长，也无法与 Spring 的整体关闭窗口对齐，因此不采用。

命名配置没有对应 `PipelineSpec` 时启动失败，避免拼写错误被静默忽略。容量、关闭时间等非法值在构建阶段失败，错误信息包含管道名。

## 异常处理语义

| 策略 | 当前消费者序列 | 下游是否可能看到失败槽位 | 适用场景 |
| --- | --- | --- | --- |
| `HALT` | 不再推进 | 否 | 默认；强调链式处理一致性，接受人工恢复 |
| `LOG_AND_CONTINUE` | 推进 | 是 | 终端消费者、幂等处理或明确接受部分处理 |

默认异常处理通过 `Disruptor.setDefaultExceptionHandler(...)` 在 topology 装配前设置，使存量和后续 processor 共享同一默认策略。处理器级策略仍使用原生 `handleExceptionsFor(...)`。

Runtime 不包装业务 handler，也不实现自动重试。自动重试需要定义幂等、顺序、最大次数和失败去向，属于业务策略，不能由基础设施猜测。

## 热路径与事件语义

- topology 完成后，业务 handler 由 LMAX processor 直接调用；
- 受管 `publishEvent/tryPublishEvent` 直接调用同一个 `RingBuffer<E>`；`SINGLE` 只增加 volatile 准入握手，`MULTI` 增加原子计数，不使用反射、动态代理或包装 translator；
- `PipelineHandle.publish(...)` 与 `tryPublish(...)` 会为每次调用创建 translator lambda，只定位为便捷 API；
- 静态 translator 的受管发布路径不创建包装 lambda；完全零代理路径使用 `unsafeRingBuffer()`；
- translator 抛异常时，Disruptor 仍发布已经领取的槽位，因此 translator 不得包含可能失败的业务逻辑；
- 事件对象循环复用，发布方必须覆盖本次所需字段；
- 可选字段清理由业务在 topology 叶子后显式注册；
- 分片使用原生 handler 模式或自定义 processor，不提供压缩生命周期回调的并行包装器。

## Spring Boot 自动配置

基础自动配置生效条件：

- classpath 存在 `Disruptor`；
- `disruptor.enabled` 缺省或为 `true`。

Bean 回退条件彼此独立：

- 缺少 `DisruptorRuntime` 时，根据全部 `PipelineSpec<?>` Bean 构建 Runtime；
- 缺少 `DisruptorLifecycle` 时，为当前 Runtime 创建生命周期适配器；
- 用户可以只替换 Runtime、只替换生命周期，或同时替换两者。

零条 `PipelineSpec` 是合法状态，会创建空 Runtime。命名属性指向不存在的管道时仍然失败。

`DisruptorLifecycle` 实现 `SmartLifecycle`，默认 phase 为 `Integer.MIN_VALUE`，使管道尽早启动、尽晚停止。它显式实现 `stop(Runnable)`：Runtime 只有在排空、halt 和线程等待完成后才返回，因此正常 callback 不会早于消费线程退出；即使关闭抛出聚合异常，也会在 `finally` 中执行 callback，避免 Spring 生命周期处理器继续等待。

classpath 存在 `disruptor-concurrent` 且应用声明 `SupervisedScheduledExecutor` 根 Bean 时，`DisruptorConcurrentLifecycle` 才装配。它过滤公开为 Bean 的 Group child，只管理 standalone loop 与 Group 根对象；启动失败会以同一有界 deadline 回滚全部根对象，关闭 callback 只在全部根对象真实 termination 后执行。自动配置不创建业务 EventLoop 或全局单例。

concurrent 的配置面只有 `disruptor.concurrent.enabled`、`lifecycle-phase` 和 `shutdown-timeout`。Health 与 Metrics 是独立条件层：缺少 Boot Health 或 Micrometer 时，基础生命周期仍能装配。

自动配置模块生成：

- `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`；
- `META-INF/spring-configuration-metadata.json`；
- `META-INF/spring-autoconfigure-metadata.properties`。

## 可观测性

Micrometer classpath、`MeterRegistry` Bean 和 `disruptor.metrics.enabled=true` 同时成立时，注册以下只读 Gauge：

- `disruptor.runtime.running`；
- `disruptor.pipeline.buffer.size`；
- `disruptor.pipeline.remaining.capacity`；
- `disruptor.pipeline.backlog`。

Gauge 在采集时读取原生状态，不进入发布或消费热路径。`runtime.running` 是生命周期状态，`backlog` 是基于 cursor 与最小 gating sequence 的近似值，两者都不是消费者健康检查。

concurrent 指标同样只读取 `EventLoopSnapshot`/`EventLoopGroupSnapshot`，覆盖 gate、worker、任务结果、调度积压和队列事实。健康判断只看基础设施：存在首个 worker/module failure 为 `DOWN`，全部根对象 RUNNING 且接受任务为 `UP`，其余状态为 `OUT_OF_SERVICE`；业务任务失败计数不改变基础设施健康。

## 验证策略

单元测试与集成测试使用真实 Disruptor，覆盖：

- 原生 `sequence`、`endOfBatch`、批次、生命周期和 sequence callback；
- rewind、自定义 processor、processor factory 与 translator 重载；
- 默认 `HALT`、显式 `LOG_AND_CONTINUE` 和处理器级异常策略；
- 同事件类型多命名管道与类型校验；
- 消费线程尚未进入 `run()` 时立即关闭仍完整排空，并重复执行并发回归场景；
- 在途发布先于关闭目标完成、`QUIESCING` 后新发布被拒绝；
- 部分启动回滚、逆序关闭、线程 join、总预算超时、停止失败聚合和禁止重启；
- 配置覆盖、非法配置、未知命名配置和零管道；
- 自动配置开关、自定义 Bean 回退、AOT 元数据与可选 Micrometer；
- 指标在启动、积压、排空和停止阶段的变化；
- bounded/unbounded queue 与 EventLoop 同一契约、Group 所有权和 fail-stop；
- JDK 调度、dynamic-delay、priority、取消、阻塞保护和 shutdownNow 返回值；
- concurrent 生命周期、Health、Metrics 的独立条件装配与 callback 时序；
- example 与 tutorial 的真实端到端流程，以及 tutorial 并发 HTTP 请求的单生产者串行化。

全仓验证命令：

```bash
mvn test
```

性能验证使用 `disruptor-benchmarks` 中的 JMH 基准，对比原生 LMAX、core 受管发布、bounded/unbounded EventLoop 与 JDK 单线程 executor；可选 profile 对比固定参考 commit 的 Commons EventLoop。性能结论必须记录硬件、JVM、参数、预热和测量配置；普通单测不设置易受机器噪声影响的吞吐阈值。

## 演进约束

- 新能力优先暴露原生 Disruptor 入口，不复制上游 API；
- 新配置必须具有明确默认值、优先级和失败语义；
- 修改生命周期、发布准入或异常策略时，必须先补真实并发行为测试；
- bounded/unbounded 不得分叉 kernel、调度或生命周期语义；
- Group child 不得出现第二个生命周期 owner；
- 公开 API 的破坏性变更需要在发布说明中明确记录；
- README 只保留使用者开始使用所需内容，内部取舍和不变量写入本文；
- 文档中的版本、默认值、命令和模块名必须能由当前仓库验证。
