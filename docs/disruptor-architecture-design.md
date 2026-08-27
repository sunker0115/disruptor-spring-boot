# Disruptor Spring Boot 最终架构

## 目标

Starter 必须做到：

- 引入依赖并声明 `PipelineSpec` Bean 后即可运行；
- 不损失、不重命名、不重新解释 LMAX Disruptor 的原生能力；
- 不在事件热路径增加反射、代理、捕获型填充器或异常吞噬；
- Spring 只负责基础设施装配和生命周期；
- 纯 Java 与 Spring 使用同一个核心运行时。

发布工程、签名和中央仓库配置不在本设计范围。

## 决策

选择 `PipelineSpec<E> + DisruptorTopology<E> + PipelineHandle<E> + DisruptorRuntime`，不维护自研阶段注解或 DAG DSL。

候选方案比较：

| 方案 | 能力完整性 | Starter 价值 | 演进成本 | 结论 |
| --- | --- | --- | --- | --- |
| 自研阶段注解和 DAG | 必须持续复制 handler、rewind、processor、异常等 API，必然遗漏 | 高 | 高 | 拒绝 |
| 业务直接创建 `Disruptor<E>` Bean | 完整 | 生命周期、命名和配置样板仍由业务承担，价值低 | 低 | 拒绝 |
| Starter 构建实例，业务用原生 topology | 完整 | 托管基础设施，同时保留全部原生语义 | 低 | 采用 |

## 边界

```text
业务代码
  ├─ PipelineSpec<E>
  ├─ EventFactory<E>
  ├─ EventHandler / RewindableEventHandler / EventProcessor
  └─ 原生 topology 回调
          │
          ▼
disruptor-core
  ├─ 合并 PipelineSpec 与 PipelineSettings
  ├─ 构建 Disruptor<E>
  ├─ 名称注册与类型校验
  └─ 一次性启动、逆序关闭、超时 halt
          │
          ▼
Spring Boot 自动配置
  ├─ 收集 PipelineSpec Bean
  ├─ 解析 defaults 与 named overrides
  ├─ 拒绝未知命名配置
  └─ SmartLifecycle 委托 DisruptorRuntime
```

core 不依赖 Spring。自动配置不扫描业务方法，不反射构造事件，不生成业务运行时 hints。

## 核心模型

### PipelineSpec

必要字段：

- `name`：全局唯一管道名；
- `eventType`：运行时类型校验；
- `eventFactory`：显式预分配工厂，不要求无参构造；
- `topology`：直接接收 `Disruptor<E>`。

可选显式覆盖：

- `bufferSize`；
- `ProducerType`；
- `Supplier<? extends WaitStrategy>`；
- `ThreadFactory`；
- `ExceptionHandler<? super E>`；
- `shutdownTimeout`。

`PipelineSpec` 的显式值优先于外部配置。等待策略使用工厂而不是共享实例，保证每条管道得到独立策略对象。

### PipelineHandle

运行时句柄公开：

- 管道名和事件类型；
- 原生 `Disruptor<E>`；
- 同一个原生 `RingBuffer<E>`；
- 已启动状态。

不复制 `EventSink` 的几十个重载。发布方直接调用 `RingBuffer`，因此单条、批量、translator、poller、barrier、容量和序列能力不会退化。

### DisruptorRuntime

注册表按名称索引，同一事件类型允许多条命名管道。`require(name, type)` 同时校验名称和类型；`unique(type)` 仅在类型唯一时可用，歧义必须失败。

状态机只有 `NEW → RUNNING → STOPPED`。LMAX Disruptor 不支持停止后重启，Runtime 不伪装此能力。

启动失败时逆序 halt 已启动管道。正常关闭按启动逆序、使用每条管道自己的超时排空；超时或失败时 halt，并继续处理其余管道。

## 配置模型

```yaml
disruptor:
  enabled: true
  lifecycle-phase: -2147483648
  defaults:
    buffer-size: 1024
    producer-type: MULTI
    wait-strategy: BLOCKING
    shutdown-timeout: 10s
    daemon-threads: false
    error-strategy: HALT
  pipelines:
    matching:
      producer-type: SINGLE
      buffer-size: 65536
```

合并只发生在自动配置边界：

```text
core 安全默认值
  < disruptor.defaults
  < disruptor.pipelines.<name>
  < PipelineSpec 显式值
```

因 `PipelineSpec 显式值` 位于链尾（最高优先级），一旦 spec 在代码里设置了某个基建旋钮（如 `bufferSize`、`waitStrategy`、`shutdownTimeout`、`errorStrategy`），`disruptor.pipelines.<name>` 与 `disruptor.defaults` 便无法再覆盖它。这是"编程优先"框架下的自然取舍——PipelineSpec 是权威源，属性只填充其未指定的字段。因此若希望某旋钮可由外部配置按环境调优（dev/prod 不同 buffer 等），spec 中应保持该字段未设置（留空），交由 `disruptor.pipelines.<name>` / `disruptor.defaults` 填充。topology、事件类型与工厂等只能存在于代码，不在此取舍范围内。

默认使用 `MULTI + BLOCKING + 1024 + 非 daemon + 10s + HALT`。异常处理统一走 `setDefaultExceptionHandler`：框架默认注入 `HALT`，基于 SLF4J 记录后终止失败消费者，保证链式拓扑的失败槽位不会继续流向下游。`LOG_AND_CONTINUE` 仅用于终端消费者、幂等处理或业务明确接受部分处理的场景；它吞掉异常并推进消费序列，因此依赖当前处理器的下游仍会看到该槽位。也可用 `PipelineSpec.exceptionHandler(...)` 提供完整自定义。

配置只为无参数常见等待策略提供枚举。需要构造参数或自定义实现时，由 `PipelineSpec` 提供原生策略工厂。

## 热路径与语义

- topology 注册完成后，处理器由 LMAX `BatchEventProcessor` 直接调用；
- Starter 不把处理器压缩为 `Consumer<E>`；
- 事件处理异常交由 Disruptor 默认 `ExceptionHandler` 处置（框架默认 `HALT`：记录后终止失败消费者）；Starter 不在消费者外再包裹 try-catch 委托层；处理器级策略走原生 `handleExceptionsFor`；
- 发布直接访问同一个 `RingBuffer<E>`，没有委托层和状态分支；
- 静态 translator 可避免捕获型 lambda 分配，但业务处理器与参数对象分配不属于 Starter 承诺；
- translator 抛出异常时仍发布已领取槽位，这是 LMAX 官方契约，文档要求 translator 不抛异常；
- 可选字段清理由业务在 topology 叶子后显式注册，Starter 不猜测事件字段；
- 分片使用官方取模 handler 模式或业务自定义 processor，不提供会压扁生命周期回调的并行包装器。

## Spring Boot

自动配置条件：

- classpath 存在 `Disruptor`；
- `disruptor.enabled` 缺省或为 `true`；
- 用户未提供自定义 `DisruptorRuntime` 或 `DisruptorLifecycle`。

自动配置收集所有 `PipelineSpec<?>` Bean，一次性构建完整 Runtime。命名配置没有对应 Spec 时启动失败。

`SmartLifecycle` 默认 phase 为 `Integer.MIN_VALUE`，因此最早启动、最晚停止。它只委托 Runtime，不复制关闭逻辑。

自动配置模块生成：

- `META-INF/spring-configuration-metadata.json`；
- `META-INF/spring-autoconfigure-metadata.properties`；
- `AutoConfiguration.imports`。

Micrometer 是可选依赖。存在 `MeterRegistry` 时注册只读容量、积压和运行状态 Gauge；指标采集不进入发布或消费热路径。

Starter 引入 `spring-boot-starter`，使用方只依赖 Starter 即具备完整基础 Spring Boot 运行依赖。

## 验收

核心测试必须使用真实 Disruptor，覆盖：

- 原生 `sequence/endOfBatch/onBatchStart/onStart/onShutdown/setSequenceCallback`；
- 默认 `HALT` 不向下游传播失败槽位、显式 `LOG_AND_CONTINUE` 推进序列，以及处理器级异常处理；
- rewind、自定义 processor 和 processor factory 的 topology 可达性；
- 0/1/2/3 参数与批量 translator；
- 无无参构造的事件工厂；
- 同事件类型多命名管道；
- 配置覆盖、非法容量、未知命名配置；
- 启动回滚、逆序关闭、超时 halt、停止后禁止重启；
- Spring 自动配置条件、生命周期 phase、配置元数据；
- example 与 tutorial 真实端到端流程。

性能验证应使用独立 JMH 基准比较原生手写 Disruptor 与 Runtime 构建后取得的 `RingBuffer`，并使用 `-prof gc` 验证静态 translator 发布路径。普通单测不设置易受机器噪声影响的吞吐阈值。
