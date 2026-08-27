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
- 保留 LMAX Disruptor 的原生拓扑、处理器、发布和扩展能力；
- 统一命名注册、配置合并、启动回滚和逆序关闭；
- 不在事件发布与消费热路径增加反射或代理层；
- 纯 Java 与 Spring Boot 使用同一个核心运行时。

### 非目标

- 不实现另一套阶段注解或 DAG DSL；
- 不提供持久化、跨进程传输、自动重试、失败补偿或消费者自动恢复；
- 不推断业务事件字段，也不自动清理复用槽位；
- 不把生命周期状态包装成消费者健康状态；
- 不在核心模块中引入 Spring 或 Micrometer。

Maven Central 发布、签名、CI、许可证和社区治理属于开源工程，不属于运行时架构，但在公开发布前必须单独完成。

## 核心决策

采用 `PipelineSpec<E> + DisruptorTopology<E> + PipelineHandle<E> + DisruptorRuntime`，由 Starter 构建实例，业务在回调中使用原生拓扑 API。

| 方案 | 原生能力 | Starter 提供的价值 | 长期成本 | 结论 |
| --- | --- | --- | --- | --- |
| 自研阶段注解和 DAG | 需要持续复制 handler、rewind、processor 和异常 API，容易遗漏 | 高 | 高 | 不采用 |
| 业务直接创建 `Disruptor<E>` Bean | 完整 | 生命周期、命名和配置样板仍由业务承担 | 低 | 不采用 |
| Starter 构建实例，业务配置原生 topology | 完整 | 托管基础设施并保留原生语义 | 低 | 采用 |

这个选择与 Disruptor 自身“预分配 RingBuffer、显式消费者依赖图”的模型保持一致。Starter 只补 Spring 基础设施，不重新解释并发模型。

## 模块与依赖边界

```text
业务代码
  ├─ PipelineSpec<E>
  ├─ EventFactory<E>
  ├─ EventHandler / RewindableEventHandler / EventProcessor
  └─ 原生 topology 回调
          │
          ▼
disruptor-core
  ├─ PipelineSpec 与 PipelineSettings 合并
  ├─ Disruptor<E> 构建
  ├─ 名称注册与类型校验
  └─ 一次性生命周期
          │
          ▼
disruptor-spring-boot-autoconfigure
  ├─ 收集 PipelineSpec Bean
  ├─ 解析默认配置与命名覆盖
  ├─ SmartLifecycle 委托
  └─ 可选 Micrometer Gauge
          │
          ▼
disruptor-spring-boot-starter
  └─ 面向使用方聚合依赖
```

`disruptor-core` 只依赖 Disruptor 与 SLF4J。自动配置模块可以依赖 Spring Boot，并把 Micrometer 保持为可选依赖。示例、教程和基准模块不进入 Starter 的传递依赖。

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
- `shutdownTimeout`。

等待策略使用工厂而不是共享实例，保证每条管道获得独立对象。`PipelineSpec` 的显式配置是代码级权威源，优先于外部属性。

### PipelineHandle

`PipelineHandle<E>` 公开：

- 管道名与事件类型；
- 原生 `Disruptor<E>` 与同一个 `RingBuffer<E>`；
- `hasStarted()`：底层实例是否曾经启动；
- `publish(...)`、`tryPublish(...)` 和 `remaining()` 便捷方法。

`hasStarted()` 在停止后仍返回 `true`，不能用于健康检查。生命周期默认由 Runtime 或 Spring 托管；业务不应通过 `disruptor()` 绕过托管流程调用 `start()`、`halt()` 或 `shutdown()`。

句柄不复制 `EventSink` 的全部重载。高性能或高级发布路径直接使用 `ringBuffer()`，避免 Starter API 随 Disruptor 演进而落后。

### DisruptorRuntime

Runtime 使用名称索引管道。同一事件类型允许对应多条管道：

- `require(name, type)` 同时校验名称与事件类型；
- `unique(type)` 只在该类型唯一时返回句柄，零匹配或多匹配都失败。

状态机为：

```text
NEW ── start() ──> RUNNING ── shutdown()/halt() ──> STOPPED
 │                    │
 └─ shutdown()/halt() ┘

STOPPED ── start() ──> IllegalStateException
```

`isRunning()` 只表示 Runtime 处于 `RUNNING` 状态，不探测每个消费者线程是否存活。

生命周期不变量：

- 处于 `RUNNING` 时重复 `start()` 幂等，重复 `shutdown()` 或 `halt()` 也幂等；
- 停止后不允许重新启动，因为 LMAX Disruptor 本身不支持重启；
- topology 不得自行调用 `start()`；
- 启动失败时，逆序 `halt` 所有已经尝试启动的管道，包括部分启动的当前管道；
- 正常关闭按启动逆序逐条排空；
- 单条管道排空超时或停止失败时强制 `halt`，并继续关闭其它管道；
- Runtime 最终进入 `STOPPED`；单条管道关闭或强制停止抛出运行时异常时，记录日志并继续后续清理。

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

默认管道设置为 `MULTI + BLOCKING + 1024 + 非 daemon + 10s + HALT`。配置层只提供无参数等待策略预设；需要构造参数或自定义实现时由 `PipelineSpec` 提供原生工厂。

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
- 发布直接访问原生 `RingBuffer<E>`，不经过 Starter 代理；
- `PipelineHandle.publish(...)` 与 `tryPublish(...)` 会为每次调用创建 translator lambda，只定位为便捷 API；
- 静态 translator 发布路径可避免捕获型 lambda 分配；
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

`DisruptorLifecycle` 实现 `SmartLifecycle`，默认 phase 为 `Integer.MIN_VALUE`，使管道尽早启动、尽晚停止。它只委托 Runtime，不复制生命周期逻辑。

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

## 验证策略

单元测试与集成测试使用真实 Disruptor，覆盖：

- 原生 `sequence`、`endOfBatch`、批次、生命周期和 sequence callback；
- rewind、自定义 processor、processor factory 与 translator 重载；
- 默认 `HALT`、显式 `LOG_AND_CONTINUE` 和处理器级异常策略；
- 同事件类型多命名管道与类型校验；
- 部分启动回滚、逆序关闭、超时强制停止、停止失败隔离和禁止重启；
- 配置覆盖、非法配置、未知命名配置和零管道；
- 自动配置开关、自定义 Bean 回退、AOT 元数据与可选 Micrometer；
- 指标在启动、积压、排空和停止阶段的变化；
- example 与 tutorial 的真实端到端流程。

全仓验证命令：

```bash
mvn test
```

性能验证使用 `disruptor-benchmarks` 中的 JMH 基准，对比原生实例与 Runtime 构建实例取得的同一类 `RingBuffer` 发布路径。性能结论必须记录硬件、JVM、参数、预热和测量配置；普通单测不设置易受机器噪声影响的吞吐阈值。

## 演进约束

- 新能力优先暴露原生 Disruptor 入口，不复制上游 API；
- 新配置必须具有明确默认值、优先级和失败语义；
- 修改生命周期或异常策略时，必须先补真实并发行为测试；
- 公开 API 的破坏性变更需要在发布说明中明确记录；
- README 只保留使用者开始使用所需内容，内部取舍和不变量写入本文；
- 文档中的版本、默认值、命令和模块名必须能由当前仓库验证。
