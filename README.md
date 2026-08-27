# disruptor-spring-boot

基于 [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) 原生 API 的 Spring Boot Starter。

项目只托管管道构建、命名注册、配置合并和 Spring 生命周期。事件拓扑、处理器、异常处理、回放、自定义处理器与事件发布仍使用 Disruptor 4.0 API，不引入另一套功能不完整的注解或 DAG DSL。

当前仓库是 `1.0.0` 开发版本，尚未发布到 Maven Central。使用前需要先在本地构建并安装。

## 适用场景

- 希望在 Spring Boot 中声明多条命名 Disruptor 管道，并由容器统一管理生命周期；
- 需要保留 `RingBuffer`、原生拓扑 DSL、自定义 `EventProcessor`、rewind 等完整能力；
- 业务愿意显式处理事件复用、背压和消费失败等 Disruptor 原生语义。

项目不提供持久化队列、跨进程消息传输、自动重试、消费者健康恢复或业务失败补偿。需要这些能力时，应由业务系统或消息中间件承担。

## 版本要求

| 组件 | 当前基线 |
| --- | --- |
| JDK | 21 或更高版本 |
| Spring Boot | 4.1.0 |
| LMAX Disruptor | 4.0.0 |
| 构建工具 | Maven |

以上是当前仓库实际编译和测试基线，不表示已经验证其它 Spring Boot 或 Disruptor 版本组合。

## 快速开始

### 1. 本地安装

```bash
mvn clean install
```

### 2. 引入 Starter

```xml
<dependency>
    <groupId>com.sstlfsj</groupId>
    <artifactId>disruptor-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 3. 定义命名管道

```java
@Bean
PipelineSpec<OrderEvent> orderPipeline(
        EventHandler<OrderEvent> validate,
        EventHandler<OrderEvent> persist,
        EventHandler<OrderEvent> audit,
        EventHandler<OrderEvent> notify) {

    return PipelineSpec.builder("orders", OrderEvent.class, OrderEvent::new)
            .producerType(ProducerType.SINGLE)
            .topology(disruptor -> disruptor
                    .handleEventsWith(validate)
                    .then(persist, audit)
                    .then(notify))
            .build();
}
```

`topology` 接收原生 `Disruptor<OrderEvent>`，因此可以直接使用：

- `EventHandler` 的 `sequence`、`endOfBatch`、批次和生命周期回调；
- `RewindableEventHandler` 与 `BatchRewindStrategy`；
- `EventProcessorFactory`、自定义 `EventProcessor`、poller 和 barrier；
- `handleExceptionsFor`、`then`、`and`、`after` 等原生拓扑 API。

### 4. 发布事件

`DisruptorRuntime` 由自动配置注册为 Spring Bean，可通过构造器注入。按名称和事件类型获取句柄，再使用原生 `RingBuffer`：

```java
private static final EventTranslatorTwoArg<OrderEvent, String, Long> TRANSLATOR =
        (event, sequence, orderId, amount) -> {
            event.setOrderId(orderId);
            event.setAmount(amount);
        };

PipelineHandle<OrderEvent> orders = runtime.require("orders", OrderEvent.class);
boolean accepted = orders.ringBuffer()
        .tryPublishEvent(TRANSLATOR, orderId, amount);
```

`PipelineHandle.publish(...)` 和 `tryPublish(...)` 可用于普通便捷调用。追求稳定低分配时，应直接使用 `ringBuffer()` 与静态 translator，避免捕获型 lambda。

## 配置

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
    orders:
      buffer-size: 65536
      producer-type: SINGLE
      wait-strategy: BUSY_SPIN
      shutdown-timeout: 30s
      daemon-threads: false
      error-strategy: HALT
  metrics:
    enabled: true
```

| 属性 | 默认值 | 说明 |
| --- | --- | --- |
| `disruptor.enabled` | `true` | 是否启用自动配置 |
| `disruptor.lifecycle-phase` | `Integer.MIN_VALUE` | Spring 生命周期阶段；越小越早启动、越晚停止 |
| `disruptor.defaults.buffer-size` | `1024` | RingBuffer 容量，必须是正的 2 的幂 |
| `disruptor.defaults.producer-type` | `MULTI` | `SINGLE` 或 `MULTI` |
| `disruptor.defaults.wait-strategy` | `BLOCKING` | 等待策略预设 |
| `disruptor.defaults.shutdown-timeout` | `10s` | 单条管道等待积压排空的最长时间，必须大于 0 |
| `disruptor.defaults.daemon-threads` | `false` | 消费线程是否为 daemon 线程 |
| `disruptor.defaults.error-strategy` | `HALT` | 默认消费异常策略 |
| `disruptor.metrics.enabled` | `true` | classpath 存在 Micrometer 时是否注册指标 |

每个 `disruptor.pipelines.<name>` 可以覆盖全部默认管道设置。最终优先级为：

```text
框架安全默认值 < disruptor.defaults < disruptor.pipelines.<name> < PipelineSpec 显式选项
```

支持的等待策略预设是 `BLOCKING`、`BUSY_SPIN`、`YIELDING` 和 `SLEEPING`。需要构造参数或自定义实现时，通过 `PipelineSpec.waitStrategy(...)` 提供原生工厂。

配置中出现没有对应 `PipelineSpec` Bean 的管道名时，应用启动失败；非法配置的错误信息会包含管道名。

## 重要运行语义

### 消费异常

默认 `HALT` 会记录错误并终止失败消费者，使其序列不再推进，避免链式拓扑把失败槽位继续交给下游。代价是该管道可能产生持续背压，应用必须监控线程异常并准备人工恢复或进程重启机制。

只有终端消费者、幂等处理或业务明确接受部分处理时，才应使用 `LOG_AND_CONTINUE`。该策略吞掉异常并推进当前消费者序列，因此依赖它的下游仍会收到失败槽位。

```yaml
disruptor:
  pipelines:
    audit:
      error-strategy: LOG_AND_CONTINUE
```

完整自定义策略使用 `PipelineSpec.exceptionHandler(...)`；处理器级差异化策略使用原生 `disruptor.handleExceptionsFor(handler).with(...)`。

### 事件发布与槽位复用

事件由 `EventFactory<E>` 预分配并循环复用，发布方必须写全本次事件需要的字段。可选字段应在拓扑叶子后由业务显式清理：

```java
disruptor.handleEventsWith(process)
        .then((event, sequence, endOfBatch) -> event.reset());
```

Disruptor 在 translator 抛异常时仍会发布已经领取的槽位。translator 应只做简单字段赋值，并且不得抛异常。

### 生命周期

`DisruptorRuntime` 是一次性状态机：`NEW → RUNNING → STOPPED`。处于 `RUNNING` 时重复调用 `start()` 不产生额外动作，重复停止也是幂等的；进入 `STOPPED` 后再次启动会失败。

启动失败时，Runtime 会逆序 `halt` 所有已经尝试启动的管道，包括发生部分启动的当前管道。正常关闭按启动逆序逐条排空；单条管道超时或停止失败时会强制 `halt`，并继续关闭其它管道。

`PipelineHandle.hasStarted()` 表示底层 Disruptor 是否曾经启动，停止后仍返回 `true`。`DisruptorRuntime.isRunning()` 只表示 Runtime 处于已启动且未停止的托管生命周期状态，两者都不表示消费者线程健康。

业务代码不应通过 `PipelineHandle.disruptor()` 自行调用 `start()`、`halt()` 或 `shutdown()`。

### 指标

classpath 同时存在 Micrometer 和 `MeterRegistry` 时，自动注册以下 Gauge：

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `disruptor.runtime.running` | 无 | Runtime 托管生命周期状态，不是健康检查 |
| `disruptor.pipeline.buffer.size` | `pipeline`、`event.type` | RingBuffer 固定容量 |
| `disruptor.pipeline.remaining.capacity` | `pipeline`、`event.type` | 当前剩余可写槽位数 |
| `disruptor.pipeline.backlog` | `pipeline`、`event.type` | 基于 cursor 与最小 gating sequence 计算的近似积压 |

指标只在采集时读取原生状态，不包装发布或消费热路径。

## 纯 Java 用法

`disruptor-core` 不依赖 Spring：

```java
PipelineSpec<MyEvent> spec = PipelineSpec.builder(
                "plain", MyEvent.class, MyEvent::new)
        .topology(disruptor -> disruptor.handleEventsWith(handler))
        .build();

DisruptorRuntime runtime = DisruptorRuntime.builder()
        .settings(PipelineSettings.defaults())
        .add(spec)
        .build();

runtime.start();
try {
    runtime.require("plain", MyEvent.class).ringBuffer()
            .publishEvent(TRANSLATOR, value);
} finally {
    runtime.shutdown();
}
```

## 示例与验证

- `disruptor-spring-boot-example`：原生菱形 DAG、分片、异常处理、事件清理、背压和纯 Java 示例；
- `disruptor-spring-boot-tutorial`：单写者撮合 Web 教程；
- `disruptor-benchmarks`：原生实例与 Runtime 构建实例的 JMH 发布路径对比。

运行全仓测试：

```bash
mvn test
```

启动撮合教程前先完成一次本地安装：

```bash
mvn -pl disruptor-spring-boot-tutorial spring-boot:run
```

另开终端执行：

```bash
bash disruptor-spring-boot-tutorial/demo.sh
```

## 模块

| 模块 | 职责 |
| --- | --- |
| `disruptor-core` | 纯 Java 管道定义、运行时句柄、注册表和生命周期 |
| `disruptor-spring-boot-autoconfigure` | 属性绑定、自动配置、Spring 生命周期和 Micrometer 指标 |
| `disruptor-spring-boot-starter` | 面向使用方的依赖聚合模块 |
| `disruptor-spring-boot-example` | 核心能力示例 |
| `disruptor-spring-boot-tutorial` | 撮合业务教程 |
| `disruptor-benchmarks` | JMH 基准 |

维护者可阅读[架构设计](docs/disruptor-architecture-design.md)。

## 开源状态

当前仓库尚未配置 Maven Central 发布、持续集成、贡献指南、安全策略和开源许可证。在根目录增加明确的 `LICENSE` 之前，源码不应被视为已经获得开源使用、修改或分发授权。

公开发布前至少应补齐：

- `LICENSE`；
- `CONTRIBUTING.md`；
- `SECURITY.md`；
- CI 构建与测试；
- Maven Central 发布、签名和项目元数据。
