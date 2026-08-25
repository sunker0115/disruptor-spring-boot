# disruptor-spring-boot

一个以 LMAX Disruptor 原生 API 为核心的 Spring Boot Starter。

它只托管基础设施：管道构建、命名注册、配置合并和 Spring 生命周期。拓扑、处理器、异常、回放、自定义处理器以及发布全部直接使用 Disruptor 4.0 API，不维护功能不完整的中间 DSL。

## 快速开始

引入 Starter：

```xml
<dependency>
    <groupId>com.sstlfsj</groupId>
    <artifactId>disruptor-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

定义一条命名管道：

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

`topology` 参数就是原生 `Disruptor<OrderEvent>`。可以直接使用：

- `EventHandler` 的 `sequence`、`endOfBatch`、批次、生命周期、超时和 sequence callback；
- `RewindableEventHandler` 与 `BatchRewindStrategy`；
- `EventProcessorFactory`、自定义 `EventProcessor`、poller 和 barrier；
- `handleExceptionsFor`、`then`、`and`、`after` 等官方 DSL；
- Disruptor 后续版本新增的原生 API，无需等待 Starter 再做映射。

## 发布事件

按管道名取得强类型句柄，直接使用原生 `RingBuffer`：

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

这条路径没有 Starter 代理层，能使用 `RingBuffer` 的全部单条、批量、阻塞和非阻塞发布重载。静态 translator 可以避免捕获型 lambda 带来的额外分配；业务参数和处理器自身是否分配由业务代码决定。

官方发布契约会在 translator 抛出异常时仍发布已领取的槽位，因此 translator 必须只做简单字段写入且不抛异常。

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
    error-strategy: LOG_AND_CONTINUE
  pipelines:
    orders:
      buffer-size: 65536
      producer-type: SINGLE
      wait-strategy: BUSY_SPIN
      shutdown-timeout: 30s
      error-strategy: HALT
  metrics:
    enabled: true
```

优先级固定为：

```text
安全默认值 < disruptor.defaults < disruptor.pipelines.<name> < PipelineSpec 显式选项
```

支持的配置型等待策略是 `BLOCKING`、`BUSY_SPIN`、`YIELDING`、`SLEEPING`。需要构造参数的等待策略直接在 `PipelineSpec.waitStrategy(...)` 中传入工厂，避免 Starter 再造一套不完整的参数模型。

配置中出现未定义的管道名会使应用启动失败，防止拼写错误被静默忽略。`buffer-size` 必须是正的 2 的幂。

## 原生语义

### 异常处理

默认策略是 `LOG_AND_CONTINUE`：某条事件处理抛异常时，记录 ERROR 日志并跳过该条、继续消费后续事件——避免 Disruptor 原生 `FatalExceptionHandler` 那样一条异常就终止消费者、令序列停推而卡死整条管道。声明式可按环境切换（`LOG_AND_CONTINUE` / `HALT`；`HALT` 记录后抛出、终止该消费者，仅用于出错即停 + 人工介入的严格场景）：

```yaml
disruptor:
  defaults:
    error-strategy: LOG_AND_CONTINUE
  pipelines:
    orders:
      error-strategy: HALT
```

需要完全自定义（按事件类型、单条重试计数、失败投递通道等）时，用编程式逃生口：

```java
PipelineSpec.builder("orders", OrderEvent.class, OrderEvent::new)
        .exceptionHandler(myExceptionHandler)
        .topology(...)
        .build();
```

需要处理器级差异化策略时，在 `topology` 中直接调用 `disruptor.handleExceptionsFor(handler).with(...)`。优先级同其它旋钮：`PipelineSpec.exceptionHandler` > `disruptor.pipelines.<name>.error-strategy` > `disruptor.defaults.error-strategy` > 框架默认 `LOG_AND_CONTINUE`。Starter 不在消费者外包裹委托层，也不会让异常事件伪装成已成功处理后流向下游。

### 事件槽位复用

事件由 `EventFactory<E>` 预分配，不要求无参构造。发布方应写全本次事件需要的字段。如果存在可选字段，在原生 DAG 的叶子后显式添加清理 handler：

```java
disruptor.handleEventsWith(process)
        .then((event, sequence, endOfBatch) -> event.reset());
```

Starter 不猜测业务字段，也不自动插入清理阶段。

### 多管道与生命周期

管道按名称全局唯一，同一事件类型可以对应多条管道：

```java
runtime.require("orders", OrderEvent.class);
runtime.require("order-audit", OrderEvent.class);
```

`DisruptorRuntime` 是一次性状态机：重复启动或停止是幂等的，停止后重新启动会失败。Spring 在最早阶段启动、最晚阶段停止；关闭时按启动逆序逐条排空，超时后 `halt`，并继续关闭其余管道。

`PipelineHandle.disruptor()` 和 `ringBuffer()` 是完整原生逃生口。默认不要自行调用 `start`、`halt` 或 `shutdown`，生命周期由 Runtime 或 Spring 托管。

classpath 存在 Micrometer 时，Starter 注册 `disruptor.runtime.running`、`disruptor.pipeline.buffer.size`、`disruptor.pipeline.remaining.capacity` 和 `disruptor.pipeline.backlog`。这些 Gauge 只在采集时读取原生状态，不包装发布或消费路径；可用 `disruptor.metrics.enabled=false` 关闭。

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

完整示例见 `disruptor-spring-boot-example`，撮合场景见 `disruptor-spring-boot-tutorial`。

## 模块

| 模块 | 职责 |
| --- | --- |
| `disruptor-core` | `PipelineSpec`、`PipelineSettings`、`PipelineHandle`、`DisruptorRuntime`；无 Spring 依赖 |
| `disruptor-benchmarks` | 原生实例与 Runtime 构建实例的 JMH 发布路径对比 |
| `disruptor-spring-boot-autoconfigure` | 属性绑定、命名设置解析、自动配置、`SmartLifecycle`、配置元数据 |
| `disruptor-spring-boot-starter` | 聚合 `spring-boot-starter` 与自动配置模块 |
| `disruptor-spring-boot-example` | 原生 DAG、分片、清理、背压和纯 Java 示例 |
| `disruptor-spring-boot-tutorial` | 单写者撮合教程 |

## 本地验证

```bash
mvn test
```

项目在父 POM 中显式启用注解处理，因此新版本 JDK 下也会生成 Spring Boot 配置元数据。发布、签名和中央仓库工程不属于当前仓库能力范围。
