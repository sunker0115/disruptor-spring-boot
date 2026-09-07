# disruptor-spring-boot

[![CI](https://github.com/sunker0115/disruptor-spring-boot/actions/workflows/ci.yml/badge.svg)](https://github.com/sunker0115/disruptor-spring-boot/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

基于 [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) 原生 API 的 Spring Boot Starter，并提供受监督的单线程执行与调度模块 `disruptor-concurrent`。

项目托管管道构建、命名注册、配置合并、发布准入和完整关闭流程。事件拓扑、处理器、异常处理、回放与自定义处理器仍使用 Disruptor 4.0 API，不引入另一套功能不完整的注解或 DAG DSL。

当前仓库是 `2.0.0-SNAPSHOT` 开发版本，尚未发布到 Maven Central。使用前需要先在本地构建并安装。

## 适用场景

- 希望在 Spring Boot 中声明多条命名 Disruptor 管道，并由容器统一管理生命周期；
- 需要保留 `RingBuffer`、原生拓扑 DSL、自定义 `EventProcessor`、rewind 等完整能力；
- 需要有界或显式无界的 `EventLoop`、固定 `EventLoopGroup`、高级调度和协作取消；
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
    <version>2.0.0-SNAPSHOT</version>
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

`DisruptorRuntime` 由自动配置注册为 Spring Bean，可通过构造器注入。发布方式分为受管发布和外部托管的原生发布；`ProducerType.SINGLE/MULTI` 只决定生产者并发模型，与发布生命周期归属是两个独立维度。

#### 受管发布（默认）

按名称和事件类型获取句柄，通过受管入口发布：

```java
private static final EventTranslatorTwoArg<OrderEvent, String, Long> TRANSLATOR =
        (event, sequence, orderId, amount) -> {
            event.setOrderId(orderId);
            event.setAmount(amount);
        };

PipelineHandle<OrderEvent> orders = runtime.require("orders", OrderEvent.class);
boolean accepted = orders.tryPublishEvent(TRANSLATOR, orderId, amount);
```

`publishEvent/tryPublishEvent` 支持 0 至 3 个参数的原生 translator，并参与 Runtime 的关闭准入屏障；`publish/tryPublish` 是接受 `Consumer<E>` 的便捷入口。Runtime 关闭时会拒绝新发布、等待在途发布完成，再排空消费者。

#### 原生发布（外部托管）

需要批量发布、varargs、完整 `EventSink` API 或完全零代理路径时，可以显式获取原生 RingBuffer：

```java
RingBuffer<OrderEvent> ringBuffer = orders.unsafeRingBuffer();
ringBuffer.publishEvent(TRANSLATOR, orderId, amount);
```

该入口不参与 Runtime 的发布准入。应用必须按以下顺序关闭：

```text
停止外部入口 → 等待全部生产线程退出 → runtime.shutdown()
```

| 发布方式 | SINGLE | MULTI | 关闭责任 |
| --- | --- | --- | --- |
| 受管入口 | 单生产者优化准入 | 并发发布者准入 | Runtime 托管 |
| `unsafeRingBuffer()` | 原生零代理 | 原生零代理 | 应用托管生产者 |

同一管道可以混用两种入口，但只要存在原生发布者，整条管道的无损关闭就依赖应用先停止这些原生生产者。`unsafe` 指生命周期绕过 Runtime，并不表示 RingBuffer API 本身不安全。

## 单线程执行与调度

`disruptor-concurrent` 不由 Starter 强制传递，按需显式引入：

```xml
<dependency>
    <groupId>com.sstlfsj</groupId>
    <artifactId>disruptor-concurrent</artifactId>
    <version>2.0.0-SNAPSHOT</version>
</dependency>
```

Spring 应用显式声明根 `EventLoop` 或 `EventLoopGroup` Bean。自动配置只负责这些根对象的生命周期、健康和指标，不创建隐藏全局实例：

```java
@Bean(destroyMethod = "")
DisruptorEventLoop orders() {
    return EventLoopBuilder.bounded("orders", 1024).build();
}

@Bean(destroyMethod = "")
DisruptorEventLoopGroup workers() {
    return EventLoopGroupBuilder.bounded("workers", 4, 1024).build();
}
```

Spring 会在全部 module 和 worker 就绪后开放提交入口。纯 Java 使用时必须先等待显式启动完成：

```java
EventLoop loop = EventLoopBuilder.unbounded("commands", 1024).build();
loop.start().toCompletableFuture().join();
Future<Order> future = loop.submit(() -> loadOrder(orderId));
```

### 选择与导航

| 需求 | 选择 | 语义 |
| --- | --- | --- |
| 入口必须快速拒绝，不能以积压掩盖背压 | `EventLoopBuilder.bounded` | 容量覆盖已接收任务；满载时 `tryExecute` 返回 `false`，`execute`/`submit` 拒绝 |
| 允许显式积压的后台任务 | `EventLoopBuilder.unbounded` | 不适合 HTTP 背压边界 |
| 所有任务必须由一个线程拥有 | 单个 `EventLoop` | `execute` 仅投递，`submit` 通过 `Future` 取得结果，`schedule` 用于延迟或周期任务 |
| 按 key 分片但每个 key 保持单线程 | `EventLoopGroup` | 用 `select(affinityKey)` 固定选择 child |
| 停止时保留已接收任务 | `shutdown()` / `GRACEFUL` | 关闭准入后排空一次性任务 |
| 需要立刻取消未开始任务 | `shutdownNow()` / `IMMEDIATE` | 返回获得所有权的未开始原始任务 |

EventLoop 的任务所有权是“谁执行普通任务”；Disruptor 管道的事件流水线是“事件经过哪些 handler”。两者可串联但不互相替代：可参考 [concurrent Spring 示例](disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/concurrent/ConcurrentExampleConfiguration.java)、[纯 Java 示例](disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/nospring/PureJavaConcurrentExample.java)，以及本次的 [撮合 tutorial 配置](disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/config/MatchConfig.java) 和 [HTTP 入口](disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/ingress/MatchingOrderIngress.java)。

高级调度通过不可变 `ScheduledTaskSpec` 表达上下文、动态延迟、执行次数、失败策略、优先级与取消令牌：

```java
TaskContext context = TaskContext.empty().with(ORDER_ID, orderId);
CancellationSource cancellation = new CancellationSource();

EventLoopScheduledFuture<Void> future = loop.schedule(
        ScheduledTaskSpec.<Void>builder()
                .context(context)
                .task(current -> {
                    refresh(current.get(ORDER_ID));
                    return null;
                })
                .scheduleMode(ScheduleMode.DYNAMIC_DELAY)
                .dynamicDelay(lastRun -> Duration.ofSeconds(lastRun.executions()))
                .maxExecutions(5)
                .priority(10)
                .cancellationToken(cancellation)
                .build());
```

`group.next()` 轮询选择 child，`group.select(affinityKey)` 对同一 key 固定选择同一 child。Group 是 child 的唯一生命周期 owner：业务可以向 child 提交和查询，但不能直接对 child 调用 `start`、`shutdown`、`shutdownNow` 或 `requestShutdown`。

标准 `shutdown()` 使用无界 graceful deadline，会保留已接受的一次性延迟任务并取消周期任务；`shutdownNow()` 返回由本次调用取得所有权的未开始原始任务，并尽力中断正在执行的任务。需要有界运维窗口时使用 `requestShutdown(mode, ShutdownDeadline.after(timeout))`，最后通过 `termination()` 或 `awaitTermination` 等待真实线程退出。Java 无法强停忽略中断的用户代码，因此 deadline 到期不会伪造 terminated。

## 配置

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
    orders:
      buffer-size: 65536
      producer-type: SINGLE
      wait-strategy: BUSY_SPIN
      daemon-threads: false
      error-strategy: HALT
  metrics:
    enabled: true
  concurrent:
    enabled: true
    lifecycle-phase: -2147483648
    shutdown-timeout: 10s
```

| 属性 | 默认值 | 说明 |
| --- | --- | --- |
| `disruptor.enabled` | `true` | 是否启用自动配置 |
| `disruptor.lifecycle-phase` | `Integer.MIN_VALUE` | Spring 生命周期阶段；越小越早启动、越晚停止 |
| `disruptor.shutdown-timeout` | `10s` | Runtime 排空全部管道并等待消费线程退出的总预算，必须大于 0 |
| `disruptor.defaults.buffer-size` | `1024` | RingBuffer 容量，必须是正的 2 的幂 |
| `disruptor.defaults.producer-type` | `MULTI` | `SINGLE` 或 `MULTI` |
| `disruptor.defaults.wait-strategy` | `BLOCKING` | 等待策略预设 |
| `disruptor.defaults.daemon-threads` | `false` | 消费线程是否为 daemon 线程 |
| `disruptor.defaults.error-strategy` | `HALT` | 默认消费异常策略 |
| `disruptor.metrics.enabled` | `true` | classpath 存在 Micrometer 时是否注册指标 |
| `disruptor.concurrent.enabled` | `true` | 是否托管显式声明的 concurrent 根 Bean |
| `disruptor.concurrent.lifecycle-phase` | `Integer.MIN_VALUE` | concurrent 根对象的 Spring 生命周期阶段 |
| `disruptor.concurrent.shutdown-timeout` | `10s` | 全部 concurrent 根对象共享的关闭时间边界 |

每个 `disruptor.pipelines.<name>` 可以覆盖 buffer、producer、wait strategy、daemon thread 和 error strategy。最终优先级为：

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

受管 `publishEvent/tryPublishEvent` 在 Runtime 进入关闭阶段后分别抛出 `IllegalStateException` 或返回 `false`。已经获准但尚未完成的发布会先完成，再被纳入本次排空目标。通过 `unsafeRingBuffer()` 发布的事件不参与该协议；若原生生产者与 Runtime 关闭并发，Runtime 只能保证受管入口的边界，不能承诺整条管道无损关闭。

命名管道按彼此独立的生命周期单元处理。要求无损级联的阶段应放在同一条 Disruptor topology 中；handler 向另一条受管管道继续发布时，应用必须先停止上游并自行编排关闭顺序。

### 生命周期

`DisruptorRuntime` 是一次性状态机：`NEW → STARTING → RUNNING → QUIESCING → STOPPING → TERMINATED`。处于 `RUNNING` 时重复调用 `start()` 不产生额外动作，重复停止也是幂等的；进入 `STOPPED` 后再次启动会失败。

优雅关闭会先拒绝新受管发布并等待在途发布完成，再捕获固定目标游标；随后按启动逆序等待最小 gating sequence 越过目标、调用 `halt()`，最后等待消费线程真正退出。该判断不依赖消费者是否已经报告 `isRunning()`，因此启动后立即关闭也不会漏掉已发布事件。

所有管道共享一个 `disruptor.shutdown-timeout` 总预算。任一管道未排空、消费者停止失败或线程未在预算内退出时，Runtime 仍会继续清理其它管道，最后抛出聚合的 `DisruptorShutdownException`，不把可能丢事件的关闭误报为成功。

`DisruptorRuntime.isRunning()` 只表示 Runtime 是否仍接受受管发布，不表示每个消费者线程健康。默认 `HALT` 导致消费者退出后，该值仍可能为 `true`；应用必须监控消费异常。

### 指标

classpath 同时存在 Micrometer 和 `MeterRegistry` 时，自动注册以下 Gauge：

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `disruptor.runtime.running` | 无 | Runtime 托管生命周期状态，不是健康检查 |
| `disruptor.pipeline.buffer.size` | `pipeline`、`event.type` | RingBuffer 固定容量 |
| `disruptor.pipeline.remaining.capacity` | `pipeline`、`event.type` | 当前剩余可写槽位数 |
| `disruptor.pipeline.backlog` | `pipeline`、`event.type` | 基于 cursor 与最小 gating sequence 计算的近似积压 |

指标只在采集时读取原生状态，不包装发布或消费热路径。

存在 `disruptor-concurrent` 时，还会按 event loop 注册准入、worker、任务、调度和队列 Gauge；bounded 公开剩余容量，unbounded 公开已分配 segment 数。存在 Boot Health 时，基础设施失败为 `DOWN`，未运行或关闭中的根对象为 `OUT_OF_SERVICE`；单个任务的业务失败计数不会把基础设施判为 `DOWN`。

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
    runtime.require("plain", MyEvent.class)
            .publishEvent(TRANSLATOR, value);
} finally {
    runtime.shutdown();
}
```

## 示例与验证

- `disruptor-spring-boot-example`：原生菱形 DAG、分片、异常处理、事件清理、背压、concurrent 与纯 Java 示例；
- `disruptor-spring-boot-tutorial`：并发 HTTP 请求经有界单入口线程串行发布的撮合 Web 教程；
- `disruptor-benchmarks`：原生实例、受管发布与 `unsafeRingBuffer()` 的 JMH 发布路径对比。

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
| `disruptor-concurrent` | 有界/无界 EventLoop、调度、取消、Group 与事实快照 |
| `disruptor-spring-boot-autoconfigure` | 两套运行时的属性、生命周期、健康与可选指标装配 |
| `disruptor-spring-boot-starter` | 面向使用方的依赖聚合模块 |
| `disruptor-spring-boot-example` | 核心能力示例 |
| `disruptor-spring-boot-tutorial` | 单生产者、单撮合消费者的 Web 业务教程 |
| `disruptor-benchmarks` | JMH 基准 |

维护者可阅读[架构设计](docs/disruptor-architecture-design.md)与固定参考 commit 的 [Commons 能力事实矩阵](docs/commons-capability-matrix.md)。

## 开源与发布

项目采用 [Apache License 2.0](LICENSE) 开源。参与开发前请阅读[贡献指南](CONTRIBUTING.md)；安全问题请按照[安全策略](SECURITY.md)私密报告。推送到 `main` 和 Pull Request 会在 GitHub Actions 中使用 JDK 21 执行全仓 `clean verify`。

当前版本尚未发布到 Maven Central，首次使用仍需按“快速开始”在本地构建安装。Maven Central 坐标、制品签名和发布自动化将在首次制品发布前单独完成。
