# disruptor-spring-boot-starter

基于 [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) 4.0 的**强类型事件处理管道** Spring Boot Starter。

忠于 Disruptor 的设计本意：每种事件类型对应一条独立的 Disruptor 管道，事件对象**预分配、原地
mutate（零分配）**，处理阶段以**静态 DAG** 编排。用注解把样板降到最低。

## 特性

- **零分配发布**：事件在 ring buffer 中预分配，发布时原地填充字段，不产生每事件的新对象（Disruptor 高性能的前提）。
- **声明式处理阶段**：`@DisruptorStage` 标注方法即处理阶段；`after` 声明依赖，容器自动编排成 Disruptor DAG（`then/and`）。
- **DAG 编排**：线性 `A→B→C`、菱形 `A→(B‖C)→D` —— 同一事件按阶段顺序流经，下游在上游处理完后才执行。
- **阶段并行**：`parallelism=N` 让阶段内并行分片处理（每事件仅由一个分片处理）；事件实现 `ShardKeyed` 则同 key 保序。
- **背压**：`tryPublish` 非阻塞（满时返回 `false`）、`remainingCapacity` 暴露堆积。
- **异常隔离**：阶段处理异常只记 ERROR 日志、不终止消费线程。
- **优雅关闭**：关闭时各管道先排空 RingBuffer（带超时）再停止。
- **native image 友好**：`@DisruptorStage` 用 Spring `@Reflective` 元注解，AOT 自动注册反射 hints。

## 环境要求

- JDK 17+
- Spring Boot 4.x

## 快速开始

### 1. 引入依赖

```xml
<dependency>
    <groupId>com.sstlfsj</groupId>
    <artifactId>disruptor-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. 定义强类型事件

**可变** POJO，需**无参构造**（供预分配）。字段供发布时原地填充：

```java
public class OrderEvent {
    private String orderId;
    private long amount;
    // getters / setters
}
```

### 3. 声明处理阶段

在任意 Spring bean 的方法上加 `@DisruptorStage`，方法签名 `void m(E event)`。同一 `pipeline`
的各阶段方法参数类型必须一致，即该管道的事件类型：

```java
@Component
public class OrderPipeline {

    @DisruptorStage(pipeline = "order", name = "validate")
    public void validate(OrderEvent e) { /* ... */ }

    @DisruptorStage(pipeline = "order", name = "persist", after = "validate")
    public void persist(OrderEvent e) { /* ... */ }

    @DisruptorStage(pipeline = "order", name = "notify", after = "persist")
    public void notify(OrderEvent e) { /* ... */ }
}
```

### 4. 发布事件（零分配填充）

注入 `EventBus`，按事件类型发布，用 filler 原地填充预分配的事件对象（**不要 new 事件**）：

```java
@Service
@RequiredArgsConstructor
public class OrderService {

    private final EventBus eventBus;

    public void create(String id, long amount) {
        eventBus.publish(OrderEvent.class, e -> {
            e.setOrderId(id);
            e.setAmount(amount);
        });
    }
}
```

也可取单条管道的发布入口：`EventPublisher<OrderEvent> p = eventBus.publisher(OrderEvent.class);`

## 阶段流水线编排（DAG）

`after` 声明阶段依赖，形成任意 DAG。**菱形**示例 `validate → (persist ‖ audit) → notify`：

```java
@DisruptorStage(pipeline = "order", name = "validate")
public void validate(OrderEvent e) { ... }

@DisruptorStage(pipeline = "order", name = "persist", after = "validate")
public void persist(OrderEvent e) { ... }

@DisruptorStage(pipeline = "order", name = "audit", after = "validate")
public void audit(OrderEvent e) { ... }

@DisruptorStage(pipeline = "order", name = "notify", after = {"persist", "audit"})
public void notify(OrderEvent e) { ... }   // persist、audit 都完成后才执行
```

阶段间通过同一事件对象传递数据：上游阶段修改事件字段，下游阶段可见。

## 编程式定义（注解的替代）

不想用注解、想用内联 lambda、或需要运行时动态组装时，用 `EventPipeline` fluent builder 在
`@Configuration` 里定义管道并作为 `@Bean`。与声明式**并存**、共用同一套 DAG 编排与 `EventBus` 发布：

```java
@Bean
public EventPipeline<OrderEvent> orderPipeline(OrderService svc) {
    return EventPipeline.builder("order", OrderEvent.class)
        .stage("validate", svc::validate)
        .stage("persist", svc::persist).after("validate")
        .stage("audit",   svc::audit).after("validate")
        .stage("notify",  svc::notify).after("persist", "audit")   // 菱形汇聚
        .parallelism("persist", 4)                                 // 可选：并行分片
        .build();
}
```

- handler 是内联 `Consumer<E>`（方法引用 / lambda），**直接调用、无反射**。
- 拓扑与注解式相同的 `name`/`after`，共用 DAG 校验与编排（借鉴 Spring Integration 的 fluent `@Bean` builder）。
- 声明式与编程式可混用；**管道名与事件类型全局唯一**（跨两种来源冲突则启动失败）。

## 阶段并行

`parallelism > 1` 时该阶段内并行分片，每个事件仅由一个分片处理（分摊阶段内的耗时业务）：

```java
@DisruptorStage(pipeline = "ingest", name = "process", parallelism = 4)
public void process(IngestEvent e) { ... }   // 4 个线程并行，每事件恰由一个处理
```

- 默认按发布序 round-robin 分片。
- 事件实现 `ShardKeyed`（`Object shardKey()`）→ 同 key 落同一分片、按发布顺序处理（保 per-key 顺序）。

## 事件复用（可选）

事件实现 `Resettable`（`void reset()`）→ 管道在 DAG 所有叶子之后接一个单线程 cleanup handler
调用 `reset()`，清空字段供槽位复用（避免发布 filler 未覆盖的字段残留旧值）。

## 背压

```java
if (!eventBus.tryPublish(OrderEvent.class, e -> { e.setOrderId(id); })) {
    // ring buffer 满：非阻塞返回 false，自行降级（丢弃 / 落库 / 告警）
}
long remaining = eventBus.remainingCapacity(OrderEvent.class);   // 剩余槽位，堆积 = bufferSize - 本值
```
`publish`（非 `tryPublish`）在满时会阻塞发布线程直到有空槽。

## 配置项

前缀 `disruptor`，全局应用于所有管道：

| 配置项                       | 类型     | 默认值     | 说明                                        |
|------------------------------|----------|------------|---------------------------------------------|
| `disruptor.buffer-size`      | int      | `1024`     | 每条管道的 RingBuffer 大小，**必须是 2 的幂**。 |
| `disruptor.wait-strategy`    | 枚举     | `YIELDING` | 等待策略：`BLOCKING`/`YIELDING`/`BUSY_SPIN`/`SLEEPING`。 |
| `disruptor.shutdown-timeout` | Duration | `10s`      | 关闭时每条管道排空的等待上限，超时强制 halt。 |

处理阶段拓扑由 `@DisruptorStage` 注解声明，不在配置里。

## 行为与约束

- **每种事件类型一条管道**：一种事件类型只能被一个 `pipeline` 使用（否则启动失败）。跨类型交互需多条管道。
- **零分配**：发布用 filler 填充预分配对象；请勿在 filler 内保存对该事件对象的长期引用（槽位会被复用）。
- **阶段内单线程**：`parallelism=1` 时该阶段单线程串行；耗时业务用 `parallelism` 或转交业务线程池。
- **启动即校验（fail-fast）**：方法非单参数、同管道类型不一致、事件类型跨管道重复、事件缺无参构造、
  阶段依赖环或引用不存在的阶段 —— 均导致启动失败。
- **仅 public 方法**：只有 public 的 `@DisruptorStage` 方法会被识别。
- **异常隔离**：阶段处理异常被记 ERROR 后跳过，不终止消费线程。
- **进程内、非持久化**：事件仅在当前 JVM 内传递；进程崩溃时未消费事件丢失。

## 覆盖默认 bean

对外 bean 均 `@ConditionalOnMissingBean`，可声明同类型 bean 覆盖：`EventBus`、`Pipelines`、
`PipelineBuilder`、`DisruptorConfig`、`StagePipelineRegistrar`、`SmartLifecycle`（生命周期）。

## 分层结构

代码按依赖分两层，边界单向、可按需拆为独立 Maven 模块：

- `com.sstlfsj.disruptor.core` —— **无 Spring 依赖**：公开 API（`EventPublisher`/`EventBus`/`EventPipeline`/
  `ShardKeyed`/`Resettable`）+ 构建与运行逻辑（`PipelineBuilder`/`DisruptorPipeline`/`Pipelines`/
  `DefaultEventBus`/`PipelineTopology`/`DisruptorConfig`）。可脱离 Spring 独立使用（手工装配
  `PipelineBuilder` + `DefaultEventBus` 即可）。
- `com.sstlfsj.disruptor.autoconfigure` —— **Spring 相关（含 Spring Boot 自动装配）**：`@DisruptorStage`、
  `StagePipelineRegistrar`（容器扫描桥接）、`DisruptorLifecycle`、`DisruptorProperties`、`DisruptorAutoConfiguration`。

依赖方向 `autoconfigure → core`。`core` 独立可复用；真需要"Spring 非 Boot"层时，从 autoconfigure
里把 `@DisruptorStage`/`StagePipelineRegistrar`/`DisruptorLifecycle` 再拆出即可。

## 已知限制（设计取舍）

- 一种事件类型一条管道，没有"一个总线随手发任意类型"——这是忠于 Disruptor 强类型/零分配的应有之义。
- 阶段内并行为分片模型（Disruptor 4.0 已移除 WorkerPool）；不提供动态负载均衡的 work-queue 语义。
- 仅进程内、无持久化/无投递保证（in-process bus，非消息队列）。
