# disruptor-spring-boot-starter

基于 [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) 4.0 的**强类型事件处理管道** Spring Boot Starter。

忠于 Disruptor 的设计本意：每种事件类型对应一条独立的 Disruptor 管道，事件对象**预分配、原地
mutate（零分配）**，处理阶段以**静态 DAG** 编排。用注解把样板降到最低。

## 适用场景

**适合**（进程内、处理拓扑相对固定、在意吞吐/延迟的异步多阶段处理）：

- **多阶段处理流水线**：一个事件顺序/并行经过若干环节，如 `校验 → 落库 →（审计 ‖ 通知）`。
- **低延迟 / 高吞吐**：金融交易、实时风控、行情 / 撮合等（Disruptor 出身即 LMAX 交易所）。
- **主流程与副作用解耦**：主线程 `publish` 后立即返回，审计 / 埋点 / 落库 / 通知在后台阶段异步跑，突发用 `tryPublish` 背压。
- **进程内事件 fan-out**：一个领域事件并行投影到多个只读模型 / 处理器（CQRS 投影）。
- 追求性能时替代 `@Async` + 线程池的进程内异步。

**不适合**：

- 跨进程 / 跨服务 → 用 Kafka / RabbitMQ（这是**进程内**总线，非消息队列）。
- 要持久化 / 投递保证 → 进程崩溃丢事件；用 MQ + outbox。
- 动态订阅 / 内容路由 / 通配 → 拓扑启动期**静态**、只按精确类型路由；用 Spring `ApplicationEvent`。
- 偶发低频事件 → 用普通 `@EventListener` 更简单（杀鸡用牛刀）。

> 一句话：**同 JVM 内、拓扑相对固定、在意吞吐 / 延迟的多阶段异步处理**用它；**跨服务、要可靠投递、要灵活路由**用 MQ 或 Spring 事件。

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
  这正是 Disruptor 官方推荐替代已移除的 `WorkerPool` 的**条带化（striped event handlers）**方案。

## 事件复用（可选）

事件实现 `Resettable`（`void reset()`）→ 管道在 DAG 所有叶子之后接一个单线程 cleanup handler
调用 `reset()`，清空字段供槽位复用（避免发布 filler 未覆盖的字段残留旧值）。

```java
public class OrderEvent implements Resettable {
    private String orderId;
    private long amount;
    private List<String> items = new ArrayList<>();
    // getters / setters

    @Override
    public void reset() {   // 每个事件流经所有阶段后被调用一次
        this.orderId = null;
        this.amount = 0L;
        this.items.clear();
    }
}
```

**为什么会有残留**：ring buffer 里的事件对象预分配复用——同一实例每隔 `bufferSize` 个事件被重新填一次。
若某次 filler 没设全字段，未设字段会残留**上一次用该槽位的事件**的值。例如可选字段 `couponCode`：

```java
publish(e -> { e.setOrderId("A"); e.setCouponCode("SAVE10"); });  // 订单 A 用券
publish(e -> { e.setOrderId("B"); });                             // 订单 B 没设 couponCode
```

若订单 B 复用了订单 A 的槽位，会读到残留的 `"SAVE10"`（脏数据）。`reset()` 在每个事件处理完后清空，杜绝残留。

**何时需要**：

- filler 每次都设置**全部**字段 → **不需要** `Resettable`（下一轮发布自然覆盖旧值）。
- filler **有条件地 / 只设部分**字段（如上面的可选字段）→ 实现 `Resettable` 清空，避免读到上一轮残留。
- 事件持有集合、大对象引用，想在处理完及时释放、避免引用滞留延迟 GC → 在 `reset()` 里 `clear()` / 置 `null`。


## 背压

`publish` 在 ring buffer 满时**阻塞**发布线程直到有空槽；`tryPublish` **非阻塞**，满时返回 `false`
由调用方决定降级；`remainingCapacity` 暴露剩余槽位（堆积 = bufferSize − 本值）。

`tryPublish` 满时（返回 `false`）的三种典型降级形态：

```java
// 1. 可丢弃（指标 / 埋点）：丢弃 + 计数
if (!eventBus.tryPublish(MetricEvent.class, e -> e.set(name, value))) {
    droppedCounter.increment();                 // Micrometer 记录背压丢弃量
}

// 2. 不能丢（审计 / 对账）：降级落库，事后补偿重放
if (!eventBus.tryPublish(AuditEvent.class, e -> e.fill(action))) {
    auditFallbackRepo.save(new PendingAudit(action));
}

// 3. 关键请求：快速失败，把压力回推给上游（限流）
if (!eventBus.tryPublish(OrderEvent.class, e -> e.fill(order))) {
    throw new ServiceBusyException("系统繁忙，请稍后重试");   // 上游转 HTTP 429 / 降级页
}
```

- 能丢 → 形态 1；不能丢 → 形态 2（落库补偿）；关键链路不能丢又要快 → 形态 3（快速失败限流）。
- **宁可阻塞等待也不丢** → 直接用 `publish`（满时阻塞发布线程）。`tryPublish` 的意义就是"宁可丢 / 降级，也不阻塞发布线程"。

## 配置项

前缀 `disruptor`，全局应用于所有管道：

| 配置项                       | 类型     | 默认值     | 说明                                        |
|------------------------------|----------|------------|---------------------------------------------|
| `disruptor.buffer-size`      | int      | `1024`     | 每条管道的 RingBuffer 大小，**必须是 2 的幂**。 |
| `disruptor.wait-strategy`    | 枚举     | `YIELDING` | 等待策略：`BLOCKING`/`YIELDING`/`BUSY_SPIN`/`SLEEPING`。 |
| `disruptor.shutdown-timeout` | Duration | `10s`      | 关闭时每条管道排空的等待上限，超时强制 halt。 |

配置示例（`application.yml`，均可选，缺省即用默认值）：

```yaml
disruptor:
  buffer-size: 2048          # RingBuffer 大小，必须是 2 的幂；默认 1024
  wait-strategy: BLOCKING    # BLOCKING / YIELDING(默认) / BUSY_SPIN / SLEEPING
  shutdown-timeout: 30s      # 关闭排空等待上限；默认 10s
```

处理阶段拓扑由 `@DisruptorStage` 注解（或编程式 `EventPipeline`）声明，不在配置里。

**编程方式设置**：声明一个 `DisruptorConfig` bean 覆盖默认（`@ConditionalOnMissingBean`，声明后
yml 的 `disruptor.*` 即被忽略）：

```java
@Bean
public DisruptorConfig disruptorConfig() {
    return new DisruptorConfig(
        2048,                                        // buffer-size
        DisruptorConfig.WaitStrategyType.BLOCKING,   // wait-strategy
        Duration.ofSeconds(30));                     // shutdown-timeout
}
```

脱离 Spring 时直接 `new DisruptorConfig(...)` 传给 `PipelineBuilder`（见「分层结构」）。

## 行为与约束

- **每种事件类型一条管道**：一种事件类型只能被一个 `pipeline` 使用（否则启动失败）。跨类型交互需多条管道。
- **零分配**：发布用 filler 填充预分配对象；请勿在 filler 内保存对该事件对象的长期引用（槽位会被复用）。
- **阶段内单线程**：`parallelism=1` 时该阶段单线程串行；耗时业务用 `parallelism` 或转交业务线程池。
- **启动即校验（fail-fast）**：方法非单参数、同管道类型不一致、事件类型跨管道重复、事件缺无参构造、
  阶段依赖环或引用不存在的阶段 —— 均导致启动失败。
- **仅 public 方法**：只有 public 的 `@DisruptorStage` 方法会被识别。
- **异常隔离**：阶段处理异常被记 ERROR 后跳过，不终止消费线程（进程内/非持久化等边界见「已知限制」）。

## 日志与可观测

logger 名为各组件类全名（`com.sstlfsj.disruptor.*`），分级如下：

- **INFO**（启动/关闭，低频）：每条管道的装配结构（事件类型、阶段 DAG、并行度、是否 `Resettable`）、
  各管道 start/stop。可从日志确认"管道注册了什么、拓扑对不对"。
- **ERROR**：阶段处理异常，带 `管道/阶段/event` 上下文（已隔离，消费线程继续）——定位"哪一步挂了"。
- **WARN**：关闭排空超时（强制 halt，可能丢事件）。
- **DEBUG**：背压——`tryPublish` 因 ring buffer 满被拒时记（含剩余容量）。默认不输出。

示例：

```
已建立管道 [order] 事件类型=OrderEvent 阶段=[validate → persist(after=validate)[x4] → notify(after=persist)] 复用=Resettable
已启动管道 [order]（事件类型 OrderEvent）
管道 [order] 阶段 [persist] 处理事件异常，event=...，已隔离（消费线程继续）
```

按需 `logging.level.com.sstlfsj.disruptor=DEBUG` 即可看到背压等细节。

## 覆盖默认 bean

对外 bean 均 `@ConditionalOnMissingBean`，可声明同类型 bean 覆盖：`EventBus`、`Pipelines`、
`PipelineBuilder`、`DisruptorConfig`、`StagePipelineRegistrar`、`DisruptorLifecycle`（生命周期）。

## 模块结构

三个 Maven 模块，父 `disruptor-spring-boot`（pom）聚合，依赖方向单向 `starter → autoconfigure → core`：

| 模块 | 依赖 | 内容 |
|---|---|---|
| `disruptor-core` | disruptor + slf4j，**无 Spring** | 公开 API（`EventPublisher`/`EventBus`/`EventPipeline`/`ShardKeyed`/`Resettable`）+ 构建/运行逻辑（`PipelineBuilder`/`DisruptorPipeline`/`Pipelines`/`DefaultEventBus`/`PipelineTopology`/`DisruptorConfig`）。可脱离 Spring 独立使用。 |
| `disruptor-spring-boot-autoconfigure` | disruptor-core + spring-boot-autoconfigure | `@DisruptorStage`、`StagePipelineRegistrar`（容器扫描）、`DisruptorLifecycle`、`DisruptorProperties`、`DisruptorAutoConfiguration`。 |
| `disruptor-spring-boot-starter` | disruptor-spring-boot-autoconfigure | 无代码，聚合依赖——**使用方引入此模块即可**（快速开始的坐标即它）。 |

- **Spring Boot 项目** → 引 `disruptor-spring-boot-starter`（自动装配全套）。
- **非 Spring / 纯 Java 项目** → 只引 `disruptor-core`，手工装配 `PipelineBuilder` + `DefaultEventBus`（见「配置项」编程方式）。

## 示例与教程

两个可跑模块（不发布，仅供学习）：

- `disruptor-spring-boot-example`：每个特性一个自包含 console demo（声明式 DAG / 编程式 builder / 并行分片 / 事件复用 / 背压三形态 / 纯 Java）。`mvn -pl disruptor-spring-boot-example spring-boot:run`。
- `disruptor-spring-boot-tutorial`：真实场景**撮合** web 小应用——`POST /orders` 用 `tryPublish` 进环立刻返回 202（满则 429），后台**单线程** Disruptor 撮合（DAG `match → emit ‖ metrics`），`GET /orders/stats`、`GET /book?symbol=` 观测。
  - 跑：`mvn -pl disruptor-spring-boot-tutorial spring-boot:run`，另开终端 `bash disruptor-spring-boot-tutorial/demo.sh` 一键演示下单/成交/盘口/背压。
  - 价值点：撮合盘口 `OrderBook` 故意非线程安全，靠 Disruptor **单消费者无锁串行**跑对——线程池要么加锁争用、要么并发算错。这把"为什么必须 Disruptor"从性能问题升级成正确性问题。
  - 进/出两端（HTTP controller、内存 sink）是薄接缝，生产对应 MQ 消费者 / 发 MQ，撮合中间段与生产逐字一致。

## 已知限制（设计取舍）

- 一种事件类型一条管道，没有"一个总线随手发任意类型"——这是忠于 Disruptor 强类型/零分配的应有之义。
- 阶段内并行采用**条带化（striped）分片**——Disruptor 4.0 已移除 `WorkerPool`/`WorkProcessor`（官方
  [Issue #323](https://github.com/LMAX-Exchange/disruptor/issues/323)，视其为不再维护、设计有瑕疵的历史包袱），
  并推荐"按 key 路由、同 key 固定由同一消费者线程处理"的条带化方案替代；本 starter 的 `parallelism` +
  `ShardKeyed` 即此方案。因此**不提供** WorkerPool 式的动态负载均衡 work-queue（纯任务分发请用 `ThreadPoolExecutor`）。
- 仅进程内、无持久化：进程崩溃丢失在途事件；运行期靠 gating sequence 背压**不覆盖未消费槽**，
  `tryPublish` 满时由调用方决定丢弃。相对 MQ（Kafka 等）无持久化与投递保证——要这些请用消息队列。
  这些是 Disruptor（进程内内存管道）的固有性质，非本 starter 引入。
