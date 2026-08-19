# disruptor-spring-boot-starter

基于 [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) 4.0 的异步事件总线 Spring Boot Starter。
引入即用：进程内、按事件运行时类型路由的发布/订阅总线，并支持用 Disruptor 原生依赖图做**阶段流水线编排**。

## 特性

- **零配置自动装配**：引入依赖即提供 `EventPublisher`、`ConsumerRegistry` 两个 bean，无需 `@Enable` 注解。
- **声明式监听**：在任意 Spring bean 的方法上加 `@DisruptorListener`，容器启动自动注册，用法对齐 Spring `@EventListener`。
- **按运行时类型路由**：事件按 `getClass()` 精确匹配分发；同一类型可注册多个消费者，`@Order` 控制顺序。
- **阶段流水线编排**：声明命名阶段与依赖（DAG），用 Disruptor `then/and` 表达 `A→B→C`、`A→(B‖C)→D`，同一事件按阶段顺序流经处理。
- **背压**：`tryPublish` 非阻塞发布（满时返回 `false`）、`remainingCapacity` 暴露堆积，便于降级与监控。
- **异常隔离**：单个消费者抛异常只记日志并跳过，不影响同类型/同阶段其它消费者，消费线程不终止。
- **优雅关闭**：应用关闭时先排空 RingBuffer（带超时上限）再停止，尽量不丢已发布未消费的事件。
- **GraalVM native image 友好**：`@DisruptorListener` 用 Spring `@Reflective` 元注解标注，AOT 自动注册反射 hints。

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

### 2. 定义事件

任意 POJO / record 均可，事件按运行时类型路由：

```java
public record OrderCreatedEvent(String orderId, long amount) {}
```

### 3. 声明式监听（推荐）

在任意 Spring bean 的方法上加 `@DisruptorListener`，方法必须恰好一个参数，参数类型即监听的事件类型：

```java
@Component
public class OrderSubscriber {

    @DisruptorListener
    public void onOrder(OrderCreatedEvent e) {
        // 处理下单事件
    }

    @DisruptorListener
    @Order(1)                 // 同类型多监听器时，值越小越先调用
    public void audit(OrderCreatedEvent e) {
        // 审计
    }
}
```

- **参数校验**：方法参数数不为 1 时，应用启动即失败（fail-fast），不会静默不生效。
- **仅 public 方法**：只有 public 方法上的 `@DisruptorListener` 会被识别。

### 4. 命令式订阅（可选）

也可在启动时手动订阅（作用于默认阶段，`subscribe` 并发安全）：

```java
@Component
@RequiredArgsConstructor
public class OrderEventSubscriber {

    private final ConsumerRegistry registry;

    @PostConstruct
    public void subscribe() {
        registry.subscribe(OrderCreatedEvent.class, e -> { /* ... */ });
    }
}
```

### 5. 发布事件

```java
@Service
@RequiredArgsConstructor
public class OrderService {

    private final EventPublisher publisher;

    public void createOrder(String orderId, long amount) {
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, amount);

        publisher.publish(event);            // 常规发布：ring buffer 满时会阻塞发布线程直到有空槽

        // 或非阻塞发布：满时返回 false，由调用方决定降级（丢弃 / 落库 / 告警）
        if (!publisher.tryPublish(event)) {
            // 处理背压：例如记录并丢弃，或转持久化队列
        }
    }
}
```

发布线程与消费线程解耦，事件由后台消费线程异步派发。

## 阶段流水线编排

当多个处理环节对同一事件有**先后依赖**时（如"先校验 → 再持久化 → 再通知"），用流水线编排。

**模型**：每个命名阶段（Stage）是一个 Disruptor EventHandler，看到流经的每个事件；阶段内部按事件类型分发给该阶段的监听器；阶段之间按声明的依赖用 Disruptor 依赖图串联——**同一事件被上游阶段处理完，才进入下游阶段**。

阶段依赖在 `disruptor.pipeline` 声明；监听器用 `@DisruptorListener(stage = "...")` 归属阶段。隐式的 `default` 阶段始终存在（无依赖的源头），未标 `stage` 的监听器与命令式 `subscribe` 都归入它。

**线性流水线 `default → persist → notify`：**

```yaml
disruptor:
  pipeline:
    persist: { after: [default] }
    notify:  { after: [persist] }
```

```java
@Component
public class OrderPipeline {

    @DisruptorListener                       // default 阶段：校验
    public void validate(OrderCreatedEvent e) { /* ... */ }

    @DisruptorListener(stage = "persist")    // 在 validate 之后
    public void persist(OrderCreatedEvent e) { /* ... */ }

    @DisruptorListener(stage = "notify")     // 在 persist 之后
    public void notify(OrderCreatedEvent e) { /* ... */ }
}
```

**菱形 `default → (persist ‖ audit) → notify`：**

```yaml
disruptor:
  pipeline:
    persist: { after: [default] }
    audit:   { after: [default] }
    notify:  { after: [persist, audit] }   # persist、audit 都完成后才执行
```

- 阶段间通过同一事件对象传递数据：上游阶段修改事件负载，下游阶段可见（Disruptor 原生用法）。
- 引用未在 `pipeline` 声明的阶段（`default` 除外）→ 启动即失败（fail-fast）。
- `pipeline` 存在环或依赖不存在的阶段 → 启动即失败。

## 配置项

前缀 `disruptor`，均可选：

| 配置项                       | 类型                          | 默认值      | 说明                                                         |
|------------------------------|-------------------------------|-------------|--------------------------------------------------------------|
| `disruptor.buffer-size`      | int                           | `1024`      | RingBuffer 大小，**必须是 2 的幂**。                          |
| `disruptor.wait-strategy`    | 枚举                          | `YIELDING`  | 消费者无事件时的等待策略，见下表。                            |
| `disruptor.shutdown-timeout` | Duration                      | `10s`       | 关闭时排空 RingBuffer 的等待上限，超时则强制 `halt`（可能丢弃未消费事件）。 |
| `disruptor.pipeline`         | Map<String, {after: List}>    | 空          | 阶段流水线声明：阶段名 → 依赖的上游阶段。空时仅有隐式 default 单阶段。 |

`wait-strategy` 可选值：

| 值          | 对应策略                  | 特点                                             |
|-------------|---------------------------|--------------------------------------------------|
| `BLOCKING`  | `BlockingWaitStrategy`    | 用锁与条件变量等待，CPU 占用最低，延迟较高。      |
| `YIELDING`  | `YieldingWaitStrategy`    | 自旋 + `Thread.yield()`，延迟与 CPU 折中（默认）。|
| `BUSY_SPIN` | `BusySpinWaitStrategy`    | 纯自旋，延迟最低、CPU 占用最高。                  |
| `SLEEPING`  | `SleepingWaitStrategy`    | 自旋后短暂 `sleep`，低 CPU、延迟中等。            |

## 行为说明与约束

- **发布语义**：`publish` 在 ring buffer 满时**阻塞**发布线程直到有空槽；`tryPublish` 非阻塞，满时返回 `false`。`remainingCapacity()` 返回剩余可写槽位（堆积 = bufferSize − 剩余），可接入监控。
- **精确类型匹配**：只触发订阅了该事件运行时类型的消费者；按父类或接口订阅**收不到**子类事件。
- **阶段内单线程串行**：同一阶段的所有监听器在该阶段的一个线程内串行执行；跨阶段才是不同线程 + 依赖编排。阶段内消费逻辑应轻量，耗时任务转交业务线程池。
- **命令式订阅仅作用于 default 阶段**：`ConsumerRegistry.subscribe` 注册到 default 阶段；要挂到其它阶段请用 `@DisruptorListener(stage = ...)`。
- **`@Order` 边界**：仅保证同一阶段、同一事件类型内注解监听器之间的顺序；与命令式 `subscribe` 混用时相对先后不保证。
- **异常处理**：消费者抛出的异常被记为 ERROR 日志后跳过，不传播、不终止消费线程。
- **发布 `null`**：被静默忽略（无对应类型、不分发）。
- **进程内、非持久化**：事件仅在当前 JVM 内传递，不落盘、不跨进程；进程崩溃时 RingBuffer 中未消费事件会丢失。

## 覆盖默认 bean

对外 bean 均标注 `@ConditionalOnMissingBean`，声明同类型 bean 即可覆盖：

```java
@Bean
public ConsumerRegistry consumerRegistry() {
    return new MyCustomConsumerRegistry();   // 作为 default 阶段的路由表
}
```

> 自动装配还提供 `DisruptorListenerRegistrar`（扫描 `@DisruptorListener`）、`PipelineTopology`、`StageRegistries` 等，同样可按需覆盖。

## 已知限制（设计取舍）

- 阶段内消费为单线程串行（Disruptor 4.0 已移除 WorkerPool，不提供阶段内并行负载均衡）。
- 仅按精确运行时类型路由，不支持按父类/接口订阅。
- 流水线为高级用法：多阶段模式下不清空 ring buffer 槽位（payload 引用滞留至槽位被下次发布覆盖，最多 bufferSize 个）；单阶段（零配置）模式会清空。
