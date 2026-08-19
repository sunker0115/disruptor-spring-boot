# disruptor-spring-boot-starter

基于 [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) 4.0 的异步事件总线 Spring Boot Starter。
引入即用，提供一个进程内、非阻塞、按事件运行时类型路由的发布/订阅事件总线。

## 特性

- **零配置自动装配**：引入依赖即提供 `EventPublisher`、`ConsumerRegistry` 两个 bean，无需 `@Enable` 注解。
- **声明式监听**：在任意 Spring bean 的方法上加 `@DisruptorListener`，容器启动时自动注册，用法对齐 Spring `@EventListener`。
- **非阻塞发布**：`publish` 写入 RingBuffer 后立即返回，由后台消费线程异步派发。
- **按运行时类型路由**：事件按 `getClass()` 精确匹配分发；同一类型可注册多个消费者。
- **异常隔离**：单个消费者抛异常只记日志并跳过，不影响同类型其它消费者，消费线程不会因此终止。
- **优雅关闭**：应用关闭时先排空 RingBuffer（有超时上限）再停止，尽量不丢已发布未消费的事件。

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

### 3. 注册消费者

在启动时订阅（`subscribe` 并发安全，同一类型可注册多个消费者，都会收到事件）：

```java
@Component
@RequiredArgsConstructor
public class OrderEventSubscriber {

    private static final Logger log = LoggerFactory.getLogger(OrderEventSubscriber.class);

    private final ConsumerRegistry registry;

    @PostConstruct
    public void subscribe() {
        registry.subscribe(OrderCreatedEvent.class, event ->
                log.info("异步处理订单：orderId={}, amount={}", event.orderId(), event.amount()));

        registry.subscribe(OrderCreatedEvent.class, event ->
                log.info("发送下单通知：{}", event.orderId()));
    }
}
```

### 3.1 声明式监听（推荐）

除手动 `subscribe`，也可在任意 Spring bean 的方法上加 `@DisruptorListener`，容器启动时自动注册。
方法必须恰好一个参数，参数类型即监听的事件类型：

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
- **与命令式混用**：`@Order` 只保证注解监听器之间的相对顺序；注解式与命令式 `subscribe`
  混用时，两者的相对先后不保证（命令式按运行时调用时机注册）。
- **GraalVM native image**：`@DisruptorListener` 已用 Spring `@Reflective` 元注解标注，
  Spring AOT 会在构建期自动注册反射 hints，native 镜像下无需额外配置。

### 4. 发布事件

```java
@Service
@RequiredArgsConstructor
public class OrderService {

    private final EventPublisher eventPublisher;

    public void createOrder(String orderId, long amount) {
        // ... 落库等同步逻辑 ...
        eventPublisher.publish(new OrderCreatedEvent(orderId, amount)); // 非阻塞，不阻塞主流程
    }
}
```

## 配置项

全部可选，前缀 `disruptor`，下表为默认值：

| 配置项                       | 类型            | 默认值      | 说明                                                         |
|------------------------------|-----------------|-------------|--------------------------------------------------------------|
| `disruptor.buffer-size`      | int             | `1024`      | RingBuffer 大小，**必须是 2 的幂**。                          |
| `disruptor.wait-strategy`    | 枚举            | `YIELDING`  | 消费者无事件时的等待策略，见下表。                            |
| `disruptor.shutdown-timeout` | Duration        | `10s`       | 关闭时排空 RingBuffer 的等待上限，超时则强制 `halt`（可能丢弃未消费事件）。 |

`wait-strategy` 可选值：

| 值          | 对应策略                  | 特点                                             |
|-------------|---------------------------|--------------------------------------------------|
| `BLOCKING`  | `BlockingWaitStrategy`    | 用锁与条件变量等待，CPU 占用最低，延迟较高。      |
| `YIELDING`  | `YieldingWaitStrategy`    | 自旋 + `Thread.yield()`，延迟与 CPU 折中（默认）。|
| `BUSY_SPIN` | `BusySpinWaitStrategy`    | 纯自旋，延迟最低、CPU 占用最高。                  |
| `SLEEPING`  | `SleepingWaitStrategy`    | 自旋后短暂 `sleep`，低 CPU、延迟中等。            |

配置示例（`application.yml`）：

```yaml
disruptor:
  buffer-size: 2048
  wait-strategy: BLOCKING
  shutdown-timeout: 30s
```

## 行为说明与约束

- **精确类型匹配**：`publish(new OrderCreatedEvent(...))` 只触发订阅 `OrderCreatedEvent.class` 的消费者；
  按父类或接口订阅**收不到**子类事件。
- **单线程串行消费**：所有类型、所有消费者运行在同一个后台守护线程 `disruptor-event-bus` 上串行执行。
  消费逻辑应尽量轻量、快速返回；耗时任务请在消费者内部再转交业务线程池，避免阻塞整个事件总线。
- **异常处理**：消费者抛出的异常会被记为 ERROR 日志后跳过，不会传播、不会终止消费线程。
- **发布 `null`**：会被静默忽略（无对应类型、不分发）。
- **进程内、非持久化**：事件仅在当前 JVM 内传递，不落盘、不跨进程；进程崩溃时 RingBuffer 中未消费的事件会丢失。

## 覆盖默认 bean

对外 bean 均标注 `@ConditionalOnMissingBean`，如需自定义实现，在自己的配置中声明同类型 bean 即可覆盖：

```java
@Bean
public ConsumerRegistry consumerRegistry() {
    return new MyCustomConsumerRegistry();
}
```

> 自动装配还提供 `DisruptorListenerRegistrar`（负责扫描 `@DisruptorListener`），同样可通过声明同类型 bean 覆盖。
