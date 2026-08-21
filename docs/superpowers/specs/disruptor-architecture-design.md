# 设计：Disruptor starter 架构（强类型管道）

状态：已实现（2026-08-20 重构落地；后续新增 example / tutorial 示例模块）

> 本文档是当前架构的唯一权威说明，合并并取代了以下已废弃的演进阶段设计（原文见 git 历史）：
> - `2026-08-19-disruptor-listener-annotation`（`@DisruptorListener` + `ConsumerRegistry` 命令式订阅语法糖）
> - `2026-08-20-production-pipeline-architecture-design`（在 Object-payload 总线上加 Stage/DAG + 背压）

## 演进历程（为什么是现在这套）

1. **listener（08-19）**：`@DisruptorListener` 把命令式 `ConsumerRegistry.subscribe` 包成注解语法糖。
2. **production-pipeline（08-20）**：加 Stage/DAG 编排 + 背压，但仍建立在"通用 `Object` 事件总线"上（`EventWrapper` 持 `Object payload`、运行时按 `getClass()` 动态路由）。
3. **强类型管道重构（08-20，当前）**：**推翻** Object-payload 路线。原因——通用 `Object payload` 是 Disruptor 反模式：每次 publish 塞新对象破坏零分配（预分配 + 原地 mutate），payload 堆上任意位置破坏缓存局部性；动态类型路由侵蚀"静态拓扑 + 极简路由 + 专用线程"的机械同情。即：用极致低延迟引擎做了个中庸通用总线，两头不讨好。

**最终定位**：忠于 Disruptor 真本事的**高性能强类型事件处理管道**为核心，其上加 Spring 注解语法糖降样板。核心不妥协、外壳求易用。`@DisruptorListener`/`ConsumerRegistry`/`EventWrapper`/`StageDefinition` 均已删除。

## 核心原则

1. **一条管道 = 一个 `Disruptor<E>` + 一个强类型可变事件 `E`**（预分配、原地 mutate、零分配）。
2. **发布 = 填充预分配槽**（`publish(Consumer<E> filler)`），绝不塞新对象。
3. **处理阶段 = 静态 DAG 拓扑**（Disruptor `then/and`），每阶段一个（可并行分片）EventHandler。
4. **注解或编程式声明拓扑**，starter 自动为每种事件类型建 Disruptor、编排、暴露发布入口。
5. 取舍：没有"一个总线随手发任意类型"；每种事件是一条独立强类型管道。

## 模块与包结构

三个 Maven 模块，依赖方向单向 `starter → autoconfigure → core`：

- **`disruptor-core`**（`com.sstlfsj.disruptor.core`，disruptor + slf4j，**无 Spring**，可独立使用）：
  - 公开 API：`EventPublisher<E>`、`EventBus`、`EventPipeline<E>`（编程式管道定义）、`ShardKeyed`、`Resettable`、`LoggingExceptionHandler`。
  - 构建/运行：`PipelineBuilder`、`DisruptorPipeline<E>`、`Pipelines`、`DefaultEventBus`、`PipelineTopology`、`DisruptorConfig`。
- **`disruptor-spring-boot-autoconfigure`**（`com.sstlfsj.disruptor.autoconfigure`，core + spring-boot-autoconfigure）：
  - `@DisruptorStage`（注解，`@Reflective` 支持 native）、`StagePipelineRegistrar`（容器扫描）、`DisruptorAutoConfiguration`、`DisruptorProperties`、`DisruptorLifecycle`。
- **`disruptor-spring-boot-starter`**：无代码，聚合依赖，使用方引入此模块。

## 组件设计

### 公开 API（`core`）

**`EventPublisher<E>`** — 单条管道发布入口（零分配）：
```java
public interface EventPublisher<E> {
    void publish(Consumer<E> filler);        // 填充预分配事件后发布；ring buffer 满时阻塞
    boolean tryPublish(Consumer<E> filler);  // 非阻塞，满时返回 false（背压）
    long remainingCapacity();                // 剩余可写槽位（堆积 = bufferSize - 本值）
}
```

**`EventBus`** — 跨管道门面（主发布 API，按事件类型定位管道）：
```java
public interface EventBus {
    <E> EventPublisher<E> publisher(Class<E> eventType);
    <E> void publish(Class<E> eventType, Consumer<E> filler);
    <E> boolean tryPublish(Class<E> eventType, Consumer<E> filler);
    long remainingCapacity(Class<?> eventType);
}
```
未知事件类型（无对应管道）→ 抛 `IllegalArgumentException`。

**`EventPipeline<E>`** — 编程式管道定义（注解式的等价替代，作 `@Bean` 暴露）：
```java
EventPipeline.builder("order", OrderEvent.class)
    .stage("validate", svc::validate)
    .stage("persist",  svc::persist).after("validate")
    .stage("audit",    svc::audit).after("validate")
    .stage("notify",   svc::notify).after("persist", "audit")
    .parallelism("persist", 4)
    .build();
```
与注解式统一收集、共用同一套 `PipelineTopology` 编排为 DAG；handler 是内联 `Consumer<E>`（无反射）。

**`ShardKeyed`**（可选）：`Object shardKey()` → parallelism>1 时按 key 分片保序；否则按发布序 round-robin。
**`Resettable`**（可选）：`void reset()` → 每条管道叶子之后由 cleanup handler 调用清空字段供槽位复用。
**`LoggingExceptionHandler`**：泛型化 `ExceptionHandler`，复用于所有管道。

### 声明处理阶段（`autoconfigure`）

**`@DisruptorStage`**（方法级，`@Reflective`）：
```java
@Target(ElementType.METHOD) @Retention(RetentionPolicy.RUNTIME) @Documented @Reflective
public @interface DisruptorStage {
    String pipeline();            // 所属管道名
    String name();                // 阶段名（管道内唯一）
    String[] after() default {};  // 依赖的上游阶段（DAG 边）；空 = 源头
    int parallelism() default 1;  // 阶段内并行分片数
}
```
标注方法签名必须 `void m(E event)`（恰一个参数）；同一 pipeline 各阶段参数类型必须一致 = 该管道事件类型 `E`。

### 装配内部（`core` + `autoconfigure`）

**`DisruptorConfig`**（core，值对象）：`bufferSize` / `waitStrategy` / `shutdownTimeout`，由 `DisruptorProperties.toConfig()` 产出，喂给 `PipelineBuilder` 与 `DisruptorLifecycle`。

**`DisruptorPipeline<E>`**（core）：一条管道运行时载体，持 `eventType`、`Disruptor<E>`、`RingBuffer<E>`；提供 `publish/tryPublish/remainingCapacity`（`EventPublisher<E>` 实现委托）。

**`Pipelines`**（core）：`Map<Class<?>, DisruptorPipeline<?>>` 共享容器；`EventBus` 与 `DisruptorLifecycle` 依赖它。事件类型跨管道唯一，重复 fail-fast。

**`PipelineBuilder`**（core）：吃一份管道定义（注解收集来的 / `EventPipeline`），用 `PipelineTopology` 校验 DAG（阶段名唯一、`after` 存在、无环）+ 拓扑排序，建 `Disruptor<E>`（`EventFactory = E::new`、全局 bufferSize/waitStrategy、`ProducerType.MULTI`、daemon 线程、默认异常处理器），按拓扑注册各阶段 handler（parallelism 分片过滤 + `Resettable` 叶子后 cleanup），不 start。

**`StagePipelineRegistrar`**（autoconfigure，`SmartInitializingSingleton`）：单例就绪后扫描所有 bean 的 `@DisruptorStage` 方法（脱代理、过滤 bridge/synthetic）+ 收集 `EventPipeline` bean，按 pipeline 分组、推断事件类型，交 `PipelineBuilder` 建管道、注册进 `Pipelines`。

**`DisruptorLifecycle`**（autoconfigure，`SmartLifecycle`，phase=`Integer.MIN_VALUE`）：托管 `Pipelines` 中所有 Disruptor——`start()` 逐个 start；`stop()` 逐个带超时 `shutdown` 排空 + `halt` 兜底（在上游停止后才关闭）。

**`DefaultEventBus`**（core，`EventBus` 实现）：依赖 `Pipelines`，按类型查 `DisruptorPipeline` 委托发布。

**`DisruptorProperties`**（autoconfigure，`disruptor.*`）：`bufferSize`（默认1024，2 的幂）、`waitStrategy`（默认 YIELDING）、`shutdownTimeout`（默认10s）。拓扑由注解/`EventPipeline` 声明，不在此配置。

**`DisruptorAutoConfiguration`**：注册 `DisruptorConfig`、`Pipelines`、`PipelineBuilder`、`StagePipelineRegistrar`、`DefaultEventBus`、`DisruptorLifecycle` bean（均 `@ConditionalOnMissingBean`）。

### 装配时序

1. 所有单例实例化（用户 stage bean、`Pipelines`、`EventBus`、`DisruptorLifecycle`、`StagePipelineRegistrar`）。
2. `StagePipelineRegistrar.afterSingletonsInstantiated`：扫描 + 建所有 `Disruptor`（不 start）→ 填充 `Pipelines`。
3. `DisruptorLifecycle.start`（SmartLifecycle，晚于步骤2）：start 所有 Disruptor。
4. 运行期：`EventBus.publish` 查 `Pipelines` 取 RingBuffer 填充发布。

## 使用示例

```java
public class OrderEvent {                 // 强类型可变事件（预分配、原地 mutate）
    private String orderId; private long amount;
    // getters/setters
}

@Component
public class OrderPipeline {
    @DisruptorStage(pipeline = "order", name = "validate")
    public void validate(OrderEvent e) { ... }

    @DisruptorStage(pipeline = "order", name = "persist", after = "validate")
    public void persist(OrderEvent e) { ... }

    @DisruptorStage(pipeline = "order", name = "notify", after = "persist")
    public void notify(OrderEvent e) { ... }
}

@Service @RequiredArgsConstructor
public class OrderService {
    private final EventBus eventBus;
    public void create(String id, long amount) {
        eventBus.publish(OrderEvent.class, e -> { e.setOrderId(id); e.setAmount(amount); });
    }
}
```

参见 example 模块（六个特性 demo + 纯 Java）与 tutorial 模块（撮合 web 场景）的可跑示例。

## 测试策略

`ApplicationContextRunner` 黑盒 + 纯逻辑单测：强类型 e2e、零分配复用（identity 断言）、DAG 顺序/菱形、并行分片（`ShardKeyed` 保序）、背压（`tryPublish`/`remainingCapacity`）、fail-fast（非单参/参数类型不一致/事件类型跨管道重复/DAG 环或缺依赖/无无参构造/未知类型发布）、`PipelineTopology` 纯逻辑单测、native `@Reflective`。
