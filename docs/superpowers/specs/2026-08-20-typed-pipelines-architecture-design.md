# 设计：强类型管道最终架构（重构）

日期：2026-08-20
状态：已确认方向（用户批准路线1 + 语法糖），重构实现中

## Context（为什么推翻重构）

现有实现把 Disruptor 当"通用 Object 事件总线"用：`EventWrapper` 持有 `Object payload`、
运行时按 `getClass()` 动态类型路由。研究确认这在架构根基上"两头不讨好"：

- **通用 `Object payload` 是 Disruptor 反模式**：每次 publish 塞一个新 payload 对象 → 破坏
  Disruptor 的零分配（预分配 + 原地 mutate）；payload 在堆上任意位置 → 破坏缓存局部性/prefetch。
- **动态类型路由**侵蚀 Disruptor 赖以高性能的"静态拓扑 + 极简路由 + 专用线程"机械同情。

即：用一个为极致低延迟设计的引擎，做了个中庸的通用总线，既没拿到 Disruptor 真本事，
又背上动态路由的每事件成本。这是"变形"的根。

**最终定位（用户确认）**：忠于 Disruptor 真本事的**高性能事件处理管道**（路线1）为核心，
其上加 Spring 注解语法糖降样板（易用）。核心不妥协、外壳求易用。

## 核心原则

1. **一条管道 = 一个 `Disruptor<E>` + 一个强类型可变事件 `E`**（预分配、原地 mutate、零分配）。
2. **发布 = 填充预分配槽**（`publish(Consumer<E> filler)`），绝不塞新对象。
3. **处理阶段 = 静态 DAG 拓扑**（Disruptor `then/and`），每阶段一个（可并行分片）EventHandler。
4. **注解声明拓扑**，starter 自动为每种事件类型建 Disruptor、编排、暴露发布入口。
5. 取舍（路线1 应有之义）：没有"一个总线随手发任意类型"；每种事件是一条独立强类型管道。

## 组件设计

### 公开 API（`com.sstlfsj.disruptor.event` 包）

**`EventPublisher<E>`** — 单条管道的发布入口（零分配）：
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
    <E> EventPublisher<E> publisher(Class<E> eventType); // 取某类型的发布入口
    <E> void publish(Class<E> eventType, Consumer<E> filler);
    <E> boolean tryPublish(Class<E> eventType, Consumer<E> filler);
    long remainingCapacity(Class<?> eventType);
}
```
未知事件类型（无对应管道）→ 抛 `IllegalArgumentException`（明确报错）。

**`@DisruptorStage`** — 声明处理阶段（方法级，`@Reflective` 支持 native）：
```java
@Target(ElementType.METHOD) @Retention(RetentionPolicy.RUNTIME) @Documented @Reflective
public @interface DisruptorStage {
    String pipeline();            // 所属管道名
    String name();                // 阶段名（管道内唯一）
    String[] after() default {};  // 依赖的上游阶段（DAG 边）；空 = 源头
    int parallelism() default 1;  // 阶段内并行分片数
}
```
标注方法签名必须为 `void m(E event)`（恰好一个参数）。同一 pipeline 各阶段方法的参数类型
必须一致 = 该管道的事件类型 `E`。

**`ShardKeyed`**（可选）：事件实现 `Object shardKey()` → parallelism>1 时按 key 分片保序；
否则按发布序 round-robin 分片。

**`Resettable`**（可选）：事件实现 `void reset()` → 每条管道叶子之后由 cleanup handler 调用，
清空字段供槽位复用（避免下轮 filler 未覆盖的字段残留旧值）。不实现则不加 cleanup handler。

**`LoggingExceptionHandler`**（保留，泛型化 `ExceptionHandler<Object>` 复用于所有管道）。

### 装配内部（`com.sstlfsj.disruptor.autoconfigure` 包）

**`DisruptorPipeline<E>`** — 一条管道的运行时载体：持有 `eventType`、`Disruptor<E>`、
`RingBuffer<E>`；提供 `publish/tryPublish/remainingCapacity`（供 `EventPublisher<E>` 实现委托）。

**`Pipelines`** — 共享容器：`Map<Class<?>, DisruptorPipeline<?>>`，初始空。由 `PipelineRegistrar` 填充；
`EventBus` 与 `DisruptorLifecycle` 依赖它读取。

**`PipelineRegistrar`（`SmartInitializingSingleton`）** — 核心装配：
1. 遍历所有 bean（`ConfigurableListableBeanFactory`，跳过 abstract、用 `ClassUtils.getUserClass` 脱代理、
   `AnnotatedElementUtils.findMergedAnnotation` 找 `@DisruptorStage`、过滤 bridge/synthetic）。
2. 校验方法签名单参数；按 `pipeline` 分组；推断事件类型 `E`（组内参数类型必须一致，否则 fail-fast）；
   校验事件类型跨管道唯一（否则 fail-fast）。
3. 每个 pipeline 用 `PipelineTopology.build`（无隐式 default，阶段全来自注解）校验 DAG（阶段名唯一、
   `after` 引用存在、无环）并拓扑排序。
4. 建 `Disruptor<E>`（`EventFactory = E::new` 反射无参构造、全局 bufferSize/waitStrategy、
   `ProducerType.MULTI`、daemon threadFactory、`setDefaultExceptionHandler`），按拓扑注册各阶段
   handler（parallelism 分片、cleanup 见下），**不 start**，存入 `Pipelines`。

**阶段 handler**（每阶段，parallelism 个分片）：
```java
EventHandler<E> h = (event, sequence, endOfBatch) -> {
    if (n > 1 && shardOf(event, sequence, n) != shardId) return;   // 分片过滤
    ReflectionUtils.invokeMethod(stageMethod, bean, event);         // 调用 @DisruptorStage 方法
};
```
拓扑编排：无依赖阶段 `disruptor.handleEventsWith(handlers)`；有依赖阶段合并依赖组
`dep1.and(dep2)...handleEventsWith(handlers)`。若事件实现 `Resettable`，所有叶子之后接单线程
cleanup handler 调 `event.reset()`。

**`EventFactory` 与预分配**：`E::new`（要求事件类有可访问无参构造，否则 fail-fast，报清晰错误）。

**`PipelineTopology`**（复用，去掉隐式 default）：输入某 pipeline 的 `stage -> after`，校验 + 拓扑排序 + 叶子识别。

**`DisruptorLifecycle`（`SmartLifecycle`，phase=Integer.MIN_VALUE）**：改为托管 `Pipelines` 中**所有**
Disruptor：`start()` 逐个 `disruptor.start()`；`stop()` 逐个带超时 `shutdown` + halt 兜底。

**`DefaultEventBus`（`EventBus` 实现）**：依赖 `Pipelines`，按类型查 `DisruptorPipeline` 委托发布。

**`DisruptorProperties`**：`bufferSize`（默认1024）、`waitStrategy`（默认 YIELDING）、
`shutdownTimeout`（默认10s）。**去掉** `pipeline` map（拓扑改由注解声明）。

**`DisruptorAutoConfiguration`**：注册 `Pipelines`、`PipelineRegistrar`、`DefaultEventBus`、
`DisruptorLifecycle` bean（均 `@ConditionalOnMissingBean`）。

### 装配时序

1. 所有单例实例化（用户 `@Component` stage bean、`Pipelines`、`EventBus`、`DisruptorLifecycle`、`PipelineRegistrar`）。
2. `PipelineRegistrar.afterSingletonsInstantiated`：扫描 + 建所有 `Disruptor`（不 start）→ 填充 `Pipelines`。
3. `DisruptorLifecycle.start`（SmartLifecycle，晚于步骤2）：start 所有 Disruptor。
4. 运行期：`EventBus.publish` 查 `Pipelines` 取 RingBuffer 填充发布。

## 删除 / 保留

- **删**：`EventWrapper`、`ConsumerRegistry`、`DefaultConsumerRegistry`、`StageRegistries`、
  旧 `RingBufferEventPublisher`、旧 `EventPublisher`(Object 版)、`DisruptorListener`、
  `DisruptorListenerRegistrar`、`StageDefinition`。
- **重写**：`DisruptorAutoConfiguration`、`DisruptorProperties`、`DisruptorLifecycle`、`PipelineTopology`。
- **保留**：`LoggingExceptionHandler`（泛型化）；`META-INF/spring/...imports`（指向重写后的 AutoConfiguration）。
- **现有测试**：基于 Object 路由的全部重写为强类型管道测试（不保留将就）。

## 测试策略（`ApplicationContextRunner` 黑盒 + 纯逻辑单测）

- 强类型管道 e2e：定义事件类 + `@DisruptorStage`，`EventBus.publish(filler)` → 阶段方法收到填充后的事件。
- 零分配填充：发布多次，事件对象被复用（同一实例 mutate，可用 identity 断言）。
- DAG：线性顺序、菱形汇聚（跨阶段 trace 顺序）。
- 并行分片：parallelism=N，每事件恰由一个分片处理（计数 == 事件数）；`ShardKeyed` 同 key 保序。
- 背压：`tryPublish` 满时 false、`remainingCapacity`。
- fail-fast：非单参数、同管道参数类型不一致、事件类型跨管道重复、DAG 环/缺失依赖、无无参构造、未知类型发布。
- 拓扑纯逻辑单测复用。
- native：`@DisruptorStage` 的 `@Reflective`。

## 包结构

- `event`（公开 API）：`EventPublisher`、`EventBus`、`DisruptorStage`、`ShardKeyed`、`Resettable`、`LoggingExceptionHandler`。
- `autoconfigure`（装配）：`DisruptorAutoConfiguration`、`DisruptorProperties`、`DisruptorLifecycle`、
  `PipelineTopology`、`PipelineRegistrar`、`Pipelines`、`DisruptorPipeline`、`DefaultEventBus`。

## 使用示例（语法糖，最终形态）

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
