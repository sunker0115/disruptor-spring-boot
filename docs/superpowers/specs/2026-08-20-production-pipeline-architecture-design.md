# 设计：生产级最终架构（阶段流水线编排 + 背压）

日期：2026-08-20
状态：已确认（用户全权授权），实现中

## Context（背景与目标）

当前 starter 把 Disruptor 用"浅"了：只注册单个 EventHandler 做类型路由分发，
未利用 Disruptor 的招牌能力——**用单 ring buffer 表达消费者依赖图（流水线编排）**；
同时发布语义有生产级缺口（`publish` 满时阻塞但文档称"非阻塞"、无背压出口、无堆积可观测）。

本次从最终架构出发，一次性补齐生产级能力，且**保证不退化、不变形、对用户易用**：
1. **阶段流水线编排**：声明式命名阶段（Stage）+ DAG 依赖，映射 Disruptor 原生 `then/and` 拓扑。
2. **背压**：非阻塞 `tryPublish` + 堆积可观测 `remainingCapacity`，修正"非阻塞"文档。
3. **渐进复杂度**：零配置时行为与现状完全一致（单默认阶段），只有需要流水线的用户才接触 Stage 概念。

## 核心架构洞察

Disruptor 是 **multicast** 模型：每个 EventHandler 看到流经 ring buffer 的每个事件；
阶段依赖由 handler 间的 sequence barrier（`then`/`and`）表达。类型路由与流水线**正交**，可统一为：

- **一个 Stage = 一个 Disruptor EventHandler**（看到所有事件）。
- **Stage 内部** = 现有类型路由（该 Stage 独立的 `ConsumerRegistry`，按事件运行时类型分发给订阅者，`@Order` 在 Stage 内生效）。
- **Stage 之间** = Disruptor DAG 依赖（`handleEventsWith(a).then(b,c).then(d)`），保证同一事件被上游 Stage 处理完才进入下游 Stage。

零配置：只有一个隐式的 `default` Stage → 单 EventHandler → 与当前行为完全等价（**不退化**）。

WorkerPool 在 Disruptor 4.0 已移除（Issue #323），并行负载均衡非 4.0 路线，故不提供；
流水线（multicast + 依赖）是 4.0 的正道。

## 组件设计

### 1. 背压与可观测（`event` 包，公开 API）

`EventPublisher` 扩展（保留 `publish` 不变，向后兼容）：
- `boolean tryPublish(Object event)`：非阻塞。ring buffer 满时返回 `false`（`ringBuffer.tryNext()` 抛 `InsufficientCapacityException` → 捕获返回 false），不阻塞发布线程。
- `long remainingCapacity()`：ring buffer 剩余可写槽位，供使用方接入监控（堆积 = bufferSize - remaining）。

`RingBufferEventPublisher` 实现二者。`publish` 语义保持（满时阻塞，`ringBuffer.next()`）。
README 修正"非阻塞发布"为"`publish` 满时阻塞；`tryPublish` 非阻塞"。

> 监控：本次仅提供 `remainingCapacity()` 只读出口（零新依赖）。Micrometer 集成
> （`@ConditionalOnClass(MeterRegistry)` 自动注册 gauge）作为架构预留的后续增强，本次不实现。

### 2. `@DisruptorListener` 增加 `stage` 属性（`event` 包）

```java
public @interface DisruptorListener {
    String stage() default "";   // 空 = default 阶段
}
```
`@Reflective` 保留。空字符串归入 `default` Stage（不指定 stage 的既有用法不变）。

### 3. Stage 与流水线配置（`autoconfigure` 包）

`DisruptorProperties` 增加：
```java
private Map<String, StageDefinition> pipeline = new LinkedHashMap<>();
```
`StageDefinition`：`private List<String> after = new ArrayList<>();`（依赖的上游 Stage 名）。
绑定示例：
```yaml
disruptor:
  pipeline:
    validate: {}                       # 无依赖，源头
    persist:  { after: [validate] }
    audit:    { after: [validate] }
    notify:   { after: [persist, audit] }   # 菱形汇聚
```
`default` Stage 隐式存在（无依赖源头），无需在 pipeline 声明；用户未标 `stage` 的监听器
与命令式 `subscribe` 都归 `default`。若用户在 pipeline 显式声明 `default` 则以其为准。

### 4. 流水线拓扑构建（`autoconfigure` 包，新增 `PipelineTopology`）

纯逻辑组件（易单测），职责：
1. 汇总所有 Stage（`default` + `pipeline` 配置的 + 被 `@DisruptorListener(stage=x)` 引用的）。
2. **校验（fail-fast，启动即失败）**：`after` 引用的 Stage 必须存在；DAG 无环（DFS 检测）。
3. 拓扑排序，产出确定的注册顺序。

`DisruptorAutoConfiguration.disruptor(...)` 改为按拓扑构建：
- 每个 Stage 一个 `ConsumerRegistry`（`DefaultConsumerRegistry`）+ 一个 EventHandler（闭包调该 registry `dispatch`）。
- 无依赖 Stage：`EventHandlerGroup g = disruptor.handleEventsWith(handler)`。
- 有依赖 Stage：合并依赖组 `dep = g[d1].and(g[d2])...`，`g[stage] = dep.handleEventsWith(handler)`。
- 保存每个 Stage 的 group 供下游引用。

**槽位清理取舍**：单 `default` Stage（零配置）时，handler 内联 `wrapper.clear()`（保留现有 G3 优化，不退化）。
多 Stage 时不清理（payload 引用滞留至槽位被下次发布覆盖）——避免多个并行叶子 Stage 并发 `clear` 同一 wrapper 的数据竞争；pipeline 为高级用法，滞留最多 bufferSize 个引用，可接受。

### 5. Stage 路由表管理（`autoconfigure` 包，新增 `StageRegistries`）

持有 `Map<String, ConsumerRegistry>`（每 Stage 一个）。
- `default` Stage 的 registry 即作为公开 `ConsumerRegistry` bean 暴露（命令式 `subscribe` 作用于 default，语义不变）。
- 其它 Stage 的 registry 内部持有，供 EventHandler 与 `DisruptorListenerRegistrar` 使用。

`DisruptorListenerRegistrar` 改为按 `(stage, eventType)` 分组，注册到对应 Stage 的 registry；
`@Order` 在 Stage 内、类型内排序（现有逻辑不变，仅多一层 stage 维度）。
引用不存在的 stage → fail-fast。

## 不退化保证（关键约束）

- `EventPublisher.publish` / `ConsumerRegistry.subscribe` / `@DisruptorListener`（无 stage）/ `@Order` /
  优雅关闭 / 异常隔离 / imports 自动发现 —— 全部保持。现有 8 个测试须持续绿。
- 零配置（无 `disruptor.pipeline`）时：仅 `default` Stage，单 EventHandler + 内联 clear = 当前实现的等价行为。

## 测试策略

沿用 `ApplicationContextRunner` 黑盒风格，新增：
- 背压：`tryPublish` 正常投递；`remainingCapacity` 反映容量。
- 拓扑校验：环 / 缺失依赖 → 启动 fail-fast。
- 流水线顺序：配置 `a → b`，`@DisruptorListener(stage=...)` 验证同一事件先被 a 阶段、后被 b 阶段处理（用时序 trace 断言跨阶段顺序）。
- 菱形：`a → (b,c) → d`，验证 d 在 b、c 都处理后才执行。
- 不退化：现有全部测试保持绿。

## 包结构（沿用 Spring Boot 惯例）

- `com.sstlfsj.disruptor.event`（库本体 / 公开 API）：`EventPublisher`、`ConsumerRegistry`、`DisruptorListener`、`DefaultConsumerRegistry`、`RingBufferEventPublisher`、`EventWrapper`、`LoggingExceptionHandler`。
- `com.sstlfsj.disruptor.autoconfigure`（自动装配）：`DisruptorAutoConfiguration`、`DisruptorProperties`（含 `StageDefinition`）、`DisruptorLifecycle`、`DisruptorListenerRegistrar`、`PipelineTopology`、`StageRegistries`。

## 本次交付范围

含：背压（tryPublish/remainingCapacity）+ 文档修正、Stage 流水线编排（DAG + 校验）、
不退化、README/设计文档更新。
不含（架构已预留、列为后续）：Micrometer 指标自动装配、Stage 内并行消费、按接口/父类路由。
