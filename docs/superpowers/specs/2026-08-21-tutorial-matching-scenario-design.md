# 设计：tutorial 撮合场景（disruptor-spring-boot-tutorial）

日期：2026-08-21
状态：已确认，待实现

> 取代 `2026-08-20-example-module-design.md` 中「模块二 tutorial」原定的"下单主流程"场景。example 模块不受影响。

## 为什么是撮合

tutorial 的定位与 example 不同：example 逐个演示特性（"每个能力单独怎么用"），tutorial 要用**一个真实场景**回答"为什么必须用 Disruptor，而不是 `@Async` 线程池 / `ApplicationEventPublisher` / Kafka"。

选定 **撮合（order matching）**，理由是价值论证最硬：

- 撮合盘口 `OrderBook` **故意非线程安全**——内部裸 `TreeMap` + `++sequence`，撮合主循环边遍历对手盘边改盘口。它**要求单线程调用者**。
- 线程池方案在这里不是"慢"，是**直接算错**（并发改 `TreeMap` / 竞态 sequence）。要正确就得加锁，而锁正是撮合的头号瓶颈。
- Disruptor 的单写者 / 单消费者是**唯一**能让这段无锁跑对的机制。于是"为什么 Disruptor"从性能问题升级成**正确性问题**——比"下单异步解耦"那种"用线程池也行"的场景说服力强得多。
- 撮合是 Disruptor 的亲妈场景（LMAX 交易所），用它最名正言顺。

## 用 vs 不用 Disruptor 的区别（tutorial 要讲清的价值）

"不用"的现实替代 = 请求线程直接撮合（Tomcat 线程）、`@Async` 线程池、或手写 `BlockingQueue`+worker。

| 维度 | 不用（线程池 / 同步 / 手写队列） | 用 Disruptor |
|---|---|---|
| **正确性** | `OrderBook` 非线程安全，并发改 `TreeMap`+竞态 `++sequence` → 丢单/算错/序列乱；要修只能加锁 → 争用、吞吐塌方 | 单消费者串行，**无锁却始终正确**（single-writer 原则） |
| **顺序/可确定** | 线程池不保证处理序 → 结果非确定、不可回放 | 严格 FIFO 环，同输入同输出、可审计可回放 |
| **延迟/吞吐** | 队列每次 put/take 走锁+条件变量+分配+上下文切换 → 慢一两个数量级、尾延迟抖动 | 预分配槽位+无锁+缓存行友好 → 纳秒级交接（LMAX 单线程 600 万+/秒） |
| **背压** | `@Async`/`ApplicationEventPublisher` 队列无界 → 突发内存无限涨 → OOM | 环有界，`tryPublish` 满返回 false → 显式甩负载（429） |
| **请求路径** | 同步撮合阻塞 HTTP 线程，p99=撮合耗时，线程被占满 | POST 微秒级返回 202，撮合在请求路径外 |
| **vs Kafka/MQ** | 多一跳网络（ms）+序列化+运维；适合跨服务/持久化 | 进程内纳秒、零基础设施；适合单机撮合 |

结论：这个场景里 Disruptor 的核心价值**不是"快一点"，是"无锁却正确 + 有序可回放 + 有界背压"**——把并发难题做成 correct-by-construction。

**用户感知**：
- API 调用方（下单者）：高负载下 POST 仍瞬时返回；突发是干脆的 429 而非超时/雪崩；成交结果始终一致（不丢/不重/不乱序）；延迟低而稳。
- 接入方（开发者）：只写 `@DisruptorStage` + `tryPublish`，白得"单线程正确+有序+背压"，不必手写 队列+worker+锁、不必推敲 `OrderBook` 线程安全。

**在 tutorial 里的可感证据**：日志里所有 `[matching/match]` 都在同一线程名上——并发进来的订单被单线程无锁串行撮合，而 `GET /orders/stats` 每次跑都一致正确；灌满时冒 429。

## 裁剪原则（本 tutorial 的核心约束）

**中间的 Disruptor 撮合管道做实，订单的"进 / 出"两端留薄接缝，但整条流程完整、端到端能跑。**

```
POST /orders (controller) ── eventBus.tryPublish(OrderEvent) ──┐
   满 → HTTP 429（背压，真状态码）                              │
   成功 → HTTP 202 + orderId（仅受理回执，撮合异步，不同步返回成交）
                                                               ▼
                                                          RingBuffer
                                                               ▼
                              [stage: match] 单线程撮合（改 OrderBook，产出 List<MatchResult>）
                                  ├─▶ [stage: emit]    after=match → MatchResultSink.accept(results)  ← 出：内存占位
                                  └─▶ [stage: metrics] after=match → 累加成交笔数 / 量
GET /orders/stats  ◀── 成交统计（读侧）
GET /book?symbol&levels ◀── 盘口深度（读侧，OrderBook.depth）
关停 → DisruptorLifecycle 排空 RingBuffer（starter 内建，白送）
```

- **进（数据怎么来）**：真 REST controller，但**薄**——只做 DTO → `tryPublish` → 202/429。不接真 MQ / 网关。一行注释标明"生产换成 MQ 消费者 / 网关"。
- **出（数据怎么走）**：`MatchResultSink` 内存实现，**占位**——不落库 / 不发 MQ / 不推 websocket。一行注释标明"生产换成落库 / 发 MQ / 推 websocket"。
- **撮合异步**：`POST` 不能同步返回成交结果（否则把异步管道退化成同步）。成交结果只从读侧端点看。

### 真实生产拓扑与 tutorial 的映射

生产里订单走 MQ，不是 HTTP 直连环：

```
下单网关/API ──▶ MQ(orders, Kafka/RocketMQ) ──▶ 撮合服务
                                                  │ MQ 消费者 pull 一条 → eventBus.tryPublish 进 Disruptor 环
                                                  ▼
                                         [Disruptor 单线程撮合] 改 OrderBook 出 List<MatchResult>
                                                  ▼ 结果发 MQ(match-results)
                                         清算 / 行情 / 风控
```

**MQ 与 Disruptor 是互补两层，不是二选一**：MQ 负责跨进程、持久、分区有序的**服务间传输**（毫秒级、抗重启）；Disruptor 负责撮合服务**进程内**把订单流汇聚到那一根撮合线程（无锁纳秒级交接 + 有界背压）。即"MQ 消费者把消息喂进环，环的单消费者做撮合"，Disruptor 不直接连 MQ。参照 raftkit：`raftkit-logsource-kafka/rocketmq` 做入口、`MqOrderSink` 做出口。

| tutorial 里做的 | 生产里对应 | 是否做实 |
|---|---|---|
| `POST /orders` → `tryPublish` 进环 | MQ 消费者 pull 订单 → `tryPublish` 进环 | 接缝（薄，形态相同） |
| **环 → 单线程 match → `List<MatchResult>`** | **完全一致** | **做实（tutorial 主体）** |
| `MatchResultSink`（内存收集） | 结果发 MQ(match-results) → 清算/行情 | 接缝（占位） |

中间那段（环→单线程撮合→结果）在 tutorial 与生产里逐字一样，正是要教的部分；两端换 HTTP-in / 内存-out 只是剥掉 MQ 层，不影响 Disruptor 用法。

**背压形态差异（诚实说明）**：HTTP 入口环满 → 返回 429 给调用方；MQ 入口环满 → 消费者暂停拉取 / 不提交 offset（靠"不再 pull"天然回压 MQ）。语义等价（满了就别灌），触发方式不同。

## 撮合核心的来源与裁剪

从 `raftkit-match-engine` 的 `.core` 包 **vendor 一个极简版**（~200 行）到 tutorial 内，**不依赖 raftkit artifact**——避免跨仓构建耦合与依赖爆炸（`.core` 之外还拖 collections/replay/logsource/kafka/rocketmq/testcontainers）。`.core` 本身近乎零外部依赖（唯一跨模块点是 `ArtPriceLevels` → raftkit-collections 的 ART 树，已有 `TreeMapPriceLevels` 纯 JDK 替代）。

**保留 DNA**：`OrderBook` + `handle(order)→List<MatchResult>` 纯函数入口 + 单线程契约 + `MatchResult` 三形态（Trade/Open/Done）+ `depth()` 盘口快照 + 价格 long 编码（×10^8 FLOOR）+ 桶内 `LinkedHashMap` 时间优先 + maker"遍历中收集、遍历后删除"。

**丢弃**：`Drive/BaseDrive/QuoteBudgetDrive/NotionalDrive` 驱动体系、`MatchSink`、`MatchHandler/AbstractMatchHandler/Spot/Futures` 继承体系、`PriceLevels/ArtPriceLevels` 接口层（ART → 内嵌 `TreeMap`）、`snapshot/restore/copy` 深拷贝、去重集 `executedOrderIds`、`userOrders` 索引、`reduceOrder`、cancel/amend/batch、`canFullyFill` peek、MARKET/IOC/FOK/POST_ONLY。**只留 LIMIT + base 驱动（remaining 量）**。

**关键塌缩**：raftkit 的 `runMatch/doMatch/handleLimit`（`AbstractMatchHandler.java:48-116`）合并为 `MatchEngine` 一个私有 `matchLimit` 方法——`fill = min(taker.remaining, maker.remaining)`，直接 `out.add(Trade(...))`，无 sink 回调、无 probe。

## 关键架构决策

1. **单消费者 = 全篇价值点**：`match` stage `parallelism=1`（`@DisruptorStage` 显式声明），注释点明"OrderBook 非线程安全，靠单写者无锁跑对"。这是整个 tutorial 的立论核心。
2. **ring 槽位 → 独立 Order 副本**：`OrderEvent` 是 ring 复用槽位（后续事件会覆盖），而挂单要长期留在 `OrderBook` 里，故 `match` stage 必须从 `OrderEvent` 拷出一个独立 `Order`。这本身是 Disruptor ring 复用语义的一个教学点。
3. **跨线程读走单写者安全发布，不加锁**：
   - 盘口深度：`MatchEngine` 每次 `handle` 末尾把该 symbol 的 depth 整体替换为不可变 Map，存 `volatile` 字段，web 线程无锁读。（比 raftkit 全量 `snapshot()` 深拷贝更轻，保留"单写者→安全发布"精髓。）
   - 统计：`tradeCount` 用 `AtomicLong`；`tradedVolume` 用 `volatile BigDecimal`，唯一写者是 `metrics` 单线程 stage（parallelism=1），读-改-写无竞争。
4. **DAG 形态**：`match`（唯一源头）→ fan-out `emit ‖ metrics`。既有"必须先撮合"的串行依赖，又有"可并行"的下游扇出，正好把注解式 DAG 讲透。
5. **错误处理对齐现有代码**：controller 内直接 `ResponseEntity` 返回状态码，不引入 `@ControllerAdvice`（仓库现无，外科式）。

## 全链路日志（可只看日志追完整条链）

SLF4J，`[matching/xxx]` 前缀（对齐 example 的 `[pipeline/stage]` 风格）：

| 接缝 | 级别 | 内容 |
|---|---|---|
| controller 受理 | INFO | `[matching/accept] 受理订单 {} symbol={} side={} price={} qty={}` |
| controller 背压拒绝 | WARN | `[matching/reject] 背压拒绝 symbol={} side={} remaining={}` |
| match 产出 | INFO | `[matching/match] 订单 {} 撮合产出 {} 条结果` |
| match 明细 | DEBUG | 逐条 Trade/Open/Done |
| emit | INFO | `[matching/emit] 下发 {} 条撮合结果` |
| metrics | DEBUG | `[matching/metrics] 累计成交 {} 笔 量 {}` |

`application.yml`：`logging.level.com.sstlfsj.disruptor.tutorial: INFO`。

## 包结构 `com.sstlfsj.disruptor.tutorial`

```
TutorialApplication.java                 @SpringBootApplication（已存在，不动）
match/    Side, Order, MatchResult, PriceBucket, OrderSide, OrderBook, MatchEngine   撮合核心（零 Spring / 零外部依赖，vendor 自 raftkit .core）
pipeline/ OrderEvent, MatchingPipeline   ring 槽位事件 + 注解式三 stage（match→emit‖metrics）
sink/     MatchResultSink, InMemoryMatchResultSink, MatchMetrics   出口接缝 + 内存实现 + 统计 holder
web/      OrderController, BookController   POST /orders、GET /orders/stats、GET /book
dto/      PlaceOrderRequest, AcceptedResponse, StatsResponse, BookResponse   请求/响应 record
config/   MatchConfig                    @Bean MatchEngine（保持 core 无 Spring 注解）
resources/application.yml                 disruptor.* + server.port + logging
```

## 依赖（pom 已就绪，无需改动）

`disruptor-spring-boot-starter` + `spring-boot-starter-web` + lombok(optional) + `spring-boot-starter-test`(test)。已在父 pom `modules` 注册。撮合核心零新增依赖；测试用 Awaitility（随 starter-test 打包）做异步断言。

## 现有约定基线（已核实真源）

- `@DisruptorStage(pipeline, name, after={}, parallelism=1)`：标注 Spring bean 方法，签名必须 `void m(E event)` 恰一个参数；同 pipeline 各 stage 事件类型一致；`after` 空=源头。源：`disruptor-spring-boot-autoconfigure/.../DisruptorStage.java`。范例：`example/.../order/OrderPipeline.java`。
- `EventBus.tryPublish(Class<E>, Consumer<E>)`→boolean（满返回 false）、`remainingCapacity(Class<?>)`。源：`disruptor-core/.../EventBus.java`。背压范例：`example/.../backpressure/BackpressureDemoRunner.java`。
- `DisruptorProperties`：`disruptor.buffer-size`(2的幂,默认1024)、`wait-strategy`(默认YIELDING)、`shutdown-timeout`(默认10s)。关停排空 starter 内建。
- 事件类型 = 可变 POJO `@Getter @Setter`。范例：`example/.../order/OrderEvent.java`。

## 不做（YAGNI / 范围边界）

- 不接真 MQ / DB / websocket / 网关（进出两端留占位）。
- 不做 MARKET / IOC / FOK / POST_ONLY / 撤单 / 改单 / futures / 多分片撮合 / 快照重放。
- 不依赖 raftkit artifact；不改 starter / autoconfigure / core / example 任何代码；不改 pom。
