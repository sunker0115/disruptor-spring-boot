# tutorial 撮合场景 Implementation Plan

> 设计见 `docs/superpowers/specs/2026-08-21-tutorial-matching-scenario-design.md`。取代 `2026-08-20-example-and-tutorial-modules.md` 中 Task 9–12 的 tutorial 下单实现（example Task 1–8 已实现，不受影响）。
>
> **For agentic workers:** 用 superpowers:subagent-driven-development 或 executing-plans 逐 Task 实现。核心撮合走 TDD（测试先行）。

**Goal:** 把 `disruptor-spring-boot-tutorial`（现为空壳）实现成一个撮合 web 小应用：`POST /orders` 用 `tryPublish` 发布订单后立刻返回 202（满则 429），后台单线程 Disruptor 管道跑撮合（`match → emit ‖ metrics`），`GET /orders/stats` / `GET /book` 观测；订单进 / 出两端留薄接缝但端到端能跑。

**Architecture:** 撮合核心从 raftkit `.core` vendor 极简 LIMIT 版（零 Spring / 零外部依赖），`OrderBook` 故意非线程安全、靠 `match` stage `parallelism=1` 单消费者无锁跑对——这是全篇价值点。注解式 `@DisruptorStage` 声明 DAG。跨线程读走单写者 volatile 安全发布。

**Tech Stack:** Java 17、Spring Boot 4.1.0、LMAX Disruptor 4.0、Lombok、JUnit 5、Awaitility、本仓 disruptor-spring-boot-starter。

**不改动：** starter / autoconfigure / core / example 任何代码；根 pom；tutorial pom（已就绪：starter + starter-web + lombok + starter-test）；`TutorialApplication.java`。

---

## 前置：本机 Maven 环境（每次跑 mvn 前 export）

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
cd /Users/sunke/dev/ai-project/disruptor-spring-boot
```

包根：`com.sstlfsj.disruptor.tutorial`。撮合 vendor 真源参考：`/Users/sunke/dev/ai-project/raftkit/raftkit-match-engine/src/main/java/com/sstlfsj/raftkit/match/core/`（`OrderBook.java`、`AbstractMatchHandler.java:48-116`、`PriceBucket.java`、`OrderSide.java`、`TreeMapPriceLevels.java`、`MatchResult.java`、`OrderConstants.java`）。

---

## Task 1：撮合核心（`match/`）+ 核心单测（TDD 先行）

**Files（`.../tutorial/match/`）:**
- Create: `Side.java` — `enum { BUY, SELL }` + `Side opposite()`（照搬 `OrderConstants.Side`）。
- Create: `Order.java` — `@Getter @Setter @Builder`；字段 `long orderId; String symbol; Side side; BigDecimal price; BigDecimal quantity; BigDecimal executedQty(=ZERO); long priceLong; long transactTime;`；`BigDecimal remaining(){ return quantity.subtract(executedQty); }`。
- Create: `MatchResult.java` — `sealed interface permits Trade, Open, Done`，`String symbol(); long sequence();`：
  - `Trade(String symbol, long sequence, long takerOrderId, long makerOrderId, Side takerSide, BigDecimal price, BigDecimal quantity, long tradeTime)`
  - `Open(String symbol, long sequence, long orderId, Side side, BigDecimal price, BigDecimal remaining)`
  - `Done(String symbol, long sequence, long orderId, Side side, BigDecimal remaining, boolean taker)`（LIMIT 只有 FILLED，去 DoneReason/userId）
- Create: `PriceBucket.java` — 照搬 raftkit（删泛型 `<T>` 收成 `Order`、删 `reduce`）：`long priceLong; BigDecimal price; LinkedHashMap<Long,Order> orders; BigDecimal quantity(=ZERO); add/remove/onFill/isEmpty/priceLong/price/quantity/orders`。
- Create: `OrderSide.java` — 内嵌 `TreeMap<Long,PriceBucket>`（替 `PriceLevels` 接口，语义照 `TreeMapPriceLevels`：`best`=buy `lastEntry`/sell `firstEntry`；`forEach`=buy `descendingMap()`/sell 正序，visitor 返回 false 即停）；字段 `boolean buy; int orderCount; BigDecimal quantity;`；方法 `append/remove/onFill/best/forEach/crosses/isEmpty/quantity`（删 `reduce/clearAll`）。`crosses(counterPriceLong)`：buy `counter<=best.priceLong` / sell `counter>=best.priceLong`。
- Create: `OrderBook.java` — 单 symbol（删 orders/userOrders/dedup/snapshot/restore/reduce/isCrossed）：`String symbol; OrderSide bids/asks; long sequence;`；`long toPriceLong(BigDecimal)`(×10^8 FLOOR `longValueExact`)；`OrderSide side(Side)`/`counter(Side)`；`long nextSequence(){ return ++sequence; }`；`record Level(BigDecimal price, BigDecimal quantity)`；`List<Level> depth(Side, int levels)`。
- Create: `MatchEngine.java` — 见下。
- Create（test）: `src/test/.../tutorial/match/MatchEngineTest.java`。

`MatchEngine`（替代整个 handler 继承体系，**单线程契约**）:
```java
public final class MatchEngine {
    private static final Logger log = LoggerFactory.getLogger(MatchEngine.class);
    private final Map<String, OrderBook> books = new HashMap<>();       // 仅 match 线程访问
    private volatile Map<String, BookDepth> depthView = Map.of();       // 跨线程只读快照
    public record BookDepth(List<OrderBook.Level> bids, List<OrderBook.Level> asks) {}

    /** 单线程契约：仅由 match stage（parallelism=1）调用。OrderBook 非线程安全，靠单写者无锁跑对。 */
    public List<MatchResult> handle(Order taker) {
        OrderBook book = books.computeIfAbsent(taker.getSymbol(), OrderBook::new);
        if (taker.getPrice() != null) taker.setPriceLong(book.toPriceLong(taker.getPrice()));
        List<MatchResult> out = new ArrayList<>();
        OrderSide counter = book.counter(taker.getSide());
        if (counter.crosses(taker.getPriceLong())) matchLimit(taker, counter, book, out);
        if (taker.remaining().signum() == 0) {
            out.add(new MatchResult.Done(book.symbol(), book.nextSequence(),
                    taker.getOrderId(), taker.getSide(), BigDecimal.ZERO, true));
        } else {
            book.side(taker.getSide()).append(taker);
            out.add(new MatchResult.Open(book.symbol(), book.nextSequence(),
                    taker.getOrderId(), taker.getSide(), taker.getPrice(), taker.remaining()));
        }
        refreshDepth(taker.getSymbol(), book);
        return out;
    }
    public BookDepth depth(String symbol) { return depthView.getOrDefault(symbol, EMPTY); }
    // matchLimit / refreshDepth 见下
}
```
`matchLimit`（对照 `AbstractMatchHandler.runMatch/doMatch`，base 驱动）：
```java
List<Order> filled = new ArrayList<>();
counter.forEach(bucket -> {
    boolean cross = taker.getSide()==Side.BUY ? taker.getPriceLong()>=bucket.priceLong()
                                              : taker.getPriceLong()<=bucket.priceLong();
    if (!cross) return false;                                   // 优先序下更差档更不交叉 → 停
    BigDecimal price = bucket.price();
    for (Order maker : new ArrayList<>(bucket.orders().values())) {   // 快照：档内摘 maker 安全
        if (taker.remaining().signum()==0) break;
        BigDecimal fill = taker.remaining().min(maker.remaining());
        taker.setExecutedQty(taker.getExecutedQty().add(fill));
        maker.setExecutedQty(maker.getExecutedQty().add(fill));
        bucket.onFill(maker, fill); counter.onFill(fill);
        out.add(new MatchResult.Trade(book.symbol(), book.nextSequence(),
                taker.getOrderId(), maker.getOrderId(), taker.getSide(), price, fill, taker.getTransactTime()));
        if (maker.remaining().signum()==0) {
            out.add(new MatchResult.Done(book.symbol(), book.nextSequence(),
                    maker.getOrderId(), maker.getSide(), BigDecimal.ZERO, false));
            filled.add(maker);                                   // 遍历后删（遍历期改 TreeMap 不安全）
        }
    }
    return taker.remaining().signum() > 0;                       // 还有量 → 继续下一档
});
for (Order m : filled) counter.remove(m);
```
`refreshDepth`：`depthView` 复制一份可变 map、`put(symbol, new BookDepth(book.depth(BUY,DEPTH_LEVELS), book.depth(SELL,DEPTH_LEVELS)))`、整体替换为 `Map.copyOf(...)` 存回 volatile。

- [ ] **Step 1：写 `MatchEngineTest`（先红）** —— 直接 `new MatchEngine()`，不启 Spring：
  - 交叉全成：SELL@100×10、BUY@100×10 → 第二单产出 1 Trade(price100,qty10) + Done(taker filled)；`depth` 两侧空。
  - 部分成交挂余：SELL@100×5、BUY@100×10 → BUY 吃掉 5 产 Trade + Open(remaining5)；`depth(BUY)` 含 (100,5)。
  - 不交叉挂单：BUY@90×5 → 仅 Open；`depth(BUY)` 含 (90,5)、asks 空。
  - 价格时间优先：两个 SELL@100 先后到 + 一个 SELL@101，BUY@101×15 → 先撮 100 档、同价先到先撮（断言 Trade 的 makerOrderId 顺序）。
- [ ] **Step 2：实现 `match/` 全部类，`MatchEngineTest` 转绿**
  - Run: `$MVN -pl disruptor-spring-boot-tutorial -am test -Dtest=MatchEngineTest -Dsurefire.failIfNoSpecifiedTests=false`

---

## Task 2：出口与统计（`sink/`）+ 核心 bean（`config/`）

**Files:**
- Create: `sink/MatchResultSink.java` — `void accept(List<MatchResult> results);`（薄接缝，一行注释"生产换落库/发MQ/推websocket"）。
- Create: `sink/InMemoryMatchResultSink.java` — `@Component`；有界收集（如 `ArrayDeque` + 上限，或仅计数）；`List<MatchResult> recent(int n)` 供调试；INFO 日志 `[matching/emit] 下发 {} 条`。
- Create: `sink/MatchMetrics.java` — `@Component`；`AtomicLong tradeCount` + `volatile BigDecimal tradedVolume`；`accumulate(List<MatchResult>)` 遍历 `Trade` 累加（单写者=metrics 线程）；`long tradeCount()` / `BigDecimal tradedVolume()`；DEBUG 日志。
- Create: `config/MatchConfig.java` — `@Configuration`，`@Bean MatchEngine matchEngine(){ return new MatchEngine(); }`（保持 core 无 Spring 注解）。
- [ ] Run: `$MVN -q -pl disruptor-spring-boot-tutorial -am compile`

---

## Task 3：事件与管道（`pipeline/`）

**Files:**
- Create: `pipeline/OrderEvent.java` — `@Getter @Setter`（ring 复用槽位）：`long orderId; String symbol; Side side; BigDecimal price; BigDecimal quantity; long transactTime; List<MatchResult> results;`。
- Create: `pipeline/MatchingPipeline.java` — `@Component @RequiredArgsConstructor`，注入 `MatchEngine engine, MatchResultSink sink, MatchMetrics metrics`：
```java
@DisruptorStage(pipeline = "matching", name = "match", parallelism = 1) // 单写者契约：OrderBook 非线程安全，必须=1
public void match(OrderEvent e) {
    Order o = Order.builder().orderId(e.getOrderId()).symbol(e.getSymbol()).side(e.getSide())
            .price(e.getPrice()).quantity(e.getQuantity()).transactTime(e.getTransactTime()).build();
    e.setResults(engine.handle(o));                                     // ring 槽位会被覆盖 → 必须拷出独立 Order
    log.info("[matching/match] 订单 {} 撮合产出 {} 条结果", e.getOrderId(), e.getResults().size());
}
@DisruptorStage(pipeline = "matching", name = "emit", after = "match")
public void emit(OrderEvent e) { sink.accept(e.getResults()); }
@DisruptorStage(pipeline = "matching", name = "metrics", after = "match")
public void metrics(OrderEvent e) { metrics.accumulate(e.getResults()); }
```
- [ ] Run: `$MVN -q -pl disruptor-spring-boot-tutorial -am compile`

---

## Task 4：web 层（`web/`）+ `application.yml`

**Files:**
- Create: `dto/PlaceOrderRequest.java` — record `(String symbol, Side side, BigDecimal price, BigDecimal quantity)`。
- Create: `dto/AcceptedResponse.java` — record `(long orderId)`。
- Create: `dto/StatsResponse.java` — record `(long tradeCount, BigDecimal tradedVolume)`。
- Create: `dto/BookResponse.java` — record `(String symbol, List<OrderBook.Level> bids, List<OrderBook.Level> asks)`。
- Create: `web/OrderController.java` — `@RestController @RequiredArgsConstructor`，注入 `EventBus`、`MatchMetrics`；`AtomicLong idGen`：
  - `POST /orders`：手工校验（symbol 非空、price/qty 正）失败→400；`long id=idGen.incrementAndGet()`；`boolean ok=eventBus.tryPublish(OrderEvent.class, e->{填槽位, e.setTransactTime(System.currentTimeMillis())})`；`ok`→`202 AcceptedResponse(id)` + INFO `[matching/accept]`；`!ok`→`429` + WARN `[matching/reject] ... remaining=eventBus.remainingCapacity(OrderEvent.class)`。**不同步返回成交。**
  - `GET /orders/stats`→`200 StatsResponse(metrics.tradeCount(), metrics.tradedVolume())`。
- Create: `web/BookController.java` — `@RestController @RequiredArgsConstructor`，注入 `MatchEngine`；`GET /book?symbol&levels(默认10)`→ `engine.depth(symbol)` 映射 `BookResponse`；无盘口返回空档位 `200`（不 404）。
- Create: `src/main/resources/application.yml`：
```yaml
server:
  port: 8080
disruptor:
  buffer-size: 1024
  wait-strategy: YIELDING
  shutdown-timeout: 10s
logging:
  level:
    com.sstlfsj.disruptor.tutorial: INFO
```
- [ ] Run: `$MVN -q -pl disruptor-spring-boot-tutorial -am compile`

---

## Task 5：端到端测试（`src/test/`）

**Files:**
- Create: `MatchingFlowTest.java` — `@SpringBootTest(webEnvironment = RANDOM_PORT)` + `TestRestTemplate`：
  - SELL@100×10 → 断言 202 + `orderId>0`；BUY@100×10 → 202。
  - `await().atMost(2s).untilAsserted(()-> GET /orders/stats → tradeCount==1 && tradedVolume==10)`（Awaitility，不 `Thread.sleep`）。
  - BUY@90×5（不交叉）→ await 后 `GET /book?symbol=...` 断言 bids 含 (90,5)、asks 空。
- Create: `BackpressureTest.java` — 确定性触发 429：
  - 测试 profile 小 `disruptor.buffer-size`（如 8）；用 `@TestConfiguration` 提供带闸门（`CountDownLatch`）的 `MatchingPipeline` 子类/替身，`match` 入口先 `await` 闸门使消费停滞（对照 `example/.../backpressure`）。
  - 循环 `POST /orders`（数量 > bufferSize，如 30），断言**至少一次** `429`。
  - 释放闸门，`await` 统计追平后结束（验证排空）。
- [ ] Run: `$MVN -pl disruptor-spring-boot-tutorial -am test`
- 纪律（CLAUDE.md §4）：异步副作用等落地再断言；sink 该有数据却空 = 红旗，翻日志查被吞异常。

---

## Task 6：全量构建 + 手工冒烟 + README 指引

- [ ] **Step 1：全量构建** —— `$MVN -q clean install`，五模块（core/autoconfigure/starter/example/tutorial）全过、测试全绿。
- [ ] **Step 2：一键演示脚本 `demo.sh`** —— Create: `disruptor-spring-boot-tutorial/demo.sh`（可执行）。带注释的 curl 剧本，一键复现整条业务流程与效果，并提示看哪几行日志：
  - 前置提示：先另开终端 `mvn -pl disruptor-spring-boot-tutorial spring-boot:run`；`BASE=${BASE:-localhost:8080}`。
  - 剧本：① 挂一个不交叉买单（BUY@90×5）→ `GET /book` 看它进盘口；② SELL@100×10 挂上、BUY@100×10 打进 → `GET /orders/stats` 看 tradeCount/volume 累加、`GET /book` 看盘口被吃掉；③ 循环快速灌单触发 429（提示：小 buffer 或高频更易看到背压）；④ 每步 `echo` 说明 + 提示去 run 窗口看 `[matching/accept] → [matching/match]（同一线程名）→ [matching/emit]` 主干。
  - 脚本用纯 `curl`+`echo`，不依赖 jq；顶部注释点明"所有 `[matching/match]` 都在同一线程 = 单线程无锁撮合，这就是 Disruptor 的价值"。
- [ ] **Step 3：手工冒烟** —— `bash disruptor-spring-boot-tutorial/demo.sh` 跑通上面剧本；确认日志主干可见、stats/book 结果正确、能观察到 429。
- [ ] **Step 4：README 补一段** —— 在 tutorial 章节加撮合场景说明 + "用 vs 不用 Disruptor"价值一句话 + `bash demo.sh` 指引（对齐现有 README 风格；仅在既有 tutorial 段落调整，外科式）。

---

## 验证总纲

1. `MatchEngineTest`：撮合正确性 / 价格时间优先 / depth。
2. `MatchingFlowTest`：202 + 异步成交 / 挂单（Awaitility）。
3. `BackpressureTest`：确定性 429 + 排空。
4. 全量 `clean install` 绿；手工 curl 冒烟 + 日志主干可见。
