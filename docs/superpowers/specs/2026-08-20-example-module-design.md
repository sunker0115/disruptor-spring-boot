# 示例与教程模块设计(disruptor-spring-boot-example / -tutorial)

日期:2026-08-20
状态:已确认,待实现

## 目标

新增**两个独立** Maven 模块,把 README 的用法落成"能跑的":

1. `disruptor-spring-boot-example` —— **特性演示**。每个核心特性一个自包含、可跑的 console demo,外加一个
   无 Spring 的纯 Java main。看"每个能力单独怎么用"。
2. `disruptor-spring-boot-tutorial` —— **真实场景教程**。一个带真实 HTTP 接口的 Spring Boot web 小应用,
   演示"下单主流程 publish 完立刻返回、副作用后台异步多阶段处理"的实际用法与价值。看"实际应用里怎么落地、有什么用"。

两模块互不依赖、各自可独立运行。

## 硬约束(两模块内各自成立)

同一 Spring 上下文里**管道名与事件类型全局唯一**(跨声明式/编程式冲突则启动 fail-fast)。example 模块六个
demo 各用独立事件类型 + 管道名;tutorial 模块只有一条 `order` 管道,不冲突。

## 前置改动(根 pom)

- `<dependencyManagement>` 补一条 `disruptor-spring-boot-starter`(`<version>${revision}</version>`),与已有
  core/autoconfigure 一致,使两个新模块引 starter 时不必写版本。
- `<modules>` 在三模块之后追加 `disruptor-spring-boot-example`、`disruptor-spring-boot-tutorial`。

两模块 pom 共性:`<parent>` 版本用 `${revision}`;packaging `jar`;纯示例不部署;
`spring-boot-maven-plugin`(`<version>${spring-boot.version}</version>`,BOM 不管插件版本)支持 `mvn spring-boot:run`;
Lombok(`org.projectlombok:lombok`,`<optional>true</optional>`,版本由父 BOM 管)写 POJO getter/setter。

---

## 模块一:disruptor-spring-boot-example(特性演示,console)

### 依赖
- `com.sstlfsj:disruptor-spring-boot-starter`(不写版本,父 depMgmt 提供)。
- `org.springframework.boot:spring-boot-starter`(带 spring-context + logback,看得到 INFO 装配日志)。
- `org.projectlombok:lombok`(optional)。
- 测试:`org.springframework.boot:spring-boot-starter-test`(test)。

### 包结构 `com.sstlfsj.disruptor.example`
```
ExampleApplication.java            @SpringBootApplication main(web-application-type=none)
order/     OrderEvent, OrderPipeline, OrderDemoRunner              demo1 声明式菱形 DAG
pay/       PayEvent, PayService, PayPipelineConfig, PayDemoRunner   demo2 编程式 builder
ingest/    IngestEvent, IngestPipeline, IngestDemoRunner           demo3 并行+ShardKeyed
reuse/     ReuseEvent, ReusePipeline, ReuseDemoRunner              demo4 Resettable
backpressure/ BpEvent, BpPipeline, BackpressureDemoRunner          demo5 tryPublish 三形态
nospring/  PureJavaExample                                         demo6 独立 main(仅 core API)
DemoResults.java                   共享结果持有器(供冒烟测试断言各 demo 完成)
resources/application.yml
```

每个 `*DemoRunner` = `@Component implements CommandLineRunner`,`@Order(1..5)` 保证顺序;内部:打印分段标题 →
`eventBus.publish(...)` → `CountDownLatch.await(超时)` 等阶段处理完 → 打印结果 → `DemoResults` 标记完成。
所有控制台输出用 `log.info`(SLF4J),不用 System.out。

### 各 demo 细节
- **demo1 order**:`OrderEvent{String orderId; long amount; String auditTrail}`;`OrderPipeline @Component`
  `@DisruptorStage` validate→persist(after=validate)→audit(after=validate)→notify(after={persist,audit});
  persist/audit 往 auditTrail 追加,notify 打印最终 auditTrail(证明下游可见上游改动 + 菱形汇聚)。runner latch=4。
- **demo2 pay**:`PayEvent{String payId; String trace}`;`PayService @Component`(validate/persist/audit/notify 方法);
  `PayPipelineConfig @Configuration` `@Bean EventPipeline<PayEvent> payPipeline(PayService svc)` 用
  `EventPipeline.builder("pay",PayEvent.class).stage("validate",svc::validate).stage("persist",svc::persist).after("validate").stage("audit",svc::audit).after("validate").stage("notify",svc::notify).after("persist","audit").build()`。runner latch=4。
- **demo3 ingest**:`IngestEvent implements ShardKeyed{String key; int seq; shardKey()=key}`;
  `IngestPipeline @Component` `@DisruptorStage(pipeline="ingest",name="process",parallelism=4)` 打印线程名+key+seq;
  runner 对 2~3 个 key 交错发布若干 seq,latch=事件总数,打印"同 key 由同一线程按 seq 递增处理"。
- **demo4 reuse**:`ReuseEvent implements Resettable{String orderId; String couponCode; reset()置null}`;
  `ReusePipeline @Component` `@DisruptorStage(pipeline="reuse",name="collect")` 打印 orderId+couponCode;
  runner 先发带 couponCode="SAVE10" 的 A,再发一批只设 orderId 的订单(数量 > buffer-size 确保槽位复用),
  latch=事件总数,打印"后续 couponCode 均为 null(无 SAVE10 残留),reset 生效"。
- **demo5 backpressure**:`BpEvent{int n}`;`BpPipeline @Component` `@DisruptorStage(pipeline="backpressure",name="slow")`
  内 `Thread.sleep(短)` 模拟慢消费;runner 突发发布远超 buffer(16)的事件填满 ring buffer,对每次
  `eventBus.tryPublish(...)` 返回 false 的三形态各演示并打印:①丢弃+计数 ②`log.info` 模拟降级落库 ③抛
  `IllegalStateException("系统繁忙")` 就地 catch 打印"快速失败回推上游";末尾打印 dropped 计数。等已入队事件处理完再结束。
- **demo6 nospring**(独立 `PureJavaExample.main`,仅 core API):`new DisruptorConfig(16,BLOCKING,Duration.ofSeconds(5))`
  → `EventPipeline.builder("plain",E.class).stage(...).build()` → `new PipelineBuilder(config).build(def)` →
  `Pipelines p=new Pipelines(); p.register(pipeline)` → `EventBus bus=new DefaultEventBus(p)` →
  `pipeline.disruptor().start()` → `bus.publish(...)` → `CountDownLatch.await` → `pipeline.disruptor().shutdown(timeout)` → 打印。

### application.yml(example)
```yaml
disruptor:
  buffer-size: 16          # 2 的幂;故意小以便 backpressure demo 触发满槽
  wait-strategy: YIELDING
  shutdown-timeout: 10s
logging:
  level:
    com.sstlfsj.disruptor: DEBUG   # 看得到装配 INFO 与背压 DEBUG
spring:
  main:
    web-application-type: none     # 非 Web,跑完 demo 即退出
```
注释:另可声明 `DisruptorConfig` `@Bean` 编程覆盖(声明后 yml 的 `disruptor.*` 被忽略);默认注释掉,保留 yml 生效。

### 冒烟测试
`ExampleSmokeTest @SpringBootTest`:启动上下文触发所有 DemoRunner,断言 `DemoResults` 里 demo1..5 均标记完成。
慢阶段 sleep 保持毫秒级,整体秒级完成。demo6 是独立 main,不纳入此测试。

---

## 模块二:disruptor-spring-boot-tutorial(真实场景,web)

**场景**:下单主流程与副作用解耦。HTTP `POST /orders` publish 一个 `OrderPlacedEvent` 后**立刻返回 202**,
后台按 DAG 异步跑:校验 → 落库 → (发确认邮件 ‖ 扣库存 ‖ 埋点)。突发流量下 `tryPublish` 满时返回 429 背压。
`GET /orders/stats` 看后台处理进度,直观体现"请求快速返回、副作用后台异步完成"。

### 依赖
- `com.sstlfsj:disruptor-spring-boot-starter`(不写版本)。
- `org.springframework.boot:spring-boot-starter-web`(真实 HTTP 接口 + 内嵌 tomcat + logback)。
- `org.projectlombok:lombok`(optional)。
- 测试:`org.springframework.boot:spring-boot-starter-test`(test)。

### 包结构 `com.sstlfsj.disruptor.tutorial`
```
TutorialApplication.java           @SpringBootApplication main(web)
OrderPlacedEvent.java              事件:@Getter @Setter, implements Resettable
OrderRecord.java                   不可变 record(落库快照,避免持有被复用的事件对象引用)
InMemoryOrderRepository.java       @Component,ConcurrentHashMap<String,OrderRecord> 模拟落库
OrderStats.java                    @Component,若干 AtomicInteger 计数(persisted/emailsSent/metricsRecorded/stockRemaining/rejected)
OrderProcessingPipeline.java       @Component,注解式五阶段 DAG
web/OrderController.java           @RestController POST /orders、GET /orders/stats
web/PlaceOrderRequest.java         record(userId, amount, couponCode)
resources/application.yml
```

### 事件与阶段
- `OrderPlacedEvent implements Resettable`:`String orderId; String userId; long amount; String couponCode`(可选);
  `reset()` 全部置 null/0(演示真实可选字段去残留)。
- `OrderProcessingPipeline @Component @RequiredArgsConstructor`(注入 repo、stats),注解式 DAG:
  - `validate`(源头):`amount<=0` 抛 `IllegalArgumentException`(顺带演示异常隔离:被记录不终止线程),否则 log。
  - `persist`(after=validate):`repo.save(new OrderRecord(orderId,userId,amount,couponCode))`(**复制字段**入库,不存事件对象);`stats.persisted++`。
  - `sendConfirmation`(after=persist):`Thread.sleep(短)` 模拟发信;`stats.emailsSent++`;log。
  - `deductInventory`(after=persist):`stats.stockRemaining--`。
  - `recordMetrics`(after=persist):`stats.metricsRecorded++`。
  DAG = validate → persist → (sendConfirmation ‖ deductInventory ‖ recordMetrics),三个副作用并行 fan-out。

### HTTP 接口(`OrderController @RestController @RequiredArgsConstructor`,注入 EventBus、OrderStats)
- `POST /orders`(body `PlaceOrderRequest{String userId; long amount; String couponCode}`):
  controller 生成 `orderId = UUID.randomUUID().toString()`,`boolean ok = eventBus.tryPublish(OrderPlacedEvent.class, e -> {填充 orderId/userId/amount/couponCode})`;
  - `ok==false`:`stats.rejected++`,返回 `429` + `{"error":"系统繁忙，请稍后重试"}`(README 背压形态 3)。
  - `ok==true`:返回 `202` + `{"orderId":..., "status":"accepted"}`(主流程 publish 完立刻返回)。
- `GET /orders/stats`:返回 persisted/emailsSent/metricsRecorded/stockRemaining/rejected 的当前值。
- `PlaceOrderRequest`:`record PlaceOrderRequest(String userId, long amount, String couponCode) {}`(Jackson 反序列化 record)。

### application.yml(tutorial)
```yaml
disruptor:
  buffer-size: 16          # 小 buffer,便于压测触发 429 背压
  wait-strategy: YIELDING
  shutdown-timeout: 10s
logging:
  level:
    com.sstlfsj.disruptor: INFO
server:
  port: 8080
```

### 端到端验证测试
`OrderFlowTest @SpringBootTest(webEnvironment=RANDOM_PORT)`,用 `TestRestTemplate`:
- POST /orders `{userId:"u1", amount:100}` → 断言 `202` 且响应含非空 orderId。
- 轮询 GET /orders/stats 直到 `persisted>=1`(超时 3s) → 断言 `emailsSent>=1`、`stockRemaining==99`、`metricsRecorded>=1`。
阶段 sleep 保持毫秒级,测试秒级完成。

---

## README 指引

主 `README.md` 末尾加「示例与教程」一节,分别指向两模块:
- 特性演示:`mvn -pl disruptor-spring-boot-example spring-boot:run`(六个 demo 顺序打印);纯 Java 路径运行 `PureJavaExample`。
- 真实场景:`mvn -pl disruptor-spring-boot-tutorial spring-boot:run` 起 web,附一条 curl 例:
  `curl -XPOST localhost:8080/orders -H 'Content-Type: application/json' -d '{"userId":"u1","amount":100}'`,
  再 `curl localhost:8080/orders/stats` 看后台异步处理结果。

## 不做(YAGNI)

- 不演示"覆盖默认 bean(EventBus/Pipelines 等)"—— 运行价值低,注释点到即可。
- 两模块均不部署;无鉴权/持久化/前端(教程聚焦"主流程解耦 + 背压"的直观价值)。
