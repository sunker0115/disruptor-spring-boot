# 示例模块设计(disruptor-spring-boot-example)

日期:2026-08-20
状态:已确认,待实现

> tutorial 模块（真实场景教程）另见 `2026-08-21-tutorial-matching-scenario-design.md`（撮合场景）。本文档只覆盖 example 特性演示模块。

## 目标

新增 `disruptor-spring-boot-example` 模块 —— **特性演示**。每个核心特性一个自包含、可跑的 console demo,外加一个
无 Spring 的纯 Java main。把 README 的用法落成"能跑的",看"每个能力单独怎么用"。可独立运行。

## 硬约束

同一 Spring 上下文里**管道名与事件类型全局唯一**(跨声明式/编程式冲突则启动 fail-fast)。example 模块六个
demo 各用独立事件类型 + 管道名,不冲突。

## 前置改动(根 pom)

- `<dependencyManagement>` 补一条 `disruptor-spring-boot-starter`(`<version>${revision}</version>`),与已有
  core/autoconfigure 一致,使新模块引 starter 时不必写版本。
- `<modules>` 在三模块之后追加 `disruptor-spring-boot-example`(tutorial 模块的接线见其专属设计文档)。

模块 pom 共性:`<parent>` 版本用 `${revision}`;packaging `jar`;纯示例不部署;
`spring-boot-maven-plugin`(`<version>${spring-boot.version}</version>`,BOM 不管插件版本)支持 `mvn spring-boot:run`;
Lombok(`org.projectlombok:lombok`,`<optional>true</optional>`,版本由父 BOM 管)写 POJO getter/setter。

---

## disruptor-spring-boot-example(特性演示,console)

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

## README 指引

主 `README.md` 加「示例与教程」一节,示例部分:
- 特性演示:`mvn -pl disruptor-spring-boot-example spring-boot:run`(六个 demo 顺序打印);纯 Java 路径运行 `PureJavaExample`。

## 不做(YAGNI)

- 不演示"覆盖默认 bean(EventBus/Pipelines 等)"—— 运行价值低,注释点到即可。
- 模块不部署;无鉴权/持久化/前端(聚焦"每个特性单独怎么用"的直观价值)。
