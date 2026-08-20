# disruptor-spring-boot-example 示例模块设计

日期:2026-08-20
状态:已确认,待实现

## 目标

新增一个 Maven 模块 `disruptor-spring-boot-example`,把 README 里的核心用法各写成**一个能跑的**演示。
一条 `mvn spring-boot:run` 看 Spring 全套特性;单独运行 `PureJavaExample.main` 看无 Spring 的纯 Java 路径。

## 硬约束

同一 Spring 上下文里**管道名与事件类型全局唯一**(跨声明式/编程式冲突则启动 fail-fast)。因此每个 demo
使用各自独立的事件类型 + 管道名,六个 demo 可在同一上下文共存。

## 模块与依赖

- 位置:父 `disruptor-spring-boot` 的新 `<module>`(加入根 pom `<modules>`,置于三模块之后)。
- 坐标:`com.sstlfsj:disruptor-spring-boot-example`;`<parent>` 版本用 `${revision}`(同其它三模块)。
- packaging:`jar`。
- 前置改动:根 pom 的 `<dependencyManagement>` 补一条 `disruptor-spring-boot-starter`(`<version>${revision}</version>`),
  与已有的 core/autoconfigure 一致,使 example 引用 starter 时不必写版本。
- 依赖:
  - `com.sstlfsj:disruptor-spring-boot-starter`(内部模块,版本由父 dependencyManagement 提供,不写版本)。
  - `org.springframework.boot:spring-boot-starter`(带 spring-context + logback,才能看到 INFO 装配日志;版本由父 BOM 管)。
  - `org.projectlombok:lombok`(`<optional>true</optional>`;POJO 的 getter/setter 用 `@Getter @Setter`,版本由父 BOM 管)。
  - 测试:`org.springframework.boot:spring-boot-starter-test`(scope test)。
- 构建插件:`org.springframework.boot:spring-boot-maven-plugin`,`<version>${spring-boot.version}</version>`
  (spring-boot-dependencies BOM 只管依赖版本、不管插件版本,故插件版本显式引用 `${spring-boot.version}`);支持 `mvn spring-boot:run`。
- 此模块为纯示例,不部署(本仓本就不 deploy,无需额外配置)。

## 包结构 `com.sstlfsj.disruptor.example`

```
example/
  ExampleApplication.java            @SpringBootApplication main
  order/     OrderEvent, OrderPipeline, OrderDemoRunner              (demo1 声明式菱形 DAG)
  pay/       PayEvent, PayPipelineConfig, PayDemoRunner              (demo2 编程式 builder)
  ingest/    IngestEvent, IngestPipeline, IngestDemoRunner           (demo3 并行+ShardKeyed)
  reuse/     ReuseEvent, ReusePipeline, ReuseDemoRunner              (demo4 Resettable)
  backpressure/ BpEvent, BpPipeline, BackpressureDemoRunner          (demo5 tryPublish 三形态)
  nospring/  PureJavaExample                                         (demo6 独立 main,仅用 core API)
  DemoResults.java                   共享结果持有器(供冒烟测试断言各 demo 完成)
resources/
  application.yml
```

每个 `*DemoRunner` 是 `@Component implements CommandLineRunner`,用 `@Order(n)` 保证顺序(1..5),
内部:打印分段标题 → `eventBus.publish(...)` 发布 → `CountDownLatch.await(超时)` 等阶段处理完 → 打印结果 →
`DemoResults` 标记完成。所有面向控制台的说明用 `log.info`(SLF4J),不用 System.out。

## 各 demo 细节

### demo1 order —— 声明式注解 + 菱形 DAG
- `OrderEvent`:可变 POJO,无参构造,字段 `String orderId; long amount; String auditTrail`(getter/setter,Lombok `@Getter @Setter`)。
- `OrderPipeline`(`@Component`):`@DisruptorStage(pipeline="order", name="validate")` validate → `persist`(after=validate) → `audit`(after=validate) → `notify`(after={persist,audit})。各阶段 `log.info` 打印自己在跑;persist/audit 往 `auditTrail` 追加字符串,notify 打印最终 `auditTrail` 证明"下游可见上游改动 + 菱形汇聚在两分支后"。
- `OrderDemoRunner`(`@Order(1)`):publish 一个订单,latch 计数=4(四阶段各 countDown),await 后打印完成。

### demo2 pay —— 编程式 EventPipeline builder
- `PayEvent`:可变 POJO,无参构造,字段 `String payId; String trace`。
- `PayPipelineConfig`(`@Configuration`):`@Bean EventPipeline<PayEvent> payPipeline(PayService)` 用
  `EventPipeline.builder("pay", PayEvent.class).stage("validate", svc::validate).stage("persist", svc::persist).after("validate").stage("audit", svc::audit).after("validate").stage("notify", svc::notify).after("persist","audit").build()`。
  `PayService` 是本包一个 `@Component`,方法引用作 handler(演示无反射内联)。
- `PayDemoRunner`(`@Order(2)`):同 demo1 方式发布 + await + 打印。

### demo3 ingest —— 阶段并行 + ShardKeyed 保序
- `IngestEvent implements ShardKeyed`:字段 `String key; int seq`;`shardKey()` 返回 `key`。
- `IngestPipeline`(`@Component`):`@DisruptorStage(pipeline="ingest", name="process", parallelism=4)`
  process 打印 `Thread.currentThread().getName()` + key + seq。
- `IngestDemoRunner`(`@Order(3)`):对 2~3 个 key 各发布若干 seq(交错发布),latch=事件总数,await 后
  打印"同一 key 的事件都由同一线程、按 seq 递增顺序处理"(把每个 key 观察到的线程名/seq 序列收集打印)。

### demo4 reuse —— Resettable 去残留
- `ReuseEvent implements Resettable`:字段 `String orderId; String couponCode`;`reset()` 置两者为 null。
- `ReusePipeline`(`@Component`):`@DisruptorStage(pipeline="reuse", name="collect")` 打印 orderId + couponCode。
- `ReuseDemoRunner`(`@Order(4)`):先 publish 带 `couponCode="SAVE10"` 的订单 A,再 publish 一批**只设 orderId**
  的订单(B、C...,数量 > buffer-size 以确保槽位复用),await 后打印"后续订单 couponCode 均为 null(未见 SAVE10 残留),
  reset 生效"。latch=事件总数。

### demo5 backpressure —— tryPublish 三种降级形态
- `BpEvent`:字段 `int n`。
- `BpPipeline`(`@Component`):`@DisruptorStage(pipeline="backpressure", name="slow")` 内 `Thread.sleep(短)`
  模拟慢消费。
- `BackpressureDemoRunner`(`@Order(5)`):突发发布远超 buffer(16)的事件,使 ring buffer 填满;对每次
  `eventBus.tryPublish(BpEvent.class, ...)` 返回 `false` 的情况,分别演示三形态并打印:
  1. 可丢弃:`droppedCounter++` 计数;
  2. 不能丢:`log.info("降级落库补偿: ...")`(用日志模拟 fallback 落库);
  3. 关键链路:命中一次 false 时 `throw new IllegalStateException("系统繁忙")` 并就地 catch 打印"快速失败,回推上游限流"。
  最后打印 dropped 计数。此 demo 不强求所有事件处理完(重点是触发并展示 false 分支);为不影响关闭排空,
  等待已入队事件处理完再结束。

### demo6 nospring —— 纯 Java 手工装配(独立 main,仅 core)
- `PureJavaExample`:`public static void main`:
  1. `DisruptorConfig config = new DisruptorConfig(16, WaitStrategyType.BLOCKING, Duration.ofSeconds(5));`
  2. `EventPipeline<E> def = EventPipeline.builder("plain", E.class).stage(...).build();`(内部静态事件类 + lambda handler)
  3. `DisruptorPipeline<E> pipeline = new PipelineBuilder(config).build(def);`
  4. `Pipelines pipelines = new Pipelines(); pipelines.register(pipeline);`
  5. `EventBus bus = new DefaultEventBus(pipelines);`
  6. `pipeline.disruptor().start();` → `bus.publish(...)` → `CountDownLatch.await` → `pipeline.disruptor().shutdown(timeout)` 排空 → 打印。
  - 只用 `disruptor-core` 的公开 API,证明脱离 Spring 可独立工作。运行方式:直接运行该 main。

## application.yml

```yaml
disruptor:
  buffer-size: 16          # 2 的幂;故意设小以便 backpressure demo 触发满槽
  wait-strategy: YIELDING  # BLOCKING / YIELDING(默认) / BUSY_SPIN / SLEEPING
  shutdown-timeout: 10s
logging:
  level:
    com.sstlfsj.disruptor: DEBUG   # 看得到装配 INFO 与背压 DEBUG
spring:
  main:
    web-application-type: none     # 非 Web,跑完 demo 即退出
```

注释:另可声明 `DisruptorConfig` `@Bean` 以编程方式覆盖(声明后 yml 的 `disruptor.*` 被忽略);默认注释掉,保留 yml 生效。

## 防腐化冒烟测试

`ExampleSmokeTest`(`@SpringBootTest`):启动上下文触发所有 DemoRunner,断言 `DemoResults` 里 demo1..5 均标记完成
(context 加载成功 + 各 demo 无异常跑通)。sleep/慢阶段耗时保持很短(毫秒级),整体秒级完成。纯 Java demo6 不在此测试内
(它是独立 main;如需可另写一个直接调用其逻辑的小测试,非必需)。

## README 指引

主 `README.md` 末尾加一小节「示例」,一句话指向 `disruptor-spring-boot-example`:`mvn -pl disruptor-spring-boot-example spring-boot:run`
看 Spring 全套,运行 `PureJavaExample` 看纯 Java 路径。

## 不做(YAGNI)

- 不演示"覆盖默认 bean(EventBus/Pipelines 等)"—— 运行价值低,注释点到即可。
- 不部署此模块。
- 不做 Web/HTTP 触发层 —— CommandLineRunner 顺序演示即够。
