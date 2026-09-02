# 受监督 Disruptor 管道实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `disruptor-core` 重构为具备消费者监督、有界可中断发布、统一状态快照和异步终止信号的单管道运行时，并接入 Spring 健康与指标。

**Architecture:** `WorkerSupervisor` 负责线程状态、首个故障和非阻塞停止控制；`ManagedPipeline` 组合它完成 LMAX 发布、排空和 halt；`DisruptorRuntime` 只聚合多条管道。所有运行状态由 `PipelineSnapshot` 对外呈现。

**Tech Stack:** Java 21、LMAX Disruptor 4.0.0、JUnit 5、Spring Boot 4.1.0、Micrometer、Maven 3.9.9。

---

所有 Maven 命令先执行：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
```

### Task 1: 建立测试基线

**Files:**

- Inspect: `pom.xml`
- Inspect: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/*.java`

- [ ] **Step 1: 运行当前 core 测试**

```bash
$MVN -pl disruptor-core test
```

Expected: `BUILD SUCCESS`；若失败，记录现有失败并先定位，不修改测试绕过。

- [ ] **Step 2: 运行当前全仓测试**

```bash
$MVN test
```

Expected: `BUILD SUCCESS`，作为破坏式 API 重构前的基线。

- [ ] **Step 3: 记录基线提交状态**

```bash
git status --short
git log -1 --oneline
```

Expected: 只包含实施计划和设计自审修改，没有未知业务代码改动。

### Task 2: 定义状态和快照模型

**Files:**

- Modify: `disruptor-core/pom.xml`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PipelineLifecycle.java`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PipelineHealth.java`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PipelineSnapshot.java`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PublicationResult.java`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownMode.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/PipelineSnapshotTest.java`

- [ ] **Step 1: 写状态派生失败测试**

```java
@Test
void runningPipelineIsHealthyOnlyWhenEveryConsumerIsAlive() {
    assertEquals(PipelineHealth.HEALTHY,
            PipelineSnapshot.builder()
                    .name("orders")
                    .lifecycle(PipelineLifecycle.RUNNING)
                    .acceptingPublications(true)
                    .expectedConsumers(2)
                    .createdConsumers(2)
                    .aliveConsumers(2)
                    .bufferSize(16)
                    .backlog(0)
                    .gracefulTermination(false)
                    .build()
                    .health());
    assertEquals(PipelineHealth.UNHEALTHY,
            PipelineSnapshot.builder()
                    .name("orders")
                    .lifecycle(PipelineLifecycle.RUNNING)
                    .acceptingPublications(false)
                    .expectedConsumers(2)
                    .createdConsumers(2)
                    .aliveConsumers(1)
                    .bufferSize(16)
                    .backlog(3)
                    .gracefulTermination(false)
                    .build()
                    .health());
}
```

- [ ] **Step 2: 确认测试因类型不存在而失败**

```bash
$MVN -pl disruptor-core -Dtest=PipelineSnapshotTest test
```

Expected: `COMPILATION ERROR`，缺少快照相关类型。

- [ ] **Step 3: 实现精确枚举与不可变快照**

```java
public enum PipelineLifecycle { NEW, STARTING, RUNNING, QUIESCING, STOPPING, TERMINATED }
public enum PipelineHealth { STARTING, HEALTHY, UNHEALTHY, OUT_OF_SERVICE, TERMINATED }
public enum PublicationResult { PUBLISHED, CAPACITY_EXHAUSTED, TIMED_OUT, NOT_RUNNING, PIPELINE_FAILED }
public enum ShutdownMode { GRACEFUL, IMMEDIATE }
```

`PipelineSnapshot.builder()` 只接收状态事实字段，并在 `build()` 内按生命周期、消费者计数和 failure 派生 health；canonical constructor 拒绝负数、存活数大于已创建数及已创建数大于预期数。

- [ ] **Step 4: 运行测试确认通过**

```bash
$MVN -pl disruptor-core -Dtest=PipelineSnapshotTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 5: 提交状态模型**

```bash
git add disruptor-core/pom.xml disruptor-core/src/main/java/com/sstlfsj/disruptor/core disruptor-core/src/test/java/com/sstlfsj/disruptor/core/PipelineSnapshotTest.java
git commit -m "feat(core): add pipeline state snapshots"
```

### Task 3: 实现 WorkerSupervisor

**Files:**

- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSupervisor.java`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSnapshot.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/WorkerSupervisorTest.java`

- [ ] **Step 1: 写首因和自停止失败测试**

```java
@Test
void failureKeepsOriginalCauseAndNeverJoinsCurrentWorker() throws Exception {
    RuntimeException boom = new RuntimeException("boom");
    CountDownLatch stopRequested = new CountDownLatch(1);
    WorkerSupervisor supervisor = WorkerSupervisor.builder("orders")
            .expectedWorkers(1)
            .shutdownTimeout(Duration.ofSeconds(1))
            .stopAction((mode, deadline) -> stopRequested.countDown())
            .build();

    Thread worker = Thread.ofPlatform().unstarted(supervisor.supervise(() -> { throw boom; }));
    supervisor.register(worker);
    supervisor.markRunning();
    worker.start();

    assertTrue(stopRequested.await(1, TimeUnit.SECONDS));
    assertSame(boom, supervisor.termination().toCompletableFuture()
            .get(1, TimeUnit.SECONDS).failure());
}
```

- [ ] **Step 2: 确认红灯**

```bash
$MVN -pl disruptor-core -Dtest=WorkerSupervisorTest test
```

Expected: 缺少 `WorkerSupervisor`。

- [ ] **Step 3: 实现最小监督器**

```java
public final class WorkerSupervisor {
    public Runnable supervise(Runnable worker);
    public void register(Thread thread);
    public void markStarting();
    public void markRunning();
    public void requestShutdown(ShutdownMode mode);
    public CompletionStage<WorkerSnapshot> termination();
    public WorkerSnapshot snapshot();
}
```

实现使用单一状态锁线性化生命周期、首因和单调停机模式；停止动作只在命名 JDK 21 虚拟控制线程串行执行，每个实际模式至多一次；worker 包装器在记录后原样重抛 Throwable。deadline 到期只锁存超时首因、升级 `IMMEDIATE` 并中断 worker，不得提前完成 `termination()`；控制线程在 deadline 后通过线程终止通知等待实际退出，不做忙轮询，也不延长 deadline。

- [ ] **Step 4: 补齐竞态测试并运行**

补充重复停止、正常提前返回、关闭与失败同时发生、UncaughtExceptionHandler 收到同一异常、超时升级动作序列，以及抗中断 worker 全部退出后才完成 termination。

```bash
$MVN -pl disruptor-core -Dtest=WorkerSupervisorTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 5: 提交监督器**

```bash
git add disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSupervisor.java disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSnapshot.java disruptor-core/src/test/java/com/sstlfsj/disruptor/core/WorkerSupervisorTest.java
git commit -m "feat(core): supervise consumer workers"
```

### Task 4: 把 ManagedPipeline 提升为独立生命周期单元

**Files:**

- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/DisruptorPipeline.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ManagedPipeline.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/DisruptorRuntime.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/DisruptorPipelineTest.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/DisruptorRuntimeTest.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/ExceptionHandlingTest.java`

- [ ] **Step 1: 写单管道终止和消费者 fail-stop 测试**

```java
@Test
void oneConsumerFailureFailsOnlyItsPipeline() throws Exception {
    RuntimeException boom = new RuntimeException("boom");
    DisruptorRuntime runtime = runtimeWithFailingAndHealthyPipelines(boom);
    runtime.start();
    PipelineHandle<TestEvent> failed = runtime.require("failed", TestEvent.class);

    assertTrue(failed.tryPublishEvent((event, sequence) -> {}));
    awaitCondition(() -> failed.snapshot().failure() == boom, Duration.ofSeconds(1));

    assertEquals(PipelineHealth.UNHEALTHY, failed.snapshot().health());
    assertEquals(PipelineHealth.HEALTHY,
            runtime.require("healthy", TestEvent.class).snapshot().health());
}
```

- [ ] **Step 2: 运行新测试确认红灯**

```bash
$MVN -pl disruptor-core -Dtest=DisruptorPipelineTest test
```

Expected: 缺少 `DisruptorPipeline`、`snapshot()` 或发布结果签名。

- [ ] **Step 3: 实现单管道接口和 ManagedPipeline 组合**

```java
public interface DisruptorPipeline<E> {
    PipelineHandle<E> handle();
    PipelineSnapshot snapshot();
    void start();
    void requestShutdown(ShutdownMode mode);
    CompletionStage<PipelineSnapshot> termination();
}
```

`ManagedPipeline` 实现该接口并使用 `WorkerSupervisor` 包装 ThreadFactory。故障停止不等待 gating sequence；优雅停止仍按“关闭准入、等在途发布、捕获 cursor、等叶子 sequence、halt、join”执行。

- [ ] **Step 4: 重写 Runtime 为聚合器**

Runtime `shutdown()` 先向全部管道请求 `GRACEFUL`，再按同一绝对截止时间等待；启动失败向已尝试管道请求 `IMMEDIATE`。保留按名称和事件类型查找。

- [ ] **Step 5: 运行生命周期回归测试**

```bash
$MVN -pl disruptor-core -Dtest=DisruptorPipelineTest,DisruptorRuntimeTest,ExceptionHandlingTest,NativeCapabilitiesTest test
```

Expected: `BUILD SUCCESS`，包括 native processor、rewind 和异常策略。

- [ ] **Step 6: 提交生命周期重构**

```bash
git add disruptor-core/src/main/java/com/sstlfsj/disruptor/core disruptor-core/src/test/java/com/sstlfsj/disruptor/core
git commit -m "refactor(core): make pipelines independently supervised"
```

### Task 5: 用有界可中断发布替换无限阻塞 API

**Files:**

- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PipelineHandle.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ManagedPipeline.java`
- Create: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/ManagedPublicationTest.java`
- Modify: all repository call sites returned by `rg -n 'publishEvent|tryPublishEvent|\.publish\(|\.tryPublish\('`

- [ ] **Step 1: 写满环、超时、中断、关闭和故障测试**

```java
@Test
void boundedPublisherStopsWaitingWhenPipelineFails() throws Exception {
    PipelineHandle<TestEvent> handle = fullRingWithStoppedConsumer();
    AtomicReference<PublicationResult> result = new AtomicReference<>();
    Thread publisher = Thread.ofPlatform().start(() -> {
        try {
            result.set(handle.publishEvent((event, sequence) -> {}, Duration.ofSeconds(10)));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    });
    failConsumer();
    publisher.join(1_000);
    assertEquals(PublicationResult.PIPELINE_FAILED, result.get());
}
```

- [ ] **Step 2: 确认旧 API 无法满足测试**

```bash
$MVN -pl disruptor-core -Dtest=ManagedPublicationTest test
```

Expected: 缺少返回 `PublicationResult` 和 `Duration` 重载。

- [ ] **Step 3: 重写 PipelineHandle 契约**

```java
PublicationResult tryPublishEvent(EventTranslator<E> translator);
PublicationResult publishEvent(EventTranslator<E> translator, Duration timeout)
        throws InterruptedException;
```

为 1、2、3 参数 translator 提供同样两组方法；便捷 `publish/tryPublish` 只委托这些方法。删除旧 `void publishEvent(...)` 和 boolean `tryPublishEvent(...)`，不加兼容层。

- [ ] **Step 4: 实现 tryPublish 重试循环**

每次重试先检查中断、failure、accepting 和 deadline；用 `LockSupport.parkNanos` 做短暂退避，关闭/故障时 `unpark` 已登记等待者。只调用 LMAX `tryPublishEvent`，不调用 `next/tryNext/publish(sequence)`。

- [ ] **Step 5: 迁移全仓调用点并运行测试**

```bash
$MVN -pl disruptor-core,disruptor-spring-boot-autoconfigure,disruptor-spring-boot-example,disruptor-spring-boot-tutorial -am test
```

Expected: `BUILD SUCCESS`；业务调用必须显式判断 `PublicationResult`。

- [ ] **Step 6: 确认旧签名消失并提交**

```bash
rg -n 'void publishEvent|boolean tryPublishEvent' disruptor-core/src/main/java
git add pom.xml disruptor-core disruptor-spring-boot-autoconfigure disruptor-spring-boot-example disruptor-spring-boot-tutorial
git commit -m "feat(core): add bounded interruptible publication"
```

Expected: `rg` 无结果。

### Task 6: 接入 Spring 健康和监督指标

**Files:**

- Modify: `disruptor-spring-boot-autoconfigure/pom.xml`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorHealthContributor.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorHealthAutoConfiguration.java`
- Modify: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorMetrics.java`
- Modify: `disruptor-spring-boot-autoconfigure/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Create: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/autoconfigure/DisruptorHealthContributorTest.java`
- Modify: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/autoconfigure/DisruptorMetricsTest.java`

- [ ] **Step 1: 写健康映射失败测试**

```java
@Test
void reportsDownOnlyForLatchedInfrastructureFailure() {
    Health health = contributorFor(failedSnapshot()).health();
    assertEquals(Status.DOWN, health.getStatus());
    assertEquals("orders", health.getDetails().get("pipeline"));
}
```

- [ ] **Step 2: 确认红灯**

```bash
$MVN -pl disruptor-spring-boot-autoconfigure -Dtest=DisruptorHealthContributorTest test
```

Expected: 健康贡献器不存在。

- [ ] **Step 3: 实现条件化健康装配与 Gauge**

使用 Boot 4.1 `org.springframework.boot.health.contributor` API。增加 runtime/pipeline healthy、consumer total/alive；failure 只放健康详情，不放 meter 标签。

- [ ] **Step 4: 运行自动配置测试**

```bash
$MVN -pl disruptor-spring-boot-autoconfigure -am test
```

Expected: 有/无 health、Micrometer classpath 均能启动。

- [ ] **Step 5: 提交 Spring 可观测性**

```bash
git add disruptor-spring-boot-autoconfigure
git commit -m "feat(boot): expose disruptor health and consumer metrics"
```

### Task 7: 更新核心文档并完成阶段验证

**Files:**

- Modify: `README.md`
- Modify: `docs/disruptor-architecture-design.md`
- Modify: affected examples and comments

- [ ] **Step 1: 删除过时语义**

删除“Runtime 不表示健康”“默认 HALT 后只靠人工监控”“无限阻塞发布”等旧段落，完整重写为 snapshot、fail-stop 和 publication result 语义。

- [ ] **Step 2: 添加有界发布示例**

```java
PublicationResult result = orders.publishEvent(
        TRANSLATOR, orderId, amount, Duration.ofMillis(20));
if (result != PublicationResult.PUBLISHED) {
    throw new RejectedExecutionException("orders publication rejected: " + result);
}
```

- [ ] **Step 3: 运行 core 阶段全仓验证**

```bash
$MVN clean verify
git diff --check
rg -n 'System\.(out|err)|TODO|TBD' disruptor-core disruptor-spring-boot-autoconfigure README.md docs/disruptor-architecture-design.md
```

Expected: Maven 和 diff 检查通过；搜索无新违规内容。

- [ ] **Step 4: 提交文档**

```bash
git add README.md docs/disruptor-architecture-design.md disruptor-spring-boot-example disruptor-spring-boot-tutorial
git commit -m "docs: document supervised pipeline semantics"
```
