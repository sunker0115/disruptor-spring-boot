# 受监督 Disruptor 管道实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `disruptor-core` 重构为具备消费者监督、有界可中断发布、统一状态快照和异步终止信号的单管道运行时，并接入 Spring 健康与指标。

**Architecture:** `WorkerSupervisor` 以单一状态锁和命名虚拟控制线程统一 worker 入场、生命周期、首因、共享绝对截止时间和真实终止；内部 `ShutdownBackend` 只提供非阻塞 `beginQuiesce/isDrained/stop` 阶段动作。`ManagedPipeline` 只把 LMAX 发布握手、gating 排空和 halt 映射到该协议；`DisruptorRuntime` 广播同一个 `ShutdownDeadline` 并聚合 child 的真实终止。

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
                    .registrationSealed(true)
                    .expectedConsumers(2)
                    .createdConsumers(2)
                    .startedConsumers(2)
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
                    .registrationSealed(true)
                    .expectedConsumers(2)
                    .createdConsumers(2)
                    .startedConsumers(2)
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

`PipelineSnapshot.builder()` 只接收状态事实字段，并在 `build()` 内按生命周期、登记是否封口、消费者计数和 failure 派生 health；canonical constructor 校验 `alive <= started <= created <= expected`。`RUNNING` 要求登记已封口且全部 expected consumer 已启动并存活；`TERMINATED` 要求 alive 为 0。`gracefulTermination` 还要求不再接收发布、无 failure，并由 supervisor 已提交的 `reachedRunning/drainCommitted/gracefulStopApplied` 历史事实产生，不能仅按终态猜测。

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

### Task 3: 重构统一受监督生命周期

**Files:**

- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownDeadline.java`
- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownBackend.java`
- Rewrite: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSupervisor.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSnapshot.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/WorkerSupervisorTest.java`

- [ ] **Step 1: 用非阻塞阶段协议替换旧停止契约并写测试**

删除“单一回调自行排空和停止”的旧契约及其测试。用可记录调用线程、并发深度和动作序列的 fake `ShutdownBackend` 验证：

- `beginQuiesce()` 至多一次；
- `isDrained()` 只在 `QUIESCING` 被重复探测；
- `stop(GRACEFUL)`、`stop(IMMEDIATE)` 各至多一次且严格串行；
- 后端方法不在 supervisor 状态锁或调用方线程内执行；
- 后端异常锁存为首因并升级 immediate。

- [ ] **Step 2: 确认红灯**

```bash
$MVN -pl disruptor-core -Dtest=WorkerSupervisorTest test
```

Expected: 旧 `WorkerSupervisor` 仍依赖单一阻塞停止回调，且缺少 `ShutdownDeadline/ShutdownBackend`、动态登记封口和 started worker 事实。

- [ ] **Step 3: 实现绝对截止时间和动态 worker 入场**

`ShutdownDeadline.after(Duration)` 使用饱和加法生成不可变 `deadlineNanos`。supervisor 第一次接受关闭请求后冻结 deadline，重复请求或模式升级不能延长。

```java
public final class WorkerSupervisor {
    public Runnable supervise(Runnable worker);
    public void register(Thread thread);
    public void markStarting();
    public void sealWorkers();
    public CompletionStage<Void> workersStarted();
    public void markRunning();
    public void fail(Throwable failure);
    public void requestShutdown(ShutdownMode mode);
    public void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline);
    public CompletionStage<WorkerSnapshot> termination();
    public WorkerSnapshot snapshot();
}
```

supervisor builder 保留默认 `shutdownTimeout`，只用于独立关闭和自主 worker 故障；Runtime/Group 可以在首次请求时传入共享 deadline。登记允许发生在未封口的 `NEW/STARTING`；`sealWorkers()` 后登记数成为本生命周期 expected 数。wrapper 只允许在 `STARTING/RUNNING` 入场，并记录 `REGISTERED → ALIVE → EXITED`；`markRunning()` 要求登记已封口、至少一个 worker、`started == alive == registered`。NEW 下启动、重复入场和封口后登记都必须失败且不执行用户任务。

- [ ] **Step 4: 实现非阻塞分阶段控制状态机**

使用单一状态锁线性化生命周期、首因、首次 deadline 和单调停机模式。`requestShutdown` 只更新状态并启动/唤醒命名虚拟控制线程；后端动作由该线程串行执行，绝不在状态锁内执行。

控制循环必须满足：

- RUNNING graceful：`QUIESCING → drain committed → STOPPING → stop(GRACEFUL)`；
- immediate、失败或超时：请求线程只提交 `STOPPING` 并唤醒控制线程，控制线程执行 `stop(IMMEDIATE) → interrupt workers`；stop 抛错也必须在 `finally` 中继续中断；
- graceful stop 后 worker 超时仍可串行升级一次 immediate stop；
- drain probe 返回 true 后在状态锁内重新确认仍为 `GRACEFUL + QUIESCING`，并发 immediate、故障或 deadline 必须胜出；
- drain probe 和 worker 等待均使用原始绝对 deadline；
- deadline 到期只记录首因和升级，由控制线程先 stop 再中断；worker 存活时不完成 termination；
- worker 完成 `finally` 且控制线程 `join` 确认真实死亡后才提交 `TERMINATED`。

- [ ] **Step 5: 补齐架构级状态机测试**

至少覆盖：

- drain 一直 pending 时，外部 `IMMEDIATE` 无需释放阻塞回调即可进入 `STOPPING` 并调用 immediate stop；
- drain probe 返回 true 与并发 immediate/故障竞态时不能提交 graceful stop；
- graceful deadline 到期沿用首次 deadline，不延长；
- `QUIESCING` 和 `STOPPING` 均可观察，不允许 `QUIESCING → TERMINATED`；
- worker 在 `STARTING/RUNNING/QUIESCING` 正常或异常提前退出都 fail-stop；
- worker 从自身请求关闭不自等待，原始 Throwable 仍交给 UncaughtExceptionHandler；
- 抗中断 worker 存活时 termination 未完成，真实退出后才完成；
- 首因不被停止异常、超时或后续 worker 异常覆盖；
- graceful flag 只在 `reachedRunning + drainCommitted + gracefulStopApplied + 全部 started worker 真实退出` 时成立；
- `WorkerSnapshot` 校验 `alive <= started <= registered`、sealed expected 和 graceful path 不变量。

```bash
$MVN -pl disruptor-core -Dtest=WorkerSupervisorTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 6: 提交统一生命周期**

```bash
git add disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownDeadline.java disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownBackend.java disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSupervisor.java disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSnapshot.java disruptor-core/src/test/java/com/sstlfsj/disruptor/core/WorkerSupervisorTest.java
git commit -m "refactor(core): define supervised shutdown lifecycle"
```

### Task 4: 把 ManagedPipeline 提升为独立生命周期单元

**Files:**

- Create: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/DisruptorPipeline.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ManagedPipeline.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/DisruptorRuntime.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/DisruptorPipelineTest.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/DisruptorRuntimeTest.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/ExceptionHandlingTest.java`

- [ ] **Step 1: 写 LMAX 阶段映射和独立管道失败测试**

覆盖在途 publisher 未归零前不能捕获 drain cursor、只捕获一次固定 cursor、所有叶子 gating 到达目标后才提交 drain，以及 `disruptor.halt()` 只在进入 `STOPPING` 后调用。一个消费者失败只能 fail-stop 所属管道，其他命名管道保持健康。

- [ ] **Step 2: 运行新测试确认红灯**

```bash
$MVN -pl disruptor-core -Dtest=DisruptorPipelineTest test
```

Expected: `ManagedPipeline.shutdown/haltNow` 仍把排空、halt 和 join 混在阻塞方法中，尚未实现 `ShutdownBackend`。

- [ ] **Step 3: 实现单管道接口和 LMAX ShutdownBackend**

```java
public interface DisruptorPipeline<E> {
    PipelineHandle<E> handle();
    PipelineSnapshot snapshot();
    CompletionStage<Void> start();
    void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline);
    CompletionStage<PipelineSnapshot> termination();
}
```

`ManagedPipeline` 实现接口并用 supervisor 包装 ThreadFactory。启动按 `markStarting → Disruptor.start → sealWorkers → workersStarted → markRunning` 执行。删除阻塞的 `shutdown/haltNow/StopResult`；后端只实现：

启动完成、关闭和发布准入必须由同一个 lifecycle lock 线性化，发布 gate 单调执行 `NEW → OPEN → CLOSED`。关闭一旦提交就不能被迟到的 `completeStart` 重新打开；启动前关闭必须显式 `markStarting + sealWorkers` 封口零 worker、异常完成同一个启动 stage，并允许直接走到真实 termination。

- `beginQuiesce` 唤醒受管发布等待者；
- `isDrained` 等在途 publisher 归零后捕获一次 cursor，再检查叶子 gating；
- `stop` 调用非阻塞 `disruptor.halt()`，不 join、不等待 backlog。

`PipelineSnapshot` 增加 started/sealed 事实，并收紧 `RUNNING`、`TERMINATED` 和 graceful path 校验。

- [ ] **Step 4: 重写 Runtime 为共享 deadline 聚合器**

Runtime 一次关闭只创建一个私有 `RuntimeShutdownSession`，集中持有首次冻结的 `ShutdownDeadline`、最高 mode、唯一 outcome、一次协调事实、在途广播计数，以及按 identity 去重的启动、请求和 child termination 失败。启动回滚与后续关闭 API 必须复用该会话；每次 API 返回 stage 前同步向全部管道广播同一个请求，再由唯一虚拟协调线程聚合 termination。每次离锁广播必须先在锁内登记 token，并在 `finally` 回锁后原子归集失败和注销；启动仍待回滚、child termination 尚未全部完成或仍有广播在途时，不能正常定稿 outcome。deadline 升级先完成自己的 immediate 广播再参与定稿；deadline 失败只等待全部已接受广播归还 token，不等待启动回滚或 child 真实终止，二者继续在后台收敛。outcome 一旦承诺定稿便关闭新的广播入口。单个 child 请求抛错不能阻断其余广播，原始异常必须进入 outcome。并发 graceful/immediate 请求只能单调升级，且每个调用返回前自身模式已完成广播。到 deadline 时，对未终止 child 使用原 deadline 升级 immediate 并返回/抛出聚合结果；后台继续等待真实终止。全部 child stage 完成后同时聚合 termination 异常和 snapshot 首因，Runtime 只有在全部 child 的 termination 完成后才能提交自身终止状态。

启动失败回滚同样向全部已尝试管道广播同一个 immediate deadline；保留按名称和事件类型查找，不恢复另一套管道状态判断。

- [ ] **Step 5: 运行生命周期回归测试**

新增多管道共享同一绝对 deadline、Runtime 不随 child 数量线性延长、超时后 child 仍处于 `STOPPING`、最后一个 child 真实退出后 Runtime 才终止，以及任意原生 topology 动态登记/封口的测试。

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

使用 Boot 4.1 `org.springframework.boot.health.contributor` API。增加 runtime/pipeline healthy、consumer total/started/alive；failure 只放健康详情，不放 meter 标签。

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
