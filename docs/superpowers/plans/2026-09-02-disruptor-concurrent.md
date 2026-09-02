# disruptor-concurrent 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `disruptor-concurrent`，提供受监督、有界或显式无界、严格单线程、支持高级调度与固定 Group 的标准 JDK Executor，并完成 Spring 集成和 Commons 能力审计。

**Architecture:** `DisruptorEventLoop` 与 `UnboundedEventLoop` 共用唯一的 `EventLoopKernel`、单一 worker 和 core `WorkerSupervisor`；两者只替换 LMAX bounded 或 segmented MPSC unbounded `TaskQueue`。Future、timer、module、shutdown、快照、首因、共享 `ShutdownDeadline` 和 Group 终止全部由同一内核实现，公共 API 为 `EventLoop`/`ScheduledExecutorService`。

**Tech Stack:** Java 21、LMAX Disruptor 4.0.0、JUnit 5、Spring Boot 4.1.0、Micrometer、JMH、Maven 3.9.9。

---

所有 Maven 命令先执行：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
```

### Task 1: 加入 Maven 模块

**Files:**

- Modify: `pom.xml`
- Create: `disruptor-concurrent/pom.xml`

- [ ] **Step 1: 添加 reactor 和受管依赖**

根 POM modules 与 dependencyManagement 同时加入：

```xml
<module>disruptor-concurrent</module>
<dependency>
    <groupId>com.sstlfsj</groupId>
    <artifactId>disruptor-concurrent</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 2: 创建模块 POM**

模块 compile 依赖 `disruptor-core` 与 `slf4j-api`，test 依赖 `junit-jupiter` 和 `assertj-core`，不依赖 Spring。

- [ ] **Step 3: 验证 reactor**

```bash
$MVN -pl disruptor-concurrent -am test
```

Expected: `BUILD SUCCESS` 且 reactor 包含 core、concurrent。

- [ ] **Step 4: 提交模块骨架**

```bash
git add pom.xml disruptor-concurrent/pom.xml
git commit -m "build: add disruptor concurrent module"
```

### Task 2: 定义 EventLoop 公共契约和有界队列

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoop.java`
- Create: `.../EventLoopSnapshot.java`
- Create: `.../EventLoopState.java`
- Create: `.../CapacityMode.java`
- Create: `.../TaskExceptionHandler.java`
- Create: `.../EventLoopBuilder.java`
- Create: `.../internal/TaskSlot.java`
- Create: `.../internal/TaskQueue.java`
- Create: `.../internal/BoundedTaskQueue.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/DisruptorEventLoopTest.java`

- [ ] **Step 1: 写公共契约编译测试**

```java
@Test
void exposesStandardScheduledExecutorContract() {
    EventLoop loop = EventLoopBuilder.builder("orders")
            .bufferSize(16)
            .maxBatchSize(4)
            .build();
    assertInstanceOf(ScheduledExecutorService.class, loop);
    assertEquals("orders", loop.name());
    assertEquals(EventLoopState.NEW, loop.snapshot().state());
}
```

- [ ] **Step 2: 确认红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=DisruptorEventLoopTest test
```

Expected: 公共类型不存在。

- [ ] **Step 3: 实现稳定公共接口**

```java
public interface EventLoop extends ScheduledExecutorService {
    String name();
    boolean inEventLoop();
    CompletionStage<Void> start();
    CompletionStage<EventLoopSnapshot> termination();
    EventLoopSnapshot snapshot();
    boolean tryExecute(Runnable command);
}
```

`EventLoopSnapshot` 包含状态、健康、容量模式、worker 登记是否封口、registered/started/alive、pending、remaining、scheduled、completed、failed、cancelled、首因和 worker 线程名。快照沿用 core 不变量：`RUNNING` 时单一 worker 必须已封口、启动且存活；`TERMINATED` 时 alive 为 0。

- [ ] **Step 4: 实现 BoundedTaskQueue 最小契约**

`TaskQueue.offer` 成功后返回单调 acceptedSequence，失败返回容量/状态原因；`poll(maxBatchSize)` 只能由单消费者调用；消费后清空 `TaskSlot` 引用。

- [ ] **Step 5: 运行测试并提交**

```bash
$MVN -pl disruptor-concurrent test
git add disruptor-concurrent
git commit -m "feat(concurrent): define event loop contracts"
```

### Task 3: 实现共享 EventLoopKernel、有界后端与 ExecutorService

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/DisruptorEventLoop.java`
- Create: `.../internal/EventLoopKernel.java`
- Create: `.../internal/EventLoopWorker.java`
- Create: `.../internal/SubmittedTask.java`
- Test: `.../DisruptorEventLoopTest.java`
- Test: `.../ExecutorContractTest.java`
- Test: `.../EventLoopBackendContractTest.java`

- [ ] **Step 1: 写启动、顺序、拒绝和终止失败测试**

```java
@Test
void executesAcceptedCommandsOnOneThreadInClaimOrder() throws Exception {
    EventLoop loop = EventLoopBuilder.builder("orders").bufferSize(64).build();
    loop.start().toCompletableFuture().get(1, SECONDS);
    List<Integer> seen = new CopyOnWriteArrayList<>();
    for (int i = 0; i < 32; i++) loop.execute(new IndexedCommand(i, seen));
    loop.shutdown();
    assertTrue(loop.awaitTermination(1, SECONDS));
    assertEquals(IntStream.range(0, 32).boxed().toList(), seen);
}
```

- [ ] **Step 2: 确认测试红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=DisruptorEventLoopTest,ExecutorContractTest test
```

Expected: `build()` 尚未返回可运行实现或 Executor 方法未实现。

- [ ] **Step 3: 接入 core 统一监督生命周期**

`EventLoopKernel` 唯一持有 core `WorkerSupervisor`。启动时登记单一 worker，进入 `STARTING` 后启动并封口；worker 完成已配置模块启动后调用 `markRunning()`。NEW 下意外启动不得执行循环；worker 在 `STARTING/RUNNING/QUIESCING` 提前退出必须 fail-stop。

kernel 实现非阻塞 `ShutdownBackend`：`beginQuiesce` 只 unpark，`isDrained` 只读取 worker 发布的 drain 事实，`stop(GRACEFUL/IMMEDIATE)` 只发布停止意图并 unpark。所有后端动作由 supervisor 控制线程串行调用；Future 终态化、TaskQueue 引用清理和模块 stop 必须在 worker `finally` 中完成，线程被 join 后才能完成 termination。

- [ ] **Step 4: 实现 EventPoller worker**

循环必须按以下顺序：执行到期 timer、poll 最多 `maxBatchSize` 条、模块 update、按下一 deadline park。每次成功发布调用 `LockSupport.unpark(workerThread)`；检查为空与 park 之间的发布依赖 unpark permit 防止丢唤醒。

- [ ] **Step 5: 实现 ExecutorService 与关闭终态**

`execute` 使用非阻塞 offer，拒绝时抛包含 `NOT_STARTED/FULL/SHUTTING_DOWN/FAILED` 的 `RejectedExecutionException`。`submit` 使用 FutureTask；`shutdown` 与 `shutdownNow` 都使用首次冻结的 `ShutdownDeadline` 非阻塞请求 supervisor。`shutdownNow` 返回空列表，pending 队列仍只由 worker 清理，避免调用线程破坏单消费者所有权；所有 accepted Future 在 termination 前终态化。

graceful 必须经历 `RUNNING → QUIESCING → STOPPING → TERMINATED`；立即停止、基础设施故障和 deadline 到期单调进入 `STOPPING`。deadline 只触发首因与 immediate 升级，worker 存活时 termination 不完成。graceful flag 要求 `reachedRunning + drainCommitted + gracefulStopApplied + 单一 worker 真实退出`。

- [ ] **Step 6: 补齐任务异常与关闭架构测试并通过**

裸任务的 `Throwable` 进入 `TaskExceptionHandler` 后继续循环；submit 的异常只进入 Future；只有任务边界之外的 processor/queue 异常触发 core supervisor。

增加参数化后端契约：bounded/unbounded 必须共用同一 kernel 快照；`QUIESCING/STOPPING` 均可观察；drain pending 时 immediate 不受阻塞；抗中断任务退出前 termination 不完成；Future、timer、module cleanup 在 termination 前完成。

```bash
$MVN -pl disruptor-concurrent -Dtest=DisruptorEventLoopTest,ExecutorContractTest test
```

Expected: `BUILD SUCCESS`。

- [ ] **Step 7: 提交共享内核和有界 EventLoop**

```bash
git add disruptor-concurrent
git commit -m "feat(concurrent): add supervised event loop kernel"
```

### Task 4: 实现取消令牌和确定性调度

**Files:**

- Create: `.../NanoClock.java`
- Create: `.../CancellationToken.java`
- Create: `.../CancellationSource.java`
- Create: `.../CancellationReason.java`
- Create: `.../TaskContext.java`
- Create: `.../ScheduledTaskSpec.java`
- Create: `.../TaskSnapshot.java`
- Create: `.../internal/ScheduledTask.java`
- Create: `.../internal/IndexedScheduledHeap.java`
- Create: `.../internal/CancellationMailbox.java`
- Test: `.../CancellationTest.java`
- Test: `.../EventLoopSchedulingTest.java`

- [ ] **Step 1: 写取消恰好一次测试**

```java
@Test
void cancellationPublishesOneReasonToEveryListener() {
    CancellationSource source = new CancellationSource();
    List<CancellationReason> seen = new ArrayList<>();
    source.token().onCancel(seen::add);
    assertTrue(source.cancel(CancellationReason.user("stop")));
    assertFalse(source.cancel(CancellationReason.user("again")));
    assertEquals(List.of(CancellationReason.user("stop")), seen);
}
```

- [ ] **Step 2: 写调度顺序失败测试**

用注入的 `ManualNanoClock` 验证一次、fixed-rate、fixed-delay、动态延迟、次数、截止时间，以及 `deadline asc -> priority desc -> acceptedSequence asc`。

- [ ] **Step 3: 确认红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=CancellationTest,EventLoopSchedulingTest test
```

Expected: 调度类型不存在。

- [ ] **Step 4: 实现 ScheduledTaskSpec**

同时给 `EventLoop` 增加高级调度入口：

```java
<V> ScheduledFuture<V> schedule(ScheduledTaskSpec<V> spec);
```

```java
ScheduledTaskSpec<Void> spec = ScheduledTaskSpec.run(command)
        .fixedRate(Duration.ZERO, Duration.ofMillis(10))
        .maxExecutions(3)
        .deadline(Duration.ofSeconds(1))
        .priority(10)
        .cancellationToken(source.token())
        .context(TaskContext.of("traceId", "abc"))
        .continueOnFailure(false)
        .build();
```

拒绝负周期、非正次数和溢出的 duration。Future.cancel 与 token cancel 汇合到一个原子终态。

- [ ] **Step 5: 实现索引堆和 cancellation mailbox**

堆的 offer/poll/remove 为 O(log n)；仅 loop 线程修改堆。外部取消每个任务最多写入一次 mailbox 并 unpark。

- [ ] **Step 6: 运行测试并提交**

```bash
$MVN -pl disruptor-concurrent -Dtest=CancellationTest,EventLoopSchedulingTest test
git add disruptor-concurrent
git commit -m "feat(concurrent): add deterministic task scheduling"
```

### Task 5: 实现模块生命周期

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopModule.java`
- Modify: `.../EventLoopBuilder.java`
- Modify: `.../internal/EventLoopWorker.java`
- Test: `.../EventLoopModuleTest.java`

- [ ] **Step 1: 写顺序与回滚失败测试**

```java
@Test
void startsInOrderAndStopsOnlyStartedModulesInReverseOrder() {
    List<String> calls = new CopyOnWriteArrayList<>();
    EventLoop loop = builderWithModules(
            module("a", calls), failingStartModule("b", calls), module("c", calls)).build();
    assertThrows(ExecutionException.class,
            () -> loop.start().toCompletableFuture().get(1, SECONDS));
    assertEquals(List.of("start-a", "start-b", "stop-a"), calls);
}
```

- [ ] **Step 2: 确认红灯并实现接口**

```java
public interface EventLoopModule {
    default void onStart(EventLoop loop) throws Exception {}
    default void onUpdate(EventLoop loop, long nowNanos) throws Exception {}
    default void onStop(EventLoop loop) throws Exception {}
}
```

- [ ] **Step 3: 实现 worker hook**

start 声明顺序、stop 逆序；每批任务后或 timer 唤醒后调用 update。模块异常在任务边界之外，触发 supervisor fail-stop；stop 异常作为 suppressed 保留。

- [ ] **Step 4: 运行测试并提交**

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopModuleTest test
git add disruptor-concurrent
git commit -m "feat(concurrent): add event loop modules"
```

### Task 6: 实现固定 EventLoopGroup

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/DisruptorEventLoopGroup.java`
- Create: `.../EventLoopGroupBuilder.java`
- Test: `.../EventLoopGroupTest.java`

- [ ] **Step 1: 写 chooser 和故障传播测试**

```java
@Test
void affinitySelectionIsStableForEveryIntKey() {
    DisruptorEventLoopGroup group = EventLoopGroupBuilder.builder("workers", 4).build();
    for (int key : List.of(Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE)) {
        assertSame(group.select(key), group.select(key));
    }
}
```

- [ ] **Step 2: 确认红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopGroupTest test
```

- [ ] **Step 3: 实现固定 children 和共享 deadline 聚合生命周期**

`next()` 使用无锁递增和 `floorMod`；`select(key)` 使用稳定 hash spread 后 `floorMod`。任一 child 基础设施故障锁存 Group 首因，以同一个 `ShutdownDeadline` 触发所有 child immediate stop，不重新映射 key。Group 只有在全部 child 真实 termination 后才能提交 terminated。

- [ ] **Step 4: 验证启动回滚、委派和终止**

补充 Group execute/submit/schedule、单 child 顺序、启动失败回滚、所有 child 收到相同绝对 deadline、同时发停止请求后聚合真实终止、不可变 iterator 测试。

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopGroupTest test
```

- [ ] **Step 5: 提交 Group**

```bash
git add disruptor-concurrent
git commit -m "feat(concurrent): add fixed event loop groups"
```

### Task 7: 实现显式无界分段 MPSC 后端

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/UnboundedEventLoop.java`
- Create: `.../internal/SegmentedMpscTaskQueue.java`
- Modify: `.../EventLoopBuilder.java`
- Test: `.../SegmentedMpscTaskQueueTest.java`
- Test: `.../EventLoopBackendContractTest.java`

- [ ] **Step 1: 写 segment 边界与并发失败测试**

```java
@Test
void consumesEveryClaimedSequenceExactlyOnceAcrossSegments() throws Exception {
    SegmentedMpscTaskQueue queue = new SegmentedMpscTaskQueue(8);
    publishConcurrently(queue, 4, 1_000);
    List<Long> sequences = drainSequences(queue, 4_000);
    assertEquals(LongStream.range(0, 4_000).boxed().toList(), sequences);
    assertEquals(1, queue.allocatedSegmentsAfterReclaim());
}
```

- [ ] **Step 2: 确认红灯并实现分段队列**

每个 segment 为 2 的幂固定槽数组；生产者以全局原子 sequence 定位 segment 和 offset，发布位使用 release/acquire；单消费者只按连续 sequence 前进。消费后清引用，越过整块后回收前序块。

- [ ] **Step 3: 仅替换共享内核的 TaskQueue 后端**

`UnboundedEventLoop` 不创建 LMAX RingBuffer，也不创建第二个 supervisor 或 lifecycle；它只向同一个 `EventLoopKernel` 注入 segmented MPSC `TaskQueue`。调度、取消、Future、模块、关闭、快照和 Group 行为与 bounded 后端使用完全相同的代码路径。

- [ ] **Step 4: 运行后端契约与压力测试**

```bash
$MVN -pl disruptor-concurrent -Dtest=SegmentedMpscTaskQueueTest,EventLoopBackendContractTest test
```

Expected: 两种 TaskQueue 通过同一队列契约，两种 EventLoop 通过同一生命周期/Future/timer/module/shutdown 契约；无丢失、重复和遗留引用。

- [ ] **Step 5: 提交无界后端**

```bash
git add disruptor-concurrent
git commit -m "feat(concurrent): add explicit unbounded event loops"
```

### Task 8: Spring 生命周期、健康和指标

**Files:**

- Modify: `disruptor-spring-boot-autoconfigure/pom.xml`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentLifecycle.java`
- Create: `.../DisruptorConcurrentAutoConfiguration.java`
- Create: `.../DisruptorConcurrentHealthContributor.java`
- Create: `.../DisruptorConcurrentMetrics.java`
- Modify: `.../META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Create: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentAutoConfigurationTest.java`

- [ ] **Step 1: 写条件装配失败测试**

```java
@Test
void managesOnlyExplicitEventLoopBeans() {
    contextRunner.withUserConfiguration(LoopBeans.class).run(context -> {
        assertThat(context).hasSingleBean(DisruptorConcurrentLifecycle.class);
        assertThat(context).doesNotHaveBean("globalEventLoop");
    });
}
```

- [ ] **Step 2: 确认红灯并添加 optional 模块依赖**

autoconfigure 对 concurrent 使用 `<optional>true</optional>`；starter 不直接依赖 concurrent。

- [ ] **Step 3: 实现生命周期、健康与 metrics binder**

生命周期按 bean 顺序启动；关闭时创建一次 `ShutdownDeadline`，向所有 bean 广播后聚合真实 termination，不能为每个 bean 重算 timeout。指标包括 healthy、worker registered/started/alive、pending、remaining（仅 bounded）、segments（仅 unbounded）、scheduled、completed、failed、cancelled。

- [ ] **Step 4: 运行自动配置测试**

```bash
$MVN -pl disruptor-spring-boot-autoconfigure -am test
```

Expected: 有 concurrent 时装配，无 concurrent classpath 时现有 starter 测试仍通过。

- [ ] **Step 5: 提交 Spring 集成**

```bash
git add disruptor-spring-boot-autoconfigure
git commit -m "feat(boot): manage disruptor event loops"
```

### Task 9: 示例、文档和 Commons 能力审计

**Files:**

- Modify: `README.md`
- Modify: `docs/disruptor-architecture-design.md`
- Create: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/concurrent/ConcurrentDemo.java`
- Modify: `disruptor-spring-boot-example/pom.xml`
- Modify: `disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/ExampleSmokeTest.java`

- [ ] **Step 1: 添加纯 Java 与 Spring 示例**

示例必须覆盖 bounded、显式 unbounded、schedule、CancellationToken、Group affinity、模块 hook 和关闭等待；不创建隐藏全局单例。

- [ ] **Step 2: 更新架构与能力矩阵**

逐项映射 Commons 的 executor、future、scheduler、cancel、context、agent/module、group、bounded/unbounded、raw event、global helper。标准 JDK 等价必须说明语义，不写“完全兼容”。

- [ ] **Step 3: 运行示例测试并提交**

```bash
$MVN -pl disruptor-spring-boot-example -am test
git add README.md docs/disruptor-architecture-design.md disruptor-spring-boot-example
git commit -m "docs: add disruptor concurrent guide"
```

### Task 10: JMH 和最终完成审计

**Files:**

- Modify: `disruptor-benchmarks/pom.xml`
- Create: `disruptor-benchmarks/src/main/java/com/sstlfsj/disruptor/benchmark/EventLoopBenchmark.java`
- Modify: `disruptor-benchmarks/src/main/java/com/sstlfsj/disruptor/benchmark/BenchmarkMain.java`

- [ ] **Step 1: 添加可运行基准**

同一 benchmark 参数下比较原生 LMAX、core managed try publish、bounded EventLoop、unbounded EventLoop 和 JDK 单线程 executor。Commons 能在本地 reactor 外构建时以独立 profile 加入，否则不增加生产依赖。

- [ ] **Step 2: 运行全仓验证**

```bash
$MVN clean verify
$MVN -pl disruptor-benchmarks -am package
java -jar disruptor-benchmarks/target/benchmarks.jar -wi 1 -i 2 -f 1
git diff --check
```

Expected: 全部测试通过，JMH 五组基准均产生结果；不设置机器敏感硬阈值。

- [ ] **Step 3: 执行静态审计**

```bash
rg -n 'System\.(out|err)|TODO|TBD|CallerRuns|DiscardPolicy' disruptor-core disruptor-concurrent disruptor-spring-boot-autoconfigure
rg -n 'void publishEvent|boolean tryPublishEvent' disruptor-core/src/main/java
git status --short
```

Expected: 无新增违规、旧受管发布签名已消失、工作区只包含本次计划内变更。

- [ ] **Step 4: 对照规格逐项核验并提交**

逐条核验两份 spec 的目标、不变量、Commons 能力矩阵和验证项，每项记录对应测试类或基准。存在未验证项时继续实现，不以窄测试代替完整完成。

```bash
git add disruptor-benchmarks README.md docs disruptor-core disruptor-concurrent disruptor-spring-boot-autoconfigure disruptor-spring-boot-example pom.xml
git commit -m "perf: benchmark disruptor concurrent executors"
```
