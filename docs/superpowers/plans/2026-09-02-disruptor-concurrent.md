# disruptor-concurrent 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `disruptor-concurrent`，以一套受监督内核提供有界和显式无界 EventLoop、完整 JDK 21 Executor/Scheduler 关闭语义、固定 Group、高级任务能力以及 Boot 4.1 运维集成。

**Architecture:** core 先把管道专属生命周期名称重构成通用 `SupervisedLifecycle`，并增加真正无界的 `ShutdownDeadline`。concurrent 只保留一个 `EventLoopKernel`；`TaskAdmissionGate` 和 `AcceptedTaskRegistry` 统一任务准入、容量和 shutdownNow 所有权，两种 `TaskQueue` 仅替换入口存储。EventLoopGroup 独占 child 生命周期，Spring 只管理根对象，并以同一个绝对 deadline 聚合真实 termination。

**Tech Stack:** Java 21、LMAX Disruptor 4.0.0、SLF4J、JUnit 5、AssertJ、Spring Boot 4.1.0、Micrometer、JMH、Maven 3.9.9。

---

所有 Maven 命令使用项目固定环境：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
```

每个 Task 都先确认红灯、再实现、再运行列出的模块测试并提交。不能用兼容别名、临时实现、禁用测试或 mock 掉并发边界让中间任务变绿。

### Task 1：core 通用监督命名与无界 deadline

**Files:**

- Rename: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PipelineLifecycle.java` → `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/SupervisedLifecycle.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSupervisor.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/WorkerSnapshot.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/PipelineSnapshot.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownBackend.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ManagedPipeline.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/DisruptorRuntime.java`
- Modify: `disruptor-core/src/main/java/com/sstlfsj/disruptor/core/ShutdownDeadline.java`
- Modify: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/DisruptorPipelineTest.java`
- Modify: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/DisruptorRuntimeTest.java`
- Modify: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/PipelineSnapshotTest.java`
- Test: `disruptor-core/src/test/java/com/sstlfsj/disruptor/core/WorkerSupervisorTest.java`

- [ ] **Step 1：先写无界 deadline 和新类型测试**

在 `WorkerSupervisorTest` 增加以下事实断言：

```java
@Test
void unboundedDeadlineNeverExpiresOrDependsOnNanoTime() {
    ShutdownDeadline deadline = ShutdownDeadline.unbounded();
    assertFalse(deadline.isBounded());
    assertFalse(deadline.isExpired());
    assertEquals(Long.MAX_VALUE, deadline.remainingNanos());
    assertSame(deadline, ShutdownDeadline.unbounded());
}

@Test
void workerSnapshotUsesGenericSupervisedLifecycle() {
    WorkerSupervisor supervisor = supervisor(TEST_TIMEOUT, new RecordingBackend());
    assertEquals(SupervisedLifecycle.NEW, supervisor.snapshot().lifecycle());
}
```

- [ ] **Step 2：运行红灯**

```bash
$MVN -pl disruptor-core -Dtest=WorkerSupervisorTest test
```

Expected: `SupervisedLifecycle`、`unbounded()` 或 `isBounded()` 尚不存在导致编译失败。

- [ ] **Step 3：直接完成通用重命名**

新枚举完整内容为：

```java
public enum SupervisedLifecycle {
    NEW, STARTING, RUNNING, QUIESCING, STOPPING, TERMINATED
}
```

用 `SupervisedLifecycle` 替换 core 全部生产和测试引用，并删除 `PipelineLifecycle`；不创建兼容类型。完成后执行：

```bash
rg -n 'PipelineLifecycle' disruptor-core
```

Expected: 无输出。

- [ ] **Step 4：实现显式 bounded/unbounded 表示**

`ShutdownDeadline` 使用 `bounded` 字段区分语义，`unbounded()` 返回静态单例；`after(Duration)` 仍校验非负并饱和换算。无界分支不得通过绝对 `Long.MAX_VALUE` 再做 `deadline-now` 判断。

- [ ] **Step 5：验证 core 并提交**

```bash
$MVN -pl disruptor-core test
git diff --check
git add disruptor-core
git commit -m "refactor(core): generalize supervised lifecycle"
```

Expected: core 全部测试通过；现有 pipeline/runtime 生命周期、失败和关闭测试没有退化。

### Task 2：模块与完整公共契约

**Files:**

- Modify: `pom.xml`
- Create: `disruptor-concurrent/pom.xml`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/SupervisedScheduledExecutor.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoop.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopGroup.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopFactory.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopModule.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopSnapshot.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupSnapshot.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopScheduledFuture.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/ScheduledTaskSnapshot.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/ScheduledTaskSpec.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/ScheduleMode.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/CapacityMode.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/TaskOutcome.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/TaskContext.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/ContextCallable.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/DynamicDelay.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/CancellationToken.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/CancellationReason.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/CancellationRegistration.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/TaskExceptionHandler.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/CancellationListenerExceptionHandler.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/BlockingOperationException.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/PublicContractTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/ScheduledTaskSpecTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/TaskContextTest.java`

- [ ] **Step 1：加入 reactor 与直接依赖**

根 POM 的 `<modules>` 和 `<dependencyManagement>` 加入 `disruptor-concurrent`。模块 POM compile 依赖必须同时包含：

```xml
<dependency><groupId>com.lmax</groupId><artifactId>disruptor</artifactId></dependency>
<dependency><groupId>com.sstlfsj</groupId><artifactId>disruptor-core</artifactId></dependency>
<dependency><groupId>org.slf4j</groupId><artifactId>slf4j-api</artifactId></dependency>
<dependency><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId><optional>true</optional></dependency>
```

test scope 加 `junit-jupiter` 与 `assertj-core`。模块 POM 不得出现 Spring 依赖。

- [ ] **Step 2：写公共类型和验证红灯**

`PublicContractTest` 用编译期赋值确认 `EventLoop` 和 `EventLoopGroup` 都是 `SupervisedScheduledExecutor`、Group 可迭代且 EventLoop 暴露 `parent()`。`ScheduledTaskSpecTest` 验证四种调度模式互斥、负 trigger/expires、非正 period/max executions，并拒绝 null dynamic calculator；calculator 运行后返回 null 的失败语义由 Task 3 的 `ScheduledTaskTest` 覆盖。`TaskContextTest` 验证 typed key、不可变复制和空 context。

```bash
$MVN -pl disruptor-concurrent -am -Dtest=PublicContractTest,ScheduledTaskSpecTest,TaskContextTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: 公共类型不存在导致编译失败。

- [ ] **Step 3：实现不依赖运行内核的完整公共类型**

`SupervisedScheduledExecutor<S>` 的签名必须与规格一致：

```java
public interface SupervisedScheduledExecutor<S>
        extends ScheduledExecutorService, AutoCloseable {
    String name();
    CompletionStage<Void> start();
    void requestShutdown(ShutdownMode mode, ShutdownDeadline deadline);
    CompletionStage<S> termination();
    S snapshot();
}
```

`ScheduledTaskSpec`、`TaskContext`、reason、snapshot 均实现真实不可变性、构造校验、equals/hashCode 所需的 record 或 value object 语义。接口只定义稳定行为，不提供抛 `UnsupportedOperationException` 的临时实现，也不创建尚不能工作的 EventLoop builder。

- [ ] **Step 4：验证模块边界并提交**

```bash
$MVN -pl disruptor-concurrent -am test
$MVN -pl disruptor-concurrent dependency:tree
git diff --check
git add pom.xml disruptor-concurrent
git commit -m "feat(concurrent): define public concurrency contracts"
```

Expected: reactor 包含 core、concurrent；dependency tree 显示 concurrent 直接依赖 LMAX、core、SLF4J，且没有 Spring。

### Task 3：任务基元、取消、上下文与 module

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/CancellationSource.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/NanoClock.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/EventLoopFutureTask.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/AcceptedTask.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/ScheduledTask.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/IndexedScheduledHeap.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/CancellationMailbox.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/ModuleLifecycle.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/CancellationSourceTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/ScheduledTaskTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/IndexedScheduledHeapTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/ModuleLifecycleTest.java`

- [ ] **Step 1：写取消竞态和监听器契约测试**

测试至少包含这些独立用例：

```text
firstCancellationWinsAndFreezesReason
lateSynchronousListenerRunsOnRegisteringThread
asynchronousListenerRunsOnProvidedExecutor
successfulUnregisterPreventsInvocationAndUnlinksNode
cancelAndUnregisterLinearizeWithoutDoubleInvocation
listenerFailureDoesNotSkipRemainingListeners
rejectedListenerExecutorIsReportedWithoutChangingReason
cancelAfterUsesExplicitSchedulerAndCancelsTimerWhenSourceWinsFirst
```

对 cancel/unregister 竞态重复运行并记录每个 listener 次数，断言只能是 0 或 1，不能大于 1。

- [ ] **Step 2：写任务、上下文、堆和 module 测试**

使用 `ManualNanoClock` 验证 timer heap 严格按 `trigger asc -> priority desc -> acceptedSequence asc`；删除任意 index 后仍保持堆序；dynamic calculator 返回 null 或负值时 Future 异常终止。module 测试断言 start 声明顺序、只 stop 已成功启动项、stop 逆序、每项一次，以及 start/update 主异常保留、stop 异常全部 suppressed。

- [ ] **Step 3：运行红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=CancellationSourceTest,ScheduledTaskTest,IndexedScheduledHeapTest,ModuleLifecycleTest test
```

Expected: 具体任务基元尚不存在。

- [ ] **Step 4：实现 CancellationSource**

用锁保护双向 listener 链表和首次 reason 提交；锁内只取得 listener 执行权并物理摘链，锁外调用。同步/异步与晚注册线程遵守规格；所有异常交给 `CancellationListenerExceptionHandler`。`cancelAfter` 强制传入 `ScheduledExecutorService`，不引用 GlobalEventLoop。

- [ ] **Step 5：实现任务物理状态和 Future 唯一终态**

`AcceptedTask` 区分 Future outcome 与物理 `WAITING/RUNNING/CANCELLED_WAITING/RETURNED/TERMINAL`，`EventLoopFutureTask` 实现 JDK Future 结果。`ScheduledTask` 只在 loop 线程计算 fixed-rate、fixed-delay、dynamic-delay 下一 trigger；dynamic 计算失败使 Future 异常终止。`CancellationMailbox` 对同一 accepted task 最多保留一个待删除节点。

- [ ] **Step 6：实现 module 生命周期并提交**

`ModuleLifecycle` 冻结输入列表，记录成功 started 的前缀；update 异常向上抛给未来 kernel；stop 遍历完整 started 逆序并聚合 suppressed。

```bash
$MVN -pl disruptor-concurrent test
git diff --check
git add disruptor-concurrent
git commit -m "feat(concurrent): add task cancellation and module primitives"
```

### Task 4：gate、registry 与两种 TaskQueue 契约

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/TaskEnvelope.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/TaskReservation.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/TaskQueue.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/BoundedTaskQueue.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/UnboundedTaskQueue.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/TaskAdmissionGate.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/AdmissionToken.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/AcceptedTaskRegistry.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/TaskAdmissionGateTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/AcceptedTaskRegistryTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/TaskQueueContractTest.java`

- [ ] **Step 1：写 gate 容量与关闭握手测试**

分别把任务停在 admission、入口、timer 模拟持有和 executing 模拟持有状态，断言有界容量都被占用；关闭 gate 后新 token 拒绝，已取得 token 可以完成登记，控制方只有在 token 清零后才能冻结 accepted 集。取消 running Future 不得提前归还 permit。

- [ ] **Step 2：写 registry shutdownNow CAS 测试**

创建乱序 `WAITING/RUNNING/CANCELLED_WAITING/RETURNED/TERMINAL` 记录并并发调用 sweep，断言：只有成功执行 `WAITING -> RETURNED` CAS 的 task 被返回；按 accepted sequence 排序；返回 original Runnable 而非 envelope；running 不返回且收到 cancel(true)；已被普通取消的 waiting task 不返回；每项最多返回一次；sweep 本身不提前释放 permit，worker 物理清理后 registry 和 permit 才都为零。

- [ ] **Step 3：写两后端参数化 queue contract**

同一测试工厂分别创建 `BoundedTaskQueue` 和 `UnboundedTaskQueue`，覆盖四生产者并发 reservation/publish、连续 sequence、abort tombstone、单消费者顺序、消费清引用、segment 跨界与回收。测试不引用 kernel，确保 queue 只承担存储职责。

- [ ] **Step 4：运行红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=TaskAdmissionGateTest,AcceptedTaskRegistryTest,TaskQueueContractTest test
```

Expected: gate、registry 和 queue 类型不存在。

- [ ] **Step 5：实现统一 reservation/publish 协议**

`TaskQueue` 契约固定为内部 claim 入口：

```java
interface TaskQueue {
    TaskReservation tryReserve();
    AcceptedTask<?> poll();
    long pending();
    long remainingCapacity();
    int allocatedSegments();
}

interface TaskReservation {
    long sequence();
    void publish(AcceptedTask<?> task);
    void abort();
}
```

`abort()` 必须发布可跳过 tombstone，不能留下 sequence hole。Bounded 使用 LMAX multi-producer RingBuffer；Unbounded 使用分段 MPSC release/acquire 发布。两者只由一个消费者 poll，poll 后必须清引用。

- [ ] **Step 6：验证压力与提交**

```bash
$MVN -pl disruptor-concurrent test
git diff --check
git add disruptor-concurrent
git commit -m "feat(concurrent): add admission registry and task queues"
```

Expected: 两后端各执行至少 4×10,000 条并发任务，无丢失、重复、永久洞和遗留引用。

### Task 5：bounded 完整 executor、scheduler 与 shutdown

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/DisruptorEventLoop.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopBuilder.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/EventLoopKernel.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/EventLoopWorker.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/EventLoopShutdownBackend.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/DisruptorEventLoopTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopExecutorContractTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopSchedulingTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopShutdownTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopBlockingGuardTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopModuleTest.java`

- [ ] **Step 1：先实现精确 startup 的红灯测试**

用可控 ThreadFactory 和阻塞 module 覆盖：构造不启动线程；register 发生在 start 前；`markStarting -> Thread.start -> finally seal`；worker 入场和 module start 后才 RUNNING/open gate/start stage complete；启动中 shutdown 唯一结局；Thread.start 失败和 module 部分启动失败都真实终止且无线程泄漏。

- [ ] **Step 2：实现唯一 kernel 与 bounded facade**

`EventLoopKernel` 持有唯一 `WorkerSupervisor`、gate、registry、Future/timer/module 状态；`DisruptorEventLoop` 只委派公共 API。`EventLoopBuilder.bounded(name, capacity)` 校验容量为 2 的幂，并创建 `BoundedTaskQueue`。在这一步通过 startup 测试后提交：

```bash
$MVN -pl disruptor-concurrent -Dtest=DisruptorEventLoopTest,EventLoopModuleTest test
git add disruptor-concurrent
git commit -m "feat(concurrent): start supervised bounded event loops"
```

- [ ] **Step 3：写并通过 Executor/Future/阻塞保护测试**

覆盖 execute、tryExecute、三个 submit 重载、invokeAll、invokeAny、拒绝原因、裸 Runnable 异常 handler、submit 异常只进 Future、严格单线程、accepted sequence，以及未完成 Future.get/awaitTermination/invoke/close 的 same-loop `BlockingOperationException`。已完成 Future.get 必须可在 loop 内读取。

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopExecutorContractTest,EventLoopBlockingGuardTest test
```

Expected: `BUILD SUCCESS`。随后提交 `feat(concurrent): execute bounded event loop tasks`。

- [ ] **Step 4：写完整调度红灯测试**

使用手动时钟逐项测试 one-shot、fixed-rate 追赶、fixed-delay、dynamic-delay、expires 前不执行、max executions、continueOnFailure、同 trigger priority、`schedule(0)` 先于后续 execute、timer/command 双向批次公平，以及 `EventLoopScheduledFuture.snapshot()` 的每次状态变化。

- [ ] **Step 5：实现 scheduler 循环并通过测试**

worker 严格执行“cancel mailbox → bounded timer batch → 至少一条且 bounded command batch → module update → 重新读时钟 → park”。遇到刚登记且已到期的 timer 立即结束 command batch。所有 schedule 入口先通过 gate/queue/registry，不允许同 loop 绕过容量。

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopSchedulingTest test
git add disruptor-concurrent
git commit -m "feat(concurrent): schedule bounded event loop tasks"
```

- [ ] **Step 6：写 JDK 21 shutdown 红灯测试**

测试名称和核心断言固定为：

```text
shutdownUsesUnboundedDeadlineAndRunsAcceptedFutureOneShot
shutdownCancelsWaitingPeriodicButDoesNotInterruptCurrentInvocation
shutdownNowReturnsWaitingOriginalRunnablesInAcceptedOrder
shutdownNowDoesNotReturnRunningTaskAndInterruptsWorker
boundedGracefulDeadlineEscalatesWithoutFakingTermination
terminationWaitsForInterruptIgnoringTaskToReallyExit
everyAcceptedFutureIsTerminalAndEveryReferenceIsCleared
```

未来 one-shot 用手动时钟推进证明 shutdown 不会提前取消；抗中断任务用 latch 证明 deadline 到期后 termination 仍未完成。

- [ ] **Step 7：实现非阻塞阶段关闭并通过完整模块测试**

`shutdown()` 调用 `requestShutdown(GRACEFUL, ShutdownDeadline.unbounded())`；`shutdownNow()` 先 close/await admission，再 registry CAS 收集，最后 immediate。`EventLoopShutdownBackend` 的 `beginQuiesce` 只 unpark，`isDrained` 只读 worker 发布的事实，`stop` 只发布模式并唤醒；worker finally 完成 Future、引用和 module 清理，supervisor join 后才 termination。

```bash
$MVN -pl disruptor-concurrent test
git diff --check
git add disruptor-concurrent
git commit -m "feat(concurrent): complete bounded event loop shutdown"
```

### Task 6：unbounded 共用全部契约

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/UnboundedEventLoop.java`
- Modify: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopBuilder.java`
- Create: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopBackendContractTest.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/UnboundedEventLoopTest.java`

- [ ] **Step 1：把完整 EventLoop 契约参数化**

`EventLoopBackendContractTest` 的 factory 参数分别构造 bounded/unbounded，并复用 Task 5 的 startup、Executor、四种 schedule、cancel、module、shutdown、blocking guard 和真实 termination 核心断言。增加反射或 package-private 测试入口，断言两种 facade 内持有的对象 class 都是同一个 `EventLoopKernel`。

- [ ] **Step 2：运行 unbounded 红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopBackendContractTest,UnboundedEventLoopTest test
```

Expected: `UnboundedEventLoop` 与 builder 入口不存在。

- [ ] **Step 3：只注入 UnboundedTaskQueue**

`EventLoopBuilder.unbounded(name, segmentSize)` 创建 `UnboundedEventLoop`，除此之外走与 bounded 完全相同的 kernel 构造。不得复制 worker loop、scheduler、Future、module、supervisor 或 shutdown 类。snapshot 的 remaining capacity 固定表达 unbounded，allocated segments 报真实值。

- [ ] **Step 4：验证并提交**

```bash
$MVN -pl disruptor-concurrent test
rg -n 'class EventLoopKernel|class EventLoopWorker' disruptor-concurrent/src/main/java
git diff --check
git add disruptor-concurrent
git commit -m "feat(concurrent): add unbounded event loop facade"
```

Expected: kernel/worker 各只有一个生产实现；两后端通过同一契约。

### Task 7：EventLoopGroup 生命周期所有权

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/DisruptorEventLoopGroup.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupBuilder.java`
- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/GroupLifecycleCoordinator.java`
- Create: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupTest.java`
- Create: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupShutdownTest.java`

- [ ] **Step 1：写选择、parent 和 owner gate 红灯测试**

对 `Integer.MIN_VALUE/-1/0/1/Integer.MAX_VALUE` 重复调用 `select`，断言固定映射；`next` 按 child index 轮询；iterator 不可修改。child `parent()` 返回同一个 Group，Group RUNNING 前和关闭开始后 child 直接提交都拒绝。

- [ ] **Step 2：写 child 生命周期所有权测试**

从 `select`/iterator 取得 child 后调用 `start/shutdown/shutdownNow/requestShutdown`，断言抛 `ChildLifecycleOwnershipException` 且 Group 状态未被部分改变。只有 Group 内部 coordinator 持 owner token 才能驱动 child 生命周期。

- [ ] **Step 3：写聚合启动和关闭测试**

覆盖全部 child 成功后 Group gate 一次开放；一个 child 启动失败先关闭 gate、再以同一 rollback deadline fail-stop 所有 child；child 运行时基础设施失败不重映射 affinity；标准 Group shutdown 使用同一 unbounded deadline；Group shutdownNow 按 child index 和 child accepted sequence 聚合返回值；显式 bounded request 把同一个对象广播给全部 child；Group termination 等最后一个抗中断 child 真实退出。

- [ ] **Step 4：运行红灯**

```bash
$MVN -pl disruptor-concurrent -Dtest=EventLoopGroupTest,EventLoopGroupShutdownTest test
```

Expected: Group 实现与 owner coordinator 不存在。

- [ ] **Step 5：实现固定 Group 与唯一关闭会话**

`GroupLifecycleCoordinator` 维护 startup outcome、首个 deadline、最高 shutdown mode、broadcast-in-flight token、child termination facts 和唯一 outcome。任务委派在提交时选择 child；Group snapshot 按 child index 聚合。启动/停止广播不得因单个 child 抛异常而跳过其余 child。

- [ ] **Step 6：验证并提交**

```bash
$MVN -pl disruptor-concurrent test
git diff --check
git add disruptor-concurrent
git commit -m "feat(concurrent): add owned event loop groups"
```

### Task 8：Boot 4.1 生命周期、健康与指标

**Files:**

- Modify: `disruptor-spring-boot-autoconfigure/pom.xml`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentProperties.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentLifecycle.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentAutoConfiguration.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentHealthContributor.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentHealthAutoConfiguration.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentMetrics.java`
- Create: `disruptor-spring-boot-autoconfigure/src/main/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentMetricsAutoConfiguration.java`
- Modify: `disruptor-spring-boot-autoconfigure/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/DisruptorConcurrentAutoConfigurationTest.java`
- Test: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentLifecycleTest.java`
- Test: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentHealthContributorTest.java`
- Test: `disruptor-spring-boot-autoconfigure/src/test/java/com/sstlfsj/disruptor/autoconfigure/DisruptorConcurrentMetricsTest.java`

- [ ] **Step 1：添加 optional compile/test 依赖**

autoconfigure compile optional 加 `disruptor-concurrent` 和 `org.springframework.boot:spring-boot-health`；现有 `micrometer-core` 保持 optional。测试依赖加入 concurrent 与 Boot health/metrics 实现。starter POM 不添加 concurrent。

- [ ] **Step 2：写三类条件装配红灯测试**

分别用 `FilteredClassLoader` 隐藏 concurrent、`HealthIndicator`、`MeterRegistry`，断言基础生命周期、health、metrics 互不误装配且不存在 `NoClassDefFoundError`。有两个 standalone loop 和一个 Group Bean 时只管理三个根 Bean，不扫描 Group child，不创建全局 loop。

- [ ] **Step 3：写 SmartLifecycle 真实 termination 测试**

`stop(callback)` 测试记录每个 root 收到的 deadline 对象身份；断言先完成全部同步 request，再异步等待 termination；一个 root 超时或异常时 callback 仍只在全部 root 真实 termination 后恰好一次执行。

- [ ] **Step 4：实现拆分自动配置**

基础配置不得 import health 或 Micrometer 类型。Health 配置使用 Boot 4.1 `org.springframework.boot.health.contributor` 包；Metrics 配置只从 snapshot 注册 gauge。属性前缀为 `disruptor.concurrent`，只包含 `enabled`、`lifecycle-phase` 和 `shutdown-timeout`，不自动创建业务 EventLoop。

- [ ] **Step 5：验证并提交**

```bash
$MVN -pl disruptor-spring-boot-autoconfigure -am test
git diff --check
git add disruptor-spring-boot-autoconfigure
git commit -m "feat(boot): manage supervised event loops"
```

Expected: 无 actuator/health 时基础装配通过，无 Micrometer 时 health 仍通过，三类 metadata 都由真实测试覆盖。

### Task 9：示例、架构文档与 Commons 事实矩阵

**Files:**

- Modify: `README.md`
- Modify: `docs/disruptor-architecture-design.md`
- Modify: `docs/superpowers/specs/2026-09-02-disruptor-concurrent-design.md`
- Modify: `disruptor-spring-boot-example/pom.xml`
- Create: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/concurrent/ConcurrentExampleConfiguration.java`
- Create: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/concurrent/OrderEventLoopService.java`
- Create: `disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/concurrent/ConcurrentExampleTest.java`
- Create: `docs/commons-capability-matrix.md`

- [ ] **Step 1：写会真实运行的示例测试**

Spring 示例显式声明一个 bounded root loop 和一个两 child Group；测试等待 start stage，提交 `ContextCallable`、fixed-rate、dynamic-delay、cancel listener 和 affinity 任务，然后通过 lifecycle 发起共享 deadline 关闭并等待真实 termination。示例不创建 hidden singleton，也不直接关闭 Group child。

- [ ] **Step 2：更新用户与维护者文档**

README 只说明依赖、启动、普通提交、高级 schedule、shutdown 与 Group owner 规则。架构文档更新最终模块依赖、`SupervisedLifecycle`、标准无界 shutdown 与显式 bounded 运维关闭，不保留旧 `PipelineLifecycle` 或“shutdownNow 返回空列表”的描述。

- [ ] **Step 3：固化 Commons 源码证据矩阵**

`docs/commons-capability-matrix.md` 记录参考 commit `5c831c06` 和具体类/方法证据；dynamic delay 与 priority 标为本项目增强；lazy start、阻塞保护、cancelAfter、parent、factory、local order、Agent phases、ComponentId indexing 分别标为覆盖、项目级替代或不复制。文档明确“不承诺二进制/API 兼容”。

- [ ] **Step 4：验证示例与文档一致性并提交**

```bash
$MVN -pl disruptor-spring-boot-example -am test
rg -n 'PipelineLifecycle|等价覆盖.*Agent' README.md docs/disruptor-architecture-design.md docs/commons-capability-matrix.md
rg -n '本项目.*shutdownNow.*空列表|完全兼容' README.md docs/disruptor-architecture-design.md docs/commons-capability-matrix.md
git diff --check
git add README.md docs disruptor-spring-boot-example
git commit -m "docs: document supervised concurrent usage"
```

Expected: 示例通过；搜索无过时断言，能力矩阵每行都有明确分类。

### Task 10：JMH 与全仓完成审计

**Files:**

- Modify: `disruptor-benchmarks/pom.xml`
- Create: `disruptor-benchmarks/src/main/java/com/sstlfsj/disruptor/benchmark/EventLoopBenchmark.java`
- Create: `disruptor-benchmarks/src/commons/java/com/sstlfsj/disruptor/benchmark/CommonsEventLoopBenchmark.java`
- Modify: `disruptor-benchmarks/src/main/java/com/sstlfsj/disruptor/benchmark/BenchmarkMain.java`
- Create: `docs/disruptor-concurrent-verification.md`

- [ ] **Step 1：写可枚举的基准 smoke test**

基准类包含相同工作负载下的：原生 LMAX publish/consume、core managed publish、bounded EventLoop、unbounded EventLoop、JDK single-thread executor。先用 JMH runner 的 include 列表和最短配置运行，断言五个基准名称都能被发现并完成。

- [ ] **Step 2：加入可选 Commons profile**

POM 的 `commons-baseline` profile 只在显式 `-Pcommons-baseline` 时依赖 `cn.wjybxx.commons:commons-concurrent:2.0.0`，并通过 `build-helper-maven-plugin:3.6.1` 把 `src/commons/java` 加入该 profile 的编译源。运行前在参考仓安装当前 commit：

```bash
$MVN -f /Users/sunke/dev/ai-project/commons/java/pom.xml -pl Commons-Concurrent -am install -DskipTests
```

默认 reactor 不依赖本机参考仓；profile 缺少本地 artifact 时明确构建失败，不静默跳过后又宣称已比较。

- [ ] **Step 3：运行全仓验证**

```bash
$MVN clean verify
$MVN -pl disruptor-benchmarks -am package
java -jar disruptor-benchmarks/target/benchmarks.jar EventLoopBenchmark -wi 1 -i 2 -f 1
$MVN -pl disruptor-benchmarks -am -Pcommons-baseline package
java -jar disruptor-benchmarks/target/benchmarks.jar CommonsEventLoopBenchmark -wi 1 -i 2 -f 1
```

Expected: 全 reactor `BUILD SUCCESS`；默认五组和 Commons profile 都产生结果。不设置机器敏感的吞吐硬阈值。

- [ ] **Step 4：执行静态边界审计**

```bash
rg -n 'System\.(out|err)|CallerRuns|DiscardPolicy|UnsupportedOperationException' disruptor-core disruptor-concurrent disruptor-spring-boot-autoconfigure
rg -n 'class EventLoopKernel|class EventLoopWorker|class TaskAdmissionGate|class AcceptedTaskRegistry' disruptor-concurrent/src/main/java
rg -n 'PipelineLifecycle' disruptor-core/src disruptor-concurrent/src disruptor-spring-boot-autoconfigure/src
git diff --check
git status --short
```

Expected: 无新增违规或旧生命周期名称；kernel、worker、gate、registry 各只有一个生产实现；工作区只有本任务计划内文件。

- [ ] **Step 5：逐条记录完成证据并提交**

`docs/disruptor-concurrent-verification.md` 按设计文档“验证策略”和 Commons 矩阵逐项记录对应测试类、测试方法、JMH 名称或明确的非复制边界。每个 design requirement 必须有直接证据；缺少证据时回到对应 Task 实现，不能用全仓绿灯替代范围证明。

```bash
git add disruptor-benchmarks docs/disruptor-concurrent-verification.md
git commit -m "perf: verify disruptor concurrent architecture"
git status --short --branch
```

Expected: 最终工作区干净，当前分支包含十个纵向切片的可审计提交。
