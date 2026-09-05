# Concurrent 吞吐内核重写 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `disruptor-concurrent` 执行/调度内核重写为槽位原生零分配架构,消除普通提交路径的每任务对象分配与 monitor 同步,同时不退化任何公开行为契约。

**Architecture:** 预分配 typed ring 槽即任务记录(ordinary 零分配);容量用 `TaskAdmissionGate` 内的显式 `outstanding` 账本(bounded 走 packed word、unbounded 走独立 64 位计数器);统一物理所有权状态机 + 两种存储(ordinary 槽内 CAS,由 queue 暴露控制面;tracked/scheduled 外部记录 + tracked-only index);关闭在 core `WorkerSupervisor` 之上加从属 `TaskDispositionCoordinator`;无界后端槽池化 + 发布代际 + 段生命周期锁;Group 用单个全局 `accepting` volatile。

**Tech Stack:** Java 21(VarHandle/packed AtomicLong/虚拟线程)、LMAX Disruptor 4.0.0(`RingBuffer<Cell>` + `BusySpinWaitStrategy`)、`disruptor-core`(`WorkerSupervisor`/`ShutdownBackend`/`ShutdownDeadline`)、JUnit 5、JMH 1.37。

**Spec:** `docs/superpowers/specs/2026-09-05-1438-concurrent-throughput-architecture-design.md`(executor 必须与本计划同读)。

## Global Constraints

- 单一 `EventLoopKernel`;bounded/unbounded 只换 `TaskQueue`,不分叉 kernel/调度/生命周期(spec §3.3)。
- ordinary 热路径**零堆分配**;Future/记录/index 只用于 `submit/schedule`(spec §3.2/§4.1)。
- 生命周期/mode/deadline/首因/termination 唯一权威是 core `WorkerSupervisor`(spec §3.3/§4.8)。
- **容量账本归 `TaskAdmissionGate`,`TaskQueue` 只管物理存储/游标/publication**(spec §5)。bounded packed outstanding、unbounded 独立 `AtomicLong` outstanding **都在 gate**;kernel 不按后端分叉查容量。
- `EventLoopSnapshot` 硬约束 `outstanding ≤ capacityLimit` 恒成立(spec §3.3)。
- `execute()` 满容量 fail-fast 抛 `RejectedExecutionException`,不阻塞重试(spec §3.3/§4.4)。
- **无兼容层、无临时补丁**:接口切换会造成循环编译依赖,凡互相依赖的模块在**同一提交内原子切换**(见 Task 1),不在旧接口上加 shim。
- 生产代码禁止 `System.out/System.err`、`CallerRuns`、`DiscardPolicy`;默认 jar 无 `cn.wjybxx`(spec §7)。
- TDD:先写测试再实现;每个**可独立编译绿**的 Task 结束时提交;不用 `--no-verify`/禁用测试换绿。
- **本机测试命令**(mvn 不在 PATH;亦可 `/mvn-env`):
  ```bash
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
  $MVN -pl disruptor-concurrent -am -Dtest=<Class>[,<Class2>] -Dsurefire.failIfNoSpecifiedTests=false test
  ```
  全量:`$MVN -pl disruptor-concurrent -am test`;core 回归:`$MVN -pl disruptor-core -am test`。

## 文件结构与职责

| 文件 | 动作 | 职责 |
| --- | --- | --- |
| `internal/TaskAdmissionGate.java` | 重写 | packed `AtomicLong`(NEW/OPEN/CLOSED + activePublishers)+ 容量账本(bounded packed outstanding、unbounded 独立 `AtomicLong`);enter/leave/completeBatch/outstanding/open/close/awaitDrained,无 monitor |
| `internal/TaskQueue.java` | 重写接口 | claim/write*/publish/消费前缀游标/ordinary 物理态控制面/scan;**不感知 outstanding** |
| `internal/BoundedTaskQueue.java` | 重写 | `RingBuffer<Cell>` typed 槽 + 双消费游标 + `BusySpinWaitStrategy` |
| `internal/UnboundedTaskQueue.java` | 重写 | 分段 typed 槽 + 发布代际 + 段生命周期锁 + `maxPooledSegments` |
| `internal/AcceptedTask.java` | 重写 | 物理所有权状态机(含周期 RUNNING→WAITING 条件)+ `shutdownNowReturnValue`;仅 tracked/scheduled |
| `internal/AcceptedTaskRegistry.java` | 重写 | tracked-only index;`register/terminalizeAndRemove/scanForShutdown`(处理 WAITING 与 RUNNING) |
| `internal/EventLoopKernel.java` | 重写 admit/consume/shutdown | 编排 gate+queue+registry;类型分派;正确 ordinary 消费序;单写者指标;基础三态关闭 |
| `internal/TaskDispositionCoordinator.java` | 新建(Task 2) | RETURNING/DISCARDING 完成阶段、并发关闭、卡死异步取消、退出序列 |
| `internal/EventLoopShutdownBackend.java` | 改 | `stop(IMMEDIATE)` 保留 `kernel.stopWorker` + 登记 DISCARD + unpark |
| `internal/ScheduledTask.java`、`internal/CancellationMailbox.java` | 改 | 适配新 `AcceptedTask` |
| `internal/GroupLifecycleCoordinator.java`、`DisruptorEventLoopGroup.java` | 改(Task 3) | 全局 `accepting` volatile,删每任务 lease |
| `EventLoopSnapshot.java`、`EventLoopGroupSnapshot.java`、`autoconfigure/DisruptorConcurrentMetrics.java`、`EventLoopBuilder.java` | 改(Task 4) | `activeQueueSegments`/`discardedTasks`/`maxPooledSegments` |
| 删除 | `internal/AdmissionToken.java`、`internal/TaskReservation.java`、`internal/TaskEnvelope.java`(Task 1 内) |

---

## Task 0: 契约特征化 + JMH 基线

**Files:**
- Test: 新增/补齐 `disruptor-concurrent` 内需要冻结的行为断言(execute 全序、容量含 running、shutdownNow 原始返回、running future 取消、schedule 全序)——大多已存在,本任务只**清点并运行**确认当前全绿,作为 cutover 的回归基线。
- 记录:`disruptor-concurrent-verification.md` 现有 JMH 表(bounded 756 B/op 等)即改造前红信号基线,不改文件。

- [ ] **Step 1:** 运行 `$MVN -pl disruptor-concurrent -am test`,记录当前测试数与全绿事实(cutover 后逐字段比对)。
- [ ] **Step 2:** 构建 benchmarks 跑 `EventLoopBenchmark.boundedEventLoop -prof gc`,记录当前 ≈756 B/op(cutover 的真实红信号,B1)。
- [ ] **Step 3:** 无代码改动,不提交(纯基线清点);若补了新的 characterization 测试则 `git commit -m "test(concurrent): freeze pre-cutover behavior baseline"`。

---

## Task 1: 原子 kernel cutover(单次绿提交)

> 这是一次**原子替换**:gate、公共 `TaskQueue`、bounded/unbounded 两个队列、记录状态机、tracked index、kernel admit/consume/基础关闭同时切换,删除三个旧类。**只在整个模块重新编译且既有测试(语义不变部分)+ 新增契约测试全绿后提交一次。** 内部按下列子步顺序施工。

**Files:** 重写 `internal/TaskAdmissionGate.java`、`internal/TaskQueue.java`、`internal/BoundedTaskQueue.java`、`internal/UnboundedTaskQueue.java`、`internal/AcceptedTask.java`、`internal/AcceptedTaskRegistry.java`、`internal/EventLoopKernel.java`;改 `internal/ScheduledTask.java`、`internal/CancellationMailbox.java`、`internal/EventLoopShutdownBackend.java`;删 `internal/AdmissionToken.java`、`internal/TaskReservation.java`、`internal/TaskEnvelope.java`。
**Test:** `internal/TaskAdmissionGateTest`、`internal/TaskQueueContractTest`、`internal/AcceptedTaskRegistryTest`(均重写)+ 既有 `EventLoopExecutorContractTest`、`EventLoopSchedulingTest`、`EventLoopBackendContractTest`、`EventLoopShutdownTest`、`UnboundedEventLoopTest`(必须全绿)。

**Interfaces(本 Task 内一次性定型):**
```java
// --- gate 拥有容量账本(A3) ---
final class TaskAdmissionGate {
    static TaskAdmissionGate bounded(int capacity);   // >0 且 ≤2^31-1 fail-fast
    static TaskAdmissionGate unbounded();
    void open(); void closeForAdmissions(); boolean isAccepting();
    boolean tryEnter();          // bounded: OPEN&&outstanding<capacity → pub++,outstanding++; unbounded: OPEN → pub++,outstanding++(AtomicLong,无上限)
    void leave(boolean rollbackOutstanding);   // pub--;rollback 则同 CAS/计数 outstanding--
    void completeBatch(int n);   // outstanding-=n（bounded packed / unbounded AtomicLong,按 mode）
    long outstanding(); long activePublishers();
    void awaitDrained();         // spin-then-park;末位 leave 唤醒
}
// --- queue 不感知容量,暴露 ordinary 物理态控制面(A2) ---
enum TaskType { ORDINARY, TRACKED, SCHEDULE, TOMBSTONE }
enum OrdinaryState { WAITING, RUNNING, RETURNED, DISCARDED, TERMINAL }
enum OrdinaryDisposition { RETURN, DISCARD }
interface TaskQueue {
    long tryClaim();                                  // -1=物理满
    void writeOrdinary(long seq, Runnable task);
    void writeTracked(long seq, AcceptedTask<?> record);
    void writeSchedule(long seq, AcceptedTask<?> record);
    void writeTombstone(long seq);
    void publish(long seq);
    boolean poll();                                    // 只前进到下一个连续已发布槽
    TaskType currentType(); Runnable currentOrdinary(); AcceptedTask<?> currentRecord();
    boolean tryStartCurrentOrdinary();                 // WAITING→RUNNING(worker 运行前)
    OrdinaryState currentOrdinaryState();              // worker 识别 scanner 已抢的 RETURNED/DISCARDED
    void terminalizeCurrentOrdinary();                 // RUNNING→TERMINAL
    void advanceConsumer();                            // release 推进 consumerSequence(离开 ingress)
    void releaseCurrentSlot();                         // 清槽引用 + 推进 gatingSequence(允许复用)
    long claimedCursor(); long pending();              // pending=claimedCursor-consumerSequence
    void scanOrdinaryUnstarted(long claimedInclusive, OrdinaryDisposition disp, OrdinaryClaimedSink sink);
    int allocatedSegments(); int activeSegments();     // bounded 恒 0
}
@FunctionalInterface interface OrdinaryClaimedSink { void accept(long seq, Runnable original); } // 只回调已 CAS 成功抢占的
// --- 记录状态机 + 关闭遍历(A5) ---
final class AcceptedTask<V> {
    enum PhysicalState { WAITING, RUNNING, RETURNED, DISCARDED, CANCELLED_WAITING, TERMINAL }
    long acceptedSequence(); EventLoopFutureTask<V> future(); ScheduledTask<V> scheduledTask();
    Runnable shutdownNowReturnValue();                 // Runnable 源=原始 Runnable;Callable/spec 源=RunnableFuture/ScheduledFuture
    boolean tryStart(); boolean tryReturn(); boolean tryDiscard(); boolean markCancelledWaiting();
    boolean returnToWaiting(RearmCheck check);         // RUNNING→WAITING,check 全条件
    boolean terminate(); PhysicalState state();
}
@FunctionalInterface interface RearmCheck { boolean canRearm(); }
final class AcceptedTaskRegistry {                     // tracked-only
    void register(AcceptedTask<?> task); void remove(AcceptedTask<?> task);
    void terminalizeAndRemove(AcceptedTask<?> task);   // no-throw
    void scanForShutdown(ShutdownAction action);       // ticket 升序;action 内:WAITING→CAS RETURN/DISCARD、RUNNING→future.cancel(true) 不返回、其它忽略
    int size();
}
@FunctionalInterface interface ShutdownAction { void handle(AcceptedTask<?> task); }
```

- [ ] **Step 1.1 gate(先写测试)** — 重写 `TaskAdmissionGateTest`:容量 off-by-one(`bounded(1)` 首个 `tryEnter()` 真、第二个假)、`leave(true)` 不泄漏(即使未 claim)、`closeForAdmissions` 后 `tryEnter` 假且 `awaitDrained` 等到 `activePublishers==0`、unbounded `tryEnter` 无容量门但 `outstanding()` 如实增长、`completeBatch` 递减、capacity `2^31` fail-fast。实现 packed 布局 `[63:62]`lifecycle/`[61:31]`outstanding/`[30:0]`pub;unbounded 用同 word 的 lifecycle+pub 加一个独立 `AtomicLong outstanding`。
- [ ] **Step 1.2 queue(先写测试)** — 重写 `TaskQueueContractTest`(bounded):消费前缀遇空洞停(先发 s1 留 s0 空洞→`poll` 假,补 s0 后才真)、`tryStartCurrentOrdinary` WAITING→RUNNING、`advanceConsumer` 后槽未 `releaseCurrentSlot` 则 `tryClaim` 拿不到(容量 1)、`releaseCurrentSlot` 后可复用、4×1000 多生产者每 seq 恰好消费一次、tombstone 被跳过。实现 bounded:`createMultiProducer(Cell::new, cap, new BusySpinWaitStrategy())`,`consumerSequence`/`gatingSequence` 两 `Sequence`,只 `gatingSequence` 入 `addGatingSequences`。
- [ ] **Step 1.3 record + index(先写测试)** — 重写 `AcceptedTaskRegistryTest`:WAITING 只能被 return 或 discard 之一(互斥)、`scanForShutdown` 按 ticket 升序、RUNNING 记录被 `future.cancel(true)` 且不进 sink、周期 `returnToWaiting` 在 quiescing/expires/max/非周期返回 false。实现 `AcceptedTask`(`AtomicReference<PhysicalState>` CAS)、registry(`ConcurrentSkipListMap`)。适配 `ScheduledTask`(周期重排调 `returnToWaiting(check)`)、`CancellationMailbox`。
- [ ] **Step 1.4 unbounded queue** — `UnboundedTaskQueue` 实现同 `TaskQueue`:分段 `Cell[]`+`long[] publishedSeq`(release/acquire 代际)、跨段 `segmentLock`(`ReentrantLock`)回收到空闲链表、超 `maxPooledSegments`(暂内部常量 8,Task 4 提为 builder 配)释放 GC、复用前清 payload/state;`TaskQueueContractTest` 无界部分(段回收、代际、双 scanner+回收无 use-after-free)。
- [ ] **Step 1.5 kernel admit(零分配三阶段)** — 重写 `EventLoopKernel.admit`:ordinary=`gate.tryEnter → queue.tryClaim → writeOrdinary → publish → leave(false)`,三层嵌套 finally 保证 publish/leave 不跳过、回滚 no-throw(spec §4.4);tracked/scheduled 建 `EventLoopFutureTask`+`AcceptedTask`、`index.register`、`writeTracked/Schedule`;满容量 `execute` 抛 `RejectedExecutionException`、`tryExecute` 假;删 `AdmissionToken/TaskReservation/TaskEnvelope` 三文件并清 import。
- [ ] **Step 1.6 kernel 消费(正确 ordinary 序,A4)** — 按 `currentType()` 分派。**ORDINARY 顺序严格为**:
  ```
  if (!queue.tryStartCurrentOrdinary()) { // scanner 已抢 RETURNED/DISCARDED
      queue.advanceConsumer(); queue.releaseCurrentSlot(); continue; }
  queue.advanceConsumer();                 // 已离 ingress(scanner 排除、pending 不含 executing)
  try { runOrdinary(currentOrdinary()); } finally {
      queue.terminalizeCurrentOrdinary();  // RUNNING→TERMINAL
      queue.releaseCurrentSlot();          // 清槽 + 推进 gating(才允许复用)
  }
  ```
  TRACKED/SCHEDULE:读 `currentRecord()` 后 `advanceConsumer()+releaseCurrentSlot()`(早释放),再 run future/`timers.add`。指标 `completed/failed/cancelled` 覆盖**所有 accepted task**(含 ordinary),worker 单写者;每批 `gate.completeBatch(n)`。唤醒协议(spec §4.7):park 前 `lazySet(parked=true)` 双检、只对未来 timer `parkNanos`、无 timer 无限 park、生产者仅 `parked` 时 unpark、tombstone 也 unpark。
- [ ] **Step 1.7 kernel 基础三态关闭** — `closeForAdmissions()` 同步幂等前置;`shutdownNow`:关准入→`gate.awaitDrained`→冻结 `claimedCursor`→`queue.scanOrdinaryUnstarted(claimed, RETURN, sink)`+`registry.scanForShutdown`(RETURNED 前先 `future.cancel`;RUNNING→`cancel(true)` 不返回)→按 ticket 合并返回→`supervisor.requestShutdown(IMMEDIATE)`,不等 termination;`requestShutdown(IMMEDIATE)`/`shutdown()`/graceful 走对应处置(此步先实现单线程正确版本,并发/卡死硬化在 Task 2)。`EventLoopShutdownBackend.stop(IMMEDIATE)` 保留 `kernel.stopWorker(IMMEDIATE)`。
- [ ] **Step 1.8 ordinary-running vs scanner 竞争测试** — 追加:一个 ordinary 正在运行(占 worker),另一线程 `shutdownNow`——断言运行中 ordinary **不被** RETURNED(计入 completed/failed 而非 returned),排队 ordinary 才进 returned。
- [ ] **Step 1.9 全模块编译 + 全绿 + GC + 单次提交** — `$MVN -pl disruptor-concurrent -am test` 全绿(逐项比对 Task 0 清单,语义不变的测试数不减、断言不弱);benchmarks GC 手测 bounded/unbounded ≤10 B/op(真实红→绿信号,B1)。绿后一次提交:`git commit -m "feat(concurrent): atomic slot-native kernel cutover"`

---

## Task 2: TaskDispositionCoordinator + 关闭硬化

**Files:** Create `internal/TaskDispositionCoordinator.java`;Modify `internal/EventLoopKernel.java`(接入完成阶段/退出序列)、`internal/EventLoopShutdownBackend.java`。
**Test:** `EventLoopShutdownTest.java`(既有全绿 + 新增并发/卡死)。

**Interfaces:**
```java
final class TaskDispositionCoordinator {
    enum State { NONE, RETURNING, RETURNED, DISCARDING, DISCARDED }
    boolean tryBeginReturning();   // NONE→RETURNING(单赢家)
    boolean tryBeginDiscarding();  // NONE→DISCARDING(败给 RETURNING 返回 false)
    void failReturningToDiscarding(Throwable cause);
    void completeReturned(); void completeDiscarded();
    State state(); boolean isTerminal();
}
```

- [ ] **Step 1: 写失败测试(并发不拆分 + 卡死取消 Future,B2 用 `awaitIgnoringInterrupt`)**
  ```java
  // EventLoopShutdownTest.java 追加
  @Test
  void concurrentShutdownNowDoesNotSplitReturnedSet() throws Exception {
      DisruptorEventLoop loop = runningLoop("concurrent-now", 64);
      CountDownLatch block = new CountDownLatch(1);
      loop.execute(() -> awaitIgnoringInterrupt(block));      // 占住 worker,忽略中断(否则提前退出致抖动)
      List<Runnable> queued = new ArrayList<>();
      for (int i = 0; i < 20; i++) { Runnable r = () -> {}; queued.add(r); loop.execute(r); }
      var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
      var f1 = pool.submit(loop::shutdownNow);
      var f2 = pool.submit(loop::shutdownNow);
      List<Runnable> a = f1.get(2, TimeUnit.SECONDS), b = f2.get(2, TimeUnit.SECONDS);
      block.countDown();
      List<Runnable> union = new ArrayList<>(a); union.addAll(b);
      assertEquals(20, union.size());                         // 并集恰好 20
      assertTrue(a.isEmpty() || b.isEmpty());                 // 单赢家,不拆分
      pool.shutdownNow();
  }

  @Test
  void stuckWorkerStillCancelsQueuedFutureOnImmediateDeadline() throws Exception {
      DisruptorEventLoop loop = runningLoop("stuck", 16);
      CountDownLatch block = new CountDownLatch(1);
      loop.execute(() -> awaitIgnoringInterrupt(block));       // worker 卡住、忽略中断
      Future<?> queued = loop.submit(() -> {});                // 未开始的 tracked
      loop.requestShutdown(com.sstlfsj.disruptor.core.ShutdownMode.IMMEDIATE,
              com.sstlfsj.disruptor.core.ShutdownDeadline.after(java.time.Duration.ofMillis(100)));
      awaitCondition(queued::isCancelled);                     // 异步执行者取消,不依赖卡死的 worker
      block.countDown();
  }
  ```
- [ ] **Step 2: 运行验证失败** — `-Dtest=EventLoopShutdownTest` → FAIL(基础关闭无完成阶段/异步取消)。
- [ ] **Step 3: 实现** — `shutdownNow`:`tryBeginReturning` 赢家立即 `supervisor.requestShutdown(IMMEDIATE)`(尽早 interrupt)再扫描,**RETURNING 由调用线程独占、worker 不协助抢占**,扫描失败→`failReturningToDiscarding`→worker 接管丢弃(spec §4.8 A1);`requestShutdown(IMMEDIATE)`/`stop(IMMEDIATE)`:`tryBeginDiscarding`+起命名虚拟线程执行者(取消未开始 Future、标 DISCARDED,带失败 completion→supervisor 首因)+`kernel.stopWorker(IMMEDIATE)`+unpark;worker 退出序列:处置终态前不批量清未开始任务,终态后物理收敛。
- [ ] **Step 4: 运行验证通过** — `-Dtest=EventLoopShutdownTest` → PASS(既有 `shutdownNowReturnsWaitingOriginalRunnablesInAcceptedOrder`、`shutdownNowDoesNotReturnRunningTaskAndInterruptsWorker`、`terminationWaitsForInterruptIgnoringTaskToReallyExit` 全绿)。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): disposition coordinator with return/discard phases"`

---

## Task 3: Group 全局 accepting

**Files:** Modify `internal/GroupLifecycleCoordinator.java`、`internal/EventLoopKernel.java`(child enter 读 parent accepting)、`DisruptorEventLoopGroup.java`。
**Test:** `EventLoopGroupTest.java`、`EventLoopGroupShutdownTest.java`(既有全绿 + startup 窗口)。

**Interfaces:** `GroupLifecycleCoordinator.isAccepting()`(单 `volatile boolean`);删 `AdmissionLease`。

- [ ] **Step 1: 写失败测试(startup 无提前提交窗口,B3 具体化)**
  ```java
  // EventLoopGroupTest.java 追加
  @Test
  void childRejectsUntilGroupGloballyAccepting() throws Exception {
      CountDownLatch startGate = new CountDownLatch(1);
      // 一个 module,其 onStart 阻塞在 startGate,拖住 Group 整体 start
      EventLoopModule blocking = new EventLoopModule() {
          @Override public void onStart(EventLoop loop) throws Exception { awaitIgnoringInterrupt(startGate); }
      };
      // 经 EventLoopGroupBuilder 构建 2-child 有界 Group,child 携带 blocking module(用 factory)
      DisruptorEventLoopGroup group = EventLoopGroupBuilder
              .bounded("startup-window", /*children*/2, /*capacity*/16,
                       (parent, childIndex) -> /*EventLoopFactory: build child with 'blocking' module*/ null)
              .build();
      CompletionStage<Void> starting = group.start();          // 异步,不 join;卡在 module onStart
      EventLoop child = group.iterator().next();               // 取一个已构造 child 引用
      assertThrows(RejectedExecutionException.class, () -> child.execute(() -> {}));  // 全局未 accepting → 拒绝
      startGate.countDown();                                    // 放行 start
      starting.toCompletableFuture().get(2, TimeUnit.SECONDS);  // Group 整体 RUNNING
      CountDownLatch ran = new CountDownLatch(1);
      child.execute(ran::countDown);
      assertTrue(ran.await(2, TimeUnit.SECONDS));               // 现在接受
      group.close();
  }
  ```
  (executor 按 `EventLoopGroupBuilder.bounded(name,children,capacity,factory)` 与 `EventLoopFactory(parent,childIndex)` 的实际签名补全 factory;module 注入方式沿用现有 `EventLoopGroupTest` 的 child 构造约定。)
- [ ] **Step 2: 运行验证失败** — `-Dtest=EventLoopGroupTest` → FAIL。
- [ ] **Step 3: 实现** — `GroupLifecycleCoordinator` 用 `volatile boolean accepting`;全部 child RUNNING 后单点置真;关闭先置假再逐 child `closeForAdmissions`+各自 `awaitDrained`;`EventLoopKernel.admit` 在 `gate.tryEnter` 成功后检查 `groupOwner==null || groupOwner.isAccepting()`,否则 `leave(true)` 拒绝;删每任务 lease 及其方法。
- [ ] **Step 4: 运行验证通过** — `-Dtest=EventLoopGroupTest,EventLoopGroupShutdownTest` → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): group admission via single global accepting flag"`

---

## Task 4: 可观测性贯穿

**Files:** Modify `EventLoopSnapshot.java`、`EventLoopGroupSnapshot.java`、`autoconfigure/DisruptorConcurrentMetrics.java`、`EventLoopBuilder.java`(`maxPooledSegments` 默认 8)。
**Test:** metrics/auto-configuration 测试、snapshot 不变量测试。

- [ ] **Step 1: 写失败测试** — immediate 关闭后 `snapshot().discardedTasks()>0`;`EventLoopGroupSnapshot` 聚合 child `discardedTasks`;metrics 注册 `disruptor.eventloop.tasks.discarded` 与 `disruptor.eventloop.queue.segments.active`;`outstanding ≤ capacityLimit` 恒成立。
- [ ] **Step 2: 运行验证失败** → FAIL。
- [ ] **Step 3: 实现** — snapshot 加 `activeQueueSegments`/`discardedTasks`(record 构造签名变更允许);`allocatedQueueSegments` 保持物理保留总数;Group 聚合 sum;metrics binder 加 gauge;builder 暴露 `maxPooledSegments`(替换 Task 1.4 的内部常量)。两维度正交(completed/failed/cancelled 覆盖所有 accepted;returned/discarded 物理处置)。
- [ ] **Step 4: 运行验证通过** → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): thread discarded/segment observability through snapshot and metrics"`

---

## Task 5: 上游文档同步

**Files:** Modify `docs/disruptor-architecture-design.md`(第 193/195 行 Group/关闭)、`docs/superpowers/specs/2026-09-02-disruptor-concurrent-design.md`(`TaskAdmissionGate` 准入步骤)。

- [ ] **Step 1:** `disruptor-architecture-design.md`"Group gate 先于 child gate 获取准入 token"整段改为全局 `accepting` 协议;`shutdownNow` 描述对齐新三态。
- [ ] **Step 2:** `2026-09-02` 并发设计的 admission 步骤(owner lease/`AdmissionToken`/reservation)改为 packed word + 三阶段 + 从属 disposition。
- [ ] **Step 3: 提交** — `git commit -m "docs: sync upstream architecture docs with rewritten concurrent kernel"`

---

## Task 6: JMH 相对门槛 + 能力矩阵/验证刷新

**Files:** Modify `disruptor-benchmarks`(如需混合/多生产者基准)、`docs/commons-capability-matrix.md`、`docs/disruptor-concurrent-verification.md`。全量:`$MVN clean verify`。

- [ ] **Step 1:** 全仓 `$MVN clean verify` 全绿。
- [ ] **Step 2:** 同机 JMH 3 次取中位跑 bounded/unbounded + Commons profile;通过条件 = 相对比 `median(project)/median(Commons) ≥ 80%`(bounded、unbounded 各自),绝对值仅参考;分配 ≤10 B/op(只统计框架自身分配、预创建 Runnable,spec §7)。
- [ ] **Step 3:** 用实测数字刷新 `disruptor-concurrent-verification.md`(JMH 表 + 结论段)与 `commons-capability-matrix.md`(20/51/53 行:registry→扫描/index、槽复用池化、任务池动机已解决)。
- [ ] **Step 4: 提交** — `git commit -m "docs: refresh capability matrix and verification with measured throughput"`

---

## 执行顺序与依赖

Task 0(基线)→ Task 1(原子 cutover,唯一大提交)→ Task 2(关闭硬化)→ Task 3(Group)→ Task 4(可观测)→ Task 5(文档)→ Task 6(JMH/矩阵)。Task 1 是唯一跨多文件原子提交,其余各自独立绿。每个 Task 结束时该模块定向测试 + 未触及语义的既有测试必须绿。
