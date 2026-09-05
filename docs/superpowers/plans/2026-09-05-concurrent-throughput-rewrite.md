# Concurrent 吞吐内核重写 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `disruptor-concurrent` 执行/调度内核重写为槽位原生零分配架构,消除普通提交路径的每任务对象分配与 monitor 同步,同时不退化任何公开行为契约。

**Architecture:** 预分配 typed ring 槽即任务记录(ordinary 零分配);容量用 packed admission word 内的显式 `outstanding` 计数(单 CAS enter);统一物理所有权状态机 + 两种存储(ordinary 槽内 CAS / tracked-scheduled 外部记录 + tracked-only index);关闭在 core `WorkerSupervisor` 之上加从属 `TaskDispositionCoordinator`(RETURNING/DISCARDING 完成阶段);无界后端槽池化 + 发布代际 + 段生命周期锁;Group 用单个全局 `accepting` volatile。

**Tech Stack:** Java 21(VarHandle/packed AtomicLong/虚拟线程)、LMAX Disruptor 4.0.0(`RingBuffer<Cell>` + `BusySpinWaitStrategy`)、`disruptor-core`(`WorkerSupervisor`/`ShutdownBackend`/`ShutdownDeadline`)、JUnit 5、JMH 1.37。

**Spec:** `docs/superpowers/specs/2026-09-05-1438-concurrent-throughput-architecture-design.md`(executor 必须与本计划同读;计划的每个判定都源自该 spec 的对应 §)。

## Global Constraints

- 单一 `EventLoopKernel`;bounded/unbounded 只换 `TaskQueue`,不分叉 kernel/调度/生命周期(spec §3.3)。
- ordinary(`execute/tryExecute`)热路径**零堆分配**;Future/记录/index 只用于 `submit/schedule`(spec §3.2/§4.1)。
- 生命周期/mode/deadline/首因/termination 唯一权威是 core `WorkerSupervisor`;concurrent 不另造关闭生命周期状态机(spec §3.3/§4.8)。
- `EventLoopSnapshot` 硬约束 `outstanding ≤ capacityLimit` 恒成立(spec §3.3)。
- `execute()` 满容量 fail-fast 抛 `RejectedExecutionException`,不阻塞重试(spec §3.3/§4.4)。
- 生产代码禁止 `System.out/System.err`、`CallerRuns`、`DiscardPolicy`;默认 jar 无 `cn.wjybxx`(spec §7)。
- 每任务实现同 commit 带测试(TDD,先写复现/契约测试再实现);频繁提交;不用 `--no-verify`/禁用测试换绿。
- **本机测试命令**(mvn 不在 PATH;亦可 `/mvn-env` skill):
  ```bash
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
  $MVN -pl disruptor-concurrent -am -Dtest=<Class>[,<Class2>] -Dsurefire.failIfNoSpecifiedTests=false test
  ```
  全量:`$MVN -pl disruptor-concurrent -am test`;core 回归:`$MVN -pl disruptor-core -am test`。
- 删除类前全仓 grep 确认零引用;改动产生的孤儿 import/字段一并清理(外科式修改)。

## 文件结构与职责

| 文件 | 动作 | 职责 |
| --- | --- | --- |
| `internal/TaskAdmissionGate.java` | 重写 | packed `AtomicLong`(NEW/OPEN/CLOSED + activePublishers + outstanding);enter/leave/open/close/awaitDrained/outstanding,无 monitor |
| `internal/TaskQueue.java` | 重写接口 | claim/write*/publish/消费前缀游标/scan;消费前缀与 ordinary/tracked 槽释放时机写入契约 |
| `internal/BoundedTaskQueue.java` | 重写 | `RingBuffer<Cell>` typed 槽 + 双消费游标 + `BusySpinWaitStrategy` + `scanOrdinaryUnstarted` |
| `internal/UnboundedTaskQueue.java` | 重写 | 分段 typed 槽 + 发布代际 + 段生命周期锁 + `maxPooledSegments` + 独立 64 位 outstanding |
| `internal/AcceptedTask.java` | 重写 | 物理所有权状态机(含周期 RUNNING→WAITING 条件)+ `shutdownNowReturnValue`;只服务 tracked/scheduled |
| `internal/AcceptedTaskRegistry.java` | 重写 | tracked-only accepted index;有序枚举 + 记录 CAS 抢占 + `terminalizeAndRemove` |
| `internal/TaskDispositionCoordinator.java` | 新建 | NONE/RETURNING/RETURNED/DISCARDING/DISCARDED;RETURN 调用线程独占,DISCARD 异步+worker 协助 |
| `internal/EventLoopKernel.java` | 重写 admit/consume/shutdown | 编排 gate+queue+coordinator;类型分派;单写者指标;退出序列 |
| `internal/EventLoopShutdownBackend.java` | 改 | `stop(IMMEDIATE)` 保留 `kernel.stopWorker` + 登记 DISCARD + unpark |
| `internal/ScheduledTask.java`、`internal/CancellationMailbox.java` | 改 | 适配新 `AcceptedTask` 语义 |
| `internal/GroupLifecycleCoordinator.java` | 改 | 删每任务 owner lease,全局 `accepting` volatile |
| `EventLoopSnapshot.java`、`EventLoopGroupSnapshot.java` | 改 | 新增 `activeQueueSegments`、`discardedTasks` |
| `autoconfigure/DisruptorConcurrentMetrics.java` | 改 | 新增 `disruptor.eventloop.tasks.discarded` + 段 meter |
| `EventLoopBuilder.java` | 改 | 新增 `maxPooledSegments`(默认 8) |
| 删除 | `internal/AdmissionToken.java`、`internal/TaskReservation.java`、`internal/TaskEnvelope.java` |

删除类的职责已被吸收:permit→packed outstanding、reservation→直接 claim/publish、envelope→typed 槽 + tombstone。

---

## Task 1: TaskAdmissionGate — packed admission word

**Files:**
- Modify(重写): `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/TaskAdmissionGate.java`
- Test: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/TaskAdmissionGateTest.java`(重写)

**Interfaces:**
- Produces:
  ```java
  final class TaskAdmissionGate {
      static TaskAdmissionGate bounded(int capacity);   // capacity 需 >0 且 ≤ 2^31-1,fail-fast
      static TaskAdmissionGate unbounded();
      void open();                       // NEW→OPEN;CLOSED 时抛 IllegalStateException
      void closeForAdmissions();         // →CLOSED,幂等,永久
      boolean isAccepting();             // state==OPEN
      long tryEnter();                   // 成功返回 ticket 前的 outstanding 快照标记(≥0);拒绝返回 -1
                                          // bounded: 单 CAS 检查 OPEN && outstanding<capacity 后 activePublishers++、outstanding++
                                          // unbounded: 单 CAS 检查 OPEN 后 activePublishers++(outstanding 走独立计数,见 Task 6)
      void leave(boolean rollbackOutstanding);  // activePublishers--;rollbackOutstanding 则同一 CAS outstanding--
      void completeBatch(int n);         // worker 单写者:outstanding -= n(bounded)
      long outstanding();                // 快照读
      long activePublishers();
      void awaitDrained();               // spin-then-park 等 activePublishers==0;末位 leave 者 unpark
  }
  ```
- packed long 布局:`[63:62]` 生命周期、`[61:31]` outstanding(31 位)、`[30:0]` activePublishers(31 位)。掩码/移位不跨字段进位。

- [ ] **Step 1: 写失败测试(容量 off-by-one + 不泄漏)**
  ```java
  // internal/TaskAdmissionGateTest.java
  @Test
  void boundedEnterAdmitsExactlyCapacityAndOffByOneIsCorrect() {
      TaskAdmissionGate gate = TaskAdmissionGate.bounded(1);
      gate.open();
      assertTrue(gate.tryEnter() >= 0);        // 首个:outstanding 0<1
      assertEquals(1, gate.outstanding());
      assertEquals(-1, gate.tryEnter());       // 满容量拒绝(1<1 假)
      gate.completeBatch(1);                   // worker 释放
      assertEquals(0, gate.outstanding());
      assertTrue(gate.tryEnter() >= 0);        // 复位后可再进
  }

  @Test
  void leaveWithRollbackDoesNotLeakOutstandingEvenWhenClaimFailed() {
      TaskAdmissionGate gate = TaskAdmissionGate.bounded(4);
      gate.open();
      gate.tryEnter();                         // 模拟 enter 成功但后续 tryNext 失败
      gate.leave(true);                        // rollbackOutstanding=true
      assertEquals(0, gate.outstanding());
      assertEquals(0, gate.activePublishers());
  }

  @Test
  void closeThenAwaitDrainedReturnsAfterActivePublishersZero() throws Exception {
      TaskAdmissionGate gate = TaskAdmissionGate.bounded(4);
      gate.open();
      gate.tryEnter();                         // 一个在途 publisher
      gate.closeForAdmissions();
      assertEquals(-1, gate.tryEnter());       // CLOSED 后拒绝
      Thread t = new Thread(gate::awaitDrained); t.start();
      Thread.sleep(50); assertTrue(t.isAlive());  // 未 drain
      gate.leave(false);                        // publisher 归还
      t.join(1000); assertFalse(t.isAlive());
  }
  ```
- [ ] **Step 2: 运行验证失败** — `$MVN ... -Dtest=TaskAdmissionGateTest test` → FAIL(方法不存在/旧实现语义不符)。
- [ ] **Step 3: 实现 packed gate** — 用一个 `AtomicLong`,`compareAndSet` 循环实现 `tryEnter/leave/completeBatch`;`open/close` 用 CAS 改生命周期位;`awaitDrained` 用 `Thread.onSpinWait()` 自旋 N 次后 `LockSupport.park`,`leave` 在 `activePublishers` 归零且 CLOSED 时 `LockSupport.unpark(awaiter)`(记录 awaiter 引用)。位常量:`LIFECYCLE_SHIFT=62`、`OUTSTANDING_SHIFT=31`、`PUB_MASK=(1L<<31)-1`。
- [ ] **Step 4: 运行验证通过** — 同命令 → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): packed admission word gate"`

**验收补充**(同任务追加测试):`unbounded()` 的 `tryEnter` 不做容量门(始终 OPEN 时成功);`bounded` capacity `2^31` builder fail-fast;activePublishers 饱和拒绝;并发 4×N `tryEnter/completeBatch` 后 `outstanding` 与净提交数一致(jcstress 风格用 `CountDownLatch` + 多线程)。

---

## Task 2: BoundedTaskQueue — typed 槽 + 双消费游标 + scan

**Files:**
- Modify(重写接口): `internal/TaskQueue.java`
- Modify(重写): `internal/BoundedTaskQueue.java`
- Test: `internal/TaskQueueContractTest.java`(扩写 bounded 部分)

**Interfaces:**
- Consumes: 无(纯队列)。
- Produces:
  ```java
  enum TaskType { ORDINARY, TRACKED, SCHEDULE, TOMBSTONE }

  interface TaskQueue {
      long tryClaim();                              // 返回 seq;-1 表示物理满(bounded)
      void writeOrdinary(long seq, Runnable task);  // 写 type+state=WAITING+ordinary
      void writeTracked(long seq, AcceptedTask<?> record);
      void writeSchedule(long seq, AcceptedTask<?> record);
      void writeTombstone(long seq);
      void publish(long seq);                        // release 发布;含 tombstone
      // 单消费者游标(消费前缀):advance 到下一个连续已发布槽;无则 false
      boolean poll();
      TaskType currentType();
      Runnable currentOrdinary();
      AcceptedTask<?> currentRecord();
      void advanceConsumer();   // release 推进 consumerSequence(离开 ingress)
      void releaseCurrentSlot(); // 清槽引用 + 推进 gatingSequence(允许复用)
      long claimedCursor();      // 冻结扫描用
      long pending();            // claimedCursor - consumerSequence
      // 关闭枚举:遍历 [consumerNext, claimedInclusive] 内 ORDINARY 未开始槽
      void scanOrdinaryUnstarted(long claimedInclusive, OrdinarySink sink);
      int allocatedSegments();   // bounded 恒 0
      int activeSegments();      // bounded 恒 0
  }
  @FunctionalInterface interface OrdinarySink { void accept(long seq, Runnable original); }
  ```
- `Cell{ int type; int state; Runnable ordinary; AcceptedTask<?> tracked; }`,`state` 用 `VarHandle` CAS(`ORDINARY_STATE`)。物理状态常量与 `AcceptedTask.PhysicalState` 序号一致(Task 3 定义)。

- [ ] **Step 1: 写失败测试(消费前缀 + 槽释放时机)**
  ```java
  // internal/TaskQueueContractTest.java
  @Test
  void consumerStopsAtFirstUnpublishedHole() {
      BoundedTaskQueue q = new BoundedTaskQueue(8);
      long s0 = q.tryClaim(), s1 = q.tryClaim();
      q.writeOrdinary(s1, () -> {}); q.publish(s1);   // 先发布 s1,留 s0 空洞
      assertFalse(q.poll());                           // 消费者停在空洞前
      q.writeOrdinary(s0, () -> {}); q.publish(s0);
      assertTrue(q.poll());                            // s0 补齐后才可消费
      assertEquals(TaskType.ORDINARY, q.currentType());
  }

  @Test
  void ordinarySlotNotReusedUntilReleased() {
      BoundedTaskQueue q = new BoundedTaskQueue(1);     // 容量 1
      long s0 = q.tryClaim(); q.writeOrdinary(s0, () -> {}); q.publish(s0);
      assertTrue(q.poll()); q.advanceConsumer();        // 离开 ingress
      assertEquals(-1, q.tryClaim());                    // 槽未 release,生产者拿不到
      q.releaseCurrentSlot();                            // run+清槽后
      assertTrue(q.tryClaim() >= 0);                     // 现在可复用
  }
  ```
- [ ] **Step 2: 运行验证失败** — `-Dtest=TaskQueueContractTest` → FAIL。
- [ ] **Step 3: 实现** — `RingBuffer.createMultiProducer(Cell::new, capacity, new BusySpinWaitStrategy())`;`consumerSequence` 与 `gatingSequence` 两个 `Sequence`,只有 `gatingSequence` 作为 `addGatingSequences` 参与 `tryNext` 背压;`poll` 用 `isAvailable(consumerNext)` 判连续前缀;`releaseCurrentSlot` 清 `cell.ordinary/tracked=null` 后 `gatingSequence.set(seq)`;`scanOrdinaryUnstarted` 从 `consumerSequence.get()+1` 到 `claimedInclusive` 逐槽 `isAvailable` 校验后回调。
- [ ] **Step 4: 运行验证通过** — PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): typed slot bounded queue with dual consumer cursors"`

**验收补充**:4×1000 多生产者 `tryClaim/write/publish` + 单消费者 `poll`,断言每 seq 恰好消费一次、无丢失/无重复(移植现有 `TaskQueueContractTest.concurrentReservationsPublishEverySequenceExactlyOnce`);tombstone 槽被 `poll` 跳过;`pending()==claimedCursor-consumerSequence`。

---

## Task 3: 物理所有权状态机 + tracked-only index

**Files:**
- Modify(重写): `internal/AcceptedTask.java`、`internal/AcceptedTaskRegistry.java`
- Modify: `internal/ScheduledTask.java`、`internal/CancellationMailbox.java`(适配)
- Delete: `internal/AdmissionToken.java`、`internal/TaskReservation.java`、`internal/TaskEnvelope.java`
- Test: `internal/AcceptedTaskRegistryTest.java`(重写)、`internal/ScheduledTaskTest.java`(回归)

**Interfaces:**
- Produces:
  ```java
  final class AcceptedTask<V> {
      enum PhysicalState { WAITING, RUNNING, RETURNED, DISCARDED, CANCELLED_WAITING, TERMINAL }
      long acceptedSequence();
      EventLoopFutureTask<V> future();
      ScheduledTask<V> scheduledTask();       // null=非调度
      Runnable shutdownNowReturnValue();      // Runnable 源=原始 Runnable;Callable/spec 源=RunnableFuture/ScheduledFuture
      boolean tryStart();                     // WAITING→RUNNING
      boolean tryReturn();                    // WAITING→RETURNED
      boolean tryDiscard();                   // WAITING→DISCARDED
      boolean markCancelledWaiting();         // WAITING→CANCELLED_WAITING(仅用户/expires/max)
      boolean returnToWaiting(RearmCheck check); // RUNNING→WAITING 仅当 check 全条件满足
      boolean terminate();                    // *→TERMINAL(getAndSet)
      PhysicalState state();
  }
  @FunctionalInterface interface RearmCheck { boolean canRearm(); } // 周期&&未终态&&未quiescing/immediate&&未expires&&未达max&&失败策略允许

  final class AcceptedTaskRegistry {   // tracked-only index
      void register(AcceptedTask<?> task);           // 按 acceptedSequence 有序登记
      void terminalizeAndRemove(AcceptedTask<?> task); // no-throw:置 TERMINAL 并移除
      void remove(AcceptedTask<?> task);
      // 关闭枚举:按 ticket 升序对每个 WAITING 记录调用 action(返回是否抢占成功)
      void scanUnstarted(java.util.function.Predicate<AcceptedTask<?>> claim);
      int size();
  }
  ```
- Consumes: `EventLoopFutureTask`(不变)、`ScheduledTask`(改为持 `AcceptedTask` 引用不变)。

- [ ] **Step 1: 写失败测试(状态 CAS 互斥 + 有序枚举)**
  ```java
  // internal/AcceptedTaskRegistryTest.java
  @Test
  void waitingCanBeReturnedOrDiscardedButNotBoth() {
      AcceptedTask<Void> t = trackedTask(5);   // helper 建 seq=5 的 tracked 记录
      assertTrue(t.tryReturn());
      assertFalse(t.tryDiscard());             // 已 RETURNED,互斥
      assertEquals(AcceptedTask.PhysicalState.RETURNED, t.state());
  }

  @Test
  void scanUnstartedVisitsRecordsInAcceptedOrder() {
      AcceptedTaskRegistry idx = new AcceptedTaskRegistry();
      idx.register(trackedTask(7)); idx.register(trackedTask(3)); idx.register(trackedTask(5));
      List<Long> claimed = new ArrayList<>();
      idx.scanUnstarted(t -> { if (t.tryReturn()) { claimed.add(t.acceptedSequence()); return true; } return false; });
      assertEquals(List.of(3L, 5L, 7L), claimed);
  }
  ```
- [ ] **Step 2: 运行验证失败** — `-Dtest=AcceptedTaskRegistryTest` → FAIL。
- [ ] **Step 3: 实现 + 删除类** — `AcceptedTask.state` 用 `AtomicReference<PhysicalState>` 的 CAS;`returnToWaiting` 先 `check.canRearm()` 再 CAS RUNNING→WAITING;registry 用 `ConcurrentSkipListMap<Long,AcceptedTask<?>>`;全仓 grep 确认 `AdmissionToken/TaskReservation/TaskEnvelope` 仅剩本任务待删引用后 `git rm` 三个文件,清理 import。
- [ ] **Step 4: 运行验证通过** — `-Dtest=AcceptedTaskRegistryTest,ScheduledTaskTest` → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): physical ownership state machine and tracked-only index"`

**验收补充**:周期 `returnToWaiting` 在 quiescing/expires/max/非周期时返回 false 并允许 TERMINAL;`shutdownNowReturnValue` 对 `submit(Callable)` 返回其 `RunnableFuture`、对 `execute` 返回原始 `Runnable`(在 Task 4/5 端到端验证)。

---

## Task 4: EventLoopKernel — admit 重写 + 消费分派 + 单写者指标

**Files:**
- Modify: `internal/EventLoopKernel.java`(admit/submitOrdinary/processCommands/runOrdinary/指标)
- Test: `EventLoopExecutorContractTest.java`(既有,必须全绿)、新增 `EventLoopBackendContractTest` 相关断言、`disruptor-benchmarks` GC profile 手测

**Interfaces:**
- Consumes: Task 1 `TaskAdmissionGate`、Task 2 `TaskQueue`、Task 3 `AcceptedTask`/`AcceptedTaskRegistry`。
- Produces(kernel 内部):
  ```java
  // admit 三阶段(spec §4.4),ordinary 零分配:
  boolean tryExecute(Runnable);      // enter→claim→writeOrdinary→publish→leave;满容量 false
  void execute(Runnable);            // 同上;满容量抛 RejectedExecutionException
  <T> Future<T> submit(...);         // 建 EventLoopFutureTask+AcceptedTask,index.register,writeTracked
  // 指标:completed/failed/cancelled 覆盖所有 accepted task(含 ordinary),worker 单写者
  ```

- [ ] **Step 1: 确认既有契约测试覆盖 ordinary 指标口径**(spec §4.6/§7)。既有 `EventLoopExecutorContractTest.executeAndAllSubmitOverloadsUseOneWorkerInAcceptedOrder`(completedTasks==4 含 ordinary)、`nakedFailureUsesHandlerButSubmittedFailureStaysOnlyInFuture`(failedTasks==2 含 ordinary)必须保持通过——它们是本任务的失败/回归基线。先运行确认当前状态。
- [ ] **Step 2: 新增零分配快路径失败测试**
  ```java
  // EventLoopExecutorContractTest.java 追加
  @Test
  void ordinaryExecuteHoldsCapacityUntilRunCompletes() throws Exception {
      DisruptorEventLoop loop = runningLoop("cap-ordinary", 1);
      CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
      loop.execute(() -> { entered.countDown(); await(release); });
      assertTrue(entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertEquals(1, loop.snapshot().outstandingTasks());   // 运行中仍占容量
      assertFalse(loop.tryExecute(() -> {}));                // 满容量拒绝,零 tombstone
      assertThrows(RejectedExecutionException.class, () -> loop.execute(() -> {}));
      release.countDown();
      awaitCondition(() -> loop.snapshot().outstandingTasks() == 0);
  }
  ```
- [ ] **Step 3: 运行验证失败** — `-Dtest=EventLoopExecutorContractTest` → FAIL(旧 admit 仍分配/语义不符)。
- [ ] **Step 4: 重写 admit/消费** — ordinary 走 `gate.tryEnter→queue.tryClaim→writeOrdinary→publish→leave(false)`(三层 finally,spec §4.4);消费按 `currentType()` 分派:ORDINARY `run()` 后 `advanceConsumer`→`RUNNING→TERMINAL`→`releaseCurrentSlot`;TRACKED/SCHEDULE 读记录后可先 `advanceConsumer+releaseCurrentSlot`;指标 `completed/failed` 由 worker 对**所有** accepted task 单写者累计;每批 `gate.completeBatch(n)`。
- [ ] **Step 5: 唤醒协议 + 调度回归**(spec §4.7)——实现条件 unpark:worker park 前 `lazySet(parked=true)` 双检 queue/mailbox/到期 timer,只对未来 timer `parkNanos(nextTrigger-now)`、无 timer 无限 park;生产者仅 `parked==true` 时 unpark,tombstone 也 unpark。追加空载唤醒延迟测试:
  ```java
  @Test
  void idleWorkerWakesOnSubmitWithoutPolling() throws Exception {
      DisruptorEventLoop loop = runningLoop("wakeup", 16);
      Thread.sleep(50);                              // 让 worker 进入 park
      CountDownLatch ran = new CountDownLatch(1);
      loop.execute(ran::countDown);
      assertTrue(ran.await(500, TimeUnit.MILLISECONDS));   // 无固定轮询也快速唤醒
  }
  ```
  `EventLoopSchedulingTest`(四种调度、批次公平、`schedule(0)` 全序、priority)是消费循环回归,必须保持全绿。
- [ ] **Step 6: 运行验证通过** — `-Dtest=EventLoopExecutorContractTest,EventLoopBackendContractTest,EventLoopSchedulingTest` → PASS。
- [ ] **Step 7: GC 手测** — 构建 benchmarks 跑 `EventLoopBenchmark.boundedEventLoop -prof gc`,确认 bounded B/op ≤10;记录到本任务提交说明(非 CI 门槛,spec §7)。
- [ ] **Step 8: 提交** — `git commit -m "feat(concurrent): zero-alloc ordinary admit path and single-writer metrics"`

---

## Task 5: TaskDispositionCoordinator + 关闭三态 + 退出序列

**Files:**
- Create: `internal/TaskDispositionCoordinator.java`
- Modify: `internal/EventLoopKernel.java`(shutdown/requestShutdown/shutdownNow/退出序列)、`internal/EventLoopShutdownBackend.java`
- Test: `EventLoopShutdownTest.java`(既有全绿 + 新增并发/卡死用例)

**Interfaces:**
- Produces:
  ```java
  final class TaskDispositionCoordinator {
      enum State { NONE, RETURNING, RETURNED, DISCARDING, DISCARDED }
      boolean tryBeginReturning();   // NONE→RETURNING(单赢家)
      boolean tryBeginDiscarding();  // NONE→DISCARDING(单赢家;败给 RETURNING 返回 false)
      void failReturningToDiscarding(Throwable cause); // RETURNING→DISCARDING + 记 supervisor 首因
      void completeReturned();       // RETURNING→RETURNED
      void completeDiscarded();      // DISCARDING→DISCARDED
      State state();
      boolean isTerminal();          // RETURNED||DISCARDED
  }
  ```
- Consumes: `WorkerSupervisor.requestShutdown(mode,deadline)`、`AcceptedTaskRegistry.scanUnstarted`、`TaskQueue.scanOrdinaryUnstarted`。

- [ ] **Step 1: 写失败测试(并发不拆分 returned + 卡死取消 Future)**
  ```java
  // EventLoopShutdownTest.java 追加
  @Test
  void concurrentShutdownNowDoesNotSplitReturnedSet() throws Exception {
      DisruptorEventLoop loop = runningLoop("concurrent-now", 64);
      CountDownLatch block = new CountDownLatch(1);
      loop.execute(() -> await(block));                     // 占住 worker
      List<Runnable> queued = new ArrayList<>();
      for (int i = 0; i < 20; i++) { Runnable r = () -> {}; queued.add(r); loop.execute(r); }
      // 两个线程并发 shutdownNow
      var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
      var f1 = pool.submit(() -> loop.shutdownNow());
      var f2 = pool.submit(() -> loop.shutdownNow());
      List<Runnable> a = f1.get(2, TimeUnit.SECONDS), b = f2.get(2, TimeUnit.SECONDS);
      block.countDown();
      // 赢家拿到全部 20 个,败者空;两者并集恰好 20、无重复
      List<Runnable> union = new ArrayList<>(a); union.addAll(b);
      assertEquals(20, union.size());
      assertTrue(a.isEmpty() || b.isEmpty());
      pool.shutdownNow();
  }
  ```
- [ ] **Step 2: 运行验证失败** — `-Dtest=EventLoopShutdownTest` → FAIL。
- [ ] **Step 3: 实现** — 所有关闭入口先同步 `gate.closeForAdmissions()`;`shutdownNow`:`tryBeginReturning` 赢家立即 `supervisor.requestShutdown(IMMEDIATE)` 再 `gate.awaitDrained`→冻结 `claimedCursor`→`scanOrdinaryUnstarted`+`registry.scanUnstarted`(RETURNED 前先 `future.cancel`)→`completeReturned`→返回;败者返回空。`requestShutdown(IMMEDIATE)`/`stop(IMMEDIATE)`:`kernel.stopWorker(IMMEDIATE)`+`tryBeginDiscarding`+起虚拟线程执行者(取消未开始 Future、标 DISCARDED)+unpark;worker 退出序列:处置终态前不批量清未开始任务,终态后物理收敛。RETURNING 仅调用线程扫描,worker 不协助(spec §4.8 A1)。
- [ ] **Step 4: 运行验证通过** — `-Dtest=EventLoopShutdownTest` → PASS(既有 `shutdownNowReturnsWaitingOriginalRunnablesInAcceptedOrder`、`terminationWaitsForInterruptIgnoringTaskToReallyExit` 等全绿)。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): task disposition coordinator with return/discard phases"`

**验收补充**:graceful 保留 ordinary+one-shot、取消 waiting periodic;immediate 未开始一律 DISCARDED;worker 卡在忽略中断任务时未开始 Future 仍被异步取消(注入一个 `while(!interrupted-ignored)` 任务,断言另一排队 submit 的 Future 在超时内 `isCancelled()`)。

---

## Task 6: UnboundedTaskQueue — 池化 + 发布代际 + 段锁

**Files:**
- Modify(重写): `internal/UnboundedTaskQueue.java`
- Modify: `EventLoopBuilder.java`(`maxPooledSegments` 默认 8)
- Test: `internal/TaskQueueContractTest.java`(unbounded 部分)、`UnboundedEventLoopTest.java`(回归)

**Interfaces:**
- Produces: 实现 Task 2 的 `TaskQueue`;额外 `long outstanding()`(独立 64 位 `AtomicLong`,spec §4.3);`allocatedSegments()`=活跃+池、`activeSegments()`=活跃。
- 每槽 `long publishedSequence`(release/acquire 校验);段生命周期锁 `ReentrantLock segmentLock`。

- [ ] **Step 1: 写失败测试(段回收代际 + 独立 outstanding)**
  ```java
  // internal/TaskQueueContractTest.java
  @Test
  void unboundedRecyclesSegmentsAndValidatesGeneration() {
      UnboundedTaskQueue q = new UnboundedTaskQueue(4, 8);   // segmentSize=4, maxPooled=8
      for (int i = 0; i < 12; i++) { long s = q.tryClaim(); q.writeOrdinary(s, () -> {}); q.publish(s); }
      for (int i = 0; i < 12; i++) { assertTrue(q.poll()); q.advanceConsumer(); q.releaseCurrentSlot(); }
      assertTrue(q.activeSegments() <= 2);        // 跨段后回收,活跃段有界
      assertFalse(q.poll());                       // 无残留旧代际读出
  }

  @Test
  void unboundedReportsOutstandingViaSeparateCounter() {
      UnboundedTaskQueue q = new UnboundedTaskQueue(4, 8);
      long s = q.tryClaim(); q.writeOrdinary(s, () -> {}); q.publish(s);
      assertEquals(1, q.outstanding());
      assertTrue(q.poll()); q.advanceConsumer(); q.releaseCurrentSlot();
      q.completeBatch(1);
      assertEquals(0, q.outstanding());
  }
  ```
- [ ] **Step 2: 运行验证失败** — `-Dtest=TaskQueueContractTest` → FAIL。
- [ ] **Step 3: 实现** — 分段链表,每段 `Cell[] + long[] publishedSeq`;生产者按全局 seq 定位段/偏移、末尾 release 写 `publishedSeq[off]=seq`;消费者 acquire 校验 `publishedSeq[off]==expected`;跨段时 `segmentLock` 内 unlink+回收到空闲链表(超 `maxPooledSegments` 释放 GC);复用前清 payload/state。scanner 持 `segmentLock` 遍历。
- [ ] **Step 4: 运行验证通过** — `-Dtest=TaskQueueContractTest,UnboundedEventLoopTest` → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): pooled unbounded queue with publication generation"`

**验收补充**:双 scanner + worker 回收并发无 use-after-free(段锁串行化);`UnboundedEventLoopTest` 全绿;GC 手测 unbounded B/op ≤10。

---

## Task 7: Group 全局 accepting

**Files:**
- Modify: `internal/GroupLifecycleCoordinator.java`、`internal/EventLoopKernel.java`(child enter 多读 parent `accepting`)、`DisruptorEventLoopGroup.java`
- Test: `EventLoopGroupTest.java`、`EventLoopGroupShutdownTest.java`(既有全绿 + startup 窗口用例)

**Interfaces:**
- Produces: `GroupLifecycleCoordinator.isAccepting()`(单 volatile);child `enter` = 本地 gate.tryEnter 成功 **且** `parentAccepting`(独立 loop 恒真)。删除 `AdmissionLease` 相关方法。

- [ ] **Step 1: 写失败测试(startup 无提前提交窗口)**
  ```java
  // EventLoopGroupTest.java 追加
  @Test
  void childRejectsSubmissionUntilGroupGloballyAccepting() throws Exception {
      // 构造一个 child start 会阻塞在 barrier 的 Group;在 Group 未整体 open 前,已构造 child 拒绝提交
      // (用 EventLoopFactory 注入一个 start 时 await(latch) 的 module;取一个 child 引用直接 execute → 应抛 RejectedExecutionException)
      // ... 见 spec §4.5;断言 Group.start() 完成前 child.execute 被拒
  }
  ```
- [ ] **Step 2: 运行验证失败** — `-Dtest=EventLoopGroupTest` → FAIL。
- [ ] **Step 3: 实现** — `GroupLifecycleCoordinator` 用 `volatile boolean accepting`;全部 child RUNNING 后单点 `accepting=true`;关闭先 `accepting=false` 再逐 child `closeForAdmissions`+各自 `awaitDrained`;`EventLoopKernel.admit` 在 `gate.tryEnter` 后检查 `groupOwner==null || groupOwner.isAccepting()`,否则 `leave(true)` 并拒绝。删除每任务 lease 对象与其方法。
- [ ] **Step 4: 运行验证通过** — `-Dtest=EventLoopGroupTest,EventLoopGroupShutdownTest` → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): group admission via single global accepting flag"`

---

## Task 8: 可观测性贯穿(snapshot + Group + metrics)

**Files:**
- Modify: `EventLoopSnapshot.java`(+`activeQueueSegments`、`discardedTasks`)、`EventLoopGroupSnapshot.java`(+`discardedTasks` 聚合)、`autoconfigure/DisruptorConcurrentMetrics.java`
- Test: `DisruptorConcurrentAutoConfigurationTest` 或 metrics 测试、snapshot 不变量测试

**Interfaces:**
- Produces: snapshot 新字段;`disruptor.eventloop.tasks.discarded`、`disruptor.eventloop.queue.segments.active` meter。计数两维度正交(completed/failed/cancelled 覆盖所有 accepted;returned/discarded 物理处置)。

- [ ] **Step 1: 写失败测试** — 断言 immediate 关闭后 `snapshot().discardedTasks()>0`、`EventLoopGroupSnapshot` 聚合 child discarded、metrics 注册 `disruptor.eventloop.tasks.discarded`。
- [ ] **Step 2: 运行验证失败** → FAIL。
- [ ] **Step 3: 实现** — 加字段与 builder 校验(record 构造签名变更允许);`allocatedQueueSegments` 保持物理保留总数、新增 active;Group snapshot sum;metrics binder 加 gauge。保持 `outstanding ≤ capacityLimit` 校验。
- [ ] **Step 4: 运行验证通过** → PASS。
- [ ] **Step 5: 提交** — `git commit -m "feat(concurrent): thread discarded/segment observability through snapshot and metrics"`

---

## Task 9: 上游文档同步

**Files:**
- Modify: `docs/disruptor-architecture-design.md`(第 193/195 行 Group/关闭)、`docs/superpowers/specs/2026-09-02-disruptor-concurrent-design.md`(`TaskAdmissionGate` 准入步骤)

- [ ] **Step 1:** 把 `disruptor-architecture-design.md` 的"Group gate 先于 child gate 获取准入 token"整段改为全局 `accepting` 协议;`shutdownNow` 描述对齐新三态。
- [ ] **Step 2:** 把 `2026-09-02` 并发设计的 admission 步骤(owner lease/`AdmissionToken`/reservation)改为 packed word + 三阶段 + 从属 disposition。
- [ ] **Step 3: 提交** — `git commit -m "docs: sync upstream architecture docs with rewritten concurrent kernel"`

---

## Task 10: JMH 相对门槛 + 能力矩阵/验证刷新

**Files:**
- Modify: `disruptor-benchmarks`(如需新增混合/多生产者基准)、`docs/commons-capability-matrix.md`、`docs/disruptor-concurrent-verification.md`
- 全量回归:`$MVN test`(全仓)

- [ ] **Step 1:** 全仓 `$MVN clean verify`,确认既有 273 测试(语义不变部分)+ 新增契约测试全绿。
- [ ] **Step 2:** 同机 JMH 3 次取中位跑 bounded/unbounded + Commons profile,记录相对比 `median(project)/median(Commons)`;通过条件 ≥80%(绝对值仅参考,spec §7)。
- [ ] **Step 3:** 用实测数字刷新 `disruptor-concurrent-verification.md`(JMH 表 + 结论段)与 `commons-capability-matrix.md`(第 20/51/53 行机制描述:registry→扫描/index、槽复用池化、任务池动机已解决)。
- [ ] **Step 4: 提交** — `git commit -m "docs: refresh capability matrix and verification with measured throughput"`

---

## 执行顺序与依赖

Task 1→2→3 是并行基元(gate/queue/记录),3 依赖 2 的 `TaskType`/`Cell.state` 序号约定;Task 4 依赖 1/2/3;Task 5 依赖 4;Task 6 依赖 2/4(复用同 kernel 消费);Task 7 依赖 4/5;Task 8 依赖 5/6;Task 9/10 收尾。每个 Task 结束时该模块定向测试 + 未触及语义的既有测试必须绿。
