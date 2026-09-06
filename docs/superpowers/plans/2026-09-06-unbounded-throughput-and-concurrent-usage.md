# Unbounded 吞吐与 Concurrent 使用体验 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变公开 API、不削弱 accepted 全序、全生命周期容量和精确 `shutdownNow()` 所有权契约的前提下，把 unbounded 普通任务数据面改为无显式锁结构并达到可验证的吞吐目标；同时补齐可直接运行的 pure Java、Spring example 和 tutorial。

**Architecture:** bounded 继续使用 LMAX ring 并作为业务入口默认选择；unbounded 由 `TaskAdmissionGate` 与 `UnboundedTaskQueue` 共享 `UnboundedTaskLedger`，claim 即 admitted total，worker 批量推进 completed，异常路径记录 rollback，移除每任务独立 outstanding CAS。分段链以 CAS 链接/推进，唯一消费者回收完整段，绝对 publication sequence 提供代际隔离，固定容量无锁池负责复用；scanner 只在 gate 封口、publisher drain、worker 停止取新任务后的静默链上运行。生产代码不保留 locked/lock-free 用户开关，`TaskQueue` 是内部替换边界。

**Tech Stack:** Java 21（`VarHandle`、`AtomicLong`、`AtomicReferenceArray`、`LockSupport`）、LMAX Disruptor 4.0.0、Spring Boot、JUnit 5、JMH 1.37、Maven 3.9.9。

**Spec:** `docs/superpowers/specs/2026-09-06-unbounded-throughput-and-concurrent-usage-design.md`（执行者必须与本计划同读）。

## 全局约束

- 不新增或修改公开并发 API；不让用户选择 locked/lock-free 算法。
- 单一 `EventLoopKernel`、scheduler、lifecycle 和 shutdown 协议；bounded/unbounded 只替换内部存储与账本。
- ordinary、tracked、scheduled 保持唯一 accepted sequence 全序；消费者不得越过 publication hole。
- capacity 继续表示 accepted-but-not-cleaned 的全生命周期任务数；unbounded 的 snapshot 允许并发近似，但不得为负，终止后必须精确归零。
- `shutdownNow()` 仍只返回本调用取得所有权的未开始原始任务；running task 不返回；并发调用不拆分返回集合。
- ordinary 热路径分配继续 `<= 10 B/op`，不通过 envelope、节点或每任务 Future 换吞吐。
- release publication 与 acquire consumption 不降级为 opaque/plain；`park/unpark` 首轮不改，只有 burst/idle 数据证明瓶颈后才单独设计。
- 测试先于实现；删除绑定 `segmentLock` 的白盒断言时，必须用覆盖相同风险的行为测试替代，测试数量和语义不得弱化。
- 每个 Task 独立测试通过后提交；不使用 `--no-verify`、跳过测试或放宽断言换绿。
- Maven 命令使用 `/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn`，并设置 `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home`。

## Task 1：建立可归因的基准与契约红线

**Files:**

- Create: `disruptor-benchmarks/src/main/java/com/sstlfsj/disruptor/concurrent/internal/UnboundedTaskQueueBenchmark.java`
- Modify: `disruptor-benchmarks/src/main/java/com/sstlfsj/disruptor/benchmark/EventLoopBenchmark.java`
- Modify: `disruptor-benchmarks/src/test/java/com/sstlfsj/disruptor/benchmark/EventLoopBenchmarkSmokeTest.java`
- Modify: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/TaskQueueContractTest.java`
- Modify: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopParkingTest.java`

- [ ] **Step 1：先读实现再写基准。** 核对 `TaskQueue`、`UnboundedTaskQueue`、`WorkerWakeup` 和 `EventLoopKernel.processCommands()` 的实际消费/释放顺序；基准直接调用真实队列，不复制生产算法。
- [ ] **Step 2：增加 queue-level JMH。** 分开测量同段 `claim/write/publish/poll/release`、每次跨段、段复用；提供 1/2/4/8 producer group，固定预创建 `Runnable`，参数化 `segmentSize` 与 `maxPooledSegments`。吞吐单位统一 ops/s，并用 `-prof gc` 报告分配。
- [ ] **Step 3：补 kernel-level 场景。** 保留现有持续提交基准，新增 burst-drain、idle-wakeup、1/2/4/8 producer；对每个基准预热后校验 accepted 数与 completed 数相等，防止把拒绝或未排空当吞吐。
- [ ] **Step 4：扩充行为测试。** 在 `TaskQueueContractTest` 增加 1/2/4/8 producer 跨段乱序发布、代际复用、落后 producer 与 head 推进交错；在 `EventLoopParkingTest` 增加空闲到首任务、虚假唤醒、publication 后不丢通知。现阶段测试应通过现实现，作为重构基线。
- [ ] **Step 5：运行测试与三轮基线。**
  ```bash
  $MVN -pl disruptor-concurrent,disruptor-benchmarks -am test
  $MVN -pl disruptor-benchmarks -am package -DskipTests
  java -jar disruptor-benchmarks/target/benchmarks.jar '.*(EventLoopBenchmark|UnboundedTaskQueueBenchmark).*' -wi 5 -i 5 -f 3 -prof gc
  ```
  保存原始结果到临时工作记录，不在生产代码长期保留旧队列副本。
- [ ] **Step 6：提交。** `git commit -m "bench(concurrent): isolate unbounded queue and wakeup costs"`

## Task 2：修复 claim 异常窗口并引入共享无界账本

**Files:**

- Create: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/UnboundedTaskLedger.java`
- Create: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/UnboundedTaskLedgerTest.java`
- Modify: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/TaskAdmissionGate.java`
- Modify: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/UnboundedTaskQueue.java`
- Modify: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/EventLoopKernel.java`
- Modify: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/WorkerWakeup.java`
- Modify: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/TaskAdmissionGateTest.java`
- Modify: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/WorkerWakeupTest.java`

**Internal protocol:**

```java
final class UnboundedTaskLedger {
    long candidateSequence();
    boolean tryCommitClaim(long expectedSequence);
    void rollbackClaim();
    void completeBatch(int count);
    long claimedCursor();
    long outstanding();
}
```

- [ ] **Step 1：写 ledger 失败测试。** 覆盖并发 claim 唯一连续、rollback 只减一次、single-writer batch completion、并发 snapshot 不为负、终止归零、`Long.MAX_VALUE` 前 fail-fast 且不回绕。
- [ ] **Step 2：写 claim 分配失败测试。** 给 `UnboundedTaskQueue` 增加仅包内测试可用的 segment factory 注入点；让扩段第一次抛出异常，断言 `claimedCursor` 未推进、重试后 sequence 连续、消费者无永久 hole。该注入点不得进入公开 builder。
- [ ] **Step 3：实现账本与可恢复 claim。** `UnboundedTaskQueue.tryClaim()` 循环执行“读取 candidate → 确保目标 segment → CAS 提交 claim”；所有可能抛错的分配发生在提交 sequence 前。CAS 失败表示其他 producer 已推进，重新定位后重试。
- [ ] **Step 4：把 unbounded gate 接到共享 ledger。** bounded packed word 完全不改；unbounded `tryEnter()` 只登记 active publisher，成功 claim 才进入 ledger。`WorkerWakeup.finishAdmission(...)` 必须区分“已预留 bounded capacity”和“已提交 unbounded claim”，异常路径只 rollback 自己实际拥有的账本项。
- [ ] **Step 5：worker 用 ledger 批量完成。** `EventLoopKernel` 继续在槽物理清理后调用 `completeBatch`；unbounded 委托 ledger，bounded 仍走 packed outstanding。删除 `TaskAdmissionGate` 内独立 `unboundedOutstanding`，更新其白盒测试为账本行为测试。
- [ ] **Step 6：运行定向与全模块测试。**
  ```bash
  $MVN -pl disruptor-concurrent -am -Dtest=UnboundedTaskLedgerTest,TaskAdmissionGateTest,TaskQueueContractTest,WorkerWakeupTest,EventLoopShutdownTest -Dsurefire.failIfNoSpecifiedTests=false test
  $MVN -pl disruptor-concurrent -am test
  ```
- [ ] **Step 7：运行 Task 1 基准并记录单项增益。** bounded 回退不得超过 5%；若 unbounded 退化，先定位账本 CAS/快照，不进入下一 Task。
- [ ] **Step 8：提交。** `git commit -m "perf(concurrent): unify unbounded claim and capacity ledger"`

## Task 3：无显式锁分段数据面与 O(1) 代际复用

**Files:**

- Modify: `disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/UnboundedTaskQueue.java`
- Modify: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/internal/TaskQueueContractTest.java`
- Modify: `disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/UnboundedEventLoopTest.java`

- [ ] **Step 1：先写失败测试。** 新增：旧槽为 `RETURNED`/`DISCARDED`/`TERMINAL` 后整段复用仍只识别新绝对 sequence；两个 producer 同时扩同一目标段只链接一个 segment；落后 producer 持有早期 sequence 时消费者不能回收错误代际；池满交给 GC 后 `allocated >= active >= 1`；稳定链上的双 scanner 仍只有一个 ordinary ownership 赢家。
- [ ] **Step 2：移除逐 Cell reset。** `Segment` 首次构造初始化 Cell；复用只改 segment id、`prev`、`next` 和边界元数据。worker 的 `releaseCurrentSlot()` 负责清除 payload 引用；producer 在 release publication 前完整覆盖 type/state/payload；`publishedSequence` 永不回写哨兵。
- [ ] **Step 3：实现 CAS segment chain。** producer 从已校验 id 的 segment 定位目标，缺段时创建/取池并 CAS 到 `next`，失败者归还未使用 segment 并沿赢家链接继续；协助 CAS 推进 `tail`。每个缓存/前后指针命中必须验证 segment id。
- [ ] **Step 4：实现唯一消费者回收。** 只有整段最后一个槽已消费并 `releaseCurrentSlot()` 后才能 CAS/单写推进 `head`；旧段断开后放入固定容量 `AtomicReferenceArray<Segment>`，producer 通过 `getAndSet(null)` 取得唯一所有权。池扫描固定上限，失败只放弃复用，不等待。
- [ ] **Step 5：把 scanner 限定为静默期只读。** 删除 `segmentLock`、`ReentrantLock` 和 `ArrayDeque`；scanner 不改变链和游标。保留 kernel 顺序：关闭 admission → 等 active publishers 归零 → disposition 阻止 worker 获取下一任务 → 扫描。将原 `unboundedDelayedProducerLookupIsProtectedBySegmentLifecycleLock` 重写为同风险的行为交错测试。
- [ ] **Step 6：运行压力测试。** 每个 1/2/4/8 producer 用不同随机 seed 重复跨段至少 100 次；校验 sequence 严格递增、任务不重不漏、引用最终释放、segment 统计不变量和 shutdown ownership。
- [ ] **Step 7：运行全模块测试和 queue/kernel JMH。** 达标线：相对 Task 1 unbounded 基线至少提升 20%，达到同次 bounded 的 95% 以上，分配 `<=10 B/op`；若未达标，使用 JFR/async-profiler 只对剩余热点做下一 Task，不能降低内存序或关闭契约。
- [ ] **Step 8：提交。** `git commit -m "perf(concurrent): make unbounded segment data path lock-free"`

## Task 4：证据驱动的最后一轮热路径优化

**Files:** 只修改 Task 3 profile 中占比明确且与本模块有关的文件；预期候选为 `UnboundedTaskQueue.java`、`TaskAdmissionGate.java`、`WorkerWakeup.java`、`EventLoopKernel.java`，但禁止无证据全改。

- [ ] **Step 1：分别采集持续、MPSC、burst/idle profile。** 报告成功 CAS/失败 CAS、段分配/复用、park/unpark 次数、空闲到首任务 p50/p99、`processCommands` 批大小；持续吞吐不能用来推断唤醒成本。
- [ ] **Step 2：只选择一个最大且可安全消除的热点。** 若是重复 segment lookup，加入带 segment-id 校验的 producer cursor；若是 tail 争用，借鉴 XADD 的独占轮转但不得预留不可回滚的 accepted sequence；若是 wakeup，另写 notification coalescing 设计与丢唤醒测试后再改。三者不在同一提交混改。
- [ ] **Step 3：为选中的热点先增加失败基准断言或行为测试，再实施最小修改。** release/acquire publication、ordinary state CAS、disposition ownership 不作为可放宽项。
- [ ] **Step 4：复测三类负载。** bounded 回退 `<=5%`；unbounded 满足 Task 3 门槛且不存在 2/4/8 producer 或 idle p99 结构性退化。只要 profile 仍显示可归因的模块成本且修改不增加契约风险，重复本 Task；不以达到 Commons 某个百分比为停止理由。
- [ ] **Step 5：每个独立优化单独提交。** 提交信息写明被消除的热点，例如 `perf(concurrent): cache validated producer segment cursor`。

## Task 5：补齐可运行的 concurrent example

**Files:**

- Create: `disruptor-spring-boot-example/README.md`
- Create: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/nospring/PureJavaConcurrentExample.java`
- Create: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/concurrent/ConcurrentDemoRunner.java`
- Modify: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/DemoResults.java`
- Modify: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/concurrent/OrderEventLoopService.java`
- Modify: `disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/concurrent/ConcurrentExampleTest.java`
- Create: `disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/nospring/PureJavaConcurrentExampleTest.java`

- [ ] **Step 1：写两个失败 smoke test。** pure Java main 在有限时间内完成并确认 bounded rejection、context、schedule cancellation、graceful termination；Spring runner 确认单 loop、group affinity、fixed-rate cancellation 都真实执行，容器关闭后所有 worker 真实终止。
- [ ] **Step 2：实现 pure Java 入口。** 展示显式 `start()`、bounded 作为默认、`tryExecute` fail-fast、仅在内部突发场景创建 unbounded、`TaskContext`、one-shot/dynamic schedule、`CancellationSource`、`shutdown()`/`awaitTermination()`；`shutdownNow()` 用独立短例展示返回边界，不制造泄漏线程。
- [ ] **Step 3：实现 Spring runner。** 复用现有 configuration/service，不复制业务逻辑；结果写入线程安全 `DemoResults` 供测试检查。日志统一 SLF4J，不使用 `System.out/System.err`。
- [ ] **Step 4：写 example README。** 给出两条可复制运行命令、预期结果、bounded/unbounded、loop/group、execute/submit/schedule 的场景表；代码片段从真实类引用，避免不可编译伪代码。
- [ ] **Step 5：运行模块测试并提交。**
  ```bash
  $MVN -pl disruptor-spring-boot-example -am test
  git commit -m "docs(example): add runnable concurrent usage paths"
  ```

## Task 6：tutorial 用 EventLoop 表达单发布者入口

**Files:**

- Modify: `disruptor-spring-boot-tutorial/pom.xml`
- Modify: `disruptor-spring-boot-tutorial/src/main/resources/application.yml`
- Modify: `disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/config/MatchConfig.java`
- Modify: `disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/ingress/MatchingOrderIngress.java`
- Modify: `disruptor-spring-boot-tutorial/src/test/java/com/sstlfsj/disruptor/tutorial/ingress/MatchingOrderIngressTest.java`
- Modify: `disruptor-spring-boot-tutorial/src/test/java/com/sstlfsj/disruptor/tutorial/MatchingFlowTest.java`
- Modify: `disruptor-spring-boot-tutorial/src/test/java/com/sstlfsj/disruptor/tutorial/BackpressureTest.java`
- Modify: `README.md`

- [ ] **Step 1：先写生命周期与线程所有权失败测试。** Runtime 和 EventLoop 都启动后入口才接受；停止先封入口、排空 EventLoop、再停止 Runtime；相同 symbol 的并发 HTTP 请求最终只由固定 EventLoop worker 调用 `MatchingPipeline.tryPublish`；拒绝仍同步返回且不偷偷重试。
- [ ] **Step 2：引入 concurrent 依赖与 bounded bean。** 使用最小显式配置创建撮合 ingress EventLoop，容量与 ring buffer 一致或由已有 matching 配置派生；不新增隐藏 singleton，不使用 unbounded 兜住 HTTP 流量。
- [ ] **Step 3：替换手写 `ThreadPoolExecutor`。** `MatchingOrderIngress` 提交到 bounded EventLoop 并等待结果，保持“确定入环/确定未入环”语义；`SINGLE` RingBuffer 的唯一 publisher 仍是 EventLoop worker。删除旧 executor、queue 和对应 shutdown 代码产生的孤儿 import。
- [ ] **Step 4：明确 phase。** 入口开放发生在 Runtime 与 EventLoop 运行之后；关闭时入口先拒绝新请求，EventLoop 排空后 Runtime 才终止。若现有 `DisruptorConcurrentLifecycle` phase 已能表达顺序，仅配置并测试，不新增 owner 抽象。
- [ ] **Step 5：更新根 README。** 增加选择表与导航：外部入口默认 bounded，unbounded 只用于有独立资源预算的内部控制流；说明 EventLoop 是任务所有权/串行化，Disruptor pipeline 是事件流水线，两者可组合但不互相替代。
- [ ] **Step 6：运行 tutorial 与全仓测试并提交。**
  ```bash
  $MVN -pl disruptor-spring-boot-tutorial -am test
  $MVN clean verify
  git commit -m "docs(tutorial): teach bounded event-loop ingress"
  ```

## Task 7：最终性能、正确性与文档验收

**Files:**

- Modify: `docs/disruptor-concurrent-verification.md`
- Modify: `docs/commons-capability-matrix.md`
- Modify: `docs/superpowers/specs/2026-09-06-unbounded-throughput-and-concurrent-usage-design.md`（只写最终已验证事实，不改既定契约）

- [ ] **Step 1：全仓干净验证。** `$MVN clean verify`，记录 concurrent、example、tutorial 测试数；任何原测试减少都要解释并补等价覆盖。
- [ ] **Step 2：三轮同机 JMH 取中位。** 同时跑项目 bounded、项目 unbounded、固定 Commons 和 queue-level 细分；统一 JVM、fork、warmup、measurement、producer 数和 in-flight。报告 ops/s、B/op、burst drain、idle p99，不只报告单个峰值。
- [ ] **Step 3：核对完成门槛。** unbounded 比 11,639,896 基线提升至少 20%，达到同次 bounded 95% 以上，ordinary 分配 `<=10 B/op`，bounded 回退 `<=5%`，MPSC 与 idle 无结构性退化。未达到时回到 Task 4，不修改验收线。
- [ ] **Step 4：刷新验证和能力矩阵。** 写入硬件/JVM/命令、原始中位数、相对值、适用场景与剩余限制；Commons 作为参照而非上限。
- [ ] **Step 5：代码审查。** 独立检查内存序、claim/rollback、段回收、scanner 静默前提、shutdown ownership、示例线程泄漏和文档可执行命令；修复后重跑受影响测试。
- [ ] **Step 6：提交。** `git commit -m "docs: verify lock-free unbounded throughput and usage"`

## 执行顺序与并行边界

Task 1 → Task 2 → Task 3 → Task 4 → Task 7 是核心依赖链。Task 5 可在 Task 2 后由独立 worker 并行完成；Task 6 依赖现有公开 API，不依赖 Task 3 内部结构，也可与 Task 3/4 并行。核心并发 Task 使用 `gpt-6-astra` 高推理；example/tutorial 使用 `gpt-5.6-terra`；每个实现 Task 完成后使用独立审查 worker，不由实现者自审代替。
