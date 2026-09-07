# Unbounded 吞吐与 Concurrent 使用体验设计

## 1. 目标

本专项同时解决两个问题：

1. 在不改变公开 API、不削弱并发与关闭契约的前提下，提高 `disruptor-concurrent` 的 unbounded 持续吞吐与多生产者扩展性。Commons 只作为同机参照，不是性能上限。
2. 让使用者通过可直接运行的 example 和 tutorial，快速判断何时使用 bounded、unbounded、单 EventLoop 或 EventLoopGroup，并正确处理启动、调度、取消和关闭。

性能优化必须保留以下不变量：

- ordinary、tracked、scheduled 共用唯一 accepted 全序；
- bounded/unbounded 共用唯一 `EventLoopKernel`、scheduler、lifecycle 和 shutdown 协议；
- capacity 表示 accepted-but-not-cleaned 的全生命周期任务数；
- `shutdownNow()` 只返回由本次调用取得所有权的未开始原始任务，绝不返回 running task；
- `WorkerSupervisor` 仍是真实 termination、deadline 和首因的唯一权威；
- publication hole 不能被消费者越过，段复用不能产生旧代误读或 use-after-recycle；
- ordinary 热路径继续保持实测低分配，不通过任务包装换吞吐。

## 2. 当前证据

Task 6 的干净机器结果为：bounded 14,867,520 ops/s，unbounded 11,639,896 ops/s，Commons 对应为 18,589,910/18,381,694 ops/s；本项目分配率为 bounded 0.004 B/op、unbounded 1.607 B/op。分配问题已经解决，剩余缺口是同步与队列路径成本。

当前 unbounded 普通提交至少经过：admission CAS、独立 outstanding CAS、sequence fetch-add、publication release、publisher leave CAS；消费端还要 acquire publication、ordinary 状态 CAS、逐任务指标写和批量 outstanding CAS。与 bounded 相比，unbounded 有一条独立的共享 outstanding 原子链。

当前 `UnboundedTaskQueue` 同段快路径本身不持 `segmentLock`。锁主要出现在扩段、消费者换段、池化、scanner 和快照，因此不能把单生产者 JMH 的全部差距简单归因于“段锁竞争”。现有短采样也只确认 `Segment.reset`、`segmentForConsumer` 可见，尚不足以给出 CPU 周期占比结论。

当前段复用会遍历整段，把每个 Cell 的 type、state、payload 和 volatile `publishedSequence` 全部重置。由于发布已经使用绝对 sequence，旧代 publishedSequence 与新代期望值天然不相等，这部分逐槽清零是可消除的重复工作。

当前 example 虽有 Spring Bean 和覆盖多项 API 的 service/test，但没有用户可运行入口；tutorial 的撮合入口使用 `ThreadPoolExecutor`，没有依赖或教授 `disruptor-concurrent`。

## 3. 行业与标准参照

- [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor/blob/master/src/docs/asciidoc/en/disruptor.adoc)：沿用预分配、claim/publish 分离、绝对 sequence、单写者和批处理；不采用会预留大段 accepted sequence 的 producer batching，因为慢生产者会扩大 publication hole 和 shutdown 责任。
- [JCTools 队列选择指南](https://github.com/JCTools/JCTools/wiki/Getting-Started-With-JCTools)：分段无界队列通过 chunk、XADD 和池化在吞吐、占用和分配间取舍；XADD 变体适合作为多生产者扩段竞争的升级参照。
- [JCTools MPMC XADD 实现](https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/MpmcUnboundedXaddArrayQueue.java)：借鉴独占段轮转、绝对代际和池化思想，但不复制其 Queue 语义，因为本项目还承担 accepted 全序、tracked/scheduled 和精确 shutdown 所有权。
- [Java 21 VarHandle](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/invoke/VarHandle.html)：payload/type/state 的 plain 写必须发生在 publishedSequence 的 release 写之前，worker/scanner 只有 acquire 读到同一绝对 sequence 后才能读取 payload。
- [Java 21 LockSupport](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/locks/LockSupport.html)：park/unpark 必须由 volatile/atomic 状态控制并循环复查，permit 不累计，不能用一次空队列观察替代握手。

### 3.1 必要性与使用场景结论

无界队列不是一般业务入口的默认答案。LMAX Disruptor 本身选择预分配有界 ring；JCTools 也明确建议优先选择有界或可增长但有上限的队列，因为真正无界只把背压问题转化为内存风险。因此文档和 example 必须把 bounded 放在默认位置。

unbounded 仍有真实且不可被 bounded 完全替代的场景：

- EventLoop 之间的内部控制命令或完成通知，调用方不能阻塞，也不能通过 caller-runs 改变线程所有权；
- shutdown、取消、定时重排等内部协作中，拒绝可能形成控制面死锁；
- 峰值可持续时间很短、上游已经有独立资源预算，但无法给单个 EventLoop 预先分配合理固定容量；
- 需要保留 accepted 全序和精确 shutdown 所有权，同时允许瞬时积压跨越单个预分配 ring。

以下场景不应使用 unbounded：外部 HTTP/MQ 入口、用户流量缓冲、慢 I/O 消费、无法估算最长积压时间、没有内存/延迟告警的服务。它们应使用 bounded + `tryExecute`/拒绝结果把背压返回上游。

对单生产者、持续有任务的常态负载，完整无锁段轮转不一定比“短临界区 + O(1) 复用”显著更快；但公开的 unbounded 同时承诺 MPSC，多生产者跨段正是显式锁会放大的场景。为避免先保留结构锁、随后再做第二次不兼容的内部重写，本专项直接以“无显式锁的数据面”作为最终结构，同时用基准确认其是否带来实际收益。这里的无锁是进度性质：一个 CAS 失败意味着其他线程完成了推进；不承诺 wait-free，也不把对象分配、GC 或用户任务执行称为无锁。

## 4. 候选方案

### 4.1 方案 A：仅优化现有段锁与 reset

保留独立 unbounded outstanding 计数和现有段结构，只把复用改为 O(1)，缩短锁持有时间并改善落后生产者定位。

优点是改动集中、风险最低；缺点是每任务额外 outstanding 原子操作仍存在，难以解释和消除 unbounded 与 bounded 的稳定差距。它适合作为首个可独立测量提交，不足以作为专项终点。

### 4.2 方案 B：共享无界账本 + 无锁段数据面 + 静默期扫描

为 unbounded queue 与 gate 注入同一个内部账本：sequence claim 同时成为 admitted total 的唯一来源，worker 物理完成只推进 single-writer completed total，失败 claim 通过稀有 rollback total 扣除。`outstanding = claimed - completed - rolledBack`，正常提交不再单独 CAS unbounded outstanding。

段复用只更新 segment id/link，不清零 publishedSequence；新写入完整覆盖 type/state/payload，再以绝对 sequence release 发布。生产者通过 CAS 链接新段并推进 tail，唯一消费者推进 head；空闲段进入固定槽位的无锁池。shutdown scanner 只在 gate close、active publishers drain、disposition 阻止 worker 继续取任务后遍历稳定链，不参与热路径同步。

优点是一次完成最终段结构，直接消除每任务额外记账和跨段显式锁，同时保持单一 kernel 和所有权模型；风险集中在派生 outstanding 的读取一致性、claim 异常窗口、CAS 轮转和段回收证明。该方案为推荐方案。

### 4.3 方案 C：bounded 主环 + unbounded overflow 双层队列

常态进入预分配 ring，满时进入分段 overflow，再按 accepted sequence 合并消费。

该方案可能让低积压吞吐接近 bounded，但会形成双存储、多路 publication hole 和更复杂的 shutdown scanner；它把当前一个无界问题变成合并排序问题，不符合最小架构原则，因此不采用。

### 4.4 是否提供有锁/无锁可替换策略

技术上可以在 `UnboundedTaskQueue` 下再抽象 `SegmentCoordinator`，提供 locked 与 lock-free 两个实现，并由 builder 选择。但本项目不把它做成公开或长期生产配置，原因如下：

- 使用者真正需要选择的是 bounded/unbounded 语义，不应再判断底层并发算法；
- 两个实现必须永久覆盖 publication hole、段复用、scanner、shutdown ownership、snapshot 和 MPSC 交错，维护成本接近两套无界队列；
- 同一公开语义在不同配置下出现不同吞吐、延迟和故障边界，会增加运维不确定性；
- `TaskQueue` 已经是 package-private 的算法替换边界，未来替换不需要改公开 API；
- 为单次 A/B 实验保留永久抽象违反当前项目的最小实现原则。

实施期间允许在 benchmark/test 源集保留有锁基线，或通过包内测试工厂注入旧实现，以便同进程、同负载对比。性能和正确性结论确定后删除临时策略层，生产代码只保留胜出的无锁实现。若未来在另一架构/JVM 上出现有证据的回归，再新增第二个 `TaskQueue` 实现并在内部工厂选择；不提前暴露用户开关。

## 5. 推荐架构

### 5.1 组件边界

- `EventLoopKernel`：继续只编排 gate、queue、registry、timer、wakeup 和 supervisor，不出现第二套 unbounded worker loop。
- `TaskAdmissionGate`：继续拥有生命周期、active publishers 与容量语义；bounded 维持 packed outstanding，无界 outstanding 委托共享账本计算。
- `UnboundedTaskLedger`：新的内部小组件，唯一职责是统一 sequence claim、rollback、single-writer completion 和 exact-enough snapshot 读取；不持有 payload、segment 或 lifecycle。
- `UnboundedTaskQueue`：只管理 sequence 到 typed slot 的映射、publication、消费游标、无锁 segment 生命周期和静默期 ordinary scanner。
- `WorkerWakeup`：首轮保持现有 admission-backed 握手；不和段优化混改。

该边界已经支持方便替换：`EventLoopKernel.unbounded(...)` 只依赖 `TaskQueue`，算法替换发生在构造注入点。无需让每次 queue 操作再经过一层可配置策略分派。

### 5.2 线性化点

| 语义 | 线性化点 |
| --- | --- |
| 准入与关闭竞争 | `TaskAdmissionGate` admission CAS |
| unbounded accepted 全序 | `UnboundedTaskLedger.claim()` 成功取得的绝对 sequence |
| bounded accepted 全序 | LMAX `RingBuffer.tryNext()` 取得的 sequence |
| payload 可见 | `publishedSequence.setRelease(sequence)` 或 bounded `RingBuffer.publish(sequence)` |
| ordinary 开始执行 | `WAITING -> RUNNING` CAS |
| shutdown 返回/丢弃所有权 | `WAITING -> RETURNED/DISCARDED` CAS |
| 物理容量归还 | worker 清理槽/timer/index 后的 `completeBatch()` |
| 真实终止 | `WorkerSupervisor` 发布全部线程退出事实 |

### 5.3 无界账本

`UnboundedTaskLedger` 维护：

- `claimed`：生产者原子递增，返回的旧值即 sequence；到 `Long.MAX_VALUE` 前 fail-fast，不能回绕。
- `rolledBack`：只有取得 sequence 后未形成 accepted task 的异常路径递增；正常热路径不写。
- `completed`：正常情况下只有 worker 批量推进；使用 release 可见的 single-writer long，不做共享 CAS。

读取 outstanding 时固定按 `completed -> rolledBack -> claimed` 的 acquire 顺序取快照，并在并发边界下重试不可能值，返回 `claimed - rolledBack - completed`。producer claim 先于 publication release，worker publication acquire 先于 completed release；rollback 也发生在 claim 之后，因此该读取顺序能避免把新 completion/rollback 与更旧 claim 拼成负数。

bounded gate 不使用该账本，继续在 enter 时原子预留容量，保证 `outstanding <= capacityLimit`。

### 5.4 claim 异常窗口

现实现先递增 sequence，再在 `segmentForClaim` 中分配段；若分配抛错，调用方尚未拿到 sequence，无法发布 tombstone。

新协议把“确保目标 segment 可用”与“提交 sequence”组成可恢复循环：读取候选 sequence，确保对应 segment 已存在，再以 CAS 提交 claim；CAS 失败说明其他 producer 已推进，重新验证即可。可失败的段分配发生在占号前。单生产者常态 CAS 一次成功；多生产者竞争通过独立基准验证，但不以更快的 fetch-add 换取无法恢复的 OOME publication hole。

### 5.5 段复用与发布代际

- Segment 和 Cell 只在首次分配时完整初始化。
- worker 在 `releaseCurrentSlot()` 清除 ordinary/tracked 引用；整段只有在全部槽已消费/释放后才能回收。
- 复用 `Cell[]` 时创建具有新 id 的 segment 节点，不遍历 Cell，不把 publishedSequence 写回哨兵。
- producer 为新任务完整覆盖 type、ordinaryState、ordinary/tracked，最后 `setRelease` 新绝对 sequence。
- worker/scanner 以 `getAcquire == expectedSequence` 判定该代已发布；旧 sequence 无法冒充新代。
- `tail` 与每段 `next` 通过 CAS/release 发布。一个 producer 成功链接新段，其他 producer 观察该链接并协助推进 tail；CAS 失败代表全局进度。
- segment 节点只保留不可变 id 与永不改写的 `next`；tail 已越过目标段时，落后 producer 从 head 向后定位。目标 publication hole 未补齐前，唯一消费者不能越过并回收该段，因此引用保持有效。
- 唯一消费者在整段全部消费并释放后推进 head，再把旧段的 `Cell[]` 放入固定容量的 `AtomicReferenceArray<Cell[]>` 池；producer 以 `getAndSet(null)` 取得唯一所有权，并为复用存储创建新节点。池满时放弃复用并修正 allocated 计数。
- 池操作只扫描固定的 `maxPooledSegments` 个槽位，可能保守地放弃复用但不能阻塞；池大小仍是构建期固定值。
- scanner 不与正常段回收并发：RETURNING/DISCARDING 先阻止 worker 获取下一任务，关闭 gate 并等待 active publishers 归零后，head/tail 链在扫描期间稳定。scanner 不修改链接、不推进游标。
- segment 统计使用边界级原子计数或可重试快照，不在每任务路径更新；必须持续满足 `allocated >= active >= 1`。
- 任何缓存命中都必须校验 segment id，不能用无验证的 ThreadLocal 段引用。
- `maxPooledSegments`、`allocated >= active >= 1` 与池满交给 GC 的行为保持不变。

### 5.6 park/unpark

首轮保留：producer `publication -> gate.leave CAS -> parked.get -> conditional unpark`；worker `parked=true -> gate acquire -> queue/mailbox/timer 全条件复查 -> park`。

稀疏/突发基准单独统计空闲到首任务延迟、unpark 次数和虚假 park。只有这些数据证明唤醒为主要限制，才引入带睡眠代际的 notification coalescing；持续积压吞吐不能作为修改唤醒协议的依据。

## 6. 性能验证

新增两层 JMH：

1. queue-level：同段 publish/poll、跨段、复用、1/2/4/8 producer、不同 segment/pool 大小。
2. kernel-level：现有持续吞吐，加 burst-drain、idle-wakeup、tracked/scheduled 混合和 Group MPSC。

每个架构提交都以相同机器、相同 JVM、相同 fork/warmup/measurement 参数，与以下三条基线同时对比：改动前项目、同次项目 bounded、固定 Commons。禁止只挑最优参数或单次峰值。

专项完成条件：

- 原有 145 个 concurrent 测试和全仓测试不减少、不弱化；新增交错测试全绿；
- unbounded ordinary 分配仍 <= 10 B/op；
- bounded 吞吐相对当前干净中位回退不超过 5%；
- unbounded 持续吞吐至少比 11,639,896 基线提升 20%，并达到同次项目 bounded 的 95% 以上；
- 继续以 Commons 和 native LMAX 报告对照，若仍有可归因且可安全消除的热路径成本则继续优化，不把 80% 当停止线；
- 多生产者与稀疏唤醒没有吞吐、尾延迟或 CPU 的结构性退化。

## 7. 正确性测试

除现有契约外，新增：

- 旧槽状态为 RETURNED/DISCARDED/TERMINAL 时复用，不得误判为新代已发布；
- 1/2/4/8 producer 跨段乱序发布，首个 hole 补齐后严格按 sequence 消费；
- 落后 producer 在 head 前进后仍能定位自己的未发布 segment；
- segment 分配失败发生在 claim 提交前，不遗留永久 hole；
- sequence 到上限时拒绝且不回绕；
- scanner 暂停期间链保持静默，不得发生会改变其目标代际的段复用；
- RETURNING/DISCARDING 双 scanner、worker running 与段边界交错仍只有一个所有权赢家；
- tombstone、earlier timer、cancellation、shutdown 和虚假唤醒均不会丢通知；
- 派生 outstanding 在并发 snapshot 下不为负，终止时归零。

原测试中绑定 `segmentLock` 具体实现的断言，应重写为“落后 producer 与段回收并发时不会访问错误代际”的行为断言，不删除覆盖。

## 8. Example 与 Tutorial

### 8.1 用户入口结构

- 根 `README.md`：只保留选型和导航，新增 bounded/unbounded、单 loop/group、execute/submit/schedule、graceful/immediate 四组短表，并链接到可运行代码。
- `disruptor-spring-boot-example/README.md`：给出纯 Java与 Spring 两条运行命令、预期日志和场景索引。
- `PureJavaConcurrentExample`：独立 main，展示显式 start、bounded fail-fast、unbounded、TaskContext、one-shot/dynamic schedule、CancellationSource、graceful shutdown 与 shutdownNow 返回边界。
- `ConcurrentDemoRunner`：复用现有 Spring Bean/service，实际执行 context、Group affinity、fixed-rate cancellation，并将结果写入 `DemoResults` 供 smoke test 验证。

### 8.2 tutorial 定位

撮合 tutorial 继续教授“并发 HTTP -> 单入口所有权 -> SINGLE RingBuffer -> 单线程撮合”。它新增 `disruptor-concurrent` 依赖，并用 bounded `DisruptorEventLoop` 承担单入口命令串行化，从而替换手写 `ThreadPoolExecutor`，但保持以下业务语义：

- HTTP 调用仍同步得到“确定入环/确定未入环”；
- EventLoop 容量与 RingBuffer 背压均使用 fail-fast，不隐藏重试；
- 入口在 Runtime 和 EventLoop 都启动后开放；停止时先封 HTTP 入口，再排空 EventLoop，最后排空 Runtime；
- `SINGLE` RingBuffer 始终只有 EventLoop worker 一个发布者；
- tutorial 不把撮合状态机搬进 EventLoop，也不混淆 task queue 与 event pipeline。

Spring 生命周期通过独立 phase 明确排序，并增加集成测试验证启动/停止顺序和真实 termination。若现有自动配置无法表达该顺序，只补最小 phase 配置或 owner 组件，不增加隐藏 singleton。

### 8.3 示例验收

- 纯 Java main 可由测试启动并在有限时间内正常退出；
- Spring demo 启动后实际完成并记录结果，容器关闭后 loop/group 真实终止；
- tutorial 原有撮合与背压测试全绿，新增相同 symbol 的请求只由固定 EventLoop worker 发布；
- 文档中的每段代码从真实示例提取或由编译测试覆盖，不保留不可运行的伪代码。

## 9. 交付顺序

1. 增加分层基准与缺失交错测试，冻结真实瓶颈和契约。
2. 实施 O(1) 代际复用、claim 异常窗口和共享无界账本，分别测量收益。
3. 将 segment 创建、链接、head/tail 推进和固定容量池切换为无显式锁数据面，完成静默期 scanner 协议。
4. 继续剖析 admission、ordinary 状态和 wakeup；只要仍有明确安全收益空间就继续，而不是到 Commons 80% 即停止。
5. 补齐纯 Java/Spring example、README 和 tutorial 的真实 EventLoop 入口，并明确 bounded 默认、unbounded 限定场景。
6. 全量测试、三组干净 JMH、GC/延迟/CPU 复核，刷新 verification 与能力矩阵。
