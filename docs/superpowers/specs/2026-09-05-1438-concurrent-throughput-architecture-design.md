# disruptor-concurrent 吞吐最终架构设计

> 范围:确定 `disruptor-concurrent` 执行/调度内核的最终内部表示与协议,允许重构、删除、
> 替换。上游文档:`2026-09-02-disruptor-concurrent-design.md`(公开契约来源)、
> `docs/disruptor-concurrent-verification.md`(基线数据)、
> `docs/disruptor-architecture-design.md`(整仓边界)。参照源码:
> `/Users/sunke/dev/ai-project/commons` commit `5c831c06afed35dd23b666c902e5ce212ff22487`
> (`java/Commons-Concurrent/.../DisruptorEventLoop.java`、
> `java/Disruptor/.../MpUnboundedBuffer.java`)。本文是内核重写的目标架构,实施计划另出。

## 1. 问题陈述

普通提交路径与 Commons 参考存在数量级差距(基线见验证文档):

| 基准 | 吞吐 ops/s | 分配 B/op |
| --- | ---: | ---: |
| Commons bounded | 14,585,312 | ≈0.001 |
| Commons unbounded | 12,103,175 | 2.918 |
| 本项目 bounded | 1,213,550 | 756.718 |
| 本项目 unbounded | 1,311,134 | 792.514 |

约 10× 吞吐差、数百× 分配差。环境:Apple M1 Max 10 核,Zulu JDK 21,JMH 1.37,统一
65,536 in-flight 背压窗口,单提交线程。已排除测量伪影(两侧同 JMH 参数、同 in-flight
窗口、同计数命令、同排空校验)。

## 2. 根因

对 `execute(Runnable)/tryExecute(Runnable)` 这条无用户句柄的 fire-and-forget 最热路径,当前
`EventLoopKernel.admit()` 把每个任务无条件做成"全生命周期被跟踪实体"。JMH `-prof gc` 量化 bounded
提交 ≈757 B/op(验证文档);profiler 需在实施前确认主要分配来源,当前判定为下列每任务对象簇:

- 提交侧分配:提交 lambda、`TaskFactory` lambda、`AdmissionToken`、`BoundedTaskQueue.Reservation`、
  `EventLoopFutureTask`、`ScheduledTaskSnapshot`+builder、`AcceptedTask`、cancellation lambda、registry
  `Entry`、`ConcurrentSkipListMap` 节点、`TaskEnvelope`。
- 提交侧同步:`TaskAdmissionGate.tryAcquire()` monitor;`AdmissionToken.commit()` monitor + `notifyAll`;
  skip-list `putIfAbsent`;reservation CAS;ring multi-producer CAS;每次提交无条件 `LockSupport.unpark(worker)`。
- 消费侧(每任务):skip-list `get`+`remove`;`releaseOutstanding()` monitor + `notifyAll`;
  `executing`/`completed` 共 3 次 `AtomicLong` CAS。

对照 Commons `DisruptorEventLoop.publishTask`(`DisruptorEventLoop.java:247`):`nextSequence(1)` CAS claim →
预分配事件槽字段写 → `publish(sequence)` setRelease → 仅在 `WAKEUP_THREAD` 且不在 loop 内时 wakeup。零分配、
无 registry、无 token、无 Future。需要句柄的 `submit/schedule` 才建 `PromiseTask`。根因是任务表示与记账协议的
架构问题,逐点优化不收敛。

## 3. 架构决策

### 3.1 保留全部自研公开语义,只重写内部表示

三条 Commons 没有的自研更强公开契约全部保留,做到"ordinary 路径零分配、tracked/scheduled 才记账、关闭时枚举":

| 自研语义 | 与 Commons 差异 | 保留方式 |
| --- | --- | --- |
| ① timer 占用容量 | Commons 定时任务移入独立堆后不占 ring 容量 | 容量用显式 `outstanding` 计数;enter 占用、worker 物理完成释放;timer 移入 heap 不释放,天然占容量 |
| ② `shutdownNow()` 精确返回未开始原始任务 | Commons 返回空列表 | 关准入后扫 ring(ordinary)+ 遍历 tracked accepted index(tracked/scheduled),CAS 抢占物理所有权 |
| ③ `execute` 与 `schedule(0)` 统一全序 | Commons 有 `LOCAL_ORDER` 分叉 | 二者同走 ring claim 得单调 admission ticket;worker 遇已到期 SCHEDULE 槽提前结束入口批次先执行它 |

### 3.2 收敛为"统一物理所有权协议 + 两种存储",非补丁

不在 tracked 记账旁加 ordinary 旁路,而是把任务表示、容量记账、关闭所有权统一到一套物理所有权协议,由不同
公开能力决定两种必要存储形态:ordinary 用预分配槽内 CAS 状态(零分配),tracked/scheduled 用其本就要分配的对象
承载状态。删除 `AdmissionToken`、`TaskReservation`、`TaskEnvelope`;registry 收窄为 tracked-only accepted index。

### 3.3 保持不退化的行为契约

以下来自 `2026-09-02-disruptor-concurrent-design.md`,本设计换实现但**行为不退化**:

- 统一 admission 顺序全序,`execute` 与 `schedule(0)` 同序(语义③);
- 有界容量覆盖准入/入口/timer/executing(语义①);快照硬约束 `outstanding ≤ capacityLimit`
  (`EventLoopSnapshot` 第 62 行);Future 终态与物理任务状态分离(上游设计第 223 行):取消运行中任务不提前
  释放 permit,permit 只在用户调用真实返回后释放;
- `execute()` 满容量 fail-fast 抛 `RejectedExecutionException`、`tryExecute()` 返回 `false`;
- 所有关闭入口**同步、幂等关闭准入**后才请求 supervisor;`shutdownNow()` 按 admission 序返回未开始原始任务、
  不返回 running、**调用本身不等待 termination**(上游设计第 343 行);deadline 到期锁存 timeout 并升级 immediate、
  取消未开始任务、请求中断运行任务(上游设计第 355 行);`shutdown()` 用 `ShutdownDeadline.unbounded()`;
- Future 恰好一个终态;同 loop 未完成 Future 的阻塞保护;
- 单一 `EventLoopKernel`,bounded/unbounded 只换 `TaskQueue`;Group 唯一 owner、稳定选择、fail-stop、共享 deadline、
  真实聚合终止;
- 生命周期/mode/deadline/首因/termination 的唯一权威是 core `WorkerSupervisor`;concurrent 只加从属任务处置,不另造
  关闭生命周期状态机;
- JDK 21 executor/scheduler 契约、显式有界 `requestShutdown`、精确启动回滚、module hook。

### 3.4 公开 API 变更(项目允许重构,不伪装成纯内部替换)

- builder 新增 `maxPooledSegments`(无界池上限,默认 8);
- `EventLoopSnapshot` 新增 `activeQueueSegments`(活跃段数,`allocatedQueueSegments` 仍报物理保留总数)与
  `discardedTasks`(immediate 丢弃计数);
- 记录/快照为 record 时构造签名随之变更——允许,不保留旧位置参数构造。

## 4. 目标架构

### 4.1 统一物理所有权状态机 + 两种存储

物理所有权状态机是 run / cancel / shutdownNow-return / shutdown-discard 的线性化点:

```
WAITING ──worker tryStart──▶ RUNNING ──▶ TERMINAL
   │  ▲                       │              ▲ ▲ ▲
   │  └ periodic re-arm ──────┘（RUNNING→WAITING,条件见下）
   │                                          │ │ │
   ├── shutdownNow tryReturn ─▶ RETURNED ─────┘ │ │
   ├── immediate discard ─▶ DISCARDED ──────────┘ │
   └── user cancel ─▶ CANCELLED_WAITING ──────────┘
```

- `RETURNED`:被 `shutdownNow()` 抢占并返回给调用方。`DISCARDED`:被 immediate 关闭取消、不返回——**immediate
  关闭的未开始任务一律进 `DISCARDED`(含 tracked/scheduled)**。`CANCELLED_WAITING`:**只用于用户/token/expires/
  次数上限等非关闭取消**,仅 tracked/scheduled。二者按原因分离,不混用。
- `RUNNING→WAITING`(周期重排):**当且仅当同时满足**——仍为周期任务、Future 未终态、未进入 quiescing/immediate、
  未 expires、未达 `maxExecutions`、失败策略允许继续——才成功并回 timer heap;否则 `RUNNING→TERMINAL`。
- **执行结果与物理处置是两个正交维度**:`completed/failed/cancelled` 是**所有 accepted task**(含 ordinary)的逻辑执行
  结果——ordinary 由 worker 直接记账(现有契约:`execute(naked)` 计入 `failedTasks`、`execute` 成功计入 `completedTasks`),
  tracked/scheduled 由 Future 终态提供;`returned/discarded` 是正交的关闭物理处置。同一任务被 shutdownNow 抢占时同时计
  `cancelled` 与 `returned`(ordinary 亦然),维持原指标口径。

两种存储,共用同一状态协议:

- **ordinary**(`execute/tryExecute`):物理状态是预分配槽 `Cell` 内的 `int state`(VarHandle CAS),槽内持原始
  `Runnable`。无 Future、无 index、零堆分配。不可被用户取消,状态集 `WAITING/RUNNING/RETURNED/DISCARDED/TERMINAL`。
  **状态在可复用槽内,故 ordinary 的槽在用户调用真实返回并 TERMINAL/清槽前不得释放**(§4.6)。
- **tracked/scheduled**(`submit/schedule`):物理状态在其已有对象(`AcceptedTask` 记录),记录持原始返回对象、
  Future/`ScheduledTask`,登记到 tracked accepted index。状态集含 `CANCELLED_WAITING`。**状态在外部记录,worker 读取
  引用后可提前释放槽**。

JDK 21 `FutureTask.run()` 设置 runner 后、任务返回前 state 仍为 `NEW`,`cancel(true)` 可成功;因此物理所有权用独立
状态机 CAS 判定,不折进 Future 终态。

### 4.2 槽位、记录与消费前缀

bounded 后端 LMAX `RingBuffer<Cell>` 的可变 typed 槽(对齐 Commons `RingBufferEvent`):

```java
static final class Cell {
    int  type;                 // ORDINARY / TRACKED / SCHEDULE / TOMBSTONE
    int  state;                // ORDINARY 物理状态,VarHandle CAS(其它 type 忽略)
    Runnable ordinary;         // ORDINARY: 原始 Runnable
    AcceptedTask<?> tracked;   // TRACKED/SCHEDULE: 指向 index 中同一记录
}
```

- `ORDINARY`:消费侧直接 `ordinary.run()`,异常交 `taskExceptionHandler`;物理状态在槽内。
- `TRACKED`:槽 `tracked` 指向记录(持 `EventLoopFutureTask`);消费侧读引用后即可释放槽,再 run future。
- `SCHEDULE`:槽 `tracked` 指向记录(内含 `ScheduledTask`);消费侧读出加入 timer heap 并释放槽,outstanding 不释放。
- `TOMBSTONE`:仅 post-claim 异常时写入(过载不产生 tombstone),消费侧跳过。

**消费前缀契约(TaskQueue 通用)**:多生产者可先发布 `seq=N+1` 而 `seq=N` 未发布;单消费者**只消费连续已发布前缀**,
遇第一个空洞(bounded `!isAvailable(seq)`;无界 `publishedSequence` 断档)即停。当前 `BoundedTaskQueue.poll()`(第 39
行)已具此行为,本设计提升为 `TaskQueue` 契约,无界后端按 `publishedSequence` 实现同一规则。**槽释放时机的 ordinary/
tracked 区别(§4.1)也写入该契约**。

**tracked accepted index**:按 admission ticket 有序、支持外部线程枚举与按记录 CAS 抢占(实现
`ConcurrentSkipListMap<Long, AcceptedTask>`,tracked/scheduled 非热路径,分配可接受)。ordinary 永不进入,保住零分配。

### 4.3 容量:显式 outstanding 计数

容量是显式 `outstanding` 计数,不用 `seq−finished`(后者在过载 tombstone 时越界,违反 `EventLoopSnapshot` 硬约束):

- **enter 占用**:bounded `enter` 在同一 packed CAS 内一次完成——检查 `OPEN`、检查 `outstanding<capacity`、
  `activePublishers++`、`outstanding++`。满容量**直接拒绝,不 claim、不 tombstone**(正常过载零 tombstone)。
- **释放**:worker 每批物理完成 N 个后批量 `outstanding-=N`(单写者);取消运行中不提前释放;scheduled 移入 heap
  不释放 ⇒ 语义①。post-claim 异常回滚(§4.4)。

其它游标独立推导,不引入每任务 `AtomicLong`:ingress pending = `claimedCursor − consumerSequence`(沿用现有"已 claim、
尚未消费"语义);ring occupancy = `claimedCursor − gatingSequence`,`ringSize = capacity`(2 的幂)。

**unbounded outstanding**:上游要求所有快照报告 `outstanding`,bounded/unbounded 只在容量拒绝上不同。unbounded 不进 packed
word 的 31 位 outstanding 字段(避免给无界模式引入隐藏 `2^31−1` 上限),而用一个**独立 64 位 `AtomicLong outstanding`**——
enter 时 `++`(不做容量门,仅记账)、worker 物理完成时批量 `-=N`。bounded 用 packed word 的 outstanding(容量门),unbounded 用
该 64 位计数器,二者快照都如实报告;溢出行为写入验收测试。

### 4.4 准入线性化(packed admission word + 分阶段标志)

**packed `AtomicLong admission` 位布局(64 位)**:`[63:62]` 生命周期(`NEW=0/OPEN=1/CLOSED=2`);`[61:31]` outstanding
(31 位,上限 `2^31−1`);`[30:0]` activePublishers(31 位)。`capacity ≤ 2^31−1`,builder fail-fast 校验(且 bounded 需 2 的
幂 ≤ ring 上限);activePublishers 到上限拒绝新 enter;mask/shift 与 CAS 更新不得跨字段进位。`NEW` 与 `CLOSED` 都拒绝提交;
`open()` 仅在非 `CLOSED` 时 `NEW→OPEN`;`CLOSED` 永久不可逆。协议所有权:`TaskAdmissionGate` 拥有生命周期门 + 逻辑容量
账本(`activePublishers`、`outstanding`、`capacity`);`TaskQueue` 拥有物理存储与游标;`EventLoopKernel` 编排二者。

生产者(bounded ordinary,零分配),`rollbackOutstanding/registered/committed` 标志保证原子提交与容量不泄漏:

```
enter: 单 packed CAS —— 仅当 state==OPEN && outstanding<capacity 则 activePublishers++、outstanding++;
       否则 fail-fast reject（满容量在此拒绝,不 claim、不 tombstone）
boolean rollbackOutstanding = true;      // 成功 enter 即先假定回滚,提交完成才清零
long seq = -1; boolean registered = false;
try {                                     // 最外层:保证 leave 永不跳过
  try {                                   // 中间层:保证 publish 永不跳过
    try {                                 // 内层:业务构造
        seq = ringBuffer.tryNext()        // 可能 InsufficientCapacity 抛出;seq 仍 -1
        构造任务; if tracked/scheduled: index.register(record); registered = true
        写 cell(type + state=WAITING + 原始 Runnable / tracked 记录)
        rollbackOutstanding = false      // 提交成功,容量正式占用
    } finally {
        if seq >= 0 && rollbackOutstanding:      // post-claim 异常:清理均为 no-throw
            if registered: index.terminalizeAndRemove(record)   // no-throw
            cell.type = TOMBSTONE                                // no-throw 字段写
    }
  } finally {
    if seq >= 0:                          // 只有真正 claim 了才发布,claimed 槽恒发布,绝不留 hole
        ringBuffer.publish(seq)
        if worker.parked: LockSupport.unpark(worker)   // 任何 publish(含 tombstone)都条件唤醒
  }
} finally {
    leave: packed CAS —— activePublishers--; 若 rollbackOutstanding 同一 CAS 内 outstanding--   // 不可跳过
}
```

- 三层嵌套 `finally` 保证 `publish(seq)` 与 `leave()` 各处于不可跳过层("处于 finally"不等于其内后续语句不被异常中断);
  回滚清理 API(`terminalizeAndRemove`、tombstone 字段写)定义为 **no-throw**;`leave()` 是最外层不可绕过操作。
- `leave` 是否回滚 `outstanding` **只看 `rollbackOutstanding`,不看 `seq>=0`**;`tryNext()` 抛异常(seq=-1)时仍回滚容量,
  不泄漏。`seq>=0` 只决定是否写 tombstone 与 publish。unbounded 的 outstanding 回滚同理走独立 64 位计数器。
- 满容量在 `enter` 直接拒绝,不 claim,故正常过载无 tombstone;tombstone 只因 post-claim 异常且随之回滚 outstanding。
- 成功 `enter` 的提交属于关闭前并发操作,一律允许完成并纳入冻结集;`enter` 之后不二次检查 `CLOSED`。线性化:`enter`
  是唯一检查点,读 `OPEN` 后暂停、再 `enter` 时若已 `CLOSED` 则失败;accepted-set 冻结点在 `activePublishers` 清零之后,
  线性化点是非 tombstone 的 `publish`。

`execute()` 拒绝即 fail-fast 抛 `RejectedExecutionException`;`tryExecute()` 返回 `false`。成本:enter/leave 两次 packed
CAS + ring claim CAS,零分配。内部 Sequencer 的 LMAX `WaitStrategy` 固定为 `signalAllWhenBlocking()` 为 no-op 的
`BusySpinWaitStrategy`(默认 `BlockingWaitStrategy` 每次 `publish` 走 `synchronized`+`notifyAll`,4.0.0 字节码已核实,会抵消
条件 unpark;worker 不用 LMAX barrier 等待)。

### 4.5 Group 准入(无每任务 owner lease)

child 有效准入 = **child 本地 `OPEN` 且 parent Group volatile `accepting` 为真**。child `enter` 只多读一次 parent
`accepting`(独立 loop 视为恒真),不增加第二组 CAS。逐个把 child `NEW→OPEN` 期间,外部持有的 child 因 parent
`accepting==false` 仍拒绝,只有 Group 单点翻转全局 `accepting` 后 child 才真正接任务,消除 startup 提前提交窗口。Group
关闭:先把全局 `accepting` 置假(同步关闭选择与所有 child 准入),再对每个 child 走 §4.4 CLOSED,各自等 `activePublishers`
清零后统一广播。Group fail-stop、首因冻结、共享 deadline、真实聚合终止仍由 `GroupLifecycleCoordinator` 维护。

### 4.6 消费循环(单写者指标 + 槽释放时机)

worker 主循环保留四段结构(语义不动):cancellation mailbox → 到期 timer(≤ `maxTimerBatchSize`)→ 入口命令(≥1、≤
`maxCommandBatchSize`,遇已到期 SCHEDULE 槽提前结束本批)→ module `onUpdate`。实现差异:

- **两个消费游标分离**(避免槽提前复用):`consumerSequence`(离开 ingress 进度,供 pending/scan 下界)与
  `gatingSequence`(槽复用,LMAX gating)在不同点推进。ORDINARY 严格按:
  `CAS WAITING→RUNNING` → release 推进 `consumerSequence`(已离 ingress,scanner 排除、pending 不含 executing)→
  `ordinary.run()` → `RUNNING→TERMINAL` → 清槽 → **最后**推进 `gatingSequence`(才允许生产者复用槽)。
  TRACKED/SCHEDULE 状态在外部记录,读出引用后 `consumerSequence` 与 `gatingSequence` 均可提前推进(早释放槽),再 run
  future / `timers.add`。据此 scanner 排除 running、pending 不含 executing,而 ordinary 槽不会提前复用(§4.1/§4.2)。
- **指标口径**:`completed/failed/cancelled` 覆盖**所有 accepted task**(含 ordinary,非 Future-only)的逻辑执行结果——
  ordinary 由 worker 记账、tracked/scheduled 取 Future 终态;`returned/discarded` 是正交物理处置。**指标最终只由 worker 在
  物理终结时累计,保持单写者;coordinator 只写物理状态与处置结果**。worker 每批物理完成 N 个后一次性 `outstanding-=N` + 一次
  drain-fact 发布。

### 4.7 唤醒协议(条件 unpark)

worker 空转判定(park 前设 `parked` 后重读 queue/mailbox/时钟/最早 trigger 再决定):入口有**连续已发布**任务、mailbox
非空、或 timer **已到期** → 不 park;只有未来 timer → `parkNanos(nextTrigger − now)`;完全无 timer → 无限 park。("timer
非空"不等于不 park:一年后的 timer 只应 parkNanos 到 trigger,不能持续 spin。)醒后 `lazySet(parked=false)`。生产者仅在
`parked==true` 时 unpark;shutdown、更早 timer 登记、cancellation 无条件 unpark。消除丢唤醒窗口,不做固定毫秒轮询。

### 4.8 关闭:core 权威 + 从属 TaskDispositionCoordinator

生命周期/mode/deadline/首因/termination 唯一权威是 core `WorkerSupervisor`;concurrent 通过 `ShutdownBackend`
(`beginQuiesce/isDrained/stop`,只由 supervisor 控制线程串行调用、有界非阻塞、不等 sequence/Future/join)接入。**所有
公开关闭入口先同步、幂等 `closeAdmissions()`,再请求 supervisor**(supervisor 请求是异步起控制线程,若不先同步关准入,
返回后到 `beginQuiesce()` 之间仍可能接任务)。

concurrent 增加从属 **`TaskDispositionCoordinator`**,负责未开始任务处置与 `returned`/`discarded`,不管 lifecycle/
termination。处置状态带**完成阶段**,单赢家 CAS:

```
NONE ─shutdownNow CAS─▶ RETURNING ─扫描完成─▶ RETURNED
NONE ─immediate  CAS─▶ DISCARDING ─取消完成─▶ DISCARDED
```

- **`shutdownNow(deadline)`**(外部调用线程):`closeAdmissions()` → CAS `NONE→RETURNING`;赢家**立即**
  `supervisor.requestShutdown(IMMEDIATE, deadline)`(尽早让 worker interrupt 当前运行任务;RETURNING 屏障保证 worker 在
  `RETURNED` 前不清未开始任务,故不改变 `returned` 集),再在**本调用线程**等 `activePublishers==0`、冻结 `claimed`、枚举得
  `returned`(规模有界于此刻队列)、置 `RETURNED`,**立即返回不等 termination**;败者 `returned` 空,同样请求 supervisor。
- **`requestShutdown(IMMEDIATE, deadline)`** 与 **graceful 超时升级(`ShutdownBackend.stop(IMMEDIATE)`)**:`stop(IMMEDIATE)`
  **必须原子设置 kernel stop mode(`kernel.stopWorker(IMMEDIATE)`)**+ CAS `NONE→DISCARDING`(登记)+ unpark,保持有界非阻塞;
  然后由**一次性异步 disposition 执行者**(命名虚拟线程,带失败结果的 completion)等 `activePublishers==0`、冻结 accepted set、
  **立即取消 tracked/scheduled 未开始 Future 至终态并置物理 `DISCARDED`**、对 ordinary 槽 CAS 标 `DISCARDED`,置全局 `DISCARDED`;
  物理清槽仍可由 worker 完成。这样即使 worker 卡在忽略中断的用户任务,未开始 Future 也被及时取消,满足 deadline 升级语义。
  执行者异常经 completion 上报 `WorkerSupervisor` 首因,并允许 worker 接管 best-effort 收敛(§下"退出序列"),不让状态永久卡在
  `DISCARDING`。CAS 败给 `RETURNING` 则让位。
- **`shutdown()` / `requestShutdown(GRACEFUL,…)`**:`closeAdmissions()`;处置保持 `NONE`;保留 ordinary 与 one-shot delayed
  继续执行、取消 waiting periodic,由 worker 在自身线程完成。

**冻结退出序列**(闭合 disposition 完成、worker 退出与异常传播):

```
stop requested（stopMode 已设）
→ worker 不再启动新任务；当前运行任务真实返回
→ worker 等待或【协助】disposition 到达终态（RETURNED/DISCARDED）——不永久卡 RETURNING/DISCARDING
→ 物理清槽、清 heap/index、释放 outstanding（worker 做物理收敛）
→ module stop（逆序）
→ worker 退出
→ core WorkerSupervisor termination（观测 stop callback + worker join,第 527 行）
```

- worker 收到 immediate stop 可提前 interrupt、可完成当前运行任务,但**处置终态前不得批量清理未开始任务**(否则破坏精确
  `returned`/丢弃集)。
- **RETURNING 由获胜的 `shutdownNow()` 调用线程独占**:worker 只能等待,**不得协助扫描/抢占**——否则 worker 把任务
  `WAITING→RETURNED` 后调用线程 CAS 失败,任务被标 RETURNED 却进不了任何返回列表(有结果交付通道的只有调用线程)。若 RETURN
  owner 扫描失败,显式 `RETURNING→DISCARDING`(失败过渡)、记 `WorkerSupervisor` 首因,再允许 worker/异步执行者按逐任务 CAS
  接管**丢弃**。
- **DISCARDING 不产生返回列表**,允许异步执行者与 worker 基于逐任务 CAS **共同**驱动至终态;异步执行者启动失败或异常时其
  completion 带失败结果 → 进 supervisor 首因,worker 退出前接管收敛,不让状态永久停在处理中。
- `termination` 由 core 反映 worker 真实退出;未开始 Future 的及时取消由 disposition 执行者(或 worker 接管)保证。registry/
  Future/槽引用/outstanding 全部清理是 worker 退出前退出序列的一部分。

结果 C(RETURN)枚举协议(timer heap 始终 worker 独占):

1. **ring 扫 ORDINARY**,`[consumerNext, claimed]` 闭区间(worker release 写 `consumerNext`,scanner acquire 读)。逐槽先
   校验发布(bounded `isAvailable(seq)` 的 lap;无界 `publishedSequence` 代际)再读。竞态定序:worker `CAS WAITING→RUNNING`
   后推进 `consumerSequence`,`gatingSequence` 按 §4.6 在清槽后才推进;scanner 先读 `cell.ordinary` 入局部再
   `CAS WAITING→RETURNED`,成功收集局部。scanner 胜则 worker 后续见 `RETURNED` 才清槽(scanner 已持引用)。
2. **tracked index 枚举**,按 ticket 序 `CAS WAITING→RETURNED` 成功后**先把 Future 置取消终态,再收集**记录单一
   `shutdownNowReturnValue`(上游第 276 行:RETURNED 成功须取消 Future 并入返回列表)。ring 中未消费的 tracked 由 index 认领,
   ring 扫描只处理 ORDINARY,避免双重处理。
3. 两来源扫描都遇 RUNNING:tracked/scheduled 执行 `cancel(true)`,ordinary 只触发 `worker.interrupt()`;running 不入 `returned`。
4. 合并两来源按 ticket 排序作为 `returned`;Future 终态与物理 RETURNED 全部落定后才把全局 disposition 置 `RETURNED`。DISCARDING
   用同一枚举机制,动作换为取消 Future 至终态 + 标物理 `DISCARDED`,不收集返回。

`shutdownNowReturnValue` 统一规则:所有 `Runnable` 来源(`execute`/`submit(Runnable[,result])`/`schedule(Runnable,…)`)存
原始 `Runnable`;所有 `Callable`/`ScheduledTaskSpec` 来源(`submit(Callable)`/`schedule(Callable,…)`/`schedule(ScheduledTaskSpec)`)
存返回给用户的 `RunnableFuture`/`ScheduledFuture`。记录只存一个该值,枚举不按 API 类型临时推导。

### 4.9 无界后端(同一槽协议 + 池化 + 代际 + 段生命周期锁)

`UnboundedTaskQueue` 分段 MPSC buffer 的段也存可变 typed 槽,段从"分配-丢弃(GC)"改空闲链表回收(对齐 Commons
`MpUnboundedBuffer` recycleChunks)。三项协议:

- **发布代际**:池化后同一 `Cell`/段对应不同绝对 sequence,仅靠 type/state 无法区分本轮与残留。每槽存 `publishedSequence`;
  producer 末尾 release 写、consumer/scanner acquire 校验(消费前缀按此判连续);段复用前重置并清空全部 payload/state。
- **段生命周期锁**:布尔 pin 无法阻止并发回收。用一把段生命周期锁:scanner 持锁捕获 head/consumer 并遍历;worker 只在跨段
  unlink/recycle 时取同锁;普通单槽消费不加锁;多个 scanner 由锁串行化。
- **池上限**:`maxPooledSegments`(默认 8,参照 Commons `maxPooledChunks`);超上限空闲段释放给 GC 不入池。

快照:`allocatedQueueSegments`=物理保留总数(活跃+池),`activeQueueSegments`=活跃段数。unbounded 的 DISCARD 由 disposition
执行者/worker 逐段处理,不在 `ShutdownBackend` 回调内同步做无界扫描。禁止复制 kernel、禁止让无界队列伪装成固定 RingBuffer。

## 5. 组件级蓝图

协议所有权:`TaskAdmissionGate` 拥有生命周期门 + 逻辑容量账本;`TaskQueue` 只拥有物理存储、消费前缀游标与 publication;
`EventLoopKernel` 编排二者与 `TaskDispositionCoordinator`;`WorkerSupervisor` 仍是关闭生命周期唯一权威。

重写:

- `internal/BoundedTaskQueue.java`:`Cell` 改可变 typed 槽(含 `int state` VarHandle);claim/write*/publish、消费前缀游标
  `poll()`+`currentType/…`(ordinary 槽 run 后才释放,tracked/scheduled 读引用后早释放)、`scanOrdinaryUnstarted(from,to,sink)`
  (`isAvailable`/lap 校验)、gating/`consumerNext` release;内部 Sequencer 用 `BusySpinWaitStrategy`;删 `AtomicLong pending`。
- `internal/UnboundedTaskQueue.java`:typed 槽 + 段池化 + 发布代际 + 段生命周期锁 + `maxPooledSegments` + 消费前缀。
- `internal/TaskQueue.java`:接口改为 claim/write/publish/消费前缀游标/scan(不含 enter/leave/生命周期/容量);消费前缀与
  ordinary/tracked 槽释放时机写入接口契约。
- `internal/TaskAdmissionGate.java`:packed `AtomicLong admission`(§4.4 位布局);enter 单 CAS(open/容量/publisher++/
  outstanding++)、leave(publisher-- 及按 `rollbackOutstanding` 的 outstanding--)、worker 批量 outstanding--;open/close/
  awaitDrained 无 monitor(spin-then-park + 末位 publisher 唤醒);builder fail-fast 校验 capacity 上限。
- `internal/AcceptedTaskRegistry.java`:收窄为 tracked-only accepted index(ordinary 不登记);有序枚举 + 记录 CAS 抢占 +
  `terminalizeAndRemove`;记录存单一 `shutdownNowReturnValue`。
- `internal/AcceptedTask.java`:保留物理所有权状态机(含周期 `RUNNING→WAITING` 全条件),只服务 tracked/scheduled;新增
  `shutdownNowReturnValue`。ordinary 不构造它。
- `internal/ScheduledTask.java`、`internal/CancellationMailbox.java`:随记录语义与关闭枚举调整而重写。
- `internal/TaskDispositionCoordinator.java`(新增):`NONE/RETURNING/RETURNED/DISCARDING/DISCARDED` 单赢家 CAS;RETURN 在
  调用线程扫描,DISCARD 由一次性异步执行者取消未开始 Future;带**失败结果的 completion**(异常上报 supervisor 首因),
  与 worker 协调:终态前不批量清未开始任务,worker 退出前可接管把 disposition 推到终态(§4.8 退出序列)。
- 可观测贯穿:`EventLoopSnapshot` 新增 `activeQueueSegments`(仅 unbounded)、`discardedTasks`;`EventLoopGroupSnapshot`
  聚合 `discardedTasks`;Spring `DisruptorConcurrentMetrics` 新增 `disruptor.eventloop.tasks.discarded` 与区分
  active/allocated 段的 meter。计数按两维度(Future outcome:completed/failed/cancelled;物理 disposition:returned/discarded),
  正交不互斥。
- `internal/EventLoopKernel.java`:`admit()` 重写为 §4.4;`submitOrdinary` 不建 Future/记录;消费按 type 分派 + 两消费游标
  分离(§4.6)+ 单写者指标 + 批量 outstanding--(bounded 走 packed word、unbounded 走独立 64 位计数器);冻结退出序列;
  `shutdown/requestShutdown/shutdownNow` 先同步 `closeAdmissions()` 再走 §4.8。
- `internal/EventLoopShutdownBackend.java`:`stop(mode)` 保持 `kernel.stopWorker(mode)` 设置 worker stop mode;IMMEDIATE 额外
  登记 DISCARDING 并 unpark(有界非阻塞)。
- `internal/GroupLifecycleCoordinator.java`:删每任务 owner lease,改 §4.5 全局 `accepting` volatile + 各 child 自身 drain;
  child 保持 `NEW` 直到 Group 置全局 `accepting`。

删除:`internal/AdmissionToken.java`、`internal/TaskReservation.java`、`internal/TaskEnvelope.java`。

保留:`IndexedScheduledHeap`(worker 独占)、`EventLoopFutureTask`、`ModuleLifecycle`、公开门面与全部公开类型。

## 6. 实施时的文档同步

实施同一变更内必须同步重写,不让两个"最终架构"并存:

- `docs/disruptor-architecture-design.md` 第 193 行"Group gate 先于 child gate 获取准入 token"改为 §4.5 全局 `accepting`
  协议;第 195 行 shutdownNow 描述与本文 §4.8 对齐;
- `docs/superpowers/specs/2026-09-02-disruptor-concurrent-design.md` 的 `TaskAdmissionGate` 准入步骤改为本文 §4.4/§4.5/§4.8;
- `EventLoopGroupSnapshot` 聚合新增 `discardedTasks`;`DisruptorConcurrentMetrics` 补 `disruptor.eventloop.tasks.discarded`
  与 active/allocated 段 meter(与 §3.4/§5 一致);
- `docs/commons-capability-matrix.md`(20/51/53 行机制)与 `docs/disruptor-concurrent-verification.md`(JMH 表与结论)在代码 +
  同机 JMH 实测通过后刷新,不预先宣称。

## 7. 验收门槛

- **功能**(按新内部契约重写并通过契约测试,不删不弱断言):admission 全序含 `schedule(0)` 先于其后 `execute`;显式
  outstanding 且过载零 tombstone、`outstanding ≤ capacityLimit` 恒成立、容量含 running 与 heap timer;**`tryNext` 失败不泄漏
  outstanding**、post-claim 异常回滚且无 hole;**ordinary 槽运行期间不被复用覆盖**;`execute` 满容量 fail-fast;所有关闭入口
  同步幂等关准入;三种处置(shutdownNow 返回且不等 termination / immediate 丢弃不返回 / graceful 保留)+ 完成阶段
  `RETURNING/DISCARDING`:并发关闭下 worker 在处置终态前不清未开始任务、returned 不被拆分/破坏;`stop(IMMEDIATE)` 同时设
  worker stop mode 与登记 DISCARD;worker 卡在忽略中断任务时未开始 Future 仍被异步取消(deadline 升级);消费前缀遇空洞即停;
  周期 `RUNNING→WAITING` 全条件;park 只对未来 timer parkNanos;无界发布代际/段锁/池上限;Group startup 全局 `accepting` 无
  提前提交、fail-stop 与共享 deadline;JDK/有界 deadline 关闭;同 loop 阻塞保护;四种调度 + expires + maxExecutions +
  continueOnFailure;`activeQueueSegments`/`discardedTasks` 快照;RETURNED 先取消 Future 再入返回列表、immediate 未开始一律
  DISCARDED(不用 CANCELLED_WAITING)、扫描遇 RUNNING 按 tracked/ordinary 分别取消;两消费游标分离(scanner 排除 running、
  pending 不含 executing、槽不提前复用);准入异常三层 finally 必 publish+leave;退出序列闭环(disposition 失败进 supervisor
  首因、worker 接管收敛、不永久卡处理中);unbounded `outstanding` 独立 64 位计数器如实上报且溢出受控;指标两维度正交
  (同一 tracked 同时计 cancelled 与 returned/discarded)。单一 `EventLoopKernel` 反射不变量保持。
- **性能**(同机同 JMH 纪律,人工 release gate,不进普通 CI;3 次取中位):通过条件是**同次运行相对门槛**
  `median(project)/median(Commons) ≥ 80%`(bounded、unbounded 各自对比);绝对 ~11.7M/~9.7M ops/s 只作当前 M1 Max 参考基线,
  **不作通过条件**(尊重上游"不设机器敏感硬阈值",若正式推翻须同步改上游设计,不并存两份终版)。分配门槛 ≤10 B/op 独立保留,
  但**只统计框架自身分配**并固定任务对象预创建方式。`coreManaged`/`nativeLmax` 不回退;扩充 1/2/4/8 生产者曲线、空载首提交
  p50/p99 唤醒延迟、ordinary/submit/schedule 混合负载、timer 满容量拒绝成本、cancellation 与 shutdownNow 扫描成本、CPU/队列深度。
- **静态**:生产代码无 `System.out/System.err`/`CallerRuns`/`DiscardPolicy`;删除的三个类零引用;默认 jar 无 `cn.wjybxx`;
  `git diff --check` 通过。

## 8. 风险与对策

| 风险 | 对策 |
| --- | --- |
| Future 终态与物理态不闭合 | RETURNED 先取消 Future 再入列表;immediate 一律 DISCARDED;扫描处理 RUNNING;两维度正交计数 |
| 双游标混推致槽提前复用 | `consumerSequence`(离 ingress,CAS RUNNING 后推)与 `gatingSequence`(复用,run+TERMINAL+清槽后推)分离 |
| disposition 卡死/异常不传播 | 冻结退出序列;coordinator completion 带失败结果进 supervisor 首因;worker 退出前接管收敛 |
| unbounded outstanding 失真/隐藏上限 | unbounded 用独立 64 位计数器(非 packed 31 位);快照如实报告,溢出入验收 |
| 性能硬阈值与上游冲突 | 通过条件改相对比 ≥80%,绝对值仅参考;推翻"不设硬阈值"须同步上游 |
| 准入异常漏 publish/leave | 三层嵌套 finally;回滚清理 no-throw;leave 最外层不可绕过 |
| `tryNext` 失败泄漏 outstanding | leave 回滚只看 `rollbackOutstanding`(enter 后即置真、commit 才清)不看 seq;seq 仅决定 tombstone/publish |
| ordinary 槽运行期被复用覆盖 | ORDINARY 槽 run 返回 + TERMINAL + 清槽后才推进 gating;tracked/scheduled 读引用后早释放 |
| 关闭方法返回后仍接任务 | 所有关闭入口先同步幂等 `closeAdmissions()` 再请求 supervisor;Group 先关全局 accepting |
| 并发关闭破坏 returned | `RETURNING/DISCARDING` 完成阶段单赢家;worker 处置终态前不清未开始任务 |
| `stop(IMMEDIATE)` 未设 worker 停止 | `stop` 保留 `kernel.stopWorker(mode)`;IMMEDIATE 额外登记 DISCARD + unpark,有界非阻塞 |
| worker 卡死时未开始 Future 不取消 | 一次性异步 disposition 执行者取消未开始 Future,独立于 worker;termination 仍等线程真实退出 |
| 容量越界/tombstone 风暴 | 显式 outstanding,enter 单 CAS 满容量直接拒绝、不 claim;tombstone 仅 post-claim 异常且回滚 |
| 关闭归属与 core 冲突 | WorkerSupervisor 唯一权威;concurrent 只加从属 coordinator;shutdownNow 不等 termination |
| packed word 溢出/跨字段进位 | 固定位布局 lifecycle/outstanding/publisher;capacity 上限 builder fail-fast;publisher 饱和拒绝 |
| 未来 timer 持续 spin | park 判定用 timer 已到期;未来 timer parkNanos |
| 多生产者 publication hole | 消费前缀契约:遇第一个未发布 seq 即停 |
| 无界段回收 use-after-free | 发布代际 release/acquire + 段生命周期锁 + 复用前清 payload/state |
| 每 publish 隐藏 monitor | 内部 Sequencer 用 `BusySpinWaitStrategy`(signalAllWhenBlocking no-op) |
| Group child 未 ready 接任务 | child 有效准入 = 本地 OPEN && 全局 `accepting` volatile;Group 单点翻转 |
| 物理所有权误判已完成 | 独立状态机 CAS,不用 Future 终态;JDK FutureTask 运行中 state 仍 NEW 测试 |
| 可见性 | VarHandle release/acquire 配对 + jcstress 风格压力测试 |

## 9. 明确不做

- 不复制 Commons 类型体系(`AgentEvent`/`PromiseTask`/`IEventLoopAgent`/`ComponentId`/`LOCAL_ORDER`/任务池/
  `GlobalEventLoop`),不引入 `cn.wjybxx` 依赖。
- 不放弃三条自研语义(timer 占容量、精确 `shutdownNow`、统一全序)换吞吐。
- 不把 `execute()` 改成阻塞语义;不引入 `publication-mode` 式关闭语义开关。
- 不把"单 CAS"设为先验目标;不为吞吐把关闭状态融合进自定义 sequencer;不在 concurrent 侧另造关闭生命周期状态机。
