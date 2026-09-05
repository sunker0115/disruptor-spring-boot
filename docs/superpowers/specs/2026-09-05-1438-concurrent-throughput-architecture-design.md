# disruptor-concurrent 吞吐架构重设计

> 设计时间:2026-09-05 14:38。
> 范围:只做架构决策与迁移路线图,本阶段不动生产代码。
> 上游文档:`2026-09-02-disruptor-concurrent-design.md`(契约来源)、
> `docs/disruptor-concurrent-verification.md`(基线数据)。
> 参照源码:`/Users/sunke/dev/ai-project/commons` commit
> `5c831c06afed35dd23b666c902e5ce212ff22487`。

## 1. 问题陈述

当前普通提交路径与 Commons 参考存在数量级差距,按架构级问题处理而非热点补丁:

| 基准 | 吞吐 ops/s | 分配 B/op |
| --- | ---: | ---: |
| Commons bounded | 14,585,312 | ≈0.001 |
| Commons unbounded | 12,103,175 | 2.918 |
| 本项目 bounded | 1,213,550 | 756.718 |
| 本项目 unbounded | 1,311,134 | 792.514 |

差距约 10×,分配差距约数百倍。环境:Apple M1 Max 10 核,Zulu JDK 21,JMH 1.37,
统一 65,536 in-flight 背压窗口。

## 2. 根因分析(已完成调查)

### 2.1 基准同构性判定:成立

两侧同 JMH 参数、同 in-flight 窗口、同计数命令、同排空校验、同单提交线程。
仅两处不对称且均不足以解释差距:

- 本项目 bounded 基准在生产者侧 `tryExecute`+自旋重试;Commons 在 sequencer
  `next()` 内部阻塞。背压语义等价。
- 消费侧等待策略:本项目 park/unpark;Commons 默认 `TimeoutSleepingWaitStrategy`
  (spin 10 → yield 10 → parkNanos 100µs)。

结论:差距是真实的架构差距,不是测量伪影。

### 2.2 成本分解(每任务,bounded 路径)

本项目提交侧:
- 分配 ≈757 B/op:提交 lambda、AdmissionToken、Reservation(内含 AtomicBoolean)、
  Snapshot Builder、Snapshot ×3(初始/RUNNING/SUCCEEDED,后两个在消费侧重建)、
  EventLoopFutureTask(内含 7 个原子对象)、`this::inEventLoop` 方法引用、
  AcceptedTask、取消 lambda、Registry Entry、ConcurrentSkipListMap 节点、
  TaskEnvelope、FutureTask 内部 AtomicInteger。
- 同步:`gate.tryAcquire()` synchronized;`token.commit()` synchronized +
  notifyAll;skip-list putIfAbsent;reservation CAS 发布;
  `LockSupport.unpark(worker)` 每提交一次。

本项目消费侧(每任务):
- skip-list get+remove;`releaseOutstanding()` synchronized + notifyAll;
  FutureTask 状态机 2 次 synchronized(completionLock)+ 2 次 `toBuilder()`
  快照重建;executing increment/decrement + completed 共 3 次 AtomicLong CAS。

Commons(bounded,每任务):
- `next(1)` 1 次 CAS claim;预分配 AgentEvent 槽位 3 个字段写;
  `publish(sequence)` 1 次 setRelease;消费侧批量(默认批次),每批 1 次
  progress 写;槽位消费后 `clean()` 复用。零分配、无锁、无唤醒。

### 2.3 根因定性

本项目把每个任务实现为**无条件的全生命周期被跟踪实体**(Future 快照、accepted
record、registry 条目、reservation、gate permit 强制存在),并采用 park/unpark
唤醒协议与 skip-list registry。而公共 API 层 `execute()/tryExecute()` 没有用户
可见句柄——Future 记账在普通提交上是纯内部开销。Commons 的结构性选择是:
普通提交 = 预分配槽位直写(零记账);需要句柄的 submit/schedule 才构造
PromiseTask;shutdownNow 明确返回空列表(不跟踪)。

因此这是任务表示与记账协议的架构问题,逐个热点优化无法收敛。

## 3. 目标架构(方案 A:两层任务模型 + 槽位原生快路径)

### 3.1 不变的语义契约

| 语义契约 | 新实现载体 |
| --- | --- |
| accepted sequence 统一入口全序 | ring claim sequence 本身,不引入第二序号体系 |
| bounded 容量覆盖准入/入口/timer/running | `claimed − finished ≥ capacity` 双单调计数器;finished 在物理完成后推进,timer 等待与 running 天然占容量 |
| 精确 shutdownNow 返回未开始任务(按 accepted 序) | 关准入 → 等 `activeSubmissions == 0` → 扫槽(consumer→cursor);槽内即任务,顺序即 accepted 序;ordinary 任务的 skip-list registry 整体消失 |
| 取消语义 | tracked 任务(submit/schedule)保留现状(cancellationMailbox + future);ordinary 无用户句柄本就不可取消,与今天一致 |
| 异常处理 | ordinary 改 worker 直接 try/catch 调 taskExceptionHandler,不经 Future |
| 真实 termination / Group lease / fail-stop | Group lease 改双侧检查 + TOMBSTONE;单 kernel 反射不变量保持 |
| 单一 EventLoopKernel | 两层路径是同一 kernel 内的两条入口,不是两套实现 |

### 3.2 两层任务表示

- **快路径(tier 1)**:`execute()/tryExecute()` 直写 ring buffer 预分配 Cell
  (tag=ORDINARY,payload=Runnable),零分配,1 次 CAS claim + 1 次 release 发布。
- **记账路径(tier 2)**:`submit()/schedule()` 构造 EventLoopFutureTask /
  ScheduledTask(Future 句柄本来就必须存在),registry 只登记 tracked 任务,
  槽位 tag=TRACKED 存 AcceptedTask 引用。
- 消费侧:单一 runLoop,批量 drain;tag 分派执行;每批一次 progress 写。

### 3.3 提交协议(双侧检查,对齐 Commons nextSequence 时序)

```
state != OPEN → 拒绝
activeSubmissions++ (CAS)
claim sequence(容量原子性由此保证;满则 activeSubmissions-- 后拒绝)
写槽(tag + payload,普通任务 2 个字段写)
release publish
state != OPEN? → 写 TOMBSTONE 而非任务(保留 reservation.abort 语义)
activeSubmissions--
parked 标志为真才 unpark worker
```

### 3.4 容量与等待协议

- `claimed` 由 sequencer CAS 保证无超发;`finished` 为 worker 单写者
  volatile long(release 写)。
- 准入拒绝条件:`claimed − finished ≥ capacity`。慢路径生产者自旋+parkNanos
  (对齐 Commons producerSleepNanos 模式)。
- Worker 等待:spin → yield → park;park 前 lazySet parked=true 并双检队列,
  醒后 lazySet parked=false;生产者仅在 parked 为真时 unpark。shutdown/timer
  路径无条件 unpark。
- 指标(executing/completed/failed/cancelled/returned)单写者化,去
  AtomicLong CAS;snapshot 读 volatile。

### 3.5 无界路径

UnboundedTaskQueue 的 Segment 从"分配-丢弃(GC)"改为空闲链表池化回收
(对齐 Commons MpUnboundedBuffer recycleChunks);allocatedSegments 指标
语义变为活跃段数。

## 4. 迁移顺序

每阶段独立提交、可单阶段回退、全量测试必须绿后才进入下一阶段。

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| P0 基准加固 | bounded 补 execute 阻塞语义变体(消除 tryExecute 自旋不对称);`-prof gc` 纳入标准记录;锁定现有 7 行基线表 | 基线表入库,两套基准可复跑 |
| P1 gate 去 monitor | TaskAdmissionGate 热路径 → activeSubmissions CAS 计数 + state volatile + claimed/finished 双计数器;monitor/notifyAll 只留 shutdown 慢路径 | 273 测试绿;JMH 可测提升;B/op 下降 |
| P2 槽位原生快路径 | Cell 直存 ordinary Runnable;shutdownNow 扫槽替代 ordinary skip-list | 新增契约测试集;bounded B/op < 10 |
| P3 消费协议 + 唤醒 | 批量 drain、每批一次 progress 写、pending 推导化;指标单写者化;parked 条件唤醒 | bounded ≥ 11.7M ops/s;空载提交延迟不回退 |
| P4 无界池化 | Segment 空闲链表回收 | unbounded B/op < 10、≥ 9.7M ops/s |
| P5(可选)tracked 瘦身 | Future/Snapshot 的 toBuilder 重建消除、池化 | 仅当 profile 显示必要 |

## 5. 验收门槛

- **功能**:273 现有测试全绿、不删不弱断言;新增快路径契约测试(全序、容量含
  running、精确 shutdownNow 顺序、TOMBSTONE 无洞、4×1000 多生产者、内存可见性
  压力、空载唤醒延迟);Commons 能力矩阵重验;单 kernel 反射不变量保持。
- **性能**(同机同 JMH 纪律,3 次取中位):bounded ≤10 B/op 且 ≥11.7M ops/s
  (Commons bounded 的 80%);unbounded ≤10 B/op 且 ≥9.7M ops/s(Commons
  unbounded 的 80%);coreManaged/nativeLmax 不回退。
- **静态**:生产代码 System.out/CallerRuns/DiscardPolicy 为零;默认 jar 无
  `cn.wjybxx`;`git diff --check` 通过。

## 6. 风险与对策

| 风险 | 对策 |
| --- | --- |
| park 协议丢唤醒 | 双检 + parked 标志;空载延迟压力测试专门覆盖 |
| shutdownNow 扫槽与在途提交竞态 | 先关准入 → 等 activeSubmissions 归零 → 再扫(等价现状 awaitAdmissions) |
| 双计数器内存可见性 | VarHandle release/acquire 配对 + jcstress 风格压力测试 |
| Group 时序破坏 | 双侧检查 + TOMBSTONE;Group 13 项测试全保 |
| 语义静默削弱 | 契约测试先行,验收标准 = 测试不删不弱 |

## 7. 明确不做

- 不复制 Commons 类型体系(AgentEvent/PromiseTask 等),不引入 `cn.wjybxx` 依赖。
- 不以恢复 Commons 的 shutdownNow 空返回或绕过统一入口换吞吐。
- 不在本阶段修改生产代码;本文件只作为后续实施计划的目标架构输入。
