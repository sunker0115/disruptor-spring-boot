# disruptor-concurrent 完成验证

本文记录 `docs/superpowers/specs/2026-09-02-disruptor-concurrent-design.md` 的完成证据。
对标源码固定为 `/Users/sunke/dev/ai-project/commons` commit
`5c831c06afed35dd23b666c902e5ce212ff22487`。这里的“覆盖”指场景与契约覆盖，
不表示 `cn.wjybxx.*` API 兼容，也不表示吞吐持平。

## 构建与测试

2026-09-05 使用 JDK 21 执行：

```bash
mvn clean verify
```

8 个 reactor 模块全部 `BUILD SUCCESS`。Surefire 报告合计 273 个测试，0 failure、
0 error、0 skipped：

| 模块 | 测试数 |
| --- | ---: |
| `disruptor-core` | 131 |
| `disruptor-concurrent` | 98 |
| `disruptor-benchmarks` | 3 |
| `disruptor-spring-boot-autoconfigure` | 30 |
| `disruptor-spring-boot-example` | 2 |
| `disruptor-spring-boot-tutorial` | 9 |

`disruptor-spring-boot-starter` 没有测试源码；其依赖边界由 reactor 打包、POM 和
依赖树审计证明。

## 设计契约到证据

| 设计要求 | 直接证据 | 判定 |
| --- | --- | --- |
| core 使用通用监督状态并提供真正无界 deadline | `WorkerSupervisorTest`、`MonotonicDeadlineTest`、`DisruptorRuntimeTest`；源码搜索不存在 `PipelineLifecycle` | 通过 |
| loop/group 使用 JDK 21 executor/scheduler 与只读启动、终止事实 | `PublicContractTest`、`EventLoopExecutorContractTest.executeAndAllSubmitOverloadsUseOneWorkerInAcceptedOrder` | 通过 |
| 有界默认、无界显式，两者只有队列与容量不同 | `EventLoopBackendContractTest.bothBackendsShareStartupExecutionSchedulingCancellationAndShutdown`、`facadesHoldTheSameKernelImplementationClass` | 通过 |
| MPSC sequence 无丢失、无重复、无 publication hole，并清理槽位/segment | `TaskQueueContractTest.concurrentReservationsPublishEverySequenceExactlyOnce`、`abortPublishesTombstoneWithoutLeavingASequenceHole`、`unboundedQueueReclaimsConsumedSegments` | 通过 |
| bounded permit 覆盖准入、入口、timer、running，物理清理后才归还 | `TaskAdmissionGateTest` 全组、`EventLoopExecutorContractTest.boundedCapacityCoversTheRunningTaskAndRecoversOnlyAfterPhysicalCompletion` | 通过 |
| accepted registry 精确冻结等待/运行/取消/返回/终态所有权 | `AcceptedTaskRegistryTest` 全组、`EventLoopShutdownTest.shutdownNowReturnsWaitingOriginalRunnablesInAcceptedOrder` | 通过 |
| `schedule(0)` 与普通提交保持 accepted 顺序；同 trigger 按 priority/sequence | `EventLoopSchedulingTest.zeroDelayScheduleAcceptedBeforeExecuteAlwaysRunsFirst`、`sameTriggerUsesPriorityThenAcceptedSequence` | 通过 |
| command/timer 两侧都有批次公平 | `EventLoopSchedulingTest.boundedBatchesPreventTimersAndCommandsFromStarvingEachOther` | 通过 |
| one-shot、fixed-rate、fixed-delay、dynamic-delay、expires、次数上限、继续失败和重调度快照原子发布 | `EventLoopSchedulingTest` 8 项、`ScheduledTaskTest` 10 项 | 通过 |
| 显式不可变 context，不捕获 ThreadLocal | `TaskContextTest`、`ConcurrentExampleTest` | 通过 |
| 取消首次获胜，覆盖早晚监听、同步/异步、解注册、异常和显式 scheduler | `CancellationSourceTest` 8 项 | 通过 |
| 同 loop 未完成 Future/get、await、invoke、close 不允许阻塞 | `EventLoopBlockingGuardTest.rejectsEveryBlockingPathFromOwnerLoopButAllowsCompletedFutureRead` | 通过 |
| 精确 startup、并发 shutdown、module 部分启动逆序回滚和 suppressed failure | `DisruptorEventLoopTest` 5 项、`ModuleLifecycleTest` 3 项 | 通过 |
| `shutdown()` 使用无界 deadline，保留一次性延迟任务并取消周期任务 | `EventLoopShutdownTest.shutdownUsesUnboundedDeadlineAndRunsAcceptedFutureOneShot`、`shutdownCancelsWaitingPeriodicButDoesNotInterruptCurrentInvocation` | 通过 |
| `shutdownNow()` 返回未开始原始 Runnable，不返回 running，并尽力中断 | `EventLoopShutdownTest.shutdownNowReturnsWaitingOriginalRunnablesInAcceptedOrder`、`shutdownNowDoesNotReturnRunningTaskAndInterruptsWorker` | 通过 |
| 有界 deadline 只升级模式，不伪造 termination | `EventLoopShutdownTest.boundedGracefulDeadlineEscalatesWithoutFakingTermination`、`terminationWaitsForInterruptIgnoringTaskToReallyExit` | 通过 |
| Group gate、稳定选择、child owner、fail-stop、共享 deadline、真实终止 | `EventLoopGroupTest` 5 项、`EventLoopGroupShutdownTest` 8 项 | 通过 |
| Spring 只管理根对象，启动失败回滚，共享 deadline 后等待真实 termination 再回调 | `DisruptorConcurrentLifecycleTest` 2 项、`DisruptorConcurrentAutoConfigurationTest.managesExplicitRootsWithoutManagingAnExposedGroupChildTwice` | 通过 |
| health、metrics 与 concurrent classpath 各自独立条件装配 | `DisruptorConcurrentAutoConfigurationTest.healthAndMetricsAreIndependentOptionalLayers`、health/metrics 测试 3 项 | 通过 |
| 示例覆盖 context、四类调度中的固定与动态模式、取消、affinity 和 Spring 关闭 | `ConcurrentExampleTest` 与 `OrderEventLoopService` | 通过 |

## Commons 能力矩阵逐项证据

下表与 `docs/commons-capability-matrix.md` 顺序一致；参考侧源码位置及分类理由见该
矩阵，本表只补完成证据。

| 能力项 | 本项目完成证据 |
| --- | --- |
| 单/多生产者 RingBuffer、批量 claim/publish | `NativeCapabilitiesTest`、`ManagedPublicationTest` |
| barrier、依赖序列、alert | `PipelineSpec.topology` 原生 LMAX 边界、`DisruptorPipelineTest` |
| wait strategy | `PipelineSpecTest`、`DisruptorAutoConfigurationTest` |
| 用户事件、原始 sequence 与 topology | `NativeCapabilitiesTest` |
| 分段 MPSC 无界 sequencer 场景 | `UnboundedTaskQueue`、`TaskQueueContractTest`、`UnboundedEventLoopTest` |
| 低层 producer/consumer barrier | `PipelineHandle.unsafeRingBuffer()` 明确逃生口；不复制 fork API |
| EventLoop 与 JDK executor/scheduler | `EventLoopExecutorContractTest`、`EventLoopSchedulingTest` |
| EventLoopGroup、chooser、affinity | `EventLoopGroupTest.selectionIsStableRoundRobinAndChildrenExposeTheirSingleParent` |
| one-shot/fixed-rate/fixed-delay | `EventLoopSchedulingTest` |
| dynamic-delay | `dynamicDelayReceivesLastRunAndFailureCanContinueUntilCountLimit` |
| priority | `sameTriggerUsesPriorityThenAcceptedSequence` |
| timeout、次数限制、周期异常继续 | `EventLoopSchedulingTest`、`ScheduledTaskTest` |
| CancelToken、原因、监听和解注册 | `CancellationSourceTest` |
| `cancelAfter` | `cancelAfterUsesExplicitSchedulerAndCancelsTimerWhenSourceWinsFirst`；显式 scheduler 替代隐藏默认值 |
| task context | `TaskContextTest`、`OrderEventLoopService` |
| 同 loop 阻塞保护 | `EventLoopBlockingGuardTest` |
| lazy start | 明确不复制；`taskSubmissionIsRejectedBeforeStartAndAfterShutdown` 与 Spring lifecycle 证明显式启动边界 |
| parent 与 EventLoopFactory | `EventLoopGroupTest` |
| `LOCAL_ORDER` | 明确不复制；`zeroDelayScheduleAcceptedBeforeExecuteAlwaysRunsFirst` 证明统一入口全序 |
| Agent 主循环阶段 | module start/stop/update 加显式周期调度替代；`EventLoopModuleTest`、`ModuleLifecycleTest` |
| early/update/late phase | 明确由一个业务调度任务编排，不宣称框架等价 |
| ComponentId/依赖解析/数组索引 | 明确留给 Spring/应用 DI，不属于运行时边界 |
| module 构建后冻结 | `EventLoopModuleTest`、`ModuleLifecycleTest` |
| EventLoop 用户事件/handler | `disruptor-core` 原生 topology 承担；不把业务事件塞入 Runnable 槽 |
| 自研 Future/Promise | JDK `Future`/`ScheduledFuture`/`CompletionStage` 替代；executor、scheduling、blocking 测试 |
| Future 只读视图与组合 | `termination()` minimal stage、JDK CompletionStage；`PublicContractTest` |
| 立即执行器与适配器 | JDK `Runnable::run`/`ExecutorService`，不增加重复类型 |
| stackless cancel/timeout/FutureLogger | `CancellationReason`、标准异常、SLF4J handlers；`CancellationSourceTest` |
| bounded/unbounded EventLoop 后端 | `EventLoopBackendContractTest` 11 项、单一 kernel 反射断言 |
| `shutdownNow()` | `EventLoopShutdownTest`、`EventLoopGroupShutdownTest`；行为强于参考空列表 |
| 自定义拒绝策略 | `tryExecute` 与 `RejectedExecutionException`；`EventLoopExecutorContractTest` |
| 任务对象池 | 明确不复制；GC 基准已证明当前普通提交存在显著分配成本，见下节 |
| WatcherMgr | module/取消监听/JDK Flow 为项目级替代，不增加全局 watcher 框架 |
| GlobalEventLoop | Spring Bean 或应用 owner；示例没有隐藏 singleton |
| Group 失败收敛与共享 deadline | `EventLoopGroupShutdownTest` 8 项 |
| 启动回滚与精确终止 | `WorkerSupervisorTest`、`ModuleLifecycleTest`、Group shutdown 测试 |
| Spring 生命周期、健康和指标 | concurrent auto-configuration、lifecycle、health、metrics 测试 |

## JMH 结果

环境：Apple M1 Max，10 核，32 GiB，Darwin 25.6.0 arm64，Zulu OpenJDK
21.0.2，JMH 1.37。统一参数为 1 个 fork、1 次 1 秒预热、2 次 1 秒测量、1 个
提交线程，吞吐单位为 ops/s。五条本项目路径与两条 Commons 路径都使用 65,536 的
统一 in-flight 上限；每次提交先占一个窗口位置，消费后释放，Trial 关闭时强制校验
`submitted == consumed`。因此结果表达可持续的提交/消费吞吐，不把无界队列的短期积压
速度混入比较。结果仅用于当前机器的架构诊断，不设 CI 硬阈值。

默认可执行包：

```bash
java -jar disruptor-benchmarks/target/benchmarks.jar \
  com.sstlfsj.disruptor.benchmark.EventLoopBenchmark -wi 1 -i 2 -f 1
```

| 基准 | 吞吐 ops/s |
| --- | ---: |
| `EventLoopBenchmark.nativeLmax` | 23,999,555.914 |
| `EventLoopBenchmark.coreManaged` | 3,705,119.691 |
| `EventLoopBenchmark.jdkSingleThreadExecutor` | 4,534,510.639 |
| `EventLoopBenchmark.boundedEventLoop` | 1,213,550.375 |
| `EventLoopBenchmark.unboundedEventLoop` | 1,311,134.827 |

Commons profile 使用同一固定参考 commit。有界基线使用多生产者 sequencer，与本项目
MPSC 正确性模型一致；JMH 仍只有一个提交线程：

```bash
git -C /Users/sunke/dev/ai-project/commons rev-parse HEAD \
  | grep -Fx '5c831c06afed35dd23b666c902e5ce212ff22487'
mvn -f /Users/sunke/dev/ai-project/commons/java-apts/pom.xml install -DskipTests
mvn -f /Users/sunke/dev/ai-project/commons/java/pom.xml \
  -pl Commons-Concurrent -am install -DskipTests
mvn -pl disruptor-benchmarks -am -Pcommons-baseline clean package
java -jar disruptor-benchmarks/target/benchmarks.jar \
  com.sstlfsj.disruptor.benchmark.CommonsEventLoopBenchmark -wi 1 -i 2 -f 1
```

profile 的 `validate` 阶段还会独立验证参考仓 HEAD、拒绝 tracked dirty worktree，并核对
实际解析的 `commons-concurrent`、`commons-base`、Commons `disruptor` 三个 JAR 的
规范内容 SHA-256（按 entry 名排序后散列名称与解压内容，不受 ZIP 时间戳影响）；因此同版本
的旧包或远程包不能冒充该 commit。benchmarks 模块在每次构建的
`initialize` 阶段清理 profile 相关 class 和 JMH 生成元数据，再按当前 profile 的源码集合
重新编译；切回默认 profile 时不会把 Commons 内容带入默认 jar。

| 固定 artifact | 规范内容 SHA-256 |
| --- | --- |
| `commons-concurrent-2.0.0.jar` | `092b61b98875714f82bdf952b91369d4cca39ca29cdbf15cb55f0085ba22cbaa` |
| `commons-base-2.0.0.jar` | `84040c8fa896ad2c4ad710f2d169476bcfe09e64b4a6df0f98c8fa5239751678` |
| `disruptor-2.0.0.jar` | `e66abdce2da8e6b04567a5a9fcf753cdeadfe4aa72861978ea4323c2190e67db` |

| 基准 | 吞吐 ops/s |
| --- | ---: |
| `CommonsEventLoopBenchmark.commonsBoundedEventLoop` | 14,585,312.839 |
| `CommonsEventLoopBenchmark.commonsUnboundedEventLoop` | 12,103,175.303 |

额外使用 `-prof gc` 的诊断结果：

| 基准 | 分配 B/op |
| --- | ---: |
| Commons bounded | 约 0.001 |
| Commons unbounded | 2.918 |
| 本项目 bounded | 756.718 |
| 本项目 unbounded | 792.514 |

结论必须分开表述：设计范围内的功能与关闭契约全部通过自身测试，并有 dynamic-delay、
priority、精确 `shutdownNow`、Group fail-stop、共享 deadline、启动回滚和 Spring 运维
增强；lazy start、`LOCAL_ORDER`、Agent phases 和自研 Future 等能力则按矩阵明确不复制或
由项目级边界替代。普通 `execute/tryExecute` 的吞吐和分配率存在明确性能退化，不能宣称
性能持平，也不能把项目级替代解释为 API 等价。
源码证据显示当前提交为每个任务建立 Future 快照、accepted record、registry 条目、
reservation 和 envelope，并经过全生命周期容量 gate；Commons 有界路径复用预分配事件槽，
且 `shutdownNow()` 不追踪并返回未开始任务。后续若优化，必须在保留 accepted sequence、
全生命周期容量、精确 `shutdownNow` 所有权和真实 termination 的前提下重构任务表示；不能
通过恢复 Commons 的空返回或绕过统一入口来换吞吐。

## 静态边界审计

- `System.out/System.err`、`CallerRuns`、`DiscardPolicy` 在三个生产模块中均为零；
- `UnsupportedOperationException` 只出现在不可变集合契约测试和 core 测试桩，生产代码为零；
- `EventLoopKernel`、`EventLoopWorker`、`TaskAdmissionGate`、`AcceptedTaskRegistry` 各只有一个生产实现；
- `PipelineLifecycle` 在 core/concurrent/autoconfigure 源码中为零；
- 默认 benchmarks jar 与默认 dependency tree 均不包含 `cn.wjybxx`；只有显式
  `commons-baseline` profile 才加入参考源码和 artifact；
- `git diff --check` 通过。
