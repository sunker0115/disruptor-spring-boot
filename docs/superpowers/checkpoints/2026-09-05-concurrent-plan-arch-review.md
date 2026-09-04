# Concurrent plan / architecture review 恢复检查点

> 保存时间：2026-09-05。本文只记录跨会话恢复所需事实；设计契约仍以
> `../specs/2026-09-02-disruptor-concurrent-design.md` 为准，任务顺序仍以
> `../plans/2026-09-02-disruptor-concurrent.md` 为准。

## 当前结论

`disruptor-concurrent` 不复制 Commons 的类型体系，而是在本项目最终模块边界内做能力覆盖：

- `disruptor-core` 提供通用 `SupervisedLifecycle`、`WorkerSupervisor` 和显式有界/无界 `ShutdownDeadline`。
- `disruptor-concurrent` 只有一个 `EventLoopKernel`、一个 worker 实现和一套关闭后端。
- 有界与无界 EventLoop 只替换 `TaskQueue` 和容量 gate，不复制调度、Future、取消、module 或生命周期逻辑。
- `EventLoopGroup` 是固定 child 集合的唯一生命周期 owner；child 保留任务提交和状态查询能力，但不得独立启动或关闭。
- Spring 只管理根 EventLoop/EventLoopGroup；Group 使用同一个冻结的绝对 deadline 聚合真实 termination。
- 最终以 Commons 能力矩阵、全量测试、竞态/压力测试和 JMH 结果证明覆盖、增强与有意不复制项，不能用“API 兼容”代替能力验证。

## 已落地并提交

按提交顺序：

```text
ac89b13 docs: close disruptor concurrent architecture contracts
52b9569 fix(core): close managed publication races
4b6b059 refactor(core): generalize supervised lifecycle
88bc643 feat(concurrent): define public concurrency contracts
4df7c3c feat(concurrent): add task cancellation and module primitives
32b5ea1 feat(concurrent): add admission registry and task queues
fcd33cc feat(concurrent): complete bounded event loop runtime
28f06fb feat(concurrent): add unbounded event loop facade
```

已验证事实：

- concurrent 84 个测试与 core 131 个测试在开始 Group 工作前全部通过。
- 有界/无界后端已运行同一套参数化契约，包括 4×1000 多生产者、四种调度模式、同 loop 阻塞保护、`shutdownNow` 原始任务顺序和真实 termination。
- 反射测试确认两个公共门面共享同一个 kernel；无界 segment 分配/回收已有快照测试。
- Future 取消竞态、启动 outcome 竞态、managed publication 竞态已修正。

## 当前正在实施：EventLoopGroup

架构不变量：

1. Group 使用共享 owner admission gate；提交先取得 owner lease，再进入 child 本地 admission。
2. owner lease 必须持有到 child publish/rollback 完成，Group 关闭先关 owner gate、等待 lease 清零，再广播关闭。
3. Group 为所有 child 冻结并广播同一个 `ShutdownDeadline` 对象；模式只允许 graceful 升级为 immediate，不替换 deadline。
4. child 公共 `start/shutdown/shutdownNow/requestShutdown/close` 必须抛 `ChildLifecycleOwnershipException`；只有 Group 持有的私有 token 能走 package-private owner 路径。
5. child 仍使用现有唯一 `EventLoopKernel`，不能新增 Group 专用 kernel 或通过“先检查 parent、再提交”的竞态补丁实现。
6. 任一 child 启动或基础设施失败触发整个 Group fail-stop；启动失败保留精确原始首因，并等待全部 child 真实终止。
7. `shutdownNow()` 聚合顺序为 `childIndex asc -> child acceptedSequence asc`，不虚构跨 child 全局提交顺序。
8. Group termination 只有在所有 child worker 真实退出后才完成；迟到 child 事实只能补充会话，不能重开广播或替换已冻结 outcome。

当前未提交文件：

```text
disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/ChildLifecycleOwnershipException.java
disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupBuilder.java
disruptor-concurrent/src/main/java/com/sstlfsj/disruptor/concurrent/internal/GroupLifecycleCoordinator.java
disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupShutdownTest.java
disruptor-concurrent/src/test/java/com/sstlfsj/disruptor/concurrent/EventLoopGroupTest.java
```

`GroupLifecycleCoordinator.java` 已写入 561 行，但尚未与 `AbstractEventLoop`、`EventLoopKernel` 和新的 `DisruptorEventLoopGroup` 接通；因此当前 Group 测试是预期红灯，不能把该状态误判为实现完成。

## 恢复后的直接执行顺序

1. 先完整审查 `GroupLifecycleCoordinator`，重点检查 startup/shutdown 并发、首因冻结、广播串行化、同 deadline 身份以及真实终止条件。
2. 修改 `AbstractEventLoop`，加入 parent/childIndex/owner token 绑定和 owner-only 生命周期入口。
3. 修改 `EventLoopKernel`，在本地 admission 前取得 Group owner lease，并为 owner 增加接收同一 deadline 的 `shutdownNow` 路径。
4. 新建 `DisruptorEventLoopGroup`，完成固定 child、稳定 affinity、round-robin、提交时选 child、聚合快照与关闭。
5. 运行 Group 两组定向测试，修正到绿后运行 concurrent/core 全量测试并提交 Task 7。
6. 继续实施 Spring 根对象集成、Commons 能力矩阵、全量/竞态/压力测试与 JMH；这些完成前不能宣称整个目标完成。

## 恢复时的验证命令

本机 Maven 固定使用 JDK 21 与项目 Maven 3.9.9：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
$MVN -pl disruptor-concurrent -am \
  -Dtest=EventLoopGroupTest,EventLoopGroupShutdownTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

全仓验证可能因 tutorial 测试绑定本地随机端口而需要在沙箱外执行；不得用禁用测试、mock 掉并发边界或兼容层换取绿灯。
