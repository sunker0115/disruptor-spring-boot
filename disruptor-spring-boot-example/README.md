# concurrent 示例

本模块同时给出不使用 Spring 的独立入口，以及由 Spring 管理生命周期的入口。默认选择有界 `EventLoop`，用于将积压限制在可预期范围；无界队列只适合可控的内部短突发或控制流，不能作为上游无限生产的替代方案。

## 运行

在仓库根目录执行：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
$MVN -pl disruptor-spring-boot-example -am test
$MVN -pl disruptor-spring-boot-example org.codehaus.mojo:exec-maven-plugin:3.5.0:java -Dexec.mainClass=com.sstlfsj.disruptor.example.nospring.PureJavaConcurrentExample
$MVN -pl disruptor-spring-boot-example spring-boot:run
```

纯 Java 入口会输出 one-shot 的 `request-42` 上下文、`tryExecute` 接收结果、两次动态调度、`demo-complete` 取消事件，以及一个仅用于内部短突发的 unbounded loop，然后优雅关闭并等待终止。Spring 启动会运行已有 demo 与 `demo6`，其中输出 `order-42@tenant-a`、相同 affinity key 的同一 worker、fixed-rate 取消和 `demo-complete`。

## 选择 API

| 需求 | 选择 | 说明 |
| --- | --- | --- |
| 单线程保序工作 | 单个 `EventLoop` | 用 `bounded(name, capacity)`；独立 Java 程序自行 `start`/关闭。 |
| 固定并行度与分片保序 | `EventLoopGroup` | 通过 `select(affinityKey)` 让同一 key 落到同一 child。 |
| 不可丢失的普通任务 | `execute` 或 `submit` | 接收失败会抛出，调用方按业务处理。 |
| 需要立即反馈容量不足 | `tryExecute` | 返回 `false` 时立刻限流、降级或回推上游。 |
| 延迟、固定频率或动态间隔 | `schedule` / `scheduleAtFixedRate` | `ScheduledTaskSpec` 支持 one-shot 与 dynamic delay；周期任务应保存 future 并取消。 |
| 正常停机 | `shutdown` 后 `awaitTermination` | 停止接收新任务并尽量完成已接收工作。 |
| 进程必须尽快退出 | `shutdownNow` | 中断/返回未执行工作，只用于无法继续等待的场景。 |

Spring 应用中，`ConcurrentExampleConfiguration` 声明的根对象由 `DisruptorConcurrentLifecycle` 启停；业务 runner 只提交任务，不直接调用 `shutdown` 或 `shutdownNow`。纯 Java 示例则由 `try/finally` 负责 graceful shutdown、超时后的 immediate fallback 和 `awaitTermination`，避免遗留后台线程。
