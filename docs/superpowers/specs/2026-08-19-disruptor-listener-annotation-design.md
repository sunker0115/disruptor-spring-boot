# 设计：`@DisruptorListener` 注解式监听

日期：2026-08-19
状态：已确认，待实现

## Context（背景与动机）

当前 starter 只提供命令式订阅：使用方需拿到 `ConsumerRegistry` 手动调
`subscribe(Class<T>, Consumer<T>)` 登记「事件类型 → 处理逻辑」。这与 Spring 生态里
`@EventListener` / `@KafkaListener` / `@RabbitListener` 的声明式写法不一致，使用方需写
样板注册代码，不够直观。

目标：新增声明式监听能力，对齐 Spring `@EventListener` 的使用体验——在任意 Spring bean
的方法上加 `@DisruptorListener`，容器启动时自动扫描注册，无需手动 `subscribe`。

## 目标 / 非目标

**目标**
- 方法级 `@DisruptorListener` 注解，方法参数类型即监听的事件类型。
- 容器启动时自动扫描所有 bean、注册标注方法。
- `@Order` 控制同一事件类型下多个监听器的调用顺序。
- 兼容 GraalVM native image。

**非目标（对齐时明确砍掉的 Spring 高级特性）**
- SpEL `condition` 条件过滤。
- 监听方法返回值链式再发布。
- 注解显式指定事件类型 / 一个方法监听多种类型（只靠单参数推断）。

## 设计

### 1. 注解 `@DisruptorListener`

方法级注解，无属性。用 Spring 的 `@Reflective` 元注解标注以获得 native 支持（见第 5 节）。

```java
package com.sstlfsj.disruptor.event;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Reflective   // org.springframework.aot.hint.annotation.Reflective
public @interface DisruptorListener {
}
```

约束：标注方法**必须恰好一个参数**，该参数类型即事件类型。

使用示例：
```java
@Component
public class OrderSubscriber {

    @DisruptorListener
    public void onOrder(OrderCreatedEvent e) { ... }

    @DisruptorListener
    @Order(1)                 // 值越小越先，同类型多监听器时生效
    public void audit(OrderCreatedEvent e) { ... }
}
```

### 2. 注册器 `DisruptorListenerRegistrar`

实现 `SmartInitializingSingleton`（对齐 Spring `EventListenerMethodProcessor` 的触发时机：
所有单例初始化后触发一次）。依赖注入 `ConsumerRegistry` 与 `ConfigurableListableBeanFactory`
（用于遍历 bean 名并按需取 bean）。

启动流程：
1. 遍历容器所有 bean，用 `ReflectionUtils.doWithMethods` + `AnnotatedElementUtils` 找出标注
   `@DisruptorListener` 的方法（注意用**目标类**而非代理类扫描，`AopUtils.getTargetClass`）。
2. **校验**：方法参数数 ≠ 1 → 抛 `IllegalStateException`，消息含类名+方法名（fail-fast，
   不静默失效）。
3. 收集 `(eventType = 方法参数类型, method, bean, order)`，按 `eventType` 分组，组内用
   `AnnotationAwareOrderComparator` 依据 `@Order` 升序排序。
4. 依次 `consumerRegistry.subscribe(eventType, payload -> 反射调用 method)` 落地。

排序能生效的原因：`DefaultConsumerRegistry` 用 `CopyOnWriteArrayList` 按 `subscribe` 调用
顺序追加、`dispatch` 按此顺序遍历——注册器排好序后依次 subscribe 即保证调用顺序。

### 3. 调用方式：反射（与 Spring 一致）

注册时一次性 `ReflectionUtils.makeAccessible(method)`，运行时反射调用：
```java
registry.subscribe(eventType,
        payload -> ReflectionUtils.invokeMethod(method, bean, payload));
```
理由：事件消费本就异步、单后台线程串行，业务耗时（日志/DB/通知）远大于反射开销（纳秒级、
JIT 会优化 accessor）；Spring `@EventListener` 官方实现同样是 `method.invoke`。不引入
LambdaMetafactory/MethodHandle 的额外复杂度（YAGNI，且二者对 native 支持反而更麻烦）。

`subscribe` 是泛型 `<T>`，注册器持有的是 `Class<?>` 与 `Consumer<Object>`，落地处用
`@SuppressWarnings("unchecked")` 做一次强转（`DefaultConsumerRegistry` 内部本就以
`Class<?>` 存储）。

### 4. 与现有能力的关系

- **命令式 `subscribe` 保留不变**：注解层是叠加的语法糖，底层同样落到
  `ConsumerRegistry.subscribe`；现有契约测试不受影响。
- **异常隔离复用**：反射调用抛出的异常由 `DefaultConsumerRegistry.dispatch` 已有的
  per-consumer try-catch 兜住，消费线程不受影响（本次审查 F2 已修）。
- **`@Order` 边界**：只保证**注解监听器之间**的相对顺序；注解式与命令式 `subscribe` 混用时，
  两者的相对先后不保证（命令式按运行时调用时机注册）。此边界写入 README。

### 5. GraalVM native image 支持

问题：native image 闭世界静态分析，运行时反射目标必须在构建期登记，否则方法被裁剪或
`method.invoke` 运行时失败。

解法：`@DisruptorListener` 加 `@Reflective` 元注解，复用 Spring AOT 的
`ReflectiveProcessorBeanFactoryInitializationAotProcessor`——构建期自动为所有标注方法注册
反射 INVOKE hints。这正是 Spring 为 `@EventListener` 采用的机制
（`@Reflective(EventListenerReflectiveProcessor.class)`）。

- 普通 JVM：`@Reflective` 仅为惰性元数据，无运行时开销/行为影响。
- native image：自动注册 hints，反射调用正常，**无需自写任何 RuntimeHints 代码**。
- `@Reflective` 来自 `spring-core`（已随 `spring-boot-autoconfigure` 传递引入），不新增依赖。

### 6. 自动装配与包位置

- `DisruptorAutoConfiguration` 新增
  `@Bean @ConditionalOnMissingBean DisruptorListenerRegistrar`。
- 注解 `DisruptorListener` 与注册器 `DisruptorListenerRegistrar` 放
  `com.sstlfsj.disruptor.event` 包（与 `ConsumerRegistry` 同属订阅范畴）。

## 测试

沿用现有 `ApplicationContextRunner` 黑盒风格，`withUserConfiguration` 注入带监听方法的测试 bean：
1. `@DisruptorListener` 方法能被自动注册并异步收到事件。
2. 同一类型多个监听器按 `@Order` 升序执行（用列表记录顺序断言）。
3. 非单参数方法在启动时 fail-fast 抛 `IllegalStateException`。

## 依赖影响

无新增依赖。`@Reflective`、`@Order`、`AnnotationAwareOrderComparator`、`ReflectionUtils`
均来自 `spring-core`（已传递引入）。
```
