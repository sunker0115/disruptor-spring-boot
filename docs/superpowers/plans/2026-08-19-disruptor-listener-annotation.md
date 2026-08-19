# @DisruptorListener 注解式监听 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让使用方能在任意 Spring bean 的方法上加 `@DisruptorListener`，容器启动时自动注册到事件总线，无需手动 `subscribe`；支持 `@Order` 排序与 GraalVM native image。

**Architecture:** 新增方法级注解 `@DisruptorListener`（用 Spring `@Reflective` 元注解支持 native），配套 `DisruptorListenerRegistrar`（`SmartInitializingSingleton`）在单例就绪后扫描全部 bean 的标注方法，按事件类型分组、按 `@Order` 升序排序，用反射封装成 `Consumer` 注册到现有 `ConsumerRegistry`。注解层是命令式 `subscribe` 的语法糖，底层复用同一套分发与异常隔离。

**Tech Stack:** Java 17、Spring Boot 4.1（Spring Framework 7.0）、LMAX Disruptor 4.0、JUnit 5、AssertJ、`spring-boot-test` 的 `ApplicationContextRunner`。所有新依赖类均来自已传递引入的 `spring-core`/`spring-beans`，不新增依赖。

---

## File Structure

- Create: `src/main/java/com/sstlfsj/disruptor/event/DisruptorListener.java` — 方法级注解，含 `@Reflective`。
- Create: `src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java` — 启动扫描 + 注册逻辑。
- Modify: `src/main/java/com/sstlfsj/disruptor/DisruptorAutoConfiguration.java` — 新增 registrar bean。
- Create: `src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java` — 黑盒验收测试。
- Modify: `README.md` — 补注解式监听用法、`@Order`、native、混用边界。

## 运行测试的环境前缀

本机 `mvn` 不在 PATH，所有 `mvn` 命令前先设置（JDK 需 ≥17，用 21）：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
cd /Users/sunke/dev/ai-project/disruptor-spring-boot-starter
```

---

## Task 1: `@DisruptorListener` 注解

**Files:**
- Create: `src/main/java/com/sstlfsj/disruptor/event/DisruptorListener.java`

- [ ] **Step 1: 创建注解**

```java
package com.sstlfsj.disruptor.event;

import org.springframework.aot.hint.annotation.Reflective;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注在 Spring bean 的方法上，声明式订阅事件总线。方法必须恰好一个参数，
 * 该参数类型即监听的事件类型；容器启动时由 {@link DisruptorListenerRegistrar}
 * 自动注册到 {@link ConsumerRegistry}。
 *
 * <p>同一事件类型有多个监听器时，可配合 {@code @org.springframework.core.annotation.Order}
 * 控制调用顺序（值越小越先）。</p>
 *
 * <p>用 {@link Reflective} 元注解标注，使 Spring AOT 在构建期为标注方法注册反射
 * INVOKE hints，从而兼容 GraalVM native image。</p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Reflective
public @interface DisruptorListener {
}
```

- [ ] **Step 2: 编译验证**

Run: `$MVN -q compile`
Expected: 编译通过，无错误（`@Reflective` 可正常解析）。

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/sstlfsj/disruptor/event/DisruptorListener.java
git commit -m "feat: 新增 @DisruptorListener 方法级注解(含 @Reflective 支持 native)"
```

---

## Task 2: 注册器骨架 + 自动注册（最小实现）

先只做「扫描标注方法 + 反射注册」，不含排序与参数校验（分别在 Task 3、4 用测试驱动加入）。

**Files:**
- Create: `src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java`
- Modify: `src/main/java/com/sstlfsj/disruptor/DisruptorAutoConfiguration.java`
- Test: `src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java`

- [ ] **Step 1: 写失败测试（自动注册生效）**

创建 `src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java`：

```java
package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.event.DisruptorListener;
import com.sstlfsj.disruptor.event.EventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 注解式监听黑盒验收测试：自动注册、@Order 顺序、非单参数 fail-fast。
 */
class DisruptorListenerTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    public record OrderEvent(String id) {
    }

    static final CountDownLatch AUTO_LATCH = new CountDownLatch(1);
    static final AtomicReference<String> AUTO_RECEIVED = new AtomicReference<>();

    @Configuration
    static class AutoRegisterConfig {
        @Bean
        AutoRegisterListener autoRegisterListener() {
            return new AutoRegisterListener();
        }
    }

    static class AutoRegisterListener {
        @DisruptorListener
        public void onOrder(OrderEvent e) {
            AUTO_RECEIVED.set(e.id());
            AUTO_LATCH.countDown();
        }
    }

    @Test
    void annotatedMethodIsAutoRegistered() {
        runner.withUserConfiguration(AutoRegisterConfig.class).run(ctx -> {
            EventPublisher publisher = ctx.getBean(EventPublisher.class);
            publisher.publish(new OrderEvent("A-1"));
            assertTrue(AUTO_LATCH.await(3, TimeUnit.SECONDS), "@DisruptorListener 方法应被自动注册并收到事件");
            assertEquals("A-1", AUTO_RECEIVED.get());
        });
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `$MVN test -Dtest=DisruptorListenerTest#annotatedMethodIsAutoRegistered`
Expected: FAIL —— 事件发布后监听器未被调用（`AUTO_LATCH.await` 超时返回 false），因为 registrar 尚不存在。

- [ ] **Step 3: 创建注册器（最小版）**

创建 `src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java`：

```java
package com.sstlfsj.disruptor.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.function.Consumer;

/**
 * 在所有单例初始化后（{@link SmartInitializingSingleton}），扫描容器中所有 bean 的
 * {@link DisruptorListener} 标注方法，并注册到 {@link ConsumerRegistry}。
 */
public class DisruptorListenerRegistrar implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(DisruptorListenerRegistrar.class);

    private final ConsumerRegistry consumerRegistry;
    private final ConfigurableListableBeanFactory beanFactory;

    public DisruptorListenerRegistrar(ConsumerRegistry consumerRegistry,
                                      ConfigurableListableBeanFactory beanFactory) {
        this.consumerRegistry = consumerRegistry;
        this.beanFactory = beanFactory;
    }

    @Override
    public void afterSingletonsInstantiated() {
        int count = 0;
        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            Class<?> type = beanFactory.getType(beanName);
            if (type == null) {
                continue;
            }
            for (Method method : type.getMethods()) {
                if (!method.isAnnotationPresent(DisruptorListener.class)) {
                    continue;
                }
                Class<?> eventType = method.getParameterTypes()[0];
                Object bean = beanFactory.getBean(beanName);
                register(eventType, bean, method);
                count++;
            }
        }
        log.debug("已注册 {} 个 @DisruptorListener 监听方法", count);
    }

    @SuppressWarnings("unchecked")
    private void register(Class<?> eventType, Object bean, Method method) {
        ReflectionUtils.makeAccessible(method);
        Consumer<Object> consumer = payload -> ReflectionUtils.invokeMethod(method, bean, payload);
        consumerRegistry.subscribe((Class<Object>) eventType, consumer);
    }
}
```

- [ ] **Step 4: 在自动装配中注册 registrar bean**

修改 `src/main/java/com/sstlfsj/disruptor/DisruptorAutoConfiguration.java`，新增 import 与 bean 方法。

新增 import（与现有 import 同区）：
```java
import com.sstlfsj.disruptor.event.DisruptorListenerRegistrar;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
```

在 `eventPublisher` bean 方法之后、类结束大括号之前，新增：
```java
    @Bean
    @ConditionalOnMissingBean
    public DisruptorListenerRegistrar disruptorListenerRegistrar(ConsumerRegistry consumerRegistry,
                                                                 ConfigurableListableBeanFactory beanFactory) {
        return new DisruptorListenerRegistrar(consumerRegistry, beanFactory);
    }
```

- [ ] **Step 5: 跑测试确认通过**

Run: `$MVN test -Dtest=DisruptorListenerTest#annotatedMethodIsAutoRegistered`
Expected: PASS —— 监听器被自动注册并收到 `OrderEvent("A-1")`。

- [ ] **Step 6: 跑全量测试确认无回归**

Run: `$MVN test`
Expected: PASS，`Tests run: 6`（原 4 契约 + AutoConfigurationImportsTest 1 + 本测试 1）。

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java \
        src/main/java/com/sstlfsj/disruptor/DisruptorAutoConfiguration.java \
        src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java
git commit -m "feat: 自动扫描并注册 @DisruptorListener 监听方法"
```

---

## Task 3: `@Order` 消费顺序

**Files:**
- Modify: `src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java`
- Test: `src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java`

- [ ] **Step 1: 写失败测试（@Order 决定顺序）**

在 `DisruptorListenerTest` 类中追加静态字段、测试配置与测试方法：

```java
    static final java.util.List<String> ORDER_TRACE = new java.util.concurrent.CopyOnWriteArrayList<>();
    static final CountDownLatch ORDER_LATCH = new CountDownLatch(2);

    public record PayEvent(String id) {
    }

    @Configuration
    static class OrderedConfig {
        @Bean
        OrderedListeners orderedListeners() {
            return new OrderedListeners();
        }
    }

    static class OrderedListeners {
        @DisruptorListener
        @org.springframework.core.annotation.Order(2)
        public void second(PayEvent e) {
            ORDER_TRACE.add("second");
            ORDER_LATCH.countDown();
        }

        @DisruptorListener
        @org.springframework.core.annotation.Order(1)
        public void first(PayEvent e) {
            ORDER_TRACE.add("first");
            ORDER_LATCH.countDown();
        }
    }

    @Test
    void listenersRunInOrderAnnotationSequence() {
        runner.withUserConfiguration(OrderedConfig.class).run(ctx -> {
            EventPublisher publisher = ctx.getBean(EventPublisher.class);
            publisher.publish(new PayEvent("P-1"));
            assertTrue(ORDER_LATCH.await(3, TimeUnit.SECONDS), "两个监听器都应被调用");
            assertEquals(java.util.List.of("first", "second"), ORDER_TRACE,
                    "应按 @Order 升序调用：first(1) 先于 second(2)");
        });
    }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `$MVN test -Dtest=DisruptorListenerTest#listenersRunInOrderAnnotationSequence`
Expected: FAIL —— 顺序按方法反射遍历顺序（`getMethods()` 顺序不保证），断言 `List.of("first","second")` 很可能不成立。

- [ ] **Step 3: 重写注册器加入分组排序**

将 `DisruptorListenerRegistrar` 的 `afterSingletonsInstantiated` 与相关辅助整体替换为下面版本（新增按事件类型分组、按 `@Order` 升序）：

```java
    @Override
    public void afterSingletonsInstantiated() {
        // 先收集全部标注方法，再按事件类型分组、组内按 @Order 升序，最后依次注册，
        // 使 ConsumerRegistry 的追加顺序 = 期望调用顺序。
        Map<Class<?>, List<ListenerMethod>> grouped = new LinkedHashMap<>();
        for (String beanName : beanFactory.getBeanDefinitionNames()) {
            Class<?> type = beanFactory.getType(beanName);
            if (type == null) {
                continue;
            }
            for (Method method : type.getMethods()) {
                if (!method.isAnnotationPresent(DisruptorListener.class)) {
                    continue;
                }
                Class<?> eventType = method.getParameterTypes()[0];
                Object bean = beanFactory.getBean(beanName);
                grouped.computeIfAbsent(eventType, k -> new ArrayList<>())
                        .add(new ListenerMethod(bean, method, orderOf(method)));
            }
        }

        int count = 0;
        for (Map.Entry<Class<?>, List<ListenerMethod>> entry : grouped.entrySet()) {
            List<ListenerMethod> methods = entry.getValue();
            methods.sort(Comparator.comparingInt(ListenerMethod::order));
            for (ListenerMethod lm : methods) {
                register(entry.getKey(), lm.bean(), lm.method());
                count++;
            }
        }
        log.debug("已注册 {} 个 @DisruptorListener 监听方法", count);
    }

    private static int orderOf(Method method) {
        Order order = AnnotatedElementUtils.findMergedAnnotation(method, Order.class);
        return order != null ? order.value() : Ordered.LOWEST_PRECEDENCE;
    }

    private record ListenerMethod(Object bean, Method method, int order) {
    }
```

并在文件顶部补充 import：
```java
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.Order;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `$MVN test -Dtest=DisruptorListenerTest#listenersRunInOrderAnnotationSequence`
Expected: PASS —— 调用顺序为 `["first", "second"]`。

- [ ] **Step 5: 跑全量测试确认无回归**

Run: `$MVN test`
Expected: PASS，`Tests run: 7`。

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java \
        src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java
git commit -m "feat: @DisruptorListener 支持 @Order 控制同类型消费顺序"
```

---

## Task 4: 非单参数方法 fail-fast

**Files:**
- Modify: `src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java`
- Test: `src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java`

- [ ] **Step 1: 写失败测试（非单参数启动报错）**

在 `DisruptorListenerTest` 中追加：

```java
    @Configuration
    static class InvalidSignatureConfig {
        @Bean
        InvalidListener invalidListener() {
            return new InvalidListener();
        }
    }

    static class InvalidListener {
        @DisruptorListener
        public void twoArgs(OrderEvent e, String extra) {
        }
    }

    @Test
    void nonSingleParamMethodFailsFast() {
        runner.withUserConfiguration(InvalidSignatureConfig.class).run(ctx ->
                org.junit.jupiter.api.Assertions.assertTrue(
                        ctx.getStartupFailure() != null
                                && hasIllegalStateInChain(ctx.getStartupFailure()),
                        "非单参数 @DisruptorListener 方法应导致启动失败(IllegalStateException)"));
    }

    private static boolean hasIllegalStateInChain(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IllegalStateException) {
                return true;
            }
        }
        return false;
    }
```

说明：`ApplicationContextRunner` 捕获启动异常而非抛出，通过 `ctx.getStartupFailure()` 取上下文启动失败，沿 cause 链找 `IllegalStateException`。

- [ ] **Step 2: 跑测试确认失败**

Run: `$MVN test -Dtest=DisruptorListenerTest#nonSingleParamMethodFailsFast`
Expected: FAIL —— 当前实现对两参数方法取 `getParameterTypes()[0]` 照常注册，不抛异常，`getStartupFailure()` 为 null。

- [ ] **Step 3: 在扫描处加入参数校验**

在 `DisruptorListenerRegistrar.afterSingletonsInstantiated` 的内层循环中，紧接 `if (!method.isAnnotationPresent(...)) continue;` 之后、取 `eventType` 之前，插入校验：

```java
                if (method.getParameterCount() != 1) {
                    throw new IllegalStateException(
                            "@DisruptorListener 方法必须恰好一个参数：" + method.getDeclaringClass().getName()
                                    + "#" + method.getName() + " 有 " + method.getParameterCount() + " 个参数");
                }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `$MVN test -Dtest=DisruptorListenerTest#nonSingleParamMethodFailsFast`
Expected: PASS —— 启动因 `IllegalStateException` 失败，被断言捕获。

- [ ] **Step 5: 跑全量测试确认无回归**

Run: `$MVN test`
Expected: PASS，`Tests run: 8`。

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/sstlfsj/disruptor/event/DisruptorListenerRegistrar.java \
        src/test/java/com/sstlfsj/disruptor/DisruptorListenerTest.java
git commit -m "feat: @DisruptorListener 非单参数方法启动 fail-fast"
```

---

## Task 5: 更新 README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 在「3. 注册消费者」小节后新增注解式监听小节**

在 `README.md` 的「### 3. 注册消费者」代码块之后、「### 4. 发布事件」之前，插入：

````markdown
### 3.1 声明式监听（推荐）

除手动 `subscribe`，也可在任意 Spring bean 的方法上加 `@DisruptorListener`，容器启动时自动注册。
方法必须恰好一个参数，参数类型即监听的事件类型：

```java
@Component
public class OrderSubscriber {

    @DisruptorListener
    public void onOrder(OrderCreatedEvent e) {
        // 处理下单事件
    }

    @DisruptorListener
    @Order(1)                 // 同类型多监听器时，值越小越先调用
    public void audit(OrderCreatedEvent e) {
        // 审计
    }
}
```

- **参数校验**：方法参数数不为 1 时，应用启动即失败（fail-fast），不会静默不生效。
- **与命令式混用**：`@Order` 只保证注解监听器之间的相对顺序；注解式与命令式 `subscribe`
  混用时，两者的相对先后不保证（命令式按运行时调用时机注册）。
- **GraalVM native image**：`@DisruptorListener` 已用 Spring `@Reflective` 元注解标注，
  Spring AOT 会在构建期自动注册反射 hints，native 镜像下无需额外配置。
````

- [ ] **Step 2: 更新「覆盖默认 bean」说明中的 bean 列表（如已存在则补充）**

在 `README.md`「## 覆盖默认 bean」小节，确保描述提到自动装配还提供
`DisruptorListenerRegistrar`。若原文仅列 `EventPublisher`/`ConsumerRegistry`，在其后补一句：

```markdown
> 自动装配还提供 `DisruptorListenerRegistrar`（负责扫描 `@DisruptorListener`），同样可通过声明同类型 bean 覆盖。
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README 补充 @DisruptorListener 声明式监听用法"
```

---

## Self-Review 记录

- **Spec 覆盖**：注解(Task1) / 自动注册(Task2) / @Order(Task3) / fail-fast(Task4) / native `@Reflective`(Task1) / README 含混用边界与 native(Task5) / 不新增依赖（全用 spring-core 已有类）——逐条有对应任务。
- **类型一致性**：`DisruptorListenerRegistrar(ConsumerRegistry, ConfigurableListableBeanFactory)` 构造签名在 Task2 定义、Task2 Step4 的 bean 方法与之匹配；`ListenerMethod(bean, method, order)` 在 Task3 定义并在同任务使用；`register(Class<?>, Object, Method)` 贯穿 Task2/3 一致。
- **无占位符**：每个改动步骤均给出完整代码与确切命令、预期输出。
- **测试计数**：初始 5（Task 前）→ 6（Task2）→ 7（Task3）→ 8（Task4），与全量测试步骤预期一致。
```
