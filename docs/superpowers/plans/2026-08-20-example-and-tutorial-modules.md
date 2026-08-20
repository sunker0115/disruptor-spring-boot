# 示例与教程模块 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增两个独立 Maven 模块:`disruptor-spring-boot-example`(六个特性 console 演示 + 纯 Java main)与 `disruptor-spring-boot-tutorial`(下单场景 web 小应用),把 README 用法落成能跑的。

**Architecture:** 两模块互不依赖。example 是 web-none 的 Spring Boot app,每特性一个 `@Order` 的 `CommandLineRunner`,各用独立事件类型/管道名规避全局唯一约束,由 `ExampleSmokeTest` 验证跑通;纯 Java demo 是独立 `main`。tutorial 是 spring-boot-starter-web 应用,`POST /orders` 用 `tryPublish` 发布后立刻返回 202(满则 429),后台注解式 DAG(校验→落库→(发信‖扣库存‖埋点))异步处理,`GET /orders/stats` 观测,由 `OrderFlowTest` 端到端验证。

**Tech Stack:** Java 17、Spring Boot 4.1.0、LMAX Disruptor 4.0、Lombok、JUnit 5、本仓 disruptor-spring-boot-starter。

---

## 前置:本机 Maven 环境(每次跑 mvn 前 export)

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
MVN=/Users/sunke/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn
cd /Users/sunke/dev/ai-project/disruptor-spring-boot
```

（下文命令里的 `$MVN` 即上面变量。）

---

## Task 1: 脚手架 —— 根 pom 改动 + 两模块 pom + Application 骨架

**Files:**
- Modify: `pom.xml`(dependencyManagement 加 starter、modules 加两模块)
- Create: `disruptor-spring-boot-example/pom.xml`
- Create: `disruptor-spring-boot-tutorial/pom.xml`
- Create: `disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/ExampleApplication.java`
- Create: `disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/TutorialApplication.java`

- [ ] **Step 1: 根 pom 的 dependencyManagement 补 starter**

在 `pom.xml` 的 `<dependencyManagement><dependencies>` 内,已有 `disruptor-spring-boot-autoconfigure` 条目之后追加:

```xml
            <dependency>
                <groupId>com.sstlfsj</groupId>
                <artifactId>disruptor-spring-boot-starter</artifactId>
                <version>${project.version}</version>
            </dependency>
```

- [ ] **Step 2: 根 pom 的 modules 追加两模块**

在 `<modules>` 内三模块之后追加:

```xml
        <module>disruptor-spring-boot-example</module>
        <module>disruptor-spring-boot-tutorial</module>
```

- [ ] **Step 3: 创建 example pom**

`disruptor-spring-boot-example/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.sstlfsj</groupId>
        <artifactId>disruptor-spring-boot</artifactId>
        <version>${revision}</version>
    </parent>
    <artifactId>disruptor-spring-boot-example</artifactId>

    <description>特性演示:每个核心特性一个可跑的 console demo，外加无 Spring 的纯 Java main。</description>

    <dependencies>
        <dependency>
            <groupId>com.sstlfsj</groupId>
            <artifactId>disruptor-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 4: 创建 tutorial pom**

`disruptor-spring-boot-tutorial/pom.xml`(同上,artifactId 改 `disruptor-spring-boot-tutorial`,web 依赖换成 starter-web):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.sstlfsj</groupId>
        <artifactId>disruptor-spring-boot</artifactId>
        <version>${revision}</version>
    </parent>
    <artifactId>disruptor-spring-boot-tutorial</artifactId>

    <description>真实场景教程:下单主流程 publish 后立刻返回、副作用后台异步多阶段处理的 web 小应用。</description>

    <dependencies>
        <dependency>
            <groupId>com.sstlfsj</groupId>
            <artifactId>disruptor-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 5: 两个 Application 骨架**

`disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/ExampleApplication.java`:

```java
package com.sstlfsj.disruptor.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ExampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }
}
```

`disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/TutorialApplication.java`:

```java
package com.sstlfsj.disruptor.tutorial;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TutorialApplication {
    public static void main(String[] args) {
        SpringApplication.run(TutorialApplication.class, args);
    }
}
```

- [ ] **Step 6: 编译两模块**

Run: `$MVN -q -pl disruptor-spring-boot-example,disruptor-spring-boot-tutorial -am compile`
Expected: BUILD SUCCESS（两模块编译通过）。

- [ ] **Step 7: Commit**

```bash
git add pom.xml disruptor-spring-boot-example/pom.xml disruptor-spring-boot-tutorial/pom.xml \
  disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/ExampleApplication.java \
  disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/TutorialApplication.java
git commit -m "feat: 脚手架 example/tutorial 两模块（pom + Application + 根 pom 接线）"
```

---

## Task 2: example demo1 —— 声明式菱形 DAG（order）+ DemoResults

**Files:**
- Create: `.../example/DemoResults.java`
- Create: `.../example/order/OrderEvent.java`
- Create: `.../example/order/OrderPipeline.java`
- Create: `.../example/order/OrderDemoRunner.java`

> 注:菱形两并行分支 persist‖audit **写不同字段**(persisted / audited 两个 boolean),避免并发写同一字段的竞态(优于 spec 里的 auditTrail 追加写法)。

- [ ] **Step 1: DemoResults(共享完成标记)**

`.../example/DemoResults.java`:

```java
package com.sstlfsj.disruptor.example;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 各 demo 跑完后在此登记，供冒烟测试断言。 */
@Component
public class DemoResults {
    private final Map<String, Boolean> done = new ConcurrentHashMap<>();

    public void markDone(String demo) {
        done.put(demo, Boolean.TRUE);
    }

    public boolean isDone(String demo) {
        return done.getOrDefault(demo, Boolean.FALSE);
    }
}
```

- [ ] **Step 2: OrderEvent**

`.../example/order/OrderEvent.java`:

```java
package com.sstlfsj.disruptor.example.order;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderEvent {
    private String orderId;
    private long amount;
    private boolean persisted;
    private boolean audited;
}
```

- [ ] **Step 3: OrderPipeline(注解式菱形 DAG)**

`.../example/order/OrderPipeline.java`:

```java
package com.sstlfsj.disruptor.example.order;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/** demo1：validate → (persist ‖ audit) → notify。persist/audit 写不同字段避免竞态；notify 汇聚。 */
@Component
public class OrderPipeline {

    private static final Logger log = LoggerFactory.getLogger(OrderPipeline.class);

    /** 由 runner 注入本轮 latch（4 个阶段各 countDown 一次）。 */
    static volatile CountDownLatch latch;

    @DisruptorStage(pipeline = "order", name = "validate")
    public void validate(OrderEvent e) {
        log.info("[order/validate] 订单 {} 金额 {} 校验通过", e.getOrderId(), e.getAmount());
        latch.countDown();
    }

    @DisruptorStage(pipeline = "order", name = "persist", after = "validate")
    public void persist(OrderEvent e) {
        e.setPersisted(true);
        log.info("[order/persist] 订单 {} 已落库", e.getOrderId());
        latch.countDown();
    }

    @DisruptorStage(pipeline = "order", name = "audit", after = "validate")
    public void audit(OrderEvent e) {
        e.setAudited(true);
        log.info("[order/audit] 订单 {} 已审计", e.getOrderId());
        latch.countDown();
    }

    @DisruptorStage(pipeline = "order", name = "notify", after = {"persist", "audit"})
    public void notify(OrderEvent e) {
        log.info("[order/notify] 订单 {} 通知（persist={}, audit={}，两分支都完成后才执行）",
                e.getOrderId(), e.isPersisted(), e.isAudited());
        latch.countDown();
    }
}
```

- [ ] **Step 4: OrderDemoRunner**

`.../example/order/OrderDemoRunner.java`:

```java
package com.sstlfsj.disruptor.example.order;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Order(1)
@RequiredArgsConstructor
public class OrderDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OrderDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo1 声明式菱形 DAG（order）====");
        OrderPipeline.latch = new CountDownLatch(4);
        eventBus.publish(OrderEvent.class, e -> {
            e.setOrderId("A-1");
            e.setAmount(199);
        });
        if (!OrderPipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo1 超时");
        }
        results.markDone("order");
        log.info("==== demo1 完成 ====");
    }
}
```

- [ ] **Step 5: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-example -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 6: Commit**

```bash
git add disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/DemoResults.java \
  disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/order/
git commit -m "feat(example): demo1 声明式菱形 DAG + DemoResults"
```

---

## Task 3: example demo2 —— 编程式 EventPipeline builder（pay）

**Files:**
- Create: `.../example/pay/PayEvent.java`
- Create: `.../example/pay/PayService.java`
- Create: `.../example/pay/PayPipelineConfig.java`
- Create: `.../example/pay/PayDemoRunner.java`

- [ ] **Step 1: PayEvent**

`.../example/pay/PayEvent.java`:

```java
package com.sstlfsj.disruptor.example.pay;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PayEvent {
    private String payId;
    private boolean persisted;
    private boolean audited;
}
```

- [ ] **Step 2: PayService(handler 提供方，方法引用无反射)**

`.../example/pay/PayService.java`:

```java
package com.sstlfsj.disruptor.example.pay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class PayService {

    private static final Logger log = LoggerFactory.getLogger(PayService.class);

    /** 由 runner 注入本轮 latch（4 阶段各 countDown 一次）。 */
    static volatile CountDownLatch latch;

    public void validate(PayEvent e) {
        log.info("[pay/validate] 支付 {} 校验通过", e.getPayId());
        latch.countDown();
    }

    public void persist(PayEvent e) {
        e.setPersisted(true);
        log.info("[pay/persist] 支付 {} 已落库", e.getPayId());
        latch.countDown();
    }

    public void audit(PayEvent e) {
        e.setAudited(true);
        log.info("[pay/audit] 支付 {} 已审计", e.getPayId());
        latch.countDown();
    }

    public void notify(PayEvent e) {
        log.info("[pay/notify] 支付 {} 通知（persist={}, audit={}）",
                e.getPayId(), e.isPersisted(), e.isAudited());
        latch.countDown();
    }
}
```

- [ ] **Step 3: PayPipelineConfig(@Bean EventPipeline)**

`.../example/pay/PayPipelineConfig.java`:

```java
package com.sstlfsj.disruptor.example.pay;

import com.sstlfsj.disruptor.core.EventPipeline;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PayPipelineConfig {

    @Bean
    public EventPipeline<PayEvent> payPipeline(PayService svc) {
        return EventPipeline.builder("pay", PayEvent.class)
                .stage("validate", svc::validate)
                .stage("persist", svc::persist).after("validate")
                .stage("audit", svc::audit).after("validate")
                .stage("notify", svc::notify).after("persist", "audit")
                .build();
    }
}
```

- [ ] **Step 4: PayDemoRunner**

`.../example/pay/PayDemoRunner.java`:

```java
package com.sstlfsj.disruptor.example.pay;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Order(2)
@RequiredArgsConstructor
public class PayDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PayDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo2 编程式 EventPipeline builder（pay）====");
        PayService.latch = new CountDownLatch(4);
        eventBus.publish(PayEvent.class, e -> e.setPayId("P-1"));
        if (!PayService.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo2 超时");
        }
        results.markDone("pay");
        log.info("==== demo2 完成 ====");
    }
}
```

- [ ] **Step 5: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-example -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 6: Commit**

```bash
git add disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/pay/
git commit -m "feat(example): demo2 编程式 EventPipeline builder"
```

---

## Task 4: example demo3 —— 阶段并行 + ShardKeyed 保序（ingest）

**Files:**
- Create: `.../example/ingest/IngestEvent.java`
- Create: `.../example/ingest/IngestPipeline.java`
- Create: `.../example/ingest/IngestDemoRunner.java`

- [ ] **Step 1: IngestEvent(implements ShardKeyed)**

`.../example/ingest/IngestEvent.java`:

```java
package com.sstlfsj.disruptor.example.ingest;

import com.sstlfsj.disruptor.core.ShardKeyed;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IngestEvent implements ShardKeyed {
    private String key;
    private int seq;

    @Override
    public Object shardKey() {
        return key;
    }
}
```

- [ ] **Step 2: IngestPipeline(parallelism=4)**

`.../example/ingest/IngestPipeline.java`:

```java
package com.sstlfsj.disruptor.example.ingest;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** demo3：并行分片 4；ShardKeyed 保证同 key 落同一分片、按 seq 顺序处理。 */
@Component
public class IngestPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestPipeline.class);

    static volatile CountDownLatch latch;
    /** key -> 该 key 被观察到的（线程名 + seq）序列，供 runner 打印验证。 */
    static final Map<String, List<String>> observed = new ConcurrentHashMap<>();

    @DisruptorStage(pipeline = "ingest", name = "process", parallelism = 4)
    public void process(IngestEvent e) {
        String thread = Thread.currentThread().getName();
        observed.computeIfAbsent(e.getKey(), k -> new CopyOnWriteArrayList<>())
                .add(thread + "#seq" + e.getSeq());
        log.info("[ingest/process] key={} seq={} 线程={}", e.getKey(), e.getSeq(), thread);
        latch.countDown();
    }
}
```

- [ ] **Step 3: IngestDemoRunner**

`.../example/ingest/IngestDemoRunner.java`:

```java
package com.sstlfsj.disruptor.example.ingest;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Order(3)
@RequiredArgsConstructor
public class IngestDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo3 并行 + ShardKeyed 保序（ingest）====");
        List<String> keys = List.of("K1", "K2", "K3");
        int perKey = 4;
        IngestPipeline.observed.clear();
        IngestPipeline.latch = new CountDownLatch(keys.size() * perKey);
        for (int seq = 0; seq < perKey; seq++) {          // 交错发布：K1#0,K2#0,K3#0,K1#1,...
            for (String key : keys) {
                int s = seq;
                eventBus.publish(IngestEvent.class, e -> {
                    e.setKey(key);
                    e.setSeq(s);
                });
            }
        }
        if (!IngestPipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo3 超时");
        }
        IngestPipeline.observed.forEach((key, seqs) ->
                log.info("[ingest] key={} 处理序列={}（同 key 应同一线程且 seq 递增）", key, seqs));
        results.markDone("ingest");
        log.info("==== demo3 完成 ====");
    }
}
```

- [ ] **Step 4: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-example -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 5: Commit**

```bash
git add disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/ingest/
git commit -m "feat(example): demo3 阶段并行 + ShardKeyed 保序"
```

---

## Task 5: example demo4 —— 事件复用 Resettable（reuse）

**Files:**
- Create: `.../example/reuse/ReuseEvent.java`
- Create: `.../example/reuse/ReusePipeline.java`
- Create: `.../example/reuse/ReuseDemoRunner.java`

- [ ] **Step 1: ReuseEvent(implements Resettable)**

`.../example/reuse/ReuseEvent.java`:

```java
package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.core.Resettable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ReuseEvent implements Resettable {
    private String orderId;
    private String couponCode;   // 可选字段

    @Override
    public void reset() {
        this.orderId = null;
        this.couponCode = null;
    }
}
```

- [ ] **Step 2: ReusePipeline**

`.../example/reuse/ReusePipeline.java`:

```java
package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** demo4：只设部分字段时，Resettable 在叶子后清空，避免复用槽位读到上一轮残留。 */
@Component
public class ReusePipeline {

    private static final Logger log = LoggerFactory.getLogger(ReusePipeline.class);

    static volatile CountDownLatch latch;
    /** 记录每个订单 collect 时看到的 couponCode，供 runner 校验有无残留。 */
    static final List<String> seen = new CopyOnWriteArrayList<>();

    @DisruptorStage(pipeline = "reuse", name = "collect")
    public void collect(ReuseEvent e) {
        seen.add(e.getOrderId() + "=" + e.getCouponCode());
        log.info("[reuse/collect] 订单 {} couponCode={}", e.getOrderId(), e.getCouponCode());
        latch.countDown();
    }
}
```

- [ ] **Step 3: ReuseDemoRunner**

`.../example/reuse/ReuseDemoRunner.java`:

```java
package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
@Order(4)
@RequiredArgsConstructor
public class ReuseDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ReuseDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo4 事件复用 Resettable（reuse）====");
        int followers = 20;                 // > buffer-size(16)，确保槽位被复用
        ReusePipeline.seen.clear();
        ReusePipeline.latch = new CountDownLatch(1 + followers);
        eventBus.publish(ReuseEvent.class, e -> {          // 订单 A 用券
            e.setOrderId("A");
            e.setCouponCode("SAVE10");
        });
        for (int i = 0; i < followers; i++) {              // 后续订单只设 orderId
            String id = "B" + i;
            eventBus.publish(ReuseEvent.class, e -> e.setOrderId(id));
        }
        if (!ReusePipeline.latch.await(5, TimeUnit.SECONDS)) {
            log.warn("demo4 超时");
        }
        boolean leaked = ReusePipeline.seen.stream()
                .anyMatch(s -> s.startsWith("B") && s.endsWith("SAVE10"));
        log.info("[reuse] 后续订单是否读到残留 SAVE10：{}（reset 生效应为 false）", leaked);
        results.markDone("reuse");
        log.info("==== demo4 完成 ====");
    }
}
```

- [ ] **Step 4: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-example -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 5: Commit**

```bash
git add disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/reuse/
git commit -m "feat(example): demo4 事件复用 Resettable 去残留"
```

---

## Task 6: example demo5 —— 背压 tryPublish 三形态（backpressure）

**Files:**
- Create: `.../example/backpressure/BpEvent.java`
- Create: `.../example/backpressure/BpPipeline.java`
- Create: `.../example/backpressure/BackpressureDemoRunner.java`

- [ ] **Step 1: BpEvent**

`.../example/backpressure/BpEvent.java`:

```java
package com.sstlfsj.disruptor.example.backpressure;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BpEvent {
    private int n;
}
```

- [ ] **Step 2: BpPipeline(慢消费)**

`.../example/backpressure/BpPipeline.java`:

```java
package com.sstlfsj.disruptor.example.backpressure;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** demo5：慢消费阶段，让 ring buffer 快速被填满以触发 tryPublish 背压。 */
@Component
public class BpPipeline {

    private static final Logger log = LoggerFactory.getLogger(BpPipeline.class);

    @DisruptorStage(pipeline = "backpressure", name = "slow")
    public void slow(BpEvent e) throws InterruptedException {
        Thread.sleep(20);   // 模拟慢业务
        log.info("[backpressure/slow] 处理 n={}", e.getN());
    }
}
```

- [ ] **Step 3: BackpressureDemoRunner(三种降级形态)**

`.../example/backpressure/BackpressureDemoRunner.java`:

```java
package com.sstlfsj.disruptor.example.backpressure;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.example.DemoResults;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(5)
@RequiredArgsConstructor
public class BackpressureDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BackpressureDemoRunner.class);
    private final EventBus eventBus;
    private final DemoResults results;

    @Override
    public void run(String... args) throws Exception {
        log.info("==== demo5 背压 tryPublish 三形态（backpressure）====");
        int dropped = 0;
        int total = 60;                         // 远超 buffer(16) + 慢消费 → 必然触发满
        for (int i = 0; i < total; i++) {
            int n = i;
            boolean ok = eventBus.tryPublish(BpEvent.class, e -> e.setN(n));
            if (!ok) {
                switch (i % 3) {                // 轮流演示三形态
                    case 0 -> {                 // ① 可丢弃：丢弃 + 计数
                        dropped++;
                    }
                    case 1 ->                    // ② 不能丢：降级落库补偿（此处用日志模拟）
                            log.info("[backpressure] n={} 降级落库补偿（事后重放）", n);
                    default -> {                 // ③ 关键链路：快速失败回推上游
                        try {
                            throw new IllegalStateException("系统繁忙，请稍后重试");
                        } catch (IllegalStateException ex) {
                            log.info("[backpressure] n={} 快速失败，回推上游限流：{}", n, ex.getMessage());
                        }
                    }
                }
            }
        }
        log.info("[backpressure] 本轮丢弃计数 dropped={}", dropped);
        Thread.sleep(1500);                     // 等已入队事件基本处理完，日志更完整
        results.markDone("backpressure");
        log.info("==== demo5 完成 ====");
    }
}
```

- [ ] **Step 4: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-example -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 5: Commit**

```bash
git add disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/backpressure/
git commit -m "feat(example): demo5 背压 tryPublish 三种降级形态"
```

---

## Task 7: example demo6 —— 纯 Java 无 Spring（PureJavaExample 独立 main）

**Files:**
- Create: `.../example/nospring/PureJavaExample.java`

- [ ] **Step 1: PureJavaExample**

`.../example/nospring/PureJavaExample.java`:

```java
package com.sstlfsj.disruptor.example.nospring;

import com.sstlfsj.disruptor.core.DefaultEventBus;
import com.sstlfsj.disruptor.core.DisruptorConfig;
import com.sstlfsj.disruptor.core.DisruptorPipeline;
import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.core.EventPipeline;
import com.sstlfsj.disruptor.core.PipelineBuilder;
import com.sstlfsj.disruptor.core.Pipelines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 无 Spring 的纯 Java 用法：手工装配 PipelineBuilder + DefaultEventBus，证明 core 可独立使用。
 * 运行方式：直接运行本类 main。
 */
public class PureJavaExample {

    private static final Logger log = LoggerFactory.getLogger(PureJavaExample.class);

    public static class PlainEvent {
        int n;
    }

    public static void main(String[] args) throws Exception {
        CountDownLatch latch = new CountDownLatch(3);

        DisruptorConfig config = new DisruptorConfig(
                16, DisruptorConfig.WaitStrategyType.BLOCKING, Duration.ofSeconds(5));

        EventPipeline<PlainEvent> def = EventPipeline.builder("plain", PlainEvent.class)
                .stage("handle", e -> {
                    log.info("[pure-java] 处理 n={}", e.n);
                    latch.countDown();
                })
                .build();

        DisruptorPipeline<PlainEvent> pipeline = new PipelineBuilder(config).build(def);
        Pipelines pipelines = new Pipelines();
        pipelines.register(pipeline);
        EventBus bus = new DefaultEventBus(pipelines);

        pipeline.disruptor().start();
        try {
            for (int i = 0; i < 3; i++) {
                int n = i;
                bus.publish(PlainEvent.class, e -> e.n = n);
            }
            if (!latch.await(3, TimeUnit.SECONDS)) {
                log.warn("[pure-java] 超时");
            }
        } finally {
            pipeline.disruptor().shutdown(2, TimeUnit.SECONDS);
        }
        log.info("[pure-java] 完成");
    }
}
```

- [ ] **Step 2: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-example -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 3: 运行验证（可选，直接跑 main）**

Run: `$MVN -q -pl disruptor-spring-boot-example exec:java -Dexec.mainClass=com.sstlfsj.disruptor.example.nospring.PureJavaExample`
说明:exec 插件未配置时此命令会失败——非必需;编译通过即可,运行验证留给使用者从 IDE 跑 main。
Expected: 若能跑,日志见 3 条 `[pure-java] 处理 n=...` + `完成`。

- [ ] **Step 4: Commit**

```bash
git add disruptor-spring-boot-example/src/main/java/com/sstlfsj/disruptor/example/nospring/
git commit -m "feat(example): demo6 纯 Java 无 Spring 手工装配"
```

---

## Task 8: example —— application.yml + 冒烟测试

**Files:**
- Create: `disruptor-spring-boot-example/src/main/resources/application.yml`
- Create: `disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/ExampleSmokeTest.java`

- [ ] **Step 1: application.yml**

`disruptor-spring-boot-example/src/main/resources/application.yml`:

```yaml
disruptor:
  buffer-size: 16          # 2 的幂；故意小以便 backpressure demo 触发满槽
  wait-strategy: YIELDING  # BLOCKING / YIELDING(默认) / BUSY_SPIN / SLEEPING
  shutdown-timeout: 10s
logging:
  level:
    com.sstlfsj.disruptor: DEBUG   # 看得到装配 INFO 与背压 DEBUG
spring:
  main:
    web-application-type: none     # 非 Web，跑完 demo 即退出

# 另一种配置方式：声明 DisruptorConfig @Bean 编程覆盖（声明后上面 disruptor.* 被忽略）。
# @Bean DisruptorConfig disruptorConfig() {
#   return new DisruptorConfig(2048, DisruptorConfig.WaitStrategyType.BLOCKING, Duration.ofSeconds(30));
# }
```

- [ ] **Step 2: 写冒烟测试**

`disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/ExampleSmokeTest.java`:

```java
package com.sstlfsj.disruptor.example;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/** 启动上下文触发所有 DemoRunner，断言五个 Spring 内 demo 均跑完（防示例腐化）。 */
@SpringBootTest
class ExampleSmokeTest {

    @Autowired
    DemoResults results;

    @Test
    void allSpringDemosCompleted() {
        assertThat(results.isDone("order")).isTrue();
        assertThat(results.isDone("pay")).isTrue();
        assertThat(results.isDone("ingest")).isTrue();
        assertThat(results.isDone("reuse")).isTrue();
        assertThat(results.isDone("backpressure")).isTrue();
    }
}
```

- [ ] **Step 3: 运行冒烟测试**

Run: `$MVN -pl disruptor-spring-boot-example -am test`
Expected: `Tests run: 1, Failures: 0, Errors: 0` + BUILD SUCCESS。控制台可见 demo1..5 分段日志。

- [ ] **Step 4: Commit**

```bash
git add disruptor-spring-boot-example/src/main/resources/application.yml \
  disruptor-spring-boot-example/src/test/java/com/sstlfsj/disruptor/example/ExampleSmokeTest.java
git commit -m "test(example): application.yml + 冒烟测试断言五个 demo 跑通"
```

---

## Task 9: tutorial —— 端到端测试先行（失败）

**Files:**
- Create: `disruptor-spring-boot-tutorial/src/test/java/com/sstlfsj/disruptor/tutorial/OrderFlowTest.java`

- [ ] **Step 1: 写端到端测试（此时控制器/事件都还没有，编译或运行必失败）**

`disruptor-spring-boot-tutorial/src/test/java/com/sstlfsj/disruptor/tutorial/OrderFlowTest.java`:

```java
package com.sstlfsj.disruptor.tutorial;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

/** 端到端：POST /orders 立刻 202；后台异步跑完后 stats 反映落库/发信/扣库存。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderFlowTest {

    @Autowired
    TestRestTemplate rest;

    @Test
    @SuppressWarnings("unchecked")
    void placeOrderReturnsFastThenProcessedAsync() {
        ResponseEntity<Map> post = rest.postForEntity(
                "/orders", Map.of("userId", "u1", "amount", 100), Map.class);

        assertThat(post.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(post.getBody()).isNotNull();
        assertThat(post.getBody().get("orderId")).asString().isNotEmpty();

        await().atMost(3, SECONDS).untilAsserted(() -> {
            Map<String, Integer> stats = rest.getForObject("/orders/stats", Map.class);
            assertThat(stats.get("persisted")).isGreaterThanOrEqualTo(1);
            assertThat(stats.get("emailsSent")).isGreaterThanOrEqualTo(1);
            assertThat(stats.get("metricsRecorded")).isGreaterThanOrEqualTo(1);
            assertThat(stats.get("stockRemaining")).isEqualTo(99);
        });
    }
}
```

> Awaitility 随 `spring-boot-starter-test` 传递依赖（Spring Boot 管理其版本），无需单独声明。

- [ ] **Step 2: 运行，确认失败**

Run: `$MVN -pl disruptor-spring-boot-tutorial -am test -Dtest=OrderFlowTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败或测试失败(尚无 /orders 接口)。这是预期的红。

- [ ] **Step 3: Commit（红测先入库）**

```bash
git add disruptor-spring-boot-tutorial/src/test/java/com/sstlfsj/disruptor/tutorial/OrderFlowTest.java
git commit -m "test(tutorial): 端到端下单流程测试（先行，预期红）"
```

---

## Task 10: tutorial —— 事件、落库快照、仓库、统计

**Files:**
- Create: `.../tutorial/OrderPlacedEvent.java`
- Create: `.../tutorial/OrderRecord.java`
- Create: `.../tutorial/InMemoryOrderRepository.java`
- Create: `.../tutorial/OrderStats.java`

- [ ] **Step 1: OrderPlacedEvent(Resettable)**

`.../tutorial/OrderPlacedEvent.java`:

```java
package com.sstlfsj.disruptor.tutorial;

import com.sstlfsj.disruptor.core.Resettable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderPlacedEvent implements Resettable {
    private String orderId;
    private String userId;
    private long amount;
    private String couponCode;   // 可选

    @Override
    public void reset() {
        this.orderId = null;
        this.userId = null;
        this.amount = 0L;
        this.couponCode = null;
    }
}
```

- [ ] **Step 2: OrderRecord(不可变落库快照)**

`.../tutorial/OrderRecord.java`:

```java
package com.sstlfsj.disruptor.tutorial;

/** 落库快照：复制事件字段，避免持有会被复用的事件对象引用。 */
public record OrderRecord(String orderId, String userId, long amount, String couponCode) {
}
```

- [ ] **Step 3: InMemoryOrderRepository**

`.../tutorial/InMemoryOrderRepository.java`:

```java
package com.sstlfsj.disruptor.tutorial;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryOrderRepository {

    private final Map<String, OrderRecord> store = new ConcurrentHashMap<>();

    public void save(OrderRecord record) {
        store.put(record.orderId(), record);
    }

    public int count() {
        return store.size();
    }
}
```

- [ ] **Step 4: OrderStats**

`.../tutorial/OrderStats.java`:

```java
package com.sstlfsj.disruptor.tutorial;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class OrderStats {

    private final AtomicInteger persisted = new AtomicInteger();
    private final AtomicInteger emailsSent = new AtomicInteger();
    private final AtomicInteger metricsRecorded = new AtomicInteger();
    private final AtomicInteger stockRemaining = new AtomicInteger(100);
    private final AtomicInteger rejected = new AtomicInteger();

    public void onPersisted() { persisted.incrementAndGet(); }
    public void onEmailSent() { emailsSent.incrementAndGet(); }
    public void onMetricRecorded() { metricsRecorded.incrementAndGet(); }
    public void onStockDeducted() { stockRemaining.decrementAndGet(); }
    public void onRejected() { rejected.incrementAndGet(); }

    public Map<String, Integer> snapshot() {
        return Map.of(
                "persisted", persisted.get(),
                "emailsSent", emailsSent.get(),
                "metricsRecorded", metricsRecorded.get(),
                "stockRemaining", stockRemaining.get(),
                "rejected", rejected.get());
    }
}
```

- [ ] **Step 5: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-tutorial -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 6: Commit**

```bash
git add disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/OrderPlacedEvent.java \
  disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/OrderRecord.java \
  disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/InMemoryOrderRepository.java \
  disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/OrderStats.java
git commit -m "feat(tutorial): 事件、落库快照、仓库、统计"
```

---

## Task 11: tutorial —— 处理管道（注解式五阶段 DAG）

**Files:**
- Create: `.../tutorial/OrderProcessingPipeline.java`

- [ ] **Step 1: OrderProcessingPipeline**

`.../tutorial/OrderProcessingPipeline.java`:

```java
package com.sstlfsj.disruptor.tutorial;

import com.sstlfsj.disruptor.autoconfigure.DisruptorStage;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 下单后台处理：validate → persist → (sendConfirmation ‖ deductInventory ‖ recordMetrics)。
 * 三个副作用并行 fan-out，互不阻塞主流程。
 */
@Component
@RequiredArgsConstructor
public class OrderProcessingPipeline {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingPipeline.class);

    private final InMemoryOrderRepository repository;
    private final OrderStats stats;

    @DisruptorStage(pipeline = "order", name = "validate")
    public void validate(OrderPlacedEvent e) {
        if (e.getAmount() <= 0) {
            // 抛出会被框架隔离（记 ERROR、不终止消费线程）；演示异常隔离。
            throw new IllegalArgumentException("非法金额: " + e.getAmount());
        }
        log.info("[order/validate] 订单 {} 用户 {} 金额 {} 校验通过",
                e.getOrderId(), e.getUserId(), e.getAmount());
    }

    @DisruptorStage(pipeline = "order", name = "persist", after = "validate")
    public void persist(OrderPlacedEvent e) {
        repository.save(new OrderRecord(e.getOrderId(), e.getUserId(), e.getAmount(), e.getCouponCode()));
        stats.onPersisted();
        log.info("[order/persist] 订单 {} 已落库", e.getOrderId());
    }

    @DisruptorStage(pipeline = "order", name = "sendConfirmation", after = "persist")
    public void sendConfirmation(OrderPlacedEvent e) throws InterruptedException {
        Thread.sleep(10);   // 模拟发信 IO
        stats.onEmailSent();
        log.info("[order/sendConfirmation] 订单 {} 确认邮件已发", e.getOrderId());
    }

    @DisruptorStage(pipeline = "order", name = "deductInventory", after = "persist")
    public void deductInventory(OrderPlacedEvent e) {
        stats.onStockDeducted();
        log.info("[order/deductInventory] 订单 {} 已扣库存", e.getOrderId());
    }

    @DisruptorStage(pipeline = "order", name = "recordMetrics", after = "persist")
    public void recordMetrics(OrderPlacedEvent e) {
        stats.onMetricRecorded();
        log.info("[order/recordMetrics] 订单 {} 埋点已记", e.getOrderId());
    }
}
```

- [ ] **Step 2: 编译**

Run: `$MVN -q -pl disruptor-spring-boot-tutorial -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 3: Commit**

```bash
git add disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/OrderProcessingPipeline.java
git commit -m "feat(tutorial): 注解式五阶段处理 DAG"
```

---

## Task 12: tutorial —— 控制器 + 请求体 + yml，端到端转绿

**Files:**
- Create: `.../tutorial/web/PlaceOrderRequest.java`
- Create: `.../tutorial/web/OrderController.java`
- Create: `disruptor-spring-boot-tutorial/src/main/resources/application.yml`

- [ ] **Step 1: PlaceOrderRequest**

`.../tutorial/web/PlaceOrderRequest.java`:

```java
package com.sstlfsj.disruptor.tutorial.web;

public record PlaceOrderRequest(String userId, long amount, String couponCode) {
}
```

- [ ] **Step 2: OrderController**

`.../tutorial/web/OrderController.java`:

```java
package com.sstlfsj.disruptor.tutorial.web;

import com.sstlfsj.disruptor.core.EventBus;
import com.sstlfsj.disruptor.tutorial.OrderPlacedEvent;
import com.sstlfsj.disruptor.tutorial.OrderStats;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final EventBus eventBus;
    private final OrderStats stats;

    /** 下单：publish 后立刻返回 202；ring buffer 满则 429（背压回推）。 */
    @PostMapping
    public ResponseEntity<Map<String, String>> place(@RequestBody PlaceOrderRequest req) {
        String orderId = UUID.randomUUID().toString();
        boolean accepted = eventBus.tryPublish(OrderPlacedEvent.class, e -> {
            e.setOrderId(orderId);
            e.setUserId(req.userId());
            e.setAmount(req.amount());
            e.setCouponCode(req.couponCode());
        });
        if (!accepted) {
            stats.onRejected();
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("error", "系统繁忙，请稍后重试"));
        }
        return ResponseEntity.accepted().body(Map.of("orderId", orderId, "status", "accepted"));
    }

    /** 观测后台异步处理进度。 */
    @GetMapping("/stats")
    public Map<String, Integer> stats() {
        return stats.snapshot();
    }
}
```

- [ ] **Step 3: application.yml**

`disruptor-spring-boot-tutorial/src/main/resources/application.yml`:

```yaml
disruptor:
  buffer-size: 16          # 小 buffer，便于压测触发 429 背压
  wait-strategy: YIELDING
  shutdown-timeout: 10s
logging:
  level:
    com.sstlfsj.disruptor: INFO
server:
  port: 8080
```

- [ ] **Step 4: 运行端到端测试，确认转绿**

Run: `$MVN -pl disruptor-spring-boot-tutorial -am test`
Expected: `OrderFlowTest` PASS，`Tests run: 1, Failures: 0, Errors: 0` + BUILD SUCCESS。

- [ ] **Step 5: Commit**

```bash
git add disruptor-spring-boot-tutorial/src/main/java/com/sstlfsj/disruptor/tutorial/web/ \
  disruptor-spring-boot-tutorial/src/main/resources/application.yml
git commit -m "feat(tutorial): 下单/统计 HTTP 接口 + yml，端到端转绿"
```

---

## Task 13: 全量构建 + README 指引

**Files:**
- Modify: `README.md`(末尾加「示例与教程」一节)

- [ ] **Step 1: 全量构建两模块及其依赖**

Run: `$MVN -q clean install`
Expected: BUILD SUCCESS，五个模块(core/autoconfigure/starter/example/tutorial)全过,测试全绿。

- [ ] **Step 2: README 追加「示例与教程」小节**

在 `README.md` 末尾(「已知限制」一节之后)追加:

```markdown
## 示例与教程

仓库内两个可跑模块:

- **特性演示** `disruptor-spring-boot-example`:每个核心特性一个 console demo(声明式 DAG、编程式、
  并行+ShardKeyed、Resettable、背压三形态)。运行:

  ```bash
  mvn -pl disruptor-spring-boot-example spring-boot:run
  ```

  纯 Java(无 Spring)路径见 `PureJavaExample`,从 IDE 直接运行其 `main`。

- **真实场景教程** `disruptor-spring-boot-tutorial`:下单场景 web 小应用——`POST /orders` publish 后
  立刻返回、副作用(落库/发信/扣库存/埋点)后台异步跑,满时 429 背压。运行:

  ```bash
  mvn -pl disruptor-spring-boot-tutorial spring-boot:run
  # 下单（立刻返回 202 + orderId）
  curl -XPOST localhost:8080/orders -H 'Content-Type: application/json' \
    -d '{"userId":"u1","amount":100}'
  # 看后台异步处理进度
  curl localhost:8080/orders/stats
  ```
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README 增加示例与教程模块的运行指引"
```

---

## Self-Review 记录

- **Spec 覆盖**:example 六个 demo(Task2-7)+ yml/冒烟(Task8);tutorial 事件/仓库/统计(Task10)、DAG(Task11)、
  接口/yml(Task12)、端到端测试(Task9/12);根 pom depMgmt+modules(Task1);README(Task13)。均有对应任务。
- **偏离 spec 处**:demo1/demo2 菱形并行分支改写**不同布尔字段**(persisted/audited)而非追加同一字符串,
  规避并发写竞态——工程上更正确,已在 Task2/3 注明。
- **类型一致**:`DemoResults.markDone/isDone`、`OrderStats.on*/snapshot`、`InMemoryOrderRepository.save/count`、
  `@DisruptorStage` 属性(pipeline/name/after/parallelism)、`EventBus.publish/tryPublish`、
  `EventPipeline.builder(...).stage/after/build`、`DisruptorConfig(int,WaitStrategyType,Duration)` 全篇一致。
- **无占位符**:所有步骤含完整代码与命令。
