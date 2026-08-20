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
