package com.sstlfsj.disruptor.example.nospring;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThatCode;

class PureJavaConcurrentExampleTest {

    @Test
    void mainCompletesWithoutLeavingTheConcurrentLoopRunning() throws Exception {
        Class<?> example = Class.forName(
                "com.sstlfsj.disruptor.example.nospring.PureJavaConcurrentExample");
        Method main = example.getMethod("main", String[].class);

        assertThatCode(() -> main.invoke(null, (Object) new String[0]))
                .doesNotThrowAnyException();
    }
}
