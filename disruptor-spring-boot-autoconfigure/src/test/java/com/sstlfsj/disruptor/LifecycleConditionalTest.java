package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.autoconfigure.DisruptorLifecycle;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 回归：真实 Boot 应用里通常已存在其它 SmartLifecycle bean（如 applicationTaskExecutor、
 * springBootLoggingLifecycle）。disruptorLifecycle 的 @ConditionalOnMissingBean 必须按具体类型
 * {@link DisruptorLifecycle} 匹配——否则会被无关的 SmartLifecycle 抑制、消费线程永不启动、管道停摆。
 */
class LifecycleConditionalTest {

    @Test
    void lifecycleStillCreatedWhenAnotherSmartLifecyclePresent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class))
                .withUserConfiguration(CompetingLifecycleConfig.class)
                .run(ctx -> assertThat(ctx).hasSingleBean(DisruptorLifecycle.class));
    }

    @Configuration
    static class CompetingLifecycleConfig {
        @Bean
        SmartLifecycle competingLifecycle() {
            return new SmartLifecycle() {
                private volatile boolean running;

                @Override
                public void start() {
                    running = true;
                }

                @Override
                public void stop() {
                    running = false;
                }

                @Override
                public boolean isRunning() {
                    return running;
                }
            };
        }
    }
}
