package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentAutoConfiguration;
import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentHealthAutoConfiguration;
import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentHealthContributor;
import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentLifecycle;
import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentMetrics;
import com.sstlfsj.disruptor.autoconfigure.DisruptorConcurrentMetricsAutoConfiguration;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoop;
import com.sstlfsj.disruptor.concurrent.DisruptorEventLoopGroup;
import com.sstlfsj.disruptor.concurrent.EventLoop;
import com.sstlfsj.disruptor.concurrent.EventLoopBuilder;
import com.sstlfsj.disruptor.concurrent.EventLoopGroupBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class DisruptorConcurrentAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    DisruptorConcurrentAutoConfiguration.class,
                    DisruptorConcurrentHealthAutoConfiguration.class,
                    DisruptorConcurrentMetricsAutoConfiguration.class));

    @Test
    void managesExplicitRootsWithoutManagingAnExposedGroupChildTwice() {
        contextRunner.withUserConfiguration(RootConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorConcurrentLifecycle.class);
                    DisruptorConcurrentLifecycle lifecycle = context.getBean(
                            DisruptorConcurrentLifecycle.class);
                    assertThat(lifecycle.isRunning()).isTrue();
                    assertThat(context.getBeansOfType(EventLoop.class)).hasSize(3);
                    assertThat(context.getBean("standaloneOne", EventLoop.class)
                            .snapshot().acceptingTasks()).isTrue();
                    assertThat(context.getBean("standaloneTwo", EventLoop.class)
                            .snapshot().acceptingTasks()).isTrue();
                    assertThat(context.getBean(DisruptorEventLoopGroup.class)
                            .snapshot().acceptingTasks()).isTrue();
                    assertThat(context.getBean("exposedChild", EventLoop.class).parent())
                            .isSameAs(context.getBean(DisruptorEventLoopGroup.class));
                });
    }

    @Test
    void healthAndMetricsAreIndependentOptionalLayers() {
        contextRunner.withUserConfiguration(
                        RootConfiguration.class, MeterRegistryConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorConcurrentLifecycle.class);
                    assertThat(context).hasSingleBean(DisruptorConcurrentHealthContributor.class);
                    assertThat(context).hasSingleBean(DisruptorConcurrentMetrics.class);
                });

        contextRunner.withClassLoader(new FilteredClassLoader(HealthIndicator.class))
                .withUserConfiguration(RootConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorConcurrentLifecycle.class);
                    assertThat(context).doesNotHaveBean("disruptorConcurrentHealthContributor");
                });

        contextRunner.withClassLoader(new FilteredClassLoader(MeterRegistry.class))
                .withUserConfiguration(RootConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorConcurrentLifecycle.class);
                    assertThat(context).hasSingleBean(DisruptorConcurrentHealthContributor.class);
                    assertThat(context).doesNotHaveBean("disruptorConcurrentMetrics");
                });
    }

    @Test
    void missingConcurrentClassesAndDisabledPropertyLeaveExistingAutoConfigurationUsable() {
        contextRunner.withClassLoader(new FilteredClassLoader(
                        "com.sstlfsj.disruptor.concurrent"))
                .run(context -> {
                    assertThat(context).doesNotHaveBean("disruptorConcurrentLifecycle");
                    assertThat(context).doesNotHaveBean("disruptorConcurrentHealthContributor");
                    assertThat(context).doesNotHaveBean("disruptorConcurrentMetrics");
                });

        contextRunner.withUserConfiguration(RootConfiguration.class)
                .withPropertyValues("disruptor.concurrent.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(DisruptorConcurrentLifecycle.class);
                    assertThat(context).doesNotHaveBean(DisruptorConcurrentHealthContributor.class);
                    assertThat(context).doesNotHaveBean(DisruptorConcurrentMetrics.class);
                });
    }

    @Test
    void generatesConcurrentPropertiesAndAutoConfigurationMetadata() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        try (var configurationMetadata = classLoader.getResourceAsStream(
                "META-INF/spring-configuration-metadata.json");
             var autoConfigurationMetadata = classLoader.getResourceAsStream(
                     "META-INF/spring-autoconfigure-metadata.properties");
             var imports = classLoader.getResourceAsStream(
                     "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")) {
            assertThat(configurationMetadata).isNotNull();
            assertThat(autoConfigurationMetadata).isNotNull();
            assertThat(imports).isNotNull();
            assertThat(new String(configurationMetadata.readAllBytes(), StandardCharsets.UTF_8))
                    .contains("disruptor.concurrent.enabled")
                    .contains("disruptor.concurrent.lifecycle-phase")
                    .contains("disruptor.concurrent.shutdown-timeout");
            assertThat(new String(autoConfigurationMetadata.readAllBytes(), StandardCharsets.UTF_8))
                    .contains(DisruptorConcurrentAutoConfiguration.class.getName())
                    .contains(DisruptorConcurrentHealthAutoConfiguration.class.getName())
                    .contains(DisruptorConcurrentMetricsAutoConfiguration.class.getName());
            assertThat(new String(imports.readAllBytes(), StandardCharsets.UTF_8))
                    .contains(DisruptorConcurrentAutoConfiguration.class.getName())
                    .contains(DisruptorConcurrentHealthAutoConfiguration.class.getName())
                    .contains(DisruptorConcurrentMetricsAutoConfiguration.class.getName());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RootConfiguration {

        @Bean(destroyMethod = "")
        DisruptorEventLoop standaloneOne() {
            return EventLoopBuilder.bounded("spring-one", 8).build();
        }

        @Bean(destroyMethod = "")
        EventLoop standaloneTwo() {
            return EventLoopBuilder.unbounded("spring-two", 8).build();
        }

        @Bean(destroyMethod = "")
        DisruptorEventLoopGroup eventLoopGroup() {
            return EventLoopGroupBuilder.bounded("spring-workers", 2, 8).build();
        }

        @Bean(destroyMethod = "")
        EventLoop exposedChild(DisruptorEventLoopGroup eventLoopGroup) {
            return eventLoopGroup.select(0);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MeterRegistryConfiguration {

        @Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
