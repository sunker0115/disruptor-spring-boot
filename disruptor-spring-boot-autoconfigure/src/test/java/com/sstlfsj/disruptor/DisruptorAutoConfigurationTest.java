package com.sstlfsj.disruptor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.ProducerType;
import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import com.sstlfsj.disruptor.autoconfigure.DisruptorLifecycle;
import com.sstlfsj.disruptor.autoconfigure.DisruptorMetrics;
import com.sstlfsj.disruptor.autoconfigure.DisruptorMetricsAutoConfiguration;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineHandle;
import com.sstlfsj.disruptor.core.PipelineSpec;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.aot.generate.ClassNameGenerator;
import org.springframework.aot.generate.DefaultGenerationContext;
import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.context.aot.ApplicationContextAotGenerator;
import org.springframework.context.annotation.AnnotatedBeanDefinitionReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.javapoet.ClassName;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class DisruptorAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DisruptorAutoConfiguration.class));

    @Test
    void lifecycleCallbackRunsAfterConsumerThreadStops() {
        AtomicBoolean consumerStopped = new AtomicBoolean();
        AtomicBoolean callbackSawStoppedConsumer = new AtomicBoolean();
        PipelineSpec<TestEvent> spec = PipelineSpec.builder("lifecycle", TestEvent.class, TestEvent::new)
                .topology(disruptor -> disruptor.handleEventsWith(new EventHandler<>() {
                    @Override
                    public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
                    }

                    @Override
                    public void onShutdown() {
                        consumerStopped.set(true);
                    }
                }))
                .build();
        DisruptorLifecycle lifecycle = new DisruptorLifecycle(
                DisruptorRuntime.builder().add(spec).build(), Integer.MIN_VALUE);

        lifecycle.start();
        lifecycle.stop(() -> callbackSawStoppedConsumer.set(consumerStopped.get()));

        assertThat(callbackSawStoppedConsumer).isTrue();
    }

    @Test
    void buildsNamedRuntimeFromPipelineSpecBeansAndAppliesPropertyLayers() {
        contextRunner.withUserConfiguration(TwoPipelines.class)
                .withPropertyValues(
                        "disruptor.defaults.buffer-size=64",
                        "disruptor.defaults.producer-type=single",
                        "disruptor.pipelines.orders.buffer-size=128",
                        "disruptor.lifecycle-phase=-100")
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorRuntime.class);
                    assertThat(context).hasSingleBean(DisruptorLifecycle.class);
                    DisruptorRuntime runtime = context.getBean(DisruptorRuntime.class);
                    assertThat(runtime.require("orders", TestEvent.class).unsafeRingBuffer().getBufferSize())
                            .isEqualTo(128);
                    assertThat(runtime.require("audit", TestEvent.class).unsafeRingBuffer().getBufferSize())
                            .isEqualTo(32);
                    assertThat(context.getBean(DisruptorLifecycle.class).getPhase()).isEqualTo(-100);
                });
    }

    @Test
    void startsPipelineAndKeepsNativePublishingApi() {
        CountDownLatch consumed = new CountDownLatch(1);
        contextRunner.withBean("nativePipeline", PipelineSpec.class, () -> PipelineSpec
                        .builder("native", TestEvent.class, TestEvent::new)
                        .topology(disruptor -> disruptor.handleEventsWith(
                                (event, sequence, endOfBatch) -> consumed.countDown()))
                        .build())
                .run(context -> {
                    PipelineHandle<TestEvent> handle = context.getBean(DisruptorRuntime.class)
                            .require("native", TestEvent.class);
                    EventTranslatorOneArg<TestEvent, String> translator =
                            (event, sequence, value) -> event.value = value;
                    assertThat(handle.tryPublishEvent(translator, "value")).isTrue();
                    assertThat(consumed.await(2, TimeUnit.SECONDS)).isTrue();
                });
    }

    @Test
    void defaultErrorStrategyHaltsAfterHandlerException() {
        CountDownLatch succeeded = new CountDownLatch(2);
        contextRunner.withBean("faulty", PipelineSpec.class, () -> PipelineSpec
                        .builder("faulty", TestEvent.class, TestEvent::new)
                        .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                            if ("boom".equals(event.value)) {
                                throw new IllegalStateException("boom");
                            }
                            succeeded.countDown();
                        }))
                        .build())
                .withPropertyValues("disruptor.shutdown-timeout=200ms")
                .run(context -> {
                    PipelineHandle<TestEvent> handle = context.getBean(DisruptorRuntime.class)
                            .require("faulty", TestEvent.class);
                    EventTranslatorOneArg<TestEvent, String> translator =
                            (event, sequence, value) -> event.value = value;
                    handle.publishEvent(translator, "ok-1");
                    handle.publishEvent(translator, "boom");
                    handle.publishEvent(translator, "ok-2");
                    assertThat(succeeded.await(300, TimeUnit.MILLISECONDS)).isFalse();
                    assertThat(succeeded.getCount()).isEqualTo(1);
                });
    }

    @Test
    void explicitLogAndContinueKeepsConsumingAfterHandlerException() {
        CountDownLatch succeeded = new CountDownLatch(2);
        contextRunner.withBean("faulty", PipelineSpec.class, () -> PipelineSpec
                        .builder("faulty", TestEvent.class, TestEvent::new)
                        .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                            if ("boom".equals(event.value)) {
                                throw new IllegalStateException("boom");
                            }
                            succeeded.countDown();
                        }))
                        .build())
                .withPropertyValues("disruptor.defaults.error-strategy=log-and-continue")
                .run(context -> {
                    PipelineHandle<TestEvent> handle = context.getBean(DisruptorRuntime.class)
                            .require("faulty", TestEvent.class);
                    EventTranslatorOneArg<TestEvent, String> translator =
                            (event, sequence, value) -> event.value = value;
                    handle.publishEvent(translator, "ok-1");
                    handle.publishEvent(translator, "boom");
                    handle.publishEvent(translator, "ok-2");
                    assertThat(succeeded.await(2, TimeUnit.SECONDS)).isTrue();
                });
    }

    @Test
    void bindsErrorStrategyFromPropertiesWithRelaxedEnum() {
        contextRunner.withUserConfiguration(SinglePipeline.class)
                .withPropertyValues(
                        "disruptor.defaults.error-strategy=log-and-continue",
                        "disruptor.pipelines.orders.error-strategy=halt")
                .run(context -> assertThat(context).hasSingleBean(DisruptorRuntime.class));
    }

    @Test
    void failsOnUnknownNamedConfiguration() {
        contextRunner.withUserConfiguration(SinglePipeline.class)
                .withPropertyValues("disruptor.pipelines.misspelled.buffer-size=64")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining("misspelled"));
    }

    @Test
    void canDisableTheWholeAutoConfiguration() {
        contextRunner.withUserConfiguration(SinglePipeline.class)
                .withPropertyValues("disruptor.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(DisruptorRuntime.class);
                    assertThat(context).doesNotHaveBean(DisruptorLifecycle.class);
                });
    }

    @Test
    void createsAnEmptyRuntimeWhenNoPipelineSpecsExist() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(DisruptorRuntime.class);
            assertThat(context).hasSingleBean(DisruptorLifecycle.class);
            assertThat(context.getBean(DisruptorRuntime.class).handles()).isEmpty();
        });
    }

    @Test
    void backsOffForCustomRuntime() {
        contextRunner.withUserConfiguration(CustomRuntimeConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorRuntime.class);
                    assertThat(context).hasSingleBean(DisruptorLifecycle.class);
                    assertThat(context.getBean(DisruptorRuntime.class))
                            .isSameAs(CustomRuntimeConfiguration.RUNTIME);
                });
    }

    @Test
    void backsOffForCustomLifecycle() {
        contextRunner.withUserConfiguration(CustomLifecycleConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorRuntime.class);
                    assertThat(context).hasSingleBean(DisruptorLifecycle.class);
                    assertThat(context.getBean(DisruptorLifecycle.class))
                            .isSameAs(CustomLifecycleConfiguration.LIFECYCLE);
                });
    }

    @Test
    void invalidPipelineConfigurationReportsItsName() {
        contextRunner.withUserConfiguration(SinglePipeline.class)
                .withPropertyValues("disruptor.pipelines.orders.buffer-size=3")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().hasMessageContaining(
                                "管道 'orders' 配置无效：bufferSize 必须是正的 2 的幂，实际值=3"));
    }

    @Test
    void generatesConfigurationAndAutoConfigurationMetadata() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        try (var configurationMetadata = classLoader.getResourceAsStream(
                "META-INF/spring-configuration-metadata.json");
             var autoConfigurationMetadata = classLoader.getResourceAsStream(
                     "META-INF/spring-autoconfigure-metadata.properties")) {
            assertThat(configurationMetadata).isNotNull();
            assertThat(autoConfigurationMetadata).isNotNull();
            assertThat(new String(configurationMetadata.readAllBytes(), StandardCharsets.UTF_8))
                    .contains("disruptor.defaults.buffer-size")
                    .contains("disruptor.pipelines");
            assertThat(new String(autoConfigurationMetadata.readAllBytes(), StandardCharsets.UTF_8))
                    .contains(DisruptorAutoConfiguration.class.getName());
        }
    }

    @Test
    void springAotGenerationDoesNotNeedBusinessReflectionHints() {
        GenericApplicationContext context = new GenericApplicationContext();
        new AnnotatedBeanDefinitionReader(context).register(
                DisruptorAutoConfiguration.class, SinglePipeline.class);
        InMemoryGeneratedFiles generatedFiles = new InMemoryGeneratedFiles();
        DefaultGenerationContext generationContext = new DefaultGenerationContext(
                new ClassNameGenerator(ClassName.get("com.sstlfsj.disruptor", "AotApplication")),
                generatedFiles);

        new ApplicationContextAotGenerator().processAheadOfTime(context, generationContext);
        generationContext.writeGeneratedContent();

        assertThat(generatedFiles.getGeneratedFiles(GeneratedFiles.Kind.SOURCE)).isNotEmpty();
        context.close();
    }

    @Test
    void exposesOptionalPollingMetricsWithoutWrappingTheRingBuffer() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DisruptorAutoConfiguration.class, DisruptorMetricsAutoConfiguration.class))
                .withUserConfiguration(MetricsConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorMetrics.class);
                    SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
                    context.getBean(DisruptorMetrics.class).bindTo(registry);

                    assertThat(registry.get("disruptor.runtime.running").gauge().value()).isEqualTo(1.0);
                    assertThat(registry.get("disruptor.pipeline.buffer.size")
                            .tag("pipeline", "orders").gauge().value()).isEqualTo(1024.0);
                    assertThat(registry.get("disruptor.pipeline.remaining.capacity")
                            .tag("pipeline", "orders").gauge().value()).isEqualTo(1024.0);
                    assertThat(registry.get("disruptor.pipeline.backlog")
                            .tag("pipeline", "orders").gauge().value()).isZero();
                });
    }

    @Test
    void canDisableMetricsOnly() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DisruptorAutoConfiguration.class, DisruptorMetricsAutoConfiguration.class))
                .withUserConfiguration(MetricsConfiguration.class)
                .withPropertyValues("disruptor.metrics.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorRuntime.class);
                    assertThat(context).doesNotHaveBean(DisruptorMetrics.class);
                });
    }

    @Test
    void startsWithoutMicrometerOnTheClasspath() {
        new ApplicationContextRunner()
                .withClassLoader(new FilteredClassLoader(MeterRegistry.class))
                .withConfiguration(AutoConfigurations.of(
                        DisruptorAutoConfiguration.class, DisruptorMetricsAutoConfiguration.class))
                .withUserConfiguration(SinglePipeline.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(DisruptorRuntime.class);
                    assertThat(context).doesNotHaveBean("disruptorMetrics");
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoPipelines {

        @Bean
        PipelineSpec<TestEvent> orders() {
            return pipeline("orders", null);
        }

        @Bean
        PipelineSpec<TestEvent> audit() {
            return pipeline("audit", 32);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SinglePipeline {

        @Bean
        PipelineSpec<TestEvent> orders() {
            return pipeline("orders", null);
        }
    }

    @Test
    void autoBindsMetricsThroughRealMicrometerStack() {
        // registry 由 SimpleMetricsExportAutoConfiguration 提供（非用户 bean），
        // 只有 DisruptorMetricsAutoConfiguration 的 afterName 生效、排在 registry 装配之后，
        // @ConditionalOnBean(MeterRegistry) 才成立、DisruptorMetrics 才会被创建并自动绑定。
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        MetricsAutoConfiguration.class,
                        SimpleMetricsExportAutoConfiguration.class,
                        DisruptorAutoConfiguration.class,
                        DisruptorMetricsAutoConfiguration.class))
                .withUserConfiguration(SinglePipeline.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(MeterRegistry.class);
                    assertThat(context).hasSingleBean(DisruptorMetrics.class);
                    MeterRegistry registry = context.getBean(MeterRegistry.class);
                    // 未手动 bindTo：MeterRegistryPostProcessor 应已自动绑定 MeterBinder。
                    assertThat(registry.get("disruptor.runtime.running").gauge().value()).isEqualTo(1.0);
                    assertThat(registry.get("disruptor.pipeline.buffer.size")
                            .tag("pipeline", "orders").gauge().value()).isEqualTo(1024.0);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class MetricsConfiguration {

        @Bean
        PipelineSpec<TestEvent> orders() {
            return pipeline("orders", null);
        }

        @Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomRuntimeConfiguration {

        private static final DisruptorRuntime RUNTIME = DisruptorRuntime.builder().build();

        @Bean
        DisruptorRuntime customRuntime() {
            return RUNTIME;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomLifecycleConfiguration {

        private static final DisruptorRuntime RUNTIME = DisruptorRuntime.builder().build();
        private static final DisruptorLifecycle LIFECYCLE = new DisruptorLifecycle(RUNTIME, 123);

        @Bean
        DisruptorRuntime customRuntime() {
            return RUNTIME;
        }

        @Bean
        DisruptorLifecycle customLifecycle() {
            return LIFECYCLE;
        }
    }

    private static PipelineSpec<TestEvent> pipeline(String name, Integer bufferSize) {
        PipelineSpec.Builder<TestEvent> builder = PipelineSpec.builder(name, TestEvent.class, TestEvent::new)
                .producerType(ProducerType.MULTI)
                .topology(disruptor -> disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                }));
        if (bufferSize != null) {
            builder.bufferSize(bufferSize);
        }
        return builder.build();
    }

    static final class TestEvent {
        private String value;
    }
}
