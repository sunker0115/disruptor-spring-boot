package com.sstlfsj.disruptor.autoconfigure;

import com.lmax.disruptor.dsl.Disruptor;
import com.sstlfsj.disruptor.core.DisruptorRuntime;
import com.sstlfsj.disruptor.core.PipelineSpec;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 使用 {@link PipelineSpec} Bean 构建并托管命名 Disruptor 管道。 */
@AutoConfiguration
@ConditionalOnClass(Disruptor.class)
@ConditionalOnProperty(prefix = "disruptor", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(DisruptorProperties.class)
public class DisruptorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DisruptorRuntime disruptorRuntime(List<PipelineSpec<?>> specs,
                                             DisruptorProperties properties) {
        validateNamedConfiguration(specs, properties);
        return DisruptorRuntime.builder()
                .addAll(specs)
                .settingsResolver(properties::settingsFor)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public DisruptorLifecycle disruptorLifecycle(DisruptorRuntime runtime,
                                                 DisruptorProperties properties) {
        return new DisruptorLifecycle(runtime, properties.getLifecyclePhase());
    }

    private static void validateNamedConfiguration(List<PipelineSpec<?>> specs,
                                                   DisruptorProperties properties) {
        Set<String> names = new HashSet<>();
        for (PipelineSpec<?> spec : specs) {
            names.add(spec.name());
        }
        Set<String> unknown = new HashSet<>(properties.getPipelines().keySet());
        unknown.removeAll(names);
        if (!unknown.isEmpty()) {
            throw new IllegalStateException("disruptor.pipelines 包含未定义的管道：" + unknown);
        }
    }
}
