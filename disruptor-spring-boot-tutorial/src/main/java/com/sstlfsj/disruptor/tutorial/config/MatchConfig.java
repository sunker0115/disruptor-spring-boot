package com.sstlfsj.disruptor.tutorial.config;

import com.sstlfsj.disruptor.tutorial.match.MatchEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 把撮合核心暴露为 bean（核心本身保持零 Spring 注解，可脱离容器独立使用）。
 */
@Configuration
public class MatchConfig {

    @Bean
    public MatchEngine matchEngine() {
        return new MatchEngine();
    }
}
