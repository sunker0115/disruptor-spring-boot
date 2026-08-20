package com.sstlfsj.disruptor;

import com.sstlfsj.disruptor.autoconfigure.DisruptorAutoConfiguration;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 锁死 F1 回归：{@link DisruptorAutoConfiguration} 必须登记在 Spring Boot 的
 * AutoConfiguration.imports 清单中，否则真实使用方引入 starter 时自动装配不会被发现。
 * 现有黑盒测试用 {@code AutoConfigurations.of(...)} 显式加载，会绕过该发现机制，
 * 无法暴露清单缺失——因此单独校验 classpath 上的清单文件内容。
 */
class AutoConfigurationImportsTest {

    private static final String IMPORTS_RESOURCE =
            "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports";

    @Test
    void autoConfigurationIsRegisteredForDiscovery() throws Exception {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(IMPORTS_RESOURCE)) {
            assertNotNull(in, "缺少 " + IMPORTS_RESOURCE + "，starter 引入后自动装配不会被发现");
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(content.contains(DisruptorAutoConfiguration.class.getName()),
                    "imports 清单应登记 " + DisruptorAutoConfiguration.class.getName());
        }
    }
}
