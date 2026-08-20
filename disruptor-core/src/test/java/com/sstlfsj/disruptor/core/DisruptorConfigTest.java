package com.sstlfsj.disruptor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * DisruptorConfig 纯逻辑单元测试：访问器回传构造值，等待策略枚举映射到对应 LMAX 实现。
 */
class DisruptorConfigTest {

    @Test
    void accessorsReturnConstructorValues() {
        DisruptorConfig config = new DisruptorConfig(
                1024, DisruptorConfig.WaitStrategyType.BLOCKING, Duration.ofSeconds(5));
        assertEquals(1024, config.bufferSize());
        assertEquals(DisruptorConfig.WaitStrategyType.BLOCKING, config.waitStrategyType());
        assertEquals(Duration.ofSeconds(5), config.shutdownTimeout());
    }

    @Test
    void createWaitStrategyMapsEachEnumToLmaxImpl() {
        assertInstanceOf(BlockingWaitStrategy.class,
                configWith(DisruptorConfig.WaitStrategyType.BLOCKING).createWaitStrategy());
        assertInstanceOf(YieldingWaitStrategy.class,
                configWith(DisruptorConfig.WaitStrategyType.YIELDING).createWaitStrategy());
        assertInstanceOf(BusySpinWaitStrategy.class,
                configWith(DisruptorConfig.WaitStrategyType.BUSY_SPIN).createWaitStrategy());
        assertInstanceOf(SleepingWaitStrategy.class,
                configWith(DisruptorConfig.WaitStrategyType.SLEEPING).createWaitStrategy());
    }

    private static DisruptorConfig configWith(DisruptorConfig.WaitStrategyType type) {
        return new DisruptorConfig(8, type, Duration.ofSeconds(1));
    }
}
