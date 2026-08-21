package com.sstlfsj.disruptor.tutorial.sink;

import com.sstlfsj.disruptor.tutorial.match.MatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 成交统计 holder（单写多读）。唯一写者是 {@code metrics} stage（parallelism=1），故 {@code tradedVolume}
 * 用 volatile BigDecimal 做读-加-写无竞争、web 读走 volatile 可见性；{@code tradeCount} 用 AtomicLong。
 */
@Component
public class MatchMetrics {

    private static final Logger log = LoggerFactory.getLogger(MatchMetrics.class);

    private final AtomicLong tradeCount = new AtomicLong();
    private volatile BigDecimal tradedVolume = BigDecimal.ZERO;

    /** 累加本次撮合产出里的成交（仅 metrics 单线程调用）。 */
    public void accumulate(List<MatchResult> results) {
        long added = 0;
        BigDecimal vol = BigDecimal.ZERO;
        for (MatchResult r : results) {
            if (r instanceof MatchResult.Trade t) {
                added++;
                vol = vol.add(t.quantity());
            }
        }
        if (added == 0) {
            return;
        }
        tradeCount.addAndGet(added);
        tradedVolume = tradedVolume.add(vol);
        log.debug("[matching/metrics] 累计成交 {} 笔 量 {}", tradeCount.get(), tradedVolume);
    }

    public long tradeCount() { return tradeCount.get(); }

    public BigDecimal tradedVolume() { return tradedVolume; }
}
