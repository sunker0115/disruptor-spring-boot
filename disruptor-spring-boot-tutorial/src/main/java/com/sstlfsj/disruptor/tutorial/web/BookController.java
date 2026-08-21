package com.sstlfsj.disruptor.tutorial.web;

import com.sstlfsj.disruptor.tutorial.match.MatchEngine;
import com.sstlfsj.disruptor.tutorial.match.OrderBook;
import com.sstlfsj.disruptor.tutorial.dto.BookResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 盘口深度观测（读侧）。撮合前盘口本就为空，无对应 symbol 返回空档位 200（不 404）。
 */
@RestController
@RequiredArgsConstructor
public class BookController {

    private final MatchEngine engine;

    @GetMapping("/book")
    public BookResponse book(@RequestParam("symbol") String symbol,
                             @RequestParam(name = "levels", defaultValue = "10") int levels) {
        MatchEngine.BookDepth depth = engine.depth(symbol);
        return new BookResponse(symbol, take(depth.bids(), levels), take(depth.asks(), levels));
    }

    private static List<OrderBook.Level> take(List<OrderBook.Level> levels, int n) {
        return levels.size() <= n ? levels : levels.subList(0, n);
    }
}
