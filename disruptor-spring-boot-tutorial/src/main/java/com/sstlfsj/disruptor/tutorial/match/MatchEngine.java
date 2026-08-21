package com.sstlfsj.disruptor.tutorial.match;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 多 symbol 撮合入口。核心是 {@link #handle(Order)}：喂一笔单，改盘口，吐回一串 {@link MatchResult}
 * ——纯函数式副作用集中在盘口，无外部 I/O。
 *
 * <p><b>单线程契约</b>：{@code books} 与 {@link OrderBook} 均非线程安全，仅允许由 Disruptor 的
 * {@code match} stage（{@code parallelism = 1}）单线程调用。这不是性能优化而是正确性前提——并发调用会撕裂
 * TreeMap、竞态 sequence。这正是本 tutorial "为什么必须 Disruptor" 的立论核心。</p>
 *
 * <p>撮合逻辑精简自 raftkit {@code AbstractMatchHandler}：只保留 LIMIT + base 驱动（remaining 量），
 * 把 runMatch/doMatch/handleLimit 塌缩为一个 {@link #matchLimit} —— fill = min(taker, maker)，无 sink 回调、无 peek。</p>
 *
 * <p>跨线程读盘口：{@link #handle} 末尾把各 symbol 的深度整体替换到 volatile {@link #depthView}，web 线程无锁读
 * （单写者安全发布）。</p>
 */
public class MatchEngine {

    private static final Logger log = LoggerFactory.getLogger(MatchEngine.class);

    /** 每 symbol 缓存的盘口深度档数（≥ web 默认展示档数）。 */
    private static final int DEPTH_LEVELS = 20;
    private static final BookDepth EMPTY = new BookDepth(List.of(), List.of());

    /** 仅 match 线程访问。 */
    private final Map<String, OrderBook> books = new HashMap<>();
    /** 跨线程只读快照，match 线程整体替换、web 线程读。 */
    private volatile Map<String, BookDepth> depthView = Map.of();

    /** 某 symbol 的盘口深度快照（买卖两侧档位）。 */
    public record BookDepth(List<OrderBook.Level> bids, List<OrderBook.Level> asks) {}

    /**
     * 撮合一笔 taker（LIMIT，价格时间优先）。<b>仅撮合线程调用。</b>
     *
     * @return 本次产出的成交/挂单/完成结果（有序、带确定性 sequence）
     */
    public List<MatchResult> handle(Order taker) {
        OrderBook book = books.computeIfAbsent(taker.getSymbol(), OrderBook::new);
        taker.setPriceLong(book.toPriceLong(taker.getPrice()));

        List<MatchResult> out = new ArrayList<>();
        OrderSide counter = book.counter(taker.getSide());
        if (counter.crosses(taker.getPriceLong())) {
            matchLimit(taker, counter, book, out);
        }
        if (taker.remaining().signum() == 0) {
            out.add(new MatchResult.Done(book.symbol(), book.nextSequence(),
                    taker.getOrderId(), taker.getSide(), BigDecimal.ZERO, true));
        } else {
            book.side(taker.getSide()).append(taker);
            out.add(new MatchResult.Open(book.symbol(), book.nextSequence(),
                    taker.getOrderId(), taker.getSide(), taker.getPrice(), taker.remaining()));
        }
        refreshDepth(taker.getSymbol(), book);
        return out;
    }

    /** web 线程读：某 symbol 的盘口深度（无锁 volatile 快照）。 */
    public BookDepth depth(String symbol) {
        return depthView.getOrDefault(symbol, EMPTY);
    }

    /** taker 吃对手盘：单次前向遍历（优先序），逐档逐 maker 成交，fill = min(taker, maker)。 */
    private void matchLimit(Order taker, OrderSide counter, OrderBook book, List<MatchResult> out) {
        List<Order> filled = new ArrayList<>();
        counter.forEach(bucket -> {
            boolean cross = taker.getSide() == Side.BUY
                    ? taker.getPriceLong() >= bucket.priceLong()
                    : taker.getPriceLong() <= bucket.priceLong();
            if (!cross) {
                return false;   // 优先序下更差档更不交叉 → 停整趟
            }
            BigDecimal price = bucket.price();
            for (Order maker : new ArrayList<>(bucket.orders().values())) {  // 快照：档内摘 maker 安全
                if (taker.remaining().signum() == 0) {
                    break;
                }
                BigDecimal fill = taker.remaining().min(maker.remaining());
                taker.setExecutedQty(taker.getExecutedQty().add(fill));
                maker.setExecutedQty(maker.getExecutedQty().add(fill));
                bucket.onFill(fill);
                counter.onFill(fill);
                out.add(new MatchResult.Trade(book.symbol(), book.nextSequence(),
                        taker.getOrderId(), maker.getOrderId(), taker.getSide(), price, fill, taker.getTransactTime()));
                if (maker.remaining().signum() == 0) {
                    out.add(new MatchResult.Done(book.symbol(), book.nextSequence(),
                            maker.getOrderId(), maker.getSide(), BigDecimal.ZERO, false));
                    filled.add(maker);   // 遍历后删（遍历期改 TreeMap 不安全）
                }
            }
            return taker.remaining().signum() > 0;   // 还有量 → 继续下一档
        });
        for (Order m : filled) {
            counter.remove(m);
        }
    }

    private void refreshDepth(String symbol, OrderBook book) {
        Map<String, BookDepth> next = new HashMap<>(depthView);
        next.put(symbol, new BookDepth(book.depth(Side.BUY, DEPTH_LEVELS), book.depth(Side.SELL, DEPTH_LEVELS)));
        depthView = Map.copyOf(next);
    }
}
