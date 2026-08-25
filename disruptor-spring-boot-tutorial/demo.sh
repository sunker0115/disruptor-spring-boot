#!/usr/bin/env bash
#
# 撮合 tutorial 一键演示：下单 → 后台单线程 Disruptor 撮合 → 读侧观测 → 背压。
#
# 用法：
#   1) 另开一个终端启动应用：
#        cd <repo> && mvn -pl disruptor-spring-boot-tutorial spring-boot:run
#   2) 本脚本：bash disruptor-spring-boot-tutorial/demo.sh
#
# 看什么：把启动应用那个终端拉出来，观察日志主干
#   [matching/accept] → [matching/match] → [matching/emit]
# 关键：所有 [matching/match] 都打印在【同一个线程名】上 —— 并发进来的订单被单线程无锁串行撮合，
# 盘口却始终正确。这就是"为什么必须 Disruptor 而不是线程池"的价值证据。
#
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"
SYM="${SYM:-BTCUSDT}"

post() {  # side price qty
  curl -s -o /dev/null -w "%{http_code}" -XPOST "$BASE/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"symbol\":\"$SYM\",\"side\":\"$1\",\"price\":$2,\"quantity\":$3}"
}

echo "== 1. 挂一个不交叉买单 BUY@90x5（进盘口）=="
echo "  POST /orders BUY 90 5 -> HTTP $(post BUY 90 5)"
sleep 0.3
echo "  GET /book:"; curl -s "$BASE/book?symbol=$SYM"; echo

echo
echo "== 2. 挂卖 SELL@100x10，再打入买 BUY@100x10（成交）=="
echo "  POST /orders SELL 100 10 -> HTTP $(post SELL 100 10)"
echo "  POST /orders BUY  100 10 -> HTTP $(post BUY 100 10)"
sleep 0.3
echo "  GET /orders/stats:"; curl -s "$BASE/orders/stats"; echo
echo "  GET /book（卖盘被吃掉，仅剩买 90 档）:"; curl -s "$BASE/book?symbol=$SYM"; echo

echo
echo "== 3. 快速灌单触发背压（部分返回 429）=="
codes=""
for i in $(seq 1 60); do codes="$codes $(post BUY 80 1)"; done
echo "  60 次快速下单的 HTTP 状态分布:"
echo "$codes" | tr ' ' '\n' | grep -v '^$' | sort | uniq -c
echo "  （出现 429 = RingBuffer 满、tryPublish 回推背压；默认 buffer=1024 时需更高频，"
echo "    要稳定复现可用更小 buffer 启动：--disruptor.pipelines.matching.buffer-size=16）"

echo
echo "完成。回到应用日志窗口看 [matching/accept]→[matching/match]（同一线程名）→[matching/emit] 主干。"
