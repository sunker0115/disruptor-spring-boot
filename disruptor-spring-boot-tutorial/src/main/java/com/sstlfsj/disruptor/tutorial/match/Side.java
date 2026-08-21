package com.sstlfsj.disruptor.tutorial.match;

/** 买卖方向。 */
public enum Side {
    BUY, SELL;

    public Side opposite() {
        return this == BUY ? SELL : BUY;
    }
}
