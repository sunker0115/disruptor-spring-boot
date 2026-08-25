package com.sstlfsj.disruptor.example.reuse;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ReuseEvent {
    private String orderId;
    private String couponCode;   // 可选字段

    public void reset() {
        this.orderId = null;
        this.couponCode = null;
    }
}
