package com.sstlfsj.disruptor.example.reuse;

import com.sstlfsj.disruptor.core.Resettable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ReuseEvent implements Resettable {
    private String orderId;
    private String couponCode;   // 可选字段

    @Override
    public void reset() {
        this.orderId = null;
        this.couponCode = null;
    }
}
