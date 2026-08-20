package com.sstlfsj.disruptor.example.order;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderEvent {
    private String orderId;
    private long amount;
    private boolean persisted;
    private boolean audited;
}
