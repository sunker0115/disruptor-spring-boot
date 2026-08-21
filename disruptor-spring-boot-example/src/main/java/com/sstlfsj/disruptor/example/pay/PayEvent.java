package com.sstlfsj.disruptor.example.pay;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PayEvent {
    private String payId;
    private boolean persisted;
    private boolean audited;
}
