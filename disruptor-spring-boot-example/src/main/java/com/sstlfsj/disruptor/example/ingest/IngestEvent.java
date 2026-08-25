package com.sstlfsj.disruptor.example.ingest;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IngestEvent {
    private String key;
    private int seq;

}
