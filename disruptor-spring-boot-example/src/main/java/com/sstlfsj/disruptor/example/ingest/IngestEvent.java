package com.sstlfsj.disruptor.example.ingest;

import com.sstlfsj.disruptor.core.ShardKeyed;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IngestEvent implements ShardKeyed {
    private String key;
    private int seq;

    @Override
    public Object shardKey() {
        return key;
    }
}
