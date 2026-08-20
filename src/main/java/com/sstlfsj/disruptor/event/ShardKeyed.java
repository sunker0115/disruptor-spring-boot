package com.sstlfsj.disruptor.event;

/**
 * 事件可实现此接口，在阶段并行（{@code parallelism > 1}）时启用<strong>按 key 分片</strong>：
 * 同一 {@link #shardKey()} 的事件落到同一分片、由同一线程按发布顺序处理，从而保证 per-key 顺序。
 *
 * <p>未实现时，阶段并行按发布序 round-robin 分片：每个事件仍恰好由一个分片处理，
 * 但同类事件可能落在不同分片、不保证 per-key 顺序。</p>
 */
public interface ShardKeyed {

    /** @return 用于分片的 key；相同 key 的事件由同一分片顺序处理。可为 {@code null}（视作固定分片）。 */
    Object shardKey();
}
