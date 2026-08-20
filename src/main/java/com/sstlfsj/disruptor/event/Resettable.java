package com.sstlfsj.disruptor.event;

/**
 * 事件可实现此接口以支持槽位复用时的字段重置。管道会在 DAG 所有叶子阶段之后接一个单线程
 * cleanup handler 调用 {@link #reset()}，清空事件字段，避免该槽位被下轮发布复用时残留旧值
 * （尤其当发布 filler 未覆盖全部字段时）。未实现则不注册 cleanup handler。
 */
public interface Resettable {

    /** 清空事件字段，使其可安全复用。 */
    void reset();
}
