package com.sstlfsj.disruptor.concurrent;

/** Group child 的生命周期被外部调用时抛出。 */
public final class ChildLifecycleOwnershipException extends IllegalStateException {

    ChildLifecycleOwnershipException(String childName, String groupName) {
        super("EventLoop child 的生命周期只能由所属 Group 驱动：child="
                + childName + "，group=" + groupName);
    }
}
