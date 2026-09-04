package com.sstlfsj.disruptor.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/** 按显式 typed key 传递的不可变任务上下文。 */
public final class TaskContext {

    private static final TaskContext EMPTY = new TaskContext(Map.of());

    private final Map<Key<?>, Object> values;

    private TaskContext(Map<Key<?>, Object> values) {
        this.values = values;
    }

    public static TaskContext empty() {
        return EMPTY;
    }

    public static TaskContext copyOf(Map<Key<?>, ?> source) {
        Objects.requireNonNull(source, "source 不能为空");
        if (source.isEmpty()) {
            return EMPTY;
        }
        Map<Key<?>, Object> copy = new HashMap<>(source.size());
        source.forEach((key, value) -> {
            Objects.requireNonNull(key, "context key 不能为空");
            Objects.requireNonNull(value, "context value 不能为空");
            key.type().cast(value);
            copy.put(key, value);
        });
        return new TaskContext(Map.copyOf(copy));
    }

    public <T> TaskContext with(Key<T> key, T value) {
        Objects.requireNonNull(key, "key 不能为空");
        Objects.requireNonNull(value, "value 不能为空");
        key.type().cast(value);
        Map<Key<?>, Object> copy = new HashMap<>(values);
        copy.put(key, value);
        return new TaskContext(Map.copyOf(copy));
    }

    public <T> T get(Key<T> key) {
        return find(key).orElseThrow(() -> new NoSuchElementException(
                "context key 不存在: " + key.name()));
    }

    public <T> Optional<T> find(Key<T> key) {
        Objects.requireNonNull(key, "key 不能为空");
        Object value = values.get(key);
        return value == null ? Optional.empty() : Optional.of(key.type().cast(value));
    }

    public boolean contains(Key<?> key) {
        Objects.requireNonNull(key, "key 不能为空");
        return values.containsKey(key);
    }

    public Map<Key<?>, Object> asMap() {
        return values;
    }

    @Override
    public boolean equals(Object object) {
        return this == object || object instanceof TaskContext that && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "TaskContext" + values;
    }

    /** 由名称和声明类型共同定位的上下文键。 */
    public record Key<T>(String name, Class<T> type) {

        public Key {
            Objects.requireNonNull(name, "name 不能为空");
            Objects.requireNonNull(type, "type 不能为空");
            if (name.isBlank()) {
                throw new IllegalArgumentException("name 不能为空白");
            }
        }

        public static <T> Key<T> of(String name, Class<T> type) {
            return new Key<>(name, type);
        }
    }
}
