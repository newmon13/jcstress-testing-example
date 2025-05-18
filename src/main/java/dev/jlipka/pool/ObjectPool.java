package dev.jlipka.pool;

import java.util.Optional;

public interface ObjectPool<T> {
    Optional<T> take();
    void release(T object);
    int getSize();
}
