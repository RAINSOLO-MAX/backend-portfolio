package dev.rainsolo.delayqueue.idempotent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Swap for Redis SETNX / a unique DB index in production. */
public final class InMemoryIdempotentStore implements IdempotentStore {

    private final Set<String> processed = ConcurrentHashMap.newKeySet();

    @Override
    public boolean isProcessed(String messageId) {
        return processed.contains(messageId);
    }

    @Override
    public void markProcessed(String messageId) {
        processed.add(messageId);
    }

    @Override
    public int size() {
        return processed.size();
    }
}
