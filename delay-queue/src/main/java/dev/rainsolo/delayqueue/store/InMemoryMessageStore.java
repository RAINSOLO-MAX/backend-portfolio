package dev.rainsolo.delayqueue.store;

import dev.rainsolo.delayqueue.core.DelayedMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryMessageStore implements MessageStore {

    private final ConcurrentMap<String, DelayedMessage> messages = new ConcurrentHashMap<>();

    @Override
    public void save(DelayedMessage message) {
        messages.put(message.getId(), message);
    }

    @Override
    public DelayedMessage find(String id) {
        return messages.get(id);
    }

    @Override
    public List<DelayedMessage> all() {
        return new ArrayList<>(messages.values());
    }

    @Override
    public void remove(String id) {
        messages.remove(id);
    }
}
