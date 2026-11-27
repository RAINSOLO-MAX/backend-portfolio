package dev.rainsolo.delayqueue.store;

import dev.rainsolo.delayqueue.core.DelayedMessage;

import java.util.List;

public interface MessageStore {

    void save(DelayedMessage message);

    DelayedMessage find(String id);

    List<DelayedMessage> all();

    void remove(String id);
}
