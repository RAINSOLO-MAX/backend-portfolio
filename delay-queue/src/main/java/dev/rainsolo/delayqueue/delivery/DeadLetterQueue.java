package dev.rainsolo.delayqueue.delivery;

import dev.rainsolo.delayqueue.core.DelayedMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Messages that exhausted all delivery attempts, kept for inspection and replay. */
public final class DeadLetterQueue {

    private final List<DelayedMessage> letters = new CopyOnWriteArrayList<>();

    public void add(DelayedMessage message) {
        letters.add(message);
    }

    public List<DelayedMessage> list() {
        return List.copyOf(letters);
    }

    public int size() {
        return letters.size();
    }
}
