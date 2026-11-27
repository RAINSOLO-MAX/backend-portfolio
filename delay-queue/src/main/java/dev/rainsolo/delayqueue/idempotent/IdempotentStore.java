package dev.rainsolo.delayqueue.idempotent;

public interface IdempotentStore {

    boolean isProcessed(String messageId);

    void markProcessed(String messageId);

    int size();
}
