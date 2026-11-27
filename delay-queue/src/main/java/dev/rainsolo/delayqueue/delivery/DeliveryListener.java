package dev.rainsolo.delayqueue.delivery;

import dev.rainsolo.delayqueue.core.DelayedMessage;

/** Consumer callback. Throwing signals a failed delivery and triggers retry/backoff. */
@FunctionalInterface
public interface DeliveryListener {

    void onMessage(DelayedMessage message) throws Exception;
}
