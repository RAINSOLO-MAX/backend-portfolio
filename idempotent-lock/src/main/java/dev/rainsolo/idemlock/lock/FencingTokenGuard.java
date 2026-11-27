package dev.rainsolo.idemlock.lock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Rejects writes carrying a stale fencing token. Only tokens strictly greater than the
 * last accepted one are allowed, which blocks a paused former lock holder from writing
 * after its lease expired and another holder took over.
 */
public final class FencingTokenGuard {

    private final AtomicLong lastAccepted = new AtomicLong(-1L);
    private final AtomicLong rejected = new AtomicLong();

    public boolean accept(long token) {
        while (true) {
            long last = lastAccepted.get();
            if (token <= last) {
                rejected.incrementAndGet();
                return false;
            }
            if (lastAccepted.compareAndSet(last, token)) {
                return true;
            }
        }
    }

    public long lastAccepted() {
        return lastAccepted.get();
    }

    public long rejectedCount() {
        return rejected.get();
    }
}
