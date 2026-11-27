package dev.rainsolo.minirpc.common;

import java.util.UUID;

/**
 * Thread-local trace context. The client proxy injects the ids into frame headers;
 * the server handler restores them before invoking the local service and clears them afterwards.
 */
public final class TraceContext {

    public static final String HEADER_TRACE_ID = "traceId";
    public static final String HEADER_SPAN_ID = "spanId";

    private static final ThreadLocal<String> TRACE_ID = new ThreadLocal<>();
    private static final ThreadLocal<String> SPAN_ID = new ThreadLocal<>();

    private TraceContext() {
    }

    public static String currentTraceId() {
        return TRACE_ID.get();
    }

    public static String currentSpanId() {
        return SPAN_ID.get();
    }

    /** Reuse an inbound trace id, or start a new trace when this is the entry point. */
    public static String traceIdOrNew() {
        String existing = TRACE_ID.get();
        if (existing != null && !existing.isEmpty()) {
            return existing;
        }
        String generated = UUID.randomUUID().toString().replace("-", "");
        TRACE_ID.set(generated);
        return generated;
    }

    public static String nextSpanId() {
        String span = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        SPAN_ID.set(span);
        return span;
    }

    public static void restore(String traceId, String spanId) {
        TRACE_ID.set(traceId);
        SPAN_ID.set(spanId);
    }

    public static void clear() {
        TRACE_ID.remove();
        SPAN_ID.remove();
    }
}
