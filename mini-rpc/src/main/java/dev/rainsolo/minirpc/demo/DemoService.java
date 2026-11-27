package dev.rainsolo.minirpc.demo;

public interface DemoService {

    String echo(String message);

    int add(int a, int b);

    /** Returns the trace id visible on the provider thread, proving header propagation. */
    String traceId();

    /** Artificial latency, used by timeout and circuit-breaker tests. */
    String slow(long millis);

    /** Always throws on the provider side, used to verify business errors are not retried. */
    String boom();
}
