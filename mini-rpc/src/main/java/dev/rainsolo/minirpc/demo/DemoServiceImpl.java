package dev.rainsolo.minirpc.demo;

import dev.rainsolo.minirpc.common.TraceContext;

public final class DemoServiceImpl implements DemoService {

    @Override
    public String echo(String message) {
        return "echo:" + message;
    }

    @Override
    public int add(int a, int b) {
        return a + b;
    }

    @Override
    public String traceId() {
        return TraceContext.currentTraceId();
    }

    @Override
    public String slow(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "done";
    }

    @Override
    public String boom() {
        throw new IllegalStateException("provider exploded on purpose");
    }
}
