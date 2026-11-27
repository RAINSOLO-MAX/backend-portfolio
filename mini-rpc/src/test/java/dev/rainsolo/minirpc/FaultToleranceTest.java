package dev.rainsolo.minirpc;

import dev.rainsolo.minirpc.common.RpcException;
import dev.rainsolo.minirpc.common.RpcStatus;
import dev.rainsolo.minirpc.fault.CircuitBreaker;
import dev.rainsolo.minirpc.fault.RetryPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FaultToleranceTest {

    @Test
    void circuitOpensAfterFailuresAndRecoversThroughHalfOpen() throws InterruptedException {
        CircuitBreaker breaker = new CircuitBreaker(3, 10, 80);
        for (int i = 0; i < 3; i++) {
            breaker.recordFailure();
        }
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
        assertFalse(breaker.allowRequest());
        assertTrue(breaker.rejectedRequests() > 0);

        Thread.sleep(110);
        assertTrue(breaker.allowRequest());
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());

        breaker.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
        assertTrue(breaker.allowRequest());
    }

    @Test
    void halfOpenProbeFailureReopensImmediately() throws InterruptedException {
        CircuitBreaker breaker = new CircuitBreaker(2, 5, 40);
        breaker.recordFailure();
        breaker.recordFailure();
        Thread.sleep(60);
        assertTrue(breaker.allowRequest());
        breaker.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
    }

    @Test
    void onlyTransportFailuresAreRetried() {
        RetryPolicy policy = new RetryPolicy(3, 10, 2.0, 100);
        RpcException timeout = RpcException.transport(RpcStatus.TIMEOUT, "timeout", null);
        RpcException business = RpcException.business(RpcStatus.SERVER_ERROR, "bad request");

        assertTrue(policy.shouldRetry(timeout, 1));
        assertFalse(policy.shouldRetry(timeout, 3));
        assertFalse(policy.shouldRetry(business, 1));
        for (int attempt = 1; attempt <= 5; attempt++) {
            assertTrue(policy.backoffMillis(attempt) <= 100);
        }
    }
}
