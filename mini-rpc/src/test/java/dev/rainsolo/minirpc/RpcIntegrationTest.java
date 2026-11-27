package dev.rainsolo.minirpc;

import dev.rainsolo.minirpc.client.RpcClient;
import dev.rainsolo.minirpc.common.RpcException;
import dev.rainsolo.minirpc.common.RpcStatus;
import dev.rainsolo.minirpc.common.TraceContext;
import dev.rainsolo.minirpc.config.RpcConfig;
import dev.rainsolo.minirpc.demo.DemoService;
import dev.rainsolo.minirpc.demo.DemoServiceImpl;
import dev.rainsolo.minirpc.registry.InMemoryRegistry;
import dev.rainsolo.minirpc.server.RpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcIntegrationTest {

    private InMemoryRegistry registry;
    private RpcServer server;

    @BeforeEach
    void startServer() throws Exception {
        registry = new InMemoryRegistry();
        server = new RpcServer(0, RpcConfig.defaults(), registry);
        server.registerService(DemoService.class, new DemoServiceImpl());
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.close();
        registry.close();
        TraceContext.clear();
    }

    @Test
    void remoteCallsWorkOverRealNettyChannel() {
        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            assertEquals("echo:hello", service.echo("hello"));
            assertEquals(42, service.add(17, 25));
        }
    }

    @Test
    void traceIdIsPropagatedToProviderThread() {
        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            TraceContext.restore("integration-trace-001", "span-a");
            assertEquals("integration-trace-001", service.traceId());
        }
    }

    @Test
    void oneLongLivedChannelIsReused() {
        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            for (int i = 0; i < 25; i++) {
                service.echo("call-" + i);
            }
            assertEquals(1, client.connectionPool().size());
        }
    }

    @Test
    void timeoutIsReportedAsRetryableTransportFailure() {
        RpcConfig config = RpcConfig.builder()
                .requestTimeoutMillis(120)
                .maxAttempts(2)
                .build();
        try (RpcClient client = new RpcClient(config, registry)) {
            DemoService service = client.createProxy(DemoService.class);
            RpcException error = assertThrows(RpcException.class, () -> service.slow(500));
            assertEquals(RpcStatus.TIMEOUT, error.getStatus());
            assertTrue(client.circuitBreakers().size() > 0);
        }
    }

    @Test
    void businessExceptionIsNotRetried() {
        try (RpcClient client = new RpcClient(RpcConfig.defaults(), registry)) {
            DemoService service = client.createProxy(DemoService.class);
            RpcException error = assertThrows(RpcException.class, service::boom);
            assertEquals(RpcStatus.SERVER_ERROR, error.getStatus());
            assertTrue(!error.isRetryable());
        }
    }
}
