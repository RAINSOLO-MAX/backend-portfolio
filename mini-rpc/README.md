# mini-rpc

手写的 Netty RPC 框架，零 Spring 依赖。完整说明见 [仓库根 README](../README.md)。

## 模块结构

```
common/      请求/响应 DTO、状态枚举、ServiceMeta、TraceContext
protocol/    RpcMessage、帧头编解码（22 字节固定头）、Header 编解码
serialize/   Serializer SPI + 默认 JDK 实现
registry/    InMemoryRegistry（测试/单机）、FileServiceRegistry（跨 JVM + 心跳续约）
loadbalance/ 轮询、加权随机、一致性哈希（160 虚拟节点）
fault/       滑动窗口熔断、可重试异常分类 + 指数退避
client/      连接池、请求-响应关联、超时、动态代理
server/      Netty 服务端、读空闲断连、反射调用
demo/        单机 demo、跨 JVM demo、压测
```

## 运行

```bash
mvn -B -pl mini-rpc -am test
mvn -q -pl mini-rpc org.codehaus.mojo:exec-maven-plugin:3.2.0:java \
  -Dexec.mainClass=dev.rainsolo.minirpc.demo.AllInOneDemo
```

## 可替换点

- 序列化：在 `META-INF/services/dev.rainsolo.minirpc.serialize.Serializer` 注册自己的实现（Hessian / Protobuf / JSON）。
- 注册中心：实现 `ServiceRegistry` 换成 ZooKeeper / Nacos。
- 策略：`RpcConfig.loadBalance("consistentHash")` 切换负载均衡；`requestTimeoutMillis` / `maxAttempts` / `failureThreshold` 均可调。
