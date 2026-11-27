# mini-rpc

基于 Netty 的私有协议 RPC 框架，实现自定义二进制协议编解码、服务注册与发现、负载均衡、容错与链路透传，运行时仅依赖 `netty-all`（零 Spring）。

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

## 设计要点与实测结果

- **能力**：22 字节定长头的私有二进制协议、编解码与粘包拆包处理、JDK SPI 可插拔序列化、内存 / 文件两种注册中心（心跳续约 + 过期剔除）、轮询 / 加权随机 / 一致性哈希三种负载均衡、长连接池复用、请求级超时、可重试异常分类、按节点粒度的熔断、TraceId 透传与 JDK 动态代理。
- **取舍**：解码器先做绝对读判断长度，不足时直接 return 且不移动 `readerIndex`，并限制单帧 8 MiB；每节点只维护一条长连接，由 `IdleStateHandler` 驱动心跳、读空闲超时主动断连；只有超时与网络异常才重试，业务异常不重试，每次重试重新做服务发现以天然实现 failover；一致性哈希 160 虚拟节点，扩缩容只重映射约 1/N 的 key。
- **实测**：16 线程 × 2,000 次 = 32,000 次调用，吞吐 22,289 req/s，P50 / P95 / P99 为 0.69 / 1.30 / 1.58 ms，零失败。

仓库总览见 [根 README](../README.md)。
