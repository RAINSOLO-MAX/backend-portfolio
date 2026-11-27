# delay-queue

分层时间轮延迟队列，零外部依赖。完整说明见 [仓库根 README](../README.md)。

## 模块结构

```
core/        Clock（单调时间）、DelayedMessage、MessageStatus
config/      QueueConfig：三层轮的 tick 与槽数、重试参数
wheel/       WheelLevel + HierarchicalTimeWheel（含溢出小顶堆）
delivery/    DeliveryListener、指数退避、死信队列
idempotent/  幂等表（生产可替换为 Redis SETNX）
store/       MessageStore + SnapshotStore（相对剩余时间快照）
metrics/     投递/重试/死信/重复抑制计数
demo/        端到端 demo、压测
```

## 运行

```bash
mvn -B -pl delay-queue -am test
java -cp delay-queue/target/classes dev.rainsolo.delayqueue.demo.DelayQueueDemo
```

## 配置示例

```java
QueueConfig config = QueueConfig.builder()
        .tickMillis(1)            // 更高精度、更高 CPU
        .l0Slots(64).l1TickMillis(10).l1Slots(64)
        .l2TickMillis(100).l2Slots(64)
        .defaultMaxAttempts(3)
        .baseBackoffMillis(50).backoffFactor(2.0).maxBackoffMillis(5_000)
        .build();

DelayQueueService service = DelayQueueService.manual(config, new SystemClock(), snapshotPath);
service.registerListener("order", message -> handle(message));
service.submit("order-1", "order", "cancel-if-unpaid", 30_000);
service.tick();      // 手动驱动；create(...) 则自带后台线程
service.snapshot();  // 只写入未完成消息
```

`manual(...)` 用于确定性测试；`create(...)` 启动后台 driver 线程，两者 API 一致。
