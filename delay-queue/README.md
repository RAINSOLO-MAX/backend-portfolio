# delay-queue

分层时间轮延迟队列，提供至少一次投递、幂等消费、指数退避、死信队列与快照恢复，运行时零外部依赖。

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

## 设计要点与实测结果

- **能力**：三层时间轮 + 溢出小顶堆调度（最长小时级延迟）、至少一次投递、幂等消费、指数退避 + equal jitter、死信队列、按"相对剩余毫秒"持久化的快照与崩溃恢复。
- **取舍**：高层任务 tick 到点时降级重新计算，内存占用与消息数量无关、只与层数有关；所有 deadline 使用 `System.nanoTime()` 单调时钟，wall clock 只用于展示创建时间，系统时间回拨不会造成重复投递或永久延迟；快照保存相对剩余时间而非绝对时间戳，恢复时用当前单调时钟重建 deadline，天然跨重启与时钟变更。
- **实测**：10,000 条消息（延迟 10～199 ms），投递吞吐 43,656 msg/s，调度误差 P50 11.58 ms / P95 18.10 ms / P99 19.49 ms；误差主要来自 10 ms tick 粒度与批量到期排队，把 tick 调到 1 ms 可降低延迟但提高 CPU 占用。

仓库总览见 [根 README](../README.md)。
