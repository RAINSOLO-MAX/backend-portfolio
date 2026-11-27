# distributed-scheduler

纯 JDK 实现的分布式定时调度模块，面向「按 cron 周期广播分片任务」；与同仓库的 delay-queue（单条延迟消息的到期投递）不同，本模块关注分片广播、misfire 补偿、failover 重分配与幂等执行台账。

## 模块结构

```
distributed-scheduler/
├── pom.xml
├── src/main/java/dev/rainsolo/scheduler/
│   ├── cron/          CronExpression（5/6 段解析、位集、逐字段进位）、CronParseException
│   ├── job/           JobDefinition(builder)、MisfirePolicy、JobListener、ShardContext
│   ├── sharding/      ShardingStrategy、AverageShardingStrategy、ConsistentHashShardingStrategy
│   ├── worker/        WorkerNode、WorkerRegistry（心跳与超时剔除）
│   ├── scheduler/     Clock/SystemClock、ExecutionState/ExecutionRecord/ExecutionLedger、
│   │                  LastFireStore、MissedFireDetector、SchedulerCoordinator、
│   │                  FailoverRebalancer、SchedulerMetrics、Scheduler(builder 门面)
│   └── demo/          ManualClock、SchedulerDemo、SchedulerBenchmark
└── src/test/java/dev/rainsolo/scheduler/   MutableClock 与 5 组 JUnit 5 测试
```

## 运行

```bash
# 在仓库根目录执行
mvn -B -pl distributed-scheduler -am test
java -cp distributed-scheduler/target/classes dev.rainsolo.scheduler.demo.SchedulerDemo
java -cp distributed-scheduler/target/classes dev.rainsolo.scheduler.demo.SchedulerBenchmark
```

确定性驱动示例：

```java
Scheduler scheduler = Scheduler.builder()
        .clock(manualClock)                       // 测试注入可变时钟
        .zone(ZoneOffset.UTC)
        .shardingStrategy(new ConsistentHashShardingStrategy())
        .workerTimeoutNanos(TimeUnit.SECONDS.toNanos(30))
        .maxAttempts(3)
        .build();
scheduler.workerRegistry().register("worker-A", manualClock.nanoTime());
scheduler.registerJob(JobDefinition.builder()
        .jobName("orderSync").cron("0/10 * * * * ?")
        .shardingTotalCount(5).misfirePolicy(MisfirePolicy.FIRE_ONCE_NOW)
        .build(), ctx -> handle(ctx));
scheduler.tick();   // 手动驱动；start()/stop() 另有后台 driver 线程
```

## 调度链路

```mermaid
flowchart LR
    A[CronExpression<br/>位集解析] --> B[MissedFireDetector<br/>触发检测与补偿]
    B --> C[ShardingStrategy<br/>分片计算]
    C --> D[WorkerRegistry<br/>存活 worker 分配]
    D --> E[ExecutionLedger<br/>幂等台账去重]
    E --> F[JobListener<br/>分片执行]
    F -->|FAILED/TIMEOUT| E
```

## misfire 策略

停机期间跨越两个及以上周期时生效；单个周期视为正常触发。无论哪种策略，`lastFire` 都会推进到最近一个应触发时刻，避免重复补偿。

| 策略 | 补偿次数 | lastFire 更新 | 适用场景 |
| --- | --- | --- | --- |
| `IGNORE` | 0 | 推进到最近周期 | 允许丢周期，只求恢复节奏 |
| `FIRE_ONCE_NOW` | 1 | 推进到最近周期 | 汇总类任务，多次补偿无意义 |
| `RESCHEDULE_NEXT` | 0 | 推进到最近周期 | 严格等待下一次计划触发 |

## 关键取舍

- **自己写 Cron 而不引 Quartz**：仅需 5/6 段子集与 `nextAfter`，手写解析器可完全控制位集表示、字段进位和异常语义；引入 Quartz 会带来大量 Job/Trigger/Calendar 抽象与依赖，与「纯 JDK、零外部依赖」目标冲突。
- **逐字段进位 vs 暴力每秒扫描**：月份不匹配直接跳到下月 1 日、日不匹配跳到次日 0 点、时/分/秒分别按字段步长推进，配合迭代上限与八年搜索上界；避免 `0 0 29 2 ?` 这类稀疏表达式扫描上亿秒，同时天然正确处理闰年 2/29 与跨月跨年。
- **一致性哈希 160 虚拟节点**：节点数过少会导致分片倾斜，过多则增加建环成本；160 是常见折中，worker 增减时重映射比例约 1/N，分片集合始终完整无遗漏。
- **misfire 只补一次 + 幂等键边界**：`FIRE_ONCE_NOW` 用最近一个错过周期的 `fireTimeMillis` 生成幂等键 `jobName#fireTimeMillis#shardItem`，不会与历史成功记录冲突；但补偿只覆盖一次业务语义，不重放全部错过的周期，业务需自行保证幂等与可重复处理。
- **failover 与至少一次投递边界**：worker 失联后分片由存活者重新分配，配合同一幂等键使得已成功的分片被抑制、真正丢失的分片被补投；这是「至少一次」而非「恰好一次」，listener 内部仍需业务幂等。
- **为何纯 JDK**：运行时只依赖 `java.time`、`java.util.concurrent`、`java.nio.file`；持久化用 `Properties`，便于审阅、无供应链依赖，也便于在测试中注入 `Clock` 与临时文件。

## 压测

运行 `dev.rainsolo.scheduler.demo.SchedulerBenchmark`（JDK 17 / Windows，20 个 job × 每 job 200 次触发 × 8 分片，4 个 worker）：

```
triggered          : 4000
shardExecutions    : 32000
wallElapsedMillis  : 618
throughput         : 11729.29 triggers/s
tickP99Nanos       : 7080500   (约 7.08 ms，含 8 个分片 listener 同步执行)
assignP99Nanos     : 288000    (约 0.29 ms，纯分片分配)
```

> `tick` p99 包含业务 listener 的同步执行耗时，**不代表调度器自身开销**；纯分片分配的 p99（0.29 ms）才反映策略成本。执行为单线程串行，生产环境需替换为线程池 + 分布式协调。

## 简历描述

- **分片调度组件 distributed-scheduler**（Java 17，纯 JDK）：手写 5/6 段 Cron 解析器（位集 + 逐字段进位）、一致性哈希分片广播、misfire 补偿、worker 心跳剔除与 failover 重分配、幂等执行台账。与同仓库 delay-queue（单条延迟消息到期投递）定位互补。
- **难点与取舍**：不引 Quartz，自己解析 Cron 以完全控制字段进位与异常语义；对 `0 0 29 2 ?` 这类稀疏表达式按"月不匹配跳下月 1 日、日不匹配跳次日 0 点"逐字段跳跃，而非每秒暴力扫描，配合迭代上限与 8 年搜索上界，正确处理闰年 2/29 与跨年。misfire 选择**只补最近一次**并推进 `lastFire`，配合幂等键 `jobName#fireTimeMillis#shardItem` 避免补偿与历史成功记录冲突；failover 属"至少一次"，明确要求 listener 业务幂等。一致性哈希取 160 虚拟节点，实测 3→4 worker 时仅约 **1/4** 分片重映射（初版 FNV-1a 哈希分布偏斜导致 200/300 重映射，加入 Murmur avalanche 混合后修正）。
- **量化结果**：22 个单测覆盖闰年 2/29、跨年、非法表达式、停机跨 3 周期补偿、worker 宕机重分配与重复触发抑制；压测 **11,729 triggers/s**、32,000 次分片执行，分片分配 p99 **0.29 ms**。

**面试可追问**：Cron 的 `nextAfter` 为什么不能暴力每秒扫描？日和周字段同时指定时是 AND 还是 OR？misfire 为什么只补一次、幂等键怎么设计才不撞历史？failover 后如何保证已成功的分片不被重复执行？
