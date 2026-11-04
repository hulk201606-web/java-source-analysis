# Apache Spark Structured Streaming + DAG Stage/Job 源码级深度解析

> **源码基线**：Apache Spark 3.5.x（Scala/Java）  
> **核心位置**：spark-sql/execution/streaming + spark-core/scheduler  
> **面试权重**：⭐⭐⭐⭐⭐（流处理两巨头之一，必对比 Flink）

---

## 一、Structured Streaming 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Structured Streaming = Spark SQL + 流式语义              │
│                                                               │
│  核心思路：                                                    │
│   把实时数据流看作一张不断追加的"动态表（Unbounded Table）"    │
│   对这张表执行 SQL/DSL 查询 → 增量计算 → 输出结果表           │
│                                                               │
│  与 Flink 核心差异（面试必背对比表见第六节）：                │
│   Structured Streaming 是 **微批（Micro-Batch）** 架构       │
│   Flink 是 **连续算子链 + 事件驱动** 架构                    │
│   Spark 3.3+ Continuous Processing 支持真正的低延迟           │
│                                                               │
│  输出模式 Output Modes：                                       │
│   Append Mode：只输出新追加的行（无聚合/去重）                │
│   Complete Mode：全量重算输出整个结果表                      │
│   Update Mode：只输出被更新的行（有聚合/去重时）             │
│                                                               │
│  Source → Transform → Sink 三阶段：                          │
│   Source：Kafka/FileSocket/Rate 等 + Schema                  │
│   Transform：select/where/groupBy/watermark/join             │
│   Sink：foreachBatch/foreach/outputMode + checkpoint         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、StreamExecution 主循环源码级

### 2.1 核心主循环

```scala
// ★ Structured Streaming 执行主循环（源码级）
//
// spark-sql/src/main/scala/org/apache/spark/sql/execution/streaming/StreamExecution.scala
//
// abstract class StreamExecution {
//
//   // ★ 所有 Structured Streaming 查询都走这里的循环
//   def run(): Unit = {
//     // ① 初始化 checkpoint（从 checkpointLocation 读上次进度）
//     initializeLogically()
//
//     // ② 主循环：不断推进 epoch（微批编号）
//     while (state == ACTIVE) {
//
//       // ★ MicroBatch 模式核心循环
//       // spark-sql/.../MicroBatchExecution.scala
//       val executedPlan = constructExecutionPlan()
//       //    constructExecutionPlan：
//       //    读 checkpoint → 恢复状态 → 生成本次 epoch 的物理计划
//
//       // ③ 找本次 epoch 要处理的 offset 范围
//       val offsets = getNextOffsets()
//       //    Source.getOffset 拿到最新可用 offset
//       //    与上次 committed offset 之间的差 = 本次微批数据量
//
//       // ④ 构造本次 Batch 的 Dataset 查询计划
//       //    把 source 的 offset 范围注入物理计划
//       //    Catalyst 优化器处理（列裁剪/谓词下推/Join 重排）
//
//       // ⑤ ★ 提交给 Spark Core DAGScheduler 执行
//       //    spark-core/.../DAGScheduler.submitJob()
//       val runId = sparkSession.sparkContext.setJobGroup(...)
//       val result = executedPlan.toDataset(sparkSession).collect()
//       //    collect() 会触发 DAGScheduler 切 Stage → 分发 Task
//
//       // ⑥ Commit：推进 Source offset + 写 checkpoint
//       //    注意：Source offset 和 Sink 写入必须原子提交
//       commitOffsets(offsets)
//     }
//   }
// }
//
```

### 2.2 微批推进核心：MicroBatchExecution

```scala
// ★ MicroBatchExecution.runBatch 关键流程（源码级）
//
// spark-sql/src/main/scala/org/apache/spark/sql/execution/streaming/MicroBatchExecution.scala
//
// class MicroBatchExecution extends StreamExecution {
//
//   override def runBatch(): Unit = {
//
//     // ① 从 checkpoint 恢复上次 epoch 的状态（Source offset + Sink 进度）
//     val lastCheckpoint = checkpointFileManager.readCheckpoint()
//     val lastOffsets = lastCheckpoint.sources.toMap
//
//     // ② 向 Source 询问新 offset（推进流式进度）
//     val latestOffsets = sources.map(_.getOffset)
//     //    Kafka Source：从 Kafka broker 拉最新 consumer offset
//     //    File Source：扫目录找新文件
//
//     // ③ 确定本次 epoch 的 offset 区间
//     val currentOffsets = computeCurrentOffsets(lastOffsets, latestOffsets)
//     //    限制 maxFilesPerTrigger / maxBytesPerTrigger
//     //    避免一次拉太多（微批的"微"）
//
//     // ④ 构造本次执行计划
//     //    logicalPlan → Catalyst 优化 → physicalPlan
//     //    注入 Source 的 offset 范围（SourceExec 节点）
//     //    注入 Stateful 算子的状态恢复逻辑
//     val physicalPlan = createPhysicalPlan(currentOffsets)
//
//     // ⑤ ★ 触发 Spark Core Job 执行（DAG 调度）
//     sparkSession.sparkContext.submitJob(physicalPlan)
//
//     // ⑥ 执行后：State Store Checkpoint
//     //    spark-sql/.../state/StateStore.scala
//     //    有状态算子（聚合/Join）的状态快照写 checkpoint
//     stateStores.foreach(_.checkpoint())
//
//     // ⑦ 最终 Commit：所有步骤都成功后
//     //    Source offset → committed offsets
//     //    写 metadata checkpoint（原子性）
//     writeCheckpoint(currentOffsets)
//   }
// }
//
```

### 2.3 Continuous Processing（真正低延迟）

```scala
// ★ Spark 3.3+ Continuous Processing（源码级）
//
// spark-sql/src/main/scala/org/apache/spark/sql/execution/streaming/continuous/ContinuousExecution.scala
//
// class ContinuousExecution extends StreamExecution {
//
//   // 区别于 MicroBatch 的 runBatch 循环：
//   // Continuous 使用 ContinuousReader + 持续 Task
//   // 没有 epoch 概念，数据持续流入算子链
//   //
//   // 关键：ContinuousTrigger（trigger=Continuous）
//   // 支持毫秒级延迟（微批通常 1s+）
//   //
//   // 内部：
//   //  - ContinuousDataSource 提供 ContinuousReader
//   //  - Task 持续运行，checkpoint 定期触发
//   //  - Exactly-Once 靠 checkpoint + 幂等 Sink 保证
//
//   // ★ 但限制较多：
//   //  - 只支持少数 Source（Kafka + 实现了 Continuous 的）
//   //  - 有状态算子有限支持
//   //  - 大多数生产场景仍用 MicroBatch（1s~1min 触发周期）
// }
//
```

---

## 三、Spark Core DAG Stage/Job 调度源码级

### 3.1 DAGScheduler 核心

```scala
// ★ DAGScheduler 主调度器（源码级）
//
// spark-core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
//
// class DAGScheduler(sc) {
//
//   // ★ 入口：应用代码调用 sparkContext.submitJob() / collect()
//   def submitJob[T](...) {
//     // ① 将 RDD DAG 转换成 Stage DAG
//     //    识别 shuffle 边界（宽依赖 → ShuffleMapStage）
//     //    无 shuffle 的连续窄依赖 → 一个 ResultStage
//     val job = newActiveJob(...)
//     val stages = jobToStages(job)
//     //    stages 是 DAG，按依赖拓扑排序
//
//     // ② 提交 job：先提交无依赖的 Stage
//     submitStage(stages.head)
//   }
//
//   // ★ Stage 提交（递归处理依赖）
//   private def submitStage(stage: Stage) {
//     // 等待父 Stage 完成（shuffle 输出就绪）
//     val missing = getMissingParentStages(stage)
//     if (missing.isEmpty) {
//       // 父依赖就绪 → 创建 TaskSetManager 提交给 TaskScheduler
//       val taskSet = new TaskSet(stage, stage.tasks)
//       taskScheduler.submitTasks(taskSet)
//     } else {
//       missing.foreach(submitStage) // 先递归提交父 Stage
//     }
//   }
//
//   // ★ Stage 完成回调
//   // 当 TaskSetManager 完成所有 Task 后触发：
//   def stageCompleted(stage: Stage) {
//     // 标记 shuffle map output 就绪
//     // 检查是否有下游 Stage 可以启动了
//     activeStages.filter(_.dependsOn(stage)).foreach(submitStage)
//   }
// }
//
```

### 3.2 Stage 切分：窄依赖 vs 宽依赖

```scala
// ★ DAG Stage 切分规则（源码级，面试必背）
//
// spark-core/.../rdd/RDD.scala + DAGScheduler.scala
//
// ★ 窄依赖（Narrow Dependency）：
//    父 RDD 的每个 Partition 最多被一个子 Partition 消费
//    例：map/filter/flatMap/union 同分区数
//    → 一个 Stage 内完成，不需要 shuffle
//
// ★ 宽依赖（Shuffle Dependency）：
//    一个父 Partition 的数据被多个子 Partition 消费
//    例：groupByKey/reduceByKey/join/distinct
//    → 必须产生 shuffle，形成 Stage 边界
//
// // RDD.partitioner 判断是否 shuffle：
// // spark-core/.../ShuffleDependency.scala
// case class ShuffleDependency(
//     rdd: RDD[_],
//     partitioner: Partitioner,   // 分区策略（HashPartitioner 等）
//     serializer: Serializer,      // shuffle 数据序列化格式
//     keyOrdering: Option[Ordering[_]]
// ) extends Dependency(rdd)
//
// ★ Stage 类型（源码级）：
//  // ShuffleMapStage：产出 shuffle 中间数据
//  class ShuffleMapStage(..., numShufflePartitions: Int) extends Stage
//  //    Task 是 ShuffleMapTask：把数据按 partitioner 写磁盘
//  //
//  // ResultStage：产出最终结果给 Driver
//  class ResultStage(..., func: (TaskContext, Iterator[_]) => _) extends Stage
//  //    Task 是 ResultTask：执行最终计算返回给 Driver
//  //    Structured Streaming 的 ResultStage 最后一步是写 Sink
//
```

### 3.3 TaskScheduler + TaskSetManager

```scala
// ★ TaskSchedulerImpl + TaskSetManager（源码级）
//
// spark-core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala
// spark-core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala
//
// class TaskSchedulerImpl(sc) {
//   // ★ 接收 DAGScheduler 提交的 TaskSet
//   def submitTasks(taskSet: TaskSet) {
//     val manager = new TaskSetManager(this, taskSet, maxFailures = 4)
//     //    TaskSetManager：跟踪这个 Stage 的所有 Task
//     //    负责任务调度、重试、推测执行
//
//     // 用 FIFO / Fair Scheduler 资源池分组
//     // 按 application → pool → stage 三级优先级调度
//     schedulableBuilder.addTaskSetManager(manager)
//
//     // 向所有 TaskManager 发 RPC：offer 资源
//     // taskManager.endpoint.ask(ReviveOffers)
//     backend.reviveOffers()
//   }
//
//   // ★ TaskManager 回复 offer（有空闲 slot）
//   def resourceOffers(offers: Seq[WorkerOffer]) {
//     // 按调度策略（FIFO / FAIR）给 offers 分配 tasks
//     sortedPools.flatMap { pool =>
//       sortedTaskSets.flatMap { manager =>
//         manager.resourceOffer(...)  // 分配一个 task 到一个 slot
//       }
//     }
//   }
// }
//
// class TaskSetManager {
//   // 核心字段：
//   val pendingTasks = new HashSet[Int]    // 还没分配的 task index
//   val runningTasks = new HashMap[Int, TaskInfo]
//   val failedTasks = new HashMap[Int, Int] // taskIndex → 失败次数
//   val taskAttempts = new HashMap[Int, List[TaskInfo]] // 每个 task 的历史尝试
//
//   // ★ 推测执行（Slow Task Speculation）
//   //   spark.task.speculation.interval（毫秒）
//   //   如果同一个 task 的多个 attempt 中，25% 慢于中位数 1.75 倍
//   //   则启动额外 attempt（最快完成的胜出）
//   //   class TaskSetManager.checkSpeculation()
// }
//
```

---

## 四、Structured Streaming 状态管理

### 4.1 State Store 核心

```scala
// ★ Structured Streaming 状态存储（源码级）
//
// spark-sql/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala
// spark-sql/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateStoreProvider.scala
//
// ★ 状态类型（面试必背）：
//   ① 分组聚合状态：groupByKey + agg 维护每个 key 的累加器
//   ② Join 状态：stream-stream join 维护两侧的事件窗口
//   ③ FlatMapGroupsWithState：用户自定义状态
//
// trait StateStore {
//   // 核心能力：
//   def get(key: UnsafeRow): UnsafeRow      // 读状态
//   def put(key: UnsafeRow, value: UnsafeRow): Unit  // 写状态
//   def remove(key: UnsafeRow): Unit        // 删状态
//   def checkpoint(): Long                 // 刷 checkpoint，返回版本号
//
//   // 实现：内存 + RocksDB
//   // 小状态用 MemoryStateStoreProvider（适合测试）
//   // 大状态用 RocksDBStateStoreProvider（生产默认）
// }
//
// class RocksDBStateStoreProvider {
//   // 每个 partition 对应一个 RocksDB 实例
//   // checkpoint = RocksDB SST 文件快照 + 元数据
//   // Structured Streaming 的 checkpoint 目录里：
//   //   offsets/    → Source 消费进度
//   //   commits/    → 提交点（保证 Exactly-Once）
//   //   state/      → State Store checkpoint
//   //   metadata/   → 查询计划等
// }
//
```

### 4.2 Watermark + EventTime

```scala
// ★ Structured Streaming EventTime + Watermark（源码级）
//
// spark-sql/.../execution/streaming/eventTime/EventTime.scala
// spark-sql/.../WatermarkPropagator.scala
//
// ★ 核心概念：
//   Event Time：事件真正发生的时间（数据里的 timestamp 字段）
//   Processing Time：事件被 Spark 处理的时间
//   Watermark：Spark 假设的"Event Time 至少推进到了 t"的水位
//
// // watermark("eventTime", "10 minutes") 含义：
// // 延迟到达超过 10 分钟的事件会被丢弃
// // 窗口/Join 等状态在 watermark 推进后会自动清理旧状态
//
// class WatermarkPropagator {
//   // 在每个 MicroBatch 结束时：
//   //   扫描 Source 的 EventTime 最大值 → maxTimestamp
//   //   当前 watermark = maxTimestamp - delayThreshold
//   //   watermark 单调递增（不能倒退！）
//   //
//   // ★ 为什么单调递增：
//   //   如果 watermark 倒退，之前被清理的状态就不该清理
//   //   保证"一旦 watermark 过了某个时间点，就永远不会回来"
// }
//
// ★ Window + Watermark 配合：
//   Tumbling Window：固定大小不重叠（window(eventTime, "10 minutes")）
//   Sliding Window：固定大小可重叠（window(eventTime, "10 minutes", "5 minutes")）
//
// // 窗口状态清理：
// // watermark 超过 window.endTime + delayThreshold 后
// // 这个 window 的所有 key 的聚合状态被删除（节省内存）
//
```

---

## 五、Checkpoint 与 Exactly-Once

### 5.1 Checkpoint 目录结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Structured Streaming Checkpoint 目录（面试高频）          │
│                                                               │
│  checkpointLocation/                                          │
│  ├── offsets/          Source 消费进度（每个 epoch 一个文件）│
│  │   └── 0 → 1000.json
│  │   └── 1000 → 2000.json
│  │   └── ...
│  │   └── 当前 offset.json（最新提交点）                      │
│  │                                                             │
│  ├── commits/          提交记录（保证原子性）               │
│  │   └── 0 → 1000.json                                        │
│  │   └── 1000 → 2000.json                                     │
│  │   （只有写了 commit 才表示这一轮 epoch 真正完成）        │
│  │                                                             │
│  ├── state/            State Store 快照                      │
│  │   └── [opId]/[partitionId]/[version]/ (RocksDB SST)      │
│  │                                                             │
│  └── metadata/         查询计划 + 状态恢复元信息             │
│      └── checkpoint                                            │
│      └── offsets                                               │
│
│  ★ Exactly-Once 保证：                                        │
│   ① Source offset 先推进到 checkpoint（但 commit 还没写）    │
│   ② 执行本次 epoch 的计算（DAG）                             │
│   ③ Sink 写入（可能重入，需要幂等 Sink）                     │
│   ④ 所有都成功 → 写 commit 文件（原子提交点）                │
│                                                               │
│   失败恢复时：                                                  │
│   从最后一个 commit 恢复 → 重新执行 → Source 幂等读          │
│   Sink 幂等写 → 保证 Exactly-Once 语义                      │
│
└─────────────────────────────────────────────────────────────┘
```

### 5.2 foreachBatch Sink 两阶段提交

```scala
// ★ foreachBatch + ForeachWriter 实现 Exactly-Once Sink（源码级）
//
// spark-sql/.../execution/streaming/sink/ForeachWriter.scala
//
// // ForeachWriter 生命周期（面试必背）：
// trait ForeachWriter[T] extends Serializable {
//   def open(partitionId: Long, version: Long): Boolean
//   // 每个 partition 每个 epoch 调用一次
//   // open 返回 false → 跳过这个 partition（幂等）
//
//   def process(value: T): Unit
//   // 处理每一行数据
//
//   def close(errorOrNull: Throwable): Unit
//   // 成功时 close(null)；失败时 close(exception)
// }
//
// // ★ 生产级 Exactly-Once Sink：
// //   JDBC Sink：每个 batch 开事务，全部成功后 commit
// //   Kafka Sink：Producer 幂等 + 事务 (transactional.id)
// //   Iceberg Sink：Write → Commit 两阶段
// //
// // 关键：Sink 的幂等性靠 Sink 自身保证
// // Structured Streaming 负责"重放失败的 epoch"
// //
```

---

## 六、差异化对比：Spark Structured Streaming vs Flink

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试最高频对比表（Spark SS vs Flink）                     │
│  ┌──────────┬───────────────────┬──────────────────┐        │
│  │ 维度      │ Spark Structured   │ Flink            │        │
│  │           │ Streaming         │                  │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 架构模型  │ ★ 微批（Micro-     │ 事件驱动算子链   │        │
│  │           │   Batch）          │ （Continuous）   │        │
│  │           │ Continuous 3.3+    │                  │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 延迟      │ 1s~1min（微批）   │ ★ ms 级         │        │
│  │           │ Continuous: 100ms │                  │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 执行引擎  │ ★ Spark Core DAG  │ 独立执行引擎     │        │
│  │           │ （批流一体）      │ （StreamRuntime）│        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 状态存储  │ ★ StateStore      │ KeyedState       │        │
│  │           │ （RocksDB/内存）  │ （RocksDB/FS）   │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ Checkpoint│ ★ Metadata+RocksDB│ ★ Barrier+状态  │        │
│  │           │ SST 快照          │ 异步快照         │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 窗口语义  │ ★ EventTime/      │ ★ EventTime/     │        │
│  │           │ Watermark/窗口    │ Watermark/窗口+  │        │
│  │           │                  │ CEP              │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 反压      │ 弱（微批吞吐限制）│ ★ Credit 流控    │        │
│  │           │                  │ 精细反压         │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 容错恢复  │ 重跑整个 Stage    │ ★ 局部恢复       │        │
│  │           │ （可能重算大量）  │ （按 subtask）   │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ SQL/DML   │ ★ Spark SQL 完整  │ ★ Flink SQL 完整 │        │
│  │           │ + Catalyst 优化   │ + Calcite 优化   │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 批流一体  │ ★ 原生            │ 1.12+ 完善       │        │
│  │           │ （同一 Dataset）  │ （动态表+SQL）    │        │
│  ├──────────┼───────────────────┼──────────────────┤        │
│  │ 社区/生态  │ ★ 大，企业应用多 │ ★ 流处理第一     │        │
│  │           │                  │ 实时首选         │        │
│  └──────────┴───────────────────┴──────────────────┘        │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   低延迟流处理 → Flink（反压精细、毫秒级、CEP）              │
│   批流统一数仓 → Spark（ETL+实时 同 API、Spark SQL 完整）    │
│   已有 Spark 栈 → 优先 SS（运维成本、团队技能）               │
│   已有 Flink 栈 → 优先 Flink（实时能力更强）                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（28 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Structured Streaming 核心思想？| 把流式数据看作**动态表**，用 SQL/DSL 增量查询 |
| 2 | 输出模式有哪些？| **Append（只新行）+ Complete（全量重算）+ Update（更新行）** |
| 3 | 微批模式原理？| 周期性触发（1s/1min），每次拿"上次 offset 到新 offset"作为一批，提交给 Spark Core DAG 执行 |
| 4 | StreamExecution 主循环？| 初始化 → 循环：constructPlan → getOffsets → 执行 DAG → commit |
| 5 | 连续处理 Continuous？| Spark 3.3+ 支持 ContinuousReader，真正事件驱动，毫秒级延迟 |
| 6 | DAGScheduler 的作用？| 把 RDD DAG 按 shuffle 边界切成 Stage DAG，调度执行 |
| 7 | 窄依赖 vs 宽依赖？| 窄：父 partition 只被一个子用（map/filter）；宽：被多个子用（groupBy/join）→ 形成 Stage 边界 |
| 8 | Stage 类型？| **ShuffleMapStage（产出 shuffle 数据）+ ResultStage（产出最终结果给 Driver/Sink）** |
| 9 | TaskSchedulerImpl 做什么？| 接收 DAGScheduler 的 TaskSet，用 FIFO/Fair 调度给 TaskManager 的 slot |
| 10 | TaskSetManager 跟踪什么？| pending/running/failed task、尝试次数、推测执行检查 |
| 11 | 推测执行？| 同一 task 25% 的 attempt 慢于中位数 1.75 倍 → 启动额外 attempt，最快完成的胜出 |
| 12 | Structured Streaming 状态类型？| 分组聚合状态、Join 状态、FlatMapGroupsWithState 自定义状态 |
| 13 | StateStore 实现？| 默认 RocksDBStateStoreProvider（生产）；测试用 MemoryStateStoreProvider |
| 14 | Watermark 是什么？| Spark 假设 Event Time 至少推进到 t 的水位；单调递增不后退 |
| 15 | Watermark 作用？| 延迟到达超过 watermark 的事件丢弃；推进后自动清理过期状态 |
| 16 | Checkpoint 目录结构？| **offsets（Source 进度）+ commits（原子提交点）+ state（状态快照）+ metadata（计划）** |
| 17 | Exactly-Once 怎么保证？| Source offset + DAG 执行 + Sink 幂等写 → 全部成功才写 commit 文件 |
| 18 | ForeachWriter 生命周期？| **open(partition, version) → process(value) → close(error)**；open 返回 false 幂等跳过 |
| 19 | Spark SS vs Flink 延迟？| SS 微批 1s+；Flink ms 级；SS Continuous 100ms 但限制多 |
| 20 | DAGScheduler.submitJob 做什么？| RDD DAG → Stage DAG → 提交无依赖 Stage → 回调启动下游 |
| 21 | shuffle 边界判断？| 检查 Dependency 类型是 ShuffleDependency（分区数/partitioner 不同） |
| 22 | 窗口类型？| Tumbling（固定大小不重叠）+ Sliding（固定大小可重叠） |
| 23 | 面试最高频考点排序？| **1. 微批架构+主循环 2. DAG Stage 切分+宽窄依赖 3. Checkpoint 目录+Exactly-Once 4. Watermark 5. SS vs Flink 对比** |
| 24 | 学习路径？| StreamExecution → MicroBatchExecution → DAGScheduler → StateStore → Watermark |
| 25 | 面试常见追问？| 微批的"微"怎么控制？→ maxFilesPerTrigger/maxBytesPerTrigger/processingTime |
| 26 | 面试常见追问？| 一个 Structured Streaming 查询有几个 Job？→ 每个 MicroBatch 一个 Job（Spark UI 上会看到多个 Job）|
| 27 | 面试陷阱？| 说 Spark Structured Streaming 是批处理 → ❌ 是流处理，只是用微批架构实现；说 SS 没有 Checkpoint → ❌ 是核心机制 |
| 28 | 选型建议？| 低延迟流处理 → Flink；批流一体数仓 → Spark SS；已有 Spark 栈 → 优先 SS |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：spark-sql/execution/streaming/StreamExecution.scala + MicroBatchExecution.scala + spark-core/scheduler/DAGScheduler.scala + TaskSchedulerImpl.scala + state/StateStore.scala  
**面试考点：28 题**  
**差异化点**：微批 vs 事件驱动（vs Flink）、宽窄依赖 Stage 切分、StateStore 状态管理、Checkpoint 原子性、Watermark 单调递增
