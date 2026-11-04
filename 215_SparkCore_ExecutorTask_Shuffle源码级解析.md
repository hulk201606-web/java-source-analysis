# Spark Core Executor Task + Shuffle 源码级深度解析

> **源码基线**：Apache Spark 3.5+（Scala/Java）  
> **核心位置**：core/src/main/scala/org/apache/spark/  
> **面试权重**：⭐⭐⭐⭐⭐（Spark 执行引擎核心，StreamExecution 162 补全）

---

## 一、Spark Core 执行全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Spark Core = DAG Scheduler + Task Scheduler + Executor    │
│                                                               │
│  核心架构（面试必背）：                                        │
│                                                               │
│  Driver（应用端）                                              │
│      │                                                        │
│      ├─ DAGScheduler：RDD → DAG → Stage（宽窄依赖切分）       │
│      │                                                        │
│      └─ TaskScheduler：Stage → TaskSet → 分发到 Executor      │
│                                                               │
│  Executor（Worker 节点）                                       │
│      │                                                        │
│      ├─ TaskRunner：拉 Task → 执行 → 结果回传                 │
│      │                                                        │
│      └─ ShuffleManager：Shuffle Map 端写出 + Reduce 端拉取     │
│
│  ★ 三种调度模式（面试高频）：                                  │
│   1. FIFO（默认）：一个 Application 先跑完下一个才跑          │
│   2. Fair Scheduler：多个 Application 公平分配资源             │
│   3. Dynamic Allocation：Executor 数量动态伸缩                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、DAGScheduler Stage 切分源码级

```scala
// ★ Spark DAG Stage 切分核心源码级
//
// core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala
// core/src/main/scala/org/apache/spark/rdd/RDD.scala
//
// // DAG 是什么？
// //   Spark RDD 操作形成的有向无环图
// //   RDD A → map → B → filter → C → shuffle → D → join → E
// //
// // ★ Stage 切分规则（面试核心！宽窄依赖）
// //
// // 1. Narrow Dependency（窄依赖）：
// //    一个 Parent Partition 只被一个 Child Partition 使用
// //    // map / filter / union 都是窄依赖
// //    // → 可以在同一个 Stage 里流水线执行
// //    // → 不需要数据 Shuffle
// //
// // 2. Wide Dependency（宽依赖）：
// //    一个 Parent Partition 被多个 Child Partition 使用
// //    // groupByKey / reduceByKey / join（HashJoin）
// //    // → 需要 Shuffle（数据重分布）
// //    // → Shuffle 边界切分 Stage
// //
// // ★ Stage 切分算法（源码级，面试高频）：
// def submitStage(rdd: RDD[_]): Unit = {
//     if (!running.contains(stage.id)) {
//         // 找 RDD 的第一个 Shuffle Dependency
//         // 之前的都是 Narrow → 合并到一个 Stage
//         // 遇到 Wide → 切分新 Stage
//         val missing = getMissingParentStages(rdd)
//         // missing = 所有 Wide Dependency 的 Parent Stage
//         missing.foreach(s => submitStage(s))  // 先提交父 Stage
//         submitTasks(stage, ...)              // 再提交当前 Stage
//     }
// }
//
// // ★ Stage 数量 = Shuffle 次数 + 1
// //   例：map → filter → groupByKey → join → count
// //   Stage 1: map → filter（窄依赖，合并）
// //   Stage 2: groupByKey（遇到宽依赖 → 切分）
// //   Stage 3: join（另一个宽依赖）
// //   → 3 个 Stage
//
```

---

## 三、Executor Task 执行源码级

### 3.1 Task 分发 + 执行

```scala
// ★ Spark Executor Task 执行源码级（面试核心）
//
// core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala
// core/src/main/scala/org/apache/spark/executor/Executor.scala
// core/src/main/scala/org/apache/spark/executor/TaskRunner.scala
//
// // Task 分发流程：
// //
// // 1. Driver TaskScheduler 生成 TaskSet（一个 Stage 的所有 Partition）
// //    → 每个 Task 绑定一个 Partition（RDD 的一个分片）
// //
// // 2. Driver 发 LaunchTask RPC 给 Executor
// //    // Task 序列化后通过 Akka/Netty 发给 Executor
// //    // Task 包含：RDD 引用 + Partition ID + 依赖 Jar
// //
// // 3. Executor 收到 LaunchTask → 提交给 TaskRunner 线程
// //    new TaskRunner(task, ...).start()
// //
// // 4. TaskRunner.run() 核心流程：
// //    a. 反序列化 Task + RDD
// //    b. 设置 SparkEnv（TaskMemoryManager / BroadcastManager）
// //    c. 调用 task.run()
// //       // ShuffleMapTask：执行 RDD → 写 Shuffle 输出
// //       // ResultTask：执行 RDD → 返回结果给 Driver
// //    d. 处理 Task 结果：
// //       - Success: 回 Driver 成功状态 + 结果
// //       - FetchFailed: 告诉 Driver Shuffle 输出丢了 → 重新计算上游 Stage
// //       - Exception: 回 Driver 失败 → Driver 重试 Task
// //
// // 5. Speculative Execution（推测执行）：
// //    同一个 Stage 某些 Task 跑得特别慢
// //    // Driver 检测慢 Task → 在空闲 Executor 上启动重复的 Task
// //    // 哪个先完成用哪个
// //    // spark.speculation=true（默认 false）
//
```

### 3.2 Shuffle Map + Reduce 端

```scala
// ★ Spark Shuffle 源码级（面试核心！补全 162 Flink）
//
// core/src/main/scala/org/apache/spark/shuffle/
// core/src/main/scala/org/apache/spark/shuffle/hash/
// core/src/main/scala/org/apache/spark/shuffle/sort/
//
// // ★ Spark 三种 Shuffle 实现：
// //
// // 1. Hash Shuffle（早期，有问题）：
// //    每个 Map Task 按 Partition 写一个文件
// //    // N Map Task × R Reduce Partition = N×R 个文件
// //    // 小集群可能撑不住
// //    // shuffle.hash=false（默认关闭）
// //
// // 2. Sort Shuffle（默认，3.2+ 之前）：
// //    每个 Map Task 一个文件 + 一个索引
// //    Map 端：按 target Partition 排序 → 写一个大文件
// //    Reduce 端：用索引找自己需要的数据段
// //    // 只有 N 个文件（N = Map Task 数）
// //
// // 3. Sort-Merge Shuffle（3.2+ 默认，更优）：
// //    Map 端：和 Sort Shuffle 一样（排序后写一个文件）
// //    Reduce 端：用 SortMergeReader
// //    // 多个 Map 输出文件 → 合并成有序流
// //    // 适合大 Reduce Partition
// //
// // ★ Shuffle 流程（源码级，面试必背）：
// //
// // Map 端（ShuffleMapTask）：
// //   a. RDD 计算 → 输出 (Key, Value) 对
// //   b. Partitioner.partition(key) → 决定发哪个 Reduce Partition
// //   c. 按 Partition 排序 → 写磁盘文件 + 索引文件
// //
// // Reduce 端（ResultTask 或下一个 ShuffleMapTask）：
// //   a. 从 Driver 获取 Map 输出位置（MapOutputTracker）
// //   b. 连接每个 Map Task 的 Executor → 拉取自己 Partition 的数据
// //   c. 合并多个 Map 输出 → 得到完整数据 → 执行下游 RDD
// //
// // ★ Shuffle Fetch Failed 处理（面试高频）：
// //   Reduce 端拉取 Map 输出时失败
// //   // 可能原因：Map Task Executor 崩了 → 磁盘文件丢了
// //   // 或网络问题
// //   // Driver 收到 FetchFailed → 重新计算整个上游 Stage
// //   // （因为 Map 输出文件没了 → 只能全算）
// //
```

---

## 四、Spark Core vs Flink Streaming 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：Spark Core vs Flink Streaming 核心对比          │
│  ┌──────────────┬──────────────────┬──────────────────┐     │
│  │ 维度            │ Spark Core         │ Flink Streaming   │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 执行模型         │ ★ 批处理 + 微批    │ ★ 真流式         │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 延迟            │ 秒级（微批）        │ 毫秒级（持续）    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 状态管理         │ RDD Checkpoint     │ ★ Flink State    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Shuffle          │ ★ SortMerge 磁盘   │ 网络 Shuffle     │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ DAG 切分         │ Stage 宽窄依赖      │ Operator Chain    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Checkpoint       │ RDD 全量快照       │ ★ Barrier 增量    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Exactly-Once     │ RDD Checkpoint     │ ★ Checkpoint + TX │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 调度模型         │ TaskSet 一次执行    │ Continuous Task   │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 面试高频         │ ★★★★★          │ ★★★★★            │     │
│  └──────────────┴──────────────────┴──────────────────┘     │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   批处理 + ETL + 微批流 → Spark Core / Spark SS               │
│   真流式 + 低延迟 + 复杂窗口 → Flink Streaming                │
│   两者配合：Spark 离线 + Flink 实时（Lambda/Kappa 架构）       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Spark Core 核心架构？| **Driver（DAGScheduler+TaskScheduler）+ Executor（TaskRunner+ShuffleManager）** |
| 2 | ★ Stage 切分核心规则？| **宽窄依赖**；窄依赖合并到一个 Stage；宽依赖（Shuffle）切分新 Stage；Stage 数 = Shuffle 次数 + 1 |
| 3 | Narrow vs Wide Dependency？| Narrow：一个 Parent Partition 一个 Child（map/filter/union）；Wide：多个 Parent 一个 Child（groupBy/join）|
| 4 | ★ Executor Task 执行流程？| Driver 发 LaunchTask → Executor TaskRunner.run() → 反序列化 → 执行 task.run() → 回结果 |
| 5 | Spark 三种 Shuffle 实现？| **Hash Shuffle（早期）+ Sort Shuffle（3.2 前默认）+ Sort-Merge Shuffle（3.2+ 默认）** |
| 6 | Shuffle Map 端写什么？| 按 Partition 排序 → 写磁盘数据文件 + 索引文件 |
| 7 | Shuffle Reduce 端拉取？| 从 Driver 拿 Map 输出位置 → 连接每个 Map Executor → 拉自己 Partition 数据 → 合并 |
| 8 | FetchFailed 怎么处理？| **重新计算整个上游 Stage**（Map 输出文件丢了 → 只能全算）|
| 9 | Speculative Execution？| 检测慢 Task → 空闲 Executor 启动重复 Task → 先完成的用哪个 |
| 10 | ★ Spark Core vs Flink Streaming 核心对比？| 见第四节对比表（9 维度）|
| 11 | 面试最高频考点排序？| **1. 宽窄依赖 Stage 切分 2. Executor Task 流程 3. Shuffle 三种实现 4. FetchFailed 处理 5. Spark vs Flink** |
| 12 | 学习路径？| RDD → Narrow/Wide → Stage → Task → Shuffle → FetchFailed → 对比 |
| 13 | 面试常见追问？| Spark Shuffle 和 Flink Shuffle 区别？| Spark：**磁盘 Sort → Reduce 拉本地文件**；Flink：**网络 Shuffle → 直接 RPC 发数据** → Flink 更实时 |
| 14 | 面试常见追问？| Stage 数越多越好还是越少越好？| 越少越好（Stage 切换有开销）；但该切还是要切（Shuffle 必须切）|
| 15 | 面试陷阱？| 说 "Stage = Partition" → ❌ **一个 Stage 包含多个 Task（Partition）**；Stage 是调度单元；Task 是执行单元 |
| 16 | 面试陷阱？| 说 "FetchFailed 只重算失败的 Map Task" → ❌ **整个上游 Stage 重算**（Map 输出文件都丢了 → 只能全算）|
| 17 | Spark 调度模式？| FIFO（默认）+ Fair Scheduler + Dynamic Allocation |
| 18 | Dynamic Allocation 怎么伸缩？| 空闲 Executor 超时回收；队列积压时申请新 Executor |
| 19 | Spark RDD Checkpoint vs Cache？| Cache：内存/磁盘缓存，血统保留；Checkpoint：磁盘快照，血统截断（容错更好）|
| 20 | MapOutputTracker 作用？| Driver 维护所有 Map Task 输出位置；Reduce 端从 Driver 拉位置 |
| 21 | 典型生产配置？| **spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO + spark.speculation=true（慢 Task 多集群）** |
| 22 | Spark 3.x Adaptive Query Execution（AQE）？| 运行时自适应：合并小 Stage、动态切换 Join 策略、优化 Shuffle 分区数 |
| 23 | Spark 内存管理？| Unified Memory：Storage（Cache）+ Execution（Shuffle/Join）；动态抢占 |
| 24 | Task 重试次数？| spark.task.maxFailures=4（默认 4 次）；超过整个 Stage 失败 |
| 25 | Executor 崩溃影响？| Executor 上所有 Task 失败 → Driver 重新调度 → Shuffle 输出丢了 → 上游 Stage 重算 |
| 26 | Spark 2.x vs 3.x Shuffle 默认？| 2.x：Sort Shuffle；3.x：Sort-Merge Shuffle（更优的 Reduce 端合并）|

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：core/.../scheduler/DAGScheduler.scala + executor/Executor.scala + shuffle/  
**面试考点：26 题**  
**差异化点**：宽窄依赖 Stage 切分、三种 Shuffle 实现、FetchFailed 处理、**Spark Core vs Flink Streaming 9 维度对比**
