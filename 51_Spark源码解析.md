# Apache Spark 源码深度解析（批流一体计算引擎）

> **源码基线**：Apache Spark 3.5.x（最新稳定）  
> **核心仓库**：https://github.com/apache/spark  
> **面试权重**：⭐⭐⭐⭐⭐（大数据最高频）

---

## 一、Spark 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：批流一体大数据计算引擎（MapReduce 替代）           │
│  解决什么问题？大数据（TB-PB 级）的实时/离线计算              │
│                                                               │
│  对比 MapReduce vs Flink vs Spark：                          │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │ MapReduce    │ Spark          │ Flink        ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 模型           │ 批处理（磁盘）│ **批流统一 RDD** │ 真正流处理     ││
│  │ 延迟           │ 分钟级       │ 秒级（微批）    │ 毫秒级        ││
│  │ 吞吐量         │ ★★★★★ 最高  │ ★★★★★ 最高   │ ★★★★ 中      ││
│  │ 迭代计算       │ ❌ 每次写磁盘 │ ✅ RDD 血缘 + 缓存│ ❌（每次重算）││
│  │ 状态管理       │ ❌           │ ★★★ 内置       │ ★★★★★ 完美    ││
│  │ 生态           │ Hadoop 生态  │ Hadoop + 独立  │ K8s + 独立    ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ Spark 最大优势：迭代计算（ML/图计算）+ 批流统一 + Catalyst 优化│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Spark 集群架构                                               │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   Driver       │  ApplicationMaster │ Executor      │     │
│  │  客户端程序    │ YARN 容器（1 个）    │ Worker JVM    │     │
│  │  发起 Job      │ 调度 Coordinator    │ 执行 Task     │     │
│  │ 生成 RDD DAG   │ DAGScheduler        │ 存储 RDD 缓存 │     │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         ↓              ↓              ↓                       │
│    SparkContext   DAGScheduler    TaskScheduler               │
│                                    ↓                          │
│                              Worker 节点（N 个 Executor）    │
│                                                               │
│ ★ Spark on K8s：直接跑 Driver（Job Pod）+ Executor（动态 Pod）│
│ ★ Spark 3.0+ 默认 AQE（自适应查询执行）                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 RDD（★★★★★ 灵魂概念）

```scala
// ★ RDD = Resilient Distributed Dataset（弹性分布式数据集）
//   Spark 最核心的抽象（Spark SQL/DataFrame 底层都是 RDD）
//
// RDD 五大特征（面试高频！）：
//
// ① Partitions（分区）：数据分片，一个 RDD 有 N 个 Partition
// ② Partitioning（分区函数）：Hash / Range / Custom
// ③ Dependencies（依赖关系）：RDD 之间的血缘（Lineage）
// ④ Iterator（计算函数）：每个 Partition 的计算逻辑
// ⑤ Preferred Location（偏好位置）：数据本地化（HDFS 同节点）
//
// ★ 两种操作类型：
//
// Transformation（转换，lazy）：
//   map / filter / flatMap / groupByKey / join / reduceByKey
//   → 不立即执行，只构建 RDD DAG
//
// Action（行动，trigger）：
//   count / collect / foreach / saveAsTextFile / take / top
//   → 触发执行 DAG → 提交 Job
//
// ★ RDD 血缘（Lineage）：
//   rddA.map().filter().reduce()
//   → rddA（Source RDD）
//     → rddB（map 依赖 rddA，1:1，Narrow Dependency）
//       → rddC（filter 依赖 rddB，1:1，Narrow Dependency）
//         → reduce 触发 Shuffle → Wide Dependency
//
// ★ Narrow vs Wide Dependency（面试高频！）：
//   Narrow：父 RDD 一个 Partition → 子 RDD 一个 Partition（1:1）
//     → map / filter / flatMap / coalesce
//     → 不需要 Shuffle！可以 Chain
//   Wide：父 RDD 多个 Partition → 子 RDD 一个 Partition（N:1 / 1:N）
//     → groupByKey / reduceByKey / join / distinct
//     → 需要 Shuffle（写磁盘 + 网络拷贝）
```

### 2.2 DAGScheduler + Stage 划分（★★★★★）

```scala
// ★ DAGScheduler 是 Spark 调度核心
//   把 RDD DAG 切成 Stage
//
// Stage 划分规则：
//   每个 Shuffle Dependency 切割一个 Stage 边界
//   → Narrow Dependency 不切（可以 Chain）
//   → 最后一个 Stage 是 Result Stage（直接计算结果返回 Driver）
//   → 前面的 Stage 是 Shuffle Map Stage（输出 Shuffle 数据）
//
// 示例：
//   rddA (Source, 3 partitions)
//     → map (Narrow) → rddB
//     → filter (Narrow) → rddC
//     → groupByKey (Wide, Shuffle!) → rddD
//     → count (Action)
//
//   Stage 划分：
//   Stage 0：rddA → map → filter（Narrow 连在一起，3 partitions）
//   Stage 1：groupByKey 的 Reduce 端（N partitions 取决于 Key 分布）
//
// ★ Stage 调度流程：
//   ① DAGScheduler 发现 Stage 0 没有上游依赖 → 立即提交
//   ② TaskScheduler 给 Worker 分配 Task（每个 Partition 一个 Task）
//   ③ Executor 执行 Stage 0 Task → 输出 Shuffle 数据
//   ④ Stage 0 全部完成 → 发现 Stage 1 依赖 Stage 0 → Stage 1 就绪
//   ⑤ 提交 Stage 1 → Executor 拉 Shuffle 数据 → 执行
//   ⑥ 最后一个 Stage 完成 → 返回结果
//
// ★ Shuffle 实现：
//   Map 端：每个 Task 按分区 Hash 写磁盘文件（N 个分区 → N 个文件）
//   Reduce 端：并发拉取所有 Map 的对应分区文件 → 排序 → 聚合
//   中间：Shuffle Service（Spark 内置或外部）
```

### 2.3 Catalyst + Tungsten + Whole-Stage CodeGen（★★★★★）

```scala
// ★ Spark SQL/DataFrame 优化栈（面试最高频！）
//   Catalyst（逻辑优化）→ Tungsten（物理优化）→ Whole-Stage CodeGen（代码生成）
//
// Catalyst Optimizer（规则 + 成本）：
//   SQL / DataFrame API
//     ↓ Analysis（解析）
//   → ResolveNames / ResolveTypes / ResolveRelations
//     ↓ Logical Optimization（规则优化）
//   → MergeJoin + FilterPushDown + ColumnPruning + PredicatePushdown
//   → （比如 SELECT a, b FROM t WHERE c>10 → 把 WHERE c>10 下推到扫描阶段）
//     ↓ Physical Planning（成本优化）
//   → 生成多个物理计划（BroadcastJoin / ShuffleHashJoin / SortMergeJoin）
//   → CostModel 比较（哪个代价低选哪个）
//     ↓ Code Generation（代码生成）
//   → Whole-Stage CodeGen 把多个 Operator 拼成一个 Java 方法
//   → 编译成字节码（Java Compiler API）
//   → 类似手写的优化代码 → 比解释执行快很多
//
// ★ Whole-Stage CodeGen 核心：
//   传统：每个 Operator 一个迭代器 → filter → map → agg（多次函数调用）
//   CodeGen：把 filter.map.agg 拼成一个 while 循环
//   → for each row: if filter then map then agg
//   → 减少函数调用开销 + JIT 更容易优化
//
// ★ 和 Flink Optimizer 对比：
//   Spark Catalyst：规则 + 成本 + CodeGen
//   Flink：规则优化 + 执行计划
//   → Spark CodeGen 更激进（生成代码执行）
```

### 2.4 存储体系

```scala
// ★ Spark 存储等级（StorageLevel）：
//   NONE / DISK_ONLY / MEMORY_ONLY / MEMORY_AND_DISK / OFF_HEAP
//   MEMORY_ONLY_SER / MEMORY_AND_DISK_SER / DISK_ONLY_SER
//
// ★ Cache / Persist：
//   rdd.persist(StorageLevel.MEMORY_AND_DISK)
//   → 标记 RDD 要缓存
//   → 第一次 Action 执行完 → 把结果缓存到 Executor 内存
//   → 后续 Action 直接拿缓存（不重算）
//
// ★ 什么时候 cache ？
//   ① 同一个 RDD 被多次使用（count + collect + take）
//   ② 迭代计算（Spark ML：多轮训练同一个 DataFrame）
//   ③ 宽依赖（Shuffle 输出缓存起来，Task 失败不用重算前面的 Stage）
//
// ★ Shuffle 数据存储：
//   Map 端写磁盘 → Reduce 端拉
//   Spark 3.0+ 默认 Sort Shuffle（先排序再写磁盘）
//   → 避免 Reduce 端排序开销（已排好序了）
```

### 2.5 Spark Structured Streaming

```scala
// ★ Spark 结构化流（2.0+，流批统一）
//   用 DataFrame API 写流处理（和批处理一模一样！）
//
// 示例：
//   val lines = spark.readStream
//     .format("socket")
//     .option("host", "localhost")
//     .option("port", 9999)
//     .load()
//
//   val wordCounts = lines
//     .flatMap(_.split(" "))
//     .groupBy("value")
//     .count()
//
//   wordCounts.writeStream
//     .format("console")
//     .outputMode("complete")  // complete / append / update
//     .start()
//     .awaitTermination()
//
// ★ 和 Flink 对比（面试高频）：
//   Structured Streaming：微批（默认 1s 一次）→ 秒级延迟
//   Flink：真正流（每条事件处理）→ 毫秒级延迟
//   Spark：**和批处理 API 完全相同**（写一次跑批或流）
//   Flink：专门的流 API（DataStream API）
//
// ★ 流式计算三种语义：
//   ① At-Most-Once（至少一次）→ 不保证重跑
//   ② At-Least-Once（最多一次）→ 可能重复
//   ③ Exactly-Once（恰好一次）→ Spark Structured Streaming + Checkpoint
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RDD 五大特征？| **Partitions / Partitioning / Dependencies / Iterator / Preferred Location**；Spark 灵魂抽象 |
| 2 | Narrow vs Wide Dependency？| Narrow（1:1，map/filter/flatMap）→ 不 Shuffle，可以 Chain；Wide（N:1/1:N，groupByKey/join）→ **必须 Shuffle（写磁盘 + 网络）** |
| 3 | Stage 划分规则？| **每个 Shuffle Dependency 切割一个 Stage 边界**；Narrow 不切（Chain）；最后一个 Stage 是 Result Stage；前面是 Shuffle Map Stage |
| 4 | DAGScheduler 核心？| **把 RDD DAG 切成 Stage + 依赖调度**；发现上游完成 → 提交下游 Stage；最后 Stage 完成返回结果 |
| 5 | Catalyst Optimizer 四步？| **① Analysis（解析）② Logical Optimization（规则：FilterPushDown）③ Physical Planning（成本选计划）④ Code Generation（Whole-Stage CodeGen 生成代码）** |
| 6 | Whole-Stage CodeGen？| **把多个 Operator 拼成一个 Java 方法编译执行** → 减少函数调用 + JIT 更容易优化 → 比解释执行快很多 |
| 7 | Shuffle 实现？| Map 端按分区 Hash 写磁盘（Sort Shuffle 先排序）；Reduce 端并发拉取所有 Map 数据 → 聚合；默认 Sort Shuffle（避免 Reduce 排序）|
| 8 | Cache / Persist 什么时候用？| ① 同一个 RDD 多次使用 ② 迭代计算（ML）③ Shuffle 输出缓存（Task 失败不重算）|
| 9 | Spark vs Flink？| Spark：**批流统一 API（Structured Streaming 微批）+ 迭代计算 + Catalyst CodeGen**；Flink：真正流 + 毫秒延迟 + 完美状态管理 |
| 10 | Structured Streaming 三种语义？| At-Most-Once（快但可能丢）/ At-Least-Once（保证到达但可能重复）/ **Exactly-Once（Checkpoint + 幂等）** |
| 11 | AQE（自适应查询执行）？| Spark 3.0+ 默认开启 → 运行时根据真实数据调整执行计划（动态合并小分区 / 动态选择 Broadcast Join）|
| 12 | Spark Join 三种？| **BroadcastJoin（小表广播，O(1) 查找，最快）/ ShuffleHashJoin（Hash 分区）/ SortMergeJoin（排序合并）**；Catalyst 成本模型自动选 |
| 13 | Spark 和 Hadoop 关系？| Spark **替代 Hadoop MapReduce**（性能高 10-100 倍）；但 HDFS 是 Spark 的默认存储（Spark 自己不存数据）|
| 14 | Spark Executor 作用？| **Worker 上的 JVM 进程** → 执行 Task（一个 Executor 可以并行执行多个 Task）；存储 RDD 缓存（MemoryStore）|
| 15 | Spark Job / Stage / Task 关系？| Action 触发 Job；每个 Shuffle 切 Stage；每个 Partition 一个 Task；**DAG 切 Stage → Stage 并行执行 Task** |
| 16 | Spark MLlib？| **机器学习库**（Spark ML：DataFrame API；MLLib：旧 RDD API）；支持分类/回归/聚类/协同过滤 |
| 17 | Spark on K8s？| Spark 3.0+ 原生支持 → Driver 跑 Job Pod，Executor 跑动态 Pod；比 YARN 更灵活 |
| 18 | Spark 学习路径？| RDD API → DataFrame / SQL → Catalyst 原理 → AQE → Structured Streaming → MLlib |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Spark 3.5.x  
**面试考点：18 题**
