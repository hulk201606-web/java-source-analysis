# ClickHouse 源码深度解析（OLAP 列式分析数据库）

> **源码基线**：ClickHouse 24.x（最新稳定）  
> **核心仓库**：https://github.com/ClickHouse/ClickHouse  
> **面试权重**：⭐⭐⭐⭐（OLAP 分析高频，替代 ES/Kylin/Doris）

---

## 一、ClickHouse 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：列式存储 OLAP 数据库（分析型查询极速）             │
│  解决什么问题？PB 级数据实时分析（比 MySQL ES Kylin 都快）    │
│                                                               │
│  核心优势（为什么快？）：                                      │
│  ① 列式存储 → 只读需要的列（SELECT * FROM t 不是问题！）     │
│  ② 向量化执行 → SIMD CPU 指令一次处理一列                    │
│  ③ MergeTree 引擎 → 分区 + 稀疏索引 + 后台合并               │
│  ④ 压缩极高 → LZ4/ZSTD → 磁盘 IO 少                         │
│  ⑤ 分布式查询 → 多节点并行                                   │
│                                                               │
│  对比其他 OLAP：                                              │
│  ┌──────────┬────────────┬────────────┬────────────┐       │
│  │            │ ClickHouse  │ Doris       │ Elasticsearch│       │
│  ├──────────┼────────────┼────────────┼────────────┤       │
│  │ 存储模型    │ MergeTree │ Doris    │ Lucene 倒排 │       │
│  │ 列式存储    │ ✅        │ ✅        │ 半列式      │       │
│  │ 向量化     │ ✅★★★★  │ ✅        │ ❌          │       │
│  │ 聚合性能    │ ★★★★★   │ ★★★★     │ ★★          │       │
│  │ 全文搜索    │ ❌        │ ❌        │ ★★★★★      │       │
│  │ 实时写入    │ ★★★★    │ ★★★★     │ ★★★★★      │       │
│  │ 分区裁剪    │ ✅        │ ✅        │ ❌（靠 routing）│       │
│  └──────────┴────────────┴────────────┴────────────┘       │
│                                                               │
│ ★ 生产场景：日志分析 / 用户行为分析 / 实时报表 / AB 实验      │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 MergeTree 引擎（★★★★★ 核心）

```cpp
// ★ MergeTree 是 ClickHouse 的灵魂引擎
//   类似 InnoDB 是 MySQL 的灵魂
//
// MergeTree 核心设计：
//
// ① PARTITION BY（分区，面试高频）
//   数据按条件分区分开存 → 不同分区不混存
//   → 查询时分区裁剪（跳过不相关分区）→ 大幅减少 IO
//   → 典型：PARTITION BY toYYYYMM(create_time) → 按月分区
//
// ② ORDER BY（主键排序，★ 不是传统主键！）
//   ORDER BY (user_id, create_time)
//   → 物理上数据按 ORDER BY 列有序存储
//   → 稀疏索引基于 ORDER BY 构建
//   → ★ ClickHouse 的"主键"不是唯一约束！（可以重复）
//
// ③ 稀疏索引（★ ClickHouse 独有！）
//   每个数据块（默认 64 行）只存一个索引标记
//   → 块内 64 行有序 → 索引标记是块首行的 ORDER BY 值
//   → 查询时先查稀疏索引 → 跳过不需要的数据块
//   → 比 B+Tree 索引小很多（数据量的 1/1000）
//
// ④ Merge（后台合并，类似 ES Merge）
//   ClickHouse 支持高频写入 → 会产生大量小 Part
//   → 后台 Merge 线程把小 Part 合并成大 Part
//   → 类似 ES Segment Merge / Kafka Log Compaction
//   → Merge 占 IO + CPU（大表可能 Merge 很久）
//
// ⑤ 分区不跨 Part 合并
//   PARTITION BY 区分的分区 → 独立存储 → 合并只在分区内
//   → 不同分区的数据永不混在一起
//   → 这就是分区裁剪能高效的原因！
```

### 2.2 MergeTree 引擎家族

```cpp
// ★ MergeTree 引擎家族（面试高频！每种适用不同场景）
//
// 基础：
//   MergeTree           → 基础 OLAP 引擎（生产最常用）
//
// 带版本/去重：
//   ReplacingMergeTree  → 后台合并时去重（同一个 ORDER BY key 保留最新版本）
//   VersionedCollapsingMergeTree → 带版本号的去重
//   CollapsingMergeTree → Sign 列（1/-1）做去重/抵消
//
// 带聚合：
//   SummingMergeTree    → 后台合并时自动 SUM 数值列（预聚合）
//   AggregatingMergeTree→ 后台合并时做任意聚合（ArgMax / State）
//   GraphiteMergeTree   → Graphite 监控数据专用
//
// 复制（分布式场景）：
//   ReplicatedMergeTree → ZooKeeper 同步多副本
//   ReplicatedReplacingMergeTree / ReplicatedAggregatingMergeTree ...
//
// ★ ReplacingMergeTree 面试高频场景：
//   每天有 1 亿条用户行为日志 → 同一个 user_id 可能被写多次
//   → ReplacingMergeTree 后台合并时自动保留最新一条
//   → 省掉业务层去重逻辑
//
// ★ SummingMergeTree 面试高频场景：
//   每天实时统计每个城市的订单金额
//   → 每条明细写入 → 后台合并时自动 SUM
//   → 查 SUM(order_amount) 直接从预聚合结果取 → 极快
```

### 2.3 向量化执行（★★★★★ 为什么快）

```cpp
// ★ ClickHouse 最大的性能优势：向量化执行（SIMD）
//
// CPU SIMD 指令集：一次处理 4/8/16 个数据
//   SSE（128bit）→ 4 个 int / 2 个 double
//   AVX（256bit）→ 8 个 int / 4 个 double
//   AVX-512（512bit）→ 16 个 int / 8 个 double
//
// 传统 OLTP 数据库（MySQL）：行式处理
//   SELECT COUNT(*) FROM t WHERE age > 18;
//   → 一行一行判断 age > 18
//
// ClickHouse：向量化处理
//   Block（64K 行的一批）
//   → age 列数据一次进 SIMD 寄存器
//   → 一次比较 16 个 age 值（AVX-512）
//   → 性能提升 10-30 倍！
//
// ★ ClickHouse 向量化核心：
//   Block = Columns（列式存储的一组列）
//   → 每个 Column 是一个 Array（Data 列 + NullBitmap 列）
//   → Operator 对整个 Column 做一次 SIMD 操作
//   → 比 MySQL 行式循环快一个数量级
//
// ★ Spark Catalyst + Tungsten 也有向量化（1.4+）
//   但 ClickHouse 是原生列式 + 向量化 → 更彻底
```

### 2.4 分布式查询

```
// ★ ClickHouse 分布式架构（比 MySQL ShardingSphere 简单）
//
// Coordinator（协调节点）+ N Data Node
//
// 分布式表：
//   CREATE TABLE t_distributed
//     AS t_local ON CLUSTER cluster
//     SHARD BY hash(user_id) INTO 4 SHARDS
//     REPLICAS 2;
//
// 查询流程：
//   Client → Coordinator
//     → 分析分片条件（hash(user_id)）
//     → 如果能精确分片（WHERE user_id = ?）→ 只查对应 Data Node
//     → 如果不能精确分片 → 并发查所有 Data Node
//     → Data Node 并行返回 Part 数据
//     → Coordinator 合并 + 聚合 + 排序 + 返回
//
// ★ 和 MySQL + ShardingSphere 对比：
//   ClickHouse：分布式是核心能力 → 原生支持 → 简单高效
//   MySQL + ShardingSphere：中间件层 → SQL 改写 + 路由 → 复杂但兼容 MySQL
```

### 2.5 物化视图

```sql
// ★ ClickHouse 物化视图（面试高频，预聚合加速查询）
//
// 场景：每天要实时查"每个城市的订单总额"
// 直接查明细表太慢 → 建物化视图实时聚合
//
// CREATE MATERIALIZED VIEW order_summary
// TO order_summary_table
// AS SELECT
//     toYYYYMM(create_time) as month,
//     city,
//     SUM(amount) as total_amount,
//     COUNT() as order_count
// FROM order_table
// GROUP BY month, city;
//
// 写入流程：
//   Client 写 order_table
//   → ClickHouse 自动触发物化视图
//   → 把新数据聚合后写 order_summary_table
//   → 实时更新（不是定时批量！）
//
// ★ 查物化视图 vs 查原表：
//   查物化视图：直接拿预聚合结果 → 极快
//   查原表：实时聚合所有明细 → 慢
//
// ★ 物化视图和 ReplacingMergeTree 对比：
//   ReplacingMergeTree：后台合并时去重（不改变原始查询）
//   Materialized View：实时维护汇总表（专门查汇总）
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ClickHouse 为什么快？| **① 列式存储（只读需要的列）② 向量化执行（SIMD 指令一次处理一列）③ MergeTree 分区+稀疏索引 ④ 压缩极高（LZ4/ZSTD）⑤ 分布式并行** |
| 2 | MergeTree 引擎核心？| **PARTITION BY（分区，裁剪 IO）+ ORDER BY（物理排序 + 稀疏索引，★ 不是传统主键！可以重复）+ Merge（后台合并小 Part）** |
| 3 | 稀疏索引（ClickHouse 独有）？| 每个数据块（默认 64 行）只存一个索引标记（块首行的 ORDER BY 值）→ 比 B+Tree 小 1000 倍 → 查询时先查索引跳过不需要的数据块 |
| 4 | MergeTree 家族？| **MergeTree（基础）/ ReplacingMergeTree（后台去重）/ SummingMergeTree（后台 SUM 聚合）/ AggregatingMergeTree（任意聚合）/ Replicated*（分布式复制）**|
| 5 | Partition 裁剪？| PARTITION BY 按月/天分分区 → 查询 WHERE create_time >= '2024-01-01' → 跳过不相关分区 → 大幅减少 IO |
| 6 | ClickHouse 主键不是唯一约束？| **是的！** ORDER BY 列是物理排序键（叫"主键"）→ 可以重复 → ClickHouse 不做唯一校验 |
| 7 | 向量化执行（SIMD）？| **CPU SIMD 指令（AVX-512 一次处理 16 个 int）**→ ClickHouse Block（64K 行一批）→ 对整列做一次操作 → 比 MySQL 行式循环快 10-30x |
| 8 | ReplacingMergeTree 场景？| **需要去重的场景**（同一个 user_id 可能被写多次）→ 后台合并时自动保留最新版本 |
| 9 | SummingMergeTree 场景？| **预聚合**（每个城市的订单金额）→ 后台合并时自动 SUM 数值列 → 查 SUM() 直接拿结果 |
| 10 | ClickHouse vs ES？| ClickHouse：**OLAP 聚合 + 向量化（COUNT/SUM/GROUP BY 快）**；ES：**全文搜索 + 倒排索引 + BM25 算分**；生产常组合（ES 搜索 + ClickHouse 分析）|
| 11 | ClickHouse 分布式？| **Coordinator + N Data Node**→ 精确分片只查对应节点；不精确分片并发查所有；比 MySQL ShardingSphere 简单 |
| 12 | 物化视图？| **实时预聚合**（写原表 → 自动触发物化视图更新汇总表）→ 查汇总极快；不是定时批量，是实时 |
| 13 | ClickHouse 和 MySQL 关系？| ClickHouse **不替代 MySQL**（没有事务 / Join 弱）；场景互补：MySQL 存业务数据，ClickHouse 存明细做分析 |
| 14 | ClickHouse Join 性能？| ClickHouse Join **比 MySQL 差**（分布式 Join 要传数据）；尽量用物化视图 / GROUP BY 替代复杂 Join |
| 15 | Merge 对查询的影响？| Merge 占 IO + CPU → 大表可能 Merge 很慢；后台线程合并小 Part → 合并期间查询可能变慢（建议低峰期导入数据）|
| 16 | ClickHouse 写入性能？| **批量写入极快**（每秒 100w+）；单条写入也能接受但推荐批量；支持 Kafka 实时导入 |
| 17 | ClickHouse vs Doris？| 都是 OLAP；ClickHouse：**单列存储 + MergeTree + 向量化**；Doris：**两阶段聚合 + 物化视图管理**；国内 Doris 也流行 |
| 18 | ClickHouse 学习路径？| SQL 基础 → PARTITION / ORDER BY / MergeTree 家族 → 向量化原理 → 分布式架构 → 物化视图 + Kafka 导入 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：ClickHouse 24.x  
**面试考点：18 题**
