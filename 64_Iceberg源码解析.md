# Apache Iceberg 源码深度解析（数据湖表格式）

> **源码基线**：Apache Iceberg 1.7.x（最新稳定）  
> **核心仓库**：https://github.com/apache/iceberg  
> **面试权重**：⭐⭐⭐⭐⭐（数据湖最高频，Spark/Flink/Presto 都支持）

---

## 一、Iceberg 整体定位

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：数据湖（Data Lake）的表格式标准                   │
│  "让数据湖里的数据像数据库表一样好用"                        │
│                                                               │
│  解决什么问题？                                              │
│  传统数仓：Hive Table → 目录级分区 → 元数据在 Metastore      │
│  问题：① Schema 变更难（Hive ALTER TABLE 锁）② 分区裁剪粗    │
│  ③ 并发写入冲突（锁竞争）④ 快照管理差（只能定时快照）        │
│                                                               │
│  Iceberg：**独立的表格式**（不是存储引擎，是数据组织格式）    │
│  → 数据还是存 HDFS/S3/GCS（Parquet/ORC/Avro）               │
│  → Iceberg 负责组织数据 + 元数据管理                         │
│  → Spark / Flink / Presto / Trino **原生支持**               │
│                                                               │
│ ★ Iceberg = 数据湖时代的 Hive（新一代数仓标准）              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Iceberg vs Hive vs Delta Lake vs Paimon 对比               │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │  Hive Table    │ Iceberg      │ Delta Lake  │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 分区粒度   │ 目录级         │ **文件级**   │ 文件级       │   │
│  │ Schema 演进 │ ❌ 锁表（重写）│ ✅ 原子变更   │ ✅ 原子变更   │   │
│  │ 并发写入   │ ❌ 容易冲突     │ ✅ 乐观锁    │ ✅ 乐观锁    │   │
│  │ 快照       │ 手动/定时       │ **Snapshot 原子**│ Snapshot │   │
│  │ 增量读写   │ ❌ 全量         │ ✅ 增量     │ ✅ 增量     │   │
│  │ 社区       │ Apache 老牌    │ Apache       │ Databricks  │   │
│  │ Flink 支持 │ ❌ 不支持       │ ✅ 好        │ ❌ 差        │   │
│  │ Spark 支持 │ ✅ 原生        │ ✅ 原生      │ ✅ 原生      │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ Paimon（阿里出品）：比 Iceberg 更适合 Flink 实时场景       │
│ ★ Iceberg：Spark / Trino / Presto 支持最好                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Iceberg 元数据三层结构（★★★★★ 灵魂）

```
// ★ Iceberg 最核心的设计：三层元数据（面试必问！）
//
//   ┌─────────────────────────────────────────────┐
//   │ Layer 1: Snapshot（快照）                     │
//   │   每个写入生成一个原子快照（类似 Git Commit）  │
//   │   snapshot → manifest list 位置               │
//   └─────────────────────────────────────────────┘
//                       ↓ 指向
//   ┌─────────────────────────────────────────────┐
//   │ Layer 2: Manifest List（清单列表）             │
//   │   存这个快照包含哪些 Manifest 文件            │
//   │   → 每个 Manifest 文件 = 一个分区的数据文件列表│
//   └─────────────────────────────────────────────┘
//                       ↓ 指向
//   ┌─────────────────────────────────────────────┐
//   │ Layer 3: Manifest + Data Files（数据文件）    │
//   │   Manifest 存每个 Data File 位置/大小/行数/统计│
//   │   Data Files: Parquet/ORC/Avro 列式文件      │
//   └─────────────────────────────────────────────┘
//
// ★ 三层结构好处：
//   ① 快照是原子的（写 Manifest → 写 ManifestList → 写 Snapshot）
//   ② 查询只读需要的文件（Manifest 存了文件级分区/统计）
//   ③ 分区裁剪到文件级（不是目录级）
//   ④ Schema 变更只改元数据（不改数据文件）
//
// ★ 和 Hive 对比：
//   Hive：Table → Partition（目录）→ Data Files（散落在目录里）
//   Iceberg：Snapshot → Manifest → Manifest → Data Files（有组织）
```

### 2.2 原子写入流程（★ 面试高频）

```java
// ★ Iceberg 原子写入流程（乐观锁，和数据库 MVCC 类似）
//
// INSERT INTO iceberg_table VALUES (...);
//
// ① 开始 Transaction
//    读当前 Snapshot（从 HDFS/S3 读最新的 manifest list）
//    → 这是 base snapshot
//
// ② 写数据文件（Append-Only）
//    生成新的 Data Files（Parquet/ORC）
//    → 写到 HDFS/S3
//
// ③ 生成 Manifest
//    新数据文件的 Manifest（位置/大小/行数/分区）
//    → 写到 HDFS/S3
//
// ④ 生成 Manifest List
//    把新 Manifest 和已有 Manifest 合并
//    → 写到 HDFS/S3
//
// ⑤ **原子提交（乐观锁）**
//    比较 base snapshot 和当前最新 snapshot
//    → 如果一样（没被其他人改过）→ 写新 Snapshot → 成功 ✅
//    → 如果不一样（被其他人改过）→ 重试（从步骤 1 开始）
//    → 这就是 **乐观锁并发控制**！
//
// ★ 为什么是乐观锁？
//   数据湖写入冲突概率低（大数据量 + 写入不频繁）
//   → 乐观锁 + 重试 比悲观锁简单高效
//   → 和 TiDB Percolator 2PC / MySQL MVCC 思想一样
```

### 2.3 Partition 规范（Spec vs Layout）

```java
// ★ Iceberg Partition 规范（比 Hive 灵活！）
//
// CREATE TABLE iceberg_orders (
//     order_id BIGINT,
//     user_id BIGINT,
//     amount DECIMAL(10,2),
//     create_time TIMESTAMP
// )
// PARTITIONED BY (
//     days(create_time)           -- 按天分区
// )
// WITH (
//     'write.format.default' = 'parquet',
//     'write.distribution-mode' = 'hash(user_id)'  -- Hash 分布
// );
//
// ★ 两种 Partition 方式：
//   ① Spec（静态分区）：和 Hive 一样，按 create_time 分天
//   ② Layout（动态分布）：Hash(user_id) 分布数据 → 避免热点分区
//
// ★ 分区裁剪到文件级：
//   Hive：WHERE create_time >= '2024-01-01' → 只扫目录 dt=20240101
//   Iceberg：WHERE create_time >= '2024-01-01' → Manifest 里记录了每个文件的分区
//     → 只读包含符合条件数据的文件（更细粒度！）
//     → 比 Hive 目录级裁剪更高效
```

### 2.4 Schema 演进

```java
// ★ Iceberg Schema 演进（比 Hive 强太多！）
//
// Hive Schema 变更：
//   ALTER TABLE ADD COLUMN new_col INT;
//   → 锁表！所有读请求被阻塞！
//   → 要重写整个表！
//
// Iceberg Schema 变更：
//   ALTER TABLE iceberg_table ADD COLUMN new_col INT;
//   → 只改元数据（Snapshot 里记录新 Schema）
//   → 旧数据文件没有 new_col → 查询时自动用 NULL 填充
//   → 不锁表！不重写数据！
//   → 几毫秒完成！
//
// ★ Iceberg Schema 变更类型：
//   ADD COLUMN       → 加列（最常用，O(1)）
//   RENAME COLUMN    → 重命名
//   DROP COLUMN      → 删列（标记删除，物理文件不删）
//   UPDATE COLUMN    → 改列类型（要求兼容：int→bigint 可以）
//
// ★ 和数据库 Schema 变更对比：
//   MySQL：DDL 锁表 + 重写（大表可能几小时）
//   Iceberg：DDL 只改元数据（几毫秒）
//   → 这就是数据湖和 OLTP 数据库的核心差异！
```

### 2.5 Time Travel + 回滚

```java
// ★ Iceberg Time Travel（时光旅行）——和 Git checkout 一样！
//
// 查询历史快照：
//   SELECT * FROM iceberg_table
//   FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00';
//   → 回到这个时间点的数据
//
// 列出所有快照：
//   SELECT * FROM iceberg_table.snapshots;
//   → 每个 Snapshot 有 snapshot_id + timestamp
//
// 回滚到某个快照：
//   CALL spark_catalog.system.rollback_to_snapshot(
//     'default', 'iceberg_table', 123456
//   );
//   → 回滚到 snapshot 123456
//
// ★ 和 Git 对比：
//   Iceberg Snapshot ≈ Git Commit
//   Iceberg Time Travel ≈ Git checkout
//   Iceberg Rollback ≈ Git reset
//
// ★ 为什么能 Time Travel？
//   每个 Snapshot 指向当时的 Manifest List
//   → 只要 Snapshot 在 → 所有数据文件还在
//   → Iceberg 的 Expire Snapshots 才会物理删除旧数据文件
//   → 默认保留 7 天快照
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Iceberg 核心定位？| **数据湖表格式标准**（不是存储引擎）→ 让 HDFS/S3 上的数据像数据库表一样好用；Spark/Flink/Presto 原生支持 |
| 2 | 三层元数据？| **Snapshot（原子快照）→ Manifest List（清单列表）→ Manifest + Data Files（数据文件）**；每层都有明确职责 |
| 3 | 原子写入怎么实现？| **乐观锁**：① 读 base snapshot ② 写 Data Files → Manifest → Manifest List ③ 比较 base vs 当前 ④ 一样则提交，不一样则重试 |
| 4 | Iceberg vs Hive？| Hive：**目录级分区 + 锁表 DDL + 无快照**；Iceberg：**文件级分区裁剪 + 元数据变更 + Snapshot 时光旅行** |
| 5 | Iceberg vs Delta Lake vs Paimon？| Iceberg：**Spark/Trino 支持最好**；Delta Lake：Databricks 出品；**Paimon（阿里）更适合 Flink 实时场景** |
| 6 | Schema 演进？| **只改元数据**（Snapshot 里记录新 Schema）→ 旧数据文件无新列 → 查询时 NULL 填充 → 不锁表！几毫秒完成 |
| 7 | Time Travel？| **FOR SYSTEM_TIME AS OF TIMESTAMP '...'** → 回到历史快照；类似 Git checkout；默认保留 7 天快照 |
| 8 | Partition 裁剪粒度？| **文件级**（Manifest 里记录每个文件分区）→ 比 Hive 目录级更细 → 查询更快 |
| 9 | Iceberg 数据存储？| **HDFS / S3 / GCS / Azure Blob**（列式 Parquet/ORC/Avro）；Iceberg 是表格式，不是存储引擎 |
| 10 | 乐观锁 vs 悲观锁？| Iceberg：**乐观锁**（数据湖冲突概率低）；TiDB Percolator：悲观锁（事务冲突概率高）；选择依据：冲突概率 |
| 11 | Iceberg 学习路径？| Spark Iceberg → 写表 → 原子写入 → Time Travel → Schema 演进 → Manifest 结构 |
| 12 | Iceberg 快照 Expire？| 后台线程清理过期快照 + 删除对应数据文件；默认保留 7 天；手动：CALL expire_snapshots() |
| 13 | Iceberg Merge On Read vs Copy On Write？| COW：写入时合并旧文件（读快写慢）；MOR：写入不合并（写快读时合并）→ Flink 流处理用 MOR |
| 14 | Iceberg vs Kafka？| Kafka：消息队列；Iceberg：表格式；Kafka + Iceberg = Kafka 数据 → Iceberg 表（流入湖）|
| 15 | Iceberg Catalog？| Hive Metastore / REST Catalog / JDBC Catalog；默认 Hive Metastore；REST Catalog 是无状态 HTTP 接口（推荐云原生）|
| 16 | Iceberg DDL 类型？| ADD COLUMN（O(1)）/ DROP COLUMN（标记删除）/ RENAME / UPDATE（兼容类型）；**不支持 PRIMARY KEY/FOREIGN KEY**（数据湖无约束）|
| 17 | Iceberg 并发写入？| **乐观锁自动处理**；冲突时重试；写入冲突概率低（大数据量场景）→ 性能好 |
| 18 | Iceberg 为什么重要？| 数据湖时代的 **Hive 替代者**；Spark/Flink/Presto 三大引擎都原生支持；大规模数仓的新标准 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Iceberg 1.7.x  
**面试考点：18 题**
