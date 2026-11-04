# Apache Doris 源码深度解析（OLAP 分析数据库）

> **源码基线**：Apache Doris 2.1.x（最新稳定）  
> **核心仓库**：https://github.com/apache/doris  
> **面试权重**：⭐⭐⭐⭐（OLAP 国产高频，百度出品）

---

## 一、Doris 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：MPP 分析型数据库（Massively Parallel Processing） │
│  解决什么问题？PB 级数据秒级分析                              │
│                                                               │
│  和 ClickHouse / Presto / StarRocks 对比：                   │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Apache Doris  │ ClickHouse    │ StarRocks    │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 出处       │ 百度 (Apache) │ ClickHouse    │ 字节/阿里云   │   │
│  │ 架构       │ FE + BE     │ 单体节点集群  │ FE + BE       │   │
│  │ 列式存储   │ ✅           │ ✅            │ ✅           │   │
│  │ MPP 并行   │ ✅★★★★    │ ✅★★          │ ✅★★★★    │   │
│  │ 实时写入   │ ✅ Stream Load│ ✅ Kafka Engine│ ✅           │   │
│  │ 物化视图   │ ✅★★★★ 自动  │ ✅ 手动       │ ✅★★★★ 自动  │   │
│  │ 广播 Join   │ ✅★★★★ 强  │ ❌ 不支持     │ ✅★★★★ 强  │   │
│  │ 国内流行度  │ ★★★★★ 极高 │ ★★★★ 高     │ ★★★ 中      │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ Doris / StarRocks 是国内 OLAP 市场的主流选择（抖音/百度/美团）│
│ ★ ClickHouse：单节点极致性能；Doris/StarRocks：分布式 MPP 强 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Doris 架构组件                                              │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   FE (Frontend) │ BE (Backend)   │ BROKER        │     │
│  │ 元数据/调度   │ 数据存储/计算  │ 数据导入      │     │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         ↓              ↓              ↓                       │
│     MySQL 协议     MPP 并行执行     Stream Load / Broker Load  │
│     gRPC 元数据    LSM-Tree + 向量化│ (Kafka / HDFS)        │
│                                                               │
│ ★ FE：类似 TiDB Server + PD（元数据 + 查询调度）              │
│ ★ BE：类似 TiKV（存储 + 向量化计算）                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 FE（Frontend）

```java
// ★ FE 是 Doris 的大脑（Java 实现）
//
// 三大职责：
// ① 元数据管理（Catalog）
//    存 DB / Table / Partition / Tablet / Replica 信息
//    → 类似 TiDB Metastore + PD
//    → 元数据存在内存 + EditableLog（类似 MySQL Binlog）
//    → FE 主从同步（主 FE 写 + 从 FE 同步）
//
// ② SQL 解析 + 优化
//    Parser（yacc）→ AST → Analyzer → Optimizer（CBO）→ Physical Plan
//    → 类似 Spark Catalyst
//    → 优化：谓词下推 / 列裁剪 / **BROADCAST JOIN / 分区分片优化**
//
// ③ 查询调度 + DOP（Degree of Parallelism）
//    MPP 并行计划 → 分发到多个 BE 执行
//    DOP = 并行度（默认 BE 核数）
//
// ★ FE HA：
//   部署奇数个 FE（3/5）→ 选主（类似 Raft）
//   → 主 FE 挂了 → 从 FE 自动选主（恢复秒级）
```

### 2.2 BE（Backend，★ 数据执行核心）

```cpp
// ★ BE 是 Doris 的数据节点（C++ 实现，类似 TiKV）
//
// BE 核心组件：
//
// ① Storage Engine（LSM-Tree + 向量化）
//    → 类似 TiKV RocksDB
//    → 列存 + 压缩（LZ4/ZSTD）
//    → Compaction 后台合并
//
// ② Vectorized Execution（★ 向量化执行，Doris 2.0+ 默认）
//    → SIMD 指令（和 ClickHouse 一样）
//    → 一个 Block 64K 行一次处理
//    → 比行式执行快 5-10x
//
// ③ MPP Execution（MPP 并行查询）
//    → FE 生成分布式计划
//    → BE 之间通过 Exchange Node 交换数据
//    → 类似 Spark Shuffle / Flink KeyBy
//
// ★ Doris vs ClickHouse 向量化：
//   ClickHouse：SIMD + 列式 → 单节点极致
//   Doris：SIMD + 列式 + MPP 分布式 → 多节点横向扩展
//   → Doris 更适合大规模分布式 OLAP
```

### 2.3 数据模型（★ 核心差异）

```sql
-- ★ Doris 数据模型（面试高频！和 MySQL/ClickHouse 有何不同）
--
-- 四种 Table Model：
--
-- ① DUPLICATE KEY（明细模型，默认）
--    CREATE TABLE order_duplicate (
--        order_id BIGINT,
--        user_id BIGINT,
--        amount DECIMAL(10,2),
--        create_time DATETIME
--    ) DUPLICATE KEY(order_id, user_id)
--    DISTRIBUTED BY HASH(user_id) BUCKETS 10
--    PROPERTIES ("replication_num" = "3");
--
--    → 明细行保留所有重复（和 ClickHouse MergeTree 一样）
--    → 适合日志/行为明细场景
--
-- ② AGGREGATE KEY（聚合模型）
--    CREATE TABLE order_aggregate (
--        user_id BIGINT,
--        total_amount DECIMAL(10,2) SUM,
--        order_count BIGINT COUNT
--    ) AGGREGATE KEY(user_id)
--    DISTRIBUTED BY HASH(user_id) BUCKETS 10;
--
--    → 后台写入时自动聚合（SUM/COUNT/MAX/MIN）
--    → 类似 ClickHouse SummingMergeTree
--    → 查询直接拿聚合结果（极快！）
--
-- ③ UNIQUE KEY（唯一键模型）
--    CREATE TABLE user_unique (
--        user_id BIGINT,
--        name VARCHAR(100),
--        age INT
--    ) UNIQUE KEY(user_id)
--    DISTRIBUTED BY HASH(user_id) BUCKETS 10;
--
--    → 主键覆盖写（相同 user_id 新数据覆盖旧数据）
--    → 类似 MySQL PRIMARY KEY / ClickHouse ReplacingMergeTree
--    → Doris **实现了真正的 Unique Key 约束**（后台 Compaction 时去重）
--
-- ④ PRIMARY KEY（主键模型，★ Doris 2.0+）
--    CREATE TABLE user_pk (
--        user_id BIGINT,
--        name VARCHAR(100),
--        age INT
--    ) PRIMARY KEY(user_id)
--    DISTRIBUTED BY HASH(user_id) BUCKETS 10;
--
--    → 和 UNIQUE KEY 类似，但 Doris 2.0+ 实现了**主键索引**（类 MySQL B+Tree）
--    → 点查（WHERE user_id = ?）极快！
--
-- ★ Doris vs ClickHouse 数据模型：
--   Doris：四种模型（Duplicate/Aggregate/Unique/PK）
--   ClickHouse：MergeTree 家族（MergeTree/ReplacingMergeTree/SummingMergeTree）
--   → Doris 模型更清晰，UNIQUE KEY 真正唯一（不是后台去重）
```

### 2.4 Partition + Distribution

```sql
-- ★ Doris 分区 + 分桶（和 TiDB/ClickHouse 类似但更灵活）
--
-- Partition（分区）：
--   PARTITION BY RANGE(create_time)
--   (PARTITION p202401 VALUES [("2024-01-01"), ("2024-02-01")),
--    PARTITION p202402 VALUES [("2024-02-01"), ("2024-03-01")))
--
--   → 分区裁剪（WHERE create_time >= '2024-01-01' 只扫 p202401+）
--   → 自动按月分区：PARTITION BY RANGE(create_time) EVERY (INTERVAL 1 month)
--
-- Distribution（分桶）：
--   DISTRIBUTED BY HASH(user_id) BUCKETS 10
--   → 10 个分桶，每个分桶的数据哈希 user_id 分布
--   → 分桶数 = BE 节点数 × CPU 核数（经验公式）
--
-- ★ Doris vs MySQL 分区 vs ClickHouse 分区：
--   MySQL：PARTITION BY RANGE/KEY/LIST/HASH → 单节点分区
--   ClickHouse：PARTITION BY → 分区裁剪 + 分布式 ReplicatedMergeTree
--   Doris：PARTITION BY + DISTRIBUTED BY → 分区裁剪 + 分桶 → 完美配合 MPP
```

### 2.5 Stream Load + Broker Load（数据导入）

```sql
-- ★ Doris 三种数据导入方式（面试高频！）
--
-- ① Stream Load（实时导入，HTTP PUT 接口，★ Doris 特色）
--    curl -X PUT \
--      -u root: \
--      -H "label: order_stream_load_001" \
--      -H "column_separator: ," \
--      -T order.csv \
--      http://doris-fe:8030/api/my_db/order_duplicate/_stream_load
--
--    → HTTP PUT 上传数据 → Doris 实时写入（不落地 WAL）
--    → 延迟毫秒级（Kafka Engine 数据导入核心）
--
-- ② Broker Load（批量异步导入，从 HDFS/S3/Kafka）
--    LOAD LABEL order_broker_load_001
--    DATA INFILE "hdfs://hdfs:9000/data/order/*.csv"
--    INTO TABLE order_duplicate
--    COLUMNS TERMINATED BY ",";
--
--    → 异步提交 → Doris Broker 读 HDFS → BE 并行导入
--    → 适合大文件批量导入
--
-- ③ Routine Load（Kafka 实时消费）
--    CREATE ROUTINE LOAD order_routine_load ON order_duplicate
--    COLUMNS TERMINATED BY ","
--    FROM KAFKA (
--        "kafka_broker_list" = "kafka:9092",
--        "kafka_topic" = "order_topic",
--        "property.group.id" = "doris_group"
--    );
--
--    → 类似 Kafka Connect Source（但 Doris 原生集成）
--    → 自动管理 Consumer Group + Offset
--
-- ★ Doris vs ClickHouse 导入：
--   Doris：**Stream Load（HTTP PUT）+ Broker Load + Routine Load（Kafka）**
--   ClickHouse：SQL INSERT + Kafka Engine + S3 函数
--   → Doris 导入方式更多样（特别是 Stream Load 实时性好）
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Doris 核心定位？| **MPP 分布式分析数据库**（百度开源 Apache 顶级项目）→ PB 级数据秒级查询 |
| 2 | Doris 架构？| **FE（Java，元数据+SQL 解析+调度）+ BE（C++，存储+向量化执行）+ Broker（数据导入）** → FE HA（奇数个 FE 选主）|
| 3 | Doris 四种数据模型？| **DUPLICATE KEY（明细）/ AGGREGATE KEY（自动聚合）/ UNIQUE KEY（覆盖写）/ PRIMARY KEY（主键索引，2.0+ 点查极快）**；Doris 比 ClickHouse 模型清晰 |
| 4 | Doris vs ClickHouse？| ClickHouse：**单节点极致（SIMD + 单节点）**；Doris：**MPP 分布式（多节点并行 + 广播 Join + 自动物化视图）** |
| 5 | Vectorized Execution？| **Doris 2.0+ 默认开启** → SIMD 指令一次处理 64K 行 Block → 比行式快 5-10x（和 ClickHouse 类似）|
| 6 | Stream Load 是什么？| **Doris 特色实时导入** → HTTP PUT 接口上传数据 → 延迟毫秒级；类似 ClickHouse Kafka Engine 但更通用 |
| 7 | MPP 并行执行？| FE 生成分布式计划 → BE 之间 **Exchange Node 交换数据** → 类似 Spark Shuffle；DOP = BE 核数（并行度）|
| 8 | Doris Partition + Distribution？| **PARTITION BY RANGE（分区裁剪）+ DISTRIBUTED BY HASH（分桶）** → 完美配合 MPP；分桶数 = BE 数 × CPU 核数 |
| 9 | Doris 物化视图？| **自动增量更新（核心优势！）** → 写 MV 后底表变化自动更新；类似 Materialize |
| 10 | Doris BROADCAST JOIN？| ★ 核心优势！小表广播到所有 BE → 避免 Shuffle → Join 极快；ClickHouse **不支持广播 Join**（必须 Shuffle）|
| 11 | StarRocks vs Doris？| 都是 Apache Doris 分支 → StarRocks 字节/阿里云维护（更激进，先加新特性）；Doris 社区版（稳定）|
| 12 | Doris 导入三方式？| **Stream Load（HTTP/实时）/ Broker Load（HDFS/S3 批量）/ Routine Load（Kafka 消费）**；Doris 导入最灵活 |
| 13 | Doris FE HA？| 奇数个 FE（3/5）→ 选主；主 FE 写元数据 + EditableLog → 从 FE 同步；挂了自动选主 |
| 14 | Doris BE 故障恢复？| BE 挂了 → 副本自动从其他 BE 补（类似 TiKV Raft）；3 副本默认 |
| 15 | Doris vs Spark SQL？| Doris：**OLTP + OLAP 混合 + 点查强**；Spark SQL：**纯批流 ETL + 大规模并行**；常组合用（ETL 用 Spark，实时分析用 Doris）|
| 16 | Doris 适用场景？| **实时仪表盘 / 用户行为分析 / 广告投放分析 / 日志分析**；国内大厂（抖音/百度/美团）|
| 17 | Doris 学习路径？| Docker 起 FE + BE → 建表（四种模型都试）→ Stream Load 导入 → BROADCAST JOIN → 物化视图 |
| 18 | Doris PRIMARY KEY vs UNIQUE KEY？| 都是覆盖写；**PRIMARY KEY（2.0+）有主键索引**（类 MySQL B+Tree）→ 点查 WHERE pk = ? 极快；UNIQUE KEY 无索引 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Doris 2.1.x  
**面试考点：18 题**
