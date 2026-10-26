# Apache Paimon 源码深度解析（Flink 实时数据湖）

> **源码基线**：Apache Paimon 1.0.x（最新稳定，阿里出品）  
> **核心仓库**：https://github.com/apache/paimon  
> **面试权重**：⭐⭐⭐⭐⭐（Flink 生态最热门的数据湖表格式）

---

## 一、Paimon 整体定位

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：流批一体数据湖表格式（Flink 原生集成）            │
│  "Flink 做流处理，Paimon 做存储 + 流式读写"                │
│                                                               │
│  和 Iceberg / Delta Lake 对比（面试高频！）                  │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Paimon（阿里）│ Iceberg     │ Delta Lake    │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ Flink 支持 │ ★★★★★ 原生 │ ★★★ 一般    │ ❌ 差          │   │
│  │ Spark 支持 │ ★★★ 一般   │ ★★★★★ 原生  │ ★★★★★ 原生    │   │
│  │ 实时写入   │ ★★★★★ 强  │ ★★ 弱       │ ★★ 弱          │   │
│  │ CDC 入湖   │ ★★★★★ 强  │ ★★★ 一般    │ ★★ 弱          │   │
│  │ Compaction │ ★★★★★ 强  │ ★★★ 一般    │ ★★★ 一般      │   │
│  │ 主键表     │ ✅ 原生    │ ✅ 原生      │ ✅ 原生        │   │
│  │ Append-Only│ ✅ 原生    │ ✅ 原生      │ ✅ 原生        │   │
│  │ Sink 写入   │ ✅ Flink SQL│ ❌          │ ❌            │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ Flink + Paimon = 最佳搭档（实时数仓首选！）               │
│ ★ Iceberg：Spark / Trino 生态好                            │
│ ★ Delta Lake：Databricks 出品（和 Spark 深度集成）          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Paimon 架构（Flink 集成核心）                              │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐     │
│  │ Flink Job                                            │     │
│  │  ┌──────────────┬──────────────┬──────────────┐     │     │
│  │  │ CDC Source    │ Flink Transform│ Paimon Sink   │     │     │
│  │  │ MySQL CDC     │ SQL 聚合/Join │ 写入 Paimon    │     │     │
│  │  │ → Kafka CDC   │              │ 表（湖存储）    │     │     │
│  │  └──────────────┴──────────────┴──────────────┘     │     │
│  └─────────────────────────────────────────────────────┘     │
│         ↓                            ↓                       │
│    CDC 实时流                   HDFS / S3 存储               │
│                                                               │
│ ★ Paimon Sink 写入 → 实时 Compaction（不阻塞写入）           │
│ ★ Paimon Source 读取 → 支持增量 + 全量（Changelog）          │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 表类型 + 存储格式

```java
// ★ Paimon 三种表类型（面试高频）
//
// ① Append-Only Table（只追加）
//   CREATE TABLE paimon_logs (
//       log_id BIGINT,
//       level STRING,
//       message STRING,
//       log_time TIMESTAMP
//   ) WITH (
//       'bucket' = '8',               -- Hash 桶数
//       'bucket-key' = 'log_id'       -- Hash Key
//   );
//
//   → 类似 Iceberg Append-Only
//   → 不支持 UPDATE/DELETE（只追加）
//   → 写入最快
//
// ② Primary Key Table（★ 主键表，核心特色）
//   CREATE TABLE paimon_order (
//       order_id BIGINT PRIMARY KEY NOT ENFORCED,
//       user_id BIGINT,
//       amount DECIMAL(10,2),
//       status STRING,
//       create_time TIMESTAMP
//   ) WITH (
//       'bucket' = '8',
//       'bucket-key' = 'order_id',
//       'merge-engine' = 'deduplicate'  -- 去重合并引擎
//   );
//
//   → 支持 UPDATE/DELETE（主键覆盖写）
//   → 实时 UPSERT（CDC 入湖核心！）
//   → 底层：LSM-Tree + 实时 Compaction
//
// ③ Changelog Table（CDC 专用）
//   CREATE TABLE paimon_cdc_user (
//       user_id BIGINT,
//       name STRING,
//       age INT,
//       op STRING,        -- I/U/D 操作类型
//       ts TIMESTAMP
//   ) WITH (
//       'changelog-producer' = 'input'  -- 保留 CDC 原始操作
//   );
//
//   → 完整保留 CDC 变更（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE）
//   → 下游可以消费完整变更
//
// ★ 存储格式：
//   Paimon 用 Parquet / ORC / Avro 列式存储（和 Iceberg 一样）
//   → 数据文件还是列式，只是元数据层不同
```

### 2.2 LSM-Tree + 实时 Compaction（★ 核心）

```java
// ★ Paimon 主键表底层用 LSM-Tree（和 TiKV/RocksDB 一样）
//   支持实时写入 + 实时 Compaction
//
// LSM-Tree 写入流程：
//   ① 写 MemTable（内存表，快）
//   ② MemTable 满了 → 刷新成 ImmutableMemTable
//   ③ ImmutableMemTable 写 SSTable（磁盘不可变文件）
//   ④ 后台 Compaction 合并多个 SSTable
//      → 消除冗余主键 / 去重 / 删除过期数据
//
// ★ 为什么 LSM-Tree 适合 Flink 流写入？
//   Flink 流处理：持续写入大量数据（实时）
//   → MemTable + SSTable → 顺序写磁盘（比随机写 B+Tree 快）
//   → Compaction 后台合并（不阻塞写入）
//
// ★ Paimon Compaction 策略（面试高频）：
//   ① Size-Tiered Compaction（默认）：
//      多个 SSTable 按大小分 Tier → 同 Tier 合并成更大 Tier
//
//   ② Universal Compaction：
//      类似 Cassandra / RocksDB → 所有 SSTable 按时间合并
//
//   ③ Interval Partition Compaction：
//      按分区 + 时间间隔触发 Compaction
//
// ★ 和 Iceberg Compaction 对比：
//   Iceberg：Compaction 是后台大 Task（Flink Batch Job）
//   Paimon：**实时 Compaction（Flink Task 里做，不阻塞写入）**
//   → 这就是 Paimon 更适合 Flink 流场景的核心原因！
```

### 2.3 CDC 入湖完整链路

```sql
-- ★ Flink CDC + Paimon 入湖（面试最热点！）
--
-- ① MySQL CDC Source（Flink SQL Connector）
CREATE TABLE mysql_order (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10,2),
    status STRING,
    create_time TIMESTAMP,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql.example.com',
    'port' = '3306',
    'database-name' = 'app',
    'table-name' = 'order',
    'username' = 'cdc',
    'password' = 'xxx'
);

-- ② Paimon Sink
CREATE TABLE paimon_order_cdc (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    user_id BIGINT,
    amount DECIMAL(10,2),
    status STRING,
    create_time TIMESTAMP
) WITH (
    'bucket' = '8',
    'bucket-key' = 'order_id',
    'merge-engine' = 'deduplicate',
    'write-mode' = 'change-log'
);

-- ③ CDC → Paimon Sink（一条 SQL 搞定！）
INSERT INTO paimon_order_cdc
SELECT * FROM mysql_order;
--
-- → MySQL CDC（Binlog）→ Flink Source
-- → Flink Task 处理（Schema 演进 + 主键覆盖写）
-- → Paimon Sink 写入 LSM-Tree
-- → Paimon 自动 Compaction（去重 + 合并）
-- → Snapshot 管理（类似 Iceberg）
--
// ★ 一条 Flink SQL 实现 MySQL CDC 实时入湖！
//   这就是为什么 Flink + Paimon 是实时数仓首选
```

### 2.4 Paimon Snapshot + 读优化

```java
// ★ Paimon 也有 Snapshot（和 Iceberg 类似但更轻量）
//
// Snapshot 结构：
//   Snapshot → Manifest List → Manifest → Data Files
//   → 和 Iceberg 几乎一样！（都是 Apache 项目，互相借鉴）
//
// ★ Paimon 的独特之处：读优化
//   ① Changelog 模式：保留 CDC 完整变更（I/U/D）
//      → 下游 Flink Consumer 可以消费完整变更流
//      → 类似 Kafka Consumer 消费 Topic
//
//   ② Batch Read 模式：批量读取历史数据
//      → 类似 Hive / Spark SQL 全量查询
//
//   ③ Streaming Read 模式：实时消费增量数据
//      → Flink Source 从最新 Snapshot 开始读
//      → 后续增量 Snapshot 自动消费
//      → 类似 Kafka Consumer Group
//
// ★ 和 Iceberg 读取对比：
//   Iceberg：Spark/Flink/Presto 都支持，但 Flink 不是原生
//   Paimon：**Flink 原生支持三种读取模式**（Batch/Streaming/Changelog）
//   → 这是 Flink 生态选择 Paimon 的核心原因！
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Paimon 核心定位？| **阿里出品 Flink 原生数据湖表格式** → 实时 CDC 入湖 + 主键表 LSM-Tree；Flink 实时数仓首选！ |
| 2 | Paimon vs Iceberg？| Paimon：**Flink 原生 + 实时写入 + 实时 Compaction**；Iceberg：Spark/Trino 生态好；选哪个看主引擎 |
| 3 | Paimon 三种表类型？| **Append-Only（只追加）/ Primary Key（LSM-Tree UPSERT）/ Changelog（完整 CDC 变更）**；主键表最核心 |
| 4 | Paimon Compaction？| **Size-Tiered / Universal / Interval Partition**；**实时 Compaction（Flink Task 里做，不阻塞写入）**→ 比 Iceberg 后台 Task 更适合流 |
| 5 | Flink CDC + Paimon 入湖？| **一条 Flink SQL 搞定**：mysql-cdc Source → Paimon Sink；自动处理主键覆盖写 + Compaction + Snapshot |
| 6 | Paimon LSM-Tree？| 主键表底层用 LSM-Tree（MemTable → SSTable → Compaction）→ 顺序写磁盘适合实时流 |
| 7 | Paimon Snapshot？| 和 Iceberg 类似：Snapshot → Manifest List → Manifest → Data Files；但更轻量 |
| 8 | Paimon 读取三种模式？| **Batch（批量历史）/ Streaming（实时增量）/ Changelog（完整 CDC 变更）**→ Flink 原生支持 |
| 9 | Paimon merge-engine？| dedup（去重，主键覆盖）/ aggregation（聚合，SUM/COUNT 预聚合）/ first-row（首行优先）/ last-row（最新优先）|
| 10 | Paimon 存储格式？| Parquet / ORC / Avro 列式 → 和 Iceberg 一样；区别在元数据层（Snapshot + LSM） |
| 11 | Paimon 学习路径？| Docker 起 Flink + Paimon → MySQL CDC → 主键表入湖 → Flink SQL 一条命令搞定 → Compaction |
| 12 | Paimon vs Delta Lake？| Delta Lake：Databricks 出品 + Spark 深度集成；**Paimon：Flink 原生 + 实时场景强**；选哪个看主引擎 |
| 13 | Paimon 适用场景？| **实时数仓 CDC 入湖 / 实时指标聚合 / 维表 Join / 数仓分层（ODS→DWD→DWS）**；国内大厂（阿里/字节/美团） |
| 14 | Paimon 为什么火？| Flink 生态原生支持 + 实时 Compaction + CDC 入湖一条 SQL；**实时数仓 = Flink + Paimon + Iceberg（批量）** |
| 15 | Paimon 架构组件？| **Flink Table Store API → LSM-Tree / Append-Only / Snapshot → HDFS/S3 存储**；无独立进程（Flink Task 内） |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Paimon 1.0.x（阿里出品）  
**面试考点：15 题**
