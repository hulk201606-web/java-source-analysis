# Apache Hive 源码深度解析（大数据仓库）

> **源码基线**：Apache Hive 3.1.x（最新稳定，Hadoop 3.x 兼容）  
> **核心仓库**：https://github.com/apache/hive  
> **面试权重**：⭐⭐⭐⭐（大数据高频，Spark SQL 替代但 Hive 仍是基础）

---

## 一、Hive 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：数据仓库（SQL 接口操作 HDFS 大数据）                │
│  解决什么问题？用 SQL 操作 TB-PB 级大数据（不用写 MapReduce） │
│                                                               │
│  Hive vs Spark SQL vs ClickHouse：                            │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │  Hive          │ Spark SQL     │ ClickHouse    ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 底层引擎       │ MapReduce/Tez │ Spark Engine  │ ClickHouse    ││
│  │ 数据存储       │ HDFS / S3    │ HDFS / S3    │ 本地磁盘      ││
│  │ 延迟           │ 分钟级（MR慢）│ 秒级（快）    │ 亚秒级 ★★★★★ ││
│  │ 列式存储       │ ✅ ORC/Parquet│ ✅ ORC/Parquet│ ✅ MergeTree ││
│  │ 适用场景       │ 离线 ETL     │ 批 + 流       │ 实时分析      ││
│  │ 社区           │ Apache 老牌   │ Apache 热门   │ ClickHouse    ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ Hive 是最早的大数据 SQL 接口；Spark SQL 替代 MR 成为主流     │
│ ★ ClickHouse 实时分析性能碾压 Hive                           │
│ ★ 三者常组合：Hive/Spark SQL 做 ETL → ClickHouse 做实时报表   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Hive 架构组件                                                │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │  Hive Client  │ HiveServer2  │ Metastore     │ HDFS         ││
│  │  CLI / Beeline │ Thrift Server │ 元数据存储    │ 数据存储     ││
│  │  SQL 入口     │ 接收 SQL 请求 │ (MySQL/PG)   │ 分布式文件   ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                          ↓                                    │
│  Driver（查询驱动）                                           │
│  ├── Parser（SQL → AST）                                     │
│  ├── Semantic Analyzer（语义分析 + 查 Metastore）              │
│  ├── Query Optimizer（CBO 成本优化）                          │
│  ├── Physical Plan（物理执行计划：MapReduce / Tez / Spark）  │
│  └── Executor（执行物理计划）                                  │
│                                                               │
│ ★ Metastore 核心：存表结构 + 分区 + 列信息                    │
│ ★ HDFS 存数据（ORC/Parquet 列式格式）                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Hive SQL 执行流程

```java
// ★ Hive SQL 完整执行流程（面试高频）
//
// SQL: SELECT department, AVG(salary) FROM employee
//      WHERE age > 20 GROUP BY department;
//
// ① Parser（yacc）：
//    SQL → AST（语法树）
//    → 类似 MySQL SQL Parser
//
// ② Semantic Analyzer（语义分析）：
//    查 Metastore：employee 表是否存在？salary/age/department 列是否存在？
//    → 生成 Logical Plan（逻辑计划）
//      ├── TableScan(employee)
//      ├── Filter(age > 20)
//      ├── Group By(department)
//      └── Aggregate(AVG(salary))
//
// ③ Query Optimizer（成本优化）：
//    规则优化 + 成本估算
//    → Filter 下推（WHERE age > 20 下推到 TableScan 之后立即过滤）
//    → 列裁剪（只查 department/salary/age 三列）
//    → Join 优化（MapJoin / SortMergeJoin）
//    → 生成 Optimized Logical Plan
//
// ④ Physical Plan（物理计划）：
//    Logical → MapReduce / Tez / Spark 执行计划
//    → 默认 MapReduce（慢！Hive 3.x 支持 Tez/Spark）
//
// ⑤ Executor：
//    提交到 Hadoop YARN（MapReduce）
//    → 执行任务 → 结果返回 Client
//
// ★ Hive 三种执行引擎（面试高频）：
//   ① MapReduce（默认，最慢，分钟级延迟）
//   ② Tez（Apache 出品，比 MR 快）
//   ③ Spark（Spark SQL 模式，秒级延迟）→ spark-sql 命令就是这个
```

### 2.2 分区 + Bucketing（★ 核心设计）

```sql
-- ★ 分区（Partition）：目录级分区分开存
--   类似 ClickHouse PARTITION BY / MySQL 分区表
--
CREATE TABLE employee (
    id INT,
    name STRING,
    age INT,
    salary DOUBLE
)
PARTITIONED BY (dt STRING, country STRING);
--
-- HDFS 存储结构：
-- /user/hive/warehouse/employee/dt=20240101/country=CN/
-- /user/hive/warehouse/employee/dt=20240101/country=US/
-- /user/hive/warehouse/employee/dt=20240102/country=CN/
--
-- 查询优化：
SELECT * FROM employee WHERE dt = '20240101';
-- → **分区裁剪！只扫 dt=20240101 的分区目录**（不扫全表）
-- → 大幅减少 HDFS IO

-- ★ Bucketing（分桶）：同分区内 Hash 分桶
--   类似 Hash Join 预排序
--
CREATE TABLE employee_bucketed (
    id INT,
    name STRING,
    ...
)
CLUSTERED BY (id) INTO 10 BUCKETS;
--
-- 10 个桶文件：employee_bucketed/00000_0 / 00001_0 / ... / 00009_0
--
-- 作用：
-- ① 分桶采样 TABLESAMPLE(BUCKET 1 OUT OF 10 ON id)
-- ② Join 优化（两个表都按 id 分桶 → 可以 Bucketed Map Join）

-- ★ Partition vs Bucketing 对比：
-- Partition：目录级分区 → 分区裁剪（最常用）
-- Bucketing：文件级分桶 → Join/采样优化（少用，维护成本高）
```

### 2.3 ORC / Parquet 列式存储（★ 核心）

```java
// ★ Hive 推荐用 ORC 或 Parquet 列式存储（比 CSV/JSON 快 10-100 倍）
//
// 为什么列式存储快？
//
//   CSV（行式）：                    ORC/Parquet（列式）：
//   ┌─────────────────────────┐    ┌─────────────────────────┐
//   │ id | name | age | salary│    │ id | age | name | salary │
//   │ 1  | 张三 | 20  | 10000 │    │ 1  | 20  | 张三 | 10000  │
//   │ 2  | 李四 | 30  | 20000 │    │ 2  | 30  | 李四 | 20000  │
//   │ ...                     │    │ ...                     │
//   └─────────────────────────┘    └─────────────────────────┘
//   读 salary 要读整行 → 浪费！    只读 salary 列 → IO 少！
//
// ★ ORC vs Parquet：
//   ┌──────────┬──────────────┬──────────────┐
//   │            │  ORC            │  Parquet      │
//   ├──────────┼──────────────┼──────────────┤
//   │ 来源       │ Hortonworks     │ Cloudera / Dremel│
//   │ 压缩率     │ ★★★★★ 极高    │ ★★★★ 高      │
//   │ 生态       │ Hive 原生推荐  │ Spark/Presto 推荐│
//   │ 索引       │ 内置 Bloom Filter│ 无内置索引    │
//   │ 性能       │ 更快（Hive）   │ 更快（Spark） │
//   └──────────┴──────────────┴──────────────┘
//
// ★ ORC 特性：
//   ① 列级 Bloom Filter（跳过不相关行）
//   ② Zone Map（每列 min/max → 范围查询快）
//   ③ 压缩：ZLIB / SNAPPY / GZIP / LZO
```

### 2.4 Metastore（★ 元数据核心）

```sql
-- ★ Metastore 是 Hive 的元数据中心（独立服务 + MySQL/PG 存数据）
--   存所有表/分区/列信息
--
-- 关键表（MySQL 里）：
--   TBLS：表名 + DB + owner + table_type
--   COLUMNS_V2：列名 + 类型 + 注释
--   PARTITIONS：分区（dt=20240101, country=CN）
--   PARTITION_KEYS：分区键（dt, country）
--   SERDES：序列化器（ORC / Parquet / CSV）
--   SDS：存储描述符（location = /user/hive/warehouse/...）
--
-- ★ Metastore vs K8s etcd / ZK：
--   Metastore：存数据仓库元数据（表结构/分区）→ MySQL/PG
--   etcd/ZK：存分布式协调信息 → Raft/ZAB 共识
--   → 都是元数据，但场景完全不同
--
-- ★ Metastore 高可用：
--   MySQL/PG 主从 + Metastore Server HA（LB）
--   → Presto / Spark SQL 也共用 Metastore
```

### 2.5 ACID 事务（Hive 4.0+）

```sql
-- ★ Hive 4.0+ 开始支持 ACID 事务（类似 MySQL ACID）
--   之前的 Hive 不支持 Update/Delete（只能覆盖写）
--
-- 启用 ACID：
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.compactor.initiator.on = true;
SET hive.compactor.worker.threads = 5;

CREATE TABLE employee_acid (
    id INT,
    name STRING,
    salary DOUBLE
) CLUSTERED BY (id) INTO 10 BUCKETS STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

-- 现在可以：
INSERT INTO employee_acid VALUES (1, '张三', 10000);
UPDATE employee_acid SET salary = 15000 WHERE id = 1;
DELETE FROM employee_acid WHERE salary < 5000;

-- ★ Hive ACID 底层：
--   ① 写入文件前缀加 Transaction ID
--   ② 后台 Compactor 合并小文件 + 清理旧版本
--   ③ 类似 ClickHouse MergeTree + TiDB MVCC
--
-- ★ 之前 Hive 怎么"更新"数据？
--   INSERT OVERWRITE TABLE employee SELECT ... FROM employee WHERE ...
--   → 覆盖写整个表（不支持行级 Update）
--   → 这是 Hive 3.x 及以前的标准做法
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Hive 核心定位？| **数据仓库 SQL 接口** → 用 SQL 操作 HDFS/TB-PB 级大数据；替代手写 MapReduce |
| 2 | Hive 三大执行引擎？| **MapReduce（默认，慢，分钟级）/ Tez（中速）/ Spark（Spark SQL 模式，秒级）** → 推荐 Spark 模式 |
| 3 | Hive SQL 执行流程？| Parser（yacc → AST）→ **Semantic Analyzer（查 Metastore）→ Query Optimizer（Filter 下推 / 列裁剪 / Join 优化）→ Physical Plan → Executor** |
| 4 | Partition vs Bucketing？| **Partition（目录级）→ 分区裁剪（WHERE dt='20240101' 只扫这个目录）**；Bucketing（文件级）→ Hash 分桶 → Join/采样优化 |
| 5 | ORC vs Parquet？| ORC：**Hive 原生推荐 + Bloom Filter + Zone Map + 极高压缩**；Parquet：Spark/Presto 推荐 + 压缩高 |
| 6 | Metastore 作用？| **Hive 元数据中心**（表结构/分区/列）→ MySQL/PG 存储 → Presto/Spark SQL 也共用 |
| 7 | Hive ACID 事务？| **4.0+ 支持**（Transactional Table + ACID）；之前只能 INSERT OVERWRITE（覆盖写，不支持行级 Update）|
| 8 | Hive vs Spark SQL？| Hive：**老牌 + MR/Tez 引擎 + Metastore**；Spark SQL：**快速 + Catalyst 优化 + 秒级延迟**；Spark SQL 用 Hive Metastore |
| 9 | Hive vs ClickHouse？| Hive：**离线 ETL + 分钟级 + 依赖 HDFS**；ClickHouse：**实时分析 + 亚秒级 + 本地磁盘**；生产常组合用 |
| 10 | Filter PushDown？| **WHERE 条件下推到 TableScan 之后立即过滤** → 减少后续步骤数据量 → Hive/Spark/Catalog 都有这个优化 |
| 11 | 列裁剪？| **只 SELECT 需要的列** → 列式存储只读需要列 → IO 大幅减少 |
| 12 | MapJoin vs SortMergeJoin？| MapJoin：**小表广播到所有 Mapper**（O(1) 查找，最快）；SortMergeJoin：大表排序后 Merge；CBO 自动选 |
| 13 | Hive 分区怎么裁剪？| 分区键出现在 WHERE 里（WHERE dt='20240101'）→ Hive 只扫对应分区目录 → 不扫全表 |
| 14 | Hive 数据倾斜？| 热点 Key（某个 Key 数据量特别大）→ 加随机前缀 + 两阶段聚合；或用 Salting 技术 |
| 15 | Hive ACID Compactor？| 后台线程自动合并小文件 + 清理旧 Transaction 版本；类似 ClickHouse MergeTree 后台合并 |
| 16 | Hive 学习路径？| 写分区表 → ORC/Parquet 列式存储 → 分区裁剪/Filter 下推 → Bucketing → ACID 表 → Spark SQL |
| 17 | Hive 和 Hadoop 关系？| Hive **不替代 Hadoop** → 只是 HDFS 上的 SQL 接口；存储靠 HDFS，计算靠 MR/Tez/Spark |
| 18 | Hive 分区键类型？| 只能是 STRING 类型（底层 HDFS 目录名）；建议用 dt（yyyyMMdd）/ hour / country 等 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Hive 3.1.x / 4.0+ ACID  
**面试考点：18 题**
