# Apache Doris 源码深度解析（OLAP 分析数据库）

> **源码基线**：Apache Doris 2.1.x（最新稳定）  
> **核心仓库**：https://github.com/apache/doris  
> **面试权重**：⭐⭐⭐⭐⭐（OLAP 国产最高频，抖音/百度/美团在用）

---

## 一、Doris 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：MPP 分布式分析数据库（Massively Parallel Processing） │
│  PB 级数据秒级分析 —— 国产 OLAP 事实标准                     │
│                                                               │
│  Doris 架构组件（面试高频）：                                 │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   FE (Frontend) │ BE (Backend)   │ BROKER        │     │
│  │ 元数据/调度   │ 存储/计算    │ 数据导入      │     │
│  │ Java 实现     │ C++ 实现     │ C++ 实现      │     │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         ↓              ↓              ↓                       │
│     MySQL 协议     LSM + 向量化   Stream Load                │
│     gRPC 元数据    + MPP 并行     / Broker Load               │
│                                                               │
│ ★ FE 高可用：奇数个 FE（3/5）选主；主 FE 挂了自动选主         │
│ ★ BE 核心：LSM-Tree + 向量化 SIMD + MPP 并行（三个核心技术）│
│ ★ 每个 Table 有 N 个 Primary Shard（默认 3 副本）            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Doris vs ClickHouse vs StarRocks（OLAP 三巨头对比）       │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Apache Doris  │ ClickHouse    │ StarRocks    │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 架构       │ FE+BE MPP     │ 单体节点集群  │ FE+BE MPP     │   │
│  │ 向量化     │ ✅ SIMD       │ ✅ SIMD      │ ✅ SIMD      │   │
│  │ BROADCAST  │ ★★★★★ 原生   │ ❌ 不支持    │ ★★★★★ 原生   │   │
│  │ 物化视图   │ ★★★★★ 自动   │ ✅ 手动       │ ★★★★★ 自动   │   │
│  │ 实时写入   │ ✅ Stream Load│ ✅ Kafka Eng. │ ✅           │   │
│  │ 国内流行度  │ ★★★★★ 最高  │ ★★★★ 高     │ ★★★ 中      │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ Doris 国内最流行（抖音/百度/美团/小米）                   │
│ ★ ClickHouse：单节点极致性能，但分布式和 Join 弱            │
│ ★ StarRocks：字节/阿里云维护（Doris 分支，更激进）          │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 四种数据模型（★★★★★ 面试核心）

```sql
-- ★ Doris 四种 Table Model（面试必问！和 MySQL/ClickHouse 有何不同）
--
-- ① DUPLICATE KEY（明细模型，默认）
CREATE TABLE order_duplicate (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10,2)
) DUPLICATE KEY(order_id, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;
-- → 保留所有重复（类似 ClickHouse MergeTree）→ 适合日志/行为明细

-- ② AGGREGATE KEY（聚合模型，★ Doris 特色）
CREATE TABLE order_aggregate (
    user_id BIGINT,
    total_amount DECIMAL(10,2) SUM,    -- ★ 预聚合！
    order_count BIGINT COUNT            -- ★ 后台写入时自动聚合
) AGGREGATE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;
-- → 后台写入自动 SUM/COUNT/MAX/MIN → 查询直接拿聚合结果 → 极快！
-- → 类似 ClickHouse SummingMergeTree

-- ③ UNIQUE KEY（唯一键模型）
CREATE TABLE user_unique (
    user_id BIGINT,
    name VARCHAR(100),
    age INT
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;
-- → 主键覆盖写（相同 user_id 新数据覆盖旧数据）
-- → ★ Doris 实现了真正的 Unique Key 约束（不是后台去重！）

-- ④ PRIMARY KEY（主键模型，★ Doris 2.0+ 核心）
CREATE TABLE user_pk (
    user_id BIGINT,
    name VARCHAR(100),
    age INT
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 10;
-- → 和 UNIQUE KEY 类似，但 Doris 2.0+ 实现了**主键索引**（类 MySQL B+Tree）
-- → 点查 WHERE user_id = ? 极快（毫秒级！）
--
-- ★ Doris vs ClickHouse 数据模型：
--   Doris：四种模型（Duplicate/Aggregate/Unique/PK）+ 真正约束
--   ClickHouse：MergeTree 家族（MergeTree/ReplacingMergeTree/SummingMergeTree）
--   → Doris 模型更清晰，UNIQUE KEY 真唯一（ClickHouse 是后台去重）
```

### 2.2 MPP 并行 + BROADCAST JOIN（★ 核心优势）

```cpp
// ★ Doris MPP 并行查询（面试高频）
//
// FE 生成分布式计划 → BE 之间通过 Exchange Node 交换数据
//
// 完整执行流程：
//   SELECT u.name, SUM(o.amount)
//   FROM user u JOIN order o ON u.id = o.user_id
//   WHERE o.dt = '2024-01-01'
//   GROUP BY u.name;
//
// ① FE 优化器：
//    - 谓词下推（WHERE dt=20240101 下推到 Partition Scan）
//    - 分区裁剪（只扫 dt=20240101 分区）
//    - **BROADCAST JOIN**（user 表小，广播到所有 BE → 避免 Shuffle）
//
// ② BE 并行执行：
//    BE1 → Scan order Part → Hash Join with user（已广播）→ Partial Agg
//    BE2 → Scan order Part → Hash Join with user（已广播）→ Partial Agg
//    BE3 → Scan order Part → Hash Join with user（已广播）→ Partial Agg
//
// ③ FE 合并：
//    所有 BE 返回 Partial Agg → FE Final Agg → 最终结果
//
// ★ BROADCAST JOIN 为什么是 Doris 核心优势？
//   ClickHouse：**不支持广播 Join**（必须 Shuffle 数据 → 慢！）
//   Doris：小表广播到所有 BE → O(1) 查找（Hash Join）→ 极快！
//   → 多表 Join 场景 Doris 比 ClickHouse 快 10x！
```

### 2.3 Stream Load（★ Doris 实时写入特色）

```bash
# ★ Doris Stream Load（HTTP PUT 接口，实时导入，面试高频）
#
# 相比 Broker Load（批量异步），Stream Load 是实时同步：
#
# curl -X PUT \
#   -u root: \
#   -H "label: order_stream_load_001" \
#   -H "column_separator: ," \
#   -T order.csv \
#   http://doris-fe:8030/api/my_db/order_duplicate/_stream_load
#
# → HTTP PUT 上传数据 → Doris 实时写入
# → 延迟毫秒级（不落地 WAL，直接写 BE）
# → 比 Kafka Engine 更通用（任何能发 HTTP PUT 的系统都能用）
#
# ★ Doris 三种导入方式（面试高频对比）：
#
#   Stream Load（实时）：HTTP PUT → 延迟低（ms）→ 数据量小-中
#   Broker Load（批量）：从 HDFS/S3 异步读 → 延迟高（min）→ 数据量大
#   Routine Load（Kafka）：消费 Kafka → 持续实时 → 类似 Kafka Connect
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Doris 核心定位？| **百度开源 Apache 顶级 MPP 分布式分析数据库** → PB 级秒级查询；国内 OLAP 事实标准 |
| 2 | Doris 架构？| **FE（Java，元数据+调度）+ BE（C++，存储+向量化+MPP）+ BROKER（数据导入）**；FE HA（奇数个选主）|
| 3 | Doris 四种数据模型？| **DUPLICATE（明细）/ AGGREGATE（预聚合）/ UNIQUE（覆盖写）/ PRIMARY KEY（主键索引，2.0+ 点查极快）**；Doris 模型最清晰 |
| 4 | Doris vs ClickHouse？| ClickHouse：**单节点极致 + 不支持广播 Join**；Doris：**MPP 分布式 + 广播 Join + 物化视图**；Join 场景 Doris 快 10x |
| 5 | Doris BROADCAST JOIN？| ★ 核心优势！**小表广播到所有 BE → 避免 Shuffle → Hash Join O(1) 查找**；ClickHouse 不支持！ |
| 6 | Stream Load 是什么？| **Doris 特色实时导入** → HTTP PUT 接口 → 毫秒级延迟；比 Kafka Engine 更通用 |
| 7 | Doris 向量化？| 2.0+ 默认开启 → SIMD AVX-512 指令一次处理 16 int → 比行式快 5-10x（和 ClickHouse 类似）|
| 8 | Doris Partition + Bucket？| **PARTITION BY RANGE（分区裁剪）+ DISTRIBUTED BY HASH（分桶）**；分桶数 = BE 数 × CPU 核数（经验公式）|
| 9 | Doris 物化视图？| **自动增量更新（核心优势）** → 写 MV 后底表变化自动更新；类似 Materialize |
| 10 | StarRocks vs Doris？| 都是 Apache Doris 分支 → StarRocks 字节/阿里云维护（更激进，新特性先加）；Doris 社区版（稳定）|
| 11 | Doris FE HA？| 奇数个 FE（3/5）→ Raft 选主；主 FE 写元数据 + EditableLog → 从 FE 同步；挂了秒级恢复 |
| 12 | Doris BE 故障恢复？| BE 挂了 → 副本自动从其他 BE 补（3 副本默认）；类似 TiKV Raft |
| 13 | Doris 适用场景？| **实时仪表盘 / 用户行为分析 / 广告投放 / 日志分析**；国内大厂（抖音/百度/美团/小米）|
| 14 | Doris PRIMARY KEY vs UNIQUE KEY？| 都是覆盖写；**PRIMARY KEY（2.0+）有主键索引**（类 MySQL B+Tree）→ 点查毫秒级；UNIQUE KEY 无索引 |
| 15 | Doris 学习路径？| Docker 起 FE+BE → 建四种模型表 → Stream Load 导入 → BROADCAST JOIN → 物化视图 |
| 16 | Doris vs Spark SQL？| Doris：**OLAP + 点查强 + 自动 MV**；Spark SQL：**批流 ETL + 大规模并行**；常组合（ETL Spark，实时 Doris）|
| 17 | Doris Compaction？| BE 后台线程自动合并小文件 → 类似 ClickHouse MergeTree Compaction |
| 18 | Doris 导入三方式？| **Stream Load（HTTP PUT/实时）/ Broker Load（HDFS/S3 批量）/ Routine Load（Kafka 消费）** |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Doris 2.1.x  
**面试考点：18 题**
