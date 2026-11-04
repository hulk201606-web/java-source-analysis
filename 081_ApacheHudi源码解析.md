# Apache Hudi 源码深度解析（数据湖三大家）

> **源码基线**：Apache Hudi 1.0.x（最新稳定）  
> **核心仓库**：apache/hudi（Java 实现，Uber 开源）  
> **面试权重**：⭐⭐⭐⭐（数据湖高频，和 Iceberg/Paimon 差异化）

---

## 一、Hudi 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  Hudi = **数据湖三大家之一**（Hudi + Iceberg + Paimon）      │
│                                                               │
│  三家差异化定位（面试最高频对比）：                            │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Apache Hudi    │ Apache Iceberg│ Apache Paimon│   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 开源方     │ Uber → Apache │ Netflix → Apache│ 阿里 → Apache│   │
│  │ 核心灵感   │ **HBase + Log**  │ Git + Manifest │ **LSM-Tree + Flink**│
│  │ 存储模型   │ **Hoodie 表 + File Slice** │ Snapshot + Manifest│ Snapshot + LSM-Tree │
│  │ 实时写入   │ ✅ MOR 模式     │ ✅ Copy On Write│ ★★★★★ 实时  │
│  │ 主键表     │ ✅ Hoodie Key  │ ✅ Keyed Table  │ ★★★★★ 核心 │
│  │ 兼容引擎   │ Spark/Flink/Presto│ Spark/Flink/Trino│ Flink ★★★★★│
│  │ 国内流行度  │ ★★★ 中        │ ★★★★ 高      │ ★★★★★ 最高 │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ 选型建议：                                                  │
│   Spark 生态 / Trino 多引擎   → **Iceberg**（支持最广）      │
│   Flink 实时流处理 + 主键 UPSERT → **Paimon**（Flink 原生） │
│   Uber 技术栈 / Hive 兼容     → **Hudi**                    │
│                                                               │
│ ★ 为什么三家不一样？因为底层设计灵感不同：                    │
│   Hudi → HBase（File Slice 类似 HBase Region）              │
│   Iceberg → Git（Snapshot 类似 Commit）                      │
│   Paimon → LSM-Tree（类似 RocksDB + Flink 集成）             │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Hudi 核心概念（Hoodie！面试高频）

```java
// ★ Hudi 一切都叫 Hoodie（帽子，Uber 创始人喜欢）
//   → HoodieTable / HoodieFile / HoodieKey / HoodieLog...
//
// Hudi 核心设计（面试高频！和 Iceberg/Paimon 差异化）：
//
// ① Hoodie Table：支持 **两种存储模式**
//
//   a. Copy On Write（COW，类似 Iceberg COW）
//      写入：直接重写 File Slice → 读快（Parquet 列式）
//      读：直接读 Data File
//      写：慢（每次重写文件）
//      → 适合低写入频率场景
//
//   b. Merge On Read（MOR，Hudi 特色！类似 HBase MemStore+StoreFile）
//      写入：写 HoodieLog（追加写，类似 Kafka Log）
//      读：**Data File + HoodieLog Merge** → 慢（要合并）
//      后台 Compaction：把 HoodieLog 合并回 Data File
//      → 高写入频率场景（实时 CDC 入湖）
//
// ★ Hoodie File Slice（Hudi 特色）：
//   Hoodie Table 分区目录下的一组文件：
//     ├── abc123.parquet   → Base Data File
//     ├── abc123.log1      → HoodieLog（追加写日志）
//     └── abc123.log2      → HoodieLog（更多日志）
//   → Slice 有 FileGroupId 标识
//   → 类似 Iceberg Manifest 但更细粒度
```

### 2.2 HoodieKey + 主键索引

```java
// ★ Hudi HoodieKey（主键索引，面试高频）
//
// HoodieKey = RecordKey + PartitionPath + FileGroupId
//   → RecordKey：业务主键（user_id / order_id）
//   → PartitionPath：分区路径（dt=2024-01-01）
//   → FileGroupId：File Slice 唯一标识
//
// ★ Hudi 主键索引（比 Iceberg 更成熟）：
//
//   ① Bloom Filter Index（默认）：
//      每个 File Slice 有 Bloom Filter → 快速判断主键是否存在
//      → O(1) 判断 → 避免扫全表
//
//   ② HBase Index：
//      用 HBase 做主键索引（Hudi 就是 Uber 从 HBase 演进的）
//      → 最快（毫秒级点查）
//
//   ③ Flink State Index：
//      Flink 实时写入时用 Flink State（RocksDB）做主键索引
//      → 实时 UPSERT 最快
//
// ★ 和 Iceberg/Paimon 主键索引对比：
//   Hudi：多种索引（Bloom/HBase/Flink State）→ 更灵活
//   Iceberg：隐藏分区 + Metadata（索引在 Manifest 里）
//   Paimon：LSM-Tree 本身就是主键索引（类 RocksDB）
```

### 2.3 MOR 模式实时写入

```java
// ★ Hudi MOR（Merge On Read）实时写入完整流程（面试高频！）
//
// INSERT INTO hudi_orders VALUES (...)
//
// ① 写入 HoodieLog（Append Only）
//    → 按 FileGroupId 找到对应的 File Slice
//    → 追加写 HoodieLog 文件（类似 Kafka Log 追加写）
//    → 顺序写磁盘（快！O(1)）
//
// ② 后台 Compaction（类似 Flink RocksDB Compaction）
//    → Compaction 线程：
//      读 HoodieLog → 读 Data File → Merge 合并
//      → 生成新的 Parquet Data File
//      → 旧 HoodieLog + Data File 标记删除
//      → 新 Parquet 成为新的 Base File
//
// ③ 读取 MOR 表：
//    → 先读 Parquet Base File（历史数据）
//    → 再读 HoodieLog（最新变更）
//    → **在读取时做 Merge**（慢！需要合并）
//    → 查询延迟取决于 HoodieLog 大小
//
// ★ 和 Paimon MOR 对比：
//   Hudi MOR：读时 Merge → 查询慢（需要合并 HoodieLog + Base File）
//   Paimon LSM：写时 Compaction（Flink Task 里做）→ 查询快（直接读新 Base File）
//   → Paimon 更适合实时场景！
//
// ★ 和 Iceberg COW 对比：
//   Iceberg COW：写时重写 → 写慢读快
//   Hudi MOR：写时追加 → 写快读慢
//   → 互补场景选择
```

### 2.4 Hudi vs Iceberg vs Paimon 三大家

```
// ★ 面试必问：Hudi vs Iceberg vs Paimon 到底怎么选？
//
// 本质设计差异：
//
//   Hudi（Uber）→ HBase 风格
//     Hoodie File Slice + HoodieLog（类似 HBase MemStore + StoreFile）
//     → MOR 读时合并 → 写快读慢
//     → 索引成熟（Bloom/HBase/Flink State）
//
//   Iceberg（Netflix）→ Git 风格
//     Snapshot → Manifest List → Manifest → Data Files
//     → 原子快照 + Schema 演进 + Time Travel
//     → Spark/Trino 支持最好
//
//   Paimon（阿里）→ LSM-Tree 风格
//     Flink 原生集成 + 主键表 LSM-Tree
//     → 写时 Compaction（Flink Task 里做）
//     → 查询快（直接读新 Base File）
//     → Flink 实时数仓首选
//
// ★ 选型建议（面试高频）：
//   Spark + Trino 多引擎   → Iceberg（支持最广）
//   Flink 实时流           → Paimon（Flink 原生 + 写时 Compaction）
//   Uber 技术栈 + Hive     → Hudi（成熟稳定）
//   简单场景               → Iceberg（最通用）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Hudi 核心定位？| **数据湖三大家之一**（Uber 开源 Apache）→ 支持 MOR/COW 两种模式 → Hoodie File Slice + HoodieLog |
| 2 | Hoodie 是什么？| Hudi 一切核心对象：HoodieTable/HoodieFile/HoodieKey/HoodieLog；Uber 创始人喜欢 Hoodie 这个名字 |
| 3 | COW vs MOR？| **COW（Copy On Write）**：写时重写 File → 读快写慢；**MOR（Merge On Read）**：写时追加 HoodieLog → 写快读慢（读时合并）|
| 4 | HoodieKey 是什么？| **RecordKey（业务主键）+ PartitionPath + FileGroupId** → 定位数据 |
| 5 | Hudi 主键索引？| **Bloom Filter（默认）/ HBase Index（最快）/ Flink State Index（实时）** → 比 Iceberg 更成熟 |
| 6 | Hudi vs Iceberg vs Paimon？| Hudi：**HBase 风格 + MOR 读时合并 + 索引成熟**；Iceberg：**Git 风格 + 原子快照 + Spark 支持最好**；Paimon：**LSM-Tree + Flink 原生 + 写时 Compaction** |
| 7 | Hudi MOR 实时写入？| 写 HoodieLog（Append Only 快）→ 后台 Compaction 合并回 Parquet → 读时要合并 HoodieLog + Base File |
| 8 | Hudi HoodieLog？| 类似 Kafka Log 追加写 → Write Ahead Log（保证恢复）→ 后台 Compaction 清理 |
| 9 | Hudi 适用场景？| Uber 技术栈 + Hive 生态 + 实时 CDC 入湖（MOR）|
| 10 | Hudi 学习路径？| Spark + Hudi → MOR 表写入 → HoodieLog 观察 → Compaction 触发 → 读时合并 |
| 11 | 三家数据湖怎么选？| **Spark/Trino → Iceberg（支持最广）**；**Flink 实时 → Paimon（Flink 原生）**；Uber/Hive → Hudi |
| 12 | Hudi Schema 演进？| 和 Iceberg 类似：添加/重命名列只改元数据 → 旧文件无新列查询时 NULL 填充 |
| 13 | Hudi Time Travel？| **Instant（类似 Iceberg Snapshot）** → 可以回滚到任意 Instant；Hudi 有自己的时间线（Timeline）|
| 14 | Hudi vs Flink CDC？| Flink CDC 是数据捕获；Hudi 是数据湖存储 → CDC + Hudi = 实时入湖完整链路 |
| 15 | Hudi MOR 查询为什么慢？| 每次查询都要 **Data File + HoodieLog Merge** → HoodieLog 越大越慢 → 需要 Compaction 后台清理 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Hudi 1.0.x（Java）  
**面试考点：15 题**  
**差异化点**：数据湖三大家 + HBase 风格 + HoodieLog 读时合并
