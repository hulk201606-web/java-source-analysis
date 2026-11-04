# RocksDB 源码深度解析（LSM-Tree 基石）

> **源码基线**：RocksDB 9.1.x（Facebook 开源）  
> **核心仓库**：facebook/rocksdb（C++ 实现）  
> **面试权重**：⭐⭐⭐⭐⭐（TiKV/ETCD/Doris/Paimon 底层引擎）

---

## 一、RocksDB 核心地位

```
┌─────────────────────────────────────────────────────────────┐
│  RocksDB：**几乎所有分布式存储的底层引擎**                    │
│                                                               │
│  谁在用 RocksDB？（面试高频！）                               │
│                                                               │
│  ① TiKV（TiDB 存储层）       ★ 字节/阿里                     │
│  ② ETCD（K8s 元数据）        ★ CNCF 毕业                     │
│  ② Apache Doris BE           ★ Apache 顶级                   │
│  ② Apache Paimon 主键表      ★ Apache 顶级                   │
│  ② CockroachDB               ★ Citus Data                    │
│  ② Amazon Timestream / DocumentDB                           │
│  ② Facebook 内部（原始作者）                                │
│                                                               │
│ ★ RocksDB 基于 Google LevelDB 增强而来                       │
│   → LevelDB（Google，2011）→ RocksDB（Facebook，2013）       │
│   → 核心改进：多线程 + 更灵活的 Compaction + 插件式          │
│                                                               │
│ ★ 为什么不是 BadgerDB/ BoltDB？                              │
│   BadgerDB（Dgraph）：Go 实现 + 单 LSM-Tree + 写放大大       │
│   BoltDB（Cockroach 早期）：B+Tree + 单线程 + 不适合大写入   │
│   RocksDB：C++ + **插件式 LSM-Tree** + 极致性能 + 活跃维护   │
│   → 分布式存储的事实标准（TiKV/ETCD 都用它）                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、LSM-Tree 核心原理（★★★★★ 面试必背）

### 2.1 写入路径

```c
// ★ RocksDB 写入完整流程（面试高频！）
//
// 写入：db.Put(key, value)
//
// ① 写 WAL（Write-Ahead Log）
//    → 追加写 WAL 日志（保证崩溃恢复）
//    → 顺序写磁盘（快！因为是追加）
//    → WAL 满了 → 滚动创建新 WAL
//
// ② 写 MemTable（内存，跳表结构）
//    → 红黑树/跳表（有序）
//    → 内存操作（纳秒级）
//    → MemTable 满了（~64MB）→ 转 Immutable MemTable（不再接受写）
//    → 触发后台 Flush
//
// ③ Immutable MemTable 写 SSTable（磁盘）
//    → Flush 线程把 Immutable MemTable 写成不可变 SSTable 文件
//    → 路径：/data/rocksdb/db0001.sst
//    → SSTable 有序 + 压缩 + 索引
//    → 这就是 L0 层的 SSTable
//
// ★ 关键：MemTable + Immutable MemTable = 双缓冲
//   → 一个在写，另一个在 Flush → 不阻塞写入！
//   → 类似 Kafka 双 Log / Doris 双 WAL
```

### 2.2 读取路径

```c
// ★ RocksDB 读取完整流程（面试高频！）
//
// 读取：db.Get(key)
//
// ① MemTable（内存活跃表）
//    → 跳表 O(log N) 查找
//    → 最快路径（大概率命中）
//
// ② Immutable MemTable（内存只读表）
//    → 也是跳表 O(log N)
//    → 如果 MemTable 没找到 → 查 Immutable
//
// ③ L0 SSTable（磁盘，最新数据）
//    → SSTable 有 Bloom Filter（快速判断 key 是否存在）
//    → Bloom Filter 说可能存在 → 读 SSTable（mmap + 索引）
//    → 多个 L0 SSTable 按时间从新到旧查
//
// ④ L1 → LN SSTable（磁盘，更老数据）
//    → 每一层内部 SSTable key 范围不重叠（有序）
//    → 二分查找定位 SSTable
//    → 逐层查（最多 N 层）
//
// ⑤ 如果找不到 → 返回 NotFound
//
// ★ 为什么 LSM-Tree 读比 B+Tree 慢？
//   B+Tree：O(log N) 一次定位
//   LSM-Tree：最坏情况 MemTable + Immutable + L0 + L1 + ... + LN
//   → 可能多次磁盘 IO
//   → **Level 分层（每一层不重叠）+ SSTable 索引 + Bloom Filter 缓解**
//
// ★ Compaction 为什么重要？
//   不 Compaction → L0 堆积 → 读要查很多 SSTable → 读放大爆炸！
//   Compaction 把 L0 合并到 L1 → L1 合并到 L2 → 每一层 SSTable 减少
//   → 读放大可控
```

### 2.3 Compaction 策略（★ 面试核心）

```c
// ★ RocksDB Compaction 三种策略（面试高频！）
//
// ① Leveled Compaction（默认，推荐）
//    → 每一层数据量：L1=1GB, L2=10GB, L3=100GB...（指数增长）
//    → 每一层内部 SSTable key 范围不重叠
//    → **优点**：读放大小（每层查一个 SSTable）
//    → **缺点**：写放大略高（需要多层合并）
//    → TiKV/ETCD/Doris 都用这个
//
// ② Size-Tiered Compaction
//    → 多个 SSTable 按大小分 Tier → 同 Tier 合并
//    → 类似 Cassandra/ClickHouse MergeTree
//    → **优点**：写放大低（合并次数少）
//    → **缺点**：读放大大（可能有重叠）
//    → 适合写密集 + 读不频繁场景
//
// ③ Universal Compaction
//    → 类似 Cassandra/ScyllaDB
//    → 所有 SSTable 按时间排序 → 最老的合并成最新的
//    → 适合时间序列（越老数据越大块）
//
// ★ Leveled Compaction 细节（TiKV 用的就是这个）：
//   L0 → Flush 产生的 SSTable（key 范围可能重叠）
//   L1 → 合并 L0 SSTable（key 范围不重叠）
//   L2 → 合并 L1（10x 数据量）
//   ...
//   → Compaction 触发条件：L0 数量 > 阈值 或 某层数据量超标
//   → 后台线程自动做，不阻塞读写
```

### 2.4 SSTable 文件结构

```c
// ★ RocksDB SSTable 内部结构（面试高频）
//
// SSTable 文件：/data/rocksdb/db0001.sst
//
// 内部结构（按顺序）：
//
//   ┌─────────────────────────────────────────────┐
//   │ Block 1: Data Block （key-value 数据）       │
//   │ Block 2: Data Block （继续）                 │
//   │ ...                                          │
//   │ Block N: Data Block（最后一个）             │
//   │                                              │
//   │ Filter Block: Bloom Filter（快速判断 key 存在）│
//   │ Index Block:  Data Block 索引（key → Block 位置）│
//   │ Footer:        元信息（Compression / 类型） │
//   └─────────────────────────────────────────────┘
//
// ★ 为什么 SSTable 这么快？
//   ① Data Block 按 key 排序 → 二分查找
//   ② Bloom Filter → O(1) 判断 key 可能存在
//   ③ Index Block → 直接定位 Data Block
//   ④ MMAP 模式 → 操作系统 Page Cache 自动管理
//   ⑤ 压缩（Snappy/ZSTD/LZ4）→ 减少磁盘 IO
//
// ★ SSTable vs Lucene Segment vs Kafka Log：
//   SSTable：有序 KV + Bloom Filter（读优化）
//   Lucene Segment：倒排索引 + Stored Fields（搜索优化）
//   Kafka Log：Append-Only 原始日志（写优化）
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RocksDB 是什么？| **Facebook 开源 C++ LSM-Tree 引擎** → TiKV/ETCD/Doris/Paimon 底层；基于 Google LevelDB 增强 |
| 2 | 写入完整流程？| WAL（追加写保证恢复）→ **双缓冲**（MemTable 在写，Immutable MemTable Flush）→ SSTable 不可变 |
| 3 | 读取完整流程？| MemTable → Immutable MemTable → L0（可能重叠）→ L1 → ... → LN；每层 Bloom Filter + 索引加速 |
| 4 | Leveled Compaction？| **默认推荐**（TiKV/ETCD 用）；L0→L1→L2... 每 10x 数据量；**每层 key 不重叠 → 读放大小** |
| 5 | Size-Tiered vs Universal？| Size-Tiered：按大小分 Tier（写放大大，读放大也大）；Universal：按时间（适合时间序列）|
| 6 | SSTable 结构？| **Data Block（有序 KV）+ Filter Block（Bloom Filter）+ Index Block（key→Block 位置）+ Footer** |
| 7 | LSM-Tree vs B+Tree？| LSM-Tree：**写快（Append Only）+ 读慢（可能查多层）+ 空间放大**；B+Tree：读快 + 写慢（随机写）+ 无放大 |
| 8 | 写放大 vs 读放大 vs 空间放大？| **写放大**：Compaction 重写数据（多写倍数）；**读放大**：读一个 key 查多少 SSTable；**空间放大**：数据实际占用倍数 |
| 9 | TiKV 为什么用 RocksDB？| 高并发写入（MemTable + Immutable 双缓冲不阻塞）+ **LSM-Tree 适合写密集** + Leveled Compaction 可控读放大 |
| 10 | ETCD 为什么用 RocksDB？| **有序 KV + Watch 可以按 Range 订阅**（RocksDB 有序支持 Range Scan）+ Compaction 后台做 |
| 11 | Doris BE 为什么用 RocksDB？| Doris 主键表需要 LSM-Tree + 实时 UPSERT（覆盖写）+ Doris 自研上层（LSM 存储层用 RocksDB）|
| 12 | 如何降低读放大？| ① Leveled Compaction（每层不重叠）② Bloom Filter（跳过不存在的 key）③ 缩小 MemTable（Flush 更频繁 → L0 小）|
| 13 | 如何降低写放大？| ① Size-Tiered Compaction（合并次数少）② 调大 MemTable（Flush 少）③ 减少删除（删除会让 Compaction 做额外工作）|
| 14 | WAL 作用？| **崩溃恢复**：写入先 WAL（持久化）再 MemTable（内存）；WAL 满了 → 滚动新 WAL；崩溃后从 WAL 恢复 MemTable |
| 15 | MMAP 读取？| SSTable 用 MMAP → OS Page Cache 自动管理 → 不需要进程管理内存 → 省内存 + 快 |
| 16 | RocksDB vs BadgerDB vs BoltDB？| RocksDB：**C++ + LSM + 极致性能**；Badger：Go + LSM + 写放大大；Bolt：Go + B+Tree + 单线程；选 RocksDB |
| 17 | RocksDB 学习路径？| 源码仓库 clone → 看 db/Put → db/Get → compaction 触发 → SSTable 格式 → TiKV 集成 |
| 18 | LSM-Tree 限制？| ① 读放大（多层查找）② 空间放大（多版本数据）③ 随机读比 B+Tree 慢；适合写密集 + 有序读 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：RocksDB 9.1.x（C++）  
**面试考点：18 题**  
**差异化点**：几乎所有分布式存储的底层引擎（TiKV/ETCD/Doris/Paimon/CockroachDB）
