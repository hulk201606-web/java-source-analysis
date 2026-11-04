# Apache Kvrocks 源码深度解析（Redis 协议 + RocksDB 存储）

> **源码基线**：Apache Kvrocks 2.10.x（最新稳定）  
> **核心仓库**：apache/kvrocks（C++ 实现，Apache 顶级）  
> **面试权重**：⭐⭐⭐⭐（开源 Redis 替代，TiKV/ETCD 同生态）

---

## 一、Kvrocks 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  Kvrocks = **Redis 协议 + RocksDB 存储引擎**              │
│                                                               │
│  解决什么问题？                                              │
│  Redis 单节点内存有限（~256GB） + 持久化占磁盘空间大        │
│  → 想用 Redis 协议（生态成熟）+ 持久化 KV 存储（磁盘够大）   │
│                                                               │
│  四种 Redis 生态对比（面试高频！）                           │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Redis          │ **Kvrocks**     │ TiKV/ETCD    │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 协议       │ Redis Protocol │ Redis Protocol │ KV / gRPC    │   │
│  │ 存储引擎   │ 内存 + RDB/AOF│ **RocksDB LSM** │ RocksDB LSM  │   │
│  │ 数据量上限  │ ~256GB 内存   │ TB 级磁盘      │ PB 级        │   │
│  │ 延迟       │ ★★★★★ 纳秒   │ ★★★ 微秒      │ ★★★ 微秒     │   │
│  │ 持久化     │ 可选           │ 强制（磁盘）   │ 强制          │   │
│  │ 集群模式   │ Cluster（16384  │ **Raft 原生**  │ Raft         │   │
│  │            │  Slot）       │ 无 Slot 概念   │ Region 分片   │   │
│  │ 开源方     │ Redis Inc.     │ Apache         │ TiDB/CoreOS  │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ Kvrocks 本质：**用 RocksDB 模拟 Redis 的所有数据结构**   │
│   Redis String → RocksDB 单 KV                             │
│   Redis Hash   → RocksDB 多 KV（field 作为 Key 后缀）       │
│   Redis List   → RocksDB 按 Index 有序存                    │
│   Redis Set    → RocksDB 按 member 存                       │
│   Redis ZSet   → RocksDB 双索引（member+score）              │
│                                                               │
│ ★ Kvrocks 用 Raft 做集群（不是 Hash Slot！和 Redis Cluster 不同）│
│   → 每个 Namespace 一个 Raft Group                         │
│   → 类似 TiKV Region Raft Group                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Redis 协议 → RocksDB 映射（★ 面试核心）

```cpp
// ★ Kvrocks 把 Redis 所有数据结构映射到 RocksDB（面试高频！）
//
// RocksDB 是通用 KV 存储 → 没有 Hash/List/ZSet 这些概念
// → Kvrocks 用不同的 Key 编码前缀区分 Redis 数据结构
//
// Key 编码规则（面试高频）：
//
//   String：    key            → value
//   Hash：      key + "#" + field → value        (多个 KV)
//   List：      key + "#" + index → value        (有序 KV)
//   Set：       key + "#" + member → ""           (只看 Key)
//   ZSet：      key + "#" + member → score        (ZSET_HASH)
//             + key + "#Z#" + score + member → "" (ZSET_SCORE 双索引)
//   Bitmap：    key + bitmap_offset → 0/1
//   Stream：    key + "#" + entry_id → value
//
// ★ 示例：
//   Redis: HSET user:1 name "张三" age 25
//   RocksDB:
//     user:1#name → "张三"
//     user:1#age  → "25"
//
//   Redis: ZADD rank 100 "userA" 200 "userB"
//   RocksDB:
//     rank#userA    → 100       (ZSET_HASH)
//     rank#userB    → 200
//     rank#Z#100userA → ""      (ZSET_SCORE 索引)
//     rank#Z#200userB → ""
//
// ★ 为什么这样设计？
//   RocksDB LSM-Tree 对前缀扫描很友好（MemTable 有序 + Bloom Filter）
//   → 用前缀 "#" 分隔 → 可以一次拿出所有 field/member
//   → ZSet 双索引实现按 member 查 + 按 score 查
```

### 2.2 ZSet 双索引（★ 面试高频）

```cpp
// ★ Redis ZSet 为什么需要双索引？（面试必问！）
//
// ZADD rank 100 "userA"
// ZSCORE rank "userA" → 100      ← 按 member 查 score
// ZRANGEBYSCORE rank 50 150      ← 按 score 范围查 member
//
// 这两个查询方向相反！一个 member→score，一个 score→member
// → 单一索引只能高效支持一个方向
//
// ★ Kvrocks 双索引方案（面试高频）：
//
//   ZSET_HASH（member → score）：
//     Key: rank#userA    → Value: 100
//     支持：ZSCORE / ZREM / ZINCRBY（按 member 操作）
//
//   ZSET_SCORE（score → member）：
//     Key: rank#Z#100userA → Value: ""
//     支持：ZRANGEBYSCORE / ZRANK（按 score 范围查）
//
//   ZADD 时同时写两个索引
//   ZREM 时同时删两个索引
//
// ★ Redis 原生 ZSet 也这样：
//   Redis 7.x 用 + skiplist（跳表）→ 双索引更直接
//   Kvrocks 用 RocksDB 双 KV → LSM-Tree 实现双索引
//
// ★ 为什么不用 RocksDB Secondary Index？
//   RocksDB 没有原生 Secondary Index（不像 MySQL B+Tree 可以建二级索引）
//   → 必须自己用双 KV 模拟双索引
//   → TiKV 也有类似设计（TiDB Secondary Index）
```

### 2.3 Raft 集群模式

```cpp
// ★ Kvrocks Raft 集群（面试高频！和 Redis Cluster 不同）
//
// Redis Cluster：16384 Hash Slot + MOVED/ASK 重定向
// Kvrocks：**Raft Group + Namespace 分片**（类似 TiKV）
//
// 架构：
//   Kvrocks Node 1 (Raft Leader for Namespace A + Replica for B)
//   Kvrocks Node 2 (Replica for A + Raft Leader for Namespace B)
//   Kvrocks Node 3 (Replica for A + Replica for B)
//
// ★ 为什么用 Raft 不用 Hash Slot？
//   ① Raft 有强一致性（比 Hash Slot + MOVED 的最终一致好）
//   ② Raft 有 Leader 自动选主（Hash Slot 故障转移需要手动）
//   ③ Kvrocks 底层 RocksDB 适合 Raft 日志复制
//   ④ Apache 想做一个更"正统"的分布式 Redis
//
// ★ 和 TiKV Raft 对比：
//   TiKV：每个 Region（≈96MB）一个 Raft Group → 几千个 Group 并行
//   Kvrocks：每个 Namespace 一个 Raft Group → 粒度粗
//   → TiKV 设计更细粒度（适合海量数据分片）
//
// ★ Kvrocks Raft 写流程：
//   Client → Raft Leader → Append Log → 过半确认 → Apply RocksDB → 返回
//   → 和 TiKV Raft 写路径完全一致！
```

### 2.4 和 Redis 5.x/6.x/7.x 版本兼容

```cpp
// ★ Kvrocks 兼容性（面试高频）
//
// Kvrocks 支持 Redis 所有核心命令（String/Hash/List/Set/ZSet/Bitmap/Geo/Stream）
// → 几乎 Redis Client 零代码改动
//
// 差异点（面试高频）：
//
// ① 持久化：
//   Redis：RDB（快照）+ AOF（追加写日志）
//   Kvrocks：**RocksDB WAL + SSTable 后台 Compaction**
//   → 自动持久化（不需要手动触发 BGSAVE）
//
// ② 内存：
//   Redis：全部在内存（~256GB 上限）
//   Kvrocks：RocksDB 自带 Block Cache（内存缓存 + 磁盘存储）
//   → TB 级数据，内存只缓存热点
//
// ③ Hash 操作优化：
//   Redis Hash > 512 field 用 Hashtable，< 512 用 ziplist
//   Kvrocks Hash：**每个 field 一个 RocksDB KV**
//   → 对多 field Hash 可能慢点，但对磁盘友好
//
// ④ 不支持 Redis Module：
//   Redis Module 是 C 写的动态库（Redis 7.x 核心扩展）
//   Kvrocks 目前不支持 Module → 需要移植 Module 到 Kvrocks
//   → 但 Redis Module 生态（Redisson 等）都在移植
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Kvrocks 核心定位？| **Redis 协议 + RocksDB LSM-Tree 存储** → 用磁盘扩展 Redis 数据量 → TiKV/ETCD 同生态 |
| 2 | Kvrocks 用什么存储引擎？| **RocksDB LSM-Tree**（和 TiKV/ETCD/Paimon 一样）→ 底层有 WAL + SSTable + Compaction |
| 3 | Redis 数据结构怎么映射？| **用 Key 前缀 "#" 分隔**：String=KV，Hash=key#field→value，List=key#index→value，ZSet=key#member→score + key#Z#score+member（双索引）|
| 4 | ZSet 双索引为什么需要？| **两个查询方向**：member→score（ZSCORE）+ score→member（ZRANGEBYSCORE）→ 必须双索引才能高效支持 |
| 5 | Kvrocks 集群怎么实现？| **Raft Group + Namespace 分片**（类似 TiKV Region Raft）→ Leader 自动选主 + 强一致；不是 Hash Slot |
| 6 | Kvrocks vs Redis Cluster？| Redis Cluster：**16384 Hash Slot + MOVED/ASK + 最终一致**；Kvrocks：**Raft + 强一致 + 自动选主** |
| 7 | Kvrocks 持久化？| **RocksDB WAL + SSTable Compaction**（自动，不需要手动 BGSAVE）；和 Redis RDB/AOF 不同 |
| 8 | Kvrocks vs TiKV/ETCD？| TiKV：Raft + Percolator 分布式事务（两层）；ETCD：Raft 一层 + MVCC + Watch；**Kvrocks：Raft + Redis 协议（最上层）**|
| 9 | Kvrocks 学习路径？| Docker 起单节点 → Redis Client 连接 → HSET/ZADD 实验 → 看 Key 前缀编码 → Raft 集群模式 |
| 10 | Kvrocks 国内流行度？| 中：Apache 顶级但不如 Redis 官方普及；**适合 Redis 生态但需要磁盘扩展的场景** |
| 11 | Kvrocks 为什么不支持 Redis Module？| Module 是 C 动态库（Redis 7.x 扩展）→ Kvrocks C++ 实现要重新适配；正在移植 |
| 12 | Kvrocks Hash 实现？| **每个 field 一个 RocksDB KV**（key#field→value）→ 对多 field Hash 慢点但对磁盘友好 |
| 13 | Kvrocks Stream 实现？| key + "#" + entry_id → value（类似 Kafka Log 结构）→ 按 entry_id 有序 |
| 14 | Kvrocks 适用场景？| Redis 数据量超过内存（>256GB）+ 想用 Redis 生态（Redis Client 零改动）|
| 15 | Kvrocks vs Redis 7.x 性能？| Redis 内存版延迟极低（纳秒）→ Kvrocks 磁盘版延迟微秒；但 Kvrocks 能存 TB 级数据 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Kvrocks 2.10.x（C++）  
**面试考点：15 题**  
**差异化点**：Redis 协议 + RocksDB + Raft 集群（和 TiKV/ETCD 同存储生态）
