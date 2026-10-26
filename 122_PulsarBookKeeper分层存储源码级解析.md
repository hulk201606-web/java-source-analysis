# Apache Pulsar BookKeeper + 分层存储源码级深度解析

> **源码基线**：Pulsar 3.2.x（Java）  
> **核心仓库**：apache/pulsar → bookkeeper/ + managed-ledger/  
> **面试权重**：⭐⭐⭐⭐⭐（云原生 MQ 新王者，分层存储高频必问）

---

## 一、Pulsar 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Apache Pulsar = 云原生多租户分布式消息系统               │
│                                                               │
│  Pulsar 架构（面试必背！）：                                   │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Broker（消息代理，管理 Topic/Subscription）            │   │
│  │   ├── BookKeeper（持久化存储 ★ 本文重点！）            │   │
│  │   └── Tiered Storage（分层存储 ★ 本文重点！）          │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ 读写                                                │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ BookKeeper（分布式日志存储）                           │   │
│  │   ① Entry（日志条目）→ 追加到 Ledger（账本）          │   │
│  │   ② Ledger 包含多个 Entry                              │   │
│  │   ③ 多副本写入（Quorum 机制）                          │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ 冷数据下沉                                          │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Tiered Storage（S3/HDFS/MinIO 等对象存储）             │   │
│  │   热数据 → BookKeeper（快但贵）                        │   │
│  │   冷数据 → 对象存储（慢但便宜）                        │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ Pulsar vs Kafka vs RocketMQ 核心区别（面试高频！）：          │
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │            │ Pulsar       │ Kafka          │ RocketMQ      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 存储模型    │ **BookKeeper │ Parquet/Log     │ CommitLog     │   │
│   │            │  + 分层存储** │ Segment        │ + ConsumeLog  │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 持久化方式  │ 多副本 Quorum │ ISR 同步        │ 刷盘+主从      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 冷热分离    │ ✅ 原生支持   │ ❌ 3.x 才支持   │ ❌ 不支持      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 多租户      │ ✅ 原生支持   │ ❌ 不支持       │ ❌ 不支持      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 消息投递语义 │ 至少一次+事务 │ 至少一次        │ 至少一次+事务  │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 生产场景    │ 云原生/大数据  │ 实时流/日志      │ 电商/金融      │   │
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、BookKeeper 核心源码级

### 2.1 Ledger + Entry 数据结构

```java
// ★ Pulsar BookKeeper 核心数据结构（源码级）
//
// bookkeeper/ledger/Ledger.java
//
// public class LedgerHandle {
//     // ★ Ledger = 一组 Entry 的集合（类似 Kafka 的 Segment）
//     private long ledgerId;           // Ledger ID
//     private long lastEntryId;        // 最后一个 Entry ID
//     private LedgerMetadata metadata;  // 元数据（Ensemble / Quorum / Ack）
// }
//
// public class LedgerMetadata {
//     // ★ BookKeeper 三副本参数（面试高频！）
//     int ensembleSize;  // 总副本数（比如 5）
//     int writeQuorumSize; // 写入需要多少副本（比如 3）
//     int ackQuorumSize;   // 确认需要多少副本（比如 2）
//     // 5-3-2 是生产推荐：5 个副本，写 3 个，确认 2 个即成功
// }
//
// // ★ Entry = 一条消息（类似 Kafka 的 Message）
// public class Entry {
//     private long ledgerId;    // 属于哪个 Ledger
//     private long entryId;     // Entry ID（单调递增）
//     private byte[] body;      // 消息体
//     private long timestamp;   // 写入时间
// }
//
```

### 2.2 BookKeeper 写入流程

```java
// ★ Pulsar BookKeeper 写入流程（面试核心！）
//
// bookkeeper/client/ZKBookKeeper.java
//
// public class BookKeeperClient {
//
//     // ★ 核心：Append 一条 Entry 到 Ledger
//     public CompletableFuture<Long> append(long ledgerId, byte[] data) {
//         // ① 选择 Ensemble（BookKeeper 节点集合）
//         // ensembleSize=5 → 从 BookKeeper 集群里选 5 个节点
//         Ensemble ensemble = ensembleManager.getEnsemble(ledgerId);
//
//         // ② 并行写入到 writeQuorumSize 个节点
//         // writeQuorumSize=3 → 同时发给 3 个节点
//         List<BookieSocketAddress> writeQuorum = ensemble.getWriteQuorum();
//
//         // ③ 等待 ackQuorumSize 个确认
//         // ackQuorumSize=2 → 收到 2 个 ack 就算成功
//         return writeParallel(writeQuorum, data, ackQuorumSize);
//     }
//
//     private CompletableFuture<Long> writeParallel(
//             List<BookieSocketAddress> nodes,
//             byte[] data,
//             int ackQuorumSize) {
//         // ★ 并行发送到所有 writeQuorum 节点
//         List<CompletableFuture<Ack>> futures = nodes.stream()
//             .map(node -> sendAsync(node, data))
//             .collect(Collectors.toList());
//
//         // ★ 聚合 ack，收到 ackQuorumSize 个就算成功
//         return CompletableFuture.allOf(futures.stream()
//             .limit(ackQuorumSize)
//             .toArray(CompletableFuture[]::new))
//             .thenApply(v -> entryId);  // 返回 Entry ID
//     }
// }
//
// ★ BookKeeper Quorum vs Kafka ISR vs TiKV Raft：
//   BookKeeper：Ensemble 动态 + Quorum 写入（5-3-2）→ 灵活
//   Kafka ISR：固定 ISR + acks=all → 强一致
//   TiKV Raft：固定 Peer + Raft 协议 → CP
//   → BookKeeper 更灵活，性能更好
//
```

---

## 三、Pulsar 分层存储源码级

### 3.1 冷热分层机制

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar Tiered Storage（分层存储，Pulsar 独家特色）         │
│                                                               │
│  核心思想：                                                   │
│   热数据 → BookKeeper（SSD，快但贵）                          │
│   冷数据 → 对象存储（S3/HDFS/MinIO，慢但便宜）                │
│
│  自动分层机制：                                                │
│   消息写入 → 先到 BookKeeper                                  │
│   经过 TTL（默认 1h）→ 自动下沉到 Tiered Storage              │
│   Consumer 拉消息时：                                         │
│     如果还在 BookKeeper → 直接拉（快）                        │
│     如果已经下沉 → 从 Tiered Storage 拉（慢但可以用本地缓存）  │
│
│ ★ 分层存储代码示例（生产配置）：                               │
│
│ // conf/bookkeeper.conf
│ # 热数据 TTL（多久后下沉）
│ managedLedgerDefaultTimeBucket = 3600000  // 1h
│
│ // conf/pulsar.conf
│ # Tiered Storage 启用
│ tieredStorageEnabled = true
│ tieredStorageDefaultBucket = pulsar-data-bucket
│ tieredStorageDefaultRegion = us-west-1
│
│ // BookKeeper 下沉源码（managed-ledger）
│ // managed-ledger/src/main/java/org/apache/bookkeeper/mledger/
│   // 后台定时任务扫描 Ledger
│   public void runTieredStorageTask() {
│       List<LedgerHandle> coldLedgers = ledgerList.stream()
│           .filter(l -> l.getAge() > ttlHours)  // 超过 TTL
│           .collect(Collectors.toList());
│
│       for (LedgerHandle ledger : coldLedgers) {
│           // ① 从 BookKeeper 读数据
│           byte[] data = ledger.read();
│           // ② 写入 Tiered Storage
│           tieredStorage.write(ledger.getLedgerId(), data);
│           // ③ 从 BookKeeper 清理（只保留 Tiered Storage 引用）
│           ledger.markAsTiered();
│       }
│   }
│
│ ★ 面试高频：分层存储 vs Kafka 3.x Tiered Storage？              │
│   Pulsar：**原生设计**（从一开始就考虑冷热分离）              │
│   Kafka：**3.x 才引入**（基于 KIP-405，后补的功能）            │
│   → Pulsar 更成熟、性能更好                                   │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Pulsar vs Kafka 存储对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar BookKeeper vs Kafka Segment 核心区别（面试高频！） │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┐                 │
│  │            │ Pulsar BookKeeper │ Kafka Segment│              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 写入模型    │ Quorum 并行写入  │ ISR 追加复制  │              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 副本管理    │ Ensemble 动态    │ ISR 固定       │              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 冷热分离    │ ✅ 原生支持      │ ❌ 3.x 才支持  │              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 多租户      │ ✅ Namespace 原生 │ ❌ 不支持       │              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 磁盘使用率    │ 低（自动清理）  │ 高（需要手动清理）│             │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 适合场景    │ 大规模 + 多租户  │ 实时流/日志    │              │
│  └──────────┴──────────────┴──────────────┘                 │
│
│ ★ 面试高频：Pulsar 为什么是云原生 MQ 新王者？                  │
│   ① BookKeeper Quorum 并行 → 比 Kafka ISR 更快              │
│   ② 分层存储原生 → 成本低（冷数据用对象存储）                 │
│   ③ 原生多租户 → 适合云平台                                   │
│   ④ 独立存储计算 → 可以独立扩缩容                            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Pulsar 核心架构？| **Broker + BookKeeper（持久化）+ Tiered Storage（冷热分离）** |
| 2 | BookKeeper 核心数据结构？| **Ledger（账本）+ Entry（日志条目）** |
| 3 | BookKeeper Quorum 参数？| **Ensemble（5总）+ WriteQuorum（3写）+ AckQuorum（2确认）** |
| 4 | BookKeeper 写入流程？| 并行发送到 3 个节点 → 收到 2 个 Ack → 返回成功 |
| 5 | Tiered Storage 核心？| **热数据 BookKeeper + 冷数据对象存储**；TTL 自动下沉 |
| 6 | Pulsar vs Kafka 核心区别？| Pulsar BookKeeper Quorum + 原生分层存储 + 多租户；Kafka ISR + 3.x 才支持分层 |
| 7 | Pulsar vs RocketMQ？| Pulsar：云原生 + 分层存储 + Quorum；RocketMQ：电商 + 事务/延迟消息 |
| 8 | 面试最高频考点排序？| **1. BookKeeper Quorum 2. Tiered Storage 3. Pulsar vs Kafka 4. Ledger/Entry 5. 写入流程** |
| 9 | 学习路径？| Ledger.java → BookKeeperClient → writeParallel → TieredStorageTask |
| 10 | 面试常见追问？| Pulsar 为什么快？→ Quorum 并行写入 + 冷热分离；和 Kafka 比谁快？→ 各有场景 |
| 11 | BookKeeper Ensemble 动态？| 可以动态增减副本节点（比 Kafka ISR 灵活）|
| 12 | Pulsar Subscription 模式？| Exclusive（独占）+ Shared（共享）+ Failover（故障转移）+ Key_Shared |
| 13 | 2024 新特性？| Pulsar 3.2+ **异步写入优化** + **Schema Registry 增强** + **Pulsar IO 连接器** |
| 14 | 面试高频场景？| 云原生平台（多租户隔离）+ 大数据管道（冷热分离降本）+ IoT（海量消息）|
| 15 | Pulsar BookKeeper vs TiKV Raft？| BookKeeper：Quorum 写入 + 动态 Ensemble；TiKV：Raft 协议 + 固定 Peer |
| 16 | Pulsar 事务消息？| ✅ Pulsar 2.7+ 原生支持事务消息（和 RocketMQ 类似但更简单）|
| 17 | Tiered Storage 冷数据读性能？| 比 BookKeeper 慢（对象存储访问）；本地缓存可以加速 |
| 18 | BookKeeper 源码路径？| bookkeeper/client/ZKBookKeeper.java |
| 19 | Pulsar 生产推荐配置？| EnsembleSize=5, WriteQuorumSize=3, AckQuorumSize=2；TieredStorage 开启 |
| 20 | 面试陷阱？| 把 Pulsar BookKeeper = Kafka Segment → ❌ BookKeeper 有 Quorum 并行写入 |
| 21 | Pulsar Namespace 多租户？| Namespace 下多个 Topic；资源隔离（CPU/内存/存储配额）|
| 22 | Pulsar vs Flink？| Flink 是流处理引擎；Pulsar 是消息队列；常配合使用（Flink 消费 Pulsar）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：bookkeeper/client/ZKBookKeeper.java + managed-ledger/  
**面试考点：22 题**  
**差异化点**：BookKeeper Quorum 5-3-2 + Tiered Storage 原生分层 + Pulsar vs Kafka vs RocketMQ
