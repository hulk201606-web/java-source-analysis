# Apache Kafka 副本同步 + ISR 机制源码级深度解析

> **源码基线**：Kafka 3.8.x（Scala + Java）  
> **核心仓库**：apache/kafka → core/src/main/scala/kafka/cluster/ + kafka/server/  
> **面试权重**：⭐⭐⭐⭐⭐（Kafka 一致性核心，高频必问）

---

## 一、Kafka 副本架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Kafka = 分布式消息队列 + 副本机制                        │
│                                                               │
│  Kafka 集群架构：                                             │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ Broker 1  │ Broker 2  │ Broker 3  │  ...                  │
│  │ Topic-A   │ Topic-A   │ Topic-A   │ （每个 Topic 有多副本）│
│  │ P0(L) P1  │ P0 P1(L)  │ P0 P1     │  L=Leader             │
│  └──────────┴──────────┴──────────┘                         │
│
│ ★ 核心概念（面试必背！）：                                    │
│                                                               │
│  ① Partition：Topic 分成多个分区（P0/P1/P2）                │
│     每个 Partition 是有序的、可追加的日志流                   │
│
│  ② Replica：每个 Partition 有 N 个副本（默认 3）            │
│     副本分布在不同 Broker（机架感知）                         │
│
│  ③ Leader：每个 Partition 选一个 Leader                      │
│     Producer/Consumer 只跟 Leader 交互                       │
│     Followers 同步 Leader 的数据                             │
│
│  ④ ISR（In-Sync Replicas ★ 最重要！）                       │
│     与 Leader 保持同步的副本集合                              │
│     → 这个集合是动态变化的                                   │
│     → Leader 挂了 → 从 ISR 里选新 Leader                    │
│
│ ★ 副本 vs Raft vs TiKV（面试高频对比）：                      │
│   Kafka ISR：**动态同步集合**（延迟超过阈值踢出）            │
│   Raft：**全量副本同步**（所有副本都参与投票）               │
│   TiKV Raft：**Peer 全同步**（3 副本，Quorum=2）            │
│   → Kafka 更灵活：允许部分副本落后，保证高吞吐               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、ISR 机制源码级

### 2.1 ISR 核心数据结构

```scala
// ★ Kafka ISR 核心数据结构（源码级）
//
// // core/src/main/scala/kafka/cluster/Partition.scala
//
// class Partition(val topicPartition: TopicPartition, ...) {
//
//     // ★ ISR：与 Leader 保持同步的副本 ID 集合（面试核心！）
//     // 注意：这是 Scala Set，动态变化
//     private var inSyncReplicas: Set[Int] = Set(leaderEpoch)
//
//     // ★ Leader 副本自己肯定在 ISR 里
//     def isr: Set[Int] = inSyncReplicas
//
//     // ★ ISR 最小值（副本数）
//     def minIsr: Int = minIsrSize  // 默认 2（3 副本时，ISR 最少 2 个）
//
//     // ★ HW（High Watermark）：所有 ISR 副本都同步到的最大 Offset
//     // 面试必背！
//     def highWatermark: Long = leaderHW
// }
//
// ★ ISR 动态变化规则（面试必背！）：
//   ① Follower 落后太多（超过 replica.lag.time.max.ms，默认 30s）
//      → 从 ISR 踢出
//   ② Follower 追上 Leader 最新 Offset
//      → 加回 ISR
//   ③ Leader 挂了
//      → 从 ISR 里选新 Leader（优先 ISR 里最最新的副本）
//
```

### 2.2 ISR 同步检查源码级

```scala
// ★ Kafka ISR 同步检查（Leader 定期检查 Follower 是否跟上）
//
// // core/src/main/scala/kafka/cluster/Partition.scala
//
// def maybeExpandIsr(replicaId: Int, logEndOffset: Long): Unit = {
//     // ★ 检查 Follower 是否追上了 Leader
//     val replica = getReplica(replicaId)
//     val leaderHW = this.highWatermark
//
//     // Follower 的 logEndOffset >= Leader HW → 追上了
//     if (replica.logEndOffset >= leaderHW && !inSyncReplicas.contains(replicaId)) {
//         // 加回 ISR
//         val newIsr = inSyncReplicas + replicaId
//         updateIsr(newIsr)
//         // 持久化到 ZK / KRaft（Kafka 3.0+）
//         zkClient.updateIsr(topicPartition, newIsr)
//     }
// }
//
// def maybeShrinkIsr(): Unit = {
//     // ★ 定期（默认 10s）检查 ISR 里的 Follower 是否落后太多
//     val outOfSyncReplicas = mutable.Set[Int]()
//     val leaderHW = this.highWatermark
//     val now = System.currentTimeMillis()
//
//     for (replicaId <- inSyncReplicas if replicaId != leaderEpoch) {
//         val replica = getReplica(replicaId)
//         // ① Follower logEndOffset 落后 Leader HW 超过阈值
//         val lag = leaderHW - replica.logEndOffset
//         val lagTooFar = lag > replicaMaxLagBytes
//         // ② 或者很久没同步（30s）
//         val lastCaughtUpTime = replica.lastCaughtUpTimeMs
//         val notCaughtUpForAWhile = now - lastCaughtUpTime > replicaMaxLagTimeMs
//
//         if (lagTooFar || notCaughtUpForAWhile) {
//             outOfSyncReplicas.add(replicaId)
//         }
//     }
//
//     if (outOfSyncReplicas.nonEmpty) {
//         // 从 ISR 踢出落后的副本
//         val newIsr = inSyncReplicas -- outOfSyncReplicas
//         if (newIsr.size >= minIsr) {
//             updateIsr(newIsr)
//             zkClient.updateIsr(topicPartition, newIsr)
//         }
//     }
// }
//
```

---

## 三、Leader Election 源码级

```scala
// ★ Kafka Leader 选举（源码级）
//
// // Leader 挂了 → Controller（或 KRaft Controller）触发选举
//
// // core/src/main/scala/kafka/controller/Controller.scala
//
// def onLeaderFailover(partition: Partition): Unit = {
//     val topicPartition = partition.topicPartition
//     val currentLeader = partition.leaderEpoch
//
//     // ★ Step 1: 找新 Leader（从 ISR 里选）
//     val newLeaderOpt = partition.isr
//         .filter(_ != currentLeader)
//         .maxByOption { replicaId =>
//             // 选 ISR 里 logEndOffset 最大的（数据最新）
//             partition.getReplica(replicaId).logEndOffset
//         }
//
//     newLeaderOpt match {
//         case Some(newLeader) =>
//             // ✅ 从 ISR 里选到新 Leader
//             // 更新分区元数据 → 通知所有 Broker + Producer + Consumer
//             updateLeader(topicPartition, newLeader)
//
//         case None =>
//             // ❌ ISR 为空（所有副本都挂了）
//             // 从 OSR（Out-of-Sync Replicas）里选一个
//             val anyReplica = partition.replicas.maxByOption(_.logEndOffset)
//             anyReplica.foreach { replica =>
//                 updateLeader(topicPartition, replica.replicaId)
//                 // ⚠️ 注意：这个副本不在 ISR 里，可能丢数据！
//             }
//     }
// }
//
// ★ 面试高频：为什么 Leader 只从 ISR 里选？
//   ISR = 与 Leader 保持同步的副本 → 数据完整
//   OSR = 落后太多的副本 → 选它当 Leader 可能丢数据
//   → 保证高可用的同时尽量不丢数据
//
// ★ 极端情况：所有 ISR 都挂了怎么办？
//   unclean.leader.election.enable=false（默认 true 允许）
//   → false：不允许从 OSR 选 Leader → 分区不可用但不丢数据
//   → true：允许从 OSR 选 → 分区可用但可能丢数据
//   → 生产通常设为 true（可用性优先）
//
```

---

## 四、Producer 发送语义

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Kafka Producer 三种发送语义（面试必背！）                 │
│                                                               │
│  acks=0：                                                     │
│    Producer 发给 Leader → Leader 收到就返回                   │
│    → 不保证写入成功（Leader 可能收到后挂了）                   │
│    → 吞吐量最高，可靠性最低                                   │
│
│  acks=1（默认）：                                              │
│    Producer 发给 Leader → Leader 写入本地日志就返回           │
│    → 保证 Leader 写成功                                    │
│    → 但 Follower 可能还没同步完 Leader 就挂了 → 丢数据      │
│
│  acks=all（可靠性最高）：                                      │
│    Producer 发给 Leader → Leader 等待 **所有 ISR 副本** 同步  │
│    → 所有 ISR 都写完才返回                                   │
│    → 数据不会丢（只要 ISR 里至少还有一个存活）                │
│    → 吞吐量最低（等所有 ISR）                                │
│
│ ★ acks=all + min.insync.replicas=2：                           │
│    Producer 等所有 ISR → 但 ISR 必须 ≥ 2 个才允许写          │
│    → 如果 ISR 只剩 1 个 → Leader 拒绝 Producer 写入         │
│    → 保证写入时至少 2 个副本同步（更安全）                    │
│
│ ★ ISR 收缩到 min.isr 以下 → Leader 拒绝 acks=all 写入       │
│   → 保护数据安全：只有足够多副本同步才允许新写入             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题，史上最全）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Kafka ISR 核心概念？| **In-Sync Replicas**：与 Leader 保持同步的副本集合 |
| 2 | ISR 动态变化？| ① Follower 追上 Leader → 加回 ISR；② 落后超过 30s → 踢出 |
| 3 | ISR 源码位置？| core/src/main/scala/kafka/cluster/Partition.scala → `inSyncReplicas` Set |
| 4 | HW 是什么？| **High Watermark**：所有 ISR 副本都同步到的最大 Offset（面试必背！）|
| 5 | Leader 选举规则？| **从 ISR 里选** logEndOffset 最大的副本（数据最新） |
| 6 | Leader 选举源码？| core/.../controller/Controller.scala → `onLeaderFailover()` |
| 7 | acks 三种语义？| **0（不保证）+ 1（Leader 本地写）+ all（所有 ISR 同步）** |
| 8 | min.insync.replicas？| ISR 最少副本数；acks=all 时 ISR < minIsr → Leader 拒绝写入 |
| 9 | unclean.leader.election？| **true：允许从 OSR 选 Leader（可用性优先，可能丢数据）；false：不允许（一致性优先）** |
| 10 | Kafka vs Raft vs TiKV ISR？| Kafka ISR 动态集合；Raft 全副本参与；TiKV Raft 3 Peer Quorum |
| 11 | Producer 重试？| 默认 3 次 + retry.backoff.ms=100ms；可能导致重复消费 |
| 12 | 面试最高频考点排序？| **1. ISR 动态变化 2. Leader 选举 3. acks 三种语义 4. HW 5. min.insync.replicas** |
| 13 | 学习路径？| Partition.scala → maybeExpandIsr → maybeShrinkIsr → Controller 选举 → Producer 发送 |
| 14 | Kafka 3.0+ KRaft？| 用 Raft 替代 Zookeeper 做 Controller；元数据存储更高效 |
| 15 | 面试常见追问？| ISR 空了怎么办？→ unclean.leader.election.enable=true 允许从 OSR 选；false 分区不可用 |
| 16 | Consumer 消费？| Consumer 跟 Leader 读；Leader 切主 → Consumer 自动发现新 Leader |
| 17 | ISR 踢出阈值？| **replica.lag.time.max.ms=30000ms** + replica.lag.max.messages（默认无限制）|
| 18 | HW vs LEO？| HW：所有 ISR 同步的最大 Offset；LEO（Log End Offset）：Leader 最新写入 Offset |
| 19 | ISR 检查频率？| **默认 10s**（由 replica.lag.time.max.ms 控制）|
| 20 | 生产配置推荐？| acks=all + min.insync.replicas=2 + unclean.leader.election.enable=true |
| 21 | 2024 新特性？| Kafka 3.7+ KRaft 生产可用 + Tiered Storage（冷热分层）|
| 22 | 面试高频场景？| Leader 切换 + ISR 踢出 + acks=all + min.insync.replicas 防脑裂 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：core/src/main/scala/kafka/cluster/Partition.scala  
**面试考点：22 题（史上最全）**  
**差异化点**：ISR 动态机制 + Leader 选举源码级 + acks 三种语义 + HW/LEO
