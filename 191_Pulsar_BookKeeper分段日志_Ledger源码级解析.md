# Pulsar BookKeeper 分段日志 + Ledger 复制源码级深度解析

> **源码基线**：Apache Pulsar 3.0+ + Apache BookKeeper 4.15+（Java）  
> **核心位置**：pulsar-broker/.../ManagedLedger + bookkeeper-server/.../LedgerHandle/LedgerMetadata  
> **面试权重**：⭐⭐⭐⭐⭐（Pulsar 存储核心，补全 122 BookKeeper）

---

## 一、BookKeeper 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ BookKeeper = Pulsar 的分布式日志存储层                      │
│                                                               │
│  核心概念（面试必背）：                                        │
│   ✅ Entry（写入的消息块，类似 Kafka 的 Record Batch）         │
│   ✅ Ledger（有序 Entry 序列，类似 Kafka 的 Partition Log）   │
│   ✅ Ensemble（一个 Ledger 的所有副本 Bookie 节点）            │
│   ✅ Quorum（写成功的最少副本数）                              │
│   ✅ LAC（LastAddConfirmed，最后确认的 Entry ID）            │
│   ✅ Fencing（防止脑裂写入旧 Ledger）                         │
│                                                               │
│  与 Kafka 存储核心区别（第六节对比表）：                        │
│   Kafka Partition Log = 一个无限增长的 monolithic 文件        │
│   BookKeeper Ledger = 分段 Segment 日志 + 分布式 Ensemble    │
│   → BookKeeper 更灵活（可独立替换副本、动态扩缩 Ensemble）    │
│   → 但实现更复杂（LAC + Fencing + Ensemble 管理）            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Ledger Ensemble + Quorum 源码级

### 2.1 三层存储模型

```java
// ★ BookKeeper Ledger 三层存储模型（源码级）
//
// bookkeeper-server/src/main/java/org/apache/bookkeeper/
//
// // 1. LedgerManager（顶层管理）
// //    ZK 路径：/ledgers/{ledgerId}
// //    持久化 LedgerMetadata（Ensemble + Quorum + 状态）
//
// // 2. LedgerMetadata（ZK 上持久化的元数据）
// //    - ledgerId：全局唯一
// //    - ensemble：4（默认 4 个 Bookie）
// //    - writeQuorumSize：3（写需要 3 个 Bookie ACK）
// //    - ackQuorumSize：2（读确认需要 2 个 Bookie）
// //    - state：OPEN / CLOSED / IN_RECOVERY
// //    - createdAt / closedAt
// //
// // 3. LedgerHandle（Broker 内存引用）
// //    LedgerHandle = Broker 对一个 Ledger 的读写句柄
// //    持有 Ensemble Bookie 列表 + 当前 LAC
// //
// // // 创建 Ledger 流程（源码级）：
// LedgerCreateRequest request = client.newLedgerCreateRequest()
//     .setEnsembleSize(4)
//     .setWriteQuorumSize(3)
//     .setAckQuorumSize(2)
//     .setDigestType(DigestType.CRC32)
//     .setPassword("ledger-password")
//     .execute();
//
// // Broker 写入 Entry 时：
// // 从 ManagedLedger（Topic 的一个 Segment）
// // → 取当前 LedgerHandle
// // → LedgerHandle.addEntry(data, callback)
//
```

### 2.2 Entry 写入 + Quorum

```java
// ★ BookKeeper Entry 写入源码级（面试核心）
//
// bookkeeper-client/.../LedgerHandleAdv.java
// bookkeeper-server/.../BookieServer.java
// bookkeeper-server/.../Journal / LedgerStorage.java
//
// // Broker 写 Entry：
// ledgerHandle.addEntry(ByteBuffer entry, AsyncCallback.AddCallback cb) {
//     // ① 选 Entry ID（单调递增）
//     long entryId = getNextEntryId();
//
//     // ② ★ 并发向 Ensemble 的所有 Bookie 发写请求
//     for (BookieId bookie : ensemble) {
//         // Bookie 写 Entry：
//         bookie.write(ledgerId, entryId, entry);
//     }
//
//     // ③ ★ Quorum 确认：等 writeQuorumSize 个 Bookie ACK
//     //    默认 writeQuorumSize=3（Ensemble=4）
//     //    收到第 3 个 ACK → 立即返回成功给 Broker
//     //    剩下 1 个 Bookie 还没写完？→ 不等（异步补写）
//     waitForQuorum(entryId, writeQuorumSize, cb);
// }
//
// // ★ Bookie 本地存储（两级）：
// // 1. Journal（顺序追加 WAL）：
// //    bookkeeper-server/.../Journal/Journal.java
// //    先 fsync 到 Journal（可靠保证）
// //    格式：[ledgerId][entryId][entrySize][entryData][CRC]
// //    Journal 是 Bookie 崩溃恢复的核心
// //
// // 2. LedgerStorage（索引存储）：
// //    bookkeeper-server/.../LedgerStorage/LedgerStorage.java
// //    Entry 写完 Journal → 异步刷到 LedgerStorage
// //    默认 Interleaved Ledger Storage（一个 Bookie 的所有 Entry 混排）
// //    或 Flat Ledger Storage（每个 Ledger 独立文件）
// //
// //    LedgerStorage 维护 Index：ledgerId → entryId → 文件偏移
// //    读 Entry 时直接按 Index 定位
//
// // ★ Entry 格式（Bookie 内部）：
// struct Entry {
//     long ledgerId;       // 属于哪个 Ledger
//     long entryId;        // Ledger 内的序号（0, 1, 2, ...）
//     int entrySize;       // 数据大小
//     byte[] data;         // 实际 Entry 数据（Broker 写的）
//     long ackCode;        // ACK 码（成功/失败/超时）
//     CRC crc;             // 校验
// }
//
```

---

## 三、LAC + Fencing 源码级

### 3.1 LAC（LastAddConfirmed）

```java
// ★ LAC（LastAddConfirmed）核心源码级（面试必背）
//
// bookkeeper-server/.../BookieState.java
// bookkeeper-client/.../LedgerHandle.java
//
// // LAC = 最后一个确认的 Entry ID
// // 写入时 Broker 关心：我的 Entry 到底被多少 Bookie 确认了？
// //
// // 场景：
// //   Ledger Ensemble = [B1, B2, B3, B4]（4 个 Bookie）
// //   writeQuorumSize = 3
// //   Broker 写 entryId=100 → B1/B2/B3 ACK（3 个）→ 成功返回
// //   Broker 下次问 LAC → Bookie B2（或任意一个）回答 LAC=100
// //   Broker 写 entryId=101 → B1/B2 挂了 → B3/B4 只有 2 个（不够 3）
// //   → 写失败！触发 Ensemble Recovery
// //
// // ★ LAC 读确认：
// //   读 Entry 时，确保 Entry ID <= LAC 才返回
// //   防止读到正在写的 Entry（数据可能不一致）
// //
// // LedgerHandle 读 Entry：
// entry = ledgerHandle.readEntry(entryId);
// if (entryId > ledgerHandle.getLastAddConfirmed()) {
//     throw new LedgerException("Entry not yet confirmed");
// }
//
// // ★ LAC 与 Ensemble 变化的关系：
// //   当某个 Bookie 挂了 → 新 Bookie 加入 Ensemble
// //   新 Bookie 要从已有 Bookie 同步 LAC 之前的所有 Entry
// //   这个过程叫 Ledger Recovery（见第四节）
//
```

### 3.2 Fencing（防脑裂）

```java
// ★ Fencing 核心源码级（面试高频，Pulsar 独有设计）
//
// bookkeeper-server/.../BookieServer.java → fencing
// bookkeeper-client/.../LedgerHandle.java
//
// // 场景：脑裂 → 两个 Broker 都拿着同一个 LedgerHandle 写
// //   Broker A 和 Broker B 都认为自己是 Leader
// //   都在往同一个 Ledger 写 Entry
// //   → Entry ID 冲突 / 数据混乱
// //
// // ★ Fencing 机制（BookKeeper 怎么防这个）：
// //
// // 每个 LedgerHandle 有一个 **fencing token**（单调递增）
// // Bookie 收到写请求时：
// //   如果请求的 fencing token <= Bookie 当前记住的 token
// //   → 拒绝写入（返回 FencedLedgerException）
// //   Bookie 把最大的 fencing token 持久化到磁盘
// //
// // 流程：
// // 1. Broker A 拿 LedgerHandle，fencing token=100
// //    → 所有 Bookie 收到 token=100 → 记住 100
// //    → Broker A 正常写
// //
// // 2. Broker B 后来也要拿同一个 LedgerHandle
// //    → ZK 注册 fencing token=101（比 100 大）
// //    → Broker A 还在写（token=100）
// //    → Bookie 收到 Broker A 的写请求 → token=100 < 101 → 拒绝！
// //    → Broker A 发现 Fenced → 触发 Ledger Recovery
// //
// // ★ Fencing 是 BookKeeper 防脑裂的核心机制
// // 没有 Fencing → 脑裂时两个 Broker 会互相覆盖 Entry
//
// // ★ Pulsar Broker 侧 Fencing 使用：
// //   // pulsar-broker/.../ManagedLedger.java
// //   long fencingToken = managedLedger.getFencingToken();
// //   ledgerHandle = bkClient.createLedger(
// //       ensembleSize, writeQuorumSize, ackQuorumSize,
// //       fencingToken  // ★ 传给 BookKeeper
// //   );
//
```

---

## 四、Ledger Recovery 源码级

```java
// ★ Ledger Recovery 源码级（Bookie 挂了怎么办）
//
// bookkeeper-server/.../LedgerRecovery.java
// bookkeeper-server/.../BookieRegistry.java（ZK Watch 感知 Bookie 挂了）
//
// // Recovery 触发：
// // 1. Broker 写 Entry 时发现 Quorum 不够（writeQuorumSize 个 Bookie ACK 不到）
// // 2. ZK Watch 发现 Ensemble 中某个 Bookie 下线
// //
// // Recovery 步骤（源码级）：
// //
// // Step 1: 选一个新 Bookie 替换下线的
// //   BookieRegistry.getAvailableBookies() → 选最少负载的 Bookie
// //
// // Step 2: 新 Bookie 加入 Ensemble
// //   更新 ZK LedgerMetadata.ensemble
// //
// // Step 3: ★ 新 Bookie 同步 Entry（核心！）
// //   a. 找到 LAC（从还活着的 Bookie 问 LAC）
// //   b. 从 0 到 LAC 同步所有 Entry
// //   c. 每个 Entry 向还活着的 Bookie 拉取（RPC 复制）
// //   d. 同步完成 → 新 Bookie 加入正式 Ensemble
// //
// // Step 4: Fencing Token 升级
// //   防止旧 Broker（可能还在写）继续往新 Ensemble 写
// //
// // Step 5: 恢复写入
// //   新 Ensemble Quorum 够了 → Broker 继续写新 Entry
// //
// // ★ Recovery 性能影响：
// //   Bookie 恢复期间，Ledger 写可能延迟（等 Recovery 完成）
// //   数据量大的 Ledger Recovery 可能要几分钟
// //   → 生产建议：Ensemble=5, writeQuorum=3, ackQuorum=2
// //     允许 2 个 Bookie 同时挂了还能写
//
```

---

## 五、BookKeeper vs Kafka 存储 vs TiKV Raft 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比：三种分布式日志存储                          │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐  │
│  │ 维度      │ BookKeeper    │ Kafka Topic   │ TiKV Raft     │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 日志模型  │ 分段 Segment │ 无限增长 Log   │ 无限增长 Raft │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 复制协议  │ ★ Ensemble + │ ISR（In-Sync  │ ★ Raft Quorum │  │
│  │           │ LAC          │ Replicas）    │              │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 写确认    │ writeQuorum   │ acks=all/1/-1 │ Raft 半确认   │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 脑裂防   │ ★ Fencing     │ epoch（Controller）│ Raft 任期   │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 副本替换  │ ★ 动态 Ensemble│ ❌ ISR 固定    │ 静态（Raft） │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 存储格式  │ Journal +     │ .log (segment) │ RocksDB      │  │
│  │           │ LedgerStorage │ + .index      │ + Raft Log   │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 适用场景  │ Pulsar 分布式 │ Kafka 消息队列 │ TiDB 分布式  │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 面试高频  │ ★★★★★        │ ★★★★★        │ ★★★★★        │  │
│  └──────────┴──────────────┴──────────────┴──────────────┘  │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   Pulsar → **BookKeeper**（动态 Ensemble + Fencing + 分布式）│
│   Kafka → Kafka 原生存储（简单 Monolithic Log + ISR）         │
│   TiDB → TiKV Raft（KV 存储 + CP 强一致 + Percolator）       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（25 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | BookKeeper 的作用？| **Pulsar 分布式日志存储层**；分段 Segment + Ensemble 副本 |
| 2 | Ledger Ensemble Quorum？| Ensemble=4（默认副本数）；writeQuorum=3（写需要 3 个 ACK）；ackQuorum=2（读确认 2 个）|
| 3 | Entry 写入流程？| 并发向 Ensemble 所有 Bookie 写 → 等 writeQuorum ACK → 异步补写剩余 |
| 4 | Bookie 两级存储？| **Journal（WAL fsync 可靠）+ LedgerStorage（异步刷索引）** |
| 5 | ★ LAC（LastAddConfirmed）是什么？| **最后一个 Quorum 确认的 Entry ID**；Broker 读取时确保 EntryID <= LAC |
| 6 | ★ Fencing 核心？| **fencing token 单调递增**；Bookie 拒绝小 token 写 → 防脑裂 Broker 双写 |
| 7 | Fencing token 升级时机？| 新 Broker 想接管同一个 Ledger → ZK 注册更大的 fencing token → Bookie 自动隔离旧 Broker |
| 8 | Ledger Recovery 步骤？| 选新 Bookie → 加入 Ensemble → 从 LAC 之前同步所有 Entry → Fencing Token 升级 → 恢复写入 |
| 9 | Bookie 挂了为什么 LAC 会降？| LAC 是 writeQuorum 个 Bookie 的最小值；挂了一个 Bookie → 可能够不上 writeQuorum → LAC 下降 |
| 10 | Quorum 配置建议？| Ensemble=5, writeQuorum=3, ackQuorum=2；**允许 2 个 Bookie 挂**还能写 |
| 11 | BookKeeper vs Kafka 存储核心差异？| BookKeeper 分段 Ledger + 动态 Ensemble + Fencing；Kafka 无限 Partition Log + 静态 ISR + epoch |
| 12 | 面试最高频考点排序？| **1. Ensemble + Quorum 2. LAC 含义 3. Fencing 防脑裂 4. Entry 写入流程 5. 三者存储对比** |
| 13 | 学习路径？| LedgerMetadata → Ensemble → Entry 并发写 → LAC → Fencing → Recovery |
| 14 | 面试常见追问？| 为什么 BookKeeper 和 Kafka 存储设计不同？| Pulsar 要分布式多 Region 部署（BookKeeper Ensemble 更灵活）；Kafka 单集群就够（Monolithic Log 更简单）|
| 15 | 面试常见追问？| 写 Entry 为什么先写 Journal？| Journal fsync 保证持久性；崩溃恢复时从 Journal 重放 Entry |
| 16 | 面试陷阱？| 说"Fencing 就是让旧 Broker 不能再写"→ ❌ Fencing 是 Bookie 端**拒绝小 token**；不是 Broker 端主动停 |
| 17 | 面试陷阱？| 说"LAC 是最后写的 Entry"→ ❌ LAC 是**最后 Quorum 确认的**；可能有些 Bookie 还没同步，但 Broker 已经认为成功 |
| 18 | Ledger Entry ID 全局有序吗？| 在一个 Ledger 内有序；跨 Ledger 无序（不同 Segment）|
| 19 | Broker 重启后怎么恢复 Ledger 写？| ZK 查 LedgerMetadata → 拿 Ensemble + LAC → 继续写新 Entry |
| 20 | Bookie Registry 存在哪？| ZK：/bookies/available（可用 Bookie 列表）；Bookie 启动时注册临时 ZNode → 挂了自动消失 |
| 21 | Journal 空间为什么要限制？| Journal 是顺序追加文件 → 磁盘满了 Bookie 不写 → Ledger 无法写 → 生产需监控 Journal 使用率 |
| 22 | Bookie 有多个 Journal？| 多个 Journal（Disk Striping）→ 并发写多磁盘 → 提高写吞吐 |
| 23 | ManagedLedger 是什么？| Pulsar Topic 的一个 Segment；对应一个 BookKeeper Ledger；满了（Entry 数/大小到阈值）自动换新 Ledger |
| 24 | 典型生产调优？| Ensemble=5, writeQuorum=3, ackQuorum=2；Bookie Journal 多磁盘 Striping；LedgerStorage 选 DbLedgerStorage（基于 RocksDB，高写入）|
| 25 | BookKeeper vs TiKV Raft 核心差异？| BookKeeper 是**消息存储层**（不做事务/索引）；TiKV Raft 是**KV 存储层**（Percolator MVCC + 事务）|

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：bookkeeper-server/.../LedgerMetadata.java + LedgerHandleAdv.java + BookieServer.java + Journal.java  
**面试考点：25 题**  
**差异化点**：Ensemble + writeQuorum + ackQuorum 三参数、LAC（LastAddConfirmed）防脏读、Fencing token 防脑裂、**BookKeeper vs Kafka vs TiKV Raft 三存储对比**
