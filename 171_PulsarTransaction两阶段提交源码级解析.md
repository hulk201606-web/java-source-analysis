# Apache Pulsar Transaction 两阶段提交源码级深度解析

> **源码基线**：Apache Pulsar 3.0+（Java）  
> **核心位置**：pulsar-broker/src/main/java/.../transaction/ + TransactionCoordinator  
> **面试权重**：⭐⭐⭐⭐⭐（消息队列 Exactly-Once 核心，高频）

---

## 一、Pulsar Transaction 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar Transaction = 跨 Topic/Partition 原子发送           │
│                                                               │
│  解决问题：                                                    │
│   应用需要原子地向多个 Topic 发送多条消息                      │
│   → 要么全部可见（Commit）→ 要么全部不可见（Abort）          │
│   中间不能出现"部分消息可见"的半完成状态                      │
│                                                               │
│  ★ Pulsar Transaction 核心角色（面试必背）：                  │
│   ┌─────────────────────────────────────────────────┐         │
│   │ TC（Transaction Coordinator）：                  │         │
│   │  Broker 端核心协调者；处理 Begin/Publish/Commit/Abort│     │
│   │                                                │         │
│   │ PulsarClient API：                              │         │
│   │  .newTransaction() → Begin（拿到 txnID）        │         │
│   │  .send(msg, txnID) → Publish（写 Transaction Buffer）│    │
│   │  .commit(txnID) → Commit（两阶段）             │         │
│   │  .abort(txnID) → Abort（回滚）                 │         │
│   └─────────────────────────────────────────────────┘         │
│                                                               │
│  ★ 核心机制：Transaction Buffer + 两阶段提交                  │
│   - 事务期间消息先存在 Broker 的 Transaction Buffer 里       │
│   - Commit：把 Buffer 消息移动到 Topic 正常位置（可见）     │
│   - Abort：Buffer 消息直接丢弃（不可见）                     │
│   - 2PC：Prepare（写 TransactionLog）→ Commit（写 Topic）   │
│
│  对比 Flink Exactly-Once Sink（面试高频）：                    │
│   Flink 2PC：每个 Sink 独立事务 + 外部 State 恢复            │
│   Pulsar TC：Broker 端跨 Topic 统一事务协调（见第六节对比）   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Transaction Coordinator 核心源码级

### 2.1 TC 架构

```java
// ★ TransactionCoordinator 核心类（源码级）
//
// pulsar-broker/src/main/java/org/apache/pulsar/broker/transaction/TransactionCoordinator.java
// pulsar-broker/.../transaction/TransactionBuffer.java
// pulsar-broker/.../transaction/TransactionLog.java
//
// // TC 是 Broker 启动时加载的单例组件：
// public class TransactionCoordinator implements AutoCloseable {
//
//     // 核心数据结构：
//     // ★ 1. Transaction Buffer（每个 TopicPartition 一个）
//     //    事务期间消息先存在 Buffer 里，Commit 时才移动到 Topic
//     Map<String, TransactionBuffer> txnBuffers;  // topic → buffer
//
//     // ★ 2. Transaction Log（持久化事务状态）
//     //    类似 REDO Log，Broker 崩溃后恢复用
//     TransactionLogImpl transactionLog;
//     //    存储在内部 Topic：__transaction_log_{txnID_bucket}
//
//     // ★ 3. Pending ACK Manager（消费端去重）
//     PendingAckManager pendingAckManager;
//
//     // ★ 4. Timeout Task（事务超时自动 Abort）
//     TransactionTimeoutChecker timeoutChecker;
//
//     // Begin 阶段：
//     public long newTransaction(long timeoutMs) {
//         // ① 生成全局唯一 txnID（单调递增 + TC 节点 ID）
//         long txnID = transactionIdGenerator.next();
//
//         // ② 创建 TransactionState（内存状态）
//         TransactionState state = new TransactionState(txnID, timeoutMs);
//         state.setStatus(TransactionStatus.BEGIN);
//
//         // ③ 持久化到 TransactionLog
//         transactionLog.add(txnID, TransactionStatus.BEGIN);
//
//         // ④ 注册超时清理
//         timeoutChecker.register(txnID, timeoutMs);
//
//         return txnID;  // 返回给客户端
//     }
// }
//
```

### 2.2 Publish（事务期间发送）

```java
// ★ Publish 核心流程（源码级）
//
// // 客户端调用：producer.newMessage().txnID(txnID).send()
// // → Broker BrokerService.handleSend() 检测到 txnID 不为空
// // → TransactionCoordinator.handlePublish()
//
// public CompletableFuture<SendReceipt> handlePublish(
//     long txnID, String topic, int partition, ByteBuf payload) {
//
//     // ① 查 TransactionState 是否存在且状态合法
//     TransactionState state = getTransactionState(txnID);
//     if (state.getStatus() != TransactionStatus.BEGIN) {
//         return failed(PulsarTxnError.InvalidTxnState);
//     }
//
//     // ② ★ 写 Transaction Buffer（关键！）
//     //    不是直接写 Topic，而是写到 Buffer
//     TransactionBuffer buffer = getOrCreateBuffer(topic, partition);
//     //    // TransactionBuffer 实现：
//     //    //   ① 先写 Broker 内存（OrderedBufferedQueue）
//     //    //   ② flush 到本地 RocksDB（buffer-ledger）
//     //    //   Topic 可见性：Buffer 消息对消费者不可见！
//     //    //   只有 Commit 后才移到 Topic
//     buffer.write(txnID, payload, sequenceId);
//
//     // ③ 记录 txnID → topic/partition 映射
//     //    一个 txn 可能涉及多个 Topic/Partition
//     state.addTopicPartition(topic, partition);
//
//     // ④ 返回 ack 给客户端（不代表 Topic 可见，只代表 Buffer 写入成功）
//     return completed(new SendReceipt(sequenceId));
// }
//
// // ★ TransactionBuffer 可见性关键：
// //   Consumer 读 Topic 时：Buffer 中的消息会被跳过
// //   TransactionBuffer 内部维护 txnID → 消息 sequence 范围
// //   Consumer Read 时过滤掉未 Commit 的 txnID 的消息
// //   // Broker EntryFilter: TransactionBufferFilter
//
```

---

## 三、两阶段提交源码级

### 3.1 Commit：Prepare → Commit

```java
// ★ Commit 两阶段提交核心（源码级，面试必背）
//
// TransactionCoordinator.handleCommit()
//
// public CompletableFuture<Void> handleCommit(long txnID) {
//
//     TransactionState state = getTransactionState(txnID);
//
//     // ======== 阶段 1：Prepare（持久化 + 冻结状态）========
//     state.setStatus(TransactionStatus.PREPARE);
//
//     // ① 持久化 PREPARE 状态到 TransactionLog
//     transactionLog.add(txnID, TransactionStatus.PREPARE);
//     //    ★ 这是崩溃恢复的关键锚点：
//     //    Broker 在 PREPARE 后崩溃 → 重启后从 TransactionLog 看到 PREPARE
//     //    → 自动重试 Commit（幂等）
//
//     // ② 冻结所有 Transaction Buffer（禁止新 Publish）
//     for (var tp : state.getTopicPartitions()) {
//         TransactionBuffer buf = getBuffer(tp);
//         buf.setFrozen(true);  // 之后再 publish 会报错
//     }
//
//     // ======== 阶段 2：Commit（把 Buffer 消息移到 Topic）========
//     // 对每个涉及的 Topic/Partition：
//     List<CompletableFuture<Void>> futures = new ArrayList<>();
//     for (var tp : state.getTopicPartitions()) {
//         futures.add(commitSingleTopic(tp, txnID));
//     }
//
//     return CompletableFuture.allOf(futures)
//         .thenRun(() -> {
//             // ★ 所有 Topic 都 Commit 成功才标记 COMMIT
//             state.setStatus(TransactionStatus.COMMIT);
//             transactionLog.add(txnID, TransactionStatus.COMMIT);
//
//             // 清理 Transaction Buffer
//             for (var tp : state.getTopicPartitions()) {
//                 TransactionBuffer buf = getBuffer(tp);
//                 buf.deleteTxnData(txnID);  // 已移到 Topic，Buffer 里的可以删了
//             }
//         });
// }
//
// private CompletableFuture<Void> commitSingleTopic(topic, partition, txnID) {
//     TransactionBuffer buf = getBuffer(topic, partition);
//
//     // ① 把 Buffer 里的所有消息按顺序写回 Topic
//     //    // 这一步用 Broker 内部写入（不走网络）
//     //    // 保持消息顺序：原 Buffer 里的消息 → Topic 同分区
//     List<Entry> entries = buf.readAll(txnID);
//     return brokerService.internalSend(topic, partition, entries)
//         // ② 给每条 Entry 打上 txnID 标记（Entry.metadata）
//         //    Consumer 端靠这个标记知道"这是事务消息"
//         //    Shared Subscription 有 PendingAckManager 做去重
//         .thenRun(() -> buf.setCommitted(txnID));
// }
//
```

### 3.2 Abort：丢弃 Buffer

```java
// ★ Abort 流程（源码级）
//
// public CompletableFuture<Void> handleAbort(long txnID) {
//     TransactionState state = getTransactionState(txnID);
//
//     // ① 持久化 ABORT 状态（也是崩溃恢复锚点）
//     state.setStatus(TransactionStatus.ABORT);
//     transactionLog.add(txnID, TransactionStatus.ABORT);
//
//     // ② ★ 直接丢弃 Transaction Buffer 里的所有消息
//     //    不写 Topic！消费者永远看不到
//     for (var tp : state.getTopicPartitions()) {
//         TransactionBuffer buf = getBuffer(tp);
//         buf.deleteTxnData(txnID);  // Buffer 直接删
//         buf.setAborted(txnID);
//     }
//
//     // ③ 清理内存状态 + 超时检查器
//     transactionStateMap.remove(txnID);
//     timeoutChecker.unregister(txnID);
//
//     return completed(null);
// }
//
// ★ Abort 自动触发场景（面试高频）：
//   ① 客户端显式 abort(txnID)
//   ② 事务超时（timeoutChecker 超时回调）
//   ③ Broker Transaction Log 崩溃恢复时：
//      - BEGIN 状态 → 自动 Abort（不完整事务）
//      - PREPARE 状态 → 自动重试 Commit（幂等）
//      - COMMIT/ABORT 状态 → 什么都不做（已完成）
//
```

---

## 四、Transaction Log + 崩溃恢复

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Transaction Log 持久化与崩溃恢复（面试核心）              │
│                                                               │
│  Transaction Log 内部存储：                                    │
│   在 Pulsar 内部 Topic：__transaction_log_{bucket}           │
│   按 txnID hash 分桶（类似 Kafka 的 __consumer_offsets）     │
│   Broker 重启后重放 Log 恢复所有未完成事务状态               │
│                                                               │
│  ★ 崩溃恢复四种状态（面试必背）：                             │
│                                                               │
│  ┌──────────────┬──────────────────────┬────────────────────┐│
│  │ Log 中的状态   │ 恢复时做什么          │ 原因               ││
│  ├──────────────┼──────────────────────┼────────────────────┤│
│  │ BEGIN        │ 自动 Abort            │ 事务还没 Publish/   ││
│  │              │                        │ 不完整，客户端可能  ││
│  │              │                        │ 还活着但 TC 挂了    ││
│  ├──────────────┼──────────────────────┼────────────────────┤│
│  │ PREPARE      │ ★ 自动重试 Commit      │ 关键锚点：Buffer 已 ││
│  │              │ （幂等）               │ 冻结，需要保证 Topic││
│  │              │                        │ 可见或全部不可见    ││
│  ├──────────────┼──────────────────────┼────────────────────┤│
│  │ COMMIT       │ 什么都不做             │ 已完成             ││
│  ├──────────────┼──────────────────────┼────────────────────┤│
│  │ ABORT        │ 什么都不做             │ 已完成             ││
│  └──────────────┴──────────────────────┴────────────────────┘│
│                                                               │
│  ★ 幂等 Commit 实现：                                         │
│   每个 txn 涉及的 Topic/Partition 都有"已 Commit 的 txnID"  │
│   Broker 读 Transaction Log 时发现重复 Commit → 跳过         │
│   // Topic Metadata: lastCommittedTxnIDs[topic][partition]    │
│
│  ★ Transaction Buffer 持久化：                                 │
│   内存 + 本地 RocksDB（类似 BookKeeper 的 Ledger）          │
│   Broker 重启恢复 TransactionBuffer 的 txn 数据             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、消费者端：PendingAckManager + 去重

```java
// ★ 消费者端事务消息处理（源码级，面试高频）
//
// Pulsar 消费事务消息的核心挑战：
//   同一条消息可能被 Consumer 多次拉到
//   （Broker 重启 / Consumer 重启 / Shared Subscription 抢占）
//   → 需要去重保证 Exactly-Once 消费
//
// // PendingAckManager 作用：
// //   记录 Consumer 已确认的消息（messageId）
// //   防止重复消费
// //   // 事务消息的 Ack 必须走 PendingAck
// //   // 普通消息可以直接 Ack
//
// public class PendingAckManager {
//     // 每个 Consumer 有一个 PendingAckManager
//     Set<MessageId> pendingAcks = new HashSet<>();
//
//     // Consumer 收到事务消息：
//     void messageReceived(Message msg) {
//         if (msg.isTransactionMessage()) {
//             // ① 检查是否已 Ack 过（去重）
//             if (pendingAcks.contains(msg.getMessageId())) {
//                 return;  // 跳过
//             }
//
//             // ② 加入 pendingAcks
//             pendingAcks.add(msg.getMessageId());
//
//             // ③ 交给应用处理
//             listener.receive(msg);
//         }
//     }
//
//     // Consumer 确认 Ack：
//     void ack(MessageId id) {
//         // 持久化到 ManagedLedger（避免 Consumer 重启丢 ack 记录）
//         ledger.addEntry(id.serialize());
//         pendingAcks.remove(id);
//     }
//
//     // Broker 端事务可见性检查：
//     // Consumer 拉 Topic 时，Broker EntryFilter 过滤掉：
//     // - 未 Commit 的 Transaction Buffer 消息
//     // - 已 Abort 的 txn 关联的消息
// }
//
// ★ Shared Subscription 事务消息的特殊处理：
//   Shared Subscription 下多个 Consumer 消费同一 Topic 分区
//   → 每个 Consumer 都有独立的 PendingAckManager
//   → Broker 端维护全局的已 Ack 集合（避免多 Consumer 重复 Ack）
//
```

---

## 六、差异化对比：Pulsar Transaction vs Flink 2PC Sink vs Kafka Transaction

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比表：三大消息队列 Exactly-Once 机制           │
│  ┌──────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度      │ Pulsar Transaction│ Flink 2PC Sink │ Kafka TX ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 协调者     │ ★ Broker 端 TC   │ Flink JobManager│ Broker   ││
│  │           │ 跨 Topic 统一   │ 每个 Sink 独立│ Producer ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 消息缓冲   │ ★ Transaction   │ 外部 State  │ 内部 TXN ││
│  │           │ Buffer（Broker） │ Backend     │ Buffer   ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 提交方式   │ TC 统一 Commit    │ JobManager   │ Producer ││
│  │           │ 跨 Topic 原子    │ Barrier + Sink│ Commit   ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 客户端感知  │ 显式 newTransaction│ Transactional │ initT  ││
│  │           │ + txnID         │ Sink         │xns      ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 事务粒度   │ ★ 跨 Topic/Partition│ 每 Job/Sink │ 单 Topic ││
│  │           │ 真正原子        │ 协调各 Sink  │ Producer ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 幂等保证   │ TC 状态 + 去重   │ Checkpoint   │ Idempotence││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 崩溃恢复   │ ★ Transaction   │ Flink Savepoint│ __txn_ids││
│  │           │ Log 自动重放    │ + Checkpoint │ 自动清理  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 消费端去重 │ ★ PendingAck     │ 外部幂等 Sink│ Consumer ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 复杂度     │ 中（Broker 实现）│ 高（Flink 全链路）│ 低（单 Broker）││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频   │ ★★★★★          │ ★★★★★        │ ★★★★    ││
│  └──────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   跨 Topic 原子 + Broker 端协调 → **Pulsar Transaction**      │
│   Flink SQL/Stream + 多 Sink → **Flink 2PC Sink**（已有生态）│
│   单 Topic Producer 事务 → **Kafka Transactional Producer**  │
│   三者配合：Flink CDC → Pulsar Transaction → 下游多 Topic 原子消费│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Pulsar Transaction 解决什么？| 跨 Topic/Partition **原子发送**：全部可见（Commit）或全部不可见（Abort）|
| 2 | Transaction Buffer 作用？| 事务期间消息先写 Buffer 不写 Topic；Commit 时移到 Topic；Abort 时丢弃 |
| 3 | TC（Transaction Coordinator）职责？| Begin → Publish 协调 → Prepare → Commit/Abort 两阶段 → 崩溃恢复 |
| 4 | 两阶段 Commit 核心步骤？| **Prepare（写 PREPARE 到 Log + 冻结 Buffer）→ Commit（Buffer 消息移到 Topic + 写 COMMIT）** |
| 5 | Transaction Log 为什么关键？| 崩溃恢复锚点；**PREPARE 后崩溃 → 自动重试 Commit（幂等）** |
| 6 | 崩溃恢复四种状态处理？| BEGIN→自动 Abort；PREPARE→自动 Commit；COMMIT/ABORT→跳过（已完成）|
| 7 | Transaction Buffer 可见性？| **Buffer 消息对 Consumer 不可见**；Broker EntryFilter 过滤掉未 Commit 的 |
| 8 | Abort 怎么处理 Buffer？| 直接丢弃 Buffer 里的所有消息；**不写 Topic**；消费者永远看不到 |
| 9 | PendingAckManager 作用？| Consumer 端事务消息去重；防止重复消费 |
| 10 | Shared Subscription 事务去重？| 每个 Consumer 独立 PendingAck；Broker 端维护全局已 Ack 集合 |
| 11 | Transaction Buffer 持久化？| 内存 + Broker 本地 RocksDB（Ledger）；重启可恢复 |
| 12 | Transaction 超时处理？| TimeoutChecker 后台线程；超时自动触发 Abort；防止客户端挂住 Buffer 不释放 |
| 13 | 幂等 Commit 怎么保证？| Topic Metadata 记录每个 txnID 的 Commit 状态；重复请求自动跳过 |
| 14 | Pulsar Transaction vs Kafka Transaction 核心区别？| Pulsar 跨 Topic 原子；Kafka 单 Producer 内多 Topic；**Pulsar Broker 端协调，Kafka 客户端协调** |
| 15 | ★ Pulsar vs Flink 2PC vs Kafka TX 全维度对比？| 见第六节对比表（10 维度）|
| 16 | 面试最高频考点排序？| **1. Transaction Buffer + 可见性 2. 两阶段 Commit + Prepare 锚点 3. 崩溃恢复四种状态 4. PendingAck 去重 5. 三者对比** |
| 17 | 学习路径？| TransactionCoordinator → TransactionBuffer → TransactionLog → handleCommit/Abort → PendingAckManager |
| 18 | 面试常见追问？| 事务期间 Broker 挂了，客户端还活着怎么办？| TC 重启后从 Log 看到 BEGIN → 自动 Abort → 客户端重试 |
| 19 | 面试常见追问？| 一个 txn 涉及 5 个 Topic，其中 4 个 Commit 了，第 5 个失败？| **全部回滚**！TC 要等所有 Topic 都成功才标记 COMMIT；任何一个失败 → 整体失败 |
| 20 | 面试陷阱？| 说 Transaction Buffer 是"临时 Topic" → ❌ Buffer 是 Broker 内存/本地 RocksDB，不是 Pulsar Topic；Buffer 消息根本没进 Topic |
| 21 | 面试陷阱？| 说 Abort 消息是"标记成 tombstone" → ❌ Buffer 消息直接丢弃，**Topic 里根本没有** |
| 22 | Transaction Log 存哪？| 内部 Topic：__transaction_log_{bucket}；按 txnID hash 分桶 |
| 23 | 事务消息 Consumer 怎么过滤？| Broker EntryFilter + PendingAckManager；**Broker 端过滤未 Commit 的 Buffer 消息** |
| 24 | 典型生产用法？| Flink CDC → Pulsar Transaction → 同时写 ClickHouse + Elasticsearch + TiDB 原子成功 |
| 25 | 为什么 Pulsar Transaction 比 Kafka TX 强？| Broker 端 TC 跨 Topic 真正原子；Kafka TX 是 Producer 端在一个 Producer 里的多 Topic；**Pulsar 支持跨 Broker 节点** |
| 26 | Pulsar 3.0+ 事务变化？| 移除部分旧 API；TransactionState 更清晰；TransactionBuffer 从内存+磁盘改为纯 RocksDB（更可靠）|

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：pulsar-broker/transaction/TransactionCoordinator.java + TransactionBuffer.java + TransactionLog.java  
**面试考点：26 题**  
**差异化点**：Transaction Buffer 可见性机制、Prepare/Commit 两阶段锚点、崩溃恢复四种状态处理、PendingAck 去重、Pulsar vs Flink 2PC vs Kafka TX 三机制对比
