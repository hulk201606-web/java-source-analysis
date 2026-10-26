# Raft 协议源码级深度解析（分布式共识基石）

> **源码基线**：TiDB TiKV / ETCD v3.5（Go 实现）  
> **核心仓库**：tikv/raft-rs / etcd/raft  
> **面试权重**：⭐⭐⭐⭐⭐（分布式系统最高频协议，TiKV/ETCD/Prometheus 都用）

---

## 一、Raft 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  Raft：分布式系统**共识协议**的事实标准                      │
│                                                               │
│  谁在用 Raft？（面试高频！）                                  │
│                                                               │
│  ① TiKV（TiDB 存储层）        ★ 字节/阿里（raft-rs 实现）    │
│  ② ETCD（K8s 元数据）         ★ CNCF（Go 实现）              │
│  ② Prometheus / VictoriaMetrics  ★ HashiCorp Consul         │
│  ② CockroachDB                ★ Citus Data                    │
│  ② MinIO / YugabyteDB / Apache Kvrocks                     │
│                                                               │
│ ★ Raft 协议由 Stanford 2014 年提出（Diego Ongaro + John Ousterhout）│
│ ★ 论文标题：In Search of an Understandable Consensus Algorithm│
│   → 核心特点：**易懂**（比 Paxos 简单得多）                  │
│   → 工程上可以实现（不是只有学术价值）                        │
│                                                               │
│ ★ Raft 解决什么问题？                                        │
│   分布式存储需要保证：                                       │
│   ① 所有副本数据一致（Strong Consistency）                   │
│   ② Master 挂了自动选新 Master（High Availability）          │
│   ③ 至少有 Quorum（N/2+1）活着才能工作                       │
│   → Raft 完美解决这三个问题                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Raft 三核心子问题（★★★★★ 面试必背）

### 2.1 Leader Election（选主）

```
┌─────────────────────────────────────────────────────────────┐
│  一、Raft Leader Election 流程                               │
│                                                               │
│  节点角色：                                                   │
│   ┌──────────────┬──────────────┬──────────────┐             │
│   │ Follower      │ Candidate    │ Leader        │             │
│   │ 追随者        │ 候选人      │ 领导者        │             │
│   └──────┬───────┴──────┬───────┴──────┬───────┘             │
│          │ 选举超时     │ 获得过半票   │ 心跳超时              │
│          │ → 转 Candidate│ → 转 Leader  │ → Follower          │
│          ↓              ↓              ↓                     │
│        Candidate      Leader        Follower                  │
│                                                               │
│  选主完整流程（面试高频）：                                   │
│                                                               │
│  ① Follower 心跳超时（默认 150ms，带随机抖动）              │
│     → 变成 Candidate → Term（任期）+1                       │
│     → 给自己投票                                              │
│     → 向所有其他节点发 RequestVote RPC                        │
│                                                               │
│  ② Candidate 收到 VoteResponse：                             │
│     a. 超过半数回复 YES → **成为 Leader** → 广播 Heartbeat    │
│     b. 其他节点 Term 更高 → 退回 Follower，更新 Term         │
│     c. 没收到足够票（投票分裂）→ 保持 Candidate，Term+1 重试 │
│                                                               │
│  ③ Leader 保持心跳（默认 50ms）                               │
│     → 向所有 Follower 发 AppendEntries（空心跳）              │
│     → 收到心跳 → Follower 重置超时计数器                     │
│                                                               │
│  ★ PreVote 机制（TiKV/ETCD 都有）：                          │
│    竞选前先发 PreVote（试探性投票）→ 避免无效竞选             │
│    → 网络分区恢复时减少 Term 膨胀                            │
│                                                               │
│  ★ Term（任期）防止脑裂：                                    │
│    每个 Leader 有唯一 Term（递增）                            │
│    → 旧 Leader 回来发现新 Leader Term 更高 → 自动降级 Follower│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Log Replication（日志复制）

```
┌─────────────────────────────────────────────────────────────┐
│  二、Raft Log Replication 流程                               │
│                                                               │
│  写请求完整链路（面试高频！TiKV/ETCD 都这个流程）：           │
│                                                               │
│  Client write(key, value)                                    │
│    ↓                                                          │
│  ① Leader 接收请求                                           │
│     → 写入本地 Log（Append Only，类似 Kafka Log）             │
│     → Log 条目：[Term, Index, Command]                      │
│     → 先写 WAL（保证崩溃恢复）                                │
│                                                               │
│  ② Leader 并发复制给 Follower（AppendEntries RPC）          │
│     → 每个 Follower 独立复制（并行）                          │
│     → 复制成功 → Follower 回复 ACK                           │
│                                                               │
│  ③ 计算 CommitIndex（★ Raft 最核心规则！）                   │
│     CommitIndex = 超过半数 Follower 已复制的最大 Index       │
│     → Leader 记录每个 Follower 的 matchIndex                  │
│     → 超过半数 matchIndex >= N → CommitIndex = N              │
│                                                               │
│  ④ Leader 应用 Command 到状态机（apply）                    │
│     → 对 [lastApplied+1, commitIndex] 范围的 Log 执行命令    │
│     → 类似 TiKV Apply Log / ETCD Apply Event                 │
│     → 更新本地 State Machine（DB/MetaData）                  │
│                                                               │
│  ⑤ Leader 回复 Client 成功                                   │
│                                                               │
│  ★ 为什么要超过半数才能 Commit？                               │
│     保证：任何超过半数的 Quorum 至少包含一个已 Commit 的条目 │
│     → Leader 切换时新 Leader 必然有最新数据                  │
│     → 这就是 Raft Safety Property 的保证！                   │
│                                                               │
│  ★ 和 TiKV 2PC 对比：                                        │
│     Raft：**单 Leader 写 + 过半确认**（比 TiKV Percolator 2PC 简单）│
│     TiKV Percolator：2PC + Tso + Pipelined 乐观锁            │
│     → TiKV 是 **Raft 做副本复制 + Percolator 做分布式事务**  │
│     → 两层不同的一致性！                                      │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Safety Property（★ Raft 四属性，面试必背）

```
┌─────────────────────────────────────────────────────────────┐
│  三、Raft Safety 四属性（面试最高频！）                       │
│                                                               │
│  ① Election Safety（选举安全）                               │
│     → 同一 Term 最多一个 Leader                              │
│     → 保证不会两个 Leader 同时写                             │
│                                                               │
│  ② Leader Append-Only（Leader 只追加）                       │
│     → Leader 不会覆盖/删除自己的 Log 条目                    │
│     → 只会追加新条目                                          │
│                                                               │
│  ③ Log Matching（日志匹配）                                  │
│     → 如果两个节点 Log 有相同 Term + Index                    │
│     → 那这个位置及之前的 Log 完全相同                        │
│     → 保证 Follower 不会和 Leader 有冲突 Log                 │
│                                                               │
│  ④ Leader Completeness（Leader 完整性）★ 最关键！             │
│     → 如果一个 Log 被 Commit（过半确认）→ 新 Leader 必然有它  │
│     → 保证不会丢数据！                                       │
│     → 这是 Raft 和 Paxos 都要满足的核心 Safety               │
│                                                               │
│  ★ Leader Completeness 怎么保证？                            │
│     ① Follower 投票给 Candidate 时 → Compare Log             │
│     ② Candidate 必须有至少一个 Commit 条目的 Follower 投票才能当选│
│     ③ 否则即使拿到过半票也不能当 Leader                      │
│     → 新 Leader 必然包含所有已 Commit 的条目！               │
│                                                               │
│  ★ 旧 Leader 回来怎么办？                                    │
│     ① 新 Leader Term 更高 → 旧 Leader 自动降级 Follower      │
│     ② 旧 Leader 可能有 Leader 期间未 Commit 的条目           │
│     ③ 新 Leader 会 **覆盖旧 Leader 的冲突 Log 条目**          │
│       （从高 Term 到低 Term 覆盖）                           │
│     → 保证全局一致                                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Raft 工程实现（★ TiKV/ETCD 怎么实现）

### 3.1 TiKV Raft（raft-rs）

```rust
// ★ TiKV 的 Raft 实现：rust-rs/raft-rs（最主流 Rust 实现）
//
// 核心结构：
//
// pub struct RawNode {
//     pub id: u64,           // 节点 ID
//     pub raft: RaftCore,   // Raft 核心状态机
//     pub raft_log: RaftLog, // Log 存储
//     pub state: State,     // Follower/Candidate/Leader
//     pub term: u64,        // 当前 Term
//     pub vote: u64,        // 当前 Term 投给谁
//     pub commit: u64,      // CommitIndex
//     pub last_applied: u64,// 最后 Apply 的 Index
// }
//
// RaftLog（关键！TiKV 的 Log 存储）：
//   pub struct RaftLog {
//       pub entries: Vec<Entry>,     // 内存中的 Log 条目
//       pub stable: Storage,         // 持久化存储（WAL + Snapshot）
//       pub committed: u64,          // CommitIndex
//       pub applied: u64,            // AppliedIndex
//   }
//
// 工程优化（面试高频 TiKV 怎么优化 Raft）：
//
// ① 批量 Append：把多个 Log 条目一次 WAL 写入 → 减少 fsync
// ② Pipeline Append：Leader 并发复制给 Follower（不等 ACK 就发下一个）
// ③ Async Apply：CommitIndex 后异步 Apply（不阻塞下一轮写）
// ④ Snapshot 压缩：Log 太多时做 Snapshot + 丢弃旧 Log
// ⑤ PreVote：竞选前试探 → 减少 Term 膨胀
//
// ★ TiKV 写路径（Raft + Percolator）：
//   Client → PD → TiKV Leader（Raft Log + Percolator 2PC）
//     → Raft Append Log（过半确认）→ Apply Log → Percolator Commit
//     → 返回 Client
```

### 3.2 ETCD Raft（Go 实现）

```go
// ★ ETCD 的 Raft 实现：CoreOS/etcd/raft（最主流 Go 实现）
//
// 和 TiKV raft-rs 核心逻辑一样（同 Raft 论文）
//
// ETCD 工程特点：
// ① 用 bbolt 存 Log + Snapshot（嵌入式 KV，类似 RocksDB）
// ② v3 API 用 gRPC + MVCC（多版本并发控制，类似 TiDB）
// ③ Watch 机制：客户端订阅 Key 变化 → 服务端主动推送
//
// ★ ETCD vs TiKV Raft 使用差异：
//   ETCD：**Raft 是唯一一致性层**（ETCD 不做分布式事务）
//   TiKV：**Raft + Percolator 两层**（Raft 做副本 + Percolator 做事务）
//
// ★ 为什么 TiKV 要两层？
//   TiDB 是分布式关系型数据库 → 需要分布式事务（ACID）
//   → Raft 只做副本一致（不做事务）
//   → Percolator 做 2PC + Tso 时间戳（Google Spanner 简化版）
//   → 两层分工：Raft 保证数据不丢 + Percolator 保证事务一致
```

---

## 四、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Raft 三个子问题？| **① Leader Election（选主）② Log Replication（日志复制过半确认）③ Safety Property（四属性保证）**|
| 2 | Raft 四个 Safety 属性？| **① Election Safety（同 Term 最多一个 Leader）② Leader Append-Only（只追加不覆盖）③ Log Matching（同 Term+Index 之前完全一致）④ **Leader Completeness（新 Leader 必有所有已 Commit 条目）**** |
| 3 | Leader Election 流程？| Follower 心跳超时 → Term+1 → 转 Candidate → RequestVote RPC → 过半票 → 转 Leader → 广播 Heartbeat |
| 4 | CommitIndex 怎么算？| **超过半数 Follower 已复制的最大 Index** → Leader 记录每个 Follower 的 matchIndex → 排序取中位数 |
| 5 | PreVote 是什么？| 竞选前先试探性投票 → 避免网络分区恢复时无效竞选 → TiKV/ETCD 都有 |
| 6 | Raft vs 2PC vs Paxos？| Raft：简化 Paxos + Leader-Based + 工程实现；2PC：分布式事务（TiKV Percolator）；Paxos：学术版（Google Chubby）|
| 7 | TiKV Raft + Percolator 两层？| **Raft 做副本复制 + Percolator 做分布式事务**（2PC + Tso）；Raft 保证数据不丢 + Percolator 保证事务 ACID |
| 8 | Raft Pipeline Append？| Leader **并发复制**给 Follower（不等 ACK 就发下一个）→ 提高吞吐（TiKV 默认开启）|
| 9 | Raft Snapshot？| Log 太多（>10000）→ 生成 Snapshot + 丢弃旧 Log；新 Leader 从 Snapshot + 剩余 Log 恢复 |
| 10 | Raft 脑裂怎么防？| ① **Quorum**（N/2+1 节点存活才能工作）② **Term 任期**（旧 Leader 回来自动降级）③ Leader Completeness |
| 11 | Raft Log vs Kafka Log？| 类似（Append Only + Offset）；但 Raft Log **有 Term + CommitIndex**（一致性控制）；Kafka Log 无 Term（无一致性）|
| 12 | Raft 学习路径？| 读 Raft 论文 Diego Ongaro → raft-rs 源码（RawNode/RaftCore/RaftLog）→ TiKV 集成 → ETCD Go 实现 |
| 13 | Raft vs ZAB（ZK）？| ZAB：**Raft + Atomic Broadcast**（专门为 ZK 写的简化 Raft）；Raft 通用；ZAB 有 Discovery/Recovery/Broadcast 三阶段 |
| 14 | Raft 为什么不适合写密集？| 单 Leader 写（所有写都走 Leader）→ Leader 瓶颈；可以写扩展（Raft Learner / Multi-Raft）|
| 15 | TiKV Multi-Raft？| **每个 Region 一个 Raft Group**（≈96MB 数据分片）→ 整个 TiKV 有几千个 Raft Group → 并行写扩展 |
| 16 | Raft Learner？| TiKV/Etcd 支持 Learner 节点（Follower 但不参与选举/投票）→ 增加只读副本不影响 Quorum |
| 17 | Raft Read 优化？| ① ReadIndex（读走 Leader + Quorum 确认）② Lease Read（Leader 心跳内本地读，最快）|
| 18 | Raft 为什么比 Paxos 流行？| 论文 + 工程实现 + Stanford + Leader-Based 简单 → TiKV/ETCD/Prometheus 都用 Paxos 没有好工程实现 |
| 19 | Raft 存储组件？| WAL（顺序写）+ Snapshot（全量快照）+ Log Entry（增量日志）；TiKV 用 RocksDB + WAL |
| 20 | Raft 网络分区处理？| ① 少数派 Leader 停止服务（无法过半 Commit）② 多数派选新 Leader → 恢复后旧 Leader 自动降级 Follower |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV raft-rs / ETCD Go  
**面试考点：20 题**  
**差异化点**：分布式共识基石 + TiKV/ETCD/Prometheus 都用
