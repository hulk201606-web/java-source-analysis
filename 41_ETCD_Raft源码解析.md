# ETCD + Raft 协议源码深度解析（分布式共识核心）

> **源码基线**：ETCD 3.5.x（Go 实现）+ go.uber.org/raft 3.x  
> **核心仓库**：https://github.com/etcd-io/etcd / https://github.com/uber-go/raft  
> **面试权重**：⭐⭐⭐⭐⭐（分布式必问，ZAB/Paxos/Raft 三大共识）

---

## 一、Raft 协议核心（★★★★★ 面试必问）

### 1.1 Raft vs ZAB vs Paxos

```
┌─────────────────────────────────────────────────────────────┐
│  三大共识协议对比（面试高频！）                                │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐ │
│  │   协议     │    Raft       │    ZAB        │    Paxos     │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 出处      │ Stanford 2014 │ ZK 2007 项目   │ Lamport 1998 │ │
│  │ 易懂性    │ ★★★★★ 工程化  │ ★★★ 简化 Paxos │ ★★ 学术，复杂  │ │
│  │ 实现语言  │ Go/Java      │ C++/Java     │ 论文实现     │ │
│  │ 工业应用  │ ETCD/Consul   │ ZooKeeper     │ Chubby(Google)│ │
│  │           │ TiDB/CNCF    │ Dubbo/Nacos  │              │ │
│  │ 共识模型  │ 单 Leader     │ 单 Leader     │ 多 Acceptor  │ │
│  │ 日志复制  │ AppendEntries │ PROPOSE+COMMIT│ Prepare+Commit│ │
│  │ 快照压缩  │ ★ 内置        │ ★ 内置         │ 需自行实现    │ │
│  └──────────┴──────────────┴──────────────┴──────────────┘ │
│                                                               │
│  Raft 最大优势：**工程化！**                                   │
│  → 把 Paxos 拆成 3 个子问题 → 每个都简单好理解                 │
│  → 让分布式系统实现从博士论文级变成普通工程师级                │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Raft 三个子问题（★★★★★）

```
// ★ Raft 把共识拆成 3 个独立子问题（每个都是独立机制）
//
// ① Leader Election（选主）
//    谁当 Leader？→ 超时 + 投票 + Quorum
//
// ② Log Replication（日志复制）
//    Leader 收到写请求 → 复制到所有 Follower → Quorum ACK → Commit
//
// ③ Safety（安全性保证）
//    一个任期内最多一个 Leader
//    已 Commit 的日志不能丢
//    Leader 必须包含所有已 Commit 的日志（Log Matching 属性）
```

### 1.3 Leader Election 详解（★★★★★）

```
// ★ Raft 选主完整流程（简化但完整）
//
// 1. 超时触发
//    Follower 等待 electionTimeout（150-300ms，随机）
//    → 没收到 Leader 心跳 → 变成 Candidate
//
// 2. 投票
//    Candidate 给自己投票
//    向其他节点发 RequestVote RPC
//    RequestVote 参数：term, lastLogIndex, lastLogTerm
//
// 3. 投票规则（安全性保证）
//    节点收到 RequestVote：
//    if term > currentTerm → 同意投票 + 更新 term + 变成 Follower
//    if term < currentTerm → 拒绝（说自己 term 更新）
//    if 本任期已投过票 → 拒绝（只能投一个）
//    if Candidate 日志比自己新？→ 同意；否则拒绝
//      → 日志新旧比较：先比 lastLogTerm → 再比 lastLogIndex
//
// 4. 当选
//    Candidate 收到 Quorum（N/2+1）的同意票
//    → 变成 Leader → 立刻发空 AppendEntries（心跳）
//    → 阻止其他节点继续选举
//
// ★ PreVote（预投票，etcd 默认开启）：
//   分两步：PreVote（不增加 term）→ Vote（增加 term）
//   → 网络分区节点回来时不会乱增加 term（减少扰动）
//   → etcd 3.4+ 默认开启 PreVote
```

### 1.4 Log Replication 详解（★★★★★）

```
// ★ Raft 日志复制完整流程
//
// Leader 收到写请求：
//
//   ① Leader 本地 append log entry（term, index, command）
//
//   ② Leader 并行向所有 Follower 发 AppendEntries RPC
//      AppendEntries 参数：
//        term             → Leader 当前 term
//        prevLogIndex     → 要从哪个 index 开始复制
//        prevLogTerm      → prevLogIndex 处的 term（一致性检查）
//        entries[]        → 要复制的日志条目
//        leaderCommit     → Leader 已 Commit 的最高 index
//
//   ③ Follower 收到 AppendEntries
//      → 一致性检查：prevLogIndex 位置的 term 是不是 prevLogTerm？
//        ✓ 一致 → 追加新 entries → ACK 成功
//        ✗ 不一致 → 拒绝（让 Leader 回退日志再试）
//
//   ④ Leader 收到 Follower ACK
//      → 更新 nextIndex[Follower]（下次从哪开始复制）
//      → 如果 entry 已复制到 Quorum
//        → leaderCommit = entry.index
//        → 应用到 State Machine（执行 command）
//
// ★ Commit 规则：
//   一个 entry 只有在以下条件都满足时才能 Commit：
//   ① 已经复制到 Quorum Follower
//   ② 是当前 Leader 任期的 entry（不能 Commit 旧任期的 entry！）
//     → 这是 Safety 的关键！防止 Leader 切换后旧 entry 被错误 Commit
//
// ★ Leader 切换后的日志修复：
//   新 Leader 当选 → nextIndex[Follower] = leaderLastIndex + 1
//   Follower 拒绝 → Leader 递减 nextIndex 重试
//   直到 prevLogIndex/prevLogTerm 匹配 → 复制正确日志
//   → 强制覆盖 Follower 的冲突日志
```

### 1.5 Safety（安全性保证）

```
// ★ Raft 四个 Safety 属性（面试高频，证明一致性）
//
// ① Election Safety
//    一个任期（term）内最多一个 Leader
//    → 保证日志来源唯一
//
// ② Leader Append-Only
//    Leader 从不覆盖自己的日志（只追加）
//    → 保证自己的日志单调递增
//
// ③ Log Matching
//    如果两个日志有相同 index + term → 它们的内容也相同
//    （因为同一个 term 同一个 index 只会有一个 entry）
//
// ④ Leader Completeness
//    如果一个 entry 在某个 term 被 Commit → 那么它一定存在于后续 term 的 Leader 日志中
//    → 不会因为 Leader 切换丢失已 Commit 的日志
//
// ★ Safety 证明核心：
//   Leader 选主时检查 Candidate 日志是否比自己新
//   → 保证选出来的 Leader 一定有所有已 Commit 的日志
//   → Commit 规则：只 Commit 当前 term 的 entry → 防止旧 term entry 被错误 Commit
```

---

## 二、ETCD 源码核心

### 2.1 ETCD 架构

```
┌─────────────────────────────────────────────────────────────┐
│  ETCD 架构（和 ZK 类似但 Raft 协议不同）                      │
│                                                               │
│  Client（gRPC，HTTP + TLS）                                   │
│      ↓                                                        │
│  etcd Server（Go 实现）                                       │
│    ├── API Server（对外接口）                                  │
│    ├── Auth Server（用户认证）                                 │
│    ├── Raft（共识模块，核心！）                                │
│    │   └── go.uber.org/raft 库                                │
│    ├── Storage（存储模块）                                    │
│    │   ├── WAL（Write-Ahead Log）                              │
│    │   ├── Snapshot（快照）                                    │
│    │   └── Backend（BBoltDB，持久化 KV）                       │
│    └── MVCC（多版本并发控制，类似 InnoDB MVCC）                 │
│                                                               │
│  ★ 对比 ZK：                                                  │
│  ZK 用 ZAB 协议 + C++ 实现 + 默认 32MB 最大数据                 │
│  ETCD 用 Raft 协议 + Go 实现 + 默认 2GB 最大数据               │
│  ETCD 有 MVCC（历史版本查询）+ Watch（高效事件通知）           │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 RaftNode（ETCD Raft 封装）

```go
// ★ raftNode 是 ETCD 对 Raft 的封装
//   源码位置：etcdserver/raft.go

type raftNode struct {
    raft       raft.Raft           // Raft 核心对象
    raftStorage raft.Storage        // Raft 存储接口
    w          wal.WAL             // WAL 日志
    snap       raft.Snapshotter   // 快照管理
    applyc     chan apply          // 应用已 Commit 日志的通道
}

// ★ 核心循环（Ready → Advance）
//
// for {
//     rd := r.raft.Ready()  // ★ Raft 输出：这一轮该做什么
//
//     // 1. 写 WAL
//     r.wal.Save(rd.HardState, rd.Entries)
//
//     // 2. 发网络消息（给其他节点）
//     transport.Send(rd.Messages)
//
//     // 3. 应用已 Commit 的日志到 State Machine
//     for _, entry := range rd.CommittedEntries {
//         apply(entry)
//     }
//
//     // 4. 应用快照
//     if rd.Snapshot {
//         r.raftStorage.ApplySnapshot(rd.Snapshot)
//     }
//
//     // 5. 通知 Raft 这一轮处理完了
//     r.raft.Advance(rd)  // ★ 关键！告诉 Raft 这一轮已处理完
// }
//
// ★ Ready() 设计哲学（go.uber.org/raft 库）：
//   Raft 本身是纯内存状态机，**不碰磁盘不碰网络**
//   → Ready() 返回这一轮该做什么（写 WAL、发消息、应用日志）
//   → Advance() 通知处理完了
//   → 上层应用（ETCD）自己决定怎么持久化和发送
//   → 解耦！Raft 核心算法 vs 存储/网络实现
```

### 2.3 Snapshot（快照压缩）

```go
// ★ Snapshot 是 Raft 性能关键！
//   如果无限追加日志 → WAL 无限增长 → Follower 追日志越来越慢
//   → 定期做 Snapshot（快照）+ 压缩旧日志
//
// ETCD Snapshot 触发条件：
//   snapshot-count（默认 10000）→ 累积 10000 条日志做一次快照
//
// Snapshot 流程：
//   1. 遍历 BboltDB → 序列化整个 KV 状态
//   2. 生成快照文件（etcd/member/snap/db）
//   3. 记录 LastIndex + LastTerm（快照覆盖到哪）
//   4. 截断 WAL 中 index <= LastIndex 的旧日志
//
// ★ Follower 落后太多怎么办？
//   Follower 日志 index < leaderSnapshotIndex
//   → Leader 直接发完整 Snapshot（MsgSnap RPC）
//   → Follower 用 Snapshot 覆盖自己的状态
//   → 比追日志快得多！
//
// ★ WAL（Write-Ahead Log）：
//   ETCD 的 WAL 存储 Raft 日志条目
//   wal-00000000.log（追加写，类似 MySQL Redo Log）
//   每条 entry：term + index + data
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Raft vs ZAB vs Paxos？| **Raft（工程化，Stanford 2014，ETCD/Consul）→ ZAB（ZK 专用，简化 Paxos）→ Paxos（学术版，复杂，Chubby）** |
| 2 | Raft 三个子问题？| **Leader Election（选主）/ Log Replication（日志复制）/ Safety（安全性保证）** |
| 3 | Raft 选主流程？| 超时（150-300ms 随机）→ Candidate → RequestVote RPC → **先比 term，再比日志新旧（lastLogTerm + lastLogIndex）** → Quorum 同意 → 当选 Leader → 发 AppendEntries 心跳 |
| 4 | Raft 选主日志比较？| Candidate 和 Follower 日志新旧：**先比 lastLogTerm → 再比 lastLogIndex**；如果都相等或 Candidate 更新 → Follower 同意投票 |
| 5 | Raft 日志复制流程？| Leader append log → 并行 AppendEntries RPC → Follower 检查 prevLogIndex/prevLogTerm 一致性 → 追加 → ACK → **Quorum ACK 后 + 是当前 term entry** 才 Commit → 应用到 State Machine |
| 6 | Raft Commit 规则（★★★★★）？| 一个 entry **必须同时满足**：① 复制到 Quorum；② **是当前 Leader term 的 entry**（不能 Commit 旧 term 的！防止 Leader 切换后错误 Commit）|
| 7 | Raft Safety 四属性？| **Election Safety（单任期一个 Leader）/ Leader Append-Only（只追加）/ Log Matching（同 index+term 内容相同）/ Leader Completeness（新 Leader 一定有所有已 Commit 日志）**|
| 8 | PreVote 是什么？| **预投票**（etcd 默认开启）→ 分两步：PreVote（不增 term）→ Vote（增 term）；网络分区节点回来不会乱增 term → 减少集群扰动 |
| 9 | Raft Ready() 设计？| Raft 核心是**纯内存状态机**（不碰磁盘/网络）→ Ready() 输出"这一轮该做什么"（写 WAL/发消息/应用日志）→ Advance() 通知处理完 → **解耦算法和 IO** |
| 10 | Snapshot 快照作用？| **定期压缩旧 Raft 日志**（snapshot-count 默认 10000）→ 防止 WAL 无限增长；Follower 日志落后太多 → Leader 直接发完整 Snapshot |
| 11 | ETCD vs ZK？| ETCD：**Raft 协议 + Go 实现 + MVCC 多版本查询 + Watch**；ZK：ZAB 协议 + C++ 实现 + 最多 32MB 数据 |
| 12 | ETCD WAL 作用？| **Write-Ahead Log**（Raft 日志持久化）→ 类似 MySQL Redo Log；崩溃恢复时回放 WAL 恢复 Raft 状态 |
| 13 | ETCD Backend 存储？| **BBoltDB**（嵌入式 KV 数据库，B+Tree）→ 持久化存储最终数据；Raft WAL 存日志 → Commit 后写入 BboltDB |
| 14 | ETCD MVCC？| 每个 KV 保存多版本（按 revision 递增）→ 支持 Watch（某 key 变化时通知）+ 历史版本查询 |
| 15 | Raft 和分布式事务关系？| Raft 是**共识协议**（保证多个节点数据一致）；分布式事务是**业务层语义**（2PC/3PC/TCC/Saga）；Raft 可以保证事务日志在多节点一致 |
| 16 | ETCD Lease 机制？| **租约**：客户端租一个 key（默认 TTL 30s）→ 续租保持 → TTL 到期自动删除 → 类似 ZK 临时节点 |
| 17 | ETCD 分布式锁？| `etcd/client/v3/concurrency.Lock` → 基于 Lease + Campaign（竞争锁）→ Lock/Unlock API |
| 18 | Raft 为什么要随机超时？| 防止多个节点同时超时选举 → 随机（150-300ms）→ 时间差保证大多数情况只有一个先变成 Candidate |
| 19 | ETCD Watch 实现？| MVCC revision 递增 → Watch 指定某个 revision → 后续所有 revision 大于它的变化都通知 |
| 20 | Raft 和 K8s？| K8s 的 etcd 是**唯一数据源**（Pod/Service/Deployment 全部存 etcd）→ Raft 保证集群数据强一致 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：ETCD 3.5.x + go.uber.org/raft 3.x  
**面试考点：20 题**
