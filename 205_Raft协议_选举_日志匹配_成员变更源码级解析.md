# Raft 协议核心选举 + 日志匹配 + 成员变更源码级深度解析

> **源码基线**：etcd 3.5+（Go）/ TiKV（Rust）  
> **核心位置**：etcd/raft/ + TiKV/components/raftstore/  
> **面试权重**：⭐⭐⭐⭐⭐（分布式系统基石，最高频面试考点）

---

## 一、Raft 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Raft = 工业界最广泛使用的 CP 共识协议                      │
│                                                               │
│  为什么 Raft 重要（面试必背）：                                │
│   TiKV / etcd / KRaft / MinIO / CockroachDB 都用 Raft        │
│   → 分布式存储、协调服务、元数据管理的基石                      │
│                                                               │
│  ★ Raft 四大核心（面试高频）：                                │
│   1. Leader Election（选举）—— 谁当 Leader                    │
│   2. Log Replication（日志复制）—— Leader 写日志 → Follower 同步│
│   3. Log Matching（日志匹配）—— Follower 日志和 Leader 一致    │
│   4. Membership Change（成员变更）—— 动态增删节点              │
│                                                               │
│  Raft 为什么成功？                                             │
│   比 Paxos 简洁得多（可实现 + 可理解）                        │
│   Leader 强中心化 → 解决一切协调问题                           │
│   日志复制 + 成员变更都有清晰流程                               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Leader Election 源码级

### 2.1 选举流程

```go
// ★ Raft Leader Election 源码级（etcd/raft）
//
// etcd/raft/raft.go
// etcd/raft/node.go
//
// // Raft 三种状态（面试必背）：
// //   Follower → Candidate → Leader
// //
// // ★ 选举触发条件：
// //   Follower 一段时间没收到 Leader 的心跳（electionTimeout）
// //   → 变成 Candidate → 发起选举
//
// func (r *Raft) tickElection() {
//     // 心跳超时
//     if r.state == StateFollower {
//         r.becomeCandidate()
//         // 给所有 Peer 发 RequestVote RPC
//         r.sendRequestVote()
//     }
// }
//
// func (r *Raft) becomeCandidate() {
//     r.state = StateCandidate
//     r.currentTerm++         // ★ Term 递增（防止旧 Leader 复活）
//     r.votedFor = r.id       // 投自己一票
//     r.votes = map[r.id]     // 统计投票
// }
//
// // ★ RequestVote RPC 处理（源码级）：
// func (r *Raft) handleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
//     // 规则 1：请求的 Term ≥ 我的 Term → 更新我的 Term + 降级为 Follower
//     if req.Term > r.currentTerm {
//         r.currentTerm = req.Term
//         r.becomeFollower()
//     }
//
//     // 规则 2：还没投过票 或 之前投的就是这个候选人
//     // && 候选人的日志 ≥ 我的日志（不能选日志落后的当 Leader）
//     if (r.votedFor == 0 || r.votedFor == req.CandidateId) &&
//         r.log.isUpToDate(req.LastLogIndex, req.LastLogTerm) {
//         r.votedFor = req.CandidateId
//         return &pb.RequestVoteResponse{VoteGranted: true}
//     }
//     return &pb.RequestVoteResponse{VoteGranted: false}
// }
//
// // ★ isUpToDate 核心（面试高频）：
// //   候选者的日志必须 ≥ 投票者的日志
// //   选日志最新的当 Leader → 保证不丢数据
// func (l *raftLog) isUpToDate(lasti, term uint64) bool {
//     myLastIndex, myLastTerm := l.lastIndexAndTerm()
//     if term != myLastTerm {
//         return term > myLastTerm
//     }
//     return lasti >= myLastIndex   // Term 相同比 Index
// }
//
// // ★ 赢得选举：
// //   Candidate 收到过半 RequestVoteResponse(VoteGranted=true)
// //   → becomeLeader()
// //   → 开始给所有 Follower 发心跳（AppendEntries 空请求）
//
```

### 2.2 Term 防脑裂 + 选举超时

```go
// ★ Raft Term 防脑裂源码级（面试核心）
//
// // Term 是什么？
// //   一个永不递减的计数器
// //   每次选举 +1
// //   每个 Term 最多一个 Leader
// //
// // ★ Term 如何防脑裂？
// //
// // 场景：3 节点 [A, B, C] 网络分区 → [A] 和 [B, C]
// //
// // 1. [B, C] 能过半（2/3）→ B 当选 Leader，Term=5
// // 2. [A] 不能过半（1/3）→ A 一直 Candidate，Term=5（但赢不了）
// // 3. 网络恢复 → A 收到 B 的 AppendEntries(Term=5)
// //    → A.currentTerm=5 ≤ B.Term → A 降级为 Follower
// //    → A 之前的 Leader 身份作废（脑裂自动修复）
// //
// // ★ 选举超时（electionTimeout）：
// //   Follower 心跳多久没收到才发起选举？
// //   标准：150ms - 300ms 随机
// //   // 随机的目的：多个 Follower 不会同时变成 Candidate（避免死锁）
// //   // 如果两个同时发起 → 都拿不到过半票 → 下一次随机超时再试
// //
// // ★ 心跳间隔（heartbeatInterval）：
// //   Leader 多久发一次心跳？
// //   标准：electionTimeout / 2（保证超时前至少两次心跳）
//
```

---

## 三、日志复制 + 匹配源码级

### 3.1 AppendEntries 日志复制

```go
// ★ Raft AppendEntries 日志复制源码级（面试核心）
//
// etcd/raft/raft.go → AppendEntries()
//
// // Leader 收到写请求：
// // 1. 追加到本地 Log（新 Entry，Index=len+1，Term=currentTerm）
// // 2. 给所有 Follower 发 AppendEntries RPC
// // 3. 等过半 Follower ACK
// // 4. 过半 ACK → Commit → 应用到状态机
// // 5. 给客户端返回 OK
//
// // ★ AppendEntries RPC 请求格式（简化）：
// type AppendEntriesRequest struct {
//     Term              uint64      // Leader Term
//     LeaderId          uint64      // Leader ID
//     PrevLogIndex      uint64      // 上一条日志 Index（匹配点）
//     PrevLogTerm       uint64      // 上一条日志 Term（匹配点）
//     Entries           []LogEntry  // 要复制的日志（可能为空=心跳）
//     LeaderCommit      uint64      // Leader 已 Commit 的最高 Index
// }
//
// // ★ Follower 处理 AppendEntries 源码级（核心！）：
// func (r *Raft) handleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
//     // 规则 1：Term 落后 → 降级为 Follower
//     if req.Term < r.currentTerm {
//         return &pb.AppendEntriesResponse{Success: false, Term: r.currentTerm}
//     }
//     r.currentTerm = req.Term
//
//     // ★ 规则 2：日志匹配（核心！面试高频）
//     //   Follower 的 PrevLogIndex 位置的 Term 必须等于 req.PrevLogTerm
//     //   否则拒绝 → Leader 会往前回溯找匹配点
//     if !r.log.matchTerm(req.PrevLogIndex, req.PrevLogTerm) {
//         // ★ 拒绝 + 返回 NextIndex（让 Leader 往前追）
//         // NextIndex = Follower 的最后一个不匹配的 Index
//         nextIndex := r.log.findConflict(req.PrevLogIndex, req.PrevLogTerm)
//         return &pb.AppendEntriesResponse{Success: false, Term: r.currentTerm, NextIndex: nextIndex}
//     }
//
//     // 规则 3：日志匹配 → 追加新 Entry（覆盖冲突的）
//     for i, entry := range req.Entries {
//         if !r.log.entries[req.PrevLogIndex+1+i].isEqual(entry) {
//             // ★ 冲突 → 删除这个及之后的 → 用 Leader 的替换
//             r.log.truncate(req.PrevLogIndex + 1 + i)
//             r.log.append(entry)
//         }
//     }
//
//     // 规则 4：推进 Follower CommitIndex（LeaderCommit）
//     if req.LeaderCommit > r.commitIndex {
//         r.commitIndex = min(req.LeaderCommit, r.log.lastIndex())
//     }
//
//     return &pb.AppendEntriesResponse{Success: true}
// }
//
```

### 3.2 日志匹配 + NextIndex 回溯

```go
// ★ Raft NextIndex 回溯源码级（面试高频，Leader 怎么追落后 Follower）
//
// // Leader 维护每个 Follower 的 NextIndex 和 MatchIndex：
// //   NextIndex：下一个要发的日志 Index
// //   MatchIndex：已经确认复制成功的最高 Index
//
// // 正常情况：
// //   Leader 发 AppendEntries(PrevLogIndex=NextIndex-1, Entries=[NextIndex...])
// //   Follower 匹配成功 → NextIndex++
//
// // ★ Follower 日志落后（或 Term 不一致）：
// //   Follower 返回 Success=false + NextIndex=冲突点前
// //
// //   // Leader 收到后：
// //   //   NextIndex = response.NextIndex  // 往前跳
// //   //   下次 AppendEntries(PrevLogIndex=NextIndex-1, ...)
// //   //   直到 Follower Success=true
// //
// // 最坏情况（每次只跳过一条）：
// //   Leader 1000 条日志；Follower 只有 10 条
// //   → 最坏要 990 次 AppendEntries 才能追上
// //   → ★ 生产优化：Follower 返回 NextIndex 时返回冲突 Term 的第一条 Index
// //     Leader 可以一次跳到那个位置（而不是每次 -1）
//
// // ★ Commit 规则（面试陷阱！）：
// //   Leader 只能 Commit **当前 Term** 的日志
// //   之前 Term 的日志靠日志匹配隐式 Commit
// //   // 为什么？防止 Leader 切换导致之前 Term 的日志丢失
// //
// //   例子：
// //   Term 5 Leader A Commit 了 Index=100（Term=5）
// //   然后宕机 → Term 6 Leader B 当选
// //   B 收到 AppendEntries → Follower 匹配成功（Term 5 的 Index=100 在所有 Follower 上都有）
// //   B 可以安全把 Index=100 也 Commit 了
// //   // 这叫 "日志匹配隐式 Commit"
//
```

---

## 四、成员变更源码级

```go
// ★ Raft 成员变更（Membership Change）源码级
//
// etcd/raft/confchange/
// etcd/raft/raft.go → applyConfChange()
//
// // 问题：
// //   3 节点 [A, B, C] → 想加节点 D
// //   如果直接从 [A,B,C] 变成 [A,B,C,D]
// //   在变更中间可能出现两个多数派（[A,B] 和 [C,D]）→ 双 Leader
// //
// // ★ Raft 解决：Joint Consensus（两阶段变更）
// //
// // Phase 1：进入 Joint 状态
// //   配置从 [A,B,C] 变成 [A,B,C,D]（新旧两个都有效）
// //   → 投票/日志复制需要 **新旧配置都过半**（2/3 AND 2/4）
// //   → 不会出现两个独立多数派！
// //
// // Phase 2：提交 Joint → 进入新配置
// //   配置从 [A,B,C,D] 变成 [A,B,C,D]（只有新的）
// //   → 投票/日志复制只需要新配置过半（3/4）
// //
// // ★ 源码级流程：
// func (r *Raft) applyConfChange(cc pb.ConfChangeI) {
//     // Step 1: 旧配置过半同意 → 进入 Joint
//     //   r.applyConfChangeV1(cc)  // 旧版单阶段（etcd 3.0 以前，危险）
//     //   r.applyJoint(cc)          // 新版 Joint Consensus
//     //
//     //   Joint 配置：
//     //   voters = [A, B, C]  AND  votersOut = [A, B, C, D]
//     //   // 两个配置都要过半
//     //
//     // Step 2: Joint 配置过半同意 → 进入新配置
//     //   r.leaveJoint()
//     //   voters = [A, B, C, D]
//     //
//     // ★ 任何阶段出错 → 回滚到旧配置（安全）
// }
//
// // ★ Joint Consensus 为什么安全？
// //   Joint 状态要求两个配置都过半
// //   → 任何子集不可能同时在新旧配置里都是多数派
// //   → 不会出现双 Leader
// //
// // ★ 面试陷阱：
// //   说 "Raft 成员变更直接改就行" → ❌ 旧版 v1 单阶段变更会导致双 Leader
// //   新版必须用 Joint Consensus（两阶段）
//
```

---

## 五、Raft vs Paxos vs ZAB 最终对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试终极考点：三大 CP 协议完整对比                          │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ Raft              │ Paxos         │ ZAB      ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 核心思想         │ Leader 强中心化   │ Proposer/    │ 两阶段   ││
│  │                │ 日志复制          │ Acceptor     │ 广播     ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 选举            │ ★ 简单 Term+超时 │ Multi-Leader │ Zxid 最大││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 日志复制         │ ★ AppendEntries │ Accept/Learn  │ Prepare/ ││
│  │                │ 隐式 Commit      │              │ Commit   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 成员变更         │ ★ Joint 两阶段  │ ❌ 复杂       │ ❌ 静态   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 日志匹配         │ ★ Term+Index    │ 无日志概念    │ Zxid 匹配││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 实现复杂度       │ ★ 低            │ ★ 高          │ 中        ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 工程生态         │ ★ 最广          │ 有限          │ ZK 独有   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 工业应用         │ TiKV/etcd/KRaft │ 理论参考      │ ZooKeeper││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频         │ ★★★★★          │ ★★★★         │ ★★★★★    ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   分布式存储 → **Raft**（简洁 + 成员变更 + 日志复制完善）     │
│   理论研究 → Paxos（理解共识本质）                            │
│   ZK 场景（Watch + 协调） → ZAB（ZooKeeper 原生）            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Raft 三种状态？| **Follower → Candidate → Leader** |
| 2 | ★ Raft 选举触发条件？| Follower **electionTimeout（150-300ms 随机）** 没收到心跳 → 变成 Candidate |
| 3 | RequestVote 投票规则？| Term ≥ 我的 Term + 没投过票 + **候选者日志 ≥ 我的日志**（isUpToDate）|
| 4 | ★ Term 防脑裂核心？| Term 永不递减；每个 Term 最多一个 Leader；旧 Leader 复活后 Term 落后 → 自动降级为 Follower |
| 5 | 选举超时为什么随机？| **避免多个 Follower 同时发起选举（避免死锁）** |
| 6 | ★ AppendEntries 日志匹配核心？| Follower 的 PrevLogIndex 位置的 Term 必须等于 PrevLogTerm；不匹配 → Leader 回溯找匹配点 |
| 7 | NextIndex 回溯怎么工作？| Leader 维护每个 Follower 的 NextIndex/ MatchIndex；Follower 拒绝 → 返回冲突点 → Leader 跳过到冲突点前 |
| 8 | ★ Raft Commit 规则（面试陷阱）？| **Leader 只能 Commit 当前 Term 的日志**；之前 Term 的靠日志匹配隐式 Commit |
| 9 | 日志匹配隐式 Commit 是什么？| 新 Leader 当选后，如果发现上一 Term 的日志已经在所有节点上匹配 → 可以安全 Commit |
| 10 | ★ Raft 成员变更核心？| **Joint Consensus 两阶段**：Phase 1 Joint（新旧配置都要过半）→ Phase 2 提交新配置 |
| 11 | Joint Consensus 为什么安全？| 新旧两个配置都要过半才能工作 → **不可能出现两个独立多数派** |
| 12 | ★ Raft vs Paxos vs ZAB 三协议对比？| 见第五节对比表（10 维度）|
| 13 | 面试最高频考点排序？| **1. Term 防脑裂 2. AppendEntries 日志匹配 3. Commit 规则陷阱 4. Joint Consensus 成员变更 5. 三协议对比** |
| 14 | 学习路径？| 选举 → Term → AppendEntries → 日志匹配 → Commit 规则 → 成员变更 |
| 15 | 面试常见追问？| Raft 为什么叫 Raft？| Stanford 2014 年论文《In Search of an Understandable Consensus Algorithm》作者 Diego Ongaro 的 PhD 工作 |
| 16 | 面试常见追问？| etcd 和 TiKV 的 Raft 实现一样吗？| 核心算法一样；etcd 是 Go；TiKV 是 Rust；都支持 Joint Consensus |
| 17 | 面试陷阱？| 说 "Raft Leader 可以 Commit 任何 Term 的日志" → ❌ **只能 Commit 当前 Term**；之前 Term 的靠隐式 Commit |
| 18 | 面试陷阱？| 说 "Raft 成员变更可以直接改配置" → ❌ 必须 **Joint Consensus 两阶段**；旧版 v1 单阶段会双 Leader |
| 19 | Raft 过半规则？| 总节点 N → 过半 = N/2+1；Joint 状态需要新旧两个配置都过半 |
| 20 | Raft 快照机制？| Log 定期快照（Snapshot）→ 截断旧 Log → 新 Follower 先加载 Snapshot 再追赶增量 |
| 21 | Raft 读路径优化？| ReadIndex（Leader 读 + 确认自己还是 Leader）；Linearizable Read（ZAB 没有这个优化）|
| 22 | Raft 写路径延迟？| 2×RTT（Leader 写本地 + 广播 Follower + 过半 ACK + Commit + 应用）；约 10-50ms |
| 23 | TiKV Raft 和 etcd Raft 差异？| TiKV 是多 Raft（每个 Region 一个）+ Multi-Raft Group；etcd 是单 Raft Group |
| 24 | Raft Learner 是什么？| 不参与投票、不影响 Quorum 的 Follower；TiFlash/Pulsar BookKeeper 用 |
| 25 | Raft PreVote 机制？| 正式选举前先发 PreVote 探测；避免网络分区恢复后频繁选举（etcd/Raft 库支持）|
| 26 | Raft Transfer Leadership？| Leader 主动把权力转给某个 Follower；先让目标 Follower 的日志 MatchIndex 追上 → 发起 Transfer |
| 27 | 典型生产用法？| TiKV：每个 Region 3 Raft（Leader+2Follower）+ Learner（TiFlash）；etcd：单 Group 3/5 节点 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：etcd/raft/raft.go + raft_log.go + confchange/ + TiKV raftstore/  
**面试考点：27 题**  
**差异化点**：Term 防脑裂机制、AppendEntries 日志匹配 + NextIndex 回溯、**Commit 只能当前 Term（面试陷阱）**、Joint Consensus 两阶段成员变更、**Raft vs Paxos vs ZAB 三协议 10 维度对比**
