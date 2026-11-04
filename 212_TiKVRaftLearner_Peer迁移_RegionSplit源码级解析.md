# TiKV Raft Learner + Peer 迁移 + Region Split 源码级深度解析

> **源码基线**：TiKV 7.x/8.0（Rust）+ PD  
> **核心位置**：components/raftstore/ + src/raftstore/ + tidb/schedulers  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 分布式核心，Raft 深度补全）

---

## 一、TiKV Region + Raft 架构全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV = 海量 Region + Raft 副本 + PD 调度                 │
│                                                               │
│  核心架构（面试必背）：                                        │
│                                                               │
│  TiDB Server                                                  │
│      │ 按 Key Hash 定位 Region                                │
│      ▼                                                        │
│  ┌─────────────────────────────────────────────────────┐     │
│  │ TiKV Cluster（多个 TiKV 节点）                         │     │
│  │                                                        │     │
│  │   Region 1 (Key ["" , "a"))  ── Raft ── 3副本+Learner  │     │
│  │   Region 2 (Key ["a", "m"))  ── Raft ── 3副本+Learner  │     │
│  │   Region 3 (Key ["m", "z"))  ── Raft ── 3副本+Learner  │     │
│  │   ...（千上万个 Region）                                │     │
│  └─────────────────────────────────────────────────────┘     │
│      ▲                                                         │
│      │ PD Scheduler（调度核心）                               │
│      └─ Peer 迁移 + Region Split + 负载均衡                    │
│                                                               │
│  ★ Region 是什么？                                            │
│   TiKV 数据按 Key 范围切成很多 Region（默认 96MB）             │
│   每个 Region 独立 Raft Group（Leader + 2 Follower + Learner）│
│   → 并行处理请求（横向扩展）                                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Raft Learner 源码级

### 2.1 Learner vs Follower

```rust
// ★ TiKV Raft Learner 源码级（面试核心，生产高频）
//
// components/raftstore/src/store/peer.rs
// src/raftstore/peer_storage.rs
//
// // TiKV Region 默认副本：
// //   3 Voter（1 Leader + 2 Follower）+ 1 Learner（TiFlash）
// //
// // ★ Learner 和 Follower 核心区别（面试必背）：
// //
// // 1. 投票权：
// //    Follower → **参与 Leader 选举投票**
// //    Learner → ❌ **不投票，不影响 Raft Quorum**
// //
// // 2. Log 同步：
// //    Follower → 和 Leader 实时同步 AppendEntries
// //    Learner → 也同步 AppendEntries（但不影响 Leader Commit）
// //    // Learner 同步 Raft Log → Apply → TiFlash Delta Tree 更新
// //
// // 3. 故障影响：
// //    Follower 挂了 → Leader 仍需过半确认；但 ISR 缩小
// //    Learner 挂了 → **完全不影响 Raft Quorum**
// //    // Learner 就是为了"读副本不影响写性能"
// //
// // ★ TiFlash 作为 Learner 的优势（面试高频）：
// //   TiFlash 作为 Learner → TiKV 写性能不受影响
// //   → TiFlash 实时同步（Raft Log 复制，毫秒级）
// //   → 列存引擎 → MPP 并行计算
// //   → 一套数据同时服务 OLTP（TiKV）+ OLAP（TiFlash）
//
// // ★ Learner 提升为 Voter（面试考点）：
// //   场景：某个 TiKV Follower 挂了 → 要把 Learner 提升为 Voter
// //   // PD Scheduler 发起：
// //   //   1. 先给 Learner 加 Voter 权限（新 Follower 加入）
// //   //   2. 旧 Follower 从 Voter 集合移除（如果需要）
// //   //   → Joint Consensus 两阶段（Raft 成员变更）
//
```

---

## 三、Peer 迁移源码级

### 3.1 PD Scheduler 调度

```rust
// ★ TiKV Peer 迁移源码级（面试核心！）
//
// tidb/pkg/scheduler/ + components/pd/
// tidb/pkg/scheduler/schedulers/balance_leader.go
// tidb/pkg/scheduler/schedulers/balance_region.go
//
// // PD Scheduler 做什么？
// //   周期性检查 TiKV 集群状态 → 发起调度操作让集群更均衡
// //
// // ★ 三种调度目标（面试必背）：
// //
// // 1. Balance Leader（Leader 均衡）：
// //    每个 TiKV 节点的 Leader Region 数尽量平均
// //    // 均衡写入压力（Leader 承载所有写请求）
// //    // 操作：transfer leader
// //
// // 2. Balance Region（Region 数量均衡）：
// //    每个 TiKV 节点的 Region 数量尽量平均
// //    // 均衡存储和读取压力
// //    // 操作：add peer / remove peer
// //
// // 3. Balance Region Size（数据量均衡）：
// //    每个 TiKV 节点的数据量尽量平均
// //    // 大 Region 迁移后 Split（见第四节）
// //
// // ★ Peer 迁移完整流程（源码级，面试高频）：
// //
// // 场景：Region X 当前在 TiKV-A、TiKV-B、TiKV-C（3 Voter）
// //   PD 决定把一个 Peer 从 TiKV-A 迁到 TiKV-D（新节点）
// //
// // Step 1: TiKV-A 给 Region X 加 Learner Peer（TiKV-D）
// //   AdminRequest → AddPeer(TiKV-D, Learner)
// //   Raft Leader 把 TiKV-D 加入 Learner 列表
// //   TiKV-D 开始同步 Raft Log
// //
// // Step 2: 等 TiKV-D 的 Raft Log 追上 Leader（log index 差距 < threshold）
// //   PD 检查 TiKV-D 的 match_index → 追上了才继续
// //
// // Step 3: TiKV-D 从 Learner 升级为 Voter
// //   AdminRequest → ChangePeer(TiKV-D, Voter)
// //   → Raft 成员变更（Joint Consensus 两阶段）
// //   → 新配置生效：[A(V), B(V), C(V), D(V)]
// //
// // Step 4: TiKV-A 从 Voter 集合移除
// //   AdminRequest → ChangePeer(TiKV-A, Remove)
// //   → Joint Consensus → 新配置生效：[B(V), C(V), D(V)]
// //
// // ★ 为什么先加 Learner 再升级？
// //   直接加 Voter → Raft Quorum 临时变成 4 Voter → 过半需要 3 个
// //   如果 TiKV-D 还没追上 → TiKV-D 加入后可能选它当 Leader（但它数据旧）
// //   → Learner 先追上再升级 → 保证 Leader 始终是数据最新的
//
```

---

## 四、Region Split + Merge 源码级

### 4.1 Region Split 流程

```rust
// ★ TiKV Region Split 源码级（面试核心）
//
// src/raftstore/peer.rs
// components/raftstore/src/store/split.rs
//
// // Region 什么时候 Split？
// //   Region size > pd-server.region-max-size（默认 144MB）
// //   Region 内 Keys 数量太多
// //   PD 判断这个 Region 热（读/写请求多）
// //   // Split 后两个小 Region 并行处理 → 负载减轻
// //
// // ★ Region Split 源码级流程（面试高频）：
// //
// // 1. PD 给 Region Leader 发 AdminRequest（Split）
// //    AdminRequest → SplitRegion
// //    // 带上 Split Key（按中间位置，或按统计信息找热点）
// //
// // 2. Region Leader 执行 Split（本地快照 + 新 Region）：
// //
// //    a. Region R1（Split 前）：Key Range [k1, k5]
// //       Split Key = k3
// //
// //    b. 生成 Region R1'（左半）：Key Range [k1, k3)
// //       Region Peer 不变；Raft Group 还是原来的
// //       // 物理数据没动！只是 Key Range 改了
// //
// //    c. 生成 Region R2（右半）：Key Range [k3, k5]
// //       新 Region ID；新 Raft Group
// //       // Raft Log 从 Leader 复制给 R2 的 Peer
// //       // 或用 Snapshot 快速复制
// //
// //    d. 两个 Region 同时生效（原子性保证）
// //       → PD 更新 Region 元数据
// //       → TiDB Server 路由到新 Region
// //
// // 3. Split 后触发数据 Compact（后台异步）：
// //    R1' 数据应该是 [k1, k3)，实际 RocksDB 里还是 [k1, k5]
// //    → Compact 时把 [k3, k5] 的数据从 R1' 的 RocksDB 里清理
// //    → R2 的 Peer 独立 RocksDB（只有 [k3, k5] 数据）
// //    // Compact 后台做，不阻塞前台请求
// //
// // ★ Region Merge（反向操作）：
// //   两个相邻 Region 都很小 → 合并成一个
// //   流程反过来：先加 Learner → 升级 Voter → 合并 Raft Log → 删旧 Region
//
```

---

## 五、TiKV Raft vs 标准 Raft 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：TiKV Raft vs 标准 Raft 核心差异                  │
│  ┌──────────────┬──────────────────┬──────────────────┐     │
│  │ 维度            │ TiKV Raft          │ 标准 Raft（etcd）  │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Region 数量       │ ★ 成千上万（每个 Region 一个 Raft）│ 1 个或几个│     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Learner 用途     │ ★ TiFlash 同步    │ 少见（etcd 用 Learner 热备）│  │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Split/Merge     │ ★ 自动触发         │ ❌ 静态 Region    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 快照策略         │ ★ 手动 + 按需      │ 定期自动           │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ PD 调度          │ ★ 外部 PD Scheduler│ ❌ 无外部调度     │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 成员变更         │ ★ 先加 Learner 再升级│ 直接 Joint Consensus│   │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 存储引擎         │ ★ Percolator MVCC  │ 简单 KV           │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 面试高频         │ ★★★★★          │ ★★★★            │     │
│  └──────────────┴──────────────────┴──────────────────┘     │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   分布式 KV/HTAP → **TiKV Raft**（多 Region + Learner + Split/Merge）│
│   元数据/协调服务 → etcd Raft（简单 + 稳定）                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV Region 默认副本？| **3 Voter（1 L+2 F）+ 1 Learner（TiFlash）** |
| 2 | ★ Learner vs Follower 核心区别？| **Learner 不投票、不影响 Raft Quorum；Follower 投票、影响 Quorum** |
| 3 | Learner 对写性能影响？| **完全不影响！** Learner 不参与过半确认 → 写只等 Voter ACK |
| 4 | ★ Peer 迁移四步骤？| **① 加 Learner → ② 等追上 → ③ 升 Voter → ④ 删旧 Voter** |
| 5 | 为什么先加 Learner 再升级 Voter？| 直接加 Voter 可能选到数据旧的当 Leader；Learner 先追上再升级保证 Leader 数据最新 |
| 6 | PD Scheduler 三种调度目标？| **Balance Leader（Leader 均衡）+ Balance Region（Region 数量均衡）+ Balance Region Size（数据量均衡）** |
| 7 | ★ Region Split 触发条件？| Region size > region-max-size（默认 144MB）或热点 |
| 8 | Region Split 源码级流程？| PD 发 Split Request → Leader 按 Split Key 分成左右两个 Region → 同时生效（原子性）→ 后台 Compact 清理 |
| 9 | Region Split 数据真正清理什么时候？| **后台 Compact**；Split 只是改 Key Range，物理数据还在；Compact 后才真正分离 |
| 10 | Region Merge 什么时候触发？| 两个相邻 Region 都小于 region-merge-size（默认 20MB）→ 合并 |
| 11 | Split Key 怎么选？| PD 按 Region 中间位置；或按统计信息找热点（避免热点全在一边）|
| 12 | TiKV Raft vs 标准 Raft 核心差异？| 见第五节对比表（7 维度）|
| 13 | 面试最高频考点排序？| **1. Learner 不投票不影响 Quorum 2. Peer 迁移四步骤 3. Region Split 流程 4. PD Scheduler 目标 5. TiKV vs 标准 Raft** |
| 14 | 学习路径？| Region → Learner → Peer 迁移 → Split → Merge → TiKV vs 标准 Raft |
| 15 | 面试常见追问？| Region Split 会阻塞请求吗？| **不会！** Split 只是 Key Range 逻辑变更；物理数据不动；两个新 Region 立刻可用 |
| 16 | 面试常见追问？| Learner 可以升 Voter 当 Leader 吗？| Learner 先升 Voter（需要 Joint Consensus）→ 才能参与 Leader 选举 → 数据最新的才会当选 |
| 17 | 面试陷阱？| 说 "TiKV Split 和 Raft Split 一样" → ❌ TiKV Split 是**数据层面的 Region 拆分**；标准 Raft 没有 Split 概念；TiKV 多 Region = 多 Raft Group |
| 18 | 面试陷阱？| 说 "PD Scheduler 直接改 TiKV 数据" → ❌ PD 只发调度请求（AdminRequest）；TiKV Raft Group 自己做 Split/Merge/Member Change |
| 19 | Raft Learner 日志同步延迟？| 和 Follower 一样（AppendEntries 实时同步）；TiFlash Learner 延迟约 **100ms - 1s** |
| 20 | TiKV Region 默认大小？| region-max-size=144MB；region-split-size=96MB（Split 阈值）|
| 21 | Region Split 后 Compact 是什么？| RocksDB Compact 后台清理旧 Region 范围的数据 → 真正释放磁盘空间 |
| 22 | 典型生产配置？| **pd-server.region-max-size=144MB + 3 Voter + TiFlash Learner + auto-split-regions=true** |
| 23 | TiFlash 挂了影响 TiKV 吗？| **完全不影响！** TiFlash 是 Learner → 不参与投票 → TiKV Raft Quorum 不受影响 |
| 24 | PD 和 TiKV 通信协议？| gRPC；PD 作为 Raft Client 发 AdminRequest（Split/ChangePeer/TransferLeader）|
| 25 | 一个 TiKV 节点能承载多少 Region？| 数千到数万个（取决于 CPU/内存/磁盘）；每个 Region Raft 状态机约 1KB-10KB 内存 |
| 26 | TiKV Multi-Raft Group 优化？| 每个 TiKV 节点承载数千个 Raft Group → 用异步 Batch 优化 Raft 消息处理 → 减少 RPC 次数 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：components/raftstore/src/store/peer.rs + src/raftstore/split.rs + tidb/pkg/scheduler/  
**面试考点：26 题**  
**差异化点**：Learner 不投票不影响 Quorum、Peer 迁移四步骤（先 Learner 再升级）、Region Split 逻辑拆分+后台 Compact、PD Scheduler 三目标、**TiKV Raft vs 标准 Raft 7 维度对比**
