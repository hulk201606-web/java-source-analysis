# TiKV Raft Group + Region Split/Merge 源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）  
> **核心仓库**：tikv/tikv → components/raftstore/ + src/raft/  
> **面试权重**：⭐⭐⭐⭐⭐（TiKV 核心，Raft 工程实现高频必问）

---

## 一、TiKV Raft Group 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV 每个 Region = 一个 Raft Group                      │
│                                                               │
│  TiKV 核心数据组织：                                           │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Table A (100GB)                                       │   │
│  │ 按 RowID Hash 分成多个 Region（每个 Region ~200MB）     │   │
│  │                                                         │   │
│  │ Region 1: [0x0000, 0x1000)                             │   │
│  │ Region 2: [0x1000, 0x2000)   ← 每个 Region 一个 Raft Group│
│  │ Region 3: [0x2000, 0x3000)                             │   │
│  │ ...（成千上万个 Region）                                │   │
│  └───────────────────────────────────────────────────────┘   │
│
│  ★ 每个 Raft Group 结构（面试必背！）：                       │
│   3 Peer 副本（Raft 协议要求奇数）                           │
│   Leader：接受读写请求                                        │
│   Follower：同步 Leader 日志 + 投票                           │
│   Learner：只同步不投票（TiKV 3.x+）                          │
│
│  ★ TiKV Raft Group vs Kafka Partition vs Redis Cluster：     │
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │            │ TiKV Raft    │ Kafka Partition│ Redis Cluster│   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 数据组织    │ Region 连续 Key│ Partition 分区 │ Hash Slot  │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 副本同步    │ Raft（CP）   │ ISR（最终）   │ 异步复制      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 分裂机制    │ Split 自动   │ Partition 固定│ Slot 手动迁移  │   │
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Raft Group 核心数据结构

### 2.1 Peer + RaftLog

```rust
// ★ TiKV Raft Group 核心数据结构（源码级）
//
// components/raftstore/src/region.rs
//
// pub struct Region {
//     pub id: u64,                        // Region ID
//     pub start_key: Vec<u8>,             // Region 起始 Key
//     pub end_key: Vec<u8>,               // Region 结束 Key
//     pub peers: Vec<Peer>,               // ★ 3 个 Peer 副本
//     pub leader: Option<Peer>,           // Leader
//     pub raft_state: RaftState,          // Raft 状态
// }
//
// pub struct Peer {
//     pub id: u64,                        // Peer ID
//     pub store_id: u64,                  // 所在 TiKV 节点 Store ID
//     pub role: PeerRole,                 // Leader / Follower / Learner
// }
//
// // ★ RaftLog（日志）
// // src/raft/src/log.rs
// pub struct RaftLog {
//     entries: Vec<Entry>,   // Raft 日志条目
//     committed: u64,        // ★ committed index：已提交的最高日志号
//     applied: u64,          // ★ applied index：已应用到状态机的最高日志号
//     last_term: u64,        // 最后日志的 Term
// }
//
// // ★ Raft 核心流程（面试必背！）：
// // ① Client 写请求 → Leader
// // ② Leader append 新日志 → 复制到 Follower
// // ③ Quorum（3 副本 ≥ 2）复制成功 → Leader commit
// // ④ Leader apply 日志到 RocksDB → 返回 Client 成功
// //
```

### 2.2 TiKV Raft 六大工程优化（面试必背！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Raft 六大工程优化（性能提升 100x）                    │
│                                                               │
│  ┌──────┬──────────────┬──────────────────────────────┐    │
│  │ #    │ 优化项          │ 效果                        │    │
│  ├──────┼──────────────┼──────────────────────────────┤    │
│  │  1   │ **Pipeline Append** │ Leader 并行发送日志到多个 Follower │    │
│  │      │ 流水线追加日志    │ （不用等一个 Follower 收完再发给下一个）│    │
│  ├──────┼──────────────┼──────────────────────────────┤    │
│  │  2   │ **Batch Write**  │ Leader 收到多个请求后合并成一批写   │    │
│  │      │ 批量写入          │ （减少 Write Latency 和 IO 次数）    │    │
│  ├──────┼──────────────┼──────────────────────────────┤    │
│  │  3   │ **Async Apply**  │ 日志 Commit 后异步 Apply 到状态机   │    │
│  │      │ 异步应用日志      │ （不用等 Apply 完才返回 Client）    │    │
│  ├──────┼──────────────┼──────────────────────────────┤    │
│  │  4   │ **PreVote**      │ 选主前先预投票（避免无效选主）       │    │
│  │      │ 预投票            │ （Raft 标准选主：直接 Term+1 投票） │    │
│  ├──────┼──────────────┼──────────────────────────────┤    │
│  │  5   │ **Lease Read**   │ Leader 租约读（不读 Lease 读）       │    │
│  │      │ 租约读            │ （Follower 读需要 Lease + ReadIndex）│    │
│  ├──────┼──────────────┼──────────────────────────────┤    │
│  │  6   │ **Multi-Raft**   │ 多个 Raft Group 并行处理            │    │
│  │      │ 多 Region 并行    │ （1台 TiKV 可能有上万个 Raft Group）│    │
│  └──────┴──────────────┴──────────────────────────────┘    │
│
│ ★ 面试高频：为什么 TiKV Raft 比标准 Raft 快这么多？            │
│   标准 Raft：单线程 + 串行 + 同步 + 无优化                   │
│   TiKV Raft：流水线 + 批处理 + 异步 + 预投票 + 租约 + 并行    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Region Split/Merge 源码级

### 3.1 Region Split 触发条件

```rust
// ★ TiKV Region Split 触发条件（源码级，面试高频！）
//
// components/raftstore/src/region_split.rs
//
// // 自动 Split 的三种触发条件
// pub enum SplitCheck {
//     /// ① Region 太大（数据量超过阈值）
//     Size(RegionSize),
//     /// ② Region Key 数量太多
//     Keys(u64),
//     /// ③ Region 读请求热点太高
//     ReadLoad(u64),
//     /// ④ Region 写请求热点太高
//     WriteLoad(u64),
// }
//
// // ★ Split 参数（TiKV 配置）
// // tikv.toml
// [raftstore]
// region-max-size = "144MB"      // Region 最大（超过就 Split）
// region-split-size = "96MB"     // Split 后每个新 Region 的目标大小
// region-max-keys = 1_440_000    // Region 最大 Key 数
// region-split-keys = 960_000    // Split 后目标 Key 数
// region-check-split-interval = "10m"  // 多久检查一次
//
// ★ Split 核心流程（源码级）：
//   ① PD 或 TiKV 内部发现 Region 太大
//   ② TiKV Leader 找到 Split Key（Region 中间位置）
//   ③ TiKV Leader 提交 Split Raft 日志：
//      // Peer 1 Split 成 Region A 和 Region B
//      Region A: [old_start_key, split_key)
//      Region B: [split_key, old_end_key)
//   ④ Raft Commit 后：
//      a. Region A 的 Range 更新为 [old_start, split_key)
//      b. 创建 Region B 的新 Raft Group
//      c. PD 负责调度 Region B 的 3 个 Peer
//   ⑤ Split 完成
//
```

### 3.2 Region Merge

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Region Merge（面试高频！）                            │
│                                                               │
│  什么时候 Merge：                                              │
│   ① 两个相邻 Region 都很小（比如各 30MB）                    │
│   ② 空 Region（比如 DROP TABLE 后）                          │
│   ③ PD 调度认为需要 Merge 来减少 Region 数量                │
│
│  Merge 核心流程：                                              │
│   PD 发起 Merge 命令（给两个相邻 Region 的 Leader）           │
│   → Region A Leader 提交 Merge Raft 日志                     │
│   → Region B Leader 同步数据到 Region A（Raft 迁移）        │
│   → Region A Range 扩展到 [A.start, B.end)                   │
│   → Region B 删除                                              │
│   → Merge 完成                                                │
│
│ ★ Split + Merge 平衡：                                        │
│   写热点 Region → Split（自动分裂，分散压力）                │
│   小 Region → Merge（自动合并，减少 Raft Group 数量）         │
│   → TiDB 系统最终 Region 数量稳定在一个合理值                │
│
│ ★ TiKV Region vs Kafka Partition vs Redis Slot（Split/Merge 对比）：│
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │            │ TiKV Region   │ Kafka Partition│ Redis Slot   │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ Split      │ ✅ 自动       │ ❌ 固定不变    │ 手动迁移      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ Merge      │ ✅ 自动       │ ❌ 不支持      │ ❌ 不支持     │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 分片粒度    │ Key Range    │ Hash Partition│ Hash Slot    │   │
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV Raft Group 核心？| **每个 Region = 一个 Raft Group（3 Peer 副本）** |
| 2 | TiKV Raft 六大优化？| **Pipeline Append + Batch Write + Async Apply + PreVote + Lease Read + Multi-Raft** |
| 3 | Region Split 触发条件？| **Region 太大（144MB）+ Key 太多 + 读写热点高** |
| 4 | Region Split 流程？| PD 发现 → 找中间 Key → 提交 Split Raft 日志 → Commit → 更新 Range + 创建新 Region |
| 5 | Region Merge 流程？| PD 发起 → Region 数据迁移 → 扩展 Range → 删除小 Region |
| 6 | TiKV vs Kafka vs Redis Split/Merge？| TiKV：自动 Split+Merge；Kafka：Partition 固定；Redis：手动 Slot 迁移 |
| 7 | Lease Read？| Leader 租约读（Follower 读需要 ReadIndex + Lease 验证）|
| 8 | PreVote 作用？| 避免无效选主（Follower 先预投票确认能当选再 Term+1）|
| 9 | Pipeline Append？| Leader 并行发日志到多个 Follower（不用串行等待）|
| 10 | 面试最高频考点排序？| **1. 六大优化 2. Split 触发 3. Split 流程 4. Lease Read + PreVote 5. TiKV vs Kafka vs Redis** |
| 11 | 学习路径？| region.rs → raftstore split.rs → RaftLog entries/committed/applied → PD split 调度 |
| 12 | 面试常见追问？| 为什么 Region 大小是 144MB？→ 经验值；太大会导致 Split 慢，太小会导致 Raft Group 太多 |
| 13 | TiKV Region 数据组织？| 按 Key Range 划分；每个 Region 一个 Raft Group；RocksDB 存数据 |
| 14 | Learner 角色？| 只同步不投票；TiKV 3.x+ 引入；用于异地灾备 |
| 15 | 2024 新特性？| TiKV 8.0+ **Dynamic Membership**（Region 副本数动态调整）+ **Learner 加速选举** |
| 16 | 面试高频场景？| 大表 Split（亿级数据自动分裂）+ Merge 小 Region（DROP TABLE 后）|
| 17 | TiKV Split vs 分布式哈希？| TiKV：按 Range Split（有热点问题，需要 PD 调度）；哈希环：均匀分散（但数据迁移多）|
| 18 | Raft committed vs applied？| committed：Quorum 复制成功；applied：写入 RocksDB；Async Apply 让 committed 后立即返回 |
| 19 | TiKV Region 数量？| 单表通常 100-1000 个 Region；大表可能上万个 |
| 20 | Split 后 PD 做什么？| 给新 Region B 分配 3 个 Peer；考虑机架感知 + 负载均衡 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：components/raftstore/src/region.rs + src/raft/src/log.rs  
**面试考点：20 题**  
**差异化点**：六大工程优化 + Split/Merge 源码级 + Region vs Partition vs Slot
