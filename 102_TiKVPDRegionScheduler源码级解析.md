# TiKV PD Region Scheduler 源码级深度解析

> **源码基线**：TiKV PD v8.0.x（Go）  
> **核心仓库**：tikv/pd  
> **面试权重**：⭐⭐⭐⭐⭐（PD 最复杂模块，源码级深度）

---

## 一、PD Scheduler 总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV PD Region Scheduler 全景（源码级）                  │
│                                                               │
│  PD Scheduler Go 源码路径：                                    │
│  pd/server/schedule/                                           │
│  ├── scheduler.go            ★ 调度器接口                     │
│  ├── schedulers/                                              │
│  │   ├── balance_region_scheduler.go    ★ Region Balance     │
│  │   ├── balance_leader_scheduler.go    ★ Leader Balance     │
│  │   ├── hot_region_scheduler.go         ★ Hot Region         │
│  │   ├── replica_checker_scheduler.go   ★ 副本完整性          │
│  │   ├── replica_migrator_scheduler.go   ★ 手动迁移           │
│  │   └── merge_region_scheduler.go       ★ Region Merge       │
│  ├── operator.go             ★ Operator 核心（下发给 TiKV）  │
│  └── operator_controller.go  ★ Operator 下发 + 执行跟踪     │
│
│ ★ Scheduler 接口（Go 源码）：                                 │
│ type Scheduler interface {                                    │
│     // 调度器名称                                             │
│     GetName() string                                          │
│     // 每个调度周期调用，返回要执行的 Operator                │
│     Schedule(cluster Cluster) []*operator.Operator            │
│ }                                                             │
│
│ ★ Operator 类型（面试高频）：                                  │
│   AddPeer(regionID, storeID)           // 加副本              │
│   RemovePeer(regionID, storeID)        // 删副本              │
│   TransferLeader(regionID, storeID)    // 转移 Leader          │
│   MergeRegion(left, right)             // 合并 Region         │
│   SplitRegion(regionID, splitKey)      // 拆分 Region         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、BalanceRegionScheduler 源码级

### 2.1 核心算法流程

```go
// ★ BalanceRegionScheduler.Schedule() 源码级流程
//
// 每 1min 运行一次（scheduler-check-interval）：
//
// func (s *balanceRegionScheduler) Schedule(cluster) []*Operator {
//
//     // ★ Step 1: 计算每个 Store 的 Region 数（含 Leader+Follower）
//     storeStats := cluster.GetStoreStats()
//     for _, store := range storeStats {
//         store.RegionCount = len(store.GetRegion())
//         store.RegionScore = store.RegionCount * weight  // 权重（Leader 2.0 + Follower 1.0）
//     }
//
//     // ★ Step 2: 找出最热 Store 和最冷 Store
//     hotStore := findStoreWithMaxRegionScore(storeStats)
//     coldStore := findStoreWithMinRegionScore(storeStats)
//     // 如果 hotStore.score - coldStore.score < tolerance（默认 0.1*avg）
//     // → 不用调度（已经均衡）
//     if hotStore.score - coldStore.score < avg*0.1 {
//         return nil;
//     }
//
//     // ★ Step 3: 找 hotStore 上可以迁移的 Region
//     for _, region := range hotStore.GetRegions() {
//         // 条件：Region 在 coldStore 上不能已有副本（防多副本同节点）
//         if !region.HasPeerInStore(coldStore.ID) {
//             // 生成迁移 Operator
//             op = CreateRemovePeerOperator(region.ID, hotStore.ID)
//             op = CreateAddPeerOperator(region.ID, coldStore.ID)
//             return []*Operator{op}
//         }
//     }
//     return nil;
// }
//
// ★ 源码级关键细节（面试高频）：
// ① **tolerance 容忍度**：score 差 < avg*0.1 → 不调度（避免抖动）
// ② **Region 选择**：优先选 Region 小的（迁移成本低）
// ③ **目标 Store 过滤**：排除 offline / 正在调度 / 已有同 Region 副本
//
```

### 2.2 Operator 下发执行链路

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Operator 从 PD 到 TiKV 完整链路（源码级）                │
│                                                               │
│  ① PD Controller Loop：                                      │
│     OperatorController.Run()                                  │
│     → 每个调度器 Schedule() 生成 Operator                    │
│     → OperatorController 下发 Operator 给 TiKV Raft Leader  │
│
│  ② TiKV Raft Leader 接收 Operator：                          │
│     raftstore::apply_pending_peer()                          │
│     // AddPeer：                                              │
│     //   a. Leader 给新节点发 AppendEntries（日志复制）       │
│     //   b. 新节点接收完 → AppendEntryResponse(success)      │
│     // RemovePeer：                                           │
│     //   a. Leader 从 Peer 列表移除                           │
│     //   b. 触发新的 Raft 配置变更（PeerConf Change）        │
│
│  ③ OperatorController 跟踪执行：                             │
│     OperatorController 每 10s 查询 TiKV 状态                  │
│     → 如果 Operator 完成 → 从队列移除                         │
│     → 如果 Operator 超时（默认 5min）→ 放弃或重试            │
│     → 如果 Operator 失败 → 记录 Metrics + 告警              │
│
│ ★ Operator 并发控制：                                        │
│   max-snapshot-count = 3（默认）                              │
│   // 防止同时发太多 AddPeer → Snapshot 占带宽               │
│   // TiKV 同时最多 3 个 Snapshot 传输                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、BalanceLeaderScheduler 源码级

```go
// ★ BalanceLeaderScheduler.Schedule() 源码级
//
// 和 Region Balance 独立（只平衡 Leader，不迁移 Peer）：
//
// func (s *balanceLeaderScheduler) Schedule(cluster) []*Operator {
//
//     // ★ Step 1: 计算每个 Store 的 Leader 数
//     for _, store := range cluster.GetStores() {
//         store.LeaderCount = len(store.GetLeaders())
//     }
//
//     // ★ Step 2: 目标 Leader 数（理想值）
//     avgLeaderCount = totalLeaderCount / aliveStoreCount
//     targetLeaderCount = int(avgLeaderCount * 1.05)  // 容忍度 5%
//
//     // ★ Step 3: Leader 多的 → Leader 少的
//     hotStore := findStoreWithMostLeaders()
//     coldStore := findStoreWithLeastLeaders()
//     if hotStore.LeaderCount - coldStore.LeaderCount < 2 {
//         return nil;  // 已经很均衡
//     }
//
//     // ★ Step 4: 选 hotStore 上的 Leader Region
//     region := pickRegionWithLeaderOn(hotStore)
//     // 条件：coldStore 上必须有这个 Region 的 Follower Peer
//     // （因为 Raft Leader 只能在已有 Peer 之间转移）
//     if region.HasFollowerOn(coldStore.ID) {
//         op = CreateTransferLeaderOperator(region.ID, coldStore.ID)
//         return []*Operator{op}
//     }
//     return nil;
// }
//
// ★ TransferLeader 为什么快？（面试高频）
//   Raft Leader 转移（AppendEntries RPC）：
//   → Leader 给 Follower 发 AppendEntries(NextIndex)
//   → Follower 已跟上日志 → Leader 直接转权
//   → 延迟 ~10ms（毫秒级！）
//   → 比 AddPeer+RemovePeer（秒-分钟级）快 100x+
//
// ★ Leader Balance vs Region Balance：
//   Leader Balance：**只转移 Leader**（快！~10ms）
//   Region Balance：**迁移 Peer**（慢！秒级，Add+Remove）
//   → 优先做 Leader Balance（无成本），不够再做 Region Balance
//
```

---

## 四、HotRegionScheduler 源码级（最复杂）

```go
// ★ HotRegionScheduler.Schedule() 源码级完整流程
//
// 核心区别：有统计模块（region heartbeat 带 QPS/CPU）
//
// func (s *hotRegionScheduler) Schedule(cluster) []*Operator {
//
//     // ★ Step 1: 收集 TiKV 上报的热点数据
//     // TiKV 每 10s 上报：Region Leader QPS / CPU / Write Flow
//     hotRegions := cluster.GetHotRegions()
//     // 过滤：QPS > hot-read-threshold（默认 10000）
//     //      Write Flow > hot-write-threshold（默认 64MB/s）
//
//     // ★ Step 2: 分三种热点类型处理
//     //
//     // ① Write Hot：
//     //    hotStore = findStoreWithMostWriteHotRegions()
//     //    coldStore = findStoreWithLeastWriteHotRegions()
//     //    → 生成 AddPeer+RemovePeer（迁移）或 SplitRegion（拆分）
//     //
//     // ② Read Hot：
//     //    hotStore = findStoreWithMostReadHotRegions()
//     //    → 增加读副本！（Read 可以走 Follower）
//     //    → AddPeer（增加副本分散读流量）
//     //
//     // ③ Leader Hot：
//     //    hotStore = findStoreWithMostHotLeader()
//     //    → TransferLeader（转移 Leader，最快）
//     //
//     // ★ Step 3: 选目标 Store（避免冲突）
//     // targetStore 条件：
//     // - 不能已有同 Region Peer（多副本同节点风险）
//     // - CPU < 80%（否则新热点移过去没用）
//     // - Disk < 90%（快满了别加副本）
//
//     return ops
// }
//
// ★ Hot Scheduler vs Region Balance（面试高频）：
//   Region Balance：只看 Region 数量（均匀分布）
//   Hot Scheduler：**看 QPS/CPU/读写流量**（负载均衡）
//   → Hot Scheduler 更智能（但更复杂）
//   → Region Balance 是基础（保证均匀分布）
//   → Hot Scheduler 是进阶（处理热点突发）
//
```

---

## 五、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | PD Scheduler 源码路径？| **pd/server/schedule/schedulers/** → balance_region / balance_leader / hot_region 三个核心 |
| 2 | Scheduler 接口核心？| `GetName()` + `Schedule(cluster) []*Operator`；每 1min 调度一次 |
| 3 | Operator 五种类型？| **AddPeer / RemovePeer / TransferLeader / SplitRegion / MergeRegion** |
| 4 | BalanceRegion 流程？| ①算每个 Store Region Score ②找最热最冷 Store ③生成迁移 Operator ④下发给 TiKV Raft |
| 5 | BalanceRegion tolerance？| score 差 < avg*0.1 → 不调度（避免抖动） |
| 6 | TransferLeader 为什么快？| Raft AppendEntries RPC → 已跟上日志直接转权 → **~10ms（毫秒级）** |
| 7 | Leader Balance vs Region Balance？| Leader：只转移 Leader（快 100x）；Region：迁移 Peer（慢） |
| 8 | HotRegion 三种热点？| **Write Hot（迁移/Split）/ Read Hot（加副本）/ Leader Hot（TransferLeader）** |
| 9 | HotRegion 数据源？| **TiKV 每 10s 上报 Region Leader QPS / CPU / Write Flow** → PD 存 regionStats |
| 10 | Operator 并发控制？| **max-snapshot-count=3** → 同时最多 3 个 Snapshot 传输（控带宽）|
| 11 | Operator 下发执行链路？| OperatorController.Run() → TiKV Raft Leader → raftstore::apply_pending_peer() → 跟踪完成 |
| 12 | HotRegion Split 触发？| **Region 是热点 + 连续 split_score（比如 5 次调度都想迁移）+ Region 够大** → SplitOperator |
| 13 | HotRegion 目标 Store 过滤？| 排除 offline / 已有同 Region Peer / CPU>80% / Disk>90% |
| 14 | Scheduler 面试高频排序？| **1. TransferLeader vs AddPeer 快慢 2. Hot Region 三种热点处理 3. Operator 类型 4. tolerance 容忍度** |
| 15 | PD 学习路径？| pd/server/schedule/schedulers/balance_region → operator.go → TiKV raftstore::apply_pending_peer |
| 16 | Operator 超时失败？| 默认 5min 超时 → 放弃或重试；失败记录 Metrics + 告警 |
| 17 | Region Merge 什么时候触发？| **两个相邻 Region 都很小（< 64MB）** → merge_region_scheduler |
| 18 | PD 面试最高频考点？| **Scheduler 三个核心（Balance/Leader/Hot）+ Operator 五种类型 + TransferLeader 速度** |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV PD v8.0.x（Go）  
**面试考点：18 题**  
**差异化点**：PD Scheduler 源码级 + Operator 完整链路
