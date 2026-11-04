# TiDB TSO 时间戳分配 + PD 调度核心源码级深度解析

> **源码基线**：TiDB v8.0.x + PD v8.0.x（Go）  
> **核心仓库**：tikv/pd → server/ + pkg/tsop/；pingcap/tidb → session/  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 全局时钟，MVCC/Percolator 基石）

---

## 一、TSO 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TSO = Timestamp Oracle（全局时间戳服务）                  │
│                                                               │
│  PD（Placement Driver）三大核心职责：                         │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ ① ★ TSO 全局时间戳分配（本文重点！）                   │   │
│  │ ② Region 元数据管理 + 路由                             │   │
│  │ ③ Region 调度（Balance/Hot Region）                    │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ 为什么分布式数据库需要全局时间戳？（面试高频）              │
│                                                               │
│  TiDB 用 Percolator MVCC 事务：                                │
│   每个事务需要 Start TS + Commit TS                           │
│   → 必须全局单调递增、全集群唯一                              │
│   → 才能正确判断事务顺序 + MVCC 可见性                        │
│
│  ★ TSO 数字结构（面试必背！）：                                │
│   TSO 是一个 int64（64 bit）：                                │
│   ┌──────────────────────┬──────────────────┐               │
│   │ Physical（物理时间）  │ Logical（逻辑计数）│               │
│   │      46 bit           │      18 bit       │               │
│   │   毫秒级时间戳        │   同毫秒内序号     │               │
│   └──────────────────────┴──────────────────┘               │
│   Physical: 从某个起点（2016-01-01）至今的毫秒数             │
│   Logical: 同一毫秒内的自增序号（0~262143）                  │
│   → 同一毫秒可以分配 26 万个不重复时间戳！                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TSO 分配源码级

### 2.1 PD 端 TSO 生成

```go
// ★ PD TSO 分配核心源码（面试核心！）
//
// // pkg/tsop/allocators.go（PD 8.0+ 重构后的 TSO Allocator）
//
// type GlobalTSOAllocator struct {
//     // ★ 当前物理时间（ms）
//     physical int64
//     // ★ 当前逻辑计数
//     logical    int64
//     // ★ 逻辑计数最大值：2^18 - 1 = 262143
//     maxLogical int64
//     // ★ 更新间隔（默认 50ms 批量推进）
//     updateInterval time.Duration
//     mu sync.Mutex
// }
//
// // ★ 核心：批量分配时间戳
// func (a *GlobalTSOAllocator) AllocTSO(count int) []uint64 {
//     a.mu.Lock()
//     defer a.mu.Unlock()
//
//     // ① 计算需要的逻辑序号
//     // 如果当前 logical + count 超过最大值 → 推进物理时间
//     if a.logical + int64(count) > a.maxLogical {
//         // ② 等待物理时间前进（保证 physical 严格递增）
//         a.updatePhysical()
//         a.logical = 0
//     }
//
//     // ③ 批量生成 count 个连续 TSO
//     ret := make([]uint64, count)
//     for i := 0; i < count; i++ {
//         ret[i] = composeTSO(a.physical, a.logical + int64(i))
//     }
//     a.logical += int64(count)
//     return ret
// }
//
// // ★ 组合 TSO：physical 左移 18 位 + logical
// func composeTSO(physical, logical int64) uint64 {
//     return uint64(physical << 18) | uint64(logical)
// }
//
// // ★ 拆分 TSO（TiDB 端读 MVCC 时用）
// func parseTSO(tso uint64) (physical, logical int64) {
//     physical = int64(tso >> 18)
//     logical = int64(tso & 0x3FFFF)  // 低 18 bit
//     return
// }
//
```

### 2.2 TiDB 客户端批量取 TSO（性能关键！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB 客户端 TSO 批量获取（面试高频！）                     │
│                                                               │
│  问题：每个事务都要 RPC 向 PD 拿 2 个 TSO（Start + Commit）  │
│        → 高 QPS 下 PD 网络 RTT 成为瓶颈                       │
│                                                               │
│  优化：TiDB 客户端**批量预取** TSO                            │
│                                                               │
│  // tidb/session/txn.go                                       │
│  type oracle struct {                                         │
│      // ★ 本地缓存的一批 TSO                                  │
//      nextFuture tsoFuture    // 异步预取的下一批
//      // ★ 一次 RPC 申请一大批（比如 8192 个）
//      // 本地慢慢用，用完前再异步预取下一批
//  }
//
// 工作流程：                                                     │
//  ① TiDB 启动时向 PD 申请 8192 个连续 TSO                     │
//  ② 本地事务直接从缓存分配（无 RPC！）                         │
//  ③ 用了一半时 → 异步 RPC 预取下一批（Future）                │
//  ④ 用完前下一批已就位 → 永远不等待网络                        │
│                                                               │
│  ★ 效果：                                                      │
│   单 TiDB 实例 TSO 分配：百万级/s（本地内存操作）             │
│   PD 实际 RPC 压力：大大降低（8192 次分配合并成 1 次 RPC）   │
│
│ ★ 注意：批量预取不破坏全局有序                                │
│   PD 一次发放的 TSO 区间是全局连续的                          │
│   不同 TiDB 实例拿到不同区间 → 仍然全局单调                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、PD 高可用 + 时钟安全

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 高可用架构（面试高频！）                                │
│                                                               │
│  PD 集群 3/5 个节点（etcd Raft 保证一致）：                   │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ PD Leader │ PD Follower│ PD Follower│                       │
│  │ ★ 分配 TSO│ 只同步不分配│ 只同步不分配│                       │
│  └──────────┴──────────┴──────────┘                         │
│                                                               │
│  关键：只有 PD Leader 能分配 TSO（保证全局唯一来源）          │
│  Leader 挂了 → Raft 选举新 Leader → 继续分配                 │
│
│  ★ PD Leader 切换时如何保证 TSO 不回退？（面试核心！）        │
│                                                               │
│  新 Leader 启动时：                                           │
│   ① 从 etcd 读出最后分配的 TSO（lastTSO）                    │
│   ② ★ 等待本地物理时间 > lastTSO 的物理部分                  │
│      （sleep 到墙上时钟超过上次记录）                        │
│   ③ 再开始分配新 TSO                                         │
│   → 保证 TSO 全局单调不回退                                  │
│
│  // pd/server/cluster.go                                      │
//  func (c *Cluster) startLeader() {
//      lastTSO := loadLastTSOFromEtcd()
//      lastPhysical := parsePhysical(lastTSO)
//
//      // ★ 关键：等待墙上时钟超过 lastPhysical
//      sleepUntilWallClockGreaterThan(lastPhysical)
//
//      // 再加上安全窗口（防止时钟误差）
//      c.tsoAllocator.Reset(lastPhysical + safetyWindow)
//  }
│
│ ★ PD vs TrueTime（Spanner）对比（面试加分项）：                │
│  Spanner TrueTime：原子钟 + GPS，时间区间 API                │
│    TT.after() / TT.before()，CommitWait 等待时间误差         │
│  TiDB TSO：普通机器时钟 + PD 单点分配 + 等待防回退           │
│    → 依赖 NTP 时钟同步；不保证全球部署，单机房足够            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、PD 调度核心源码级

### 4.1 Scheduler 框架

```go
// ★ PD 调度器框架（源码级，面试高频）
//
// // pkg/schedule/scheduler.go
//
// type Scheduler interface {
//     // ★ 每个调度器周期执行 Schedule()
//     Schedule(opController *OperatorController) []*Operator
//     GetType() string
// }
//
// // ★ PD 内置核心调度器
// // 1. balance-leader-scheduler：Leader 均衡
// //    把 Raft Leader 在 Store 间均衡（Leader 承担读写）
// //
// // 2. balance-region-scheduler：Region 副本均衡
// //    把 Region Peer 在 Store 间均衡（磁盘容量均衡）
// //
// // 3. hot-region-scheduler：热点调度
// //    检测读写热点 Region → 把热点 Peer 调度到空闲节点
// //
// // 4. evict-leader-scheduler：驱逐 Leader（节点维护下线用）
// //
// // 5. scatter-range-scheduler：打散特定范围（建表导入数据时）
//
// // ★ 调度产出 Operator（操作符）→ 下发给 TiKV 执行
// type Operator struct {
//     steps []OpStep  // 比如：AddPeer + TransferLeader + RemovePeer
// }
//
// // 例子：把 Region 100 的 Leader 从 Store 1 迁到 Store 2
// // Operator steps:
// //   ① AddLearner(Store 2)        // 先加 Learner 追数据
// //   ② PromoteLearnerToVoter      // 追平后提升为 Voter
// //   ③ TransferLeader(Store 2)    // 转移 Leader
// //   ④ RemovePeer(Store 1)        // 移除旧 Peer
//
```

### 4.2 调度信息来源

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 调度决策的信息来源（面试高频）                          │
│                                                               │
│  TiKV 节点定期（默认 10s）向 PD Leader 上报心跳：             │
│                                                               │
│  StoreHeartbeat：                                             │
│   → Store 总容量、可用容量、Region/Leader 数量、CPU/磁盘 IO   │
│                                                               │
│  RegionHeartbeat（每个 Region Leader 上报）：                 │
│   → Region 的 Peer 分布、Leader 位置、读写流量（Bytes/Keys）  │
│   → 这些信息是 Hot Region 调度的依据                          │
│
│  PD 用这些信息构建全局视图：                                   │
│   ① 容量视图：每个 Store 剩余空间 → balance-region 判断      │
│   ② 负载视图：每个 Store 的 Leader 数 → balance-leader 判断  │
│   ③ 热点视图：Region 读写 QPS/Bytes → hot-region 判断        │
│
│ ★ 调度约束（Placement Rules）：                              │
│   副本数（默认 3）+ 机架感知（不要 3 副本在同一机架）        │
│   + Label 隔离（SSD/HDD、异地多活）                          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、TSO 完整使用链路

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 一个事务的 TSO 完整使用链路（面试串联题！）                │
│                                                               │
│  ① BEGIN：TiDB 从本地 TSO 缓存分配 Start TS                  │
│     （缓存不足则批量 RPC 向 PD Leader 预取）                 │
│                                                               │
│  ② SELECT（快照读）：                                        │
│     带 Start TS → TiKV MVCC → 找 commit_ts ≤ StartTS 的版本 │
│                                                               │
│  ③ UPDATE（Prewrite）：                                      │
│     TiKV 写 Lock CF + Data CF（带 Start TS）                │
│                                                               │
│  ④ COMMIT：TiDB 向 PD 取 Commit TS（必须 > Start TS）        │
│     TiKV 写 Write CF（start_ts → commit_ts）+ 删 Lock CF     │
│                                                               │
│  ⑤ 之后其他事务的快照读：                                     │
│     Start TS' > Commit TS → 能看到这个新版本                 │
│     Start TS' < Commit TS → 看不到（读旧版本）               │
│
│ ★ TSO 是整个 Percolator MVCC 的"时钟心跳"：                  │
│   没有全局单调 TSO → 无法判断事务顺序 → MVCC 不成立          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TSO 是什么？| **Timestamp Oracle**：PD 提供的全局单调递增时间戳，Percolator MVCC 的基础 |
| 2 | TSO 64bit 结构？| **Physical 46bit（毫秒）+ Logical 18bit（同毫秒序号，0~262143）** |
| 3 | 同毫秒能分配多少？| 2^18 = **26 万个**不重复 TSO |
| 4 | TSO 怎么组合？| **physical << 18 \| logical**（composeTSO）|
| 5 | TiDB 客户端优化？| **批量预取 8192 个 TSO** + Future 异步预取下一批，本地分配无 RPC |
| 6 | 批量预取破坏有序吗？| 不破坏；PD 发的区间全局连续，不同实例拿不同区间 |
| 7 | PD 集群架构？| 3/5 节点 etcd Raft；**只有 Leader 分配 TSO** |
| 8 | Leader 切换 TSO 不回退？| 新 Leader 读 lastTSO → **sleep 等墙上时钟超过 lastPhysical** + 安全窗口 |
| 9 | TSO vs Spanner TrueTime？| TSO 普通时钟+单点分配+等待防回退；TrueTime 原子钟+GPS+CommitWait |
| 10 | PD 三大职责？| **TSO 分配 + Region 元数据/路由 + Region 调度** |
| 11 | PD 核心调度器？| balance-leader + balance-region + hot-region + evict-leader + scatter-range |
| 12 | Operator 是什么？| 调度产出的操作步骤链（AddLearner→Promote→TransferLeader→RemovePeer）|
| 13 | 调度信息来源？| **StoreHeartbeat（容量）+ RegionHeartbeat（流量/Leader 分布）**，10s 一次 |
| 14 | Placement Rules？| 副本数 3 + 机架感知 + Label 隔离（SSD/HDD、异地）|
| 15 | 面试最高频考点排序？| **1. TSO 46/18 结构 2. 批量预取 3. Leader 切换防回退 4. PD 调度器 5. TSO 完整链路** |
| 16 | 学习路径？| tsop/allocators.go → composeTSO → TiDB oracle 批量预取 → PD startLeader → scheduler |
| 17 | 面试常见追问？| PD 挂了还能读写吗？→ 已有事务走 TiKV；新事务取不到 TSO 会阻塞等待 |
| 18 | PD 全部挂了影响？| TiDB 本地还有预取 TSO 缓存（短暂可用）；缓存耗尽后无法开新事务 |
| 19 | TSO 为什么用逻辑位？| 同毫秒内多个请求物理时间相同，用 logical 位区分并保证唯一递增 |
| 20 | Hot Region 怎么发现？| RegionHeartbeat 上报读写 Bytes/Keys → PD 统计超阈值 → 判为热点 |
| 21 | balance-leader vs balance-region？| Leader 均衡的是读写压力；Region 均衡的是存储容量/副本数 |
| 22 | 下线一个 TiKV 节点？| evict-leader 驱逐 Leader + balance-region 迁走 Peer → 节点清空 |
| 23 | 时钟漂移影响？| PD 间靠 Raft；依赖 NTP 同步；防回退 sleep 机制兜底；跨机房需谨慎 |
| 24 | 面试陷阱？| 说每个 TSO 都要 RPC PD → ❌ 客户端批量预取，大部分是本地内存分配 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：pd/pkg/tsop/allocators.go + tidb/session + pd/pkg/schedule/  
**面试考点：24 题**  
**差异化点**：TSO 46/18 位结构 + 批量预取 + Leader 切换防回退 + PD 五大调度器 + TrueTime 对比
