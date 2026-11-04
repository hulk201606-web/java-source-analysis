# TiKV Hot Region Scheduler 源码级深度解析

> **源码基线**：TiKV PD v8.0.x（Go）  
> **核心仓库**：tikv/pd → server/schedule/schedulers/hot_region_scheduler.go  
> **面试权重**：⭐⭐⭐⭐⭐（PD 最复杂 Scheduler，大厂面试高频）

---

## 一、Hot Region 问题背景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Hot Region 是什么？                                  │
│                                                               │
│  TiKV 数据分布：                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Store 1（TiKV Node）                                    │   │
│  │   Region 1（热点！QPS=50000）+ Region 2 + Region 3    │   │
│  └───────────────────────────────────────────────────────┘   │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Store 2（TiKV Node）                                    │   │
│  │   Region 4（闲置 QPS=100）+ Region 5 + Region 6       │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│  问题：Store 1 的 Region 1 QPS 是其他 Region 的 500x！        │
│  → Store 1 CPU 100% / IO 饱和 / 请求超时                    │
│  → Store 2 空着没事干（资源浪费）                            │
│
│ ★ Hot Region Scheduler 解决方案（PD 内置调度器）：            │
│  ① TiKV 每 10s 上报 Region 统计（QPS/读写/CPU/数据量）     │
│  ② PD HotRegionScheduler 扫描识别热点 Region               │
│  ③ 为热点 Region 生成 Operator（迁移/加副本/Split）        │
│  ④ Operator 下发给 TiKV Raft Layer 执行                    │
│
│ ★ TiKV 三种热点类型（面试高频！）：                           │
│  ┌──────────┬──────────────────┬──────────────────────┐     │
│  │ 热点类型   │ 识别指标           │ 典型场景               │     │
│  ├──────────┼──────────────────┼──────────────────────┤     │
│  │ Write Hot │ Write QPS > 阈值  │ 秒杀订单/批量写入     │     │
│  │ Read Hot  │ Read QPS > 阈值   │ 热门商品/排行榜      │     │
│  │ CPU Hot   │ CPU Usage > 80%   │ 复杂查询（聚合/Sort） │     │
│  └──────────┴──────────────────┴──────────────────────┘     │
│
│ ★ 和 K8s HPA 类比：                                          │
│   K8s HPA：CPU 高 → 加 Pod                                    │
│   TiKV Hot：Region 热点 → 迁移 Region / 加副本 / Split     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiKV Region Heartbeat 上报

### 2.1 TiKV 上报数据结构

```go
// ★ TiKV 每 10s 上报给 PD 的 RegionStats（Go 源码）
//
// // TiKV raftstore/src/region_stats.rs
// pub struct RegionStats {
//     pub region_id: u64,
//     pub peer_count: usize,
//
//     // ★ 核心统计（Hot Scheduler 数据来源）
//     pub leader_count: usize,          // Leader 数量（通常 1）
//     pub leader_weight: usize,         // Leader 权重（默认 1000）
//     pub leader_score: f64,            // Leader 负载得分
//     pub leader_requests: u64,         // Leader 总 QPS
//     pub leader_reads: u64,            // Leader 读 QPS
//     pub leader_writes: u64,           // Leader 写 QPS
//     pub leader_cpu_usage: f64,        // Leader CPU 使用率
//     pub leader_memory_usage: f64,     // Leader 内存使用率
//
//     pub peer_count: usize,            // Peer 总数
//     pub peer_score: f64,              // Peer 总得分
//     pub peer_requests: u64,           // Peer 总 QPS
//
//     // 数据量
//     pub bytes_read: u64,              // 读流量（字节）
//     pub bytes_written: u64,           // 写流量（字节）
//
//     pub approximate_size: u64,        // Region 近似大小
//     pub approximate_keys: u64,        // Region Key 数量
// }
//
// ★ 上报时机：
//   TiKV Raft Leader 每 10s 调 pd_client.region_heartbeat(stats)
//   → gRPC 发送给 PD
//   → PD 收到后更新 storeStats[storeID]
//
// ★ PD 存储结构（Go 源码）：
// type StoreStats struct {
//     storeID    uint64
//     regionStats map[uint64]*RegionStats  // regionID → stats
//     hotRegions   []*HotRegion            // 热点 Region 列表
// }
//
```

---

## 三、HotRegionScheduler 核心算法（源码级）

### 3.1 调度器入口

```go
// ★ HotRegionScheduler 调度入口（Go 源码简化）
//
// // pd/server/schedule/schedulers/hot_region_scheduler.go
//
// type hotRegionScheduler struct {
//     cluster    cluster.Cluster
//     config     *hotRegionSchedulerConfig
//     // ★ 三种热点类型独立的调度器
//     writeScheduler *hotWriteRegionScheduler
//     readScheduler  *hotReadRegionScheduler
//     leaderScheduler *hotLeaderScheduler
// }
//
// func (s *hotRegionScheduler) Schedule(cluster) []*Operator {
//     s.cluster = cluster
//     var ops []*Operator
//
//     // ★ 三种热点独立调度（优先级：Read > Write > Leader）
//     ops = append(ops, s.readScheduler.Schedule(cluster)...)
//     ops = append(ops, s.writeScheduler.Schedule(cluster)...)
//     ops = append(ops, s.leaderScheduler.Schedule(cluster)...)
//
//     return ops
// }
//
// ★ 三种热点调度器独立设计：
//   Read Hot 调度器 → 加副本（分散读流量）
//   Write Hot 调度器 → 迁移 Region（避开热点 Key）
//   Leader Hot 调度器 → TransferLeader（最快！）
//   → 三种独立实现，互不干扰
//
```

### 3.2 Hot Region 识别算法

```go
// ★ Hot Region 识别算法（核心！面试高频）
//
// func (s *hotRegionScheduler) collectHotRegions(cluster) {
//     hotThreshold := cluster.GetHotRegionThreshold()  // 默认 0.8
//     // 超过平均值 * threshold → 判定为热点
//
//     // ★ Step 1: 计算每个 Store 的平均 QPS
//     avgLeaderRequests := totalLeaderRequests / totalLeaderCount
//     avgLeaderReads := totalLeaderReads / totalLeaderCount
//     avgLeaderWrites := totalLeaderWrites / totalLeaderCount
//
//     // ★ Step 2: 遍历所有 Region Leader 统计
//     for _, store := range cluster.GetStores() {
//         for regionID, stats := range store.RegionStats {
//             // 判断 Write Hot
//             if stats.LeaderWrites > avgLeaderWrites * hotThreshold {
//                 hotWriteRegions = append(hotWriteRegions,
//                     {regionID, storeID, stats.LeaderWrites})
//             }
//
//             // 判断 Read Hot
//             if stats.LeaderReads > avgLeaderReads * hotThreshold {
//                 hotReadRegions = append(hotReadRegions,
//                     {regionID, storeID, stats.LeaderReads})
//             }
//
//             // 判断 CPU Hot
//             if stats.LeaderCPU > 0.8 &&
//                stats.LeaderRequests > avgLeaderRequests * hotThreshold {
//                 hotCPURegions = append(hotCPURegions,
//                     {regionID, storeID, stats.LeaderCPU})
//             }
//         }
//     }
//
//     // ★ Step 3: 排序（QPS 高的排前面，优先调度）
//     sort.Slice(hotWriteRegions, func(i, j int) bool {
//         return hotWriteRegions[i].QPS > hotWriteRegions[j].QPS
//     })
//     sort.Slice(hotReadRegions, ...)
// }
//
// ★ 阈值配置（pd-ctl config show hot-region）：
//   hotspot.high-write-ratio = 0.8   // 写 QPS > 平均 80% → 热点
//   hotspot.high-read-ratio = 0.8    // 读 QPS > 平均 80% → 热点
//   hotspot.low-write-ratio = 0.5    // 写 QPS < 平均 50% → 冷却
//   hotspot.region-write-flow-threshold = 64MB/s
//   hotspot.region-read-flow-threshold = 256MB/s
//
```

### 3.3 Hot Write 调度核心

```go
// ★ Hot Write Region 调度核心源码（面试最高频！）
//
// // Hot Write 策略：迁移 Peer + 可能触发 Split
//
// func (s *hotWriteRegionScheduler) Schedule(cluster) []*Operator {
//     var ops []*Operator
//
//     // 从热点 Store 找最冷的 Store（目标）
//     // coldStore = findColdStore(cluster)
//
//     for _, hotRegion := range s.hotWriteRegions {
//         // ★ Step 1: 选目标 Store（避免冲突）
//         targetStore := s.pickTargetStore(cluster, hotRegion.StoreID)
//         if targetStore == nil { continue }
//         // 条件：
//         //   ① 目标 Store 不是 offline
//         //   ② 目标 Store CPU < 70%（防止热点转移没用）
//         //   ③ 目标 Store 上没有该 Region 的 Peer（防多副本同节点）
//         //   ④ 目标 Store 磁盘使用率 < 90%
//
//         // ★ Step 2: 生成迁移 Operator（快）
//         // → AddPeer + RemovePeer（把热点 Region 迁到冷 Store）
//         op1 := CreateAddPeerOperator(hotRegion.RegionID, targetStore.ID)
//         op2 := CreateRemovePeerOperator(hotRegion.RegionID, hotRegion.StoreID)
//         ops = append(ops, op1, op2)
//
//         // ★ Step 3: 可能触发 Split（热点 Key 实在太集中）
//         // 如果该 Region 连续被调度 N 次（split_score > 5）
//         // → 生成 SplitOperator
//         // → Split 成两个 Region（分散热点 Key）
//         if hotRegion.SplitScore > s.config.SplitThreshold {
//             splitKey := s.findSplitKey(hotRegion.RegionID)
//             splitOp := CreateSplitOperator(hotRegion.RegionID, splitKey)
//             ops = append(ops, splitOp)
//         }
//     }
//     return ops
// }
//
// ★ Split 什么时候比迁移好？（面试高频）
//   迁移：把整个 Region 移到冷节点（不改变数据分布）
//   Split：把 Region 拆成两个（改变数据分布，从根上解决热点）
//   → 如果热点 Key 集中在 Region 内某个位置 → Split 更好
//   → 如果热点 Key 覆盖整个 Region → 迁移更好
//
```

### 3.4 Hot Read 调度核心

```go
// ★ Hot Read Region 调度核心（Read 场景有 ReadIndex！）
//
// func (s *hotReadRegionScheduler) Schedule(cluster) []*Operator {
//     var ops []*Operator
//
//     for _, hotRegion := range s.hotReadRegions {
//         // ★ Read Hot 最优策略：加 Follower 副本！
//         // Read 请求可以走 Follower（ReadIndex / Lease Read）
//         // → Follower 越多 → 读越能分摊
//
//         targetStore := s.pickColdStore(hotRegion.StoreID)
//         if targetStore == nil { continue }
//
//         // ★ AddPeer（增加副本）
//         op := CreateAddPeerOperator(hotRegion.RegionID, targetStore.ID)
//         ops = append(ops, op)
//         // → TiKV Raft 层执行 AddPeer
//         // → 新 Follower 同步日志
//         // → Read 请求开始走新 Follower
//     }
//     return ops
// }
//
// ★ Hot Read vs Hot Write 策略差异（面试高频）：
//   Write Hot → 迁移 Region（Write 只能走 Leader）
//   Read Hot → 加 Follower（Read 可以走 Follower，分散负载）
//
```

### 3.5 Operator 下发与执行

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Hot Scheduler Operator 下发到执行全链路                   │
│                                                               │
│  ① OperatorController.Run()（PD 后台线程，每 1min）          │
│     → hotRegionScheduler.Schedule() → ops []*Operator        │
│     → 去重 + 冲突检测（同一 Region 不能同时 Add 和 Remove） │
│     → 加入 OperatorController.opsQueue                       │
│
│  ② OperatorController 下发给 TiKV Raft Leader                │
│     gRPC: SendOperator(operator) → TiKV                      │
│     → raftstore::apply_pending_peer()                        │
│
│  ③ TiKV Raft Leader 执行 Operator                            │
│     // AddPeer:                                               │
│     a. Leader 给新节点发 AppendEntries（日志复制）           │
│     b. 新节点 ReceiveSnapshot 或 CatchUpLogs                  │
│     c. AppendEntryResponse(success=true)                     │
│     // Split:                                                 │
│     a. TiKV 从 Region 中间 Split 成两个 Region               │
│     b. 两个 Region 各自独立 Raft Group                       │
│     c. Split 后新 Region 上报给 PD                           │
│
│  ④ TiKV 上报执行结果 → PD OperatorController 更新状态         │
│     成功 → 从 opsQueue 移除                                   │
│     失败 → 重试或放弃                                         │
│
│  ★ Operator 并发控制（面试高频）：                            │
│     max-snapshot-count = 3（默认）                            │
│     // 同时最多 3 个 Snapshot 传输（控带宽）                 │
│     // Snapshot 传输慢 → 限制并发数量                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、边界情况处理

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Hot Scheduler 边界情况（面试高频！）                      │
│                                                               │
│  ① Flap 抖动（调度-取消-再调度）：                           │
│     → 冷却机制（low-write-ratio = 0.5）                     │
│     → 热点 Region 必须连续 N 次才调度                         │
│     → 调度后短时间内不再调度同一 Region                        │
│
│  ② Operator 执行超时：                                       │
│     默认 5min 超时 → 从队列移除 → 下次调度可能重新生成       │
│     → 避免 Operator 无限堆积                                  │
│
│  ③ Split 失败：                                              │
│     Region Split 条件变了（热点 Key 已经被迁走）             │
│     → OperatorController 标记为 failed → 丢弃                │
│     → 下次调度重新评估                                        │
│
│  ④ 多个 Scheduler 冲突：                                     │
│     BalanceRegionScheduler 想迁这个 Region（均衡分布）       │
│     HotRegionScheduler 也想迁这个 Region（热点处理）          │
│     → OperatorController 冲突检测（同一 Region 不允许多个 Operator）│
│     → 按优先级：Hot（紧急）> Balance（常规）                │
│
│  ⑤ 目标 Store 变成新热点：                                   │
│     迁移后 → 新 Store 也变热点了                              │
│     → Hot Scheduler 下一轮调度自动发现 → 继续迁移           │
│     → 最终效果：热点均匀分布到所有节点                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Hot Scheduler 解决什么？| **Region 热点**（QPS 高 / CPU 高 / 读写大）→ 迁移 / 加副本 / Split |
| 2 | 三种热点类型？| **Write Hot / Read Hot / CPU Hot**；Write→迁移，Read→加副本，CPU→TransferLeader |
| 3 | TiKV 怎么上报热点？| 每 10s 上报 RegionStats：**leader_requests/leader_reads/leader_writes/cpu_usage** |
| 4 | Hot Scheduler 源码路径？| `pd/server/schedule/schedulers/hot_region_scheduler.go` |
| 5 | Hot Region 识别算法？| **超过平均值 × threshold（默认 0.8）** → 判定为热点；排序 QPS 高的优先 |
| 6 | Read Hot 为什么加 Follower？| **Read 请求可以走 Follower（ReadIndex/Lease Read）**→ Follower 越多读越能分摊 |
| 7 | Write Hot 为什么迁移？| **Write 请求只能走 Leader**→ 加 Follower 没用 → 只能迁移 Region Leader |
| 8 | Split vs 迁移？| Split：**热点 Key 在 Region 内集中** → 从根上解决；迁移：热点覆盖整个 Region |
| 9 | Flap 抖动防住？| **low-ratio=0.5 冷却** + 连续 N 次才调度 + 调度后短时间不再调度 |
| 10 | max-snapshot-count？| **默认 3** → 同时最多 3 个 Snapshot 传输（控带宽）|
| 11 | 目标 Store 过滤条件？| ① 不是 offline ② CPU < 70% ③ 无同 Region Peer ④ 磁盘 < 90% |
| 12 | Hot Scheduler vs Balance Scheduler？| Hot：**按 QPS/CPU（负载）**；Balance：**按 Region 数量（均匀）**；Hot 更智能更复杂 |
| 13 | Operator 超时？| 默认 5min → 从队列移除 → 下次重新调度 |
| 14 | 冲突优先级？| **Hot（紧急）> Balance（常规）**；同一 Region 只允许一个 Operator |
| 15 | 面试最高频考点排序？| **1. 三种热点 + 各自处理 2. Read Hot 加 Follower 3. Split 触发 4. Flap 抖动防住** |
| 16 | 学习路径？| hot_region_scheduler.go → collectHotRegions → hotWriteRegionScheduler → hotReadRegionScheduler |
| 17 | Split 触发条件？| Region 是热点 + 连续 split_score（比如 5 次调度都想迁移）+ Region 够大（>96MB）|
| 18 | Hot Scheduler vs K8s HPA？| 都是负载均衡；K8s 加 Pod；TiKV 迁移 Region / 加副本 / Split |
| 19 | 调度频率？| 每 1min 运行一次（scheduler-check-interval）；TiKV 上报每 10s |
| 20 | 面试高频边界情况？| Flap 抖动 + 多个 Scheduler 冲突 + 目标 Store 变新热点 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV PD v8.0.x（Go）  
**面试考点：20 题**  
**差异化点**：PD 最复杂 Scheduler + 源码级算法 + 边界情况
