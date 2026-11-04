# TiKV Hot Region Scheduler 源码深度解析

> **源码基线**：TiKV v8.0.x（Go 实现）  
> **核心仓库**：tikv/pd  
> **面试权重**：⭐⭐⭐⭐⭐（PD 最复杂 Scheduler，大厂面试高频）

---

## 一、Hot Region 问题背景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Hot Region Scheduler —— 解决什么问题？              │
│                                                               │
│  TiKV 数据分布：                                              │
│  [region 1] [region 2] [region 3] ... [region N]            │
│  每个 Region ≈96MB + 一个 Raft Group                         │
│                                                               │
│  问题：                                                       │
│  ① 热点 Key：某个 Region 被高频访问（秒杀订单、热门商品）    │
│  ② 写入热点：某个 Region 写入量远大于其他                     │
│  ③ 读热点：某个 Region 读 QPS 远大于其他                      │
│  → 单个 TiKV 节点扛不住 → 性能瓶颈！                        │
│                                                               │
│  PD Hot Region Scheduler 解决方案：                          │
│  ① 识别热点 Region（TiKV 上报 QPS/写入统计）                 │
│  ② 热点 Region 迁移到空节点                                  │
│  ③ 热点 Region 增加副本（Read 场景副本越多越能分担）          │
│  ④ 热点 Key 拆分（Region Split 成多个小 Region）             │
│                                                               │
│ ★ 和 K8s HPA 类比：                                           │
│   K8s HPA：CPU 高 → 加 Pod                                    │
│   TiKV Hot Scheduler：Region 热点 → 迁移/加副本/Split       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiKV Region 上报热点数据

```go
// ★ TiKV Region 上报热点数据（PD Hot Scheduler 数据来源）
//
// TiKV 每个 Region 周期性上报（默认 10s）：
//
// message RegionStats {
//     uint64 region_id = 1;
//     uint64 leader_count = 2;        // Leader 数量（通常 1）
//     uint64 leader_weight = 3;       // Leader 权重（默认 1000）
//     uint64 leader_score = 4;        // Leader 得分（QPS 计算）
//     uint64 leader_requests = 5;     // Leader QPS
//     uint64 leader_reads = 6;        // Leader 读 QPS
//     uint64 leader_writes = 7;       // Leader 写 QPS
//     uint64 leader_cpu_usage = 8;    // CPU 使用率
//     uint64 leader_memory_usage = 9; // 内存使用率
//     uint64 peer_count = 10;         // Peer 总数
//     uint64 peer_score = 11;         // Peer 得分
//     uint64 peer_requests = 12;      // Peer QPS
//     uint64 peer_reads = 13;
//     uint64 peer_writes = 14;
// }
//
// ★ PD 收到上报后：
//   storeStats[storeID].regionStats[regionID] = regionStats
//   → 记录每个 Region 的 QPS/读写/CPU
//   → Hot Region Scheduler 定期扫描 → 找热点
//
// ★ TiKV Raft Leader 上报热点原因？
//   Raft Leader 是读写入口（所有请求都到 Leader）
//   → Leader 的 QPS/读写 = Region 的 QPS/读写
//   → Follower 只同步日志（QPS 低）
//
```

---

## 三、Hot Region Scheduler 核心算法

### 3.1 三种热点类型

```go
// ★ TiKV Hot Region Scheduler 处理三种热点（面试高频！）
//
// ① Write Hot（写热点）：
//    Region 写 QPS > hot_write_threshold（默认 1000）
//    → 场景：秒杀订单、批量写入
//    → 解决方案：Region Split + 迁移 Leader
//
// ② Read Hot（读热点）：
//    Region 读 QPS > hot_read_threshold（默认 10000）
//    → 场景：热门商品查询、排行榜
//    → 解决方案：增加 Follower 副本 + 迁移（读可以走 Follower）
//
// ③ CPU Hot（CPU 热点）：
//    Region Leader CPU > hot_cpu_threshold（默认 80%）
//    → 场景：复杂查询（聚合/Sort）在 TiKV 执行
//    → 解决方案：迁移 Leader 到 CPU 低的节点
//
// ★ 阈值配置（pd-ctl config show hot-region）：
//   hotspot.high-write-ratio = 0.8    // 写 QPS 超过平均值 80% → 热点
//   hotspot.high-read-ratio = 0.8     // 读 QPS 超过平均值 80% → 热点
//   hotspot.low-write-ratio = 0.5     // 写 QPS 低于平均值 50% → 冷却
//   hotspot.low-read-ratio = 0.5      // 读 QPS 低于平均值 50% → 冷却
//   hotspot.region-write-flow-threshold = 64MB/s  // 写流量阈值
//   hotspot.region-read-flow-threshold = 256MB/s  // 读流量阈值
```

### 3.2 Hot Peer / Hot Leader 处理流程

```go
// ★ Hot Peer（Region 的某个 Peer 在某个 Store 上热点）处理流程
// ★ Hot Leader（Region 的 Leader 在某个 Store 上热点）处理流程
//
// PD Hot Region Scheduler.Schedule() {
//     // 每 1min 运行一次（配置：scheduler-check-interval）
//
//     // 步骤 1：收集热点 Peer
//     hotPeers := collectHotPeers()
//     // 过滤：写 QPS 高 + 读 QPS 高 + CPU 高
//
//     // 步骤 2：收集热点 Leader
//     hotLeaders := collectHotLeaders()
//
//     // 步骤 3：为每个热点 Peer/Leader 生成 Operator
//     // Operator 是 PD 发给 TiKV Raft 层执行的操作
//
//     for _, peer := range hotPeers {
//         // Hot Peer → 迁移 Peer 到空节点
//         targetStore := findColdStore(peer.storeID)
//         op := AddPeerOperator(peer.regionID, targetStore)
//         op := RemovePeerOperator(peer.regionID, peer.storeID)
//         // → TiKV Raft 执行：AddPeer → RemovePeer
//     }
//
//     for _, leader := range hotLeaders {
//         // Hot Leader → 迁移 Leader 到空节点
//         targetStore := findColdLeaderStore(leader.storeID)
//         op := TransferLeaderOperator(leader.regionID, targetStore)
//         // → TiKV Raft 执行：Leader 移交（AppendEntries RPC）
//     }
//
//     // 步骤 4：Operator 下发给 TiKV
//     // TiKV Raft 层接收 Operator → 执行 → 上报完成
// }
//
// ★ 关键：迁移 Leader vs 迁移 Peer 成本
//   TransferLeader：快！（毫秒级，只是 Raft 内部移交）
//   AddPeer+RemovePeer：慢！（需要 Snapshot 或 Log 复制，秒-分钟级）
//   → Hot Leader → 优先 TransferLeader
//   → Hot Peer → 必须 AddPeer+RemovePeer
//
```

### 3.3 FindTarget（找目标节点）

```go
// ★ Hot Scheduler 找目标节点算法（面试高频！）
//
// // ① 优先找 Store 容量足够的
// func findColdStore(excludeStoreIDs []uint64) *Store {
//     // 过滤掉：下线 / 正在调度 / 已排除
//     stores := filterOfflineStores(excludeStoreIDs)
//     if len(stores) == 0 { return nil }
//
//     // 计算每个 Store 的负载得分
//     // storeScore = leader_score * weight + peer_score * weight
//     // weight: 默认 Leader 权重 2.0 + Peer 权重 1.0
//
//     // 找得分最低的 Store（最空闲）
//     sort.Slice(stores, func(i, j int) bool {
//         return stores[i].score < stores[j].score
//     })
//
//     // 但不能低于总容量 10%（保底）
//     target := stores[0]
//     if target.score < avgScore * 0.1 {
//         target = stores[1] // 次优
//     }
//     return target
// }
//
// // ② 过滤目标节点不能有冲突
// // - 目标 Store 上已不能有同 Region 的 Peer（防止多副本同节点）
// // - 目标 Store CPU > 70% → 不适合（否则热点转移过去没用）
// // - 目标 Store 磁盘使用率 > 90% → 不适合（快满了）
//
// ★ 和 K8s Scheduler 类比：
//   K8s：Filter（过滤）→ Score（打分）→ Bind（绑定）
//   TiKV：Filter（过滤）→ Score（打分）→ Operator下发（执行）
//
```

### 3.4 Region Split 触发

```go
// ★ Hot Scheduler 可以触发 Region Split（面试高频！）
//
// 场景：某个 Region 持续热点 + 迁移解决不了（比如热点 Key 在这个 Region 内）
// → PD 自动 Split Region！
//
// Hot Scheduler 触发 Split 条件：
//   ① Region 是热点（QPS > hot_threshold）
//   ② Region 有连续 split_score（比如 5 次调度都想迁移这个 Region）
//   ③ Region 大小 > split_size（默认 96MB）
//   → PD 生成 SplitOperator → TiKV 执行 Split
//
// SplitOperator 下发：
//   op := SplitOperator(regionID, splitKey)
//   → TiKV 从 Region 中间 Split 成两个 Region
//   → 每个 Region 独立 Raft Group
//   → 热点 Key 可能被分到新 Region → 减少压力
//
// ★ 和手动 Split 区别：
//   手动 Split：pd-ctl operator add split-region <regionID>
//   自动 Split：Hot Scheduler 触发 → 无需人工
//
// ★ Split 成本：
//   需要 Snapshot 或 Log 复制（类似 AddPeer）
//   → 后台异步执行 → 不阻塞读写
//
```

---

## 四、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Hot Scheduler 解决什么问题？| **Region 热点**（QPS 高 / 写流量大 / CPU 高）→ 迁移 / 加副本 / Split |
| 2 | 三种热点类型？| **Write Hot（写 QPS）/ Read Hot（读 QPS）/ CPU Hot（CPU 使用率）** |
| 3 | TiKV 怎么上报热点？| 每个 Region Leader 周期性上报 **leader_requests/leader_reads/leader_writes/cpu_usage** → PD 收到存 storeStats |
| 4 | Hot Scheduler 核心算法？| 收集热点 Peer/Leader → **FindTarget（找冷 Store）** → 生成 Operator → TiKV Raft 执行 |
| 5 | TransferLeader vs AddPeer+RemovePeer？| **TransferLeader：毫秒级（Raft 内部移交，快！）**；AddPeer+RemovePeer：秒-分钟级（Snapshot/Log 复制，慢）|
| 6 | FindTarget 怎么找目标 Store？| **打分排序（leader_score * 2.0 + peer_score * 1.0）** → 找得分最低的（最空闲） → 过滤冲突（CPU >70% / 磁盘 >90%）|
| 7 | Hot Scheduler 可以触发 Split？| ✅ 条件：**Region 是热点 + 连续 split_score + Region 够大** → SplitOperator → 分成两个 Region |
| 8 | 热点阈值配置？| high-write-ratio=0.8 / high-read-ratio=0.8 / low-write-ratio=0.5 / region-write-flow-threshold=64MB/s |
| 9 | Hot Scheduler 为什么不阻塞？| Operator 是异步下发 → TiKV Raft 后台执行 → 不阻塞 PD |
| 10 | 热点 Key 怎么办？| Region Split 把热点 Key 分到新 Region → 还是热点的话只能**业务优化**（Key 分散 / Redis 缓存）|
| 11 | Read Hot 为什么可以加 Follower？| **读请求可以走 Follower Raft 副本** → Follower 越多分担越多 → 需要 TiKV 开启 ReadIndex/Lease Read |
| 12 | Hot Scheduler 学习路径？| pd-ctl store region → pd-ctl scheduler config show hot-region → 看 TiKV RegionStats 上报 → 模拟热点场景 |
| 13 | K8s HPA vs TiKV Hot Scheduler？| 都是负载均衡；K8s 加 Pod；TiKV 迁移 Region/加副本/Split |
| 14 | Hot Scheduler 可能出问题？| ① 频繁迁移（Flap 抖动）→ 冷却期 low-ratio ② 迁移成本大（Snapshot 占带宽）→ 限制并发 ③ 目标 Store 变成新热点 |
| 15 | PD 面试最高频考点？| **Hot Region Scheduler + Balance Scheduler + Tso 分配 + Region Split**；PD 是 TiDB 架构最复杂的组件 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV PD v8.0.x（Go）  
**面试考点：15 题**  
**差异化点**：大厂面试高频 Hot 问题 + 完整处理流程
