# TiKV PD（Placement Driver）源码深度解析（Region 调度核心）

> **源码基线**：TiKV PD v8.0.x（Go 实现）  
> **核心仓库**：tikv/pd  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 架构第三核心组件，Region 分片+调度+Tso）

---

## 一、PD 核心定位（★ 面试必知）

```
┌─────────────────────────────────────────────────────────────┐
│  PD = TiDB 架构的"大脑"——**Region 调度 + 元数据管理**       │
│                                                               │
│  TiDB 三层架构（完整）：                                      │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ TiDB Server（SQL 层，Go）                             │   │
│  │   SQL 解析 + CBO + MPP                                │   │
│  └───────────────┬───────────────────────────────────────┘   │
│                  ↓ 查 Region 元数据                            │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ PD（Placement Driver，Go）★ 核心！                   │   │
│  │   ┌──────────────┬──────────────┬──────────────┐       │   │
│  │   │ Region 调度   │ **Tso Provider**  │ **元数据管理**  │   │
│  │   │ Balance/Split │ 全局时间戳   │ Region 路由表  │       │   │
│  │   └──────────────┴──────────────┴──────────────┘       │   │
│  └───────────────┬───────────────────────────────────────┘   │
│                  ↓ Raft + Percolator                          │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ TiKV（存储层，Rust）                                  │   │
│  │   Percolator 2PC + Raft Group + RocksDB               │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│ ★ PD 三大核心职责（面试高频！）                               │
│  ① Region 路由（TiDB Server 查：key 在哪个 Region）        │
│  ② Region 调度（Balance/Split/Merge/Transfer）            │
│  ③ Tso 全局时间戳（Percolator 事务开始/提交时间）          │
│                                                               │
│ ★ PD 高可用：Raft 选主（奇数 3/5 个 PD）                    │
│   Leader PD 负责写；Follower 只负责读                       │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Region 分片核心

### 2.1 Region 分片原理（★ 面试必背）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Region 分片（面试高频！每个 Region 是独立 Raft Group）│
│                                                               │
│  初始：整个数据范围一个 Region                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Region 1（[null, null)）                   │   │
│  │     所有数据都在这个 Region 里                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
│  写入 → Region 变大 → PD 自动 Split：                      │
│  ┌───────────────────────┬─────────────────────────────┐   │
│  │ Region 2([null, mid)) │ Region 3([mid, null))       │   │
│  │   前半段数据          │   后半段数据                 │   │
│  └───────────────────────┴─────────────────────────────┘   │
│                                                               │
│  继续写入 → 持续 Split → 几千个 Region（每个 ≈96MB）        │
│                                                               │
│  ★ Region 路由表（PD 管理）：                                │
│  key=id=123 → hash=xxx → 在 Region 2                      │
│  TiDB Server 查 PD → 拿到 Region 2 的 Raft Leader 地址    │
│  → 直接发给 TiKV Region 2 Leader（直连，不经过 PD 转发）  │
│                                                               │
│  ★ Region Split 条件：                                       │
│  Region size > 96MB（默认）→ 后台 Worker 自动 Split        │
│  或手动 Split：pd-ctl operator add add-peer                 │
│                                                               │
│  ★ Region Merge：                                            │
│  两个相邻小 Region → PD 自动 Merge（磁盘省空间）             │
│  条件：两个 Region 都很小（< 64MB）且相邻                  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Region 调度 Balance（★ 面试高频）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD Region Balance 调度（面试高频！解决什么问题？）        │
│                                                               │
│  问题：                                                       │
│  写入不均匀 → 某些 TiKV 节点 Region 多（热点）→ 其他空着    │
│  Region Split 后也不均匀 → 需要 Balance 调度                │
│                                                               │
│  PD 调度器（Scheduler）定期运行（默认 1min）：               │
│                                                               │
│  ① Leader Balance Scheduler：                                │
│     目标：每个 TiKV 节点 Leader 数量基本均衡                 │
│     → Leader 是读写入口 → Leader 多 = 负载高               │
│     → 把 Region Leader 从多的节点转移到少的节点            │
│     → Raft 层：Transfer Leader（旧 Leader 移交新 Leader）   │
│
│  ② Region Balance Scheduler：                                │
│     目标：每个 TiKV 节点 Region 总数（Leader+Follower）均衡 │
│     → Region 数据量大（热点）→ 迁移到空节点                │
│     → Raft 层：Add Learner → Add Peer → Remove Old Peer    │
│
│  ③ Hot Region Scheduler：                                    │
│     目标：热点 Region 分散到不同节点                        │
│     → 扫描 TiKV 上报的 Region 读写统计                     │
│     → 热点 Region（QPS 高/写入大）→ 迁移/增加副本          │
│     → 比如某个 Region 被频繁访问 → 多放几个副本分担压力    │
│
│  ★ 调度器配置（面试高频）：                                  │
│  balance-region-scheduler-enable = true                    │
│  balance-leader-scheduler-enable = true                    │
│  hot-region-scheduler-enable = true                        │
│  max-merge-region-size = 64MB（Merge 阈值）                │
│  max-snapshot-count = 3（同时 Snapshot 数量，控制带宽）    │
│
│  ★ PD Scheduler 核心算法：                                  │
│  计算每个 TiKV 的 Region 数量 → 算平均值 → 超出/低于平均值 │
│  → 生成调度 Operator（AddPeer/RemovePeer/TransferLeader） │
│  → 发给 TiKV 执行（TiKV Raft 层处理）                     │
│  → 类似 Kubernetes Scheduler（Filter → Score → Bind）     │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Tso（全局时间戳）实现

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD Tso（Global Timestamp Oracle）—— Percolator 事务核心！ │
│                                                               │
│  Tso 是什么？                                                 │
│  Percolator 事务需要 start_ts + commit_ts（全局递增）       │
│  → 不能用本地时间（不同 TiKV 时钟可能不同步）               │
│  → 必须 PD 统一分配（保证全局有序）                          │
│
│  Tso 结构（64 位）：                                          │
│  ┌──────────────────────────────────────────┬──────────────┐│
│  │ 高 46 位：物理时间戳（毫秒）              │ 低 18 位：计数 ││
│  │ 2^46 ≈ 2034 年后用完                     │ 2^18 = 262144││
│  └──────────────────────────────────────────┴──────────────┘│
│
│  Tso 生成流程：                                               │
│  PD Leader 维护一个 Counter：                                │
│    lastTimeStamp = 上次分配的物理时间                       │
│    physical = 当前物理时间                                   │
│    logical = 递增计数器（0~262143）                         │
│
│    if (physical > lastTimeStamp) {
│        // 新的毫秒级 → 计数器重置
│        logical = 0;
│        lastTimeStamp = physical;
│    } else {
│        // 同一毫秒 → 计数器 +1
│        logical++;
│    }
│    tso = (physical << 18) | logical;
│
│  ★ Tso 关键设计（面试高频）：                                │
│  ① 保证单调递增：物理时间+计数器 → 全局唯一递增             │
│  ② 高并发：同一毫秒可以分配 262144 个 Tso → 足够          │
│  ③ Raft 持久化：Counter 变化写入 Raft Log → Leader 挂了不丢│
│  ④ 备用时间（LastTS）：万一物理时间回退 → 用 LastTS+1      │
│
│  ★ Tso vs HLC vs TrueTime：                                  │
│  Tso：**PD 中央分配（简单 + 但 PD 成瓶颈）**                │
│  HLC（混合时钟）：本地物理时间 + 逻辑时钟（Spanner 简化版） │
│  TrueTime（Spanner）：原子钟 + GPS（精确但贵 + Google 专属）│
│  → TiKV 选 Tso：简单 + 够用 + 和 PD 整合好                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | PD 三大核心职责？| **① Region 路由（TiDB Server 查 Region 在哪个 TiKV）② Region 调度（Balance/Split/Merge）③ Tso 全局时间戳** |
| 2 | TiDB 三层架构？| **TiDB Server（SQL）→ PD（调度+Tso）→ TiKV（Percolator+Raft+RocksDB）**；每层职责清晰 |
| 3 | Region Split 触发条件？| **Region size > 96MB**（默认）→ PD 后台 Worker 自动 Split；手动用 pd-ctl |
| 4 | Region Balance 三种 Scheduler？| **Leader Balance（每个 TiKV Leader 均衡）/ Region Balance（每个 TiKV Region 数均衡）/ Hot Region（热点分散）** |
| 5 | Tso 结构？| **高 46 位物理时间 + 低 18 位计数器**；同一毫秒 262144 个 Tso；保证全局单调递增 |
| 6 | Tso vs HLC vs TrueTime？| Tso：**PD 中央分配（简单 + 够用）**；HLC：本地物理+逻辑时钟；TrueTime：原子钟+GPS（Spanner） |
| 7 | PD 高可用？| **Raft 选主**（奇数 3/5 个 PD）；Leader 负责写，Follower 读；挂了自动选主 |
| 8 | TiDB Server 怎么查 Region？| **查 PD 元数据** → key hash → 拿到 Region Raft Leader 地址 → 直连 TiKV Leader（不经过 PD 转发）|
| 9 | Region Merge 条件？| **两个相邻 Region 都很小（< 64MB）** → PD 自动 Merge；省磁盘空间 |
| 10 | PD Scheduler 怎么调度？| **算每个 TiKV Region/Leader 数量 → 和平均值比 → 生成 Operator（AddPeer/TransferLeader）→ 发给 TiKV Raft 层执行** |
| 11 | PD 学习路径？| pd-ctl 查询 Region/Store → 看 Leader Transfer 流程 → Hot Region Scheduler → Tso 分配源码 |
| 12 | PD vs ZooKeeper vs Etcd？| PD：**Region 调度 + Tso**（TiDB 专用）；ZK：分布式锁+KV（通用）；Etcd：K8s 元数据（通用）|
| 13 | PD 性能瓶颈？| Tso 分配是中央瓶颈 → 但同一毫秒 26w 个足够高并发；未来可能 HLC 去中心化 |
| 14 | TiKV 上报给 PD 什么？| Region 统计（读写 QPS/数据量/热点）+ Store 状态（存活/离线/容量）→ PD 根据这些做调度决策 |
| 15 | PD 面试最高频考点？| **Region Split + Balance Scheduler + Tso 分配**；TiDB 架构第三组件 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV PD v8.0.x（Go）  
**面试考点：15 题**  
**差异化点**：TiDB 架构第三核心 + Region 分片调度 + Tso 全局时间戳
