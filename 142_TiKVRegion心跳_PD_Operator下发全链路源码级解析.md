# TiKV Region 心跳 + PD Operator 下发全链路源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）+ PD v8.0.x（Go）  
> **核心仓库**：tikv/tikv → components/raftstore/；tikv/pd → pkg/schedule/  
> **面试权重**：⭐⭐⭐⭐（PD 调度如何变成 TiKV 实际动作的闭环）

---

## 一、调度闭环全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 调度是一个持续的"心跳上报 → 决策 → 下发 → 回执"闭环  │
│                                                               │
│  TiKV（数据面）                    PD（控制面）               │
│  ┌──────────────┐   StoreHeartbeat  ┌──────────────┐         │
│  │ raftstore     │ ───────────────→ │ Scheduler     │         │
│  │ （每个 Store） │  容量/磁盘/负载   │ （决策大脑）   │         │
│  └──────────────┘                  └──────┬───────┘         │
│  ┌──────────────┐   RegionHeartbeat       │ 生成 Operator    │
│  │ 每个 Region   │ ───────────────→ ┌──────↓───────┐         │
│  │ Leader        │  Peer/Leader/流量 │ Operator     │         │
│  └──────────────┘                  │ Controller   │         │
│        ↑                           └──────┬───────┘         │
│        │ 心跳响应里携带 Operator 命令      │ 下发             │
│        └───────────────────────────────────┘                 │
│                    TiKV 执行后下次心跳上报结果                │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、两类心跳源码级

### 2.1 StoreHeartbeat（节点级）

```rust
// ★ TiKV Store 心跳（源码级）
//
// components/raftstore/src/store/pd.rs
//
// TiKV 周期性（store-heartbeat-interval，默认 10s）
// 向 PD Leader 发送 StoreHeartbeat：
//
// message StoreHeartbeatRequest {
//     capacity: 总磁盘容量
//     available: 可用容量
//     used_size: 已用（所有 Region 数据大小）
//     region_count: Region 总数
//     leader_count: Leader 总数（含 learner_count）
//     // 8.x 还带 CPU、磁盘 IO、bytes read/written、keys read/written
//     // 引擎信息（tikv / tiflash）、start_time
// }
//
// PD 收到后更新内存里的 StoreInfo（容量/负载视图）
// → balance-region 看 used_size/region_count
// → balance-leader 看 leader_count
// → 空间不足触发补副本/迁移动作
//
```

### 2.2 RegionHeartbeat（Region 级，调度信息主来源）

```rust
// ★ Region 心跳（源码级，面试核心！）
//
// 只有 Region 的 Leader Peer 才上报 RegionHeartbeat
// 默认也是 10s（pd-heartbeat-tick-interval），但变更时立即上报
//
// message RegionHeartbeatRequest {
//     region: { id, start_key, end_key, peers }   // Region 拓扑
//     leader: Peer                                 // 当前 Leader
//     down_peers: []        // 掉线的 Peer + 持续时间
//     pending_peers: []     // 正在追数据的 Peer
//
//     // ★ 负载信息（热点调度依据）
//     bytes_read / bytes_written     // 读写字节
//     keys_read / keys_written       // 读写 Key 数
//     cpu_usage / written_bytes / approximate_size / approximate_keys
//
//     term, region_epoch           // 版本（成员/范围变更时递增）
// }
//
// PD 用这些信息做两件事：
//  ① 维护全局 Region 路由表（RegionInfo）
//  ② 喂给各 Scheduler 判断是否需要调度（balance / hot / 补副本）
//
```

---

## 三、PD 生成 Operator

### 3.1 Scheduler 调度决策

```go
// ★ PD Scheduler 如何产出 Operator（源码级）
//
// pkg/schedule/scheduler.go
//
// type Scheduler interface {
//     Schedule(opController *OperatorController) []*Operator
// }
//
// 每个 Scheduler 周期性执行 Schedule()：
//  ① 从缓存的 Store/Region 信息挑出不满足期望的对象
//  ② 用过滤器链选出符合约束的目标 Store（Placement Rules）
//  ③ 构造 Operator（一串 OpStep）
//
// 举例：balance-leader（Leader 不均）
//  源 Store：leader_count 最高
//  目标 Store：leader_count 最低且 Label 兼容
//  产出 Operator：[TransferLeader(region, fromPeer, toPeer)]
//
// 举例：balance-region（副本/容量不均 或 缺副本）
//  产出 Operator：
//   [AddLearner(newStore), PromoteLearner, ChangeLeader?, RemovePeer(oldStore)]
//
// 举例：hot-region（某 Region 读写流量高）
//  产出 Operator：把热点 Peer 迁到负载低的 Store
//
```

### 3.2 Operator 的结构与检查

```go
// ★ Operator（调度操作符，源码级）
//
// pkg/schedule/operator.go
//
// type Operator struct {
//     regionID, regionEpoch
//     kind     // 类型：leader/region/hot/merge/split...
//     steps    []OpStep   // ★ 有序步骤链
//     status   // CREATED → SUCCESS / TIMEOUT / EXPIRED
// }
//
// type OpStep interface {
//     // 转成发给 TiKV 的 raft 命令
// }
// 典型 step：
//   AddLearnerNode   // 先加无投票权 Learner
//   PromoteLearner   // 追平后提升为 Voter
//   TransferLeader   // 转移 Leader
//   RemoveNode       // 删除旧 Peer
//   MergeRegion / SplitRegion
//
// // OperatorController：
// //  - limit 当前并发 Operator 数（防止同时调度太多压垮集群）
// //  - 校验 regionEpoch（防止基于过期拓扑下发）
// //  - 把待执行 step 暂存，等待心跳响应捎带给 TiKV
//
```

---

## 四、Operator 下发链路（核心！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 不主动 RPC 调 TiKV，而是"搭心跳响应的便车"（面试必背）│
│                                                               │
│  关键点（高频追问）：PD 怎么把 Operator 发给 TiKV？          │
│                                                               │
│  ① Region Leader 周期性发 RegionHeartbeatRequest 给 PD       │
│  ② PD 处理心跳时，发现该 Region 有待执行的 Operator          │
│  ③ PD 把当前要执行的 step 打包进                              │
│     ★ RegionHeartbeatResponse（心跳响应！）                  │
│  ④ Leader TiKV 收到响应，取出 AdminRequest/ChangePeer 命令   │
│  ⑤ 通过 Raft 走一遍正常日志流程：                            │
│     ChangePeer / TransferLeader / Compact / Split...        │
│     → 提议 Raft Log → Quorum 复制 → Commit → Apply         │
│  ⑥ TiKV 执行结果通过【下一次 RegionHeartbeat】回报 PD        │
│  ⑦ PD 发现 step 完成 → 推进 Operator 到下一个 step          │
│     全部 step 完成 → Operator SUCCESS                        │
│                                                               │
│  // PD 侧：service/.../region.go                             │
│  // RegionHeartbeatResponse {                                │
│  //   region_id, target_version,                             │
│  //   admin_request?: ChangePeer/TransferLeader/...          │
│  // }                                                        │
│                                                               │
│  // TiKV 侧：raftstore 处理心跳响应 → 写入 proposal 队列     │
│  // components/raftstore/src/store/fsm/peer.rs                │
│
└─────────────────────────────────────────────────────────────┘
```

### 为什么借心跳通道而不是独立 RPC

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 心跳响应下发的设计优势（面试加分点）                      │
│                                                               │
│  ① 天然只有活着的 Region Leader 会上报 → 命令一定发给 Leader │
│  ② 复用长连接和心跳通道，连接数少、实现简单                  │
│  ③ PD 无状态下发：下发后靠心跳确认结果，不怕响应丢失        │
│     （心跳还会再来，PD 重试下发同一 step）                   │
│  ④ 每个操作都通过 Raft 日志复制 → 保证一致性、可持久化      │
│  ⑤ regionEpoch 校验：拓扑变了旧 Operator 自动失效（EXPIRED）│
│                                                               │
│  超时处理：                                                    │
│   Operator 有超时时间（默认较长），长时间没通过心跳确认完成  │
│   → TIMEOUT / EXPIRED，PD 重新评估调度                       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、典型调度完整时序（Add Peer）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 补一个缺失副本的完整链路（面试串联题）                    │
│                                                               │
│  ① Store3 宕机，超过 max_down_time（默认 30min）             │
│  ② 其 StoreHeartbeat 消失 + Region 心跳报 down_peers         │
│  ③ ReplicaChecker/RuleChecker 检测到 Region 缺满足规则副本  │
│  ④ 生成 Operator：AddLearner(Store5) → PromoteLearner       │
│  ⑤ 等该 Region Leader 下次心跳，响应中带 AddLearner 命令     │
│  ⑥ Leader 走 Raft ConfChange：加 Learner，Quorum 提交        │
│  ⑦ Learner 在 Store5 上追快照/日志（pending peer）          │
│  ⑧ 追平后下次心跳，PD 下发 PromoteLearner 提升为 Voter       │
│  ⑨ 旧 Store3 上的死 Peer 由 RemovePeer 清掉                  │
│  ⑩ 心跳确认 → Operator SUCCESS，集群副本恢复 3 副本          │
│                                                               │
│  Region Split/Merge 同理：PD 下发 Admin 命令 → Raft 提交 →  │
│  TiKV Apply 后拓扑变化 → 心跳上报新 epoch → PD 更新路由     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | PD 调度闭环？| **心跳上报 → Scheduler 决策 → Operator → 心跳响应下发 → Raft 执行 → 心跳回执** |
| 2 | 两类心跳？| **StoreHeartbeat（节点容量/负载）+ RegionHeartbeat（Peer/Leader/流量，仅 Leader 发）** |
| 3 | Store 心跳内容？| capacity/available/used_size/region_count/leader_count/CPU/IO |
| 4 | Region 心跳内容？| region 拓扑+leader+down/pending peers + bytes/keys read/written + epoch |
| 5 | 心跳间隔？| 默认 10s；拓扑变更时立即上报 |
| 6 | Operator 是什么？| 有序 OpStep 链（AddLearner/Promote/TransferLeader/RemovePeer/Split/Merge）|
| 7 | Operator 怎么下发？| **不主动 RPC，打包进 RegionHeartbeatResponse 捎带给 Leader** |
| 8 | TiKV 收到后怎么执行？| 取出 AdminRequest → 走 Raft 提议→Quorum→Apply |
| 9 | 怎么确认完成？| **下一次 RegionHeartbeat 上报新状态，PD 推进下一 step** |
| 10 | 为什么借心跳通道？| 只发 Leader + 复用连接 + 丢了可重发 + 走 Raft 保证一致 |
| 11 | regionEpoch 作用？| 防止基于过期拓扑执行；拓扑变了旧 Operator EXPIRED |
| 12 | Operator 超时？| 长期没心跳确认完成 → TIMEOUT/EXPIRED，PD 重新调度 |
| 13 | 补副本完整链路？| down 30min → Checker 生成 AddLearner → 追平 Promote → Remove 旧 Peer |
| 14 | 热点信息从哪来？| Region 心跳的 bytes/keys read/written → hot-region scheduler |
| 15 | 面试最高频考点排序？| **1. Operator 借心跳响应下发 2. 两类心跳内容 3. Operator step 链 4. 补副本时序 5. epoch** |
| 16 | 学习路径？| pd.rs 心跳 → scheduler.go → operator.go → service region 心跳响应 → peer.rs apply |
| 17 | 面试常见追问？| PD 挂了还能调度吗？→ 不能新调度，但已有 Raft 读写/自动选主不受影响 |
| 18 | 并发 Operator 限制？| OperatorController 限流，避免一次调度太多迁移压垮集群 |
| 19 | 谁能发 Region 心跳？| 只有 Region Leader；Follower/Learner 不发 |
| 20 | down_peers vs pending_peers？| down=疑似挂了的副本；pending=正在追数据还没跟上的副本 |
| 21 | Split/Merge 怎么走？| PD 下发 Admin 命令 → Raft 提交 Apply → 新 epoch 心跳上报 |
| 22 | 面试陷阱？| 说 PD 直接 RPC 命令 TiKV 迁副本 → ❌ 是搭 Region 心跳响应便车，且必须经 Raft 日志提交 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：components/raftstore/src/store/pd.rs + pd/pkg/schedule/operator.go + service/region.go  
**面试考点：22 题**  
**差异化点**：两类心跳字段 + Operator step 链 + 心跳响应捎带下发 + Raft 执行回执闭环 + epoch 失效
