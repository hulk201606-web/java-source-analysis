# TiDB PD TSO 全局时间戳 + 混合负载调度源码级深度解析

> **源码基线**：TiDB v8.0.x + PD v8.0.x（Go）  
> **核心位置**：pd/pkg/tsop/ + pkg/schedule/ + resource-manager/  
> **面试权重**：⭐⭐⭐⭐（TSO 深化 + HTAP 资源调度，区分度高）

---

## 一、TSO 分配机制深化（复习+源码）

### 1.1 TSO 64bit 结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TSO（Timestamp Oracle）= int64 全局时间戳                │
│                                                               │
│  ┌──────────────────────┬──────────────────┐               │
│  │ Physical 物理（46bit）│ Logical 逻辑（18bit）│              │
│  │ 毫秒（相对 epoch）   │ 同毫秒序号 0~262143 │              │
│  └──────────────────────┴──────────────────┘               │
│                                                               │
│  组合：tso = physical << 18 | logical                       │
│  同毫秒可分配 2^18 ≈ 26 万个不重复时间戳                     │
│                                                               │
│  // pd/pkg/tsop/allocators.go                                │
│  func composeTS(physical, logical int64) uint64 {            │
│      return uint64(physical)<<18 | uint64(logical&0x3FFFF)  │
│  }                                                            │
│
└─────────────────────────────────────────────────────────────┘
```

### 1.2 三种分配模式（PD 8.0 演进，面试加分）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD TSO 分配三代演进（面试高频）                            │
│                                                               │
│  ① 全局 TSO（传统模式）：                                     │
│    只有 PD Leader 分配，所有 TiDB 向 PD Leader 取            │
│    TiDB 客户端批量预取（默认 8192 个/批）+ Future 异步       │
│    适合：单数据中心                                           │
│                                                               │
│  ② ★ 全局 TSO + 批量预取（核心优化，复习）：                 │
│    TiDB 本地缓存一批 TSO，用完前异步预取下一批               │
│    本地分配零 RPC，百万级/s                                  │
│    不同 TiDB 拿不同连续区间，仍全局单调                      │
│                                                               │
│  ③ ★ Microservice 模式（PD 8.0 拆分）：                      │
│    PD 拆成多个微服务：                                        │
│      - scheduling（调度）                                     │
│      - tso（★独立 TSO 服务，可多副本）                        │
│      - resource-manager（资源管控）                          │
│      - etcd（元数据）                                        │
│    TSO 独立部署、独立扩缩容，缓解大集群 PD 压力              │
│
└─────────────────────────────────────────────────────────────┘
```

### 1.3 高可用与不回退保证

```go
// ★ TSO Leader 切换防回退（源码级，复习）
//
// 新 TSO Leader 启动：
//  ① 从 etcd 读最后分配的 TSO（lastTSO）
//  ② ★ sleep 等待本地墙上时钟 > lastTSO.physical
//     （updatePhysical 保证物理时间严格前进）
//  ③ 加安全窗口，再开始分配
//
// 关键约束：
//   - PD 节点依赖 NTP 时钟同步（不能跨时区/时钟漂移大）
//   - TSO 不是 TrueTime：没有原子钟/GPS，用等待兜底
//
// ★ PD 8.0 Microservice 下 TSO 多副本：
//   tso service 可以多副本，但同一 keyspace 同一时刻
//   只有一个 primary allocator（Raft/etcd 选主）
//   副本间靠 etcd 同步 watermark，切换同样等待防回退
//
```

---

## 二、混合事务/分析负载（HTAP）调度

### 2.1 TiDB 如何区分 TP/AP 请求

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB HTAP 负载隔离（面试核心）                            │
│                                                               │
│  TiDB 层：优化器选择存储                                      │
│   TP 点查/短事务 → TiKV（行存）                              │
│   AP 大扫描/聚合 → TiFlash（列存，Raft Learner）             │
│   选择方式：CBO 自动 或 Hint READ_FROM_STORAGE(TIFLASH)     │
│                                                               │
│  ★ 资源层面的隔离（HTAP 关键，避免 AP 拖垮 TP）：           │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 TiKV 资源管控（Resource Control）

```go
// ★ TiDB 7.1+ Resource Control（资源组调度，面试高频）
//
// pd/pkg/resourcemanager/ + tidb 资源组
//
// CREATE RESOURCE GROUP rg_ap
//   RU_PER_SEC = 500
//   PRIORITY = LOW;
//
// CREATE RESOURCE GROUP rg_tp
//   RU_PER_SEQ = 2000
//   PRIORITY = HIGH;
//
// ★ RU（Request Unit）= 标准化资源计量单位：
//   CPU + IO + 网络 + 存储的综合抽象
//   读/写/扫描不同操作折算成 RU
//
// 调度机制（源码层）：
//  ① 每个 SQL 请求绑定 Resource Group（用户/会话绑定）
//  ② TiDB/TiKV 用令牌桶按 RU_PER_SEC 限速
//  ③ ★ 空闲资源借用（Burstable）：
//     AP 组平时限制 500 RU，系统空闲时可超用空闲 RU
//     TP 组需要资源时，AP 立即让回（优先级抢占）
//  ④ TiKV 侧用优先级队列调度 read/write request
//     高优先级 TP 请求插队，AP 大查询让路
//
// ★ 效果：
//   TP 交易延迟不被 AP 大查询影响（资源隔离）
//   AP 利用空闲资源（不浪费）
//   → 这就是"混合负载"在资源调度层的落地
//
```

### 2.3 物理隔离：TiFlash 独立节点

```
┌─────────────────────────────────────────────────────────────┐
│  ★ HTAP 最强隔离：TiFlash 独立部署（面试必背）               │
│                                                               │
│  逻辑隔离（同一 TiKV 节点混跑）：                             │
│   AP/T P 共享节点资源，靠 Resource Control 软隔离            │
│                                                               │
│  ★ 物理隔离（生产推荐）：                                    │
│   TiKV 节点只跑 TP（独立机器）                                │
│   TiFlash 节点只跑 AP（独立机器，独立 CPU/内存/IO）         │
│   数据通过 Raft Learner 异步复制到 TiFlash（不占 TP 多数派）│
│   → AP 大查询的向量化/MPP 计算完全不消耗 TiKV 资源          │
│                                                               │
│  Placement Rules 配合（之前文档讲过）：                       │
│   engine=tiflash 的 rule 把列存副本调度到 TiFlash 节点       │
│   label 约束保证 TiFlash/TiKV 物理分离                       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、后台任务调度与 AP 的协调

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 如何避免后台调度影响在线事务（面试加分）              │
│                                                               │
│  ① 调度限速：                                                 │
│   region-schedule-limit / replica-schedule-limit             │
│   控制同时进行的 Operator 数量，避免迁移风暴                │
│                                                               │
│  ② Store 限流（Store Limit）：                               │
│   每个节点每分钟最多 add/remove leader/region                │
│   平衡速度和稳定性                                           │
│                                                               │
│  ③ 流量抖动感知：                                            │
│   hot-region scheduler 检测到节点高负载时暂停向它调度       │
│                                                               │
│  ④ TiFlash 同步优先级：                                      │
│   Learner 复制 AP 列存不影响 TP Raft 提交（不投票）         │
│   TiFlash 落后只影响 AP 数据新鲜度，不阻塞 TP                │
│                                                               │
│  ⑤ 读一致性协调（复习）：                                     │
│   TiFlash 查询带 TSO，等 Raft index 追上 → ReadIndex        │
│   AP 读到一致性快照，但不阻塞 TP 写入                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、PD Microservice 与调度扩展

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 8.0 微服务化架构（面试高频新趋势）                    │
│                                                               │
│  传统 PD 单体：一个进程同时做 TSO + 调度 + 元数据            │
│   大集群（百万 Region）下调度和 TSO 互相影响                │
│                                                               │
│  Microservice 模式（PD 8.0 GA 方向）：                       │
│  ┌────────────┬────────────┬────────────┐                  │
│  │ tso service│ scheduling │ resource-mgr│                  │
│  │ （时间戳）  │ service    │ service    │                  │
│  │ 可多副本    │ （调度器）  │（资源管控） │                  │
│  └────────────┴────────────┴────────────┘                  │
│              ↓ 共享 etcd（元数据 + Raft）                     │
│                                                               │
│  好处：                                                        │
│   ① TSO 与调度解耦，大集群 TSO 不被调度阻塞                 │
│   ② 各服务独立扩缩容                                         │
│   ③ scheduling 可以多 worker 提升调度吞吐                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TSO 结构？| Physical 46bit（ms）<<18 \| Logical 18bit（26 万/ms）|
| 2 | 谁能分配 TSO？| 只有 PD Leader（Microservice 下 tso primary）|
| 3 | TiDB 如何避免 RPC 瓶颈？| **批量预取一批（8192）本地分配 + Future 异步预取** |
| 4 | Leader 切换防回退？| 读 lastTSO → sleep 等墙上时钟超过 lastPhysical + 安全窗口 |
| 5 | TSO vs Spanner TrueTime？| TSO 普通时钟+等待防回退；TrueTime 原子钟 GPS + CommitWait |
| 6 | PD Microservice？| 8.0 拆 tso/scheduling/resource-manager 独立服务，共享 etcd，独立扩缩容 |
| 7 | TiDB 怎么区分 TP/AP？| CBO 自动选 TiKV/TiFlash 或 Hint；TP→行存，AP→列存 |
| 8 | HTAP 资源隔离手段？| **Resource Control（RU 软隔离）+ TiFlash 独立节点（物理隔离）** |
| 9 | RU 是什么？| Request Unit：CPU/IO/网络/存储的标准化计量单位 |
| 10 | 资源组限速原理？| 令牌桶按 RU_PER_SEC 限速 + 优先级队列 |
| 11 | Burstable 借用？| AP 空闲时超用空闲 RU；TP 需要时 AP 立即让路 |
| 12 | 最强 HTAP 隔离？| **TiFlash 独立物理节点 + Learner 异步复制 + Placement Rules** |
| 13 | TiFlash 会拖慢 TP 吗？| 不会；Learner 不投票，AP 计算在独立节点 |
| 14 | AP 读一致性？| 带 TSO + ReadIndex 等 Raft index 追上，快照一致不阻塞写 |
| 15 | PD 调度限速？| schedule-limit + Store Limit 控制 Operator 并发防迁移风暴 |
| 16 | 面试最高频考点排序？| **1. TSO 批量预取 2. 防回退 3. RU 资源管控 4. TiFlash 物理隔离 5. Microservice** |
| 17 | 学习路径？| tsop allocator → PD 选主防回退 → resource-manager RU → scheduling limit → microservice |
| 18 | 面试常见追问？| AP 大查询影响交易吗？→ Resource Control 让路 + TiFlash 独立节点双重保障 |
| 19 | 资源组怎么绑定？| CREATE RESOURCE GROUP + 用户/会话绑定 resource_group |
| 20 | PD 全挂能开新事务吗？| TiDB 本地预取 TSO 短期可用，耗尽则阻塞；已有读写不受影响 |
| 21 | 跨地域 TSO？| TiDB 用全局 TSO 不适合跨地域高延迟；多中心用 Placement Rules + 本地事务优化 |
| 22 | 面试陷阱？| 说 TiFlash 是独立集群 → ❌ 它是 TiKV 的 Raft Learner 列存副本，同一数据库 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：pd/pkg/tsop/ + resource-manager/ + schedule/  
**面试考点：22 题**  
**差异化点**：TSO 三代演进/微服务 + RU 资源管控 Burstable + TiFlash 物理隔离 + 调度限速
