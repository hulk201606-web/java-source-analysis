# TiDB TiFlash 列存 + MPP 执行引擎源码级深度解析

> **源码基线**：TiFlash v8.0.x（C++）+ TiDB v8.0.x（Go）  
> **核心仓库**：pingcap/tiflash → dbms/src/ + pingcap/tidb → executor/  
> **面试权重**：⭐⭐⭐⭐⭐（HTAP 核心，TiDB 差异化王牌）

---

## 一、TiFlash 在 TiDB HTAP 架构中的定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiFlash = TiDB 的列存副本 + MPP 分析引擎                 │
│                                                               │
│  TiDB 一套系统同时支持两类负载（HTAP）：                      │
│                                                               │
│  TP（事务处理）：点查、短事务、高并发                          │
│    → TiKV（行存，LSM-Tree + Raft）                           │
│                                                               │
│  AP（分析处理）：大表扫描、聚合、Join、报表                   │
│    → ★ TiFlash（列存 + MPP，本文重点！）                     │
│                                                               │
│  ┌──────────┐  SQL（优化器选行存/列存）                        │
│  │ TiDB Server│─────────────┬──────────────┐                 │
│  └──────────┘              │              │                  │
│        行存（TP）↓          │      列存（AP）↓                │
│  ┌──────────┐         ┌──────────┐                          │
│  │  TiKV    │ Raft Learner │ TiFlash │ ★                      │
│  │ (行存)   │←──异步复制──→│ (列存)   │                        │
│  └──────────┘         └──────────┘                          │
│
│ ★ 关键设计（面试必背）：                                       │
│  TiFlash 作为 Raft Learner 从 TiKV 异步复制数据             │
│  → ★ 不参与投票（Learner 角色），不影响 TP 的 Raft 多数派    │
│  → 同一份数据实时同步成行存 + 列存两份                        │
│  → 资源隔离：AP 大查询不拖垮 TP 交易                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiFlash 列存引擎

### 2.1 行存 vs 列存

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 为什么 AP 用列存？（面试高频！）                          │
│                                                               │
│  行存（TiKV）：一行数据连续存储                               │
│  [id=1,name=Alice,age=25,salary=100][id=2,name=Bob,...]      │
│  SELECT AVG(salary) FROM t（1 亿行）：                       │
│  → 要读全部列（即使只要 salary）→ IO 浪费                    │
│                                                               │
│  列存（TiFlash）：一列数据连续存储                           │
│  id:     [1, 2, 3, ...]                                      │
│  name:   [Alice, Bob, Carol, ...]                            │
│  age:    [25, 30, 28, ...]                                   │
│  salary: [100, 200, 150, ...]  ← 只读这一列！               │
│                                                               │
│  列存三大优势：                                                │
│  ① ★ IO 裁剪：只读查询涉及的列（AVG(salary) 只读 salary 列）│
│  ② ★ 高压缩比：同列数据类型相同 → 编码压缩率极高             │
│     （字典编码、RLE、Delta encoding，通常 5-10x）            │
│  ③ ★ 向量化/SIMD：整列批量计算（和 Doris BE 同样的思想）     │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 TiFlash 存储结构（Delta-Tree 引擎）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiFlash Delta-Tree 存储引擎（源码级，面试核心）           │
│                                                               │
│  挑战：列存擅长批量写，不擅长高频小更新（TP 数据持续同步）   │
│  解决：TiFlash 自研 Delta-Tree 引擎（类似 LSM 思想）         │
│                                                               │
│  数据分两层：                                                  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Delta 层（增量层）：                                   │  │
│  │   接收 Raft 同步来的实时写入（小批量、高频）           │  │
│  │   内存 + 磁盘（写优化，行式缓存近期更新）              │  │
│  └───────────────────────────────────────────────────────┘  │
│          ↓ 后台 Merge（达到阈值）                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Stable 层（稳定层）：                                  │  │
│  │   列式数据文件（DTFile，列存 + 高压缩 + 索引）         │  │
│  │   按 Segment 组织，读优化                              │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  查询时：读 Stable 列文件 + Delta 增量 → 合并结果            │
│  → 兼顾实时写入和列存扫描                                    │
│                                                               │
│  // tiflash/dbms/src/Storages/DeltaMerge/                    │
│  //   DeltaValueSpace（Delta 层）/ StableValueSpace（Stable）│
│  //   Segment → DMFile（列文件）                             │
│  //   rough set index / min-max index（块级裁剪）            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、MPP 执行引擎源码级

### 3.1 MPP vs 传统两阶段

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MPP = Massively Parallel Processing（大规模并行处理）     │
│                                                               │
│  TiKV Coprocessor（之前文档）：下推到存储节点算              │
│    但只有"单 Region 局部聚合 → TiDB 单点汇总"               │
│    → Join、复杂聚合的最终合并都压在 TiDB Server（单点瓶颈）│
│                                                               │
│  ★ TiFlash MPP：计算也分布式并行！                           │
│    多个 TiFlash 节点之间可以 Exchange 数据                   │
│    → Join/Agg 在多个节点并行完成，TiDB 只做最终汇总         │
│                                                               │
│  例子：SELECT a, SUM(*) FROM t1 JOIN t2 ON ... GROUP BY a    │
│                                                               │
│  无 MPP（TiKV 下推）：                                        │
│    每个 Region 局部算 → 全量数据拉回一个 TiDB → Join+Agg    │
│    → TiDB 内存/CPU 单点爆炸                                  │
│                                                               │
│  ★ 有 MPP（TiFlash）：                                       │
│    TiFlash-1: 扫描 t1 分片 ┐                                 │
│                            ├→ Exchange（Hash Shuffle）       │
│    TiFlash-2: 扫描 t2 分片 ┘    并行 Hash Join（多节点）     │
│                                      ↓                       │
│                            并行 Hash Agg（多节点）           │
│                                      ↓                       │
│                            TiDB 只收最终结果（很小）         │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 MPP 核心算子 Exchange

```cpp
// ★ TiFlash MPP Exchange 算子（源码级，面试核心！）
//
// tiflash/dbms/src/Flash/Mpp/
//
// MPP 任务通过 tipb（TiDB protobuf）下发：
//   ExchangeSender：把本节点数据按规则发给其他节点
//   ExchangeReceiver：接收其他节点发来的数据
//
// Exchange 三种数据分发方式：
//
// ① Hash Partition（Hash 重分区）：
//    按 Join Key / Group By Key 做 hash
//    → 相同 Key 的数据落到同一个下游节点
//    → 用于分布式 Join 和聚合（保证相同 Key 聚到一起）
//
// ② Broadcast（广播）：
//    小表数据复制发给所有节点
//    → Broadcast Join（小表广播，大表不动）
//
// ③ PassThrough / Single：
//    发给单个节点（最终汇总到 TiDB）
//
// // MPP Task 调度
// // TiDB 优化器生成 MPP Plan → 拆成多个 MPPTask
// // 每个 Task 调度到一个 TiFlash Store 并行执行
// // TiFlash 节点间用 gRPC + Tunnel 做 Exchange 数据传输
//
// // ExchangeSender 核心逻辑
// void ExchangeSender::write(Block block) {
//     // 按 partition 列计算 hash → 决定发给哪个下游
//     for (row : block) {
//         int target = hash(joinKey(row)) % numDownstream;
//         tunnels[target].send(row);  // gRPC tunnel
//     }
// }
//
```

### 3.3 TiDB 如何选择 TiFlash

```go
// ★ TiDB 优化器选择行存/列存（源码级）
//
// // tidb/planner/core/...
// // 优化器根据统计信息 + 代价选择 TiKV 或 TiFlash
//
// 手动 Hint（强制走列存）：
//   SELECT /*+ READ_FROM_STORAGE(TIFLASH[t]) */ * FROM t;
//
// // 自动选择（CBO 代价优化）：
// // ① 表有 TiFlash 副本（isolation read / Raft Learner）
// // ② 估算代价：
// //    大范围扫描 + 聚合/Join → TiFlash 列存代价低
// //    点查/短事务/高 QPS → TiKV 行存代价低
// // ③ 选代价最小的存储生成 PhysicalTableReader / TableFullScan
//
// ★ TiFlash 读一致性（面试高频！）：
//   TiFlash 是异步 Learner，数据可能稍落后于 Leader
//   → TiFlash 读取时带读取时间戳（TSO）
//   → 等待 Raft 复制 index ≥ 读取所需 index（Lease Read/ReadIndex）
//   → 保证快照隔离一致性（不会读到落后数据）
//
```

---

## 四、HTAP 全景对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB HTAP vs 传统方案（面试高频对比）                     │
│                                                               │
│  传统方案：                                                    │
│   TP 库（MySQL）+ ETL（T+1 抽数据）+ AP 库（ClickHouse/Hive）│
│   → 数据 T+1 延迟 + 两套系统 + ETL 维护成本                  │
│                                                               │
│  TiDB HTAP：                                                   │
│   一份数据 Raft 同时复制到 TiKV（行）+ TiFlash（列）         │
│   → 实时（无需 ETL）+ 一套系统 + 资源隔离                    │
│                                                               │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │            │ TiKV          │ TiFlash       │ Doris/CK     │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 存储       │ 行存 LSM      │ 列存 DeltaTree│ 列存         │        │
│  │ 角色       │ TP            │ AP（Learner） │ 独立 AP 引擎 │        │
│  │ 计算       │ Coprocessor   │ ★ MPP         │ MPP/向量化   │        │
│  │ 数据同步   │ Raft Leader   │ Raft Learner  │ 外部导入     │        │
│  │ 延迟       │ 实时          │ 准实时（Learner）│ T+1/分钟级  │        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiFlash 是什么？| **TiDB 列存副本 + MPP 分析引擎**，支撑 HTAP 中的 AP 负载 |
| 2 | TiFlash 角色？| **Raft Learner**：从 TiKV 异步复制，不投票不影响 TP 多数派 |
| 3 | 为什么用 Learner？| 列存只做分析读；不参与投票避免拖慢事务 Raft；资源隔离 |
| 4 | 列存三大优势？| **IO 裁剪（只读相关列）+ 高压缩比（5-10x）+ 向量化 SIMD** |
| 5 | Delta-Tree 引擎？| **Delta 层（写优化接收实时增量）+ Stable 层（列文件读优化）+ 后台 Merge** |
| 6 | MPP 解决什么？| Coprocessor 最终汇总压在 TiDB 单点；MPP 让 Join/Agg 多节点并行 |
| 7 | Exchange 三种分发？| **Hash Partition（Join/聚合）+ Broadcast（小表广播）+ PassThrough（汇总）** |
| 8 | MPP 怎么调度？| TiDB 优化器生成 MPP Plan → 拆 MPPTask → 调度到多个 TiFlash 并行 |
| 9 | 节点间怎么传数据？| gRPC + Tunnel（ExchangeSender/ExchangeReceiver）|
| 10 | 怎么强制走 TiFlash？| Hint `/*+ READ_FROM_STORAGE(TIFLASH[t]) */` |
| 11 | CBO 怎么选？| 大扫描/聚合/Join → TiFlash 代价低；点查/高 QPS → TiKV |
| 12 | TiFlash 读一致性？| 带 TSO；等 Raft index 复制到读取点（ReadIndex），保证快照隔离不读落后数据 |
| 13 | 面试最高频考点排序？| **1. Learner 复制 2. 列存优势 3. Delta-Tree 4. MPP Exchange 5. HTAP vs ETL** |
| 14 | 学习路径？| Raft Learner → DeltaMerge Delta/Stable → MPP ExchangeSender → CBO 选存储 |
| 15 | 面试常见追问？| AP 大查询会影响 TP 吗？→ Learner 异步 + 资源组隔离，不影响 TiKV 交易 |
| 16 | HTAP vs 传统 TP+ETL+AP？| 一份数据实时双存，免 ETL、T+0、一套系统 |
| 17 | TiFlash vs ClickHouse？| TiFlash 实时 Raft 同步 + 事务一致；CK 需外部导入、无事务 |
| 18 | TiFlash vs TiKV Coprocessor？| Coprocessor 单 Region 下推+TiDB 汇总；MPP 多节点分布式 Join/Agg |
| 19 | Broadcast Join？| 小表广播到所有节点，大表本地扫描，避免大表 Shuffle |
| 20 | TiFlash 源码路径？| tiflash/dbms/src/Storages/DeltaMerge/ + Flash/Mpp/ |
| 21 | 面试高频场景？| 实时大屏/报表 + 交易库直接跑分析 + 免 ETL 的 HTAP |
| 22 | 面试陷阱？| 说 TiFlash 是独立数据库 → ❌ 它是 TiKV 的 Raft Learner 列存副本，同一集群 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：tiflash/dbms/src/Storages/DeltaMerge/ + Flash/Mpp/ + tidb/planner/core  
**面试考点：22 题**  
**差异化点**：Raft Learner 行列双存 + Delta-Tree 写入/列存兼顾 + MPP Exchange 三种分发 + HTAP vs ETL
