# TiDB Flash 列存 + MPP Exchange 源码级深度解析

> **源码基线**：TiDB 7.x/8.0（Go）+ TiFlash（C++）  
> **核心位置**：tidb/planner/core/physical_optimizer + tiflash/coprocessor + tidb/planner/core/mpp  
> **面试权重**：⭐⭐⭐⭐⭐（HTAP 混合事务分析核心引擎）

---

## 一、HTAP 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB HTAP = Hybird Transactional + Analytical Processing │
│                                                               │
│  核心思想（面试必背）：                                        │
│   同一套数据同时服务 OLTP（事务）+ OLAP（分析）                │
│   - TiKV：行存（RocksDB），事务友好，TiDB Server 层查询       │
│   - TiFlash：★ 列存（Delta Tree），分析友好，MPP 并行计算     │
│   - Raft Learner：TiFlash 作为 TiKV Raft 的 Learner 节点     │
│     → 实时同步数据（秒级延迟）                                │
│                                                               │
│  ★ MPP（Massively Parallel Processing）架构：                 │
│   TiDB Server 作为 Coordinator → 把查询拆成 Exchange 任务    │
│   → 分发到多个 TiFlash 节点并行执行 → 收集结果返回            │
│   → 类似 Spark/Hive 的分布式执行引擎                           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiFlash 列存引擎源码级

### 2.1 Delta Tree 结构

```cpp
// ★ TiFlash Delta Tree 列存引擎源码级（面试核心）
//
// tiflash/storage/ddt/delta_tree/
// tiflash/storage/engine/
//
// // Delta Tree 三层结构（面试必背）：
// //
// // 1. ST：Storage（持久层）
// //    - 只读，Append-Only
// //    - 列式存储（Parquet 或自定义格式）
// //    - 由 Compaction 生成
// //
// // 2. DT：Delta Tree（增量层）
// //    - 读写
// //    - 内存优先 + 磁盘 spill
// //    - 存最新修改的数据
// //
// // 3. D：Delta（增量变更）
// //    - 每一行的变更（Write / Update / Delete）
// //    - MVCC 版本化
// //    - 存 RocksDB（TiFlash 底层用 RocksDB 存 Delta）
// //
// // ★ Delta Tree 核心特性（源码级）：
// //
// // a. 列式存储：
// //    同一列的数据连续存储 → 向量化计算快
// //    例：SELECT SUM(amount) → 只读 amount 这一列
// //    → 比行存快 10-100x
// //
// // b. MVCC 版本化：
// //    TiFlash 复制 TiKV 的 Raft Log
// //    每个 Row 有多个版本（commit_ts 递增）
// //    → 提供快照读（Read Committed）
// //
// // c. 实时同步（Raft Learner）：
// //    TiFlash 是 TiKV Raft 的 Learner（不投票，只同步）
// //    Raft Log 复制 → TiFlash Apply → Delta Tree 更新
// //    → 秒级延迟（比 Binlog 同步快得多）
//
```

### 2.2 TiFlash Raft Learner 同步源码级

```cpp
// ★ TiFlash Raft Learner 同步源码级（面试必背）
//
// tiflash/raft/raft_store/
// tiflash/raft/raft_log/
//
// // 标准 TiKV Raft 集群：
// //   Region 1: [KV-L, KV-R1, KV-R2]（3 节点，Leader 选 KV-L）
// //
// // ★ 开启 TiFlash 后：
// //   Region 1: [KV-L, KV-R1, KV-R2, TF1, TF2]
// //   → TF1 和 TF2 是 **Learner**（不参与投票）
// //   → Raft Log 同步给 TF1/TF2
// //   → TiFlash Apply Raft Log → 更新 Delta Tree
// //
// // 同步延迟：
// //   Raft Log 复制（毫秒级）
// //   → TiFlash Apply（RocksDB 写入，毫秒级）
// //   → 总延迟：**100ms - 1s**（远低于 Binlog 同步）
// //
// // TiFlash Learner 优势：
// //   ✅ 不影响 Raft Quorum（Raft 过半投票还是 KV 节点）
// //   ✅ 不影响写性能（写只需要 KV 节点 ACK）
// //   ✅ 数据完全一致（Raft 保证 CP）
// //
```

---

## 三、MPP Exchange 分布式执行源码级

### 3.1 MPP 架构源码级

```go
// ★ TiDB MPP（Massively Parallel Processing）架构源码级
//
// tidb/planner/core/mpp/
// tidb/planner/core/mpp/builder.go
// tidb/planner/core/mpp/runtime.go
//
// // MPP 核心思想：
// //   TiDB Server（Coordinator）把一个 SQL 拆成多个子任务
// //   → 每个子任务分发到一个 TiFlash 节点执行
// //   → TiDB Server 收集结果 → Merge → 返回给客户端
// //   → 类似 Spark/Hive/ClickHouse 的分布式执行
// //
// // ★ Exchange 类型（面试必背）：
// //   Exchange = 数据重分布算子（类似 Spark 的 Shuffle）
// //
// //   1. Hash Exchange（Partition Exchange）
// //      按 Key Hash 分区 → 同一个 Hash Key 发到同一个 TiFlash
// //      // 用于 JOIN 前的 Shuffle
// //      // 例：A JOIN B ON A.id=B.id → 两边都按 id Hash → 相同 id 在同一个 TiFlash
// //
// //   2. Broadcast Exchange
// //      数据小的表广播到所有 TiFlash
// //      // 用于小表 JOIN 大表
// //      // 例：Table A（100 行）JOIN Table B（1亿行）→ 把 A 广播给所有存 B 的 TiFlash
// //
// //   3. Singleton Exchange
// //      所有数据发到一个 TiFlash（Reduce 阶段）
// //      // 用于聚合的最后一步
// //      // 例：SELECT SUM(amount) FROM orders → 每个 TiFlash 先算局部 SUM → 发到一个 TiFlash 算全局 SUM
// //
// // ★ MPP 执行流程源码级：
// //
// //   SELECT ... FROM orders JOIN customers ON ... WHERE dt=... GROUP BY ...
// //
// //   Plan 拆成：
// //   ① TiDB Server：解析 SQL → 生成 MPP Plan Tree
// //      // Exchange 节点标记 Shuffle 边界
// //   ② TiFlash 节点：并行执行 Exchange 之前的部分
// //      // 每个 TiFlash 做 TableScan + Filter + Local Agg
// //   ③ Hash Exchange：按 Join Key Shuffle
// //      // 每个 TiFlash 把数据发到下一个阶段对应 TiFlash
// //   ④ 下一阶段 TiFlash：Join + Global Agg
// //   ⑤ Singleton Exchange：最终结果发到一个 TiFlash 做 Final Agg
// //   ⑥ TiDB Server 收集结果 → 返回客户端
// //
```

### 3.2 Hash Join + Agg Coprocessor 下推

```go
// ★ TiFlash Coprocessor 下推源码级（面试核心）
//
// tidb/planner/core/coprocessor/
// tiflash/coprocessor/
//
// // Coprocessor 是什么：
// //   TiDB 把 SQL 片段下推到 TiKV/TiFlash 本地执行
// //   → 只把结果拉回 TiDB Server
// //   → 减少网络传输（面试必背 TiDB 优势）
// //
// // ★ TiFlash Coprocessor 下推能力（比 TiKV 更强）：
// //
// // 1. Join 下推：
// //    TiFlash 本地做 Hash Join
// //    // 两个 TiFlash 分片数据在本地 Join
// //    // 不经过 TiDB Server
// //    // 这是 MPP 能分布式 Join 的基础
// //
// // 2. Aggregation 下推：
// //    TiFlash 本地做 Aggregation（SUM/COUNT/AVG）
// //    // 先算局部 Agg → 后算全局 Agg
// //    // 类似 Spark 的 Partial Agg + Final Agg
// //
// // 3. Sort 下推：
// //    TiFlash 本地做 Sort
// //    // ORDER BY + LIMIT 在 TiFlash 层完成
// //
// // 4. Filter 下推：
// //    WHERE 条件全部下推（TiKV 也支持）
// //
// // ★ Coprocessor 执行流程源码级：
// //
// // TiDB Planner 判断哪些能下推：
// //   canPushdownJoin() → 等值 JOIN + 两边都用 TiFlash → ✅ 下推
// //   canPushdownAgg() → TiFlash 支持的 Agg 函数 → ✅ 下推
// //
// // TiDB 构建 CoprocessorRequest：
// //   protobuf 格式：DAGRequest → TableScan → Join/Agg
// //   // 包含完整下推执行计划
// //   // 发给 TiFlash 执行
// //
// // TiFlash 执行 CoprocessorRequest：
// //   本地扫描 Delta Tree → Filter → Join/Agg → 结果传回 TiDB
// //
// // TiDB 合并结果（如果需要全局 Agg/Join）：
// //   TiFlash 结果 → Hash Exchange → 下一阶段 TiFlash → 最后聚合
// //
```

---

## 四、TiFlash vs TiKV vs ClickHouse 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种 OLAP/HTAP 引擎对比                          │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ TiFlash          │ TiKV (RocksDB)│ ClickHouse││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 存储格式         │ ★ 列存 Delta Tree │ 行存 KV     │ ★ 列存 MergeTree│
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 事务支持         │ ★ MVCC（Learner）│ ★ Percolator │ ❌ 无事务   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 数据同步         │ ★ Raft Learner  │ 主节点       │ 异步复制   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ JOIN 能力        │ ★ MPP 分布式 Hash │ 单节点 Hash  │ Cnch Merge ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ Agg Coprocessor  │ ★ 完整下推       │ 有限下推       │ 全下推     ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 适合场景         │ HTAP 实时分析     │ OLTP 事务       │ 纯 OLAP 数仓││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 运维复杂度       │ 中（维护 Flash）  │ ★ 低（主存储） │ 低        ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频         │ ★★★★★          │ ★★★★★        │ ★★★★★    ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   OLTP 主存储 → TiKV（行存 + Percolator MVCC）                │
│   实时分析看板 → TiFlash（列存 + Raft Learner 同步）           │
│   纯数仓 OLAP → ClickHouse（列式 MergeTree + 极简运维）      │
│   混合 HTAP → TiKV + TiFlash（Raft Learner 架构，秒级同步）   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | HTAP 是什么？| **Hybird Transactional + Analytical Processing**；同一数据服务 OLTP + OLAP |
| 2 | TiFlash Delta Tree 三层？| **ST（持久层，Append-Only）+ DT（增量层，读写）+ D（Delta，变更 MVCC）** |
| 3 | TiFlash 和 TiKV 什么关系？| **TiFlash 是 TiKV Raft 的 Learner（不投票，只同步数据）**；MVCC 版本化；秒级延迟 |
| 4 | ★ MPP Exchange 三种类型？| **Hash Exchange（按 Key Shuffle）+ Broadcast Exchange（小表广播）+ Singleton Exchange（发一个节点）** |
| 5 | MPP 架构核心？| TiDB Server 做 Coordinator → 拆 SQL → Exchange 分发 → 多个 TiFlash 并行执行 → 收集结果 |
| 6 | ★ TiFlash Coprocessor 下推能力？| **Join 下推 + Agg 下推 + Sort 下推 + Filter 下推**（比 TiKV 更强）|
| 7 | Coprocessor 下推核心价值？| TiFlash 本地执行 → 只把结果传回 TiDB → **减少网络传输**（TiDB 核心优势）|
| 8 | TiFlash Learner 为什么不影响写性能？| Raft Learner **不参与投票**；写只需要 KV 节点 ACK；写性能不受 TiFlash 影响 |
| 9 | TiFlash MVCC 怎么来的？| **复制 TiKV 的 Raft Log（包含 commit_ts）**；每个 Row 多个版本 |
| 10 | ★ TiFlash vs TiKV vs ClickHouse 三引擎对比？| 见第四节对比表（8 维度）|
| 11 | 面试最高频考点排序？| **1. Raft Learner 同步架构 2. MPP Exchange 三种 3. Coprocessor 下推能力 4. Delta Tree 三层 5. 三引擎对比** |
| 12 | 学习路径？| HTAP 架构 → TiFlash Learner → Delta Tree → MPP Exchange → Coprocessor 下推 |
| 13 | 面试常见追问？| TiFlash 和 TiKV 数据一致性怎么保证？| **Raft CP 保证**；TiFlash Learner 和 TiKV 用同一个 Raft Log 同步 |
| 14 | 面试常见追问？| TiFlash 延迟大概多少？| **100ms - 1s**（Raft Log 复制 + Apply）；远低于 Binlog 同步（分钟级）|
| 15 | 面试陷阱？| 说 "TiFlash 就是 TiKV 的另一个存储格式" → ❌ **TiFlash 是独立的 C++ 列存引擎**（Delta Tree）；TiKV 是 Go 写的行存（RocksDB）|
| 16 | 面试陷阱？| 说 "TiFlash 参与 Raft 投票" → ❌ **TiFlash 是 Learner**（不投票、不影响 Quorum） |
| 17 | TiFlash 和 TiKV 表结构？| 同一个表在 TiKV（行存）和 TiFlash（列存）都存在；Raft Learner 自动同步 |
| 18 | TiDB Planner 怎么决定走 TiKV 还是 TiFlash？| cost-based：**TiFlash 适合聚合/Join（列式效率高）**；TiKV 适合点查/事务（行存快）|
| 19 | TiFlash 支持哪些聚合函数？| SUM/COUNT/AVG/MIN/MAX/GROUP_CONCAT；大多数 SQL 函数都支持下推 |
| 20 | TiFlash Compaction 做什么？| Delta Tree 定期 Compaction → 把 Delta 合并到 ST → 减少存储层碎片 → 提高查询性能 |
| 21 | 典型生产架构？| TiKV 3 节点（OLTP）+ TiFlash 2 节点（HTAP）；MPP 开启 → 实时分析看板秒级响应 |
| 22 | TiDB vCPU 配额对 MPP 的影响？| MPP 需要足够 vCPU 配额才能拆分并行任务；配额不够 → 退化成单节点 Coprocessor |
| 23 | Hash Exchange 和 Broadcast Exchange 选择？| **CBO Cost 估算**：小表（<100 万行）广播更优；大表 Hash Shuffle 更优 |
| 24 | TiFlash 故障会影响 TiKV 吗？| **不会！** TiFlash 是 Learner，Raft Quorum 还是 TiKV 节点；TiFlash 挂了 → 分析查询受影响但事务不受影响 |
| 25 | TiFlash 和 ClickHouse 哪个列存更好？| TiFlash：Raft 实时同步 + MVCC + 事务；ClickHouse：纯 OLAP + 列存极致压缩 + MergeTree；场景不同 |
| 26 | TiFlash Join 支持什么类型？| 等值 JOIN（Hash Join）；MPP 架构下可跨 TiFlash 节点分布式 JOIN |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：tiflash/storage/ddt/delta_tree/ + raft/raft_store/ + tidb/planner/core/mpp/ + coprocessor/  
**面试考点：26 题**  
**差异化点**：Raft Learner 秒级同步 + Delta Tree 三层（ST/DT/D）、**MPP Exchange 三种（Hash/Broadcast/Singleton）**、Coprocessor Join/Agg 下推、TiFlash vs TiKV vs ClickHouse 三引擎对比
