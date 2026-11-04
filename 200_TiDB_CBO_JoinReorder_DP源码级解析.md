# TiDB CBO Join Reorder + DP 动态规划源码级深度解析

> **源码基线**：TiDB 7.x/8.0（Go）  
> **核心位置**：tidb/planner/core/join_reorder.go + tidb/planner/core/logical_plans.go  
> **面试权重**：⭐⭐⭐⭐⭐（HTAP 分布式 JOIN 核心，高频）

---

## 一、TiDB JOIN 重排全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ JOIN Reorder = CBO 选择最优多表 JOIN 顺序                 │
│                                                               │
│  问题场景：                                                    │
│   SELECT ... FROM A JOIN B ON ... JOIN C ON ... JOIN D ON ...│
│                                                               │
│   可能的 JOIN 顺序 = 4! = 24 种排列                            │
│   每种排列的 Cost 完全不同（可能差几个数量级）                 │
│   → 枚举所有排列 → 选 Cost 最小的                             │
│                                                               │
│  ★ 三种 JOIN Reorder 算法（面试必背）：                       │
│  ┌──────────────────────────────────────────────────┐          │
│  │ ① 贪心（Greedy）：                               │          │
│  │   每次选 Cost 最小的 JOIN 对 → 逐个 JOIN          │          │
│  │   速度快但可能错过全局最优                        │          │
│  ├──────────────────────────────────────────────────┤          │
│  │ ② 动态规划（DP, TiDB 默认）：                    │          │
│  │   枚举所有子集 JOIN → 存 Cost → 逐步扩大集合      │          │
│  │   ★ 找到全局最优（2-6 表适合）                   │          │
│  ├──────────────────────────────────────────────────┤          │
│  │ ③ 遗传算法（GA, TiDB 7.0+）：                    │          │
│  │   随机变异 + 选择 + 交叉                          │          │
│  │   适合超大多表 JOIN（>8 表，DP 太慢）             │          │
│  └──────────────────────────────────────────────────┘          │
│
│  ★ TiDB vs MySQL JOIN Reorder 核心差异（第六节对比）：        │
│   MySQL：简单贪心 + 有限 DP（Join Buffer 约束）               │
│   TiDB：完整 DP + MPP Exchange 下推 + Coprocessor Join 下推   │
│   → TiDB 分布式 JOIN 优化更强                                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、DP 动态规划源码级

### 2.1 DP 核心状态转移

```go
// ★ TiDB CBO JOIN Reorder DP 源码级（面试核心）
//
// tidb/planner/core/join_reorder.go
// tidb/planner/core/task.go → JoinReorderDP()
//
// // 假设 4 表 JOIN：A, B, C, D
// // DP 状态空间（面试高频讲解）：
// //
// // dp[S] = 集合 S 的最优 JOIN Plan + Cost
// //   S = 2^n 种状态（n 表 JOIN）
// //   n=4 → 2^4=16 个状态
// //
// // dp[{A}] = A（单表，Cost=扫描 A 的 Cost）
// // dp[{B}] = B
// // dp[{A,B}] = min(Cost(A JOIN B), Cost(B JOIN A))
// // dp[{A,B,C}] = min(
// //   dp[{A,B}] JOIN C 的 Cost,
// //   dp[{A,C}] JOIN B 的 Cost,
// //   dp[{B,C}] JOIN A 的 Cost
// // )
// // dp[{A,B,C,D}] = ...（最终答案）
//
// // ★ 源码级 DP 核心：
// func (p *task) JoinReorderDP() *task {
//     n := len(p.sourceTasks)  // n 个源表
//     totalStates := 1 << n    // 2^n 个状态
//     dp := make([]*joinGroup, totalStates)
//
//     // Step 1: 初始化单表状态
//     for i := 0; i < n; i++ {
//         state := 1 << i
//         dp[state] = &joinGroup{
//             task: p.sourceTasks[i],
//             cost: p.sourceTasks[i].cost,
//         }
//     }
//
//     // Step 2: 逐步扩大集合（从 size=2 到 size=n）
//     for size := 2; size <= n; size++ {
//         for state := 0; state < totalStates; state++ {
//             if popcount(state) != size { continue }
//
//             // ★ 关键：把 state 拆成两个非空子集 + JOIN
//             bestCost := MAXFLOAT
//             var bestJoin *joinGroup
//
//             // 枚举所有拆分方式：state = s1 ∪ s2
//             for s1 := 1; s1 < state; s1 = (s1+1) | state {
//                 s2 := state ^ s1
//                 if s2 == 0 { continue }
//
//                 // dp[s1] JOIN dp[s2]
//                 left := dp[s1]
//                 right := dp[s2]
//                 for _, joinType := range availableJoinTypes(left, right) {
//                     joinCost := estimateJoinCost(left.task, right.task, joinType)
//                     totalCost := left.cost + right.cost + joinCost
//
//                     if totalCost < bestCost {
//                         bestCost = totalCost
//                         bestJoin = buildJoin(left.task, right.task, joinType)
//                     }
//                 }
//             }
//             dp[state] = &joinGroup{task: bestJoin, cost: bestCost}
//         }
//     }
//
//     // Step 3: 返回全集的最优 Plan
//     return dp[totalStates-1].task
// }
//
```

### 2.2 DP 复杂度 + 表数限制

```go
// ★ DP 时间复杂度（面试高频）
//
// // 状态数：2^n（所有子集）
// // 每个状态要枚举所有拆分：O(2^size)
// // 总复杂度：O(n * 3^n)
// //
// // 实际可处理的表数：
// //   n=4 → 2^4=16 状态；3^4=81 拆分 → 毫秒级
// //   n=6 → 2^6=64 状态；3^6=729 拆分 → 毫秒级
// //   n=8 → 2^8=256 状态；3^8=6561 拆分 → 几十毫秒
// //   n=10 → 2^10=1024；3^10=59049 → 几百毫秒（开始慢）
// //   n>12 → DP 太慢 → 切换遗传算法（GA）
//
// // TiDB 参数控制：
// // optimizer_joins_reorder_threshold = 4
// //   超过 4 表 JOIN 才启用 DP Reorder
// // optimizer_joins_reorder_max_connections = 6144
// //   控制 DP 状态空间上限（防止 OOM）
//
// // ★ DP vs 贪心 vs GA 复杂度对比：
// //   贪心：O(n)  → 快但可能差
// //   DP：O(n * 3^n) → 最优但慢（n<12）
// //   GA：O(n * popsize * gen) → 近似最优（n>8）
//
```

---

## 三、TiDB JOIN 类型选择源码级

### 3.1 Hash Join vs Index Join vs IndexHash Join

```go
// ★ TiDB 三种 JOIN 实现源码级（面试核心）
//
// tidb/planner/core/join_plan_builder.go
// tidb/planner/core/task.go → HashJoin / IndexJoin / IndexHashJoin
//
// // 1. Hash Join（TiKV Coprocessor 可下推）：
// //    外表 + 内表都加载到内存 → Build Hash → Probe
// //    Build 表大 → 用分块 Hash（避免内存爆炸）
// //    TiKV Coprocessor 下推 Build 表 → 在 TiKV 本地 Hash Join
// //    → 减少网络传输（面试高频 TiDB 优势）
// //
// // 2. Index Join：
// //    外表每行 → 内表用索引查一次（类似 MySQL BKA）
// //    外表小 + 内表有索引 → 最优
// //    Coprocessor 也能下推外表的一部分过滤
// //
// // 3. IndexHashJoin（TiDB 独有！）：
// //    外表用索引范围扫 → 外表结果 Build Hash → 内表 Probe
// //    外表有可用索引 → 过滤 + Hash Join 一体
// //    // tidb/planner/core/task.go → IndexHashJoinTask
//
// // ★ TiDB JOIN 选择逻辑源码级：
// func (p *task) findBestJoinType(left, right *task) *joinGroup {
//     candidates := []JoinType{}
//
//     // 有可用索引 → Index Join 候选
//     if hasAvailableIndex(right) {
//         candidates.append(IndexJoin)
//     }
//
//     // 外表有索引 → IndexHashJoin 候选
//     if hasAvailableIndex(left) {
//         candidates.append(IndexHashJoin)
//     }
//
//     // 等值 JOIN → Hash Join 候选
//     if isEquiJoin(joinCondition) {
//         candidates.append(HashJoin)
//     }
//
//     // 每个候选算 Cost → 选最小的
//     return minCost(candidates)
// }
//
// // ★ TiDB MPP Exchange 对 JOIN 的影响：
// //   TiDB MPP 模式下 JOIN 可以拆成 Exchange → 分区 Hash Join
// //   // 外表按 Join Key Exchange（分区）→ TiKV 本地 Hash Join
// //   // 类似 Spark 的 Shuffle Join
// //   → 跨 TiKV 节点分布式 JOIN
//
```

---

## 四、TiDB vs MySQL JOIN Reorder 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：TiDB CBO JOIN Reorder vs MySQL JOIN Reorder    │
│  ┌──────────────┬──────────────────┬──────────────────┐     │
│  │ 维度            │ TiDB CBO          │ MySQL 8.0         │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 核心算法         │ ★ DP（2-12 表）    │ 贪心 + 有限 DP    │     │
│  │                │ + GA（>12 表）    │ （Join Buffer 约束）│     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ JOIN 类型       │ Hash/Index/      │ Hash/BNL/BKA/NLJ │     │
│  │                │ IndexHash/Merge   │ /Block Nested     │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 分布式能力       │ ★ MPP Exchange    │ ❌ 单节点串行      │     │
│  │                │ + Coprocessor    │                    │     │
│  │                │ Join 下推         │                    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Join 下推       │ ★ TiKV 本地 Hash  │ ❌ 无              │     │
│  │                │ Join（Coprocessor）│                   │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 统计信息         │ ★ Histogram +    │ Histogram（8.0+） │     │
│  │                │ TopN + 扩展统计  │ TopN 可选          │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 表数限制         │ DP ≤12 表；GA 无限 │ 实用 ≤8 表        │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ EXPLAIN          │ verbose 显示    │ EXPLAIN ANALYZE   │     │
│  │                │ Join Reorder 细节 │ 显示 Join 算法    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 面试高频         │ ★★★★★          │ ★★★★★            │     │
│  └──────────────┴──────────────────┴──────────────────┘     │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   MySQL OLTP 单节点：Hash Join + BKA + 好索引 → 够用         │
│   TiDB HTAP 分布式：DP + MPP Exchange + Coprocessor Join 下推 │
│   TiDB 优势：分布式 JOIN 在 TiKV 本地执行 → 减少网络传输      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiDB CBO JOIN Reorder 三种算法？| **贪心 + DP（2-12 表）+ 遗传算法 GA（>12 表）** |
| 2 | ★ DP 核心状态转移？| dp[S] = 集合 S 的最优 JOIN Plan；枚举所有拆分 S=s1∪s2；dp[s1] JOIN dp[s2] |
| 3 | DP 时间复杂度？| **O(n × 3^n)**；n=4→毫秒级；n=10→几百毫秒；n>12 太慢 → GA |
| 4 | TiDB 三种 JOIN 实现？| **Hash Join + Index Join + ★ IndexHashJoin（TiDB 独有）** |
| 5 | IndexHashJoin 核心？| 外表索引范围扫 + Build Hash + 内表 Probe；外表有可用索引 → 过滤 + Hash 一体 |
| 6 | ★ TiDB Hash Join vs MySQL Hash Join 核心差异？| TiDB Hash Join 可以 **Coprocessor 下推到 TiKV 本地执行** → 减少网络传输 |
| 7 | TiDB MPP Exchange 对 JOIN 的影响？| MPP 模式下 JOIN 拆成 Exchange（分区 Hash Join）→ 跨 TiKV 节点分布式 |
| 8 | optimizer_joins_reorder_threshold 默认？| **4**；超过 4 表 JOIN 才启用 DP Reorder（避免简单查询浪费时间）|
| 9 | GA 遗传算法什么时候启用？| **JOIN 表数 > 12 表**（DP 太慢）；GA 随机变异+选择+交叉 → 近似最优 |
| 10 | ★ TiDB vs MySQL JOIN Reorder 对比？| 见第四节对比表（8 维度）|
| 11 | 面试最高频考点排序？| **1. DP 状态转移 O(n×3^n) 2. 三种 JOIN 实现 3. Coprocessor Join 下推 4. TiDB vs MySQL 差异 5. GA 触发条件** |
| 12 | 学习路径？| DP 状态 → JOIN 类型选择 → HashJoin → Coprocessor → GA |
| 13 | 面试常见追问？| 6 表 JOIN 枚举多少种排列？| 6! = 720 种；DP 能全部枚举并选最优；MySQL 可能贪心选次优 |
| 14 | 面试常见追问？| TiDB 的 Hash Join 和 MySQL Hash Join 实现一样吗？| **不一样！** TiDB 可以 Coprocessor 下推到 TiKV；MySQL 单节点内存 Hash |
| 15 | 面试陷阱？| 说 "DP 能找到全局最优" → ❌ 只对**内连接等值 JOIN** 成立；外连接（LEFT/RIGHT JOIN）有方向约束，DP 不能任意交换 |
| 16 | 面试陷阱？| 说 "GA 比 DP 好" → ❌ GA 是**近似最优**；DP 是**精确最优**；GA 只有 DP 太慢（>12 表）才用 |
| 17 | LEFT JOIN 可以重排吗？| LEFT JOIN 有方向约束（外表不能放后面）；**TiDB DP 会跳过违反外连接约束的拆分** |
| 18 | TiDB EXPLAIN 怎么看 JOIN 顺序？| EXPLAIN ANALYZE FORMAT='brief' → 显示实际 JOIN 顺序 + Cost；FORMAT='tree' 显示树形结构 |
| 19 | Hash Join Build 表在 TiDB 怎么选？| Cost 估算：选行数少 + Coprocessor 下推可能的表 → 内存 Build 最小 |
| 20 | DP 内存占用？| 2^n 个状态；每个状态存一个 JOIN Plan → n=12 → 2^12=4096 个 Plan；可控 |
| 21 | optimizer_joins_reorder_max_connections？| 默认 6144；控制 DP 状态空间上限；超过报错或降级到贪心 |
| 22 | 典型生产 SQL（强制 JOIN 顺序）？| SELECT /*+ LEADING(A, B, C) */ ... FROM A JOIN B JOIN C ... |
| 23 | TiDB 7.0+ GA 参数？| tidb_optimizer_join_reorder_strategy = 'auto'（自动选择 DP vs GA）|
| 24 | JOIN Reorder 对非等值 JOIN？| DP 只用于等值 JOIN（Hash Join 只能等值）；非等值 JOIN 只能选 NLJ/BNL |
| 25 | TiDB Histogram 对 JOIN Cost 估算的影响？| JOIN Cost = 外表 rows × 内表 rows（如果无索引）；Histogram 准 → Cost 准 → JOIN 顺序对 |
| 26 | 典型生产用法？| HTAP 场景：5 表 JOIN（orders+customers+products+regions+promotions）→ TiDB DP 自动重排 → Coprocessor 下推到 TiKV 本地 Hash Join |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：tidb/planner/core/join_reorder.go + task.go + join_plan_builder.go  
**面试考点：26 题**  
**差异化点**：DP O(n×3^n) 状态转移源码级、IndexHashJoin（TiDB 独有）、Hash Join Coprocessor 下推、**TiDB vs MySQL JOIN Reorder 8 维度对比**
