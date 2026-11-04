# TiDB/MySQL CBO + EXPLAIN COST 优化器源码级深度解析

> **源码基线**：TiDB 7.x/8.0（Go）+ MySQL 8.0（C++）  
> **核心位置**：tidb/planner/ + tidb/statistics/ + sql/sql_optimizer.cc  
> **面试权重**：⭐⭐⭐⭐⭐（SQL 调优核心，高频）

---

## 一、CBO 优化器全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ CBO（Cost-Based Optimizer）= 基于成本的优化器              │
│                                                               │
│  核心思想：                                                    │
│   同一 SQL 可能有多种执行计划 → 估算每种计划的 Cost          │
│   → 选 Cost 最小的执行计划                                    │
│                                                               │
│  ★ CBO 流程四阶段（TiDB/MySQL 共通，面试必背）：             │
│  ┌──────────────────────────────────────────────────────┐    │
│  │ 1. 统计信息收集：                                     │    │
│  │    Table row count / Column null rate / Histogram   │    │
│  │    → TiDB: ANALYZE TABLE t;                          │    │
│  │    → MySQL: ANALYZE TABLE t; / 自动收集              │    │
│  ├──────────────────────────────────────────────────────┤    │
│  │ 2. Cardinality Estimation（基数估算）：               │    │
│  │    WHERE dt='2024-01-01' AND status='paid'            │    │
│  │    → 估算过滤后剩余多少行                               │    │
│  ├──────────────────────────────────────────────────────┤    │
│  │ 3. Join Reorder（Join 重排序）：                      │    │
│  │    多表 JOIN → 枚举 Join 顺序 → 找 Cost 最小的        │    │
│  ├──────────────────────────────────────────────────────┤    │
│  │ 4. Access Path Selection（访问路径选择）：            │    │
│  │    表 + WHERE 条件 → 选最优索引（全表扫/索引扫/ICP/MRR）│   │
│  └──────────────────────────────────────────────────────┘    │
│
│  对比 RBO（Rule-Based Optimizer，MySQL 5.7 前默认）：        │
│   RBO 靠硬编码规则（有主键索引就用、有 WHERE 就全表扫等）    │
│   CBO 靠统计信息估算 → 适应数据分布变化                     │
│   MySQL 8.0+ 默认 CBO；TiDB 从 2.0 开始就是 CBO             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiDB CBO 源码级

### 2.1 TiDB 优化器管道

```go
// ★ TiDB CBO 核心管道（源码级）
//
// tidb/planner/core/optimize.go
// tidb/statistics/statistics.go
// tidb/planner/core/task.go
//
// // SQL → 物理计划 完整管道：
// func Optimize(ctx context.Context, isExplain bool) (Plan, error) {
//     // ① Parser → AST（语法树）
//     // tidb/parser/parser.y → StmtNode
//
//     // ② Bind → 逻辑计划树
//     // tidb/planner/core/planbuilder.go → LogicalPlan
//     //    SELECT * FROM orders WHERE dt='2024-01-01'
//     //    → DataSource(orders) → Selection(dt=...)
//
//     // ③ ★ 逻辑优化阶段（Rule 优化，先做这层）
//     //    PredicatePushdown（谓词下推）
//     //    ProjectionEliminate（列裁剪）
//     //    AggregationPruning（聚合裁剪）
//     //    // tidb/planner/core/logical_plans.go
//     logicalPlan := logicalOptimize(logicalPlan)
//
//     // ④ ★ 统计信息加载
//     //    tidb/statistics/statistics.go
//     statsTable := getStats(ctx, ordersTable)
//     statsTable.RowCount = 100000000     // 总行数
//     statsTable.ColNullRate["status"] = 0.01  // status 列空率
//     statsTable.ColHist["dt"] = Histogram{   // dt 列直方图
//         buckets: [{lo: '2024-01-01', count: 50000}, ...],
//         null_count: 0,
//         topN: [{value: 'paid', count: 3000000}, ...]
//     }
//
//     // ⑤ ★ 核心：基于 Cost 的物理计划选择
//     //    tidb/planner/core/task.go → ExhaustPhysicalPlans
//     //    枚举所有可能的物理计划 → 估算每个 Cost → 选最小的
//     physicalPlan := physicalOptimize(ctx, logicalPlan)
//     return physicalPlan, nil
// }
//
```

### 2.2 Cardinality Estimation 源码级

```go
// ★ TiDB 基数估算核心（源码级，面试核心）
//
// tidb/statistics/statistics.go → EstimationSingeField
// tidb/planner/core/stats.go → rowCountByCondition
//
// // 估算：WHERE dt='2024-01-01' AND status='paid'
// func rowCountByCondition(
//     stats *Statistics,
//     expr Expression,
//     totalRowCount float64
// ) float64 {
//     switch expr.(type) {
//     case *Constant:
//         return expr.Value.(float64)  // 常量直接返回
//
//     case *Column:
//         // 整列 → 返回总行数
//         return totalRowCount
//
//     case *BinaryOperation:
//         switch expr.Op {
//         case EQ:  // =
//             // ★ 核心：查 Histogram 或 TopN
//             hist := stats.ColHist[expr.Left.(*Column).Name]
//             if hist.TopN.Contains(expr.Right.Value) {
//                 // 高频值：返回 TopN 记录的 count
//                 return hist.TopN[expr.Right.Value].count
//             } else {
//                 // 非常见值：用 Histogram bucket 插值
//                 return hist.BinarySearch(expr.Right.Value)
//             }
//
//         case LT, LE, GT, GE:  // 范围
//             // ★ 用 Histogram bucket 边界算范围选择性
//             hist.EstimateRange(low, high)
//             // 例：dt < '2024-02-01'
//             //   → 找到 dt='2024-02-01' 前一个 bucket
//             //   → 返回该 bucket 的累计行数
//
//         case And:
//             // ★ AND 两个条件 → 选择率相乘
//             sel1 := rowCountByCondition(stats, expr.Left, totalRowCount) / totalRowCount
//             sel2 := rowCountByCondition(stats, expr.Right, totalRowCount) / totalRowCount
//             return totalRowCount * sel1 * sel2
//
//         case Or:
//             // ★ OR → sel1 + sel2 - sel1*sel2
//             return sel1 + sel2 - sel1*sel2
//         }
//
//     case *In:  // IN (...)
//         // IN 的每个值都查 Histogram → 求和
//         return sum(hist.EstimateForEach(values))
//
//     case *Like:  // LIKE
//         // 直方图不支持 LIKE → 返回固定选择率（默认 10%）
//         return totalRowCount * 0.10
//     }
// }
//
// ★ 直方图（Histogram）源码级核心：
//   tidb/statistics/histogram.go
//   // Bucket 定义：
//   type Bucket struct {
//       LowerBound     ScalarValue  // 本 bucket 最小值
//       UpperBound     ScalarValue  // 本 bucket 最大值
//       Count          uint64       // <= UpperBound 的累计行数
//       Repeats        uint64       // UpperBound 值出现次数
//   }
//   // Histogram = []Bucket（有序不重叠）+ TopN（高频值前 N 个）+ null_count + row_count
//   // ★ TopN：前 N 个高频值的精确 count（Histogram 不擅长高频值）
//   // TiDB 8.0 默认 TopN=128；Bucket=256
//
```

### 2.3 Join Reorder + Access Path

```go
// ★ Join 重排序 + Cost 估算（源码级）
//
// tidb/planner/core/task.go → JoinType
// tidb/planner/core/physical_join.go
//
// // 多表 JOIN 枚举：
// //   SELECT * FROM A JOIN B ON ... JOIN C ON ...
// //   可能的 Join 顺序：
// //     (A JOIN B) JOIN C  Cost=?
// //     (A JOIN C) JOIN B  Cost=?
// //     (B JOIN C) JOIN A  Cost=?
// //   CBO 枚举所有排列 → 选 Cost 最小的
//
// // ★ 三种 Join 类型 Cost（面试必背）：
// // 1. Hash Join：Cost = 左表行数 + 右表行数（构建 Hash Build + 探测）
// //    特点：等值 JOIN 最优；内存大（Build 表全内存）
// //    Build 表选择：选行数少的 → Cost 低
// //
// // 2. Index Join（TiDB 独有，MySQL 叫 BKA）：
// //    外表每行 → 内表用索引查一次
// //    Cost = 外表行数 × log(内表行数)（内表索引查 O(log N)）
// //    特点：外表小、内表有索引时最优
// //
// // 3. Merge Join（MySQL 叫 Block Nested Loop）：
// //    外表 + 内表按 Join Key 排序 → 归并扫描
// //    Cost = 外表 + 内表（都要排） + 归并
// //    特点：Join Key 有索引（已有序）或 Sort Merge
//
// // ★ Access Path（访问路径）选择：
// // tidb/planner/core/physical_table_scan.go
// //
// // DataSource 条件 + 可用索引 → 选最优访问路径
// //   - 全表扫（TableScan）：无索引可用
// //   - 索引扫（IndexScan）：覆盖 WHERE 的索引
// //   - 索引条件下推（ICP）：IndexScan + 索引内过滤剩余条件
// //   - MRR（Multi-Range Read）：索引查 PK → 批量回表
// //
// // Cost 估算核心：
// //   全表扫 Cost = 总行数 × 单行读取成本
// //   索引扫 Cost = 估算行数 × 索引查找成本 + 回表成本
// //   MRR Cost = 索引查 × 批量回表（比单行回表快 5-10x）
//
```

---

## 三、MySQL 8.0 CBO 对比 TiDB

```cpp
// ★ MySQL 8.0 CBO vs TiDB CBO（源码级对比）
//
// // MySQL 8.0 Cost Model：
// sql/sql_optimizer.cc → JOIN::optimize()
// sql/cost_model.cc
// sql/range_optimizer.cc
//
// // MySQL 8.0 统计信息：
// sql/histograms.cc（8.0+ 新增）
//   - Column Statistics：高/低值、不同值数、空值数、平均长度
//   - Histogram：Bucketing + TopN（类似 TiDB）
//
// // MySQL Cost 公式（简化）：
// //   表读取 Cost = rows × io_block_read_cost() + rows × row_evaluate_cost()
// //   索引 Cost = key_access_cost × log(rows) + (rows × io_block_read_cost())
// //   Join Cost = build_rows + probe_rows × io_block_read_cost
//
// // ★ TiDB vs MySQL CBO 核心差异（面试高频）：
//
// ┌──────────────┬──────────────────┬──────────────────┐
// │                │ TiDB 8.0          │ MySQL 8.0          │
// ├──────────────┼──────────────────┼──────────────────┤
// │ 统计收集       │ ★ ANALYZE TABLE   │ ANALYZE + 自动收集 │
// │                │ 手动 + 后台定期   │ (innodb_stats_auto_recalc)│
// ├──────────────┼──────────────────┼──────────────────┤
// │ Histogram      │ ★ Bucket=256      │ Bucket=100（默认） │
// │                │ TopN=128          │ TopN=0（默认，需开）│
// ├──────────────┼──────────────────┼──────────────────┤
// │ 直方图类型     │ 等宽 Histogram    │ 等宽 + 等高 可选   │
// ├──────────────┼──────────────────┼──────────────────┤
// │ Join 类型      │ Hash/Index/Merge  │ Nested/Hash/BKA  │
// │                │ 三种原生          │ 三种（实现不同）   │
// ├──────────────┼──────────────────┼──────────────────┤
// │ 自适应         │ ★ 动态更新统计   │ 8.0+ adaptive    │
// │                │ 后台 Worker       │  hash join 切换    │
// ├──────────────┼──────────────────┼──────────────────┤
// │ CTE 支持       │ ✅ Recursive CTE  │ ✅ Recursive      │
// ├──────────────┼──────────────────┼──────────────────┤
// │ 并行查询       │ ★ MPP / Exchange  │ ❌ 单节点串行     │
// ├──────────────┼──────────────────┼──────────────────┤
// │ 跨表 Pushdown  │ ★ Coprocessor    │ ❌ 无              │
// ├──────────────┼──────────────────┼──────────────────┤
// │ 执行计划缓存   │ ★ Plan Cache     │ ❌ 无              │
// └──────────────┴──────────────────┴──────────────────┘
//
```

---

## 四、EXPLAIN ANALYZE 实战

```
┌─────────────────────────────────────────────────────────────┐
│  ★ EXPLAIN ANALYZE（TiDB 4.0+ / MySQL 8.0.18+）           │
│                                                               │
│  EXPLAIN 只估算 Cost；EXPLAIN ANALYZE 跑一遍看实际耗时        │
│  → 对比 Estimated vs Actual → 发现统计不准                 │
│                                                               │
│  例子（TiDB）：                                               │
│                                                                │
│  EXPLAIN ANALYZE SELECT * FROM orders                         │
│  WHERE dt='2024-01-01' AND status='paid';                    │
│                                                                │
│  输出：                                                        │
│  ┌───────────────┬───────┬───────────────┬────┬─────────┬─────┐│
│  │ id            │ table │ partitions     │ type│ rows     │ act ││
│  ├───────────────┼───────┼───────────────┼────┼─────────┼─────┤│
│  │ Selection_5   │       │               │     │ 80000   │ 12s ││
│  │  └─TableScan_6│ orders│ p20240101~...  │ range│100000  │ 15s ││
│  └───────────────┴───────┴───────────────┴────┴─────────┴─────┘│
│                                                                │
│  ★ 关键对比点：                                               │
│    estimated rows: 80000                                      │
│    actual rows: 100000（Selection 输出 80000 过滤后剩 100000 不对！）│
│                                                                │
│    等等——仔细看：Selection_5 估计剩 80000；实际剩 ?           │
│    如果 actual rows（真实处理行数）比估计大 5x → 统计不准    │
│                                                                │
│  常见 EXPLAIN ANALYZE 诊断（面试高频）：                      │
│   1. 索引没用到 → type=ALL 而非 range/ref → Check 索引       │
│   2. rows 估 1000 实际 100000 → 统计过期 → ANALYZE TABLE      │
│   3. 排序没用到索引 → Extra=Using filesort → 加联合索引     │
│   4. Join 顺序反了 → Hash Join 选了大表当 Build → 统计不准   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | CBO 是什么？| **Cost-Based Optimizer**；估算每个可能执行计划的 Cost → 选最小的 |
| 2 | CBO 四阶段管道？| **统计收集 → 基数估算 → Join 重排序 → 访问路径选择** |
| 3 | TiDB 直方图 Histogram 结构？| **[]Bucket（有序不重叠）+ TopN（前 N 高频值）+ null_count + row_count** |
| 4 | Bucket 核心字段？| LowerBound + UpperBound + Count（累计行数）+ Repeats（UpperBound 出现次数）|
| 5 | TopN 作用？| Histogram 插值对高频值不准；TopN 存精确 count（默认 128 个）|
| 6 | 基数估算 WHERE a='x' AND b='y'？| **sel(a) × sel(b) × totalRows**（假设条件独立）|
| 7 | 基数估算 WHERE a='x' OR b='y'？| sel(a) + sel(b) - sel(a)×sel(b)（容斥原理）|
| 8 | LIKE 基数估算？| **默认固定 10%**（Histogram 不支持模糊匹配）；可用扩展统计提高精度 |
| 9 | ★ 三种 Join Cost？| **Hash Join（rows 相加）+ Index Join（外表 × log(内表)）+ Merge Join（两表 sort + 归并）** |
| 10 | Hash Join Build 表选择？| **选行数少的**（Build 表全内存，越少越好）|
| 11 | Access Path 选择核心？| 全表扫 → 索引扫（覆盖 WHERE）→ ICP（索引过滤剩余）→ MRR（批量回表）|
| 12 | ★ TiDB vs MySQL 8.0 CBO 核心差异？| 见第三节对比表（8 维度）|
| 13 | TiDB MPP 与 CBO？| CBO 做物理计划选择后，Exchange 节点把任务分发到 TiKV 并行执行 |
| 14 | EXPLAIN vs EXPLAIN ANALYZE？| EXPLAIN 只估算 Cost；**EXPLAIN ANALYZE 跑一遍 vs 实际耗时对比** |
| 15 | EXPLAIN ANALYZE 诊断高频问题？| ① 索引没用到 ② rows 估 vs 实际差大 ③ 排序没用到索引 ④ Join 顺序选错 |
| 16 | ANALYZE TABLE 做什么？| 全表统计：行数/不同值数/null 率/直方图 + TopN |
| 17 | MySQL 8.0 Adaptive Hash Join？| 内存够 Hash Join；内存不够自动降级 Block Nested Loop；TiDB 也有类似自适应 |
| 18 | TiDB Plan Cache 做什么？| 缓存物理执行计划 → 相同模板 SQL 复用；避免每次重新优化 |
| 19 | 面试最高频考点排序？| **1. CBO 四阶段管道 2. Histogram + TopN 基数估算 3. 三种 Join Cost 4. EXPLAIN ANALYZE 诊断 5. TiDB vs MySQL 差异** |
| 20 | 学习路径？| ANALYZE TABLE → Histogram → rowCountByCondition → JoinType Cost → Access Path |
| 21 | 面试常见追问？| 统计过期怎么判断？| EXPLAIN ANALYZE 看 estimated rows vs actual rows 差 5x+ → 过期 → ANALYZE TABLE |
| 22 | 面试常见追问？| RBO 什么时候比 CBO 好？| 某些简单查询（单行点查）RBO 也够；但大多数场景 CBO 更准 |
| 23 | 面试陷阱？| 说 TiDB CBO 和 MySQL CBO 一样 → ❌ TiDB 有更丰富 Histogram、MPP、Plan Cache、Coprocessor Pushdown |
| 24 | ICP（Index Condition Pushdown）怎么提高性能？| 索引里能判断的条件（如 status='paid' 不在索引列里但在同表）→ 索引层过滤后再回表；减少回表次数 |
| 25 | MRR（Multi-Range Read）是什么？| 先用索引查出 PK 列表 → **按 PK 排序后批量回表**；比随机回表快 5-10x |
| 26 | TiDB Explain Format='verbose'？| 显示完整物理计划 + 统计信息 + Cost 细节；排查优化器选择原因 |
| 27 | 直方图倾斜怎么处理？| 高频值 → TopN 精确存储；TopN 覆盖不到 → Histogram bucket 插值不准 → 需要手动统计或扩展统计 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：tidb/planner/core/optimize.go + statistics/histogram.go + statistics.go + sql/sql_optimizer.cc  
**面试考点：27 题**  
**差异化点**：Histogram + TopN 基数估算、三种 Join Cost 模型、TiDB vs MySQL 8.0 CBO 8 维度对比、EXPLAIN ANALYZE 诊断
