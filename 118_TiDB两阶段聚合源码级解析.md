# TiDB 两阶段聚合 + TopN + Limit 源码级深度解析

> **源码基线**：TiDB v8.0.x（Go）  
> **核心仓库**：pingcap/tidb → executor/ + planner/core/  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 执行层核心，高频必问）

---

## 一、TiDB 聚合优化核心问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB 两阶段聚合 = 分布式聚合性能优化                      │
│                                                               │
│  场景：SELECT dept, SUM(salary) FROM employees GROUP BY dept   │
│                                                               │
│  ❌ 单阶段聚合（传统方式）：                                   │
│   TiDB Server 从所有 TiKV Region 拉全量数据（1000w 行）       │
│   → TiDB 内存 HashAggregate 做 GROUP BY                     │
│   → **网络 TB 级传输！TiDB 内存爆炸！**                       │
│
│  ✅ 两阶段聚合（TiDB 优化）：                                  │
│   ① **Partial Agg**（部分聚合）：                             │
│      TiDB 下发 Agg 到每个 TiKV Region                        │
│      TiKV 本地 RocksDB 扫描 + 本地 HashAgg                   │
│      → 每个 Region 返回小的聚合结果（KB 级）                │
│
│   ② **Final Agg**（最终聚合）：                               │
│      TiDB Server 收集所有 Region 的 Partial Agg 结果        │
│      → 再做一次 HashAgg（合并结果）                          │
│      → **网络传输从 TB 降到 MB！**                            │
│
│ ★ 两阶段聚合 vs TiKV Coprocessor：                            │
│   Coprocessor 是 TiKV 端的下推计算执行器                     │
│   两阶段聚合是 TiDB Optimizer 的优化策略                     │
│   → 两阶段聚合依赖 Coprocessor 执行 Partial Agg             │
│   → 两者是配合关系（TiDB 优化 + TiKV 执行）                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、两阶段聚合源码级

### 2.1 Optimizer 优化规则

```go
// ★ TiDB 两阶段聚合 Optimizer 源码级（面试核心！）
//
// planner/core/rule_aggregation_push_down.go
//
// 核心优化：把一个 Global Agg 拆成 Partial Agg + Final Agg
//
// func splitAggForUnion(agg *PhysicalAggregation) *PhysicalAggregation {
//     // ★ 判断能不能拆成两阶段
//     // 条件 1: 下推到 TiKV 可行（函数 TiKV Coprocessor 支持）
//     // 条件 2: 没有 DISTINCT（DISTINCT 不能简单下推）
//     // 条件 3: GROUP BY 列是 TiKV 索引列或主键
//
//     if !canPushDown(agg) {
//         return agg  // 不能拆，保持原样
//     }
//
//     // ★ 拆成两阶段：
//     // ① 重写 AggFunc（去掉 Final Agg 的 GROUP BY）
//     partialAgg := rewriteAggToPartial(agg)
//     //   SUM(x) → SUM(x)（Partial）
//     //   COUNT(x) → COUNT(x)（Partial）
//     //   DISTINCT 不能拆！
//
//     // ② 创建 Final Agg（合并 Partial 结果）
//     finalAgg := rewriteAggToFinal(agg)
//     //   SUM(x) → SUM(SUM(x))（Final，把 Partial 的 SUM 再加起来）
//     //   COUNT(x) → SUM(COUNT(x))（Final，把 Partial 的 COUNT 加起来）
//
//     // ③ 插入 Exchange 算子（TiDB ↔ TiKV 数据传输）
//     exchange := &PhysicalExchange{
//         PartitionType: HashPartition,  // 按 GROUP BY Hash 分区
//     }
//
//     // 最终计划：TableScan → PartialAgg → Exchange → FinalAgg
//     return finalAgg
// }
//
```

### 2.2 各 AggFunc 两阶段改写规则

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB AggFunc 两阶段改写规则（面试必背！）                  │
│                                                               │
│  ┌──────────────┬──────────────────┬──────────────────┐      │
│  │ 原始函数        │ Partial Agg       │ Final Agg          │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ SUM(col)      │ SUM(col)          │ SUM(SUM(col))      │      │
│  │               │ TiKV 本地求和      │ TiDB 合并求和       │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ COUNT(col)    │ COUNT(col)         │ SUM(COUNT(col))   │      │
│  │               │ TiKV 本地计数      │ TiDB 合并计数       │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ AVG(col)      │ SUM(col), COUNT(col)│ SUM/COUNT（计算） │      │
│  │               │ TiKV 同时算 sum+count│ TiDB sum/count  │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ MAX(col)      │ MAX(col)           │ MAX(MAX(col))     │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ MIN(col)      │ MIN(col)           │ MIN(MIN(col))     │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ DISTINCT     │ ❌ 不能下推          │ 只在 TiDB 做       │      │
│  ├──────────────┼──────────────────┼──────────────────┤      │
│  │ GROUP_CONCAT  │ ❌ 不能下推          │ 只在 TiDB 做       │      │
│  └──────────────┴──────────────────┴──────────────────┘      │
│
│ ★ AVG 两阶段特殊处理（面试高频！）：                          │
│   AVG 不能简单下推（TiKV 算 partial avg → TiDB 再平均不对）│
│   → TiKV 同时算 SUM(col) 和 COUNT(col)（Partial 两个值）   │
│   → TiDB Final Agg：SUM(SUM) / SUM(COUNT) = 最终 AVG       │
│   → 这是数学上正确的做法                                     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、HashAggregate Executor 源码级

### 3.1 TiDB 端 HashAggregate

```go
// ★ TiDB HashAggregate Executor 源码级（Go）
//
// executor/agg_hash.go
//
// type HashAggExec struct {
//     // ★ HashTable：GROUP BY key → Agg 结果
//     // 每个 Key 对应一个 AggResult（SUM/COUNT/AVG 累计值）
//     hashTable      *HashTable
//     groupByItems   []expression.Expression  // GROUP BY 表达式
//     aggFuncs       []*AggFuncDesc          // 聚合函数列表
//     isPartial      bool                    // 是不是 Partial Agg
// }
//
// // ★ 核心：处理一行数据
// func (e *HashAggExec) Next(ctx context.Context) (Row, error) {
//     // 1. 从子节点拉数据（TiKV Coprocessor 返回的 Partial Agg 结果）
//     row, err := e.child.Next(ctx)
//     if err != nil {
//         return nil, err
//     }
//     if row == nil {
//         // 子节点数据读完了 → 开始输出
//         return e.outputNext()
//     }
//
//     // 2. 提取 GROUP BY key
//     key := e.groupByItems.Eval(row)
//
//     // 3. 查找 HashTable
//     aggResult, exists := e.hashTable.Get(key)
//     if !exists {
//         // ★ 新 Key → 创建新 AggResult
//         aggResult = newAggResult(e.aggFuncs, e.isPartial)
//         e.hashTable.Put(key, aggResult)
//     }
//
//     // 4. 更新 AggResult
//     // Partial Agg: 直接累加
//     // Final Agg: 把 TiKV 返回的 Partial 结果再合并
//     aggResult.Update(row)
//
//     return e.Next(ctx)  // 继续读下一行
// }
//
```

### 3.2 TiKV 端 Partial Agg

```rust
// ★ TiKV Coprocessor 端 Partial Agg 源码级（Rust）
//
// components/coprocessor/src/endpoint.rs
//
// pub fn handle_agg(
//     ranges: Vec<KeyRange>,
//     group_by: Vec<Expr>,
//     agg_funcs: Vec<AggFunc>,
// ) -> Result<Vec<Row>> {
//     // ★ TiKV 本地 RocksDB 扫描 + Partial HashAgg
//     let mut hash_table: HashMap<Vec<Value>, AggState> = HashMap::new();
//
//     // 遍历 Region 内所有 Key（RocksDB Iterator）
//     for row in scan_ranges(&ranges)? {
//         // ① 提取 GROUP BY key
//         let key = evaluate_exprs(&group_by, &row);
//
//         // ② 查找/创建 AggState
//         let state = hash_table.entry(key)
//             .or_insert_with(|| AggState::new(&agg_funcs));
//
//         // ③ Partial Agg Update（本地累加到 AggState）
//         state.update(&row);
//     }
//
//     // ★ 返回 Partial Agg 结果（远小于原始数据！）
//     Ok(hash_table.into_iter()
//         .map(|(key, state)| state.to_row(key))
//         .collect())
// }
//
// ★ TiKV Partial Agg vs TiDB Final Agg 网络传输：
//   1000w 行原始数据 → TiKV Partial Agg → 1000 行结果
//   → 网络传输量减少 10000x！
//
```

---

## 四、TopN + Limit 两阶段

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB TopN + Limit 两阶段优化（面试高频！）                │
│                                                               │
│  场景：SELECT * FROM t ORDER BY score DESC LIMIT 10          │
│                                                               │
│  ❌ 无两阶段：                                                │
│   TiDB 从所有 TiKV Region 拉全量数据（100w 行）              │
│   → TiDB 全量排序 → 取前 10                                  │
│
│  ✅ 两阶段 TopN：                                             │
│   ① TiKV 每个 Region 本地排序 → 取前 10 → 返回              │
│      10 个 Region → 总共返回 100 行（而不是 100w！）        │
│   ② TiDB 对 100 行全局排序 → 取前 10 → 最终结果             │
│
│ ★ TiDB Optimizer 源码级（planner/core/rule_topn_push_down.go）：│
│                                                               │
│ // 原始计划：TableScan → Sort → Limit(10)                    │
│ // 优化后：TableScan → TopN(10) → Exchange → TopN(10)      │
│ //         ↑ TiKV 端 Partial TopN  ↑ TiDB 端 Final TopN      │
│
│ func pushTopN(plan LogicalPlan) {
//     // ① 判断 Sort + Limit 能不能下推到 TiKV
//     if canPushDownToTiKV(plan) {
//         // ② 拆成 Partial TopN + Final TopN
//         partialTopN := createPartialTopN(plan, limit)
//         finalTopN := createFinalTopN(plan, limit)
//
//         // ③ 插入 Exchange（HashPartition 或 SinglePartition）
//         exchange := createExchange(plan)
//
//         // ④ 组装：TableScan → PartialTopN → Exchange → FinalTopN
//         finalTopN.SetChild(exchange)
//         exchange.SetChild(partialTopN)
//     }
// }
//
// ★ TopN vs Limit：                                              │
│   Limit 无 ORDER BY → TiKV 直接返回前 N 条（不需要排序）     │
│   TopN 有 ORDER BY → TiKV 本地排序 → 返回前 N 条            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、完整 TiDB 执行计划示例

```sql
-- ★ EXPLAIN 看 TiDB 优化后的两阶段计划
EXPLAIN SELECT dept, SUM(salary)
FROM employees
GROUP BY dept;

-- TiDB 优化后（两阶段聚合）：
+----------+------------+-----------+-------------------------------+
| id       | task       | operator  | info                          |
+----------+------------+-----------+-------------------------------+
| 1        | root       | HashAgg   | group by:employees.dept        |  ← Final Agg
|          |            |           | funcs:sum(employees.salary)   |
| 2        | root       | Exchange  | data:HashPartition(employees  |
|          |            |           |   .dept)                      |
| 3        | cop[TiKV]  | HashAgg   | group by:employees.dept        |  ← Partial Agg
|          |            |           | funcs:sum(employees.salary)   |
| 4        | cop[TiKV]  | TableScan | table:employees                |  ← TiKV 扫描
+----------+------------+-----------+-------------------------------+

-- ★ 关键点：
-- ① task 列：root = TiDB Server 执行；cop[TiKV] = TiKV Coprocessor 下推
-- ② Partial Agg 在 TiKV 端执行（cop[TiKV] task）
-- ③ Exchange 是 TiDB 和 TiKV 之间的数据传输层
-- ④ Final Agg 在 TiDB 端执行（root task）
--
```

---

## 六、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiDB 两阶段聚合核心？| **Partial Agg（TiKV 本地）→ Exchange → Final Agg（TiDB 合并）** |
| 2 | 为什么要两阶段？| **网络传输从 TB 降到 MB**；TiKV 本地 RocksDB 扫描 + 本地聚合 |
| 3 | 两阶段聚合 vs Coprocessor？| Coprocessor 是 TiKV 执行器；两阶段聚合是 TiDB Optimizer 优化策略 → 配合关系 |
| 4 | 哪些 AggFunc 能下推？| **SUM/COUNT/AVG/MAX/MIN 可以下推；DISTINCT/GROUP_CONCAT 不能下推** |
| 5 | AVG 两阶段特殊处理？| TiKV 同时算 SUM(col) + COUNT(col)；TiDB Final：SUM/COUNT |
| 6 | TiDB HashAggregate Executor？| executor/agg_hash.go；HashMap<GROUP_BY Key, AggResult> |
| 7 | TiKV Partial Agg？| Rust 实现；本地 HashMap + RocksDB Iterator 扫描 |
| 8 | TopN 两阶段？| TiKV 每个 Region 本地排序取前 N → TiDB 全局排序取前 N |
| 9 | Limit vs TopN？| Limit 无 ORDER BY 直接返回；TopN 有 ORDER BY 需要 TiKV 排序 |
| 10 | Exchange 算子？| TiDB ↔ TiKV 数据传输层；HashPartition（GROUP BY）/SinglePartition |
| 11 | EXPLAIN 怎么看两阶段？| task 列：cop[TiKV] = TiKV 下推；root = TiDB 执行 |
| 12 | 面试最高频考点排序？| **1. 两阶段聚合核心 2. 能下推的 AggFunc 3. AVG 特殊处理 4. TopN 两阶段 5. EXPLAIN 解读** |
| 13 | 学习路径？| rule_aggregation_push_down.go → agg_hash.go → TiKV endpoint.rs agg → rule_topn_push_down.go |
| 14 | 面试常见追问？| DISTINCT 为什么不能下推？→ TiKV 不知道全局 DISTINCT，只知道本地，可能重复 |
| 15 | 两阶段聚合 vs Spark Stage？| Spark 每个 Stage 也是 Map → Shuffle → Reduce；思想一样（分布式聚合经典模式）|
| 16 | TiDB 不做两阶段聚合会怎样？| 全量数据拉回 TiDB → 网络和内存爆炸 → 大表查询 OOM |
| 17 | TiDB vs Doris 聚合？| TiDB：分布式两阶段 + TiKV Coprocessor；Doris：本地 MPP 向量化 |
| 18 | 2024 新特性？| TiDB 8.0+ **Agg 下推到 TiFlash**（列存引擎，向量化聚合更快）|
| 19 | 面试高频场景？| 大表 GROUP BY（1000w 行）+ TOP N 热门商品统计 |
| 20 | GROUP BY 列必须是索引吗？| 不是；但索引可以加速 TiKV 层的 Partial Agg 扫描 |
| 21 | TiDB HashAgg vs SortAgg？| HashAgg：HashTable 实现；SortAgg：先排序再聚合；HashAgg 更通用 |
| 22 | 面试陷阱？| 说 TiKV Coprocessor = 两阶段聚合 → ❌ Coprocessor 是执行器，两阶段聚合是优化策略 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：planner/core/rule_aggregation_push_down.go + executor/agg_hash.go + components/coprocessor/src/endpoint.rs  
**面试考点：22 题**  
**差异化点**：Partial→Final 两阶段 + AVG 特殊处理 + TopN 两阶段 + EXPLAIN 解读 + TiKV vs TiDB 分工
