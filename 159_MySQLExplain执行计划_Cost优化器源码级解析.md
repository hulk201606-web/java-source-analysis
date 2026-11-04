# MySQL Explain 执行计划 + Cost 优化器源码级深度解析

> **源码基线**：MySQL 8.0（C++）  
> **核心位置**：sql/sql_optimizer.cc + sql/opt_explain.cc + sql/opt_costmodel.h  
> **面试权重**：⭐⭐⭐⭐⭐（SQL 调优核心，DBA/后端 100% 高频）

---

## 一、优化器整体流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 一条 SQL 的优化链路（面试必背）                     │
│                                                               │
│  SQL 文本                                                     │
│   → ① Parser（词法/语法解析，生成解析树）                    │
│   → ② Resolver/Analyzer（名字解析、权限、视图展开）         │
│   → ③ 逻辑变换（Query Rewrite，子查询改写、常量折叠）       │
│   → ④ ★ Optimizer（基于代价 CBO，选执行计划）              │
│        - 访问路径选择（用哪个索引 / 全表扫）                │
│        - Join 顺序、Join 算法                                │
│   → ⑤ 执行计划（执行器按计划扫描/回表/过滤/排序）          │
│                                                               │
│  MySQL 优化器：历史上是启发式规则 + 代价模型                 │
│   8.0 引入 Hash Join、新直方图、超图优化器探索（有限）       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、EXPLAIN 关键字段详解

```
┌─────────────────────────────────────────────────────────────┐
│  ★ EXPLAIN 输出列（面试必背逐列）                            │
│                                                               │
│  id：查询序号（子查询/UNION 会有多个 id）                    │
│  select_type：SIMPLE / PRIMARY / SUBQUERY / DERIVED...      │
│  table：表名（或别名、derived 表）                           │
│  ★ type：访问类型（重要！性能从好到差）：                    │
│     system > const > eq_ref > ref > range >                  │
│     index > ALL                                               │
│     - const：主键/唯一索引等值，最多 1 行                    │
│     - eq_ref：Join 时被驱动表主键/唯一索引等值               │
│     - ref：普通索引等值                                       │
│     - range：索引范围扫描（>、<、between、in）               │
│     - index：扫整个索引树（比 ALL 好，索引小）               │
│     - ALL：★全表扫描（必须优化）                             │
│                                                               │
│  ★ possible_keys：可能用到的索引                             │
│  ★ key：实际选择的索引（NULL = 没用索引）                    │
│  key_len：使用的索引长度（判断联合索引用到了几列）          │
│  ref：索引比较的来源（const / 列 / func）                    │
│  ★ rows：预估要扫描的行数（CBO 估算，非精确）               │
│  filtered：过滤后剩余百分比（rows × filtered% ≈ 结果行数）   │
│  ★ Extra：额外信息（最重要，单列详解）                       │
│
└─────────────────────────────────────────────────────────────┘
```

### Extra 常见值（面试高频）

```
┌─────────────────────────────────────────────────────────────┐
│  Using index：★覆盖索引，不回表（好）                        │
│  Using index condition：★ICP 索引下推（好）                 │
│  Using where：Server 层过滤（回表后再过滤）                  │
│  Using filesort：★额外排序（无法用索引有序性，需优化）       │
│  Using temporary：★用了临时表（GROUP BY/DISTINCT，需优化）   │
│  Using join buffer (Block Nested Loop / Hash Join)：Join 缓冲│
│  Using intersect(...) / Using union(...)：索引合并           │
│  Impossible WHERE：条件恒假，无数据                          │
│  Select tables optimized away：聚合用索引直接得出（min/max）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Cost 代价模型源码级

### 3.1 代价组成

```cpp
// ★ MySQL 8.0 Cost Model（源码级，面试核心）
//
// sql/opt_costmodel.h
//
// class CostModelServerCosts {
//     // ★ 服务器级常数代价（可通过 mysql.server_costs 配置）
//     double disk_temptable_create_cost;  // 磁盘临时表创建（默认 20）
//     double disk_temptable_row_cost;     // 磁盘临时表每行（0.5）
//     double key_compare_cost;            // 索引比较（0.05）
//     double memory_temptable_create_cost;// 内存临时表（1）
//     double memory_temptable_row_cost;   // 内存临时表每行（0.1）
//     double row_evaluate_cost;           // ★ 评估一行条件的代价（0.1）
// };
//
// class CostModelSECosts（存储引擎级，engine_cost 表）：
//     double io_block_read_cost;   // ★ 读一个磁盘页的代价（默认 1.0）
//     double memory_block_read_cost; // 内存中读一个页（默认 0.25）
//
// // 一条访问路径的总代价 ≈
// //   Cost = IO 代价（页数 × io_block_read_cost）
// //        + CPU 代价（扫描行数 × row_evaluate_cost）
// //        + 排序/临时表等附加代价
//
// ★ 关键认知：
//   CBO 比较的不是"实际执行时间"，而是这个 Cost 数值
//   统计信息不准 → Cost 估算错 → 选错索引
//
```

### 3.2 统计信息与索引选择

```
┌─────────────────────────────────────────────────────────────┐
│  ★ CBO 如何估算"走索引 vs 全表扫"（面试必背）                │
│                                                               │
│  走二级索引的代价 ≈                                           │
│    预估匹配行数 × 回表随机 IO（io_block_read_cost）          │
│    + 索引范围扫描代价                                         │
│                                                               │
│  全表扫代价 ≈                                                 │
│    聚簇索引总页数 × io_block_read_cost（顺序读相对便宜）      │
│    + 总行数 × row_evaluate_cost                              │
│                                                               │
│  ★ 为什么查出 20% 以上数据时优化器倾向全表扫？                │
│   回表是随机 IO（每行可能一个页），代价高                    │
│   匹配行数多时，大量随机回表比顺序全表扫更贵                 │
│   → 优化器转 ALL（这是符合代价模型的正确决策）              │
│                                                               │
│  统计信息来源：                                                │
//  - 表：cardinality（表行数估算）
//  - 索引：各列的 Cardinality（不同值数量）
//  - 记录长度、聚簇索引页数
//  - ★ 8.0 直方图（Histogram）：对非索引列做数据分布统计
//    ANALYZE TABLE t UPDATE HISTOGRAM ON col WITH 100 BUCKETS;
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 统计信息不准的后果

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 统计信息陈旧 → 执行计划抖动（面试高频生产问题）           │
│                                                               │
│  InnoDB 索引基数默认是【采样估算】（不是精确值）：            │
│   innodb_stats_persistent_sample_pages（默认 20 页）         │
│   采样页太少 + 数据倾斜 → cardinality 偏差 → 选错索引       │
│                                                               │
│  解决：                                                        │
│   ① ANALYZE TABLE t; 重新统计                                │
│   ② 调大持久化采样页数（更准但 ANALYZE 慢）                 │
│   ③ 8.0 直方图改善非索引列选择率                             │
│   ④ 极端情况用索引提示：                                       │
│      USE INDEX / FORCE INDEX / IGNORE INDEX                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Join 算法

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL Join 算法演进（面试高频）                           │
│                                                               │
│  ① Nested Loop Join（NLJ，嵌套循环）：                       │
│     外层取一行 → 在内层表用索引查                            │
│     被驱动表有索引时高效（eq_ref/ref）                       │
│                                                               │
│  ② Block Nested Loop（BNL，5.7 及以前）：                    │
│     被驱动表无索引：外层一批行放 join buffer                 │
│     内层全表扫，批量和 buffer 比较（减少外层扫描次数）       │
│                                                               │
│  ③ ★ Hash Join（8.0.18+，替代大部分 BNL）：                  │
│     小表建内存 Hash 表 → 探测大表                            │
│     无索引 Join 性能大幅提升                                  │
│                                                               │
│  Join 顺序选择：优化器枚举表的排列组合，选 Cost 最小的顺序   │
│  （小表驱动大表的原理就在这）                                │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、EXPLAIN ANALYZE 与实战

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 8.0 EXPLAIN 增强（面试加分）                        │
│                                                               │
│  EXPLAIN ANALYZE：真正执行 SQL，输出【实际】耗时/行数         │
│   → 对比优化器预估 rows 和 actual rows，定位统计信息问题     │
│  EXPLAIN FORMAT=TREE：树状计划（显示 Hash Join 结构）        │
│  EXPLAIN FORMAT=JSON：详细代价（cost、buffer 使用）          │
│                                                               │
│  调优决策链：                                                  │
│   ① 看 type=ALL？→ 是否该加索引/数据量本来就大                │
│   ② key=NULL 而 possible_keys 有值？→ 统计信息/回表代价       │
│   ③ Using filesort/temporary？→ 索引有序性/覆盖索引优化      │
│   ④ rows 巨大？→ where 条件能否走索引、最左前缀              │
│   ⑤ EXPLAIN ANALYZE 实际 vs 预估差异大？→ ANALYZE TABLE     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | SQL 优化链路？| Parser → Resolver → Rewrite → **CBO 优化器选计划** → 执行 |
| 2 | type 访问类型顺序？| system>const>eq_ref>ref>range>index>**ALL（全表扫）** |
| 3 | const/eq_ref/ref 区别？| const 主键等值 1 行；eq_ref Join 主键/唯一；ref 普通索引等值 |
| 4 | key_len 作用？| 判断联合索引实际用到了几列（最左前缀命中长度）|
| 5 | rows 精确吗？| **预估值**（CBO 采样统计），非精确行数 |
| 6 | filtered？| 过滤后剩余百分比；rows × filtered% ≈ 最终行数 |
| 7 | Extra 好的信号？| Using index（覆盖）、Using index condition（ICP）|
| 8 | Extra 坏的信号？| **Using filesort（额外排序）、Using temporary（临时表）、ALL** |
| 9 | Cost 由什么组成？| **IO 代价（页数×io_block_read_cost）+ CPU 代价（行数×row_evaluate_cost）** |
| 10 | 关键代价常数？| io_block_read_cost=1.0、memory_block=0.25、row_evaluate=0.1（可配）|
| 11 | 为什么查得多反而全表扫？| 回表随机 IO 代价 > 顺序全表扫；匹配行多（约 >15-20%）时 CBO 选 ALL |
| 12 | 统计信息来源？| 表/索引基数（采样）+ 记录长度 + **8.0 直方图（非索引列）** |
| 13 | cardinality 不准？| InnoDB 采样估算（默认 20 页），数据倾斜会偏差 → ANALYZE TABLE |
| 14 | 直方图怎么建？| ANALYZE TABLE t UPDATE HISTOGRAM ON col WITH 100 BUCKETS |
| 15 | Join 三算法？| **NLJ（索引嵌套循环）+ BNL（旧版无索引批量）+ Hash Join（8.0.18+）** |
| 16 | 为什么小表驱动大表？| CBO 枚举 Join 顺序，外层小表使内层探测总代价最小 |
| 17 | 索引提示？| USE INDEX / FORCE INDEX / IGNORE INDEX（统计信息错误时兜底）|
| 18 | EXPLAIN ANALYZE？| 8.0 真正执行，输出**实际**行数/耗时，对比预估定位问题 |
| 19 | 面试最高频考点排序？| **1. type/key/rows 2. Extra 3. Cost 组成 4. 为何全表扫 5. Join 算法** |
| 20 | 学习路径？| sql_optimizer → opt_costmodel → 访问路径代价 → EXPLAIN 列 → Hash Join |
| 21 | 面试常见追问？| 加了索引为什么没走？→ 函数/隐式转换/最左前缀/回表代价/统计信息 |
| 22 | 索引失效常见原因？| 列上函数/运算、隐式类型转换、like '%x'前导通配、OR、不满足最左前缀 |
| 23 | Using filesort 一定磁盘排序吗？| 不一定；名字误导，内存 sort_buffer 够就在内存排，不够才落盘 |
| 24 | ORDER BY 优化？| 让排序利用索引有序性（索引列顺序一致），避免 filesort |
| 25 | FORMAT=JSON 看什么？| 详细 cost、prefix_cost、read_cost、eval_cost、buffer 使用 |
| 26 | 面试陷阱？| 说 rows 是精确扫描行数 → ❌ 是优化器基于统计的估算 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：sql/sql_optimizer.cc + sql/opt_costmodel.h + sql/opt_explain.cc  
**面试考点：26 题**  
**差异化点**：EXPLAIN 逐列 + Cost 模型常数源码 + 回表随机 IO 决策 + 统计信息/直方图 + Hash Join + EXPLAIN ANALYZE
