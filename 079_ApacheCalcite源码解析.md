# Apache Calcite 源码深度解析（SQL 解析引擎）

> **源码基线**：Apache Calcite 1.38.x（最新稳定）  
> **核心仓库**：apache/calcite（Java 实现）  
> **面试权重**：⭐⭐⭐⭐（Doris/Flink/Spark/Presto 都用它）

---

## 一、Calcite 核心地位

```
┌─────────────────────────────────────────────────────────────┐
│  Calcite 是 **大数据时代的 SQL 解析标准引擎**               │
│                                                               │
│  谁在用 Calcite？（面试高频！）                               │
│                                                               │
│  ① Apache Doris（SQL 解析 + CBO 优化 + 物理计划）  ★          │
│  ② Apache Flink（Flink SQL 核心引擎）            ★          │
│  ③ Apache Spark（Spark SQL Catalyst 前身）                   │
│  ④ Presto / Trino（Facebook/IBM 出品）                        │
│  ⑤ Apache Hive（早期用过）                                    │
│  ⑥ Apache Beam（Google 出品）                                 │
│  ⑦ Dremio / ByteBase / 自研大数据引擎                        │
│                                                               │
│ ★ 为什么不用自己写 SQL Parser？                              │
│   SQL 语法极复杂（300+ 关键字 + 子查询 + Join + 聚合 + Window）│
│   → 自己写需要几千行代码 + 大量测试                          │
│   → Calcite 已经实现了完整的 SQL 2016 标准                   │
│   → 开源 + Apache License + 活跃维护                        │
│                                                               │
│ ★ Calcite = 大数据版的 Druid（Java 连接池）                   │
│   不是所有人都需要，但做大数据 SQL 就离不开它                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Calcite 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  Calcite 三阶段处理（面试高频！Doris/Flink/Spark 都这个流程）│
│                                                               │
│  SQL 输入                                                     │
│    ↓                                                          │
│  ① Parser（解析）                                             │
│    → 用 Antlr4 词法分析 + yacc 语法分析                      │
│    → 生成 **SqlNode（AST）**                                  │
│    → 类似 Javacc / ANTLR 工具链                              │
│    ↓                                                          │
│  ② Validator + Converter（校验 + 转换）                      │
│    → 查 MetaData（表/列是否存在）                              │
│    → SqlNode → RelNode（关系代数）                            │
│    → RelNode 是无 Schema 的中间表示（类似 Spark Catalyst）    │
│    ↓                                                          │
│  ③ Optimizer（优化器）                                        │
│    → **RBO（Rule-Based Optimizer）**：规则驱动              │
│    → **CBO（Cost-Based Optimizer）**：成本估算              │
│    → 规则：谓词下推 / 列裁剪 / Join 重排序 / 聚合下推      │
│    → 输出 **物理计划**（可执行的 RelNode 树）                 │
│                                                               │
│ ★ Calcite 的核心价值：                                       │
│   1. 通用 SQL Parser（支持 SQL 2016 标准）                   │
│   2. 可插拔的 Optimizer（RBO/CBO）                           │
│   3. 可插拔的 Schema（对接不同存储）                          │
│   4. 可插拔的物理计划（对接不同执行引擎）                     │
└─────────────────────────────────────────────────────────────┘
```

---

## 三、核心源码链路

### 3.1 Parser（SQL → SqlNode AST）

```java
// ★ Calcite Parser（面试高频）
//
// 输入 SQL：SELECT city, COUNT(*) FROM orders WHERE dt >= '2024-01-01' GROUP BY city
//
// 解析流程：
//   SqlParser parser = SqlParser.create(sql);
//   SqlNode ast = parser.parseStmt();
//
// 生成的 SqlNode AST：
//   SqlSelect {
//     selectList: [SqlIdentifier("city"), SqlFunction("COUNT", [SqlStar])]
//     from: SqlIdentifier("orders")
//     where: SqlBinaryOperator(">=", SqlIdentifier("dt"), SqlLiteral("2024-01-01"))
//     groupBy: [SqlIdentifier("city")]
//   }
//
// ★ 解析引擎：Antlr4 词法 + JavaCC 语法
//   Calcite 自带：
//     - SqlDialect（MySQL/PostgreSQL/Oracle 方言）
//     - 自定义函数注册
//
// ★ Doris/Flink/Spark 怎么接入？
//   ① 注册自定义 SqlDialect（解析 Doris/Flink 特有语法）
//   ② 注册自定义 Function（TO_TIMESTAMP/TOPK 等）
//   ③ 扩展 SqlNode 类型（Doris 的 SET_OPTION 等）
```

### 3.2 RelNode（关系代数中间表示 ★ 核心）

```java
// ★ Calcite RelNode（面试核心！）
//
// SqlNode（有 Schema）→ RelNode（无 Schema 的关系代数）
//
// 为什么需要 RelNode？
//   SqlNode 直接表示 SQL 语法 → 不适合优化（语法复杂）
//   RelNode 是 **标准化的关系代数表达式** → 适合优化
//   → 类似 Spark Catalyst 的 LogicalPlan
//
// RelNode 树结构（SELECT city, COUNT(*) FROM orders WHERE dt >= '2024-01-01' GROUP BY city）：
//
//   Aggregate(groupBy=city, agg=COUNT(*))
//     └─ Project(city)
//         └─ Filter(dt >= '2024-01-01')
//             └─ TableScan(orders)
//
// ★ 四种核心 RelNode：
//   TableScan       → 扫描表（数据源）
//   Filter          → WHERE 过滤
//   Project         → SELECT 列选择（列裁剪）
//   Aggregate       → GROUP BY 聚合
//   Join            → 表连接
//   Sort            → ORDER BY
//   Union           → UNION
//
// ★ RelNode 为什么"无 Schema"？
//   RelNode 内部的字段引用用 int 下标（0, 1, 2...）
//   不是 SqlIdentifier（有名字）
//   → 方便优化时修改列顺序 / 裁剪列
//   → Optimizer 不需要关心列名
```

### 3.3 Optimizer（RBO + CBO ★ 面试核心）

```java
// ★ Calcite Optimizer（面试高频！）
//
// ① RBO（Rule-Based Optimizer，规则驱动）
//    → 一组固定规则依次应用
//    → 不估算成本（简单但性能有限）
//
//    典型规则（Calcite 内置）：
//      FilterPushDownRule      → WHERE 条件下推到 TableScan 之后立即过滤
//      ProjectPushDownRule     → 列裁剪下推到 TableScan
//      JoinReorderRule         → Join 重排序（小表 JOIN 大表）
//      AggregatePushDownRule   → 聚合下推
//      PruneEmptyRules         → 裁剪空节点
//
//    示例：FilterPushDownRule
//      Aggregate → Filter → TableScan
//        ↓ 应用规则
//      Aggregate → Project → Filter → TableScan
//      （Filter 下推到 TableScan 之后 → 减少数据量）
//
// ② CBO（Cost-Based Optimizer，成本估算 ★ 面试最高频）
//    → 估算每个执行计划的成本（IO + CPU + 网络）
//    → 选成本最低的计划
//    → Doris/Flink/Spark 都实现了 CBO
//
//    成本估算核心：
//      - 行数（RowCount）→ 行数越多，IO 越大
//      - 列数（ColumnCount）→ 列越多，内存越多
//      - 算子成本（算子类型 → 固定成本）
//      - Join 策略成本（HashJoin vs MergeJoin vs BroadcastJoin）
//
//    Doris CBO 实现（面试高频）：
//      → 统计信息：表行数 / 列基数（distinct values）/ 列分布
//      → 估算 Filter 选择性：dt >= '2024-01-01' 选中多少行？
//      → 估算 Join 策略：小表 → BROADCAST JOIN（Doris 特色）
//      → 最终选成本最低的物理计划
//
// ★ Volcano 算法（CBO 核心算法）：
//   探索搜索空间（所有可能的物理计划）→ 计算每个成本 → 选最低
//   → Calcite 默认用 Volcano
//   → Doris/Spark/Presto 都用 Volcano
//
// ★ Top-Down vs Bottom-Up：
//   Top-Down（自顶向下）：从 SqlNode 往下推规则
//   Bottom-Up（自底向上）：从 TableScan 往上推规则
//   → Calcite 支持两种，Volcano 是 Bottom-Up
```

### 3.4 物理计划 → 执行引擎

```java
// ★ 物理计划（Convention/Implement）→ 对接不同执行引擎
//
// Calcite 本身**不执行**！只负责生成最优物理计划
// → 每个大数据引擎（Doris/Flink/Spark）把物理计划翻译成自己的执行计划
//
// 对接方式（面试高频）：
//
// ① Doris 对接 Calcite：
//    Calcite RelNode 树
//    → Doris LogicalOperator 树（Doris 自研，类似 RelNode）
//    → Doris PhysicalOperator（MPP 分布式执行计划）
//    → Doris BE 并行执行
//
// ② Flink 对接 Calcite：
//    Calcite RelNode 树
//    → Flink LogicalPlan（Flink 自研，类似 RelNode）
//    → Flink ExecGraph（JobGraph / StreamGraph）
//    → Flink TaskManager 执行
//
// ③ Spark 对接 Calcite：
//    早期 Spark SQL 直接用 Calcite
//    → 后来 Spark 自研 Catalyst（借鉴 Calcite 思想）
//    → 但 Spark 3.x 又支持 Calcite Planner 插件
//
// ★ Convention 机制（Calcite 核心设计）：
//   Convention = 执行引擎的"方言"
//   → Calcite 内置：None（关系代数）/ Enumerable（Enumerable 执行器）
//   → Doris 自定义：DorisConvention（Doris MPP 执行器）
//   → Flink 自定义：FlinkConvention（Flink DataStream 执行器）
//   → 每个 Implement 规则把 RelNode 从 None Convention 转到目标 Convention
```

---

## 四、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Calcite 是什么？| **Java 实现的通用 SQL 解析引擎** → 提供 Parser + Validator + Optimizer → Doris/Flink/Spark/Presto 都用它 |
| 2 | Calcite 三阶段处理？| **Parser（SqlNode AST）→ Validator+Converter（RelNode 关系代数）→ Optimizer（RBO/CBO）** |
| 3 | RelNode 为什么无 Schema？| RelNode 用 int 下标引用字段 → 方便 Optimizer 裁剪列/改列顺序 → 不需要关心列名 |
| 4 | RBO vs CBO？| **RBO：规则驱动（简单快速）**；**CBO：成本估算（选择最优）**；Doris/Flink/Spark 都实现 CBO |
| 5 | FilterPushDownRule？| **WHERE 条件下推到数据源之后立即过滤** → 减少数据量 → RBO 最核心的规则 |
| 6 | Doris CBO 怎么估算成本？| 表行数 + 列基数（distinct values）+ Filter 选择性 + Join 策略（Broadcast/Hash）→ 选成本最低计划 |
| 7 | Calcite Convention 机制？| **执行引擎方言** → Calcite 内置 None/Enumerable → Doris/Flink 自定义 DorisConvention/FlinkConvention → Implement 规则转换 |
| 8 | Calcite vs Spark Catalyst？| Spark Catalyst 借鉴 Calcite → 自研更简单；但 Spark 3.x 又支持 Calcite Planner 插件；Flink/Presto 原生用 Calcite |
| 9 | Calcite Implement 规则？| **把 RelNode 从 None Convention 转到目标 Convention** → Doris/Flink/Spark 各有自己的 Implement |
| 10 | Volcano 算法？| **CBO 核心算法** → Bottom-Up 探索搜索空间 → 计算每个计划成本 → 选最低；Calcite/Presto 用 Volcano |
| 11 | Calcite 学习路径？| 看 Parser 入口 → RelNode 树 → FilterPushDownRule → CBO Volcano → Doris 集成代码 |
| 12 | Calcite 为什么不自己执行？| Calcite 定位是 Parser + Optimizer → 不同引擎执行模型完全不同（Doris BE 并行 vs Flink TaskManager）→ 引擎自己翻译物理计划 |
| 13 | Calcite Schema 怎么对接？| Calcite Schema SPI（自动发现）/ 手动注册 → 对接 Doris Catalog / Hive Metastore / Kafka Topic |
| 14 | Calcite 自定义 SQL 方言？| 扩展 SqlDialect（MySQL/PG/Oracle 差异）+ 注册自定义 Function（Doris TOPK/T_DIGEST） |
| 15 | Calcite 为什么不流行但又离不开？| 不是用户直接用 → 大数据引擎用它 → 用户不感知；但做大数据 SQL 就必须懂它 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Calcite 1.38.x（Java）  
**面试考点：15 题**  
**差异化点**：所有大数据引擎的 SQL 解析/优化标准引擎（Doris/Flink/Spark/Presto）
