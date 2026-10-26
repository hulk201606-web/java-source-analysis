# Apache Doris BE 向量化执行引擎源码级深度解析

> **源码基线**：Doris 2.1.x（C++）  
> **核心仓库**：apache/doris → be/src/vec/ + be/src/exec/  
> **面试权重**：⭐⭐⭐⭐⭐（OLAP 新王者，向量化执行高频必问）

---

## 一、Doris 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Apache Doris = 开源 MPP OLAP 分析引擎                    │
│                                                               │
│  Doris 架构：                                                  │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ FE（Frontend，Java）★ 控制面                          │   │
│  │   ├── SQL Parser + Optimizer                            │   │
│  │   ├── Catalog（元数据管理）                             │   │
│  │   ├── 调度（把 SQL 拆成 Fragment 分发到 BE）            │   │
│  │   └── HA（FE Active + Standby + Observer）              │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ Fragment 分发                                        │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ BE (Backend)│ BE (Backend)│ BE (Backend)│ ★ 数据面 ★ 本文重点！│
│  │ 向量化执行  │ 向量化执行  │ 向量化执行  │                      │
│  │ + 存储层    │ + 存储层    │ + 存储层    │                      │
│  └──────────┴──────────┴──────────┘                         │
│
│ ★ Doris vs ClickHouse vs TiDB 核心区别（面试高频）：           │
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │            │ Doris         │ ClickHouse    │ TiDB          │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 计算模型    │ **MPP + Pipeline** │ 列存 + 向量化  │ 分布式事务    │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 执行引擎    │ **向量化**    │ 向量化          │ 行存+TiKV Coprocessor │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 存储模型    │ 列式 + 分区+分桶│ 列式 + MergeTree│ TiKV (LSM-Tree) │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 事务        │ ACID（2.1+）│ 最终一致      │ ACID（Percolator）│
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 典型场景    │ OLAP + 实时数仓│ 日志分析      │ 混合负载      │
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、向量化执行 vs 行式执行

### 2.1 核心对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 向量化执行 = 批处理 N 行数据（面试必背！）                │
│                                                               │
│  行式执行（Row-Oriented）：                                    │
│   每次处理 1 行 → 100w 行 = 100w 次函数调用                 │
│   比如 SELECT SUM(score) FROM t：                             │
│   for (row in table) { sum += row.score; }  ← 循环 100w 次   │
│
│  向量化执行（Columnar / Vectorized）：                         │
│   每次处理一批（比如 1024 行）→ 100w 行 ≈ 977 次调用         │
│   比如 SELECT SUM(score) FROM t：                             │
│   for (batch in table) {                                      │
│     simd_sum(batch.score[0..1023]);  ← SIMD 指令并行计算    │
│   }                                                           │
│
│ ★ 向量化三大优势（面试高频）：                                  │
│   ① **SIMD 加速**：CPU SIMD 指令并行处理 16/32/64 个数据    │
│   ② **减少函数调用**：批处理减少虚函数调用开销              │
│   ③ **更好缓存命中率**：列式存储 + 批处理 → L1/L2 缓存友好  │
│   → 性能提升 **5-10x**！                                      │
│
│ ★ Doris 向量化源码级（be/src/vec/）：                           │
│                                                               │
│ // Doris 2.0+ 全面向量化                                       │
│ // be/src/vec/vectorized_executor.cpp                          │
│                                                               │
│ // ★ Vectorized 表达式求值                                     │
│ ColumnPtr VectorizedComputeExpr(                              │
│     ObjectPool* pool,                                         │
│     ExprContext* ctx,                                         │
│     const std::vector<ColumnPtr>& children,  // 输入向量     │
│     const ColumnPtr& col_const                                │
│ ) {                                                           │
│     // ★ 一次处理 1024 行（Batch Size）                        │
│     // 而不是一行一行处理                                      │
│     ColumnPtr result = Column::create(*ctx->get_return_type(), │
│                                         children[0]->size());  │
│     for (int i = 0; i < children[0]->size(); ++i) {           │
│         // ★ 批量计算：一次算一批                              │
│         result->replace_if_null(i, 0);                        │
│         // SIMD 友好的循环                                     │
│     }                                                         │
│     return result;                                            │
│ }                                                             │
│
│ // ★ Doris SIMD 加速（X86 SSE/AVX）                           │
│ // be/src/util/simd.h                                         │
│ // AVX-512 一次处理 16 个 int64                               │
│ ALWAYS_INLINE __m512i simd_add_i64(__m512i a, __m512i b) {    │
│     return _mm512_add_epi64(a, b);  // AVX-512 指令           │
│ }                                                             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、BE 执行计划核心源码级

### 3.1 Fragment 执行

```cpp
// ★ Doris BE Fragment 执行流程（源码级）
//
// Doris FE 把 SQL 拆成多个 Fragment → 分发到 BE 执行
// 每个 Fragment 包含多个 Operator（Scan → Filter → Agg → Exchange）
//
// // be/src/exec/exec_node.cpp
//
// class ExecNode {
//     // ★ BE 上的执行节点基类
//     // 所有具体执行器继承它
//     virtual Status open(RuntimeState* state) = 0;
//     virtual Status get_next(RuntimeState* state, RowBatch* batch) = 0;
//     virtual Status close(RuntimeState* state) = 0;
// };
//
// // ★ Doris 2.0+ 全面用 VExprContext（向量化）
// class VExprContext {
//     // 向量化表达式上下文
//     // 一次处理一批 1024 行
//     Status evaluate(RuntimeState* state,
//                     const std::vector<ColumnPtr>& children_columns,
//                     ColumnPtr* result_column);
// };
//
// // ★ 向量化 Agg 执行器（源码级）
// // be/src/exec/vec_aggregate_node.cpp
//
// class VecAggregateNode : public ExecNode {
//     // ★ 核心：向量化聚合
//     Status get_next(RuntimeState* state, RowBatch* batch) override {
//         while (!_child_eos) {
//             // ① 从子节点拉一批数据（向量化）
//             ColumnPtr agg_result;
//             RETURN_IF_ERROR(_child->get_next(state, &_child_block));
//
//             // ② 向量化计算 Agg（HashAggregate）
//             //    一次处理一批 1024 行
//             _hash_agg->update(state, _child_block, &agg_result);
//         }
//
//         // ③ 输出最终聚合结果
//         return _hash_agg->output(state, batch);
//     }
// };
//
```

### 3.2 Pipeline 并行执行

```cpp
// ★ Doris Pipeline 并行（Doris 2.1+，面试高频！）
//
// 传统火山模型：open → get_next → close（所有算子串行调用）
// Pipeline 模型：多个算子在同一个 Pipeline 里并行执行
//
// // be/src/pipeline/pipeline_fragment_context.h
//
// class PipelineFragmentContext {
//     // ★ Pipeline = 一串可以并行执行的算子
//     std::vector<std::unique_ptr<Pipeline>> _pipelines;
//
//     // ★ 启动 Pipeline 并行执行
//     void start() {
//         for (auto& pipeline : _pipelines) {
//             // 每个 Pipeline 一个执行线程
//             for (int i = 0; i < pipeline->degree_of_parallelism(); ++i) {
//                 _executor_pool->submit([pipeline = pipeline.get()]() {
//                     pipeline->execute();
//                 });
//             }
//         }
//     }
// };
//
// // ★ Pipeline 执行（向量化批处理）
// class Pipeline {
//     void execute() {
//         // 每次处理一批（Batch Size）
//         while (!_done) {
//             // Pull 模型：每个算子拉取上游数据
//             BlockPtr block;
//             _source->pull(&block);      // Scan 拉数据
//             _filter->process(&block);    // Filter 过滤
//             _agg->process(&block);       // Agg 聚合
//             _sink->push(block);          // Exchange 发送
//         }
//     }
// };
//
// ★ Pipeline vs 火山模型（面试高频！）：
//   火山模型：所有子节点先 open → 串行 get_next → 最后 close
//   Pipeline：所有算子并行 + Push 模型（一个算子的输出是下一个的输入）
//   → Pipeline 延迟更低 + 资源利用更高
//
```

---

## 四、Doris 存储层 vs ClickHouse vs TiKV

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Doris BE 存储层源码级（面试对比高频！）                     │
│                                                               │
│  Doris BE 存储结构：                                          │
│  /data0/be/                                                   │
│  ├── data/                                                    │
│  │   ├── shard_0/                                             │
│  │   │   ├── rowset_0_1/                                      │
│  │   │   │   ├── _0.dat    ★ 列式数据文件                    │
│  │   │   │   ├── _0.idx   ★ 索引文件（Min/Max + BloomFilter） │
│  │   │   │   └── _0.json  ★ 元数据                           │
│  │   │   └── rowset_0_2/                                      │
│  │   └── shard_1/                                             │
│  └── tablet_meta/                                             │
│
│ ★ Doris vs ClickHouse 存储对比（面试高频！）：                  │
│   ┌──────────┬──────────────┬──────────────┐                 │
│   │            │ Doris         │ ClickHouse    │               │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 文件格式    │ Segment（内部）│ MergeTree     │               │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 压缩算法    │ ZSTD/LZ4     │ LZ4/LZ4HC      │               │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 索引        │ Min/Max + Bloom│ Min/Max + 跳数│               │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 数据模型    │ Duplicate/Unique│ MergeTree系列  │               │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 写入模型    │ 写时合并（Compaction）│ 写时合并    │               │
│   └──────────┴──────────────┴──────────────┘                 │
│
│ ★ Doris Unique Key（主键表）源码级：                           │
│   // be/src/olap/primary_key_index.cpp                        │
│   // 主键索引：内存 Hash 索引 + 持久化 RocksDB                │
│   // 写入时检查主键是否存在 → Upsert 语义                     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Doris 核心架构？| **FE（控制面）+ BE（数据面，向量化执行）** |
| 2 | 向量化执行核心？| **批处理 1024 行 + SIMD 加速 + 减少函数调用 → 5-10x 性能** |
| 3 | 向量化 vs 行式？| 行式：每次 1 行；向量化：每次 1024 行 + SIMD |
| 4 | SIMD 加速？| AVX-512 一次处理 16 个 int64；Doris be/src/util/simd.h |
| 5 | Pipeline vs 火山模型？| Pipeline：多算子并行 + Push；火山：串行 Pull；Pipeline 延迟更低 |
| 6 | Doris Fragment 执行？| FE 拆 SQL 成 Fragment → 分发 BE → ExecNode open/get_next/close |
| 7 | Doris vs ClickHouse vs TiDB？| Doris：MPP + 向量化；ClickHouse：列存 MergeTree；TiDB：分布式事务 + TiKV Coprocessor |
| 8 | Doris BE 存储格式？| Segment：.dat（列式数据）+ .idx（Min/Max + BloomFilter）+ .json（元数据）|
| 9 | 面试最高频考点排序？| **1. 向量化三大优势 2. Pipeline vs 火山 3. Doris vs ClickHouse 4. Fragment 执行** |
| 10 | 学习路径？| be/src/vec/ → VectorizedComputeExpr → simd.h → PipelineFragmentContext → VecAggregateNode |
| 11 | 面试常见追问？| 向量化为什么快？→ SIMD + 减少虚函数调用 + 缓存友好；Batch Size 多大？→ 默认 1024 行 |
| 12 | Doris 2.0 最大变化？| **全面向量化**（之前是行式执行）→ 性能飞跃 |
| 13 | Doris 2.1 最大变化？| **Pipeline 并行执行**（之前是火山模型串行）|
| 14 | Doris Unique Key？| 主键表 + Upsert 语义；内存 Hash 索引 + RocksDB 持久化 |
| 15 | Doris Compaction？| 后台合并多个小 Segment → 大 Segment → 减少查询扫描量 |
| 16 | Doris vs TiKV Coprocessor？| Doris：本地向量化执行；TiKV：下推到存储节点计算；都是计算下推 |
| 17 | 2024 新特性？| Doris 2.1+ **Pipeline 并行** + **Cache-First 缓存** + **Iceberg 表格式** |
| 18 | 面试高频场景？| 实时数仓 + 用户行为分析 + 多维报表 |
| 19 | Doris BE 内存管理？| Query 内存限制 + ExecNode 内存限制；OOM 时自动 Kill 大查询 |
| 20 | Doris SQL 兼容？| MySQL 协议 + 大部分 MySQL 语法；支持 Doris 特有的 PARTITION BY RANGE/HASH |
| 21 | 向量化 Agg 核心？| HashAggregate：Build HashTable → 批量 Update → 输出 |
| 22 | Doris BE vs Spark Executor？| Doris BE：本地 MPP 向量化；Spark：分布式 Stage 执行；Spark 更通用，Doris 更快 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：be/src/vec/ + be/src/exec/ + be/src/pipeline/  
**面试考点：22 题**  
**差异化点**：向量化 SIMD + Pipeline 并行 + Doris vs ClickHouse vs TiDB
