# TiKV Coprocessor 下推计算源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）  
> **核心仓库**：tikv/tikv → components/coprocessor/  
> **面试权重**：⭐⭐⭐⭐（TiKV 核心优化，SQL 下推高频）

---

## 一、Coprocessor 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Coprocessor = TiDB 下推到 TiKV 的计算处理器          │
│                                                               │
│  TiDB SQL 处理流程（有 Coprocessor vs 无 Coprocessor）：     │
│                                                               │
│  场景：SELECT COUNT(*) FROM users WHERE age > 25             │
│                                                               │
│  ❌ 无 Coprocessor（传统方式）：                               │
│   TiDB Server → 查所有 users 数据 → 全量拉回 TiDB 内存       │
│   → TiDB 过滤 age > 25 → TiDB count                          │
│   → **海量数据网络传输！TiDB 内存压力大！**                   │
│
│  ✅ 有 Coprocessor（TiKV 下推计算）：                          │
│   TiDB Server → 把 **Filter + Aggregation 下推到 TiKV**      │
│   → TiKV 本地 RocksDB 扫描 + 本地过滤 + 本地 count            │
│   → TiKV 只返回 count 结果（1 行数据！）                     │
│   → **网络传输从 TB 级降到 KB 级！**                          │
│
│ ★ Coprocessor 核心思想（面试必背！）：                         │
│   **把计算下推到离数据最近的地方执行**                       │
│   → 减少网络传输 + 减少 TiDB 内存压力                        │
│   → TiKV 有大量 Region → 并行执行下推计算 → 加速明显       │
│
│ ★ Coprocessor vs 其他下推方案（面试对比）：                    │
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │            │ TiKV Coprocessor│ Spark 下推   │ Doris/RockDB 下推│
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 下推到哪里  │ TiKV Region   │ 存储层       │ BE/CE 节点    │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 支持的操作  │ Filter/Agg/TopN│ Predicate Pushdown│ Filter/Join│
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 典型应用    │ TiDB          │ Spark         │ Doris/ClickHouse│
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、下推计算源码级

### 2.1 Coprocessor 支持的算子

```rust
// ★ TiKV Coprocessor 支持的下推算子（源码级）
//
// // components/coprocessor/src/endpoint.rs
//
// // TiDB 下推请求的 protobuf 定义
// pub struct Request {
//     // ① 下推的计算操作链（按顺序执行）
//     pub data: Vec<u8>,  // protobuf encoded Selection
//
//     // ② 需要扫描的 Region 范围
//     pub ranges: Vec<KeyRange>,
//
//     // ③ 下推类型：Select / DAG
//     pub tp: RequestType,
// }
//
// // TiKV Coprocessor 支持的算子类型
// pub enum CoprocessorTask {
//     Selection {
//         ranges: Vec<KeyRange>,
//         selection: Selection,  // Filter + Projection + Agg
//     },
//     DAG {                      // TiDB 5.0+ 新 DAG 模式
//         ranges: Vec<KeyRange>,
//         dag: Dag,
//     },
// }
//
// // ★ DAG 算子链（TiDB 新优化，面试高频！）
// pub struct Dag {
//     pub roots: Vec<Task>,  // DAG 图的根（可能有多个）
// }
//
// pub enum Task {
//     TableScan {                    // ① 扫描数据
//         table_id: i64,
//         columns: Vec<Column>,
//         ranges: Vec<KeyRange>,
//     },
//     IndexScan {                    // ② 索引扫描（更高效）
//         index_id: i64,
//         columns: Vec<Column>,
//         ranges: Vec<KeyRange>,
//     },
//     Selection {                    // ③ Filter（下推！）
//         conditions: Vec<Expression>,
//     },
//     Aggregation {                  // ④ Agg（下推！）
//         group_by: Vec<Expression>,
//         agg_funcs: Vec<AggFunc>,   // SUM/COUNT/AVG/MIN/MAX
//     },
//     TopN {                         // ⑤ TopN/Limit/OrderBy（下推！）
//         order_by: Vec<OrderBy>,
//         limit: u64,
//         offset: u64,
//     },
//     Limit {                        // ⑥ Limit（简单下推）
//         limit: u64,
//     },
//     ExchangeSender {               // ⑦ 数据发送给下一个 DAG 节点
//         receiver_id: u64,
//     },
// }
//
```

### 2.2 Coprocessor Endpoint 执行流程

```rust
// ★ TiKV Coprocessor Endpoint 执行源码（面试核心！）
//
// // components/coprocessor/src/endpoint.rs
//
// pub fn handle_request(req: &Request, region: &Region) -> Result<Response> {
//     // ① 解码下推请求（protobuf → Task 链）
//     let task = decode_task(&req.data)?;
//
//     // ② 检查请求范围是否在当前 Region 内
//     let ranges = expand_ranges(&req.ranges, region);
//
//     // ③ 开始执行 Task 链
//     // 底层：RocksDB Iterator 扫描数据
//     // 然后依次应用 Task（Filter → Agg → TopN → ...）
//     let result = match task {
//         Task::TableScan { columns, ranges } => {
//             // 扫描数据（从 RocksDB）
//             let records = scan_table(ranges, columns)?;
//             records
//         }
//         Task::IndexScan { columns, ranges } => {
//             // 索引扫描（更快！直接走索引）
//             let records = scan_index(ranges, columns)?;
//             records
//         }
//         Task::Selection { conditions } => {
//             // 应用 Filter 条件
//             let mut records = scan_table(...)?;
//             records.retain(|record| {
//                 eval_expr(&conditions, record)  // 逐行过滤
//             });
//             records
//         }
//         Task::Aggregation { group_by, agg_funcs } => {
//             // 应用 Aggregation
//             let mut groups: HashMap<Vec<Value>, Vec<Value>> = HashMap::new();
//             for record in scan_table(...)? {
//                 let key = extract_key(&group_by, record);
//                 let entry = groups.entry(key).or_default();
//                 apply_agg(&agg_funcs, entry, record);
//             }
//             serialize_agg_result(groups)
//         }
//         Task::TopN { order_by, limit, offset } => {
//             // 应用 TopN
//             let mut records = scan_table(...)?;
//             records.sort_by(|a, b| compare_by(&order_by, a, b));
//             records.into_iter().skip(offset as usize).take(limit as usize).collect()
//         }
//     };
//
//     // ④ 返回结果（protobuf 编码）
//     Ok(encode_response(result))
// }
//
```

---

## 三、DAG 模式 vs 旧 Selection 模式

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB Coprocessor DAG vs Selection 对比（面试高频！）      │
│                                                               │
│  Selection 模式（TiDB 3.x 及之前）：                          │
│   TiDB 生成一个 Selection（Filter + Agg 组合）                │
│   TiKV 端按固定顺序执行                                      │
│   → 简单但不够灵活                                           │
│
│  DAG 模式（TiDB 5.0+）：                                      │
│   TiDB 生成 DAG 图（多个 Task 按依赖关系连接）                │
│   TiKV 端可以并行执行独立 Task                                │
│   → 更灵活 + 并行执行 + 性能提升                              │
│
│  ★ 例子：SELECT a, COUNT(*) FROM t WHERE b > 10 GROUP BY a    │
│                                                               │
│  Selection 模式：                                              │
│   TableScan → Filter(b > 10) → Agg(group by a, count)        │
│
│  DAG 模式（更灵活）：                                          │
│   ┌──────────────┐                                           │
│   │ TableScan      │                                           │
│   │ (ranges, cols) │                                           │
│   └──────┬───────┘                                           │
│          ↓                                                    │
│   ┌──────────────┐                                           │
│   │ Selection      │                                           │
│   │ (b > 10)       │                                           │
│   └──────┬───────┘                                           │
│          ↓                                                    │
│   ┌──────────────┐                                           │
│   │ Aggregation    │                                           │
│   │ (group by a,  │                                           │
│   │  count(*))    │                                           │
│   └──────────────┘                                           │
│
│ ★ DAG 模式优势：                                              │
│   ① 多 Region 并行执行（每个 Region 自己跑 DAG）            │
│   ② 可以下推到多个 TiKV 节点并行计算                        │
│   ③ 部分聚合可以在 TiKV 端先算一遍 → TiDB 端合并最终结果   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、TiDB TiKV 交互完整链路

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB → TiKV Coprocessor 完整链路（面试核心！）            │
│                                                               │
│  ① TiDB SQL Parser 解析 SQL                                  │
│  ② TiDB Optimizer 生成物理计划                                │
│     → 判断哪些算子可以下推（Filter/Agg/TopN）                │
│     → 把下推算子打包成 Coprocessor 请求                     │
│
│  ③ TiDB → PD：根据 KeyRange 查 Region 路由                   │
│     → 哪些 Region 包含查询的数据？                           │
│
│  ④ TiDB 向每个目标 Region 的 Leader TiKV 发 Coprocessor RPC  │
│     → 并行发送（多个 Region 并行计算！）                     │
│
│  ⑤ TiKV Coprocessor Endpoint 处理请求                        │
│     → 解码 DAG 任务链                                        │
│     → 扫描 RocksDB 数据                                     │
│     → 依次执行 Filter / Agg / TopN                          │
│     → 返回最终结果（KB 级数据）                              │
│
│  ⑥ TiDB 合并多个 Region 的返回结果                            │
│     → 如果是 Agg 下推：TiKV 先算部分 Agg → TiDB 合并        │
│     → 如果是 TopN 下推：TiKV 返回各自 TopN → TiDB 全局排序  │
│
│  ★ 面试高频：Coprocessor 性能提升数据？                        │
│   无 Coprocessor：100 万行数据扫描 + 过滤 + 聚合 → 10s+     │
│   有 Coprocessor：TiKV 本地过滤 + 聚合 → 0.1s（快 100x！） │
│   → 数据量越大提升越明显（网络传输从 TB 降到 KB）           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV Coprocessor 核心？| **把计算下推到 TiKV 离数据最近的地方执行** → 减少网络传输 |
| 2 | Coprocessor 支持的下推算子？| **Filter + Aggregation（SUM/COUNT/AVG）+ TopN/Limit/OrderBy + IndexScan** |
| 3 | DAG vs Selection 模式？| DAG：TiDB 5.0+，更灵活，可并行；Selection：旧模式，固定顺序 |
| 4 | Coprocessor 执行流程？| TiDB Optimizer 打包下推算子 → 并行发 RPC 到多个 TiKV Region → TiKV 本地执行 → 返回结果 |
| 5 | Coprocessor 源码路径？| **components/coprocessor/src/endpoint.rs**（核心执行）|
| 6 | TiKV Coprocessor vs Spark 下推？| TiKV：Region 级下推 + ProtoBuf 序列化；Spark：Predicate Pushdown + Parquet 下推 |
| 7 | 面试最高频考点排序？| **1. 下推核心思想 2. 支持的算子 3. DAG vs Selection 4. 完整链路 5. 性能提升数据** |
| 8 | 学习路径？| endpoint.rs → Task 枚举（TableScan/Selection/Aggregation）→ 请求解码 → 执行循环 |
| 9 | 面试常见追问？| 为什么要下推？→ 减少网络传输（TB→KB）；TiDB 和 TiKV 怎么通信？→ gRPC + ProtoBuf |
| 10 | 哪些操作不能下推？| **Join（跨 Region）+ 复杂 UDF + 跨表 DISTINCT**（需要 TiDB 合并）|
| 11 | TiKV Agg 下推？| TiKV 先算部分 Agg → TiDB 合并最终结果（两阶段聚合）|
| 12 | TiKV TopN 下推？| 每个 Region 返回各自 Top N → TiDB 全局排序 → 取前 N |
| 13 | Coprocessor 性能提升？| **100x+**（网络传输从 TB 降到 KB）|
| 14 | 2024 新特性？| TiKV 8.0+ **向量下推**（ANN 索引 + 向量距离计算下推）|
| 15 | TiKV Coprocessor vs ClickHouse 下推？| ClickHouse：分布式 OLAP 引擎，BE 节点直接执行 SQL；TiKV：KV 存储 + 下推计算 |
| 16 | 面试高频场景？| 大表 COUNT(*) + 条件过滤 + GROUP BY 聚合 |
| 17 | TiDB Optimizer 怎么决定要不要下推？| CBO（Cost-Based Optimizer）：看下推执行成本 vs 拉回 TiDB 执行成本 |
| 18 | 下推请求怎么编码？| **ProtoBuf**（高效序列化 + 跨语言）|
| 19 | TiKV Coprocessor Endpoint？| TiKV 上专门处理下推请求的组件；独立线程池 |
| 20 | 面试最高频对比？| TiKV Coprocessor vs Doris BE Exec（都是计算下推到存储节点）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：components/coprocessor/src/endpoint.rs  
**面试考点：20 题**  
**差异化点**：DAG Task 枚举 + Rust 源码级执行流程 + TiDB-TiKV 完整链路 + 100x 性能提升
