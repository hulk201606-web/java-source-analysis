# TiKV Coprocessor 下推计算源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）+ TiDB v8.0.x（Go）  
> **核心位置**：tikv/src/coprocessor + tidb/kv/copr  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 性能核心，下推是 HTAP 关键）

---

## 一、TiKV Coprocessor 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Coprocessor = TiDB 把计算下推到 TiKV 端执行          │
│                                                               │
│  为什么需要 Coprocessor（面试必背）：                         │
│                                                               │
│   无 Coprocessor（传统模式）：                                │
│   TiDB 发 SELECT * FROM t WHERE dt='2024-01-01'              │
│     → TiKV 把该表全部 block 拉到 TiDB                     │
│     → TiDB 本地过滤 → 只剩少量数据                          │
│     → 大量网络 IO 浪费（TiKV→TiDB 网络瓶颈）                │
│                                                               │
│   有 Coprocessor：                                            │
│   TiDB 发 DAGRequest protobuf（SELECT 表达式树）              │
│     → TiKV 本地 RocksDB 扫 + 本地 filter                    │
│     → 只返回过滤后的少量结果                                  │
│     → 网络 IO 节省 90%+，TiDB 压力大减                     │
│                                                               │
│  ★ 可下推的操作（面试高频）：                                  │
│   ✅ Filter（WHERE 条件）                                    │
│   ✅ Aggregate（GROUP BY + SUM/COUNT/AVG/MIN/MAX）          │
│   ✅ TopN（ORDER BY + LIMIT N）                              │
│   ✅ Limit / Offset                                           │
│   ✅ Projection（SELECT 字段裁剪）                           │
│   ✅ DISTINCT（去重）                                         │
│   ✅ JOIN（部分支持，TiDB 8.0+ 增强）                        │
│   ✅ COUNT(*) / COUNT(1) 自动下推到 TiKV                    │
│   ❌ 无法下推：复杂 UDF、跨 Region 全局排序、某些窗口函数    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiDB→TiKV 下推请求：DAGRequest

### 2.1 TiDB 端构造 DAG

```go
// ★ TiDB 端 Coprocessor 下推核心（源码级）
//
// tidb/kv/copr/coprocessor.go
// tidb/executor/builder.go（构建物理执行计划时决定下推）
//
// // 1. TiDB 解析 + 优化器阶段：
// //    把 SELECT 物理计划转换成 DAGRequest protobuf
// //    // tidb/executor/builder.go
// func buildCopDAGRequest(ctx, physicalPlan) *cop.DAGRequest {
//     dag := &cop.DAGRequest{}
//
//     // 从 PhysicalTableScan 开始：下推 Filter/Aggregate/TopN/Limit
//     // 下推条件：操作符下面直接/间接是 TableScan
//     //   Project → Filter → TableScan
//     //     → DAGRequest = Filter + TableScan
//     //   Aggregate → Filter → TableScan
//     //     → DAGRequest = Aggregate + Filter + TableScan
//
//     // 2. 下推 TableScan 节点
//     dag.Root = buildTableScan()
//     dag.Root.SetChildren(buildFilter(), buildAggregate()...)
//     //    TiDB 物理计划树 → DAG（单根多子树）
//
//     // 3. 下推 Aggregate / TopN / Limit
//     dag.Aggregation = buildAggregation()
//     dag.TopN = buildTopN()     // 或 dag.Limit
//
//     // 4. ★ 编码表达式为 Expression protobuf
//     //    WHERE dt='2024-01-01' AND status='paid'
//     //    → cop.Expression(Tree, EvalType.String, String.Data='2024-01-01')
//     dag.Where = buildWhereExpr()
//
//     // 5. 指定返回哪些列（Projection 下推）
//     dag.OffsetsInRow = []int64{0, 1, 3}  // 只返回 col0/col1/col3
//
//     // 6. 构造 Range 列表（按 TiKV Region 切分）
//     dag.Ranges = splitKeyRanges(tableID, tableSchema)
//     //    一个大表按主键/索引切成多个 Range
//     //    每个 Range 发一个独立 Coprocessor RPC 给对应 TiKV
//     return dag
// }
//
// // 7. 发送请求：
// //    tidb/store/tikv/client.go
// //    SendRequest(ctx, dag, storeAddr)
// //    → TiKV Endpoint.Coprocessor(dagReq)  // gRPC RPC
//
```

### 2.2 DAGRequest Proto 结构

```protobuf
// ★ DAGRequest protobuf 核心定义（源码级）
//
// coprocessor.proto
//
// message DAGRequest {
//   Root           dagpb.Task           = 1;  // 根任务（TableScan 或 IndexScan）
//   Aggregation    dagpb.Aggregation     = 2;  // 下推的聚合
//   TopN           dagpb.TopN            = 3;  // 下推的 TopN
//   Limit          dagpb.Limit           = 4;  // 下推的 Limit
//   Ranges         KeyRanges             = 5;  // 扫描的 Key 范围
//   StartTs        uint64                = 6;  // MVCC 读时间戳
//   IsolationLevel uint32                = 7;  // 隔离级别
//   // ...
// }
//
// message Task {
//   TaskType   type      = 1;  // TableScan / IndexScan / Selection / Aggregation / TopN / Limit
//   bytes      data      = 2;  // 具体 Task 参数（序列化的 TableScan 等）
//   repeated Task children = 3; // 子任务（可以嵌套）
// }
//
// // ★ Expression（TiKV 端表达式树，面试核心）
// message Expression {
//   ExpressionType  tp       = 1;  // 常量 / 列引用 / BinOp / Function
//   // ...
//   oneof expr {
//     StringData str_data    = 10;  // 常量字符串
//     IntData    int_data    = 11;  // 常量整数
//     ColumnRef   column      = 12;  // 列引用（col_idx）
//     BinExpr     bin_expr   = 13;  // 二元运算（+ - * / AND OR = > <）
//     FunctionCall func      = 14;  // 内置函数（date_format/like/...）
//   }
//   repeated Expression children = 20; // 子表达式
// }
//
// ★ 一个完整下推例子：
// SELECT COUNT(*) FROM orders WHERE dt='2024-01-01' AND status='paid';
//
// DAGRequest:
//   Root = TableScan(table=orders)
//   Aggregation = Aggregation{
//       group_by: [],                  // 无 group by
//       agg_funcs: [FunctionCall{name="COUNT", args=[]}]  // COUNT(*)
//   }
//   Where = BinExpr{
//       op: AND,
//       left:  BinExpr{op: EQ, left: ColumnRef{idx: dt_col},    right: StringData{'2024-01-01'}},
//       right: BinExpr{op: EQ, left: ColumnRef{idx: status_col}, right: StringData{'paid'}},
//   }
//   Ranges = [KeyRange{start: orders_tableID, end: orders_tableID + 1}]
//
```

---

## 三、TiKV 端 Coprocessor 执行源码级

### 3.1 Coprocessor Endpoint

```rust
// ★ TiKV Coprocessor 执行全链路（源码级，面试核心）
//
// tikv/src/coprocessor/coprocessor.rs
// tikv/src/storage/txn/coprocessor.rs
//
// // TiKV gRPC Endpoint 接收请求：
// async fn handle_coprocessor(
//     &self,
//     req: DAGRequest,
//     deadline: Timeout,
// ) -> Result<CoprocessorResponse> {
//
//     // 1. 权限/隔离级别校验
//     let isolation_level = parse_isolation_level(req.isolation_level);
//
//     // 2. 构造 MVCC Snapshot（按 StartTs 读快照）
//     //    // tikv/src/storage/mvcc/snapshot.rs
//     let snap = storage.get_snapshot(req.start_ts)?;
//     //    这一步保证 Coprocessor 读的是 StartTs 时刻的 MVCC 数据
//     //    不会读到 StartTs 之后的新写入
//
//     // 3. ★ 解析 DAGRequest → Task 树
//     //    DAGRequest 是 protobuf，需要解析成 TiKV 内部 Task 结构
//     let dag = DAGRequest::from_pb(req)?;
//     //    解析 Root Task（TableScan / IndexScan）
//     //    解析子 Task（Selection / Aggregation / TopN / Limit）
//
//     // 4. ★ 构建 Task 执行链（Pipeline 模式）
//     //    每个 Task 是一个数据转换阶段
//     //    数据从上向下流动（Selection → Aggregation → TopN → 输出）
//     let executors = build_executors(dag)?;
//     //    // tikv/src/coprocessor/dag/
//     //    TableScanExec → SelectionExec → AggregationExec → TopNExec → LimitExec
//     //    // 数据驱动：上游 next() 被下游调用
//
//     // 5. 执行 Pipeline
//     let mut executor = executors.root();
//     let mut results: Vec<Row> = Vec::new();
//     while let Some(row) = executor.next()? {
//         results.push(row);
//     }
//
//     // 6. ★ 编码返回结果为 protobuf
//     let resp = encode_to_pb(results)?;
//     Ok(resp)
// }
//
```

### 3.2 各 Task Executor 源码级

```rust
// ★ TiKV Coprocessor Executor 执行链（源码级）
//
// tikv/src/coprocessor/dag/table_scan.rs      → TableScanExec
// tikv/src/coprocessor/dag/selection.rs       → SelectionExec（Filter）
// tikv/src/coprocessor/dag/aggregation.rs     → AggregationExec
// tikv/src/coprocessor/dag/top_n.rs           → TopNExec
// tikv/src/coprocessor/dag/limit.rs           → LimitExec
//
// // Executor trait（所有 Coprocessor Task 都实现）：
// pub trait Executor {
//     fn next(&mut self) -> Result<Option<Row>>;
//     // 每次被调用返回下一行数据，None 表示结束
// }
//
// // TableScanExec（最底层，从 RocksDB 扫数据）：
// pub struct TableScanExec {
//     snap: MvccSnapshot,
//     range: KeyRange,             // 扫哪个 key range
//     columns: Vec<ColumnInfo>,    // 要返回的列（Projection 下推）
//     reader: DbtxnMvccReader,     // MVCC 读器
//     // ...
// }
// impl Executor for TableScanExec {
//     fn next(&mut self) -> Result<Option<Row>> {
//         // 从 RocksDB CF_DEFAULT 按 range 扫 MVCC 版本
//         // 跳过 commit_ts > start_ts 的版本
//         // 解 key-value 成 Row（列值对齐）
//         self.reader.next_row(self.range, self.columns)
//     }
// }
//
// // SelectionExec（Filter 下推核心）：
// pub struct SelectionExec {
//     inner: Box<dyn Executor>,   // 上游（TableScan）
//     conditions: Expression,     // WHERE 条件表达式树
// }
// impl Executor for SelectionExec {
//     fn next(&mut self) -> Result<Option<Row>> {
//         // 从上游拿 row
//         while let Some(row) = self.inner.next()? {
//             // 计算 WHERE 表达式
//             if self.conditions.eval_bool(&row)? {
//                 return Ok(Some(row));  // 满足条件才返回
//             }
//             // 不满足 → 丢弃，继续下一行
//         }
//         Ok(None)  // 全部扫完
//     }
// }
//
// // AggregationExec（聚合下推核心）：
// pub struct AggregationExec {
//     inner: Box<dyn Executor>,
//     group_by_cols: Vec<usize>,   // GROUP BY 的列 index
//     agg_funcs: Vec<AggFunc>,     // SUM/COUNT/AVG 等聚合函数
//     // 内存中维护 Map<group_key, PartialAggValue>
//     groups: HashMap<Row, Vec<AggState>>,
// }
// impl Executor for AggregationExec {
//     fn next(&mut self) -> Result<Option<Row>> {
//         // 第一次调用：拉完所有 Row 做聚合
//         while let Some(row) = self.inner.next()? {
//             let key = row.pick_cols(&self.group_by_cols);
//             let entry = self.groups.entry(key).or_insert_with(|| new_agg_states());
//             for (i, state) in entry.iter_mut().enumerate() {
//                 state.update(row.cols[i]);  // 增量更新聚合状态
//             }
//         }
//         // 之后：逐个返回聚合结果
//         if let Some((key, states)) = self.groups.iter().next() {
//             Ok(Some(key.merge_agg_states(states)))
//         } else {
//             Ok(None)
//         }
//     }
// }
//
// // ★ TopNExec / LimitExec（类似逻辑）：
// // TopNExec：维护小顶堆，只保留最大/最小的 N 个
// // LimitExec：计数到阈值后 stop
//
```

### 3.3 表达式求值引擎

```rust
// ★ TiKV Coprocessor 表达式求值引擎（源码级）
//
// tikv/src/coprocessor/dag/expr.rs
// tikv/src/coprocessor/dag/rpn_expr.rs
//
// // TiKV 端表达式求值：
// //   TiDB 把 WHERE/SELECT 表达式编码成 RPN（逆波兰表示）
// //   或者递归的 Expression Tree
// //   TiKV 端按 RPN 栈方式求值（高效，不需要递归调用）
//
// pub struct RpnExpression {
//     rpn_expr: Vec<RpnExpressionNode>,  // RPN 栈
//     // node 类型：ColumnRef / Const / BinOp / Function
// }
//
// impl RpnExpression {
//     pub fn eval(&self, row: &Row) -> Result<ScalarValue> {
//         let mut stack: Vec<ScalarValue> = Vec::new();
//         for node in &self.rpn_expr {
//             match node {
//                 RpnExpressionNode::ColumnRef(idx) => {
//                     stack.push(row.cols[*idx].clone());
//                 }
//                 RpnExpressionNode::Const(val) => {
//                     stack.push(val.clone());
//                 }
//                 RpnExpressionNode::BinOp(op) => {
//                     let b = stack.pop().unwrap();
//                     let a = stack.pop().unwrap();
//                     let result = apply_bin_op(*op, a, b)?;
//                     stack.push(result);
//                 }
//                 RpnExpressionNode::FunctionCall(name) => {
//                     let args = pop_args(&mut stack, name);
//                     let result = call_builtin_func(name, args)?;
//                     stack.push(result);
//                 }
//             }
//         }
//         Ok(stack.pop().unwrap())
//     }
// }
//
// ★ TiKV 内置函数清单（下推可用）：
//   算术：+, -, *, /, %, CAST
//   比较：=, !=, >, <, >=, <=, IN
//   逻辑：AND, OR, NOT
//   字符串：LIKE, SUBSTRING, LENGTH, CONCAT
//   日期：DATE_FORMAT, YEAR, MONTH, DAY
//   聚合：COUNT, SUM, AVG, MIN, MAX, COUNT(DISTINCT) 有限支持
//   聚合增强：STDDEV, VARIANCE, GROUP_CONCAT（TiDB 8.0+）
//
// 无法下推的函数：
//   UDF（自定义函数，TiKV 端不知道实现）
//   RAND()（非确定性，TiKV 端与 TiDB 端可能不同值）
//   某些窗口函数（需全局排序）
//
```

---

## 四、Range 切分 + 并发 Coprocessor

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Coprocessor 如何利用 TiKV Region 并发（面试核心）         │
│                                                               │
│  1. TiDB 优化器阶段：                                         │
│     物理计划 → 按 Region 边界切成多个 KeyRange              │
│     // tidb/executor/builder.go → copr.SplitRanges()        │
│     大表 orders（tableID=1000）→ 按主键切成：              │
│       Range 1: [1000, 2000)  → 对应 TiKV A                 │
│       Range 2: [2000, 3000)  → 对应 TiKV B                 │
│       Range 3: [3000, +inf)  → 对应 TiKV C                 │
│                                                               │
│  2. TiDB 并发发送多个 Coprocessor RPC：                      │
│     // tidb/store/tikv/client.go → BatchSend()              │
│     每个 DAGRequest 带一个 Range → 发给对应 TiKV            │
│     N 个 Range → N 个并行 gRPC 调用                        │
│     （类似 Spark DAG Stage 切分后并行发 Task）              │
│                                                               │
│  3. TiDB 端合并多个 Coprocessor 结果：                       │
│     每个 TiKV 返回部分结果 → TiDB 合并                      │
│     如果有 Global TopN → TiDB 做最后合并排序                │
│     如果有 JOIN → TiDB 把两侧 Coprocessor 结果在本地 Join   │
│                                                               │
│  ★ 性能关键：                                                  │
│   - Range 切分得越碎 → 并行度越高                          │
│   - 但 RPC 开销增加 → 平衡（一般不超过 Region 数量）       │
│   - Count(*) 最理想：每个 TiKV 只返回一个数字               │
│                                                               │
│  ★ MVCC 一致性：                                               │
│   所有 DAGRequest 带相同的 StartTs                         │
│   每个 TiKV 在自己的 Snapshot 上读 → 保证可重复读           │
│   跨 Region 读也不会看到不同时间点的数据                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、Coprocessor Scheduler + 超时

```rust
// ★ TiKV Coprocessor 调度与超时处理（源码级）
//
// tikv/src/coprocessor/scheduler.rs
// tikv/src/server/gRPC.rs → coprocessor handler
//
// // Coprocessor 请求进入 TiKV 后的调度：
// fn handle_coprocessor(req, deadline) -> Result<Response> {
//     // ① 超时检查
//     let timeout = deadline.remaining();
//     if timeout < COP_LATENCY_BARRIER {
//         // 剩余时间太少，直接让 TiDB 重试（或降级）
//         return Err(Error::Timeout);
//     }
//
//     // ② 排队（Coprocessor 是重操作：读 RocksDB + 计算）
//     //    TiKV 有独立的 Coprocessor 线程池
//     //    避免重计算阻塞正常读写
//     scheduler.schedule(Box::pin(async move {
//         let _ = coprocessor_exec(req, deadline).await;
//     }))?;
//
//     // ③ 有界队列：超过阈值拒绝
//     //    防止大查询把 TiKV 拖死
// }
//
// // ★ Coprocessor 执行中的超时打断：
// // 每个 Executor.next() 检查 deadline
// // 如果超时 → 返回 Err(Error::Timeout) → TiDB 重试
// // TiDB 有 Coprocessor Retry 机制：
// //   收到 Timeout → 把 Range 切更小重试（或用小 Region 并发）
// //   或者直接让 TiDB 端降级执行（拉全量到 TiDB 过滤）
//
// ★ Coprocessor vs 正常读写（面试高频）：
// ┌──────────┬──────────────────┬──────────────────┐
// │            │ 正常读写（Percolator）│ Coprocessor（OLAP）│
// ├──────────┼──────────────────┼──────────────────┤
// │ 操作        │ 点查/点写          │ 范围扫描+计算     │
// │ 隔离级别   │ RC/RR             │ RC（固定 StartTs 快照）│
// │ 锁          │ Percolator 行锁    │ ❌ 不加锁（读快照）│
// │ 耗时        │ ms 级              │ ms~s 级（取决于数据量）│
// │ 调度队列   │ 正常读队列          │ ★ 独立 Coprocessor 队列│
// │ 优先级     │ 高                 │ ★ 低（OLAP 不影响 OLTP）│
// │ 超时策略   │ 短超时（重试快）    │ 长超时（可切小重试）│
// └──────────┴──────────────────┴──────────────────┘
//
```

---

## 六、差异化对比：TiKV Coprocessor vs MySQL Pushdown vs Spark Exchange

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比表：三种下推计算架构                          │
│  ┌──────────┬───────────────────┬──────────────┬──────────┐│
│  │ 维度      │ TiKV Coprocessor    │ MySQL Pushdown│ Spark EX  ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 下推目标  │ TiKV Region 节点    │ MySQL Storage │ Executor  ││
│  │           │ 分布式 RPC          │ Engine 内部   │ 本地计算  ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 下推粒度  │ ★ Filter/Aggr/TopN │ Filter（ICP/ │ 算子级    ││
│  │           │ /Join（部分）       │ MRR）          │ Stage切分 ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 表达式传输│ ★ RPN/Tree Proto   │ 隐式（SQL 解析│ RDD/DAG  ││
│  │           │                    │ 在引擎层）    │ 序列化   ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 并发模型  │ ★ N Range → N RPC  │ 单节点串行    │ ★ Task并行││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 隔离级别  │ ★ 固定 StartTs     │ 会话隔离级别  │ 无隔离    ││
│  │           │ 快照读不加锁        │ 加锁          │          ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 锁机制    │ ❌ 不加锁（快照读） │ ✅ InnoDB 锁  │ ❌ 无锁   ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 性能瓶颈  │ RPC + TiKV 计算    │ Storage Engine│ 网络 Shuffle││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 适用场景  │ HTAP / 实时 OLAP   │ OLTP 索引下推 │ 批量 ETL  ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 面试高频  │ ★★★★★            │ ★★★          │ ★★★★    ││
│  └──────────┴───────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   HTAP 场景 → **TiKV Coprocessor**（TiDB 原生、分布式并发） │
│   OLTP 加速 → **MySQL ICP/MRR**（索引下推、减少回表）       │
│   批量大数据 → **Spark Exchange + Coprocessor**（完整 DAG ）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV Coprocessor 是什么？| TiDB 把 SELECT 表达式下推到 TiKV 端执行，**返回少量结果**而非拉全量数据 |
| 2 | 为什么需要 Coprocessor？| 无 Coprocessor → TiKV 拉全表到 TiDB 过滤 → 网络瓶颈；有 Coprocessor → TiKV 本地过滤 → 省 90%+ 网络 IO |
| 3 | ★ DAGRequest 是什么？| TiDB 发给 TiKV 的 protobuf 请求：**表达式树 + Range + StartTs** |
| 4 | DAGRequest 能下推哪些操作？| **Filter + Aggregate + TopN/Limit + Projection + DISTINCT + 部分 JOIN + COUNT(*)** |
| 5 | 不能下推什么？| UDF、非确定性函数（RAND）、跨 Region 全局排序、某些窗口函数 |
| 6 | TiKV Coprocessor 执行全链路？| gRPC Endpoint → MVCC Snapshot → 解析 DAG → 构建 Executor Pipeline → 执行 → 返回 protobuf |
| 7 | Executor Pipeline 模式？| TableScanExec → SelectionExec → AggregationExec → TopNExec → LimitExec；**next() 驱动** |
| 8 | TiKV 端表达式求值？| **RPN（逆波兰表示）+ 栈求值**；TiDB 把 WHERE 编码成 RPN，TiKV 按栈计算 |
| 9 | TiKV 下推支持的内置函数？| 算术/比较/逻辑 + 字符串（LIKE/SUBSTRING）+ 日期（DATE_FORMAT）+ 聚合（COUNT/SUM/AVG）|
| 10 | Coprocessor 并发模型？| TiDB 按 Region 切 Range → **N 个 RPC 并发发 N 个 TiKV**；每个 TiKV 只扫自己的 Range |
| 11 | Coprocessor 隔离级别？| **固定 StartTs 快照读**，不加 Percolator 锁；保证跨 Region 可重复读一致性 |
| 12 | MVCC Snapshot 怎么保证？| 所有 DAGRequest 带相同 StartTs → TiKV 各自 get_snapshot(ts) → 读快照 |
| 13 | TiKV Coprocessor 调度？| 独立线程池 + 有界队列；**优先级低于正常读写**；超时自动拒绝 |
| 14 | Coprocessor 超时怎么处理？| TiKV Deadline 检查 → 超时返回 Err；TiDB 可以重试（切小 Range / 降级到 TiDB 端执行）|
| 15 | COUNT(*) 下推原理？| TiDB 把 COUNT(*) 编码成 Aggregation（AggFunc=COUNT）→ TiKV 执行 → 每个 TiKV 返回一个数字 → TiDB 求和 |
| 16 | ★ Coprocessor vs 正常读写差异？| 见第五节对比表（7 维度：操作类型、锁、优先级、队列、超时）|
| 17 | TiKV Coprocessor vs MySQL Pushdown vs Spark Exchange？| 见第六节对比表（8 维度）|
| 18 | 面试最高频考点排序？| **1. DAGRequest protobuf + Range 切分 2. Executor Pipeline 执行链 3. RPN 表达式求值 4. MVCC Snapshot 隔离 5. 三架构对比** |
| 19 | 学习路径？| DAGRequest proto → coprocessor handler → Executor trait → RPN expression → Range 切分 |
| 20 | 面试常见追问？| Coprocessor 会加 Percolator 锁吗？→ **不会**，是读快照，不加锁，不影响 OLTP |
| 21 | 面试常见追问？| Coprocessor 超时会拖垮 TiKV 吗？→ 不会，独立 Coprocessor 线程池 + 有界队列 + 超时打断 |
| 22 | 面试常见追问？| JOIN 能下推吗？→ 部分能，TiDB 8.0+ 对某些 Semi-Join 有优化；复杂 Join 还是 TiDB 本地做 |
| 23 | 面试陷阱？| 说 Coprocessor 像 Spark Task 一样 Shuffle → ❌ Coprocessor 返回的是最终部分结果，TiDB 合并，没有 Shuffle |
| 24 | 面试陷阱？| 说 Coprocessor 会修改数据 → ❌ 是只读（Read-Only），不写任何 KV |
| 25 | 典型 HTAP 架构？| 业务写入 TiDB → TiKV 同步 → 实时报表用 Coprocessor 下推 → 秒级返回聚合结果 |
| 26 | Coprocessor 和 Flash MPP 区别？| Coprocessor 是单 Region 内计算；Flash MPP 是多 TiFlash 节点分布式 MP 执行引擎（列存）|
| 27 | RPN 表达式 vs 树表达式？| RPN 用栈求值，高效无递归；树表达式结构清晰；TiKV 内部主要用 RPN（性能最优）|

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：tidb/kv/copr/coprocessor.go + tikv/src/coprocessor/coprocessor.rs + tikv/src/coprocessor/dag/* + coprocessor.proto  
**面试考点：27 题**  
**差异化点**：DAGRequest protobuf 协议、Executor Pipeline 执行链、RPN 表达式求值、MVCC Snapshot 隔离、TiKV Coprocessor vs MySQL ICP vs Spark Exchange 三架构对比
