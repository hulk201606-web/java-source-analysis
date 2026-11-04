# MySQL Hash Join + BKA vs BNL Join 源码级深度解析

> **源码基线**：MySQL 8.0.x（C++）  
> **核心位置**：sql/sql_join_buffer.cc + sql/sql_hash_join.cc + sql/range_optimizer.cc  
> **面试权重**：⭐⭐⭐⭐⭐（JOIN 优化器高频选择逻辑）

---

## 一、四种 JOIN 算法全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 四种 JOIN 算法（面试必背选择逻辑）                  │
│                                                               │
│  ┌──────────────────┬──────────────────┬───────────────┐    │
│  │ 算法               │ 核心机制          │ 适用条件       │    │
│  ├──────────────────┼──────────────────┼───────────────┤    │
│  │ ★ Hash Join       │ Build+Probe 两阶段 │ 等值 JOIN +  │    │
│  │ (8.0.18+ 新增)    │ Build 表全内存 Hash │ 内存够大 Build │    │
│  │                    │ Probe 表逐行查   │ 表 + 无索引   │    │
│  ├──────────────────┼──────────────────┼───────────────┤    │
│  │ ★ BKA (Batched   │ 外表批量 JoinKey  │ 内表有可用索引│    │
│  │  Key Access)      │ → 内表批量 MRR  │ 等值 JOIN     │    │
│  │                    │ 查             │               │    │
│  ├──────────────────┼──────────────────┼───────────────┤    │
│  │ ★ BNL (Block     │ 外表批量缓存     │ 内表无索引    │    │
│  │  Nested Loop)     │ → 内表全表扫   │ + 内存够缓存  │    │
│  │                    │ 逐块遍历       │ 外表          │    │
│  ├──────────────────┼──────────────────┼───────────────┤    │
│  │ Nested Loop Join  │ 外表每行 → 内表逐 │ 最简单的默认   │    │
│  │                    │ 行查（无缓冲）  │ 退化情况       │    │
│  └──────────────────┴──────────────────┴───────────────┘    │
│                                                               │
│  ★ 优化器选择优先级（Cost 最小）：                           │
│   有可用索引 → BKA（批量索引查）> Hash Join > BNL > NLJ      │
│   无可用索引 → Hash Join > BNL（全表扫一次）> NLJ（全表扫 n 次）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Hash Join 源码级

### 2.1 Build + Probe 两阶段

```cpp
// ★ MySQL Hash Join 源码级（8.0.18+，面试核心）
//
// sql/sql_hash_join.cc
//
// // Hash Join 两阶段：
// // 阶段 1：BUILD（构建 Hash Table）
// //   选较小的表作为 Build 表（内存能装下）
// //   遍历 Build 表所有行 → 按 Join Key 存入 Hash Table
// //   Hash Table = std::unordered_map<JoinKey, vector<Row>>
// //
// // 阶段 2：PROBE（探测匹配）
// //   遍历 Probe 表（外表）每一行
// //   用 Join Key 查 Hash Table（O(1)）
// //   找到匹配 → 产生 JOIN 结果
// //
// // Hash Join 完整执行流程：
// void HashJoin::execute() {
//     // ========== Build Phase ==========
//     // 选 Build 表：Cost 估算更小的表
//     // 一般选行数少 / 列少的表（内存 Hash 占用更小）
//     JOIN *build_table = chooseBuildTable();
//     JOIN *probe_table = join_tables[0] == build_table ? join_tables[1] : join_tables[0];
//
//     // 构建 Hash Table
//     hash_table.init(build_table);
//     for (auto row = build_table->read(); !row.isEof(); row = build_table->nextRow()) {
//         JoinKey key = extractJoinKey(row);
//         hash_table.insert(key, row);
//     }
//
//     // ========== Probe Phase ==========
//     for (auto probe_row = probe_table->read(); !probe_row.isEof(); probe_row = probe_table->nextRow()) {
//         JoinKey key = extractJoinKey(probe_row);
//         auto range = hash_table.lookup(key);  // O(1) 查找
//
//         for (auto build_row : range) {
//             JOIN_ROW joined = merge(probe_row, build_row);
//             output(joined);
//         }
//     }
//
//     // Hash Table 释放
//     hash_table.clear();
// }
//
// ★ Hash Join 的核心优势：
//   时间复杂度：O(build_rows + probe_rows)  ← **接近线性！**
//   Nested Loop：O(build_rows × probe_rows)  ← **二次复杂度**
//   Build 表越大 → Hash Join 优势越明显
//
```

### 2.2 Spill to Disk + Hash Join 参数

```cpp
// ★ Hash Join Spill to Disk（源码级，面试高频）
//
// // Build 表太大装不下内存怎么办？
// // 8.0.18+ 支持动态降级到 Hash Join with spill
// //
// // 参数控制（optimizer_switch）：
// //   hash_join=on（默认开启）
// //   block_nested_loop=on
// //   batched_key_access=on
// //
// // 内存限制（join_buffer_size）：
// //   每个 JOIN 操作的 Hash Table 内存上限
// //   默认 256KB；生产建议 1-4MB
// //   超过 → 触发 Spill to Disk
// //
// // Spill 策略：
// //   1. Build 表按 Hash Key 分区（N 个 Bucket）
// //   2. 每个 Bucket 独立 Build Hash Table（内存够装）
// //   3. 写临时文件：tmpdir/hash_join_xxxx.tmp
// //   4. 每个 Bucket 单独 Probe（多次 Pass）
// //   5. 临时文件自动清理（查询结束后）
// //
// // ★ Hash Join 内存监控：
// //   EXPLAIN ANALYZE 能看到：
// //   → "Using Hash Join"
// //   → "hash_join_memory: 1.2MB"
// //   → "hash_join_buckets: 64"
// //   → "hash_join_spill: YES/NO"
//
// ★ MySQL 8.0 Adaptive Hash Join：
//   8.0.20+ 新增：运行时自适应 BNL → Hash Join
//   // 如果 Join Buffer 里能装下 Build 表 → 用 Hash Join
//   // 否则降级 BNL（全表扫一次）
//   // 优化器在 Plan 阶段先估一个，执行时再判断
//
```

---

## 三、BKA vs BNL Join 源码级

### 3.1 BKA（Batched Key Access）

```cpp
// ★ BKA Join 源码级（180 文档已覆盖，这里聚焦和 Hash Join 的关系）
//
// sql/sql_join_buffer.cc → join_read_key()
//
// // BKA 何时比 Hash Join 好？
// //   Hash Join 要把 Build 表全部放内存
// //   BKA 用内表索引 + MRR 批量查
// //   如果内表有索引 → BKA 可能更快（不用全表扫 Build）
// //
// // BKA 与 Hash Join 选择逻辑：
// //   ① 内表无索引 → Hash Join > BNL > NLJ
// //   ② 内表有索引且外表不小 → BKA > Hash Join > BNL
// //   ③ 外表非常小（几行）→ 两者差不多
// //
// // ★ BKA vs Hash Join 核心差异（面试高频）：
// //
// //   BKA（索引 JOIN）：
// //     - 外表查出来 → 批量收集 Join Key
// //     - 内表用索引一次性查所有 Key → MRR 回表
// //     - 内表有索引是前提
// //     - 外表大 → batch 也大 → 效果好
// //
// //   Hash Join（内存 JOIN）：
// //     - 外表 + 内表都要先读
// //     - Build 表全内存 → Hash Table
// //     - 内表有索引可以先过滤（减少内存占用）
// //     - Build 表太大 → Spill to Disk
//
```

### 3.2 BNL（Block Nested Loop）

```cpp
// ★ BNL Join 源码级
//
// sql/sql_join_buffer.cc
//
// // BNL 什么时候比 Hash Join 好？
// //   Hash Join 要全内存（Spill 也很慢）
// //   BNL 外表放 Join Buffer 内存 → 内表全表扫一次
// //   如果 Build 表（外表）很大 → Hash Join Spill 比 BNL 还慢
// //
// // BNL 执行流程：
// //   ① 外表读一批行到 Join Buffer（join_buffer_size）
// //   ② 内表全表扫（一行一行）
// //   ③ 外表每一行和内表每一行做 Join 条件判断
// //   ④ 外表 Buffer 满了 → 清空 → 再读下一批外表 → 内表再扫一遍
// //   ⑤ 外表 N 批 → 内表扫 N 次（但 N = 外表行数 / join_buffer_size）
// //
// // BNL vs Nested Loop 区别：
// //   NLJ：外表每行 → 内表查一次 → N 次内表查找
// //   BNL：外表每批（如 1000 行）→ 内表扫一次 → N/1000 次内表全扫
// //   → BNL 减少内表查找次数很多
//
```

---

## 四、优化器 JOIN 选择逻辑源码级

```cpp
// ★ MySQL CBO JOIN 选择逻辑（源码级，面试核心）
//
// sql/range_optimizer.cc → make_join_plan()
// sql/sql_optimizer.cc → JOIN::optimize()
//
// // 假设：JOIN t1 ON t1.id = t2.t1_id
// // 优化器在 make_join_plan() 里做：
//
// void make_join_plan(JOIN *join) {
//     // Step 1: 收集所有可用索引
//     IndexIterator it(t1);
//     for (; it.hasNext(); ++it) {
//         index_info *idx = it.next();
//         // 这个索引能覆盖 Join Key 吗？
//         if (is_join_key(idx, join_condition)) {
//             available_indexes.add(idx);
//         }
//     }
//
//     // Step 2: ★ 每种 JOIN 算法算 Cost
//
//     // Cost 1: Hash Join
//     double hash_join_cost = estimateHashJoinCost(t1, t2);
//     //    = build_rows + probe_rows + hash_table_overhead
//
//     // Cost 2: BKA Join（如果有可用索引）
//     double bka_cost = estimateBKACost(t1, t2, index);
//     //    = probe_rows + (batch_size × log(build_rows))
//
//     // Cost 3: BNL Join
//     double bnl_cost = estimateBNLCost(t1, t2);
//     //    = (probe_rows / join_buffer_size) × build_rows
//
//     // Cost 4: Nested Loop Join
//     double nlj_cost = estimateNLJCost(t1, t2);
//     //    = probe_rows × build_rows
//
//     // Step 3: ★ 选 Cost 最小的
//     JOIN_ALGO best_algo = min(hash_join_cost, bka_cost, bnl_cost, nlj_cost);
//
//     // Step 4: 设置执行计划
//     switch (best_algo) {
//         case HASH_JOIN:
//             join->set_hash_join(build_index);
//             break;
//         case BKA:
//             join->set_type(join_buffer_type);
//             join->set_index(index);
//             break;
//         case BNL:
//             join->set_type(join_buffer_type);
//             break;
//         case NLJ:
//             // 默认，什么都不设置
//             break;
//     }
// }
//
// // ★ 常见场景 Cost 计算（面试估算）：
// //
// //   外表 orders 100万行；内表 customers 1万行；join_buffer_size=256KB
// //
// //   Hash Join Cost：100万 + 1万 = 101万（线性）
// //   BNL Cost：(100万 / 10000) × 1万 = 100亿（N 次全表扫）
// //   NLJ Cost：100万 × 1万 = 1万亿（最差）
// //
// //   → 选 Hash Join！（Hash Join 快 100 亿倍）
//
```

---

## 五、四种 JOIN 算法全维度对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：四种 JOIN 算法全维度对比                         │
│  ┌──────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度      │ Hash Join          │ BKA Join      │ BNL Join  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 时间复杂度 │ ★ O(build+probe)  │ O(probe × MRR)│ O(probe × ││
│  │            │ 线性（最优）     │ 批量索引查    │ build)    ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 内存需求   │ ★ Build 表全内存  │ Join Buffer   │ Join Buff ││
│  │            │ 超了 Spill       │ + 索引查缓存  │ Buffer    ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 内表索引   │ ❌ 不需要         │ ★ 必须有      │ ❌ 不需要  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 适用场景   │ 等值 JOIN + 内存 │ 有等值索引   │ 无索引 +  ││
│  │            │ 够装 Build       │ + 外表大     │ 外表小    ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Spill 支持 │ ★ 8.0.18+ 有     │ ❌ 无         │ ❌ 限内存 ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ EXPLAIN    │ Extra=Using Hash │ Extra=Using   │ Extra=Using││
│  │ 标记       │ Join             │ join buffer   │ join buff ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Optimizer  │ hash_join=on     │ batched_key_  │ block_    ││
│  │ Switch    │                  │ access=on     │ nested_loop││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频   │ ★★★★★           │ ★★★★★       │ ★★★      ││
│  └──────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   内表有索引 → BKA Join（批量索引查最优）                     │
│   内表无索引 + Build 表内存够 → **Hash Join**（O(n+m)）      │
│   Build 表太大 + 无索引 → BNL（退化方案）                    │
│   超大 Build 表 → **Hash Join Spill to Disk**（8.0.18+）     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Hash Join 两阶段？| **Build（构造 Hash Table，选小表）→ Probe（外表逐行 O(1) 查找）** |
| 2 | Hash Join 时间复杂度？| **O(build_rows + probe_rows)** — 线性！比 NLJ/BKL 的二次复杂度快得多 |
| 3 | ★ Hash Join vs BKA vs BNL vs NLJ 核心区别？| 见第五节对比表（8 维度）|
| 4 | 优化器 JOIN 选择逻辑？| **每种 JOIN 算法算 Cost → 选最小的** |
| 5 | Hash Join Spill to Disk？| Build 表超过 join_buffer_size → 按 Hash Key 分区写临时文件 → 每 Bucket 单独 Probe |
| 6 | join_buffer_size 参数？| 默认 256KB；**生产建议 1-4MB**；Hash Join / BKA / BNL 都用 |
| 7 | 何时 BKA 比 Hash Join 好？| **内表有可用索引** + 外表不小 → BKA 批量索引查更快（不用全内存装 Build）|
| 8 | 何时 BNL 比 Hash Join 好？| Build 表太大 + Hash Join Spill 非常慢 + 无索引 → BNL（全表扫一次 N 批）|
| 9 | BKA 用哪个索引？| 等值 JOIN 的内表 Join Key 列索引；MRR 配合排序回表 |
| 10 | EXPLAIN 怎么看四种 JOIN？| Hash Join：`Extra=Using Hash Join`；BKA：`Extra=Using join buffer (Batched key access)`；BNL：`Extra=Using join buffer`；NLJ：无 Extra 标记 |
| 11 | optimizer_switch 关键开关？| **hash_join=on, batched_key_access=on, block_nested_loop=on**（MySQL 8.0 默认全开）|
| 12 | Adaptive Hash Join（8.0.20+）？| 运行时自适应 BNL → Hash Join；如果 Join Buffer 装下 Build 表 → Hash Join |
| 13 | Hash Join Build 表怎么选？| Cost 估算：选行数少 / 列少的表（内存 Hash 占用最小）|
| 14 | 面试最高频考点排序？| **1. Hash Join 两阶段 + 复杂度 2. 四种 JOIN 选择逻辑 3. BKA vs Hash Join 什么时候比对方好 4. EXPLAIN 诊断 5. 优化器 Cost 估算** |
| 15 | 学习路径？| Hash Join Build+Probe → BKA MRR → BNL Block → 优化器 make_join_plan → Cost 估算 |
| 16 | 面试常见追问？| 100万行外表 JOIN 1万行内表，哪个 JOIN 最好？| Hash Join（1万行 Build 全内存 → O(101万) 快）|
| 17 | 面试常见追问？| 4表 JOIN 的顺序怎么决定？| 优化器枚举所有排列（n!）→ 每个排列估 Cost → 选最小；复杂 JOIN 可能用贪心或动态规划 |
| 18 | 面试陷阱？| 说 Hash Join 就是"全内存扫"→ ❌ Build 表超过内存会 **Spill to Disk**（8.0.18+），不会爆内存 |
| 19 | 面试陷阱？| 说 BKA 是 Hash Join 的变种 → ❌ BKA 是**索引 JOIN**；Hash Join 是**内存 JOIN**；原理完全不同 |
| 20 | MySQL Hash Join 和 TiDB Hash Join？| MySQL 8.0 原生支持 Spill；TiDB Hash Join 可以下推到 TiKV Coprocessor（列级）|
| 21 | Hash Join 的 NULL 值处理？| Hash Table 里 NULL 统一放一个 bucket；严格 NULL 安全（a.id = b.id 中 NULL = NULL 不匹配）|
| 22 | 临时文件清理时机？| Hash Join Spill 的临时文件在查询结束后立即删除；tmpdir 目录下 |
| 23 | BNL 和 BKA 都用 join_buffer_size？| 是的；BNL 装外表批量；BKA 装 Join Key 批量 |
| 24 | 典型生产 SQL（强制 JOIN）？| SELECT /*+ HASH_JOIN(t2) */ ... FROM t1 JOIN t2 ON ... |
| 25 | Hash Join 什么时候不如 NLJ？| 两个表都极小（<100 行）→ Hash Table 构建成本 > NLJ 直接算 |
| 26 | 优化器禁用 Hash Join 看效果？| SET optimizer_switch='hash_join=off'；然后 EXPLAIN 看退化成什么算法；对比 Cost 和实际耗时 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：sql/sql_hash_join.cc + sql/sql_join_buffer.cc + sql/range_optimizer.cc → make_join_plan()  
**面试考点：26 题**  
**差异化点**：Hash Join Build+Probe 两阶段源码级、四种 JOIN 选择逻辑（Cost 估算）、Hash Join Spill to Disk、**四种 JOIN 全维度对比表**
