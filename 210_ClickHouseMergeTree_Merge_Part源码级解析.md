# ClickHouse MergeTree Merge 策略 + Part 合并源码级深度解析

> **源码基线**：ClickHouse 23.x+（C++）  
> **核心位置**：src/Storages/MergeTree/ + src/Storages/MergeTree/MergeList.cpp  
> **面试权重**：⭐⭐⭐⭐⭐（ClickHouse 列式存储核心，高频补全）

---

## 一、MergeTree 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MergeTree = ClickHouse 列式存储引擎核心                   │
│                                                               │
│  核心特性（面试必背）：                                        │
│   ✅ Part 不可变（Immutable）—— 每个写入生成一个新 Part        │
│   ✅ 后台 Merge（合并）—— 小 Part 合并成大 Part               │
│   ✅ 稀疏索引（Sparse Index）—— granule=8192 行一个索引标记   │
│   ✅ 主键排序 —— 主键索引 + 稀疏索引加速点查和范围查          │
│   ✅ 分区 —— PARTITION BY Key 物理分区                        │
│                                                               │
│  ★ 为什么叫 MergeTree？                                       │
│   - 写入时直接 Append 新 Part（LSM Tree 思想）               │
│   - 后台定期 Merge 小 Part → 大 Part（类似 RocksDB Compaction）│
│   - Merge 是后台异步的（不阻塞写入）                          │
│   → "Merge" + "Tree"（主键排序的 B+ Tree 索引）              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Part 结构源码级

### 2.1 Part 文件组织

```cpp
// ★ ClickHouse MergeTree Part 文件组织（源码级）
//
// src/Storages/MergeTree/MergeTreePartInfo.cpp
// src/Storages/MergeTree/IMergeTreeDataPart.cpp
//
// // Part 物理目录结构：
// // /var/lib/clickhouse/data/{database}/{table}/{part_name}/
// //
// // Part 命名规则：
// //   all_1_3_2    // (覆盖所有分区，MinBlock=1, MaxBlock=3, Level=2)
// //   20240101_1_5_1 // (分区 2024-01-01, MinBlock=1, MaxBlock=5, Level=1)
// //
// // ★ 核心文件（列式存储）：
// //   data_0.bin          // 第一列数据（二进制）
// //   data_0.mark         // 第一列 mark 文件（稀疏索引）
// //   data_1.bin          // 第二列数据
// //   data_1.mark
// //   ...
// //   primary.idx         // 主键索引（主键列的稀疏索引）
// //   primary.mrk2        // 主键 mark
// //   checksum.txt        // 所有文件 CRC 校验
// //   columns.txt         // 列名列表
// //   count.txt           // 行数
// //   minmax_{col}.json   // 每列 Min/Max（分区裁剪 & Merge 选择）
// //
// // ★ mark 文件（稀疏索引核心，面试高频）：
// //   granule = 8192 行（默认）
// //   每 8192 行一个 mark（索引标记）
// //   → 查询时用 mark 跳 granule（不是跳 row！）
// //   → 跳过整组数据，极大加速范围查询
// //
// // ★ Part Level（Merge 层级）：
// //   Level 0：刚写入的 Part（还没合并过）
// //   Level 1：由 Level 0 合并而来
// //   Level 2：由 Level 1 合并而来
// //   ...
// //   Level 越高 → Part 越大 → 索引越有效 → 查询越快
// //   // Merge 时相同 Level 的 Part 合并成下一级
//
```

### 2.2 主键索引 + 稀疏 mark

```cpp
// ★ ClickHouse 主键索引源码级（面试核心！）
//
// // 主键 ≠ PRIMARY KEY（其他数据库的含义）
// // ClickHouse 主键 = **排序键**（ORDER BY Key）
// // // // 每个 Part 内部数据按 ORDER BY Key 排序
// // // // primary.idx 存主键排序后每个 granule 的主键值
// //
// // 例子：
// // CREATE TABLE orders (
// //     id UInt64,
// //     user_id UInt32,
// //     amount Float64,
// //     dt Date
// // ) ENGINE = MergeTree()
// // ORDER BY (user_id, id)  // ★ 这就是"主键"
// // PARTITION BY dt;
// //
// // Part 内部排序：
// //   (user_id=1, id=100), (user_id=1, id=101), (user_id=2, id=200), ...
// //
// // primary.idx（稀疏索引）：
// //   granule 0 → (user_id=1, id=100)     ← 每个 granule 的起点值
// //   granule 1 → (user_id=2, id=200)
// //   granule 2 → (user_id=5, id=500)
// //   ...
// //   // 只有主键列存储 mark；其他列单独存 mark（data_N.mark）
// //
// // ★ 查询时索引使用（源码级）：
// //   SELECT * FROM orders WHERE user_id = 2 AND id >= 200;
// //
// //   a. 读 primary.idx → 找到 user_id=2 对应的 granule 1
// //   b. 跳过 granule 0（不可能有 user_id=2）
// //   c. granule 1 读 data → 找到具体 id
// //   // → 不是按行索引，是按 granule 索引 → 索引体积极小
// //   // → 所以能支持"全表索引"（1/8192 的开销）
// //
```

---

## 三、Merge 策略源码级

### 3.1 Merge 触发 + 选择

```cpp
// ★ ClickHouse Merge 触发源码级（面试核心）
//
// src/Storages/MergeTree/MergeList.cpp
// src/Storages/MergeTree/MergeTreeDataMergerMutator.cpp
//
// // Merge 是什么？
// //   多个小 Part → 合并成一个大 Part
// //   // 类似 RocksDB Compaction
// //   // Merge 是后台线程异步做的
// //   // 不阻塞前台写入
// //
// // ★ Merge 触发时机（三种）：
// //
// // 1. 后台定时 Merge（默认）：
// //    MergeThread 每隔 MergeTreeMergeSleepTimeMs（默认 50ms）检查一次
// //
// // 2. 写入后立即触发（optimize_on_insert）：
// //    写入完成后 → 立刻尝试 Merge（如果 Part 数量超过阈值）
// //
// // 3. 手动 OPTIMIZE 触发：
// //    OPTIMIZE TABLE orders FINAL;
// //    // 强制 Merge 所有 Part 成一个（慢但查询最快）
// //
// // ★ Merge 选择算法（源码级，面试高频）：
// //
// // MergeTreeSelector::selectPartsToMerge():
// //   1. 按 Partition 分组（不同分区不 Merge）
// //   2. 在每个 Partition 内找合适的 Part 组：
// //      // 规则 A：同 Level 的 Part 合并
// //      //   Level 0 有 > max_parts_in_total（默认 15000）→ 选几个 Level 0 合并
// //      //   Level N 有几个 Part → 合并成 Level N+1
// //      //
// //      // 规则 B：综合考虑 Part 大小和数量
// //      //   → 选择 Part 数最少但总大小合适的一组
// //      //   → 避免"一个超级 Part 合并一个极小 Part"（浪费 IO）
// //      //
// //      // 规则 C：minmax 关系
// //      //   如果 Part A 的 minmax 和 Part B 不重叠 → 合并可能更小
// //      //   但 MergeTree 主要靠 Level 判断
// //
// // 3. 并发 Merge 数量：
// //    max_merges（默认 CPU 核数 / 2）
// //    max_mutations（默认 CPU 核数 / 2）
// //
```

### 3.2 Merge 执行流程

```cpp
// ★ Merge 执行流程源码级（面试高频）
//
// MergeTreeMerger::execute()
//
// // Step 1: 加锁 + 读 Part 元数据
// MergeList 加写锁 → 找到要 Merge 的 Parts → 记录 MergeList
//
// // Step 2: 创建新 Part 目录
// new_part = MergeTreePartInfo(all, MinBlock, MaxBlock, newLevel)
// 创建目录 /database/table/{new_part_name}/
//
// // Step 3: 逐列数据合并（核心！列式存储优化）
// //
// // 关键优化：**按列合并，不是按行合并！**
// //
// // 合并流程：
// // for each column:
// //     从所有源 Part 读取该列数据
// //     → 按主键顺序归并（因为每个 Part 内部已按主键排序！）
// //     → 写入新 Part 的该列 data_N.bin + data_N.mark
// //
// // ★ 时间复杂度：O(N log N)（N = 总行数）
// //   // 每个 Part 内部已有序 → 多路归并（像 Merge Sort）
// //   // 不需要重新排序！
// //   // 列式合并：同一列连续读 → 缓存友好
// //
// // ★ 合并时自动处理 mutation（DELETE/UPDATE）：
// //   Part A（Level 0）有 Row 1-100
// //   Part B（Mutation）删了 Row 50
// //   // Merge 时读 Part A 数据 → 检查 Part B 的 DELETE 标记 → 跳过 Row 50
// //   // 输出的新 Part 不包含 Row 50
// //
// // Step 4: 写新 Part 元数据 + checksum
// //   count.txt = 合并后的总行数
// //   minmax.json = 合并后的 Min/Max
// //   primary.idx = 合并后的主键稀疏索引
// //   checksum.txt = CRC
// //
// // Step 5: 替换旧 Parts
// //   MergeList 加写锁
// //   标记旧 Parts 为 "即将删除"
// //   注册新 Part 到 MergeTreeData
// //   旧 Parts 后台异步删除（不是立刻！→ 查询还在用旧 Parts 的 mark）
//
```

---

## 四、Merge 策略对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MergeTree Merge vs RocksDB Compaction vs LSM Tree 对比   │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ MergeTree Merge   │ RocksDB Compaction│ LSM Tree ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 存储格式         │ ★ 列式           │ 行存 KV       │ 行存 KV   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 合并触发         │ Level + 定时      │ Size/Tiered   │ Tiered    ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 合并策略         │ ★ 同 Level 合并   │ Leveled 主要  │ Size-Tier ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 合并优化         │ ★ 多路归并（有序）│ Read 改写     │ Read 改写 ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 合并时处理         │ 自动处理 mutation │ 处理 Delete   │ 处理 Delete││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 读放大           │ 低（稀疏索引）    │ 中（多层查找） │ 中        ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 写放大           │ 中（Merge IO）    │ 低（顺序追加） │ 低        ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 适合场景         │ ★ 数仓 OLAP     │ 通用 KV       │ 通用 KV   ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   数仓 OLAP（聚合/Join/分析）→ **MergeTree**（列式 + Merge）  │
│   通用 KV 存储 → RocksDB（LSM Tree）                         │
│   混合场景：TiKV 用 RocksDB 存 KV + TiFlash 用 MergeTree 列式 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | MergeTree 核心是什么？| **Part 不可变 + 后台 Merge + 主键排序 + 稀疏索引** |
| 2 | ★ Part Level 含义？| Level 0（刚写入）→ Level 1（L0 合并）→ Level 2；Level 越高 Part 越大查询越快 |
| 3 | Part 命名规则？| {partition}_MinBlock_MaxBlock_Level；例：all_1_3_2 |
| 4 | ★ MergeTree "主键" 真正含义？| **ORDER BY Key（排序键）**，不是传统数据库的 PRIMARY KEY |
| 5 | 稀疏索引 granule 默认值？| **8192 行**；每 8192 行一个 mark；索引体积极小（1/8192）|
| 6 | Merge 触发三种方式？| **后台定时（默认 50ms）+ optimize_on_insert（写入后）+ OPTIMIZE TABLE（手动）** |
| 7 | ★ Merge 选择核心规则？| **同 Level 的 Part 合并**；避免超级 Part 合并极小 Part |
| 8 | Merge 执行核心优化？| **按列合并 + 多路归并**（每个 Part 内部已按主键排序 → O(N log N) 归并） |
| 9 | Merge 时自动处理什么？| **Mutation（DELETE/UPDATE）标记**；新 Part 不包含被删的行 |
| 10 | Merge 替换旧 Part 方式？| 先注册新 Part → 标记旧 Part 为待删除 → 旧 Part **异步后台删除**（不阻塞查询）|
| 11 | Merge 并发数？| max_merges（默认 CPU 核数/2）+ max_mutations |
| 12 | max_parts_in_total 默认？| **15000**；超过触发强制 Merge；防止 Part 爆炸 |
| 13 | ★ MergeTree Merge vs RocksDB Compaction 对比？| 见第四节对比表（8 维度）|
| 14 | 面试最高频考点排序？| **1. 主键=排序键 + 稀疏索引 2. Part Level 3. Merge 选择同 Level 4. Merge 多路归并 5. Part 异步删除** |
| 15 | 学习路径？| Part 结构 → 主键/稀疏索引 → Level → Merge 选择 → Merge 执行 → 对比 |
| 16 | 面试常见追问？| 为什么 MergeTree 主键不是 PRIMARY KEY？| ClickHouse 没有传统意义的主键约束；**ORDER BY Key 只是排序键**（重复数据可以插入） |
| 17 | 面试常见追问？| Merge 会阻塞查询吗？| **不会！** Merge 和查询并行；旧 Part 只有在查询结束后才真正删除 |
| 18 | 面试陷阱？| 说 "MergeTree 是树状索引" → ❌ **MergeTree 没有 B+ Tree！**；主键只是排序 + 稀疏 mark；真正的查询是按 granule 跳 mark |
| 19 | 面试陷阱？| 说 "Merge 会重新排序数据" → ❌ **不会！** 每个 Part 内部已经按主键排序；Merge 只是多路归并 |
| 20 | 分区裁剪怎么工作？| PARTITION BY dt；查询 WHERE dt='2024-01-01' → 跳过其他分区的所有 Part |
| 21 | MergeTree Min/Max 索引？| Part 里每列都存 minmax_{col}.json；查询时可以跳过整 Part（Part 的 Min > 查询条件 Max → 不可能命中）|
| 22 | 典型生产配置？| **PARTITION BY toYYYYMM(dt) + ORDER BY (user_id, dt) + MergeTreeMergeSleepTimeMs=50ms** |
| 23 | OPTIMIZE TABLE FINAL 做什么？| **强制所有 Part 合并成一个**；最慢但查询最快（只有一个 Part 要扫）|
| 24 | 为什么叫 MergeTree？| Merge（后台合并 Part）+ Tree（主键排序的树状查询逻辑）|
| 25 | MergeTree 写入性能？| Append-only 直接写新 Part → **非常快（百万行/秒）**；Merge 异步后台做 |
| 26 | 典型生产问题：Part 爆炸？| max_parts_in_total=15000 超过 → 强制 Merge；写入太快 Merge 跟不上 → 生产监控 Part 数量 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：src/Storages/MergeTree/MergeList.cpp + MergeTreeDataMergerMutator.cpp + IMergeTreeDataPart.cpp  
**面试考点：26 题**  
**差异化点**：Part Level 层级 + 稀疏索引 granule=8192、**主键=ORDER BY Key（排序键，不是 PRIMARY KEY）**、Merge 同 Level 选择 + 多路归并、MergeTree vs RocksDB vs LSM Tree 三引擎对比
