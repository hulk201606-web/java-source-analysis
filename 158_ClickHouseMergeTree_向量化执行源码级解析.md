# Apache ClickHouse MergeTree + 向量化执行源码级深度解析

> **源码基线**：ClickHouse 24.x（C++）  
> **核心位置**：src/Storages/MergeTree/ + src/Processors/ + src/Columns/  
> **面试权重**：⭐⭐⭐⭐⭐（OLAP 性能之王，向量化+列存标杆）

---

## 一、ClickHouse 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ClickHouse = Yandex 开源的列式 OLAP 数据库               │
│                                                               │
│  核心标签（面试必背）：                                        │
│   ① 列式存储（只读需要的列）                                  │
│   ② ★ 向量化执行（SIMD，批处理一列）                         │
│   ③ MergeTree 表引擎（LSM 思想 + 后台合并）                 │
│   ④ 稀疏索引（主键/排序键跳数索引）                           │
│   ⑤ 高压缩比（LZ4/ZSTD + 同列类型）                          │
│   ⑥ 单机极致性能（MPP 分片+副本）                            │
│                                                               │
│  vs Doris（面试高频）：                                       │
│   CH 单机向量化更强、运维简单；Doris 有 FE/BE MPP + 事务/统一 SQL│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、MergeTree 存储引擎

### 2.1 数据分区与 Parts

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MergeTree 磁盘结构（面试必背）                           │
│                                                               │
│  /var/lib/clickhouse/data/db/table/                          │
│  ├── 20261022_1_1_0/        ← 一个 Data Part（一次插入）    │
│  │   ├── id.bin / name.bin / age.bin   ★ 每列一个 .bin 数据 │
│  │   ├── id.mrk / name.mrk            列的 mark 偏移索引    │
│  │   ├── primary.idx                  ★ 主键稀疏索引        │
│  │   ├── count.txt                    该 part 行数           │
│  │   ├── columns.txt / checksums.txt                       │
│  │   └── default_compression_codec.txt                     │
│  ├── 20261022_2_2_0/                                        │
│  ├── 20261023_3_4_1/        ← _1 表示合并层级              │
│  └── detached/                                               │
│                                                               │
│  命名：partition_minBlock_maxBlock_level                     │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 写入：内存攒批 → 落 Part

```cpp
// ★ MergeTree 写入流程（源码级，面试核心）
//
// src/Storages/MergeTree/MergeTreeDataWriter.cpp
//
// ① INSERT 的数据先进内存表（按 block 攒批，不是内存表引擎）
//    数据按 ORDER BY 键（排序键）在内存排序
//
// ② 攒够一批（或 flush）→ 生成一个不可变 Data Part：
//    - 每列写一个 .bin（列文件，压缩）
//    - 写 .mrk（mark 文件：granule 粒度的偏移量）
//    - 写 primary.idx（稀疏主键索引，每 granule 一个索引值）
//
// ③ ★ 后台 Merge（类似 LSM Compaction）：
//    后台线程把多个小 Parts 合并成大 Part：
//    - 多路归并排序（保持 ORDER BY 键有序）
//    - 同列数据重新压缩（压缩率更高）
//    - 执行引擎特定逻辑（Replacing/Summing/Aggregating 去重聚合）
//    - 旧 Parts 标记非激活，最终删除
//
// ★ 关键设计：
//   写入顺序追加 + 后台合并 → 写吞吐高
//   Parts 不可变 → 读时文件不变，无锁并发读
//   不能原地 UPDATE/DELETE（用 ALTER MUTATION 异步重写 Part）
//
```

### 2.3 稀疏主键索引（面试重点！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ClickHouse 稀疏索引（和 MySQL B+树完全不同，面试必背）   │
│                                                               │
│  数据按 ORDER BY (a, b) 排序，每 index_granularity（默认8192）│
│  行一个 granule（数据颗粒）                                  │
│                                                               │
│  primary.idx 只存每个 granule【第一行】的主键值：            │
│  ┌──────┬──────┬──────┬──────┐                              │
│  │ a=1  │ a=15 │ a=31 │ a=47 │  （每 8192 行一个标记）      │
│  └──────┴──────┴──────┴──────┘                              │
│     granule0  granule1  granule2  granule3                   │
│                                                               │
│  查 WHERE a=20：                                              │
│   ① 二分 primary.idx 找到 a=20 落在 granule1（15~31）       │
│   ② 通过 .mrk 定位 granule1 在 .bin 的偏移                  │
│   ③ 解压该 granule 的 8192 行，逐行过滤                     │
│   ★ 索引不指向单行，而是一个 granule 范围（稀疏！）         │
│                                                               │
│  优点：索引极小（8192 行一个标记），可全放内存               │
│  代价：定位到 granule 后要扫 8192 行（靠向量化加速）        │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.4 跳数索引（Data Skipping Index）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 辅助索引：跳数索引（面试高频）                            │
│                                                               │
│  非排序列无法用主键索引，用跳数索引按 granule 存统计信息：   │
│   minmax：granule 内列的最小/最大值（范围裁剪）             │
│   set(max N)：granule 内出现的值集合                        │
│   bloom_filter：布隆过滤器（等值查询）                      │
│   tokenbf_v1 / ngrambf_v1：字符串分词布隆                    │
│                                                               │
│  查询时：如果 granule 的统计表明不可能包含目标值            │
│   → 整个 granule（8192行）跳过，不读不压                    │
│   → 进一步减少 IO                                            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、向量化执行引擎（核心！）

### 3.1 列存 + Block + SIMD

```cpp
// ★ ClickHouse 向量化执行（源码级，面试核心！）
//
// src/Columns/ + src/Functions/ + src/Processors/
//
// 核心数据结构：Block = 一批列（不是一行行处理！）
// struct Block {
//     ColumnWithTypeAndName columns[];  // 一批列
//     size_t rows;                       // 这批多少行（如 8192/65536）
// };
//
// IColumn 是列的抽象，具体实现：
//   ColumnVector<T>：连续的原生数组 T[]（★ SIMD 友好）
//     - ColumnUInt64：uint64_t data[] 连续内存
//   ColumnString：偏移量数组 + 字符池
//   ColumnNullable：嵌套列 + null 掩码
//
// ★ 函数一次算一整列：
// interface IFunction {
//     executeImpl(Block & block, arguments, result) {
//         // 一次处理 data[0..rows)，不是每行一次虚函数调用
//         const auto & a = columns[0]->getData();   // const double*
//         auto & res = result_col.getData();        // double*
//         for (size_t i = 0; i < rows; ++i) {
//             res[i] = a[i] * 2;          // 紧凑循环，编译器自动向量化
//         }
//         // 热点函数手写 SIMD（SSE4/AVX2/AVX-512）
//     }
// }
//
// ★ 为什么快（和 Doris BE 同样的原理，复习）：
//   ① 连续内存 + 紧凑循环 → CPU SIMD 一条指令算多个值
//   ② 减少虚函数调用（一批一次，不是每行一次）
//   ③ 列存只读查询需要的列 + 高压缩 → IO 少
//   ④ 缓存友好（同列数据连续，L1/L2 命中高）
//
```

### 3.2 Pipeline 执行模型（新执行引擎）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ClickHouse 执行引擎演进（面试高频）                       │
│                                                               │
│  旧引擎：火山模型（Volcano，逐行 next）                      │
│   IBlockInputStream::read() 拉取 block，算子间有队列开销     │
│                                                               │
│  ★ 新引擎：Processors / Pipeline（2020+）：                  │
│   src/Processors/                                             │
│   - ISource（读 Part）→ 多个 ISimpleTransform（函数/过滤）   │
│   - 多个 Processor 组成 DAG                                  │
│   - 有固定数量输入输出端口（Port），显式数据流                │
│   - Push/Pull 混合，配合线程池并行执行                       │
│   - 聚合/Join 有专门的并行 Processor                         │
│   → 减少虚函数、更细粒度并行、多核扩展性好                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、分布式查询与聚合

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ClickHouse 分布式执行（面试必背）                         │
│                                                               │
│  架构：                                                        │
│   Shard（分片）：数据水平拆分，每个 Shard 一部分数据         │
│   Replica（副本）：每个 Shard 可有副本（复制靠 ZooKeeper/    │
│     ClickHouse Keeper）                                       │
│                                                               │
│  Distributed 表：                                              │
│   逻辑视图，查询时把 SQL 发给所有 Shard                      │
│    ① Initiator（接收查询节点）改写 SQL                       │
│    ② 各 Shard 本地执行（扫描+两阶段聚合第一阶段）            │
│    ③ 结果返回 Initiator 做最终合并（第二阶段）               │
│                                                               │
│  两阶段聚合（和 TiDB/Doris 同理）：                           │
│   SELECT count(), sum(x) FROM t GROUP BY k：                 │
│   各 Shard 本地 GROUP BY k 返回部分结果（数据量小）          │
│   Initiator 合并各 Shard 相同 key 的聚合值                    │
│                                                               │
│  ★ 分布式 Join（广播/字典）：                                 │
│   小表广播到各 Shard（GLOBAL JOIN）                           │
│   大表 Shuffle Join 代价高（CH 更擅长单表分析）              │
│
└─────────────────────────────────────────────────────────────┘
```

### MergeTree 家族

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MergeTree 引擎家族（面试高频）                            │
│                                                               │
│  MergeTree（基础）：只排序+合并，保留所有版本                │
│  ReplacingMergeTree：合并时按主键去重（保留最新/指定版本）   │
│  SummingMergeTree：合并时对非主键数值列自动求和              │
│  AggregatingMergeTree：配合物化视图 + 聚合函数状态           │
│  CollapsingMergeTree：sign 字段折叠取消/有效行               │
│  VersionedCollapsingMergeTree：带版本的折叠                  │
│                                                               │
│  注意：这些"去重/聚合"发生在后台 Merge 时，非实时保证        │
│   查询精确去重仍需 GROUP BY / FINAL（FINAL 性能差）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ClickHouse 为什么快？| **列存裁剪 + 向量化 SIMD + 稀疏索引 + 高压缩 + 顺序写 Part** |
| 2 | MergeTree Part 结构？| 每列 .bin + .mrk 偏移 + primary.idx 稀疏索引 + count/meta |
| 3 | 写入流程？| 内存攒批排序 → 生成不可变 Part（列文件）→ 后台 Merge 合并 |
| 4 | 后台 Merge 作用？| 多路归并小 Part + 重压缩 + 执行去重/聚合引擎逻辑 |
| 5 | 稀疏索引原理？| 每 8192 行（granule）存一个主键值，二分定位 granule 再扫 8192 行 |
| 6 | 和 MySQL B+树索引区别？| CH 稀疏索引指向 granule 范围不指向行；MySQL 二级索引定位单行 |
| 7 | index_granularity？| 默认 8192，一个 granule 行数；调小索引大定位准，调大索引小 |
| 8 | 跳数索引？| minmax/set/bloom_filter 按 granule 存统计，不可能命中就跳过整 granule |
| 9 | 向量化核心结构？| **Block（一批列）+ ColumnVector 连续数组 + 函数一次算一列** |
| 10 | SIMD 加速？| 连续内存紧凑循环 + 手写 AVX2/AVX-512，一条指令算多个值 |
| 11 | 向量化四大优势？| SIMD + 减少虚函数 + 列存少 IO + 缓存友好 |
| 12 | 新执行引擎？| Processors/Pipeline DAG 替代火山模型，端口显式数据流+并行 |
| 13 | Shard/Replica？| Shard 水平分片；Replica 副本（Keeper 协调）|
| 14 | Distributed 表？| 逻辑视图，查询下发各 Shard + Initiator 汇总 |
| 15 | 两阶段聚合？| 各 Shard 本地 group by → Initiator 合并相同 key 结果 |
| 16 | GLOBAL JOIN？| 小表广播各 Shard 做本地 join（CH 弱在大表 shuffle join）|
| 17 | MergeTree 家族？| Replacing（去重）/Summing（求和）/Aggregating（物化聚合）/Collapsing（折叠）|
| 18 | ReplacingMergeTree 实时去重吗？| ❌ 去重只在后台 Merge；查询精确去重用 FINAL/GROUP BY |
| 19 | CH 支持 UPDATE/DELETE 吗？| 支持但是异步 Mutation（重写 Part，开销大）；不适合频繁更新 |
| 20 | CH vs Doris？| CH 单机向量化极致/引擎多；Doris MPP/事务/MySQL 协议/统一 FE 更全面 |
| 21 | 面试最高频考点排序？| **1. 向量化原理 2. 稀疏索引 3. MergeTree 写入/Merge 4. 列存压缩 5. 分布式两阶段** |
| 22 | 学习路径？| MergeTreeDataWriter → primary.idx/mrk → ColumnVector/Function → Processors → Distributed |
| 23 | 面试常见追问？| 为什么索引只存 granule？→ 索引极小放内存，8192 行扫描靠向量化很快 |
| 24 | 压缩为什么高？| 同列数据类型一致 + 排序后重复值连续，LZ4/ZSTD 压缩率高 |
| 25 | ClickHouse Keeper？| 8.x 自研替代 ZooKeeper 的协调组件（Raft），管副本/副本队列 |
| 26 | 面试陷阱？| 说 CH 是行存或逐行执行 → ❌ 纯列存 + 向量化批处理是核心竞争力 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：src/Storages/MergeTree/ + src/Columns/ + src/Processors/  
**面试考点：26 题**  
**差异化点**：Part/稀疏 granule 索引 + Block/ColumnVector/SIMD 向量化 + 后台 Merge + 分布式两阶段 + 引擎家族
