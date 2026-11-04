# Elasticsearch Segment Merge + Lucene 倒排索引源码级深度解析

> **源码基线**：Elasticsearch 8.x（Java）+ Apache Lucene 9.x（Java）  
> **核心位置**：server/indices/merge + org.apache.lucene.index + org.apache.lucene.codecs  
> **面试权重**：⭐⭐⭐⭐⭐（ES 存储核心，高频）

---

## 一、Lucene + ES Segment 体系全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Lucene Segment 核心特性（面试必背）                         │
│                                                               │
│  Segment = Lucene 不可变索引分片（类似 ClickHouse Part）     │
│                                                               │
│  关键规则：                                                    │
│   ① Segment 一旦写入就**不可变**（Immutable）               │
│   ② 新写入 → 新建 Segment（Append-Only 架构）              │
│   ③ 查询要扫**所有** Segment → Segment 太多 → 查询慢       │
│   ④ 后台线程做 Segment Merge → 合并小 Segment 成大 Segment  │
│                                                               │
│  ★ Segment 内部文件结构（面试高频）：                          │
│   .segments_N     segment 元数据（N 是 Generation）          │
│   .fnm            Field Names（字段名列表）                 │
│   .fnm            Field Norms（字段归一化值/权重）           │
│   .doc            Stored Fields（存储字段值）               │
│   .pos            Term Positions（词项位置，Phrase Query 用）│
│   .pay            Payloads（额外附加数据）                   │
│   ★ .tip          Term Dictionary Index（FST 索引）         │
│   ★ .tis          Term Dictionary（倒排索引：Term→Postings） │
│   ★ .post         Postings List（每个 Term 出现的文档 ID）  │
│   .nvd / .ndv     Numeric DocValues（列式存储，排序/聚合）  │
│   .kdd / .kdm     KV DocValues（BKD Tree 存储）            │
│                                                               │
│  ★ 一个 Index = 多个 Shard = 每个 Shard = 多个 Segment      │
│   ES Shard 就是一个 Lucene Directory                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Lucene 倒排索引 + FST 源码级

### 2.1 倒排索引结构

```java
// ★ Lucene 倒排索引核心（源码级，面试核心）
//
// org.apache.lucene.codecs.lucene90.Lucene90TermsReader
// org.apache.lucene.index.Terms
// org.apache.lucene.util.fst.FST（Finite State Transducer）
//
// // 倒排索引映射：Term → Postings List
// // Term = (field, termText) 例如：("title", "elasticsearch")
// // Postings List = 所有包含这个 Term 的文档 ID 列表
// //                 + 每个文档里 Term 出现频率 + 位置
//
// // Lucene90TermsReader.open() 读取 Term Dictionary：
// public final class Lucene90TermsReader extends FieldsReader {
//
//     // ★ 两级索引（性能关键）：
//     // ① FST 索引（.tip 文件）：快速定位 Term 在 .tis 里的偏移
//     // ② Term Dictionary（.tis 文件）：真正的 Term→Postings 映射
//
//     public Terms terms(String field) {
//         // 1. 打开 FST（Field → Term 字典索引）
//         FST<Long> fst = FST.read(input);  // ★ FST 内存加载
//         //    FST 是压缩的有限状态自动机，能快速查 term → pointer
//         //    内存友好：百万级 term 通常 < 100MB
//
//         // 2. 通过 FST 找到 Term 在 .tis 文件里的起点
//         Long termPointer = fst.get("elasticsearch");
//         //    FST.get() 时间复杂度：O(length(term))
//         //    不依赖 term 总数（不管百万还是千万级 term 都很快）
//
//         // 3. 从 .tis 文件读取 Postings List 的起始位置
//         //    .tis 结构：[term1][postingsOffset1][term2][postingsOffset2]...
//         //    每段 Term 用增量编码压缩（Prefix-compressed）
//
//         // 4. 从 .post 文件读 Postings List（DocID 列表）
//         //    // 用 Varint + Delta 编码压缩：
//         //    // Postings: [docID1, docID2-docID1, docID3-docID2, ...]
//         //    // 每个 docID 后面可选：tf（频率）、positions（位置）
//     }
// }
//
```

### 2.2 FST（Finite State Transducer）

```java
// ★ Lucene FST 核心（源码级，面试高频）
//
// org.apache.lucene.util.fst.FST
//
// // FST = 有限状态自动机 + 输出映射
// // 输入：字符串（Term）→ 输出：long 值（Postings Pointer）
// // 比 HashMap 内存小 5-10 倍（共享前缀 + 压缩）
// // 查询复杂度：O(length of input)
//
// // 构造过程：
// // 1. 把所有 Term 排序（字典序）
// // 2. 从叶子节点回溯构建最小化 DFA
// //    共享公共前缀（"search" 和 "searched" 共享 "search"）
// //    共享公共后缀（不同 Term 的相同后缀合并到同一状态）
// // 3. 每个 Term 末尾关联一个 output 值（postingsOffset）
//
// // FST 查询：
// FST<Long> fst = FST.read(fstInputStream);  // 从 .tip 文件加载
// Long result = fst.get("elasticsearch");
// // 状态机逐字符转移：e → s → e → a → r → c → h → ...
// // 到达终止状态 → 返回 output 值
//
// ★ 为什么 FST 这么高效（面试高频）：
//   - 内存压缩：共享前后缀 → 百万级 term < 100MB
//   - 时间 O(term 长度)：与 term 总数无关
//   - 内存 vs 磁盘 tradeoff：小 FST 全内存，大 FST mmap 部分加载
//   - FST 是 Lucene Term Dictionary 的核心索引结构
//
// ★ Lucene 还有 Term Vectors（索引时可配置）：
//   存储每个文档的 term 列表 + 频率 + 位置
//   作用：More Like This / Per-field Highlighting / 自定义评分
//
```

### 2.3 Postings List + 倒排查询

```java
// ★ Postings List 遍历查询（源码级）
//
// // 查 Term "elasticsearch" 在 Field "title" 里的所有文档：
//
// // 步骤 A：从 Term Dictionary 找到 Postings 起点
// FST<Long> fst = fstReader.get("title");        // 拿 title 的 FST
// Long offset = fst.get("elasticsearch");        // FST 快速定位
//
// // 步骤 B：读 Postings List
// // .post 文件格式（Lucene 90）：
// //   Block-based（块结构，每块 256 个 docID）
// //   每个块内：
// //     - Skip Data（跳转表：块内每个 docID 的编码位置）
// //     - DocIDs（Delta 压缩：d1, d2-d1, d3-d2...）
// //     - Frequencies（如果存在 tf）
// //     - Positions（如果存在位置）
// //
// PostingsEnum postings = reader.postings("title", "elasticsearch");
// //    postings.nextDoc() → 下一个 docID（Varint 解码）
// //    postings.freq() → 这个 docID 里 term 出现次数
// //    postings.nextPosition() → 下一个位置（Phrase Query 用）
//
// // 步骤 C：多 Term Boolean Query
// //    BoolQuery(SHOULD: [term1, term2, term3])
// //    → 每个 Term 一个 PostingsEnum → DisjunctionSumScorer 合并
// //    // 跳表（Skip List）加速：当一个 Term 遍历到块边界时
// //    // 用 Skip Data 直接跳到下一个匹配的块（O(1)）
//
// ★ Numeric DocValues（列式存储，排序/聚合用）：
//   .nvd / .ndv 文件：
//   对 INT/LONG/FLOAT/DOUBLE 字段做列式存储
//   - SortedNumericDocValues：排序后的数值列表（O(1) 随机访问）
//   - SortedSetDocValues：多值集合（IP/Category/Enum）
//   - BKD Tree：地理位置（GeoPoint/GeoShape）
//   // DocValues 用于 fielddata / sort / aggregations
//   // 不参与 Text Query（Text Query 走倒排 Term Dictionary）
//
```

---

## 三、ES Segment Merge 核心源码级

### 3.1 Merge 动机与策略

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ES Segment Merge 核心问题与策略（面试必背）                │
│                                                               │
│  Segment 不可变 → 长期运行会产生大量小 Segment               │
│  查询要扫所有 Segment → 太多 Segment → 查询延迟飙升         │
│  → 后台 Merge：把小 Segment 合并成大 Segment                 │
│                                                               │
│  ★ MergePolicy（Lucene 源码级）：                             │
│  ┌──────────────────────────────────────────────────────┐    │
│  │ TieredMergePolicy（Lucene + ES 默认）                 │    │
│  │                                                       │    │
│  │ 分两层 Tier：                                         │    │
│  │   Tier 1（大 Segment）：不主动 merge（保持稳定）     │    │
│  │   Tier 2（小 Segment）：按大小分桶，桶内做 merge      │    │
│  │                                                       │    │
│  │ 关键参数：                                            │    │
│  │   maxMergeAtOnce = 10   // 一次最多 merge 10 个 Segment│   │
│  │   maxMergeAtOnceExplicit = 30 // Force Merge 时最多 30 │   │
│  │   floorSegmentMB = 2MB  // 小于 2MB 的 Segment 优先 merge│  │
│  │   segmentsPerTier = 10 // 每层最多 10 个 Segment     │    │
│  │                                                       │    │
│  │  算法：                                               │    │
│  │   ① 把所有 Segment 按 size 降序排序                 │    │
│  │   ② 从尾部（最小的）开始找可合并桶                   │    │
│  │   ③ 桶内 Segment 总 size ≈ 桶大小                   │    │
│  │   ④ 合并时考虑 deleted docs 比例                     │    │
│  │      超过 50% deleted 的 Segment 直接 merge（回收空间）│  │
│  └──────────────────────────────────────────────────────┘    │
│                                                               │
│  其他 MergePolicy（ES 可配置）：                              │
│   - LogByteSizeMergePolicy（按字节数）                         │
│   - LogDocMergePolicy（按文档数）                             │
│   - NoMergePolicy（禁止 Merge，罕见场景）                    │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Merge 执行流程

```java
// ★ Lucene + ES Segment Merge 执行流程（源码级）
//
// org.apache.lucene.index.IndexWriter（Merge 发起者）
// org.apache.lucene.index.MergeScheduler（并发控制）
// org.apache.lucene.index.SerialMergeScheduler（Lucene 默认）
// org.elasticsearch.index.merge.MergingScheduler（ES 层调度）
//
// // 步骤 1：触发 Merge 决策
// //    IndexWriter 每次 flush/commit 后，或后台定时：
// public MergeSpecification findMerges() {
//     MergeSpecification spec = new MergeSpecification();
//     for (OneMerge merge : mergePolicy.findMerges(segments)) {
//         spec.add(merge);
//     }
//     return spec;
// }
//
// // 步骤 2：执行 Merge（后台线程）
// public void merge(OneMerge merge) {
//     // ① 准备：打开源 Segment 读流
//     List<IndexReader> readers = new ArrayList<>();
//     for (SegmentInfo info : merge.segments) {
//         readers.add(openSegmentReader(info));
//     }
//     MultiReader multiReader = new MultiReader(readers);
//
//     // ② ★ 创建新 Segment（Merge 输出）
//     SegmentWriter writer = new SegmentWriter(newSegmentDir);
//
//     // ③ 遍历源 Segment 所有文档 → 写到新 Segment
//     //    这一步会：
//     //    - 合并倒排索引（多 FST → 新 FST）
//     //    - 合并 DocValues
//     //    - 过滤已删除文档（★ Merge 顺便回收 deleted docs）
//     for (int docID = 0; docID < multiReader.maxDoc(); docID++) {
//         if (multiReader.isDeleted(docID)) continue;  // 跳过已删除
//         writer.addDocument(multiReader.document(docID));
//         writer.addIndexes(multiReader.termVectors(docID));
//         writer.addDocValues(multiReader.docValues(docID));
//     }
//     writer.finish();  // 生成新 Segment 所有文件（.tis/.tip/.post/.nvd...）
//
//     // ④ 提交 Merge：更新 segments_N
//     //    // IndexWriter 原子切换：
//     //    //    旧 Segment A + B → 新 Segment C
//     //    //    写新的 segments_N 文件指向 C，不再引用 A/B
//     //    //    删除 A/B 的文件（异步）
//     commitMerge(merge, newSegmentInfo);
//
//     // ⑤ ★ Merge 也同时做 Deleted Docs 清理：
//     //    deleted docs 原来占空间（只是在 .del 文件标记）
//     //    Merge 时不写入 deleted docs → 回收磁盘空间
// }
//
// ★ Merge 和 Deleted Docs 的关系（面试高频）：
//   删除文档：不是真删，而是在 .del 文件标记为 deleted
//   查询时跳过 deleted docs
//   Merge 时真正把 deleted docs 过滤掉 → 回收空间
//   // deleted doc 比例过高 → 查询变慢（跳过的 doc 太多）
//   // Merge 顺便解决这个问题
//
```

### 3.3 ES Merge 调度与控制参数

```java
// ★ ES 层 Merge 调度与生产参数（面试高频）
//
// server/src/main/java/org/elasticsearch/index/merge/MergingScheduler.java
//
// // ES Merge 后台调度线程：
// public class MergingScheduler {
//     void run() {
//         while (!stopped) {
//             // 1. 检查是否需要 Merge
//             //    segments 数 > max_segment_count → 强制触发
//             //    deleted docs 比例 > 50% → 强制触发
//             //    有 pending merges → 继续
//
//             // 2. 控制 Merge 并发（避免 IO 打满）
//             //    ES 层限制：
//             //    // indices.store.throttle.max_bytes_per_sec = 20MB/s（默认）
//             //    // 大 Merge 也限速，防止影响在线查询
//             //    // 超过限速 → 线程 sleep
//
//             // 3. 提交 Merge 给 Lucene IndexWriter
//             indexWriter.merge(...);
//         }
//     }
// }
//
// ★ 关键配置参数（面试高频）：
//
//  | 参数                                          | 默认值  | 说明                                  |
//  |-----------------------------------------------|--------|--------------------------------------|
//  | merge.scheduler.max_thread_count             | Math.min(1, cores-1) | Merge 并发线程数（ES 自动调优）   |
//  | indices.store.throttle.max_bytes_per_sec      | 20MB/s | Merge 限速（防止打满磁盘 IO）       |
//  | merge.policy.max_merge_at_once                | 10     | 一次最多合并几个 Segment            |
//  | merge.policy.floor_segment                    | 2MB    | 小于这个值的 Segment 优先合并       |
//  | merge.max_segment_count                       | 无硬限 | 建议 Shard 保持 Segment < 100 个   |
//  | merge.policy.segments_per_tier                | 10     | 每层最多 Segment 数                  |
//
// ★ 生产调优建议（面试追问）：
//   高写入场景（日志/指标）：
//     - refresh_interval 调大（5s→30s）→ 减少 Segment 产生
//     - 增大 merge.max_merge_at_once（10→30）→ 每次合并更多
//     - 限速可稍放宽（20MB/s→50MB/s）如果磁盘 IO 充裕
//   高查询场景（搜索/电商）：
//     - Segment 数量 < 100 每个 Shard（太多 Segment 查询慢）
//     - 定期 Force Merge（curl /_forcemerge?max_num_segments=1）
//     - 但 Force Merge 是重操作 → 业务低峰执行
//
```

---

## 四、Index 生命周期 + 生产坑点（来自实战）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ES Index 初始化生命周期（生产坑点，Experience 提炼）       │
│                                                               │
│  错误顺序：                                                   │
│   DELETE /idx; PUT /idx { mappings:...}; // 先删再建        │
│   异步消费者开始写入 → mapping 还没生效 → all shards failed │
│                                                               │
│  ★ 正确幂等初始化顺序（面试高频）：                            │
│  ① 检查 Index 是否存在（HEAD /idx）                         │
│     如果不存在 → 进入第 2 步；存在 → 跳过                    │
│                                                               │
│  ② 创建 Index（PUT /idx）                                    │
│     settings（先）→ 分片数 + 副本数 + 刷新策略 + 分析器      │
│     mappings（后）→ 字段定义（基于实体注解或显式 JSON）     │
│     // ★ 顺序：settings 先（分片策略）→ mappings 后         │
│     // 因为 mapping 依赖分片策略（routing 字段）              │
│                                                               │
│  ③ 等待 Index Ready                                         │
│     // _cluster/health?wait_for_status=green&index=idx       │
│     // 异步消费者/初始化写入 → 门控：Index 未 ready 不启动   │
│                                                               │
│  ④ 写入 + 查询                                                │
│                                                               │
│  ★ 常见生产坑（Experience 提炼）：                             │
│   坑 1：创建时副本数 > 可用节点数                           │
│     → 分片长期 unassigned → all shards failed                │
│     → 单节点测试环境：副本数设 0                              │
│                                                               │
│   坑 2：mapping 和查询字段名不一致                           │
│     → 写入成功但查询不到（字段名不匹配）                     │
│     → mapping 来源统一（实体注解 / 外部 JSON 单一来源）     │
│                                                               │
│   坑 3：异步消费者在 Index ready 前启动                      │
│     → 写入失败 → 消费者卡壳 → 无数据                         │
│     → 消费者有"门控"能力：Index 未 ready 则 wait            │
│                                                               │
│   坑 4：删了重建作为常态                                      │
│     → 丢失历史数据 + 消费者重试风暴                          │
│     → 重建作为显式开关，不要默认行为                          │
│                                                               │
│   坑 5：分析器插件在开发/生产不一致                          │
│     → 开发能查到 / 生产查不到                                │
│     → 提供可切换 analyzer 的 settings profile               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、ES vs ClickHouse vs MongoDB 存储对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比：三大 NoSQL 存储引擎                          │
│  ┌──────────┬───────────────────┬──────────────┬──────────┐│
│  │ 维度      │ ES (Lucene Segment) │ ClickHouse    │ MongoDB ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 数据结构  │ 倒排索引 + DocValues │ MergeTree    │ BSON Doc ││
│  │           │ Segment Immutable│ Part Immutable│ 文档（可变）││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 写入模式  │ Append-Only Segment│ Append-Only Part│ 原地更新 ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 合并机制  │ ★ TieredMergePolicy│ ★ MergeTree    │ ❌ 无    ││
│  │           │ 回收 deleted    │ 后台 Merge    │         ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 删除语义  │ .del 文件标记 + Merge│ GC 标记 + Merge│ 原地删除  ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 列式存储  │ ★ DocValues（可选）│ ★ 原生列式    │ ❌ 文档式││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 倒排索引  │ ★ 核心（FST+Postings）│ ❌ 无        │ ❌ 无    ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 搜索能力  │ ★ 全文/模糊/Geo  │ ❌ 弱          │ 弱       ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 聚合能力  │ 中等              │ ★ 强（向量化）│ 弱       ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ Segment/Part 过多影响│ 查询慢（扫所有）│ 类似        │ ❌ 无   ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 面试高频  │ ★★★★★            │ ★★★★★        │ ★★★      ││
│  └──────────┴───────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   全文搜索 + 聚合 → **ES**（倒排核心，搜索生态好）           │
│   OLAP 数仓 → **ClickHouse**（原生列式，向量化强）          │
│   文档存储 + 灵活 Schema → **MongoDB**                       │
│   三者配合：MongoDB（主存）→ ClickHouse（数仓）→ ES（搜索）  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Lucene Segment 核心特性？| **不可变（Immutable）+ Append-Only + 后台 Merge 合并** |
| 2 | Segment 内部核心文件？| **.tis（倒排索引）+ .tip（FST 索引）+ .post（Postings List）+ .nvd/.ndv（DocValues）** |
| 3 | ★ 倒排索引结构？| **Term → Postings List**；两级：FST 快速定位 → .tis Term Dictionary |
| 4 | FST 是什么？| **Finite State Transducer**；压缩的有限状态自动机；Term → Long Pointer；内存比 HashMap 小 5-10 倍 |
| 5 | FST 查询复杂度？| O(term 长度)；与 term 总数无关（百万级 term 都快）|
| 6 | Postings List 压缩方式？| Varint + Delta 编码；分块存储（每块 256 docID）；Skip List 加速查询 |
| 7 | DocValues 做什么？| **列式存储；用于 Sort / Aggregation / Fielddata / 不参与 Text Query** |
| 8 | ★ ES Segment Merge 核心策略？| **TieredMergePolicy**；按大小分两层 Tier；小 Segment 优先合并；deleted docs > 50% 强制合并 |
| 9 | Merge 执行流程？| findMerges() → 打开源 Segment → 创建新 Segment → 遍历文档过滤 deleted → 写新文件 → 原子切换 segments_N |
| 10 | Merge 和 Deleted Docs 关系？| 删除只是 .del 标记；**Merge 时真正过滤掉 deleted docs 回收磁盘空间** |
| 11 | 为什么 Segment 不可变？| ① 写入不需要加锁 ② 查询时 Segment 内部可并行 ③ 崩溃安全（新 Segment 写完才切换）|
| 12 | ES Merge 并发控制？| MergeScheduler 控制线程数；indices.store.throttle.max_bytes_per_sec 限速（默认 20MB/s）|
| 13 | ★ 生产调优：高写入场景？| refresh_interval 调大（5s→30s）+ merge.max_merge_at_once 增大（10→30）+ 限速放宽 |
| 14 | ★ 生产调优：高查询场景？| Segment 数 < 100/Shard + 低峰 Force Merge |
| 15 | ES Index 幂等初始化顺序？| ① HEAD 检查存在 → ② PUT 创建（settings先+ mappings后）→ ③ 等 green → ④ 写查询 |
| 16 | ★ 常见生产坑（Experience 提炼）？| 副本数 > 节点数 → 分片未分配；mapping 和查询字段名不一致；异步消费者在 Index ready 前启动 |
| 17 | Force Merge 是什么？| 把多个 Segment 强制合并成一个；重 IO 操作；业务低峰执行 |
| 18 | Merge 后台线程在哪？| ES 层 MergingScheduler → Lucene IndexWriter.merge() → SerialMergeScheduler |
| 19 | 倒排查询时怎么加速？| FST 定位 Term Dictionary → Postings Skip List 跳块 → DisjunctionSumScorer 合并多 Term |
| 20 | ★ ES vs ClickHouse vs MongoDB 存储对比？| 见第五节对比表（10 维度）|
| 21 | 面试最高频考点排序？| **1. 倒排索引 + FST 2. TieredMergePolicy 算法 3. Segment 不可变特性 4. Merge 与 Deleted Docs 5. 生产坑点** |
| 22 | 学习路径？| Lucene90TermsReader → FST → Postings → TieredMergePolicy → IndexWriter.merge |
| 23 | 面试常见追问？| Segment 数量上限多少合适？| **< 100/Shard**；太多查询变慢；太少合并压力大 |
| 24 | 面试常见追问？| DocValues 和倒排什么区别？| 倒排：Term→Doc（Text Query）；DocValues：Doc→Value（Sort/Aggregation 列式） |
| 25 | 面试陷阱？| 说 Merge 会丢失数据 → ❌ Merge 是严格的复制+过滤 deleted，不丢活数据；说删除就是真删 → ❌ 是 .del 标记，Merge 才真正回收 |
| 26 | Lucene 为什么 Segment 不可变？| 面试高频答案：无需锁（高并发写入）、崩溃安全（新 Segment 写完才切换）、内存友好（mmap 直接读取）、合并简单（只追加不修改）|
| 27 | Refresh vs Merge 区别？| Refresh（秒级）：Memory Buffer → 新 Segment（让新数据可搜索）；Merge（分钟级）：多 Segment → 大 Segment（回收 deleted + 加速查询）|

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：org.apache.lucene.index.IndexWriter + TieredMergePolicy + Lucene90TermsReader + FST  
**面试考点：27 题**  
**差异化点**：倒排索引两级结构（FST→Term Dict→Postings）、TieredMergePolicy 算法、Merge 与 Deleted Docs 回收、Index 幂等初始化坑点（Experience 提炼）、ES vs ClickHouse vs MongoDB 存储三引擎对比
