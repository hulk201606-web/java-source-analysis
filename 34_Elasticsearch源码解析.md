# Elasticsearch 源码深度解析（分布式搜索引擎）

> **源码基线**：Elasticsearch 8.15.x（最新稳定）  
> **核心仓库**：https://github.com/elastic/elasticsearch  
> **底层依赖**：Apache Lucene 9.x  
> **面试权重**：⭐⭐⭐⭐⭐（搜索高频）

---

## 一、ES 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式搜索引擎（Lucene + 分布式协调）              │
│  为什么快？倒排索引（关键词 → 文档列表）+ 内存缓存 + 分布式并行搜索│
│                                                               │
│  核心概念：                                                    │
│  Document（文档）→ Index（索引）→ Shard（分片）→ Segment（段）  │
│  一个 Index 默认 5 Primary Shards + 每个 1 Replica             │
│  每个 Shard = 一个 Lucene Index（物理存储单元）                │
│  Lucene Index = N 个 immutable Segment（段）                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  集群节点角色                                                  │
│                                                               │
│  Master Node：选主 + 管理集群状态（Index 创建/删除/分配 Shard） │
│  Data Node：存储数据 + 执行搜索                                │
│  Coordinating Node：路由请求（数据查询时决定查哪些 Shard）       │
│  Ingest Node：数据预处理（Painless Script 管道）              │
│  ML Node：机器学习（异常检测/预测）                            │
│                                                               │
│  ★ 节点合并：ES 7.0+ 默认 Master + Data 角色合并              │
│  → 小集群所有节点既是 Master 候选又是 Data Node                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  写入链路（Index Document）                                    │
│                                                               │
│  Client → Coordinating Node → 根据路由算目标 Shard            │
│    → Shard：先写 TransLog（事务日志，顺序追加）                │
│    → 再写 Lucene Index（内存 Buffer）                         │
│    → refresh（默认 1s）→ Buffer 刷成新 Segment → 可搜索      │
│    → flush（默认 30min / TransLog 满 512MB）→ Segment 落盘    │
│    → merge（后台）→ 小 Segment 合并成大 Segment（删除标记也才清理）│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  搜索链路（Search）                                            │
│                                                               │
│  Client → Coordinating Node                                   │
│    → Scatter-Gather：并行发请求给所有相关 Shard（本地 1 + 远程 N-1）│
│    → Shard 内部：                                            │
│      → QueryParser：解析 DSL 查询（Match/Term/Range/Bool）    │
│      → 倒排索引查 Posting List（关键词 → 文档 ID 列表）        │
│      → 算分（TF-IDF / BM25）                                   │
│      → Top K 结果返回                                         │
│    → Coordinating Node：合并所有 Shard 结果 → 全局 Top K      │
│    → 返回给 Client                                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Lucene IndexWriter 写入核心

```java
// ★ ES Shard 底层是 Lucene IndexWriter
//
// 写入流程：
//   IndexWriter.addDocument(Iterable<? extends IndexableField> doc)
//     ① Tokenization（分词）→ StandardAnalyzer / IKAnalyzer
//     ② Invert（倒排）→ 生成 Posting（Term → DocID → Position → Offset）
//     ③ 写入内存 Segment（RAMBuffer）
//
// ★ Lucene Segment 关键特性：
//   - Immutable（不可变）：写入后不修改
//   - Append Only（追加写）：新文档进新 Segment
//   - 好处：无锁！并发搜索不阻塞写入
//
// ★ Posting List 结构（倒排索引核心）：
//   Term（关键词）→ Posting（倒排列表）
//                  ├── DocID（文档 ID）
//                  ├── Freq（词频）
//                  ├── Pos（位置，用于短语查询）
//                  └── Payload（附加信息）
//
// ★ 为什么搜索快？
//   内存中 Posting List 已经是排序好的 DocID 列表
//   → Term 查询 → 直接跳转到 DocID O(1) 定位
//   → 比 MySQL B+Tree 走索引还要快！
```

### 2.2 Merge 合并（★ 面试高频）

```java
// ★ Merge 是 Lucene/ES 性能关键机制！
//
// 问题：持续写入产生大量小 Segment → Segment 太多 → 搜索变慢（每个 Segment 都要查）
// 方案：后台 MergeService 定期把小 Segment 合并成大 Segment
//
// Merge 做了什么：
//   ① 遍历所有 Segment
//   ② 读小 Segment 的 Posting List
//   ③ 重新排序 DocID + 合并
//   ④ 写成一个大 Segment
//   ⑤ 删除旧 Segment
//   ★ 删除的文档也在这一步被真正清理（之前只是 marked as deleted）
//
// ★ Merge 策略（ES 内置 4 种）：
//   ① TieredMergePolicy（★ 默认）
//     → 按 Segment 大小分层（默认每层 10 Segment 合并）
//     → 每层最多 50MB
//   ② LogByteSizeMergePolicy
//   ③ LogDocMergePolicy
//   ④ NoMergePolicy（不合并，测试用）
//
// ★ Merge 对查询的影响：
//   Merge 占 IO + CPU → 可能影响查询延迟
//   ES 自动 throttling（限流）→ 合并速度可配置（merge.scheduler.max_thread_count）
//   生产建议：SSD 上设置 4-6 个 merge 线程
//
// ★ 面试高频问：为什么删除文档不会立即释放磁盘空间？
//   → 因为 Segment 不可变 → 删除只是 marked → 要等 Merge 才真正清理
//   → 这就是 Delete 后磁盘空间不降的原因！
```

### 2.3 refresh vs flush（★★★ 面试高频）

```java
// ★ refresh（近实时搜索关键）：
//   - 间隔：默认 1s（index.refresh_interval）
//   - 作用：**把内存 Buffer 刷成新 Segment → 变成可搜索**
//   - 操作：内存→内存（还没落盘！）
//   - 性能：快！因为只是创建新 Segment 的内存视图
//
// ★ flush（持久化）：
//   - 间隔：默认 30min / TransLog 满 512MB（index.translog.flush_threshold_size）
//   - 作用：**把 Segment 数据刷到磁盘**
//   - 触发：TransLog 过大（宕机重启后要回放太多 TransLog 恢复状态）
//
// ★ TransLog（事务日志，类似 MySQL binlog）：
//   - 写入时先写 TransLog → 再写 Lucene Index
//   - 宕机重启：从 TransLog 恢复未 flush 的数据
//   - 保证数据不丢（但 Segment 还没落盘的话，恢复后 Lucene Index 状态未知 → 用 TransLog 补齐）
//
// ★ 三者顺序：
//   写入 → ① TransLog 追加（保证不丢）
//        → ② Lucene 内存 Buffer 追加（写索引）
//        → ③ refresh（1s）→ Segment 内存视图变可搜索
//        → ④ flush（30min）→ Segment 数据落盘
//        → ⑤ merge（后台）→ 小 Segment 合并成大 Segment
```

### 2.4 倒排索引 vs 正排索引

```
// ★ 面试高频对比题：
//
// 正排索引（Forward Index）：
//   DocID → Term 列表
//   → MySQL InnoDB（聚簇索引）→ 按主键查快
//   → 适合：按文档 ID 查内容
//
// 倒排索引（Inverted Index）：
//   Term → DocID 列表（Posting List）
//   → ES/Lucene 核心 → 按关键词查文档快
//   → 适合：全文搜索、关键词匹配
//
// ★ 为什么 ES 搜索比 MySQL LIKE 快？
//   MySQL LIKE '%hello%'：全表扫描（B+Tree 索引无法前缀不匹配）
//   ES Term 查询：直接从 Posting List 拿到所有包含 "hello" 的 DocID（O(1) 定位）
//
// ★ DocValues（正排索引在 Lucene 里的实现）：
//   ES 还会维护 DocValues（列式存储）
//   → 用于 Sort / Aggregation（不需要反序列化整个 Doc）
//   → 列式压缩效率高
```

### 2.5 相关性算分（TF-IDF vs BM25）

```
// ★ ES 7.x 之前用 TF-IDF，7.x 之后改成 BM25（默认！）
//
// TF-IDF：
//   TF（Term Frequency）：词在文档里出现的次数 / 文档总词数
//   IDF（Inverse DF）：log(总文档数 / 包含该词的文档数)
//   分数 = TF × IDF
//
// BM25（BM 是 Best Match 的缩写）：
//   b = 0.75（文档长度归一化因子）
//   k1 = 1.2（TF 饱和参数）
//   比 TF-IDF 好在哪里？
//   ① TF 饱和（词出现 10 次和 100 次分数差别不大）
//   ② 文档长度归一化（长文档 TF 被打折）
//
// ★ ES 可以通过 similarity 参数切换：
//   similarity: BM25（默认）/ classic（TF-IDF）/ boolean（只返回匹配文档不打分）
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ES Shard 底层是什么？| **一个 Lucene Index**（物理存储单元）；默认 5 Primary Shards + Replica |
| 2 | Lucene Segment 为什么不可变？| Immutable 无锁 → 并发搜索不阻塞写入；**删除文档先 marked，等 Merge 才真正清理** |
| 3 | refresh vs flush？| refresh（默认 1s）：**内存 Buffer → 内存 Segment 视图变可搜索**；flush（默认 30min）：**Segment 数据落盘** |
| 4 | TransLog 作用？| **事务日志** → 写入时先写 TransLog 再写 Lucene Index → 宕机重启从 TransLog 恢复 |
| 5 | Merge 合并策略？| **TieredMergePolicy（默认）** → 按 Segment 大小分层，每层最多 50MB；后台自动 throttling 限流 |
| 6 | 为什么删除不释放磁盘？| Segment 不可变 → 删除只是 marked → **要等 Merge 才真正清理** |
| 7 | 倒排索引核心？| **Term（关键词）→ Posting（DocID + Freq + Pos + Payload）**→ 关键词直接定位文档 ID O(1) |
| 8 | 正排索引 vs 倒排索引？| 正排 DocID→Term（MySQL B+Tree 按主键查快）；倒排 Term→DocID（ES 按关键词查快）；**DocValues 是正排在 Lucene 的列式存储实现** |
| 9 | ES 7.x 默认算分算法？| **BM25**（之前是 TF-IDF）；BM25 比 TF-IDF 好：TF 饱和 + 文档长度归一化 |
| 10 | 跨 Shard 查询原理？| **Scatter-Gather**：Coordinating Node 并行发请求给所有相关 Shard → 每个 Shard 返回 Top K → 合并 → 全局 Top K |
| 11 | Shard 数量怎么选？| 按文档量：**单个 Shard 建议 10-50GB**；大集群建议每节点不超过 3 个 Shard；Primary Shards 创建后不能改！ |
| 12 | Replication 机制？| **默认每个 Primary 有 1 Replica**；Replica 同步 TransLog + Lucene Index；Primary 挂了 Replica 自动提升 |
| 13 | 数据更新为什么慢？| Update 不是原地修改 → **先 Delete 旧 Doc（marked）→ Insert 新 Doc** → Merge 才真正清理旧 Doc → 频繁 Update 会产生大量小 Segment |
| 14 | ES vs MySQL？| ES：**全文搜索 + 分布式 + 实时性（近实时）**；MySQL：结构化数据 + 事务 + Join + 精确查询 |
| 15 | ES vs Solr？| 都是基于 Lucene；ES：分布式 + REST API + 社区活跃（国内主流）；Solr：功能完善但分布式要自己配 |
| 16 | Keyword vs Text？| **Keyword 不分词**（精确匹配、排序、聚合）；**Text 分词**（全文搜索）；面试高频！ |
| 17 | KNN 向量搜索？| ES 8.x 内置 dense_vector 类型 + HNSW 算法 → 支持相似度搜索（AI 场景） |
| 18 | 深分页问题？| `FROM 10000, 10` → 每个 Shard 都要查前 10010 条 → 内存合并 → **用 Search After（游标分页）替代** |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Elasticsearch 8.15.x + Apache Lucene 9.x  
**面试考点：18 题**
