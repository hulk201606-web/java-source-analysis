# Elasticsearch 7.x+ 源码深度解析（倒排索引 + BM25）

> **源码基线**：Elasticsearch 8.x + Lucene 9.x（最新稳定）  
> **核心仓库**：https://github.com/elastic/elasticsearch  
> **面试权重**：⭐⭐⭐⭐⭐（全文检索最高频）

---

## 一、Elasticsearch 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式搜索引擎（全文检索 + 实时分析）              │
│  底层：Apache Lucene（倒排索引）                             │
│                                                               │
│  ES vs Solr vs OpenSearch：                                   │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │  Elasticsearch │  Apache Solr │ OpenSearch    │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 出处       │ Elastic (BA)  │ Apache       │ AWS / Elasticfork││
│  │ 架构       │ 分布式原生    │ 分布式（CloudSolr）│ 分布式 │   │
│  │ 易用性     │ ★★★★★ 最高  │ ★★★ 中等    │ ★★★★ 高     │   │
│  │ 生态       │ K8s/ELK     │ SolrCloud    │ AWS OpenSearch│   │
│  │ 许可证     │ Elastic License │ Apache v2.0 │ Apache v2.0  │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ ES 7.x+ 核心变化：移除 Type（每个 Index 一个 Type）+ 默认开启 BM25 算分│
│ ★ OpenSearch：AWS 维护的 ES 开源分支（ES 官方 Elastic License 限制）│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  ES 集群架构                                                 │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐     │
│  │ ES Cluster                                          │     │
│  │  ┌──────────────┬──────────────┬──────────────┐     │     │
│  │  │ Master Node  │ Data Node 1  │ Data Node 2  │     │     │
│  │  │ （元数据/调度）│ （存储 + 计算）│ （存储 + 计算）│     │     │
│  │  │   └─ Data Role│   └─ 2 Shards │   └─ 2 Shards │     │     │
│  │  └──────────────┴──────────────┴──────────────┘     │     │
│  │                                                     │     │
│  │  Index = N Primary Shards（默认 5）                   │     │
│  │        + M Replica Shards（默认 1）                  │     │
│  │  Primary Shard 存在某个 Data Node（通过 Raft 调度） │     │
│  │  Replica Shard 存在不同 Data Node                    │     │
│  └─────────────────────────────────────────────────────┘     │
│                                                               │
│  ★ ES 7.x 默认 Primary Shard = 5，无法修改！（只能 Index 创建时指定）│
│  ★ Index 是 Shard 的逻辑分组；Shard 是 Lucene 实例           │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 倒排索引（★★★★★ Lucene 灵魂）

```java
// ★ 倒排索引（Inverted Index）——搜索引擎的灵魂
//   正向索引：doc_id → content（顺序是文档→词）
//   倒排索引：term → posting list（词→包含这个词的文档列表）
//
// 正向索引（Lucene Stored Fields）：
//   doc_001: { name: "张三", city: "北京", age: 20 }
//   doc_002: { name: "李四", city: "上海", age: 25 }
//   doc_003: { name: "王五", city: "北京", age: 30 }
//
// 倒排索引（Lucene Posting List）：
//   "张三" → [doc_001]                 # 出现一次
//   "北京" → [doc_001, doc_003]        # 出现在 doc_001 和 doc_003
//   "上海" → [doc_002]
//   20     → [doc_001]
//   25     → [doc_002]
//   30     → [doc_003]
//
// ★ 倒排索引查询原理：
//   查询：city = "北京" AND age > 25
//   1. 查倒排索引："北京" → [doc_001, doc_003]
//   2. 查倒排索引：age 范围 [26-+∞) → [doc_003]
//   3. 交集：[doc_001, doc_003] ∩ [doc_003] = [doc_003]
//   4. 拿 doc_003 正向索引内容返回
//
// ★ 为什么倒排索引快？
//   不需要遍历所有文档 → 直接定位包含查询词的文档
//   → O(K) 时间（K 是包含查询词的文档数）
//   → 不是 O(N)（N 是总文档数）
//
// ★ 和 MySQL B+Tree 索引对比：
//   MySQL 联合索引：city + age → B+Tree 有序 → 范围查询快
//   ES 倒排索引：city 和 age **各自一个倒排索引** → 先各查再交集
//   → ES 多字段组合查询的性能 **取决于倒排索引交集大小**
//   → MySQL B+Tree 组合索引 → 直接走树 → 更稳定
```

### 2.2 BM25 算分（★★★★★ ES 7.x 默认）

```java
// ★ BM25 是 ES 7.x+ 默认相关性算分算法
//   替代了旧版 TF-IDF（BM25 更精确，特别是长文档场景）
//
// BM25 公式（简化版）：
//   score = IDF * TF * K1 / (TF + K1 * (1 - B + B * dl / avg_dl))
//
// 参数含义：
//   TF（Term Frequency）：词出现的频率（文档里这个词出现几次）
//   IDF（Inverse Document Frequency）：
//     log((N - n + 0.5) / (n + 0.5) + 1)
//     → N = 总文档数，n = 包含这个词的文档数
//     → 词越稀有（n 小）→ IDF 越高 → 越重要
//   dl（Document Length）：当前文档长度
//   avg_dl：所有文档平均长度
//   K1（默认 1.2）：控制 TF 饱和速度
//   B（默认 0.75）：控制文档长度归一化（长文档惩罚）
//
// ★ 直观理解 BM25：
//   ① TF 越高越好（文档里查询词出现多）→ 但会饱和（不是线性）
//   ② IDF 越高越好（查询词稀有，不是所有文档都有）
//   ③ 长文档惩罚（B 参数）→ 长文档 TF 被稀释
//
// ★ BM25 vs TF-IDF：
//   TF-IDF：TF 线性（出现越多分越高）→ 长文档 TF 特别高（不公平）
//   BM25：**TF 饱和**（出现 10 次和 20 次区别不大）+ **长文档惩罚**
//   → BM25 更适合现代搜索引擎（Google 也用类似算法）
//
// ★ ES 算分流程：
//   Query（"北京" AND age>25）
//     → 倒排索引查 posting list
//     → 每个文档算 BM25 分
//     → 取 Top N（默认 10）
//     → 返回结果
```

### 2.3 Lucene Segment 结构（★ 核心）

```java
// ★ Lucene Segment = ES Shard 内部的存储单元
//
// Segment 核心特点：
//   ① Immutable（不可变）→ 一旦创建就不再修改（新数据新建 Segment）
//   ② Append Only（追加写）→ 写只新建 Segment，不修改旧的
//   ③ Merge（后台合并）→ 后台 Merge 线程把多个小 Segment 合并成大 Segment
//
// Segment 文件结构（面试高频！）：
//   .segments   → Segment 元信息
//   .cfe / .cfs → Segment 完整信息（Compound File）
//   .doc        → 存储的文档内容（Stored Fields）
//   .fdt / .fdx → Stored Fields 索引 + 数据
//   .nrm        → Norm（BM25 归一化参数：dl, avg_dl）
//   .prx        → Proximity（词位置，Phrase Query 用）
//   .pos        → Positions（词在文档中的位置）
//   .tin / .tip → Term Dictionary（词典：所有出现过的词）
//   .tmd        → Term Metadata（每个词的统计信息：DocFreq 等）
//   .tim        → Term Info（每个词在倒排索引里的位置）
//
// ★ Segment Merge 为什么重要？
//   不断写入 → 产生大量小 Segment → 查询时要扫所有 Segment → 慢！
//   Merge 线程把小 Segment 合并成大 Segment → 查询快
//   → 类似 ClickHouse MergeTree / Kafka Log Compaction
//
// ★ 删除是怎么回事？
//   Lucene 没有物理删除！
//   删除时只在 Segment 上加一个 deleted bitset（标记哪些 doc 被删了）
//   → 查询时跳过被删的 doc
//   → Merge 时才真正清理（把没被删的 doc 重写新 Segment）
//   → 删除不释放磁盘空间（等 Merge）
```

### 2.4 ES Search 流程

```java
// ★ ES Search 完整流程（面试高频）
//
// GET /my_index/_search?q=北京&sort=age:desc&size=10
//
// ① 客户端请求 ES Node（Coordinating Node）
//    → 选 Node（任何 Node 都可以接）
//
// ② 解析 Query + 路由到 Shard
//    → 查 Index 元数据：my_index 有 5 个 Primary Shard
//    → 这个查询要扫所有 Shard（没有 routing 参数）
//    → 如果指定 routing=user:1 → 只扫对应 Shard（性能好）
//
// ③ Scatter-Gather（Scatter Phase 散射）
//    → Coordinating Node 并发向 5 个 Primary Shard 发查询
//    → 每个 Shard 内部：
//      a. 遍历所有 Lucene Segment
//      b. 倒排索引查 posting list
//      c. BM25 算分
//      d. 取 Top 10 * 1.5（足够大的候选集合）
//
// ④ Gather Phase（聚合）
//    → Coordinating Node 收到 5 个 Shard 的结果
//    → 合并所有候选（每个 Shard 15 个 → 75 个）
//    → 重新排序（sort=age:desc）
//    → 取最终 Top 10
//    → Fetch Phase（必要时拉完整文档）
//
// ★ 为什么 Top 10 * 1.5？
//   每个 Shard 只返回自己的 Top 15
//   → 可能全局 Top 10 不在每个 Shard 的 Top 10 里
//   → 需要 Over-Fetch 候选集
//   → 如果要精确全局排序：track_total_hits + 全量返回（不推荐）
```

### 2.5 Mapping + 字段类型（★ 面试高频）

```json
// ★ ES 7.x+ Mapping（字段类型定义）
//
// 关键变化：7.x 移除了 Type！（每个 Index 只允许一个 Type）
// 为什么移除？因为：
//   不同 Type 的同名字段在同一个 Lucene 索引里
//   → Mapping 冲突（字段类型不能不一致）
//   → 官方建议：每个 Type 用单独的 Index
//
// 核心字段类型：
//   text      → 分词存储（全文检索用）→ 倒排索引
//   keyword   → 不分词（精确匹配/排序/聚合）→ 倒排索引（不分词）
//   numeric   → long/integer/double/float
//   date      → 日期（支持范围查询 + 时区）
//   boolean   → true/false
//   geo_point → 地理位置（经纬度，距离查询）
//   geo_shape → 地理形状（多边形，包含查询）
//   nested    → 嵌套对象（数组里每个元素独立索引）
//   object    → 对象（扁平化索引，和 nested 不同）
//
// ★ text vs keyword（面试必问！）
//   name: "张三是中国人"
//   text → 分词：["张三", "中国人", "中国", "人"] → 全文检索 "张三" 能查到
//   keyword → 不分词：["张三是中国人"] → 精确匹配 "张三是中国人" 才能查到
//
// ★ nested vs object：
//   object（默认）：扁平化索引
//     { user: [{name:"张三", age:20}, {name:"李四", age:25}] }
//     → 索引成：user.name = ["张三", "李四"], user.age = [20, 25]
//     → 查询 "name=张三 AND age=20" 可能错配（张三 age 25，李四 age 20）
//
//   nested：每个对象独立索引
//     → 每个 {name:"张三", age:20} 是独立文档
//     → 查询 name=张三 AND age=20 → 精确匹配
//     → 但查询慢（需要 Nested Query Join）
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 倒排索引是什么？| **term → posting list**（词→包含这个词的文档列表）；正向索引是 doc→content；搜索引擎灵魂，O(K) 时间查询 |
| 2 | ES 7.x 默认算分？| **BM25**（替代旧 TF-IDF）→ TF 饱和 + 长文档惩罚 + IDF 稀有度；Google 也用类似算法 |
| 3 | Segment 三大特性？| **Immutable（不可变）+ Append Only（追加写）+ Merge（后台合并）**；删除只加标记，Merge 时真正清理 |
| 4 | ES 搜索流程？| **Scatter-Gather**：Coordinating Node 并发查所有 Shard（Top 10 * 1.5 Over-Fetch）→ 合并 → 全局排序 → 返回 Top 10 |
| 5 | ES 7.x 核心变化？| **移除 Type 概念**（每个 Index 只允许一个 Type）；BM25 默认；支持 data_stream（时间序列）|
| 6 | text vs keyword？| **text：分词（全文检索，倒排索引存分词）**；**keyword：不分词（精确匹配/排序/聚合，倒排索引存原文）** |
| 7 | nested vs object？| object：扁平化索引 → 查询可能错配；**nested：每个对象独立索引 → 精确匹配，但查询慢** |
| 8 | Shard 是什么？| **Index 内部的 Lucene 实例**；ES 7.x 默认 Primary Shard = 5（Index 创建时指定，之后不能改）|
| 9 | ES Merge 怎么触发？| 后台线程；根据 Segment 数量/大小/文档数；`index.merge.policy` 配置；大 Segment Merge 会占 CPU/IO |
| 10 | ES vs MySQL 全文搜索？| ES：**倒排索引 + BM25 + 分布式**；MySQL：FULLTEXT 索引（简单 BM25）+ 单节点；复杂搜索用 ES |
| 11 | ES 热更新问题？| Segment Immutable → 新写建新 Segment → Merge 合并 → **实时查询**（不需要倒排索引重建）|
| 12 | ES 查询路由？| routing 参数 → 只查对应 Shard；不知道 routing → 扫所有 Shard；**精确 routing 性能好** |
| 13 | ES Mapping 动态 vs 显式？| 动态（dynamic=true）：自动推断字段类型；生产推荐显式 Mapping（控制分词器/类型）|
| 14 | ES 分词器？| 默认 standard（英文分词 + 中文单字）；生产常用 IK（中文分词）/ HanLP / SmartCN；需要插件 |
| 15 | ES vs ClickHouse？| ES：**全文搜索 + 倒排索引 + BM25**；ClickHouse：**OLAP 分析 + 列式存储 + MergeTree**；常组合用 |
| 16 | ES 删除不释放空间？| 是的！删除只加 deleted bitset → 等 Segment Merge 时真正清理 → 大表删除后要 Force Merge |
| 17 | ES OpenSearch？| AWS 维护的 ES 开源分支（ES 官方 Elastic License 有云厂商限制）→ Apache v2.0 |
| 18 | ES 学习路径？| Docker 起单节点 → 建 Index + Mapping（text/keyword）→ 写入 → Query DSL → Aggregation → 集群 + Kibana |
| 19 | ES Top Hits Over-Fetch？| 每个 Shard 返回 **Top N * 1.5** → 避免全局 Top 漏掉；精确排序要 track_total_hits |
| 20 | ES 为什么不支持事务？| 倒排索引结构决定了：写入 Segment（Append Only）+ 查询时多 Segment 合并 → 不支持 ACID；适合最终一致性场景 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Elasticsearch 8.x + Lucene 9.x  
**面试考点：20 题**
