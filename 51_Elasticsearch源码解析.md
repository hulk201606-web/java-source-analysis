# Elasticsearch 8.x 源码深度解析（倒排索引 + BM25）

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
│  ES 核心组件（面试高频）：                                    │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │ Master Node    │ Data Node      │ Coordinating Node  │     │
│  │ 元数据/调度   │ 存储+计算    │ 查询协调/转发      │     │
│  │ 必须选主       │ 可选           │ 可选（任何 Node 都行）│   │
│  └──────────────┴──────────────┴──────────────┘             │
│                                                               │
│ ★ Index = N Primary Shards（默认 5）+ M Replica Shards（默认 1）│
│ ★ **ES 7.x 移除 Type 概念**（每个 Index 只允许一个 Type）     │
│ ★ **ES 7.x 默认 BM25 算分**（替代旧 TF-IDF）               │
│                                                               │
│ 索引 vs Shard vs Segment：                                   │
│   Index → Primary Shard × N（每个 Shard 是一个 Lucene 实例） │
│   Shard → Lucene Segment × N（多个 Immutable 不可变 Segment）│
│   Segment → 倒排索引 + 正向索引 + Stored Fields              │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 倒排索引（★★★★★ Lucene 灵魂）

```java
// ★ 倒排索引（Inverted Index）——搜索引擎的灵魂（面试必背！）
//
// 正向索引（doc_id → content）：
//   doc_001: { name: "张三", city: "北京", age: 20 }
//   doc_002: { name: "李四", city: "上海", age: 25 }
//   doc_003: { name: "王五", city: "北京", age: 30 }
//
// ★ 倒排索引（term → posting list）：
//   "北京" → [doc_001, doc_003]    ← 出现在哪些文档
//   "上海" → [doc_002]
//   "张三" → [doc_001]
//
// 为什么快？
//   查询 city="北京" → 直接拿倒排索引 posting list [doc_001, doc_003]
//   → 不需要遍历所有文档！O(K) vs O(N)
//
// ★ 和 MySQL B+Tree 索引对比：
//   MySQL：联合索引 city+age → B+Tree 有序 → 范围查询快
//   ES：city 和 age **各自一个倒排索引** → 先各查 posting list → 再交集
//   → ES 多字段组合查询性能取决于倒排索引交集大小
```

### 2.2 BM25 算分（★★★★★ ES 7.x 默认）

```java
// ★ BM25 是 ES 7.x+ 默认相关性算分（面试高频！替代 TF-IDF）
//
// BM25 公式（简化版）：
//   score = IDF * TF * K1 / (TF + K1 * (1 - B + B * dl/avg_dl))
//
// 参数含义：
//   TF（Term Frequency）：词出现频率（文档里这个词出现几次）
//   IDF（Inverse Document Frequency）：
//     log((N - n + 0.5) / (n + 0.5) + 1)
//     → N=总文档数，n=包含这个词的文档数
//     → **词越稀有（n 小）→ IDF 越高 → 越重要**
//   dl（Document Length）：当前文档长度
//   avg_dl：所有文档平均长度
//   K1（默认 1.2）：控制 TF 饱和速度
//   B（默认 0.75）：控制文档长度归一化（长文档惩罚）
//
// ★ BM25 vs TF-IDF（为什么换？）：
//   TF-IDF：TF 线性增长（出现越多分越高）→ 长文档 TF 特别高（不公平）
//   BM25：**TF 饱和**（出现 10 次和 20 次区别不大）+ **长文档惩罚**
//   → BM25 更精确（Google 也用类似算法）
```

### 2.3 Lucene Segment（★ 面试高频）

```java
// ★ Lucene Segment = ES Shard 内部的存储单元
//
// Segment 三大特性（面试必记！）：
//   ① Immutable（不可变）→ 创建后不再修改
//   ② Append Only（追加写）→ 写只新建 Segment
//   ③ Merge（后台合并）→ 小 Segment 后台合并成大 Segment
//
// ★ Segment 文件结构（面试高频）：
//   .segments   → Segment 元信息
//   .cfe / .cfs → Segment 完整信息（Compound File）
//   .doc        → 存储的文档内容
//   .fdt / .fdx → Stored Fields 索引 + 数据
//   .nrm        → Norm（BM25 归一化参数：dl, avg_dl）
//   .prx        → Proximity（词位置，Phrase Query 用）
//   .tin / .tip → **Term Dictionary（词典：所有出现过的词）**
//   .tim        → Term Info（每个词在倒排索引里的位置）
//
// ★ 删除是怎么回事？
//   Lucene **没有物理删除**！
//   删除时加 deleted bitset（标记哪些 doc 被删了）
//   → 查询时跳过被删的 doc
//   → Merge 时才真正清理（把没被删的 doc 重写新 Segment）
//   → 删除**不释放磁盘空间**（等 Merge）→ 需要 Force Merge
```

### 2.4 ES Search 流程（Scatter-Gather）

```java
// ★ ES Search 完整流程（面试高频 Scatter-Gather）
//
// GET /my_index/_search?q=北京&sort=age:desc&size=10
//
// ① Coordinating Node 接收请求
//
// ② 路由到 Shard：
//    Index 有 5 个 Primary Shard → 无 routing 参数 → **扫所有 5 个**
//    → 指定 routing=user:1 → 只扫对应一个 Shard（性能好）
//
// ③ Scatter Phase（散射）：
//    并发向所有 Primary Shard 发查询
//    每个 Shard 内部：
//    a. 遍历所有 Lucene Segment
//    b. 倒排索引查 posting list
//    c. BM25 算分
//    d. 取 **Top N × 1.5**（Over-Fetch，避免全局 Top 漏掉）
//
// ④ Gather Phase（聚合）：
//    Coordinating Node 合并所有 Shard 的候选
//    → 重新全局排序（sort=age:desc）
//    → 取最终 Top 10
//    → Fetch Phase（必要时拉完整文档）
//
// ★ 为什么 Top N × 1.5？
//   每个 Shard 只返回自己的 Top 15
//   → 全局 Top 10 可能不在每个 Shard 的 Top 10 里
//   → 需要 Over-Fetch 足够候选
//   → 精确全局排序用 track_total_hits + 全量返回（不推荐）
```

### 2.5 Mapping + 字段类型

```json
// ★ ES 7.x+ Mapping（字段类型）核心变化
//
// 7.x 核心变化：**移除 Type 概念**（每个 Index 只允许一个 Type）
// 为什么？不同 Type 的同名字段在同一个 Lucene 索引里 → Mapping 冲突
// 官方建议：每个 Type 用单独的 Index
//
// ★ 核心字段类型（面试高频）：
//
//   text      → 分词存储（全文检索）→ 倒排索引存分词
//   keyword   → 不分词（精确匹配/排序/聚合）→ 倒排索引存原文
//   numeric   → long/integer/double/float
//   date      → 日期（支持范围查询 + 时区）
//   boolean   → true/false
//   geo_point → 地理位置（经纬度，距离查询）
//   nested    → 嵌套对象（每个元素独立索引，避免扁平化错配）
//   object    → 对象（扁平化索引，nested 的反面）
//
// ★ text vs keyword（面试必问！）：
//   name: "张三是中国人"
//   text → 分词：["张三", "中国人", "中国", "人"] → 全文检索 "张三" 能查到
//   keyword → 不分词：["张三是中国人"] → 精确匹配整个字符串才能查到
//
// ★ nested vs object（面试必问！）：
//   object（默认）：扁平化索引 → 查询可能错配
//     { user: [{name:"张三", age:20}, {name:"李四", age:25}] }
//     → 索引成：user.name=["张三","李四"], user.age=[20,25]
//     → 查询 "name=张三 AND age=20" 可能错配（张三 age 25？李四 age 20？）
//
//   nested：每个对象独立索引 → 精确匹配 → 但查询慢
//     → 每个 {name:"张三", age:20} 是独立文档
//     → 查询 name=张三 AND age=20 → 精确匹配
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 倒排索引是什么？| **term → posting list**（词→包含这个词的文档列表）；正向索引是 doc→content；搜索引擎灵魂，O(K) 时间查询 |
| 2 | ES 7.x 默认算分？| **BM25**（替代旧 TF-IDF）→ TF 饱和 + 长文档惩罚 + IDF 稀有度；Google 也用类似算法 |
| 3 | Segment 三大特性？| **Immutable（不可变）+ Append Only（追加写）+ Merge（后台合并）**；删除只加标记，Merge 时真正清理 |
| 4 | ES 搜索流程？| **Scatter-Gather**：Coordinating Node 并发查所有 Shard（Top N × 1.5 Over-Fetch）→ 合并 → 全局排序 → 返回 |
| 5 | ES 7.x 核心变化？| **移除 Type 概念**（每个 Index 只允许一个 Type）；BM25 默认；data_stream 时间序列 |
| 6 | text vs keyword？| **text：分词（全文检索）**；**keyword：不分词（精确匹配/排序/聚合）** |
| 7 | nested vs object？| object：扁平化索引 → 查询可能错配；**nested：每个对象独立索引 → 精确匹配，但查询慢** |
| 8 | Shard 是什么？| **Index 内部的 Lucene 实例**；ES 7.x 默认 Primary Shard = 5（Index 创建时指定，之后不能改）|
| 9 | ES Merge 触发？| 后台线程；根据 Segment 数量/大小/文档数；大 Merge 占 CPU/IO → 夜间执行 |
| 10 | ES vs MySQL 全文搜索？| ES：**倒排索引 + BM25 + 分布式**；MySQL：FULLTEXT（简单 BM25）+ 单节点；复杂搜索用 ES |
| 11 | ES 热更新问题？| Segment Immutable → 新写建新 Segment → Merge 合并 → **实时查询**（不需要倒排索引重建）|
| 12 | ES 查询路由？| routing 参数 → 只查对应 Shard；不知道 routing → 扫所有 Shard；**精确 routing 性能好** |
| 13 | ES 分词器？| 默认 standard（英文分词 + 中文单字）；生产常用 IK（中文分词）/ HanLP；需要插件 |
| 14 | ES vs ClickHouse？| ES：**全文搜索 + 倒排索引 + BM25**；ClickHouse：**OLAP + 列式存储 + MergeTree**；常组合用 |
| 15 | ES 删除不释放空间？| 是的！删除只加 deleted bitset → 等 Merge 时真正清理 → 大表删除后要 Force Merge |
| 16 | ES OpenSearch？| AWS 维护的 ES 开源分支（ES 官方 Elastic License 有云厂商限制）→ Apache v2.0 |
| 17 | ES Top Hits Over-Fetch？| 每个 Shard 返回 **Top N × 1.5** → 避免全局 Top 漏掉；精确排序 track_total_hits |
| 18 | ES 为什么不支持事务？| 倒排索引结构决定：写入 Segment（Append Only）+ 查询时多 Segment 合并 → 不支持 ACID；最终一致性 |
| 19 | ES 学习路径？| Docker 单节点 → Index + Mapping（text/keyword）→ 写入 → Query DSL → Aggregation → 集群 + Kibana |
| 20 | ES 8.x 新特性？| 默认开启安全（HTTPS + 认证）+ ESQL（SQL 查询）+ Serverless 版本（AWS OpenSearch Serverless）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Elasticsearch 8.x + Lucene 9.x  
**面试考点：20 题**
