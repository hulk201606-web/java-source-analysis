# ES DocValues 列式存储 + Query DSL Bool/Must/Should 源码级深度解析

> **源码基线**：Elasticsearch 8.x（Java）+ Lucene 9.x  
> **核心位置**：server/.../index/query/ + server/.../search/ + lucene/core/codecs  
> **面试权重**：⭐⭐⭐⭐⭐（ES 查询核心，补全 Aggregation 189）

---

## 一、ES 存储双层结构全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ES = Inverted Index（倒排索引）+ DocValues（列式存储）     │
│                                                               │
│  核心分工（面试必背）：                                        │
│  ┌───────────────────────────────────────────────────┐       │
│  │ ① Inverted Index（倒排索引）—— 搜索用              │       │
│  │   Term Dictionary → Postings List                  │       │
│  │   "Tom" → [Doc1, Doc5, Doc9]                       │       │
│  │   → 适合全文检索、Term 查询                          │       │
│  ├───────────────────────────────────────────────────┤       │
│  │ ② DocValues（列式存储）—— 聚合/排序用              │       │
│  │   每个字段一列连续存储所有值                         │       │
│  │   amount: [100, 200, 150, 300, ...]                │       │
│  │   → 适合聚合、排序、脚本计算                        │       │
│  └───────────────────────────────────────────────────┘       │
│                                                               │
│  ★ 为什么两种存储？                                           │
│   倒排索引：term → doc 快，但算 SUM 要遍历每个 doc         │
│   DocValues：列存 → 连续读快，但查 "Tom 在哪些 Doc" 慢      │
│   → 两者互补                                                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、DocValues 源码级

### 2.1 DocValues 类型 + 存储格式

```java
// ★ ES DocValues 源码级（面试核心）
//
// lucene/core/src/java/org/apache/lucene/index/DocValues.java
// server/src/main/java/org/elasticsearch/index/fielddata/
//
// // DocValues 类型（面试必背）：
// //
// // 1. SortedDocValues（Keyword / Enum）：
// //    每个字段值 → 全局字典排序后分配 ord（0, 1, 2, ...）
// //    Doc 存 ord，不存原始值 → 压缩
// //    例子：status = [pending(0), approved(1), rejected(2)]
// //
// // 2. SortedSetDocValues（Tag/Array）：
// //    多值字段；每个 Doc 有一组 ord
// //    例子：tags = [java, flink, kafka] → ord=[2, 5, 8]
// //
// // 3. NumericDocValues（Integer/Long/Float/Double）：
// //    数值连续存储；可变长度压缩（ZigZag + LZ4）
// //    例子：amount: [100, 200, 150] → 压缩后 30B
// //
// // 4. SortedNumericDocValues（多值数值）：
// //    多值数值字段
// //
// // 5. BinaryDocValues（二进制/JSON 对象）：
// //    原始字节存储；不排序；适合复杂对象
// //
// // ★ DocValues 文件格式（磁盘上）：
// //   Segment 目录下：
// //   segment_0_nvd  → NumericDocValues（金额）
// //   segment_0_nvm  → SortedNumericDocValues
// //   segment_0_si   → SortedDocValues（姓名/状态）
// //   segment_0_sdvd → SortedSetDocValues（标签）
// //   segment_0_dvd  → BinaryDocValues
//
// // ★ DocValues vs FieldData：
// //   旧版 ES 用 FieldData（内存缓存，全量加载）
// //   ES 5.0+ 推荐 DocValues（磁盘列式存储 + 按需加载）
// //   FieldData 可能 OOM；DocValues 内存可控
//
```

### 2.2 DocValues 压缩 + 访问

```java
// ★ DocValues 压缩算法源码级（面试高频）
//
// lucene/core/src/java/org/apache/lucene/util/
//
// // NumericDocValues 压缩：
// //   ZigZag（有符号变无符号）→ Delta（存差值不是绝对值）→ LZ4
// //   例子：[100, 200, 150, 300]
// //   → ZigZag: [200, 400, 300, 600]
// //   → Delta:  [200, 200, -100, 300]  // 差值
// //   → LZ4 压缩（差值重复度高，压缩率高）
// //
// // SortedDocValues 压缩：
// //   Global Dictionary 排序后存 → 每个 Doc 一个 ord
// //   ord 用 ByteBlockPool（字节块池）压缩
// //   // 比直接存 String 小 5-10 倍
// //
// // ★ DocValues 访问流程（源码级）：
// //
// // 1. 查询 Parser 解析 DSL → 选择用 DocValues 还是倒排
// // 2. 如果需要聚合/排序 → 用 DocValues
// // 3. DocValues 在 Segment Reader 中按需加载（不是全量）
// // 4. 遍历 DocValues → 聚合/排序逻辑执行
// //
// // ★ DocValues Cache：
// //   ES Cache（LRU）缓存 DocValues 的热点部分
// //   // 避免每次都从磁盘加载
// //   // Cache 大小由 indices.fielddata.cache.size 控制
// //
// // ★ Keyword 字段 doc_values=true（默认）：
// //   Keyword 字段默认开 DocValues
// //   // text 字段默认不开（text 用倒排索引）
// //   // 特殊场景（如 text + fielddata=true）需要开启
//
```

---

## 三、Query DSL Bool/Must/Should 源码级

### 3.1 Bool Query 结构

```java
// ★ ES Bool Query DSL 源码级（面试核心）
//
// server/src/main/java/org/elasticsearch/index/query/BoolQueryBuilder.java
// lucene/core/src/java/org/apache/lucene/search/BoolQuery.java
//
// // Bool Query 四种子句（面试必背）：
// //
// // 1. must：必须匹配（AND 语义）
// //    所有 must 子句都要满足
// //    → 相当于 SQL 的 WHERE col1=A AND col2=B
// //
// // 2. should：应该匹配（OR 语义）
// //    至少满足 should 子句中的 minimum_should_match 个
// //    → minimum_should_match 默认 1（如果没有 must/must_not）
// //    → 有 must/must_not 时默认 0（should 是加分项）
// //
// // 3. must_not：必须不匹配（NOT 语义）
// //    所有 must_not 子句都不能满足
// //    → 相当于 SQL 的 WHERE NOT ...
// //    // 不参与评分（只是过滤）
// //
// // 4. filter：过滤子句（不评分）
// //    和 must 一样必须匹配
// //    → 但 filter **不参与 _score 评分**
// //    → filter 会被 Cache（BoolQuery Filter Cache）
// //    → 性能比 must 好（不评分 + Cache）
// //
// // ★ Bool Query 执行流程源码级：
// //
// // 1. Lucene 解析 BoolQuery
// // 2. must 子句：用 ConjunctionScorer（AND 打分）
// // 3. should 子句：用 DisjunctionScorer（OR 打分，累加 score）
// // 4. must_not + filter：用 MatchNoDocsScorer / ConstantScoreScorer（不打分）
// // 5. 合并结果 → 返回文档列表 + 计算最终 _score
//
```

### 3.2 Bool Query 评分 + 最佳实践

```java
// ★ Bool Query 评分机制源码级（面试高频）
//
// // _score = must 子句得分总和 + should 子句得分总和（按 weight 加权）
// // must_not 和 filter 不影响 score
// //
// // ★ 例子（源码级评分）：
// // {
// //   "bool": {
// //     "must": [
// //       { "match": { "title": "Kafka" } }     // score_A
// //     ],
// //     "should": [
// //       { "match": { "tags": "streaming" } },  // score_B
// //       { "match": { "tags": "distributed" } } // score_C
// //     ],
// //     "filter": [
// //       { "range": { "dt": { "gte": "2024-01-01" } } }
// //     ],
// //     "minimum_should_match": 1
// //   }
// // }
// //
// // 最终 score = score_A + max(score_B, score_C)
// // filter 只过滤，不加 score
// //
// // ★ Bool Query 最佳实践（生产高频）：
// //
// // 1. 能用 filter 就不用 must（不评分 + 被 Cache）
// //    // filter 比 must 快 5-10 倍（不执行评分公式 + Cache）
// //
// // 2. should 子句数量 ≤ 3（避免评分太复杂）
// //    // 太多 should → 评分叠加 → 结果不好控制
// //
// // 3. minimum_should_match 显式设置
// //    // 有 must 时 should 默认 minimum_should_match=0 → should 不影响筛选
// //    // 如果 should 也决定筛选 → 必须显式设 minimum_should_match
// //
// // 4. filter 放最前面（短路优化）
// //    // Lucene BoolQuery 先评估 filter → 不匹配直接跳过 → 不用评 must/should
//
```

---

## 四、ES Query 执行完整路径源码级

```java
// ★ ES Query 执行完整路径（源码级，面试终极考点）
//
// server/src/main/java/org/elasticsearch/action/search/
// server/src/main/java/org/elasticsearch/index/query/
// lucene/core/src/java/org/apache/lucene/search/
//
// // Step 1: Coordinating Node 解析 DSL
// //   SearchRequest → SearchSourceBuilder → QueryBuilder
// //   // BoolQueryBuilder / MatchQueryBuilder / TermQueryBuilder ...
//
// // Step 2: 广播到所有 Data Node（所有 Target Shard）
// //   Coordinating Node 发 SearchRequest 给每个 Data Node
// //   // Data Node 执行本地 Lucene 查询
// //
// // Step 3: Data Node Lucene 查询执行
// //   a. QueryBuilder → Lucene Query（编译阶段）
// //      BoolQueryBuilder → Lucene BoolQuery
// //      MatchQueryBuilder → Lucene MatchQuery
// //
// //   b. Weight 创建（Lucene Query 编译阶段）
// //      // Weight = 带 Searcher + 评分信息的 Query
// //      // 预计算评分因子（BM25 IDF 等）
// //
// //   c. Scorer 遍历（Lucene 查询执行阶段）
// //      // Weight.scorer(reader) → 返回 Scorer
// //      // Scorer.nextDoc() 逐个 Doc 遍历
// //      // Scorer.score() 计算当前 Doc 的 score
// //
// //   d. 收集 Top N Docs
// //      // PriorityQueue 保存 Top N
// //      // 返回 DocID + score + sort values
// //
// // Step 4: Data Node 返回结果给 Coordinating Node
// //   // 返回 Top N Docs + 查询耗时 + 是否做了 DocValues 聚合
// //
// // Step 5: Coordinating Node Merge + 取最终 Top N
// //   // 合并所有 Data Node 的结果（PriorityQueue 再排一次）
// //   // 取全局 Top N
// //   // 返回给客户端
//
```

---

## 五、ES Query DSL vs SQL vs ClickHouse Query 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种查询引擎核心对比                             │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ ES Query DSL       │ SQL (MySQL)   │ ClickHouse││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 查询格式         │ ★ JSON DSL         │ SQL 语法      │ SQL 语法 ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 存储格式         │ 倒排 + DocValues   │ B+ Tree 行存  │ MergeTree列存││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 评分机制         │ ★ BM25              │ ❌ 无评分      │ ❌ 无评分  ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 模糊匹配         │ ★ 原生支持          │ LIKE（慢）    │ LIKE（有优化）││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 列式存储         │ ★ DocValues        │ ❌ 行存        │ ★ MergeTree││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 聚合能力         │ ★ Aggregation + Pipeline │ GROUP BY │ GROUP BY ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 典型场景         │ ★ 搜索 + 实时看板   │ 事务 OLTP     │ 数仓 OLAP││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频         │ ★★★★★          │ ★★★★★        │ ★★★★★    ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   全文搜索 + 实时聚合看板 → **ES Query DSL**（倒排+DocValues）│
│   事务 OLTP → MySQL（B+ Tree 行存）                          │
│   数仓 OLAP → ClickHouse（MergeTree 列存 + Merge）           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ES 存储双层结构？| **Inverted Index（搜索用）+ DocValues（聚合/排序用）** |
| 2 | ★ DocValues 五种类型？| **SortedDocValues（Keyword）+ SortedSetDocValues（Tag）+ NumericDocValues（数值）+ SortedNumericDocValues + BinaryDocValues** |
| 3 | DocValues 压缩算法？| **ZigZag（有符号变无符号）+ Delta（存差值）+ LZ4** |
| 4 | DocValues vs FieldData？| DocValues：磁盘列式 + 按需加载；FieldData：内存全量加载 + 可能 OOM → **ES 5.0+ 弃用 FieldData** |
| 5 | ES Bool Query 四种子句？| **must（AND 评分）+ should（OR 至少 N 个）+ must_not（排除）+ filter（过滤不评分）** |
| 6 | must vs filter 核心区别？| must 参与评分；filter 不评分 + **被 Cache（BoolQuery Filter Cache）**→ filter 比 must 快 5-10 倍 |
| 7 | should minimum_should_match 默认？| **有 must/must_not 时默认 0**（should 是加分项）；没有时默认 1 |
| 8 | ★ Bool Query 最佳实践？| 能用 filter 就不用 must；should ≤ 3 个；minimum_should_match 显式设置；filter 放最前面短路 |
| 9 | ES Query 完整执行路径？| DSL 解析 → Broker 广播 → Lucene Query 编译 → Weight 创建 → Scorer 遍历 → PriorityQueue Top N → Merge |
| 10 | BM25 评分是什么？| ES 默认评分算法；基于 Term Frequency + IDF + Field Length；比 TF-IDF 更精准 |
| 11 | ★ ES Query DSL vs SQL vs ClickHouse Query 三引擎对比？| 见第五节对比表（8 维度）|
| 12 | 面试最高频考点排序？| **1. DocValues 五种类型 2. Bool Query 四子句+filter 不评分 3. filter vs must 性能 4. 完整执行路径 5. DocValues 压缩** |
| 13 | 学习路径？| 倒排 vs DocValues → DocValues 类型/压缩 → Bool Query 四子句 → 执行路径 → 对比 |
| 14 | 面试常见追问？| DocValues 能替代倒排索引吗？| **不能！** DocValues 列存适合聚合排序；倒排索引才适合"查 Term 在哪些 Doc 里" |
| 15 | 面试常见追问？| filter 比 must 快多少？| **5-10 倍**：不执行 BM25 评分公式 + BoolQuery Filter Cache 缓存结果 |
| 16 | 面试陷阱？| 说 "should 默认至少匹配 1 个" → ❌ **有 must/must_not 时 minimum_should_match 默认 0**（should 不影响筛选，只是加分）|
| 17 | 面试陷阱？| 说 "ES Query DSL 是 SQL 子集" → ❌ **ES Query DSL 是 JSON 格式的树状结构**；和 SQL 完全不同 |
| 18 | DocValues Cache 怎么看？| `indices.fielddata.cache.size`（默认无限制）；看 Cache 命中率 `_cat/nodes?v&h=name,fielddata.mem_size,fielddata.evictions` |
| 19 | Lucene Weight + Scorer 是什么？| Weight = Query 编译后带评分信息；Scorer = Weight 的执行器，逐个 Doc 遍历 + 计算 score |
| 20 | Term Query vs Match Query？| Term Query：精确匹配（不分词）；Match Query：先分词再匹配（ES 会自动选）|
| 21 | 典型生产配置？| **keyword 字段 doc_values=true（默认）+ 能用 filter 不用 must + Query Cache 开启（默认）** |
| 22 | ES 8.x Query DSL 新特性？| KNN 向量查询 + EQL 事件查询语言 + Geo 查询增强 |
| 23 | DocValues 和 Text 字段？| Text 字段默认**不开启 DocValues**（只用倒排）；Keyword 默认开启 DocValues |
| 24 | Bool Query Filter Cache 工作原理？| 同一个 filter 子句被 Bool Query 多次使用 → Cache 结果；不评分 → 结果可缓存（确定性） |
| 25 | Scorer 遍历性能优化？| Scorer.nextDoc() 不是逐个 Doc 跳；用 BlockMaxWAND 跳过不匹配的 Block → 大幅加速 |
| 26 | ES Query 和 Aggregation 关系？| Query 先过滤 → 过滤后的 Doc 做 Aggregation（DocValues 列式）；可以多个 Query + Aggregation 组合 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：lucene/core/.../DocValues.java + BoolQuery.java + server/.../BoolQueryBuilder.java  
**面试考点：26 题**  
**差异化点**：DocValues 五种类型 + ZigZag+Delta+LZ4 压缩、**Bool Query 四子句 + filter 不评分 + Cache**、ES Query 完整执行路径（Weight→Scorer→TopN Merge）、ES DSL vs SQL vs ClickHouse 三引擎对比
