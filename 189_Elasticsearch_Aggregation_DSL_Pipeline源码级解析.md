# Elasticsearch Aggregation DSL + Pipeline 源码级深度解析

> **源码基线**：Elasticsearch 8.x（Java）  
> **核心位置**：server/.../aggregations/ + aggregations/bucket + aggregations/pipeline  
> **面试权重**：⭐⭐⭐⭐⭐（ES 搜索核心，高频）

---

## 一、ES Aggregation 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ES Aggregation = 搜索 + 分析一体化（面试必背）            │
│                                                               │
│  核心价值：                                                    │
│   传统数仓要拉全量数据 → Spark/ClickHouse 算 → 慢            │
│   ES 直接在索引上聚合 → 毫秒级返回                            │
│   → "近实时搜索 + 实时聚合分析"                                │
│                                                               │
│  ★ 三大类 Aggregation（面试必背分类）：                       │
│  ┌───────────────────────────────────────────────────┐       │
│  │ ① Metric（度量聚合）—— 单值/多值指标                │       │
│  │   avg / sum / min / max / count / cardinality      │       │
│  │   → 对所有匹配文档算一个值（或几个值）               │       │
│  │   → 类似 SQL 的 SELECT avg(col) FROM ...            │       │
│  ├───────────────────────────────────────────────────┤       │
│  │ ② Bucket（桶聚合）—— 分桶 + 每个桶再算 Metric      │       │
│  │   terms / date_histogram / histogram / range /     │       │
│  │   geo_distance / filter / global / nested          │       │
│  │   → 把文档分到多个桶 → 每个桶可以嵌套 Metric       │       │
│  │   → 类似 SQL 的 GROUP BY col                       │       │
│  ├───────────────────────────────────────────────────┤       │
│  │ ③ Pipeline（管道聚合）—— 聚合结果再处理             │       │
│  │   bucket_sort / bucket_selector / derivative /      │       │
│  │   moving_avg / serial_diff                         │       │
│  │   → 输入是其他 Aggregation 的结果 → 输出新结果     │       │
│  │   → 类似 SQL 的 HAVING + 窗口函数                   │       │
│  └───────────────────────────────────────────────────┘       │
│
│  ★ 执行流程（源码级）：                                        │
│   Request → Coordinating Node → 广播到所有 Data Node          │
│   → 每个 Data Node 本地做 Agg（Map 阶段）                     │
│   → 把 Partial 结果返回 Coordinating Node                     │
│   → Coordinating Node Merge（Reduce 阶段）→ 返回给客户端     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Metric Aggregation 源码级

### 2.1 avg/sum/min/max + cardinality

```java
// ★ Metric Aggregation 核心源码级（ES server）
//
// server/src/main/java/org/elasticsearch/search/aggregations/metric/
// server/.../aggregations/metric/avg/AvgAggregator.java
// server/.../aggregations/metric/sum/SumAggregator.java
//
// // Aggregator 核心接口：
// public abstract class Aggregator extends AggregatorBase {
//     // 创建一个 Bucket（Metric 聚合只有一个 Bucket）
//     public abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub);
// }
//
// // ★ AvgAggregator 实现：
// public class AvgAggregator extends NumericMetricsAggregator.SingleValue {
//     long count = 0;     // 文档数
//     double sum = 0;     // 值总和
//
//     // 遍历每个匹配文档：
//     public void collect(int doc, long owningBucketOrdinal) {
//         double value = valuesSource.doubleValue(doc);
//         count++;
//         sum += value;
//     }
//
//     // 最终结果：
//     public double metric() { return count > 0 ? sum / count : 0; }
// }
//
// // ★ Cardinality（基数/去重计数）核心：
// // server/.../cardinality/CardinalityAggregator.java
// // 用 HyperLogLog 算法（基数近似）
// //   HLL 精度：误差约 1.04/√precision
// //   precision 默认 4000 → 误差约 1.6%
// //   用 Redis HyperLogLog 同样算法
// //   → 大数据量下不能准确 COUNT(DISTINCT)  → 近似值
//
// // ★ 其他高频 Metric：
// //   - stats：同时返回 count/min/max/sum/avg
// //   - extended_stats：stats + 方差/标准差/峰度
// //   - percentiles：分位数（P50/P90/P99）
// //   - percentile_ranks：反向分位数
// //   - value_count：非空值计数
// //   - top_hits：取前 N 个匹配文档
// //   - scripted_metric：自定义脚本计算（最灵活）
//
```

---

## 三、Bucket Aggregation 源码级

### 3.1 terms + date_histogram + nested

```java
// ★ Bucket Aggregation 核心源码级
//
// server/.../aggregations/bucket/terms/TermsAggregator.java
// server/.../aggregations/bucket/datehistogram/DateHistogramAggregator.java
// server/.../aggregations/bucket/nested/NestedAggregator.java
//
// // ★ Terms Aggregation（最常用）源码级：
// //   类似 GROUP BY column
// //   默认返回前 size=10 个 Term（按 doc_count 降序）
// public class TermsAggregator extends BucketAggregator {
//     // 内部用 Map<Object, InternalBucket> buckets
//     // 或 FieldData（DocValues）遍历
//     public void collect(int doc, long owningBucketOrd) {
//         // ① 从 DocValues 取字段值
//         SortedDocValues values = context.reader().getSortedDocValues(field);
//         if (values.advance(doc) == doc) {
//             String value = values.value();
//             // ② 把 doc 归到对应 bucket
//             buckets.computeIfAbsent(value, v -> new InternalBucket(v, 0))
//                    .docCount++;
//         }
//     }
//
//     // ★ 性能优化：
//     //   用 DocValues（列式存储，按字段值排序）
//     //   不是倒排索引（倒排适合查询不适合聚合）
//     //   size 参数控制返回多少 Top Term
//     //   shard_size 控制每个分片查多少 Term（Coordinator Merge 时取 Top size）
// }
//
// // ★ Date Histogram Aggregation（时间窗口）：
// //   类似 GROUP BY date_trunc('interval', dt)
// //   interval 支持：1m / 1h / 1d / 1w / 1M / auto
// //   // auto 根据数据跨度自动选 interval
// //   // 用 JodaTime / Java 8 DateFormat 对齐
//
// // ★ Nested Aggregation（嵌套文档）：
// //   处理 nested 类型的字段
// //   // nested 文档独立索引 + 关联 Root
// //   // NestedAggregator 把 Root Bucket 里的 nested 文档拿出来聚合
// //   // scores_mode = avg / max / min / sum
//
// // ★ 其他高频 Bucket：
// //   - range：数值范围分桶（[0-100), [100-200), [200+)
// //   - histogram：数值区间直方图
// //   - filter / filters：按 Query 条件分桶
// //   - global：忽略 Query 条件（全局聚合）
// //   - missing：把字段缺失的文档归一个桶
// //   - geo_distance：按地理位置距离分桶
// //   - reverse_nested：从 nested 文档回到 root
//
```

### 3.2 嵌套 Aggregation 源码级

```java
// ★ ES Aggregation 嵌套特性（核心能力）
//
// // Bucket 可以嵌套 Metric 或其他 Bucket
// // 源码：server/.../AggregatorFactory → subAggs
//
// // 例子：
// // {
// //   "aggs": {
// //     "by_status": {                    // Bucket 1: terms
// //       "terms": { "field": "status" },
// //       "aggs": {                       // 嵌套：每个 status 桶里
// //         "by_dt": {                    // Bucket 2: date_histogram
// //           "date_histogram": {
// //             "field": "dt", "calendar_interval": "1d"
// //           },
// //           "aggs": {                  // 再嵌套：每个日期桶里
// //             "total": { "sum": { "field": "amount" } }   // Metric
// //           }
// //         }
// //       }
// //     }
// //   }
// // }
// //
// // → GROUP BY status, date_trunc('day', dt) → SUM(amount)
// // → ES 嵌套聚合 = SQL GROUP BY 多列
//
// // 嵌套 Aggregation 源码级执行：
// //   TermsAggregator（外层）遍历每个 Bucket → 对每个 Bucket 的文档
// //   → 触发 DateHistogramAggregator（内层）→ 再触发 SumAggregator（更内层）
// //   → 树形结构递归执行
// //
// // 嵌套深度限制：ES 8.x 默认 20（search.max_buckets 默认 10000）
// //   // search.max_buckets 防止 Bucket 爆炸导致 OOM
// //   // 如果 Terms size=10 × DateHistogram 365 天 → 3650 个 Bucket
//
```

---

## 四、Pipeline Aggregation 源码级

### 4.1 Pipeline 核心能力

```java
// ★ Pipeline Aggregation 源码级（面试高频，ES 独有优势）
//
// server/.../aggregations/pipeline/
//
// // Pipeline 是什么？
// //   输入：**其他 Aggregation 的输出结果**
// //   处理：对结果做二次计算（排序/选择/差分/移动平均）
// //   输出：新结果（不参与原始 Doc 遍历）
// //
// // // Pipeline 三类：
// //
// // 1. Parent Pipeline（输入是 Parent Aggregation）
// //    bucket_sort / bucket_selector / bucket_truncate
// //    // 对 Bucket 做排序/选择/截断
// //
// // 2. Sibling Pipeline（输入是同层级的 Metric Aggregation）
// //    min_bucket / max_bucket / avg_bucket / sum_bucket / stats_bucket
// //    // 对某个 Bucket 里的 Metric 做二次聚合
// //
// // 3. Sibling Metric Pipeline（时间序列处理）
// //    derivative / moving_avg / serial_diff / cumulative_sum
// //    // 类似 SQL 窗口函数
//
// // ★ 三个高频 Pipeline 源码级细节：
// //
// // bucket_sort：对 Bucket 排序
// // "pipeline": { "bucket_sort": { "sort": [{"total.amount": "desc"}], "size": 5 } }
// // → 取 SUM(amount) 最大的 5 个 Bucket
// //
// // bucket_selector：按条件筛选 Bucket
// // "pipeline": { "bucket_selector": { "buckets_path": "by_dt>total", "script": "params.value > 1000" } }
// // → 只保留 SUM(amount) > 1000 的日期桶
// //
// // moving_avg：时间序列移动平均
// // "pipeline": { "moving_avg": { "buckets_path": "by_dt>total", "window": 7, "model": "simple" } }
// // → 过去 7 天的移动平均（平滑时间序列波动）
//
```

---

## 五、ES Agg vs ClickHouse Agg vs TiKV Coprocessor 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种分布式聚合方案对比                          │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ ES Agg           │ ClickHouse     │ TiKV      ││
│  │                │                  │ MergeTree      │ Coprocessor││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 数据模型         │ 列式 + 倒排     │ ★ 列式优化     │ KV (RocksDB)│
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 聚合位置         │ Data Node 本地  │ Node 本地      │ ★ TiKV 本地│
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ Pipeline 能力     │ ★ 有（窗口函数）  │ ❌ SQL 窗口    │ ❌ 无      │
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 嵌套 Aggregation │ ★ 无限嵌套      │ GROUP BY 多列  │ 无         │
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 模糊匹配         │ ★ 搜索 + 聚合   │ ❌ 只有聚合    │ ❌ 无      │
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 近实时           │ ★ 毫秒级        │ 秒级（Merge）  │ 毫秒级     │
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 写入性能         │ 中（倒排更新）  │ ★ 高（Append-  │ ★ 高（Percolator）│
│  │                │                  │ Only + Merge） │           │
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 典型场景         │ 搜索 + 实时看板 │ 数仓 OLAP     │ HTAP      │
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频         │ ★★★★★          │ ★★★★★        │ ★★★★★    ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   需要搜索 + 实时聚合看板 → **ES Aggregation**（Pipeline 能力独强）│
│   纯数仓 OLAP + 海量聚合 → ClickHouse MergeTree              │
│   HTAP 同时查 TiKV 事务数据 → TiKV Coprocessor               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ES Aggregation 三大类？| **Metric（单值）+ Bucket（分桶）+ Pipeline（管道再处理）** |
| 2 | Metric 高频？| avg / sum / min / max / stats / cardinality / percentiles / top_hits |
| 3 | ★ Cardinality 原理？| **HyperLogLog 近似算法**；precision 默认 4000 → 误差约 1.6%；大数据量去重不能精确算 |
| 4 | Bucket 高频？| terms / date_histogram / histogram / range / filter / nested / geo_distance |
| 5 | Terms Aggregation 核心？| DocValues 列式扫描；size 返回 Top N Term；shard_size 每个分片查多少 |
| 6 | 嵌套 Aggregation 特性？| Bucket 里可以嵌套 Metric 或其他 Bucket；**树形递归执行**；类似多列 GROUP BY |
| 7 | ★ Pipeline Aggregation 核心？| **输入是其他 Aggregation 的结果**；对结果二次处理（排序/选择/差分/移动平均）|
| 8 | Pipeline 三类？| Parent Pipeline（Bucket 排序/选择）+ Sibling Pipeline（Metric 二次聚合）+ Sibling Metric Pipeline（时间序列）|
| 9 | 高频 Pipeline 例子？| bucket_sort（排序 Bucket）+ bucket_selector（条件筛选）+ moving_avg（移动平均）|
| 10 | search.max_buckets 默认？| **10000**；防止 Bucket 爆炸导致 OOM；报错时可调大但要注意内存 |
| 11 | ES Aggregation 执行流程？| Coordinating Node 广播 → Data Node 本地 Map → 回传 Partial → Coordinating Node Merge Reduce |
| 12 | ★ ES Agg vs ClickHouse vs TiKV Coprocessor 对比？| 见第五节对比表（9 维度）|
| 13 | 面试最高频考点排序？| **1. 三大类分类 2. Pipeline 核心能力 3. Cardinality HLL 4. 嵌套 Aggregation 5. 三者对比** |
| 14 | 学习路径？| Metric → Bucket → 嵌套 → Pipeline → 执行流程 |
| 15 | 面试常见追问？| ES Aggregation 为什么快？| 用 DocValues 列式存储（按字段值排序）+ Map-Reduce 并行；不需要全表扫描倒排索引 |
| 16 | 面试常见追问？| Terms Aggregation 返回不全怎么办？| shard_size 默认 size×1.5；调大 shard_size → 每个分片查更多 → Coordinator Merge 更全 |
| 17 | 面试陷阱？| 说 "Cardinality 是精确去重" → ❌ **是 HyperLogLog 近似算法**；precision 越高越准但越耗内存 |
| 18 | 面试陷阱？| 说 "Pipeline Aggregation 会再扫 Doc" → ❌ **Pipeline 只处理其他 Aggregation 的结果**；不接触原始文档 |
| 19 | date_histogram interval=auto 怎么选？| 根据数据跨度自动选（跨度大 → 日/周；跨度小 → 分钟/小时）；JodaTime / Java 8 对齐 |
| 20 | ES Aggregation 和 Query 的关系？| Query 先过滤 → 剩下的文档做 Aggregation；可以多个 Query + 多个 Aggregation 组合 |
| 21 | Nested Aggregation 为什么要特殊处理？| nested 类型的文档独立索引 + 关联 Root；NestedAggregator 跨 Root 和 Nested 两段索引联合聚合 |
| 22 | 典型生产用法？| 用户画像看板：**terms(性别/年龄) + date_histogram(天) + moving_avg(7天移动平均) + bucket_sort(Top 5)** |
| 23 | top_hits Aggregation 作用？| 每个 Bucket 里取前 N 个匹配文档；类似 GROUP BY 后取组内 Top N |
| 24 | Pipeline 的 buckets_path 语法？| 用 `>` 指向子 Aggregation：`by_status>by_dt>total`（status 桶 → dt 桶 → sum 指标）|
| 25 | ES 8.x Pipeline 性能？| Pipeline 只处理 Aggregation 结果（内存操作），**不扫 Doc**；几乎不增加耗时 |
| 26 | search.aggregation.bucket.paths_limit？| 默认 5000；防止 buckets_path 链太长导致内存压力 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：server/.../aggregations/metric/avg/AvgAggregator.java + bucket/terms/TermsAggregator.java + pipeline/  
**面试考点：26 题**  
**差异化点**：三大类 Aggregation 分类、Pipeline（输入是其他 Aggregation 结果）核心、Cardinality HLL 算法、**ES Agg vs ClickHouse vs TiKV Coprocessor 聚合方案对比**
