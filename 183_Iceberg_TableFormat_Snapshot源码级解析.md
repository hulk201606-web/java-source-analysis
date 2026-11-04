# Apache Iceberg Table Format + Snapshot + Time Travel 源码级深度解析

> **源码基线**：Apache Iceberg 1.5+（Java）  
> **核心位置**：core/src/main/java/org/apache/iceberg/ + spark/v3/spark/src/main/java  
> **面试权重**：⭐⭐⭐⭐⭐（湖仓统一表格式核心，Spark/Flink 通用）

---

## 一、Iceberg 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Iceberg = 开放表格式标准（Lakehouse 核心）                │
│                                                               │
│  解决 Hive/HDFS 的痛点：                                       │
│   ❌ Hive 只有分区（Partition），没有 Schema 演进原子性       │
│   ❌ Hive 元数据全在 HMS，单点瓶颈 + NameNode 压力           │
│   ❌ Hive 读要扫大量小文件 + 分区裁剪不够灵活                │
│   ❌ Hive 没有 Time Travel / Snapshot                        │
│                                                               │
│  ★ Iceberg 核心特性（面试必背）：                             │
│   ✅ Snapshot（快照）+ Time Travel（时间旅行）                 │
│   ✅ 原子 Schema Evolution（加/删/改列，不破坏历史数据）     │
│   ✅ Hidden Partitioning（隐藏分区，比 Hive 更灵活）          │
│   ✅ Hidden Column Stats（分区级列统计，优化器用）            │
│   ✅ 支持多计算引擎（Spark/Flink/Presto/Trino）               │
│   ✅ 多 Catalog（HMS / Glue / Nessie / REST）                │
│   ✅ 高性能 Parquet/ORC/Avro 数据文件                         │
│                                                               │
│  与 Hive/Delta/Hudi 核心区别（第六节对比表）：                │
│   Hive → 分区基、无快照、Schema 改了历史读挂                 │
│   Delta Lake → 类似 Iceberg 但 Databricks 商业闭源更多       │
│   Hudi → 支持 Update/Delete，但元数据管理不如 Iceberg 干净   │
│   ★ Iceberg → 最开放、生态最好、元数据原子性最强             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、元数据层级源码级

### 2.1 四级元数据结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Iceberg 四级元数据层级（面试核心，比代码锚点更重要）       │
│                                                               │
│  顶层：metadata.json                                          │
│   文件名：table-location/metadata/metadata.json              │
│   作用：当前 Table 的元数据根描述；**原子提交的锚点**          │
│   内容：                                                      │
│   ├── format-version：Iceberg 格式版本（1/2）                 │
│   ├── schema：表结构（Schema）                                │
│   ├── partition-spec：隐藏分区定义                           │
│   ├── default-snapshot-id：当前生效的 Snapshot ID            │
│   ├── snapshots[]：所有历史 Snapshot 列表（按时间）          │
│   │    └── snapshot {snapshot-id, parent-snapshot-id, manifest-list, timestamp}│
│   ├── snapshot-refs：TAG/BRANCH 引用                          │
│   └── last-updated-ms：最后更新时间戳                         │
│                                                               │
│  第二层：Manifest List                                        │
│   文件名：manifest-list-{snapshot-id}.avro                   │
│   作用：指向本 Snapshot 的所有 Manifest 文件                │
│   内容：                                                      │
│   └── List<ManifestFile>                                     │
│       ├── manifest-path：Manifest 文件名                    │
│       ├── partition-spec-id：对应的分区 spec                 │
│       ├── added-data-files-count：新增数据文件数             │
│       ├── existing-data-files-count：已有数据文件数          │
│       └── deleted-data-files-count：删除数据文件数           │
│                                                               │
│  第三层：Manifest File                                        │
│   文件名：manifest-{hash}-{seq}.avro                         │
│   作用：描述本 Manifest 关联的所有 Data/Delete File          │
│   内容：                                                      │
│   └── List<ManifestEntry>                                    │
│       ├── DataFile / DeleteFile 完整描述                     │
│       │    ├── file-path：对象存储路径                        │
│       │    ├── file-format：Parquet/ORC/Avro                 │
│       │    ├── partition：分区值                              │
│       │    ├── file-size-bytes：文件大小                      │
│       │    ├── record-count：文件记录数                      │
│       │    └── column-sizes：每列字节数                       │
│       ├── status：ADDED / EXISTING / DELETED                 │
│       └── snapshot-id：属于哪个 Snapshot                     │
│                                                               │
│  第四层：Data File / Delete File                              │
│   文件名：data-{uuid}.parquet / data-{uuid}.orc              │
│   作用：真正的行数据（Parquet/ORC/Avro 列式存储）            │
│   格式：标准列式存储格式（Snappy/Zstd 压缩）                 │
│                                                               │
│  ★ 原子提交原理：                                              │
│   每次 Commit 都创建新的 manifest list + metadata.json 副本  │
│   把 default-snapshot-id 指向新 Snapshot                     │
│   旧 Snapshot 仍然存在 → **Time Travel 靠历史 Snapshot 实现**│
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Java API 层级映射

```java
// ★ Iceberg Java API 对应元数据层级（源码级）
//
// core/src/main/java/org/apache/iceberg/Snapshot.java
// core/src/main/java/org/apache/iceberg/ManifestFile.java
// core/src/main/java/org/apache/iceberg/DataFile.java
//
// // Snapshot（对应 metadata.json 里的 snapshots[]）
// public interface Snapshot {
//     long snapshotId();              // 唯一 Snapshot ID
//     Long parentId();                // 上一个 Snapshot ID（链式）
//     long timestampMillis();         // 创建时间
//     String manifestListLocation();  // ★ 指向 Manifest List 文件
//     Map<String, String> summary();  // 本次变更摘要（added/deleted data files）
// }
//
// // ManifestFile（对应 Manifest List 里的条目）
// public interface ManifestFile {
//     String path();                  // Manifest 文件路径
//     Long partitionSpecId();         // 对应的分区 spec
//     ManifestContent content();      // DATA / DELETES
//     int addedFilesCount();          // 新增文件数
//     int existingFilesCount();       // 已有文件数
//     int deletedFilesCount();        // 删除文件数
//     long lowerBound() / upperBound(); // Manifest 内 min/max partition
//     List<FieldSummary> partitions();  // Manifest 分区级统计
// }
//
// // ManifestEntry（对应 Manifest File 里的条目）
// public interface ManifestEntry<F extends ContentFile<?>> {
//     Status status();                // ADDED / EXISTING / DELETED
//     SnapshotFile.Baseline baseline(); // EXISTING 条目来自哪个历史 Snapshot
//     F file();                       // DataFile / DeleteFile
// }
//
// // DataFile（对应第四层：真正的数据文件描述）
// public interface DataFile extends ContentFile<StructLike> {
//     @Override String path();
//     @Override FileFormat format();
//     @Override StructLike partition();
//     @Override long recordCount();
//     @Override long fileSizeInBytes();
//     List<Long> splitOffsets();      // Splittable 支持（大文件并行读）
//     ByteBuffer keyMetadata();       // Sequence Number（Delta Row）
//     // ★ Column-Level Stats（隐式列统计，优化器用）：
//     List<FieldSummary> columnValues();
//     // 每个列的 min/max/null_count/lower_bounds/upper_bounds
// }
//
```

---

## 三、Snapshot + Time Travel 核心

### 3.1 Time Travel 源码级

```java
// ★ Iceberg Time Travel 三种方式（源码级，面试核心）
//
// spark/v3/spark/src/main/java/.../SparkTable.kt
// core/.../SnapshotRefManager.java
//
// // 方式 1：AS OF TIMESTAMP（按时间点）
// // SQL: SELECT * FROM table FOR TIMESTAMP AS OF '2024-01-01 10:00:00'
// // Iceberg 内部：
// Snapshot snapshot = table.snapshotManager()
//     .closestSnapshotForTime(targetTimestamp);
// //    → 找到 timestampMillis <= targetTimestamp 的最近 Snapshot
// //    → 用这个 Snapshot 读 → 就是那个时间点的数据
//
// // 方式 2：AS OF VERSION（按 Snapshot ID）
// // SQL: SELECT * FROM table FOR VERSION AS OF 123456
// Snapshot snapshot = table.snapshot(123456);
// //    → 直接按 snapshotId 找
//
// // 方式 3：Branch / Tag（按分支或标签）
// // SQL: SELECT * FROM table@my_branch（Flink 语法）
// // SQL: SELECT * FROM table FOR SYSTEM_VERSION OF 'main'（Spark 语法）
// table.ops().manageSnapshots()
//     .createBranch("my_branch", snapshotId);
// //    → 管理 snapshot-refs（类似 Git tag/branch）
//
// // ★ Time Travel 实现原理（面试高频）：
// //   只读 Snapshot 的 manifest list + manifest file
// //   这些文件都是不可变的（Avro 文件一旦写完不再修改）
// //   所以任何历史 Snapshot 都能稳定读取
// //   不像 Hive 分区（DROP TABLE/drop partition 破坏数据）
// //
// //   代价：旧 Snapshot 的 data file 仍然在对象存储（不删）
// //   → Compaction + Snapshot Expiration 清理过期 Snapshot
//
// // ★ Snapshot 过期清理（Remove Orphan Files）：
// table.expireSnapshots()
//     .expireOlderThan(now - 7.days)
//     .commit();
// //    → 清理过期 Snapshot 引用的 data files
// //    → 但如果新 Snapshot 还引用同一个 data file（EXISTING）则不删
// //    // Iceberg 的引用计数由 Manifest 的 status（ADDED/EXISTING/DELETED）管理
//
```

### 3.2 原子 Commit 实现

```java
// ★ Iceberg 原子 Commit 源码级（面试核心）
//
// core/.../TableOperations.java
// core/.../BaseTable.java
// spark/v3/.../SparkWrite.java
//
// // Iceberg Commit = 一次事务，包含：
// //   1. 新增 Manifest Entry（ADDED 状态）
// //   2. 标记旧 Manifest Entry（EXISTING 或 DELETED）
// //   3. 生成新的 Manifest List
// //   4. 生成新的 metadata.json（default-snapshot-id = 新 Snapshot）
// //
// // Commit 是原子的：
// //   metadata.json 的原子替换 = 对象存储 CAS（CompareAndSwap）
// //   S3/Azure/GS 等对象存储不支持 rename，Iceberg 用：
// //   // 写新 metadata.json → 写位置/时间戳 → 旧 metadata.json 备份
// //   // 通过 Catalog 注册新路径（REST Catalog / HMS / Glue）
// //
// // // BaseTable.newWrite → 产生 Commit 任务：
// table.newWrite()
//     .appendFile(dataFile1)
//     .appendFile(dataFile2)
//     .commit();
//
// // Commit 内部：
// public void commit() {
//     // 1. 把 data files 打包成 Manifest Entries
//     //    新文件 → status = ADDED
//     List<ManifestEntry> entries = dataFiles.stream()
//         .map(df -> ManifestEntry.ofStatus(Status.ADDED, df))
//         .collect(Collectors.toList());
//
//     // 2. 读当前 Snapshot 的 Manifest List + Manifest Files
//     //    标记 EXISTING 状态
//     Snapshot current = table.currentSnapshot();
//     List<ManifestEntry> existing = readAllManifestEntries(current);
//     existing.forEach(e -> e.setStatus(Status.EXISTING));
//
//     // 3. 合并 + 分桶：新 Manifest File
//     List<ManifestEntry> all = Stream.concat(existing.stream(), entries.stream())
//         .collect(Collectors.toList());
//     List<ManifestFile> newManifests = createNewManifests(all);
//
//     // 4. 写新 Manifest List 文件
//     String manifestListPath = writeManifestList(newManifests);
//
//     // 5. 写新 metadata.json：
//     //    default-snapshot-id = new Snapshot ID
//     //    snapshots[] 追加新 Snapshot
//     //    Catalog 层原子替换（HMS / REST / Glue）
//     Metadata newMetadata = updateMetadata(metadata, newSnapshot);
//     catalog.commitTableChange(tableIdentifier, oldMetadata, newMetadata);
//
//     // ★ 关键：Catalog 层保证原子性
//     //   HMS：rename table metadata（HDFS 原子 rename）
//     //   REST Catalog：HTTP POST + 乐观锁（ETag）
//     //   Glue：updateTable API（AWS 原子）
// }
//
```

---

## 四、Schema Evolution + Hidden Partitioning

```java
// ★ Iceberg Schema Evolution（原子改表，面试高频）
//
// core/.../UpdateSchema.java
//
// Iceberg Schema 改动是**原子的**，并且：
//   ✅ 加列（Add Column）→ 历史数据默认 NULL
//   ✅ 改列类型（Widening Only）→ INT → BIGINT / FLOAT → DOUBLE
//   ❌ 不能改窄类型（BIGINT → INT 可能丢数据）
//   ✅ 重命名列（Rename Column）→ 只改元数据，历史列不改
//   ✅ 删列（Drop Column）→ 标记 deleted=true，历史文件仍有但读时跳过
//   ✅ 调整列顺序（Reorder Columns）
//   ✅ 加/删嵌套列（Nested Fields 完整支持）
//
// // Spark SQL 例子：
// // ALTER TABLE orders ADD COLUMN status STRING;
// // → Iceberg 元数据更新：
// table.updateSchema()
//     .addColumn("status", Types.StringType.get())
//     .commit();
// // → 新 Snapshot 产生
// // → 旧 Snapshot 的 Schema 还是旧版本（Time Travel 兼容）
// // ★ Schema ID 机制：
// //   每个 Snapshot 记录对应的 Schema ID
// //   用旧 Snapshot Time Travel 时自动用旧 Schema 解析
//
// // ★ Hidden Partitioning（比 Hive 更灵活）：
// // Hive 分区 = 表里有专门的 dt / hour 列，必须 SELECT 时带上
// // Iceberg Hidden Partitioning = 不暴露物理分区列
// //   partition-spec: [bucket(16, order_id), day(ts)]
// //   物理上按 bucket(16) 和 day(ts) 分区
// //   但查询 SELECT * FROM orders 时看不到 _bucket_order_id / _day_ts
// //   Iceberg 自动做 Partition Pruning（优化器内置）
// //
// //   bucket(16, order_id) → hash(order_id) % 16 = 分区号
// //   day(ts) → 按日期分区
// //   identity(status) → 按 status 列原值分区（等价 Hive 分区）
//
// // Partition Evolution（Iceberg 独有）：
// //   可以变更 Partition Spec 而不破坏历史数据
// //   ALTER TABLE orders ADD PARTITION FIELD hour(ts);
// //   新 Snapshot 用新 Partition Spec；旧 Snapshot 用旧 Spec
// //   // metadata.json 里记录多个 partition-spec（每个 spec 有 ID）
// //   // 每个 Manifest Entry 也记录 partition-spec-id
//
```

---

## 五、Iceberg vs Hive vs Delta Lake vs Hudi

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比表：四大潮仓表格式                            │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐│
│  │ 维度      │ Iceberg        │ Delta Lake    │ Hudi         ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ 开发方     │ 开源（Netflix）│ Databricks   │ 开源（Uber）  ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ 原子 Commit │ ★ 多级元数据  │ ★ Log File    │ ★ MOR/COW    ││
│  │            │ 原子提交      │ 原子 Commit   │ 原子写入     ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ Time Travel│ ★ Snapshot  │ ★ Snapshot   │ 有限          ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ Schema Evol│ ★ 原子       │ ★ 原子        │ 有限          ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ Update/Del │ MERGE INTO  │ MERGE INTO   │ ★ MOR/COW    ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ 隐藏分区   │ ★ 原生       │ ❌ 依赖 Z-Order│ ★ Bucket      ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ Catalog    │ 多 Catalog   │ 依赖 HMS/Glue │ 依赖 HMS      ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ 计算引擎   │ ★ Spark/Flink│ Spark 好     │ Spark 好     ││
│  │ 支持      │ Presto/Trino │ Flink 有限   │ Flink 有限   ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ 开放度     │ ★ 100% 开源  │ ★ 核心开源   │ ★ 100% 开源  ││
│  ├──────────┼──────────────┼──────────────┼──────────────┤│
│  │ 面试高频   │ ★★★★★       │ ★★★★★       │ ★★★★        ││
│  └──────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   通用湖仓统一格式、多引擎 → **Iceberg**（生态最好、最开放）  │
│   Databricks 独家 → Delta Lake（功能可能领先）               │
│   需要高频 Update/Delete → Hudi（MOR/COW 设计）              │
│   三者配合：Iceberg 做主表 + Hudi 做高频变更表 + Delta 做 Databricks 生态│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Iceberg 解决什么问题？| Hive/HDFS 的痛点：无快照、Schema 改了历史挂、元数据瓶颈、分区不灵活 |
| 2 | ★ 四级元数据层级？| **metadata.json → Manifest List → Manifest File → Data/Delete File** |
| 3 | metadata.json 是什么？| 当前 Table 根描述；**default-snapshot-id 原子锚点**；包含所有历史 Snapshot 列表 |
| 4 | Manifest List 作用？| 指向本 Snapshot 的所有 Manifest 文件；Avro 格式；包含新增/已有/删除文件计数 |
| 5 | Manifest File 作用？| 描述本 Manifest 关联的 Data/Delete File；含每个文件的 path/format/partition/stats |
| 6 | Iceberg 原子 Commit 原理？| 生成新 manifest list + 新 metadata.json；**Catalog 层原子替换**（HMS rename / REST ETag / Glue updateTable）|
| 7 | ★ Time Travel 三种方式？| **AS OF TIMESTAMP（按时间）+ AS OF VERSION（按 Snapshot ID）+ Branch/Tag** |
| 8 | Time Travel 为什么稳定？| Snapshot 引用的 manifest list + manifest file + data file **都是不可变 Avro 文件**；旧 Snapshot 永远能读 |
| 9 | Snapshot 过期清理？| table.expireSnapshots().expireOlderThan(7.days)；清理 data file 但不影响新 Snapshot 引用的 |
| 10 | Schema Evolution 原子性？| 加列/改类型（只宽不窄）/删列/重命名 → 新 Snapshot 自动对应；旧 Snapshot Time Travel 用旧 Schema |
| 11 | Hidden Partitioning 是什么？| **物理分区不暴露给查询**；partition-spec 在 metadata 里但 SELECT * 不显示分区列；自动 Partition Pruning |
| 12 | Partition Evolution 是什么？| 可以变更 Partition Spec 而不破坏历史数据；新 Snapshot 用新 Spec；旧 Snapshot 用旧 Spec |
| 13 | MERGE INTO 怎么实现？| Iceberg 原子语义；内部产生新 Manifest Entry（ADDED/DELETED）；Commit 时原子替换 metadata |
| 14 | ★ Iceberg vs Hive vs Delta vs Hudi？| 见第五节对比表（10 维度）|
| 15 | 面试最高频考点排序？| **1. 四级元数据层级 2. 原子 Commit 原理 3. Time Travel 实现 4. Hidden Partitioning 5. 四格式对比** |
| 16 | 学习路径？| metadata.json → Snapshot → Manifest List → Manifest → Data File → Time Travel |
| 17 | 面试常见追问？| Manifest Entry 的 status（ADDED/EXISTING/DELETED）什么意思？| ADDED 本次新增；EXISTING 之前已有；DELETED 本次删除 → Commit 时三者合并成新 Manifest |
| 18 | 面试常见追问？| Iceberg Compaction 和 Manifest Compaction 区别？| Manifest Compaction：多个 Manifest 合并成一个（减少 Snapshot 维护成本）；Data Compaction：多个小 Data File 合并成大文件（减少读取 IO）|
| 19 | 面试陷阱？| 说 Iceberg 是"开源版 Delta Lake" → ❌ 两者相似但开发方不同；Iceberg 是 Netflix 开源，最开放；Delta 是 Databricks 商业开源 |
| 20 | 面试陷阱？| 说 Iceberg 不支持 Update/Delete → ❌ MERGE INTO + DELETE FROM 都支持，内部用 Delete File + Manifest Entry 标记 |
| 21 | Catalog 类型有哪些？| HMS（Hive Metastore）+ Glue（AWS）+ Nessie（Git-like 版本化）+ REST Catalog（标准 HTTP）+ JDBC Catalog |
| 22 | REST Catalog 怎么保证原子性？| HTTP POST CommitRequest → Catalog 乐观锁（ETag / metadata.version）→ 成功返回新 Metadata → 客户端本地更新 |
| 23 | Iceberg 与 Flink 集成？| Flink 1.12+ 原生支持 Iceberg；Flink SQL 可以 Time Travel / MERGE / 做 CDC 写入 |
| 24 | Paimon 和 Iceberg 关系？| Paimon 是另一个开源湖仓格式（Apache 孵化中）；类似 Iceberg 但 Flink 原生集成更好 |
| 25 | 典型生产用法？| Flink CDC 实时写 Iceberg → Spark Time Travel 查历史 → Trino/Presto 湖仓联合查询 |
| 26 | Iceberg Branch/Tag vs Git？| 概念类似但不同；Tag = 指向 Snapshot ID 的命名引用（不可变）；Branch = 可继续追加新 Snapshot 的命名引用 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：core/src/main/java/org/apache/iceberg/Snapshot.java + ManifestFile.java + DataFile.java + BaseTable.java  
**面试考点：26 题**  
**差异化点**：四级元数据层级（metadata.json→Manifest List→Manifest→Data File）、原子 Commit 原理、Snapshot+Time Travel 实现、Hidden Partitioning、Iceberg vs Hive vs Delta vs Hudi 四格式对比
