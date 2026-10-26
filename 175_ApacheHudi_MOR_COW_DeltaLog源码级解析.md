# Apache Hudi MOR/COW + Delta Log 源码级深度解析

> **源码基线**：Apache Hudi 1.0+（Java）  
> **核心位置**：hudi-common/src/main/java + hudi-client/... + hudi-table  
> **面试权重**：⭐⭐⭐⭐⭐（湖仓三大格式之一，高频）

---

## 一、Hudi 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Apache Hudi = 湖仓格式（Uber 开源）                       │
│                                                               │
│  解决问题（与 Iceberg/Delta 类似但有独特设计）：                │
│   ✅ 支持 Update / Delete 原生操作（不是 Append-Only）         │
│   ✅ Merge-On-Read vs Copy-On-Write 两种写入模式             │
│   ✅ Timeline 元数据 + Commit 增量写入                        │
│   ✅ 时间旅行（Time Travel）                                 │
│   ✅ Schema Evolution（加/改列）                             │
│   ✅ 与 Spark/Flink 原生集成                                 │
│                                                               │
│  ★ Hudi vs Iceberg vs Delta Lake 核心区别（第六节对比）：    │
│   Iceberg → 最开放、元数据干净、Append-Only 为主            │
│   Delta → Databricks 商业、Log 索引、功能领先                │
│   ★ Hudi → 原生支持高频 Update/Delete（MOR 模式有 Delta Log）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、MOR vs COW 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Hudi 两种写入模式（面试核心 trade-off）                   │
│                                                               │
│  ┌─────────────────────────────────────────────────┐         │
│  │ ① COW（Copy-On-Write）：                       │         │
│  │                                                 │         │
│  │  写入时：找到旧数据文件 → 读出来 → 合并新数据  │         │
│  │         → 写新的 Parquet 文件（覆盖旧文件）    │         │
│  │         → 旧文件放 Cleaned 区                  │         │
│  │                                                 │         │
│  │  查询时：直接读最新的 Parquet 文件             │         │
│  │         ★ 读延迟低（无 Merge）                │         │
│  │                                                 │         │
│  │  缺点：写放大高（每次 Update/Delete 都重写整个文件）│      │
│  │                                                 │         │
│  └─────────────────────────────────────────────────┘         │
│                                                               │
│  ┌─────────────────────────────────────────────────┐         │
│  │ ② MOR（Merge-On-Read）—— Hudi 核心特色：       │         │
│  │                                                 │         │
│  │  写入时：Update/Delete 写到 **Delta Log**（Avro）│        │
│  │         原始数据保持不变（Parquet Base File）    │         │
│  │         ★ 写几乎零开销（追加 Delta Log）       │         │
│  │                                                 │         │
│  │  查询时：Parquet Base File + Delta Log Merge   │         │
│  │         ★ 读时合并 → 读延迟比 COW 高           │         │
│  │                                                 │         │
│  │  后台 Compaction：把 Delta Log 合并进 Parquet │         │
│  │         释放 Delta Log 空间                    │         │
│  │                                                 │         │
│  └─────────────────────────────────────────────────┘         │
│                                                               │
│  ★ MOR vs COW 取舍表格（面试高频）：                         │
│  ┌──────────┬───────────────────┬───────────────────┐      │
│  │            │ MOR                 │ COW                 │      │
│  ├──────────┼───────────────────┼───────────────────┤      │
│  │ 写延迟     │ ★ 低（追加 Delta） │ 高（重写文件）     │      │
│  ├──────────┼───────────────────┼───────────────────┤      │
│  │ 读延迟     │ 高（读时 Merge） │ ★ 低（直接读）     │      │
│  ├──────────┼───────────────────┼───────────────────┤      │
│  │ 适合场景   │ 高频 Update/Delete │ 读多写少（Append-Only）│  │
│  │            │ CDC/实时数仓      │ 报表/数仓 ETL      │      │
│  ├──────────┼───────────────────┼───────────────────┤      │
│  │ 存储格式   │ Parquet + Avro Log│ Parquet（纯列式）   │      │
│  ├──────────┼───────────────────┼───────────────────┤      │
│  │ 典型选择   │ 业务表（订单/账户）│ 数仓宽表/事实表     │      │
│  └──────────┴───────────────────┴───────────────────┘      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Timeline + Commit + Delta Log 源码级

### 3.1 Timeline 元数据层级

```java
// ★ Hudi Timeline 核心（源码级）
//
// hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTimeline.java
// hudi-table/.../view/HoodieTableFileSystemView.java
//
// // 每个 Hudi Table 有一个 .hoodie/ 元数据目录：
// //
// // table-path/.hoodie/
// // ├── .timeline/
// // │   ├── 20241026100000.commit      ← Commit 元数据（JSON）
// // │   ├── 20241026100001.deltacommit ← Delta Commit 元数据（MOR 专用）
// // │   ├── 20241026100002.compaction  ← Compaction 计划
// // │   ├── 20241026100003.replace     ← Replace Operation（Delete Partition）
// // │   └── 20241026100004.savepoint   ← Savepoint（Time Travel 锚点）
// // ├── 20241026100000.commit          ← Commit 执行结果（JSON）
// // └── 20241026100000.rollback        ← 回滚标记（失败的 Commit）
//
// // ★ 对比 Iceberg（173 文档）：
// //   Iceberg: metadata.json → manifest list → manifest → data file
// //   Hudi:    .timeline/{ts}.commit/.deltacommit → data file + delta log
// //   两者都是元数据驱动 + 不可变文件
// //   差异：Iceberg 把所有 Snapshot 信息集中在 metadata.json
// //        Hudi 把每个 Commit 单独一个文件（历史更清晰）
//
// // Commit 文件内容（JSON）：
// {
//     "commitTime": "20241026100000",
//     "operationType": "UPSERT",
//     "writeStatuses": {
//         "COMMITTED": [
//             {"fileId": "abc123", "path": "orders/2024-10-26/abc123.parquet", "records": 100},
//             {"fileId": "def456", "path": "orders/2024-10-26/def456.parquet", "records": 50}
//         ],
//         "DELETED": []
//     },
//     "stats": {"totalRecords": 150, "updateRecords": 20}
// }
//
// // ★ Delta Log（MOR 模式专用）：
// //   文件格式：Avro（比 Parquet 轻量、追加快）
// //   文件名：<baseFileId>_<timestamp>.log
// //   位置：和 Parquet Base File 同目录
// //   内容：INSERT/UPDATE/DELETE 变更记录
// //   每个 Delta Log 还带一个 .log.index（位置索引，加速合并）
//
```

### 3.2 MOR 模式读路径：Base File + Delta Merge

```java
// ★ MOR 查询时的 Merge 流程（源码级）
//
// hudi-table/.../executor/HoodieMergeOnReadTableExecutor.java
//
// // 查询一个 MOR Partition：
// public Iterator<Row> readMORPartition(String partitionPath) {
//     // ① 找出这个 Partition 下所有最新的 Parquet Base Files
//     List<String> baseFiles = timelineView.getLatestBaseFiles(partitionPath);
//
//     // ② 对每个 Base File，找出关联的 Delta Logs
//     //    Delta Log 和 Base File 共享 fileId（文件名前缀）
//     //    Base:  abc123.parquet
//     //    Delta: abc123_20241026.log, abc123_20241027.log
//     for (String baseFile : baseFiles) {
//         List<String> deltaLogs = timelineView.getDeltaLogs(baseFile);
//
//         // ③ ★ 执行 Merge（核心！）
//         //    a. 读 Parquet Base File（列式，快）
//         //    b. 读所有 Delta Log（Avro，增量变更）
//         //    c. 按 record key 合并：
//         //       - Delta 有 INSERT → 新增行
//         //       - Delta 有 UPDATE → 覆盖 Base 对应行
//         //       - Delta 有 DELETE → 标记该行删除
//         //    d. 过滤掉被 DELETE 的行
//         Iterator<Row> merged = mergeBaseAndDelta(baseFile, deltaLogs);
//         results.add(merged);
//     }
//
//     return mergeAll(results);
// }
//
// ★ MOR Merge 性能优化（面试高频）：
//   - Read Optimized（RO）查询：跳过 Delta Log 只读 Base File
//     // 适合报表场景，允许读到稍旧的数据
//     // SET hudi.query.engine = 'spark_ro'（Spark SQL）
//   - Snapshot Query：读最新 Base + 所有 Delta（默认，最准）
//   - Incremental Query：只读某个 Commit 之后的变更
//
```

---

## 四、Compaction 源码级

### 4.1 Compaction 两种类型

```java
// ★ Hudi Compaction 核心（源码级，面试高频）
//
// hudi-table/.../compaction/CompactionHelper.java
// hudi-table/.../action/compaction/RunCompactionAction.java
//
// // Compaction 类型：
// //
// // ① Delta Compaction（MOR 模式专用）：
// //    把一个 Base File 关联的多个 Delta Log 合并进新 Parquet
// //    触发条件：
// //      - Delta Log 数量超过 threshold（默认 10 个）
// //      - 或 Delta Log 总大小超过 threshold
// //    结果：新 Parquet Base File + 旧 Base + 旧 Delta Log 清理
// //
// // ② Full Compaction（两种模式通用）：
// //    把一个 Partition 下所有 Base Files 合并成一个大 Parquet
// //    触发条件：手动或定期（每 24h / 每日低峰）
// //    结果：Partition 内只有一个大 Parquet 文件
// //
// // Compaction 执行流程（源码级）：
// public void runDeltaCompaction(String baseFileId) {
//     // 1. 生成 Compaction 计划（Commit 文件）
//     CommitPlan plan = generateCompactionPlan(baseFileId);
//     //    .hoodie/.timeline/{ts}.compaction（计划）
//
//     // 2. 读 Base File + 所有 Delta Log
//     List<String> deltaLogs = getDeltaLogs(baseFileId);
//
//     // 3. 执行 Merge（和查询时一样的逻辑，但写新 Parquet）
//     Iterator<Row> merged = mergeBaseAndDelta(baseFileId, deltaLogs);
//
//     // 4. 写新的 Parquet Base File
//     String newBaseFileId = UUID.randomUUID();
//     ParquetWriter.write(newBaseFileId, merged);
//
//     // 5. 写新的 Commit 文件
//     //    .hoodie/.timeline/{ts}.deltacommit（完成标记）
//     //    标记旧 Base + 旧 Delta Log 为 DELETED
//
//     // 6. 清理旧文件（延迟，避免查询正在用）
//     //    Clean Job 定期清理 .rollback + 旧数据文件
// }
//
// ★ Async Compaction（异步 Compaction）：
//   MOR 模式下 Compaction 默认异步
//   写入线程不等待 Compaction 完成
//   后台线程定期扫描 → 触发 Delta Compaction
//   Spark DataSink 支持 "hoodie.compact.inline=true"（同步）
//   Flink Sink 默认异步（避免影响写入延迟）
//
```

### 4.2 Clean Job + Savepoint

```java
// ★ Clean Job + Savepoint（源码级）
//
// // Clean Job：定期清理旧文件
// //   清理范围：
// //     - 已 Commit 但已被 Compaction 替代的旧数据文件
// //     - 已完成的 .rollback 文件
// //     - 已过期的 .commit / .deltacommit 文件
// //   清理策略：
// //     - KEEP_LATEST_COMMITS（默认）：保留最近 N 个 Commit 的文件
// //     - KEEP_LATEST_FILE_VERSIONS：每个 fileId 保留最近 M 个版本
// //     - KEEP_LATEST_BY_HOURS：保留最近 H 小时内的文件
//
// // Savepoint：显式保存一个 Snapshot 作为 Time Travel 锚点
// //   写入 .hoodie/.timeline/{ts}.savepoint
// //   Clean Job 不会清理 Savepoint 之前的文件
// //   作用：
// //     - Time Travel 到某个业务锚点（如"双11 0点"）
// //     - Schema 大改动前先 Savepoint，万一错了可以回滚
//
// // Time Travel 语法（Spark SQL）：
// //   SELECT * FROM orders TIMESTAMP AS OF '2024-10-26 10:00:00'
// //   → 找到 timeline 中 <= 该时间的最近 Commit
// //   → 只读这个 Commit 及之前的文件
//
```

---

## 五、Iceberg vs Hudi vs Delta Lake 核心对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试最高频：湖仓三大格式深入对比                          │
│  ┌──────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度      │ Hudi (MOR/COW)    │ Iceberg       │ Delta    ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Update/Del│ ★ 原生支持（MOR  │ Merge-On-Read│ ★ 强      ││
│  │           │ Delta Log）      │ 有限支持      │          ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 写入模式   │ ★ MOR + COW      │ Append-Only   │ Log-Based││
│  │           │ 两种可选          │ 为主          │          ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 元数据结构 │ Timeline (.commit) │ metadata.json│ Log.json││
│  │           │ 每个 Commit 单独 │ → Manifest →  │ + Parquet││
│  │           │ 文件（清晰）      │ Data File     │ Checkpoint││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 时间旅行   │ Savepoint + Commit│ Snapshot     │ Version  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Compaction│ Delta + Full      │ Manifest + Data│ Z-Order  ││
│  │           │ 两种              │ 两层          │ Optimize ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Schema Evol│ 原子改表        │ ★ 原子改表   │ 原子改表  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 隐藏分区   │ Bucket + Identity│ ★ Hidden Part│ ❌       ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 引擎支持   │ ★ Spark/Flink 好 │ ★ Spark/Flink│ Spark 好 ││
│  │           │ Presto 有限      │ Presto/Trino  │ Flink 有限││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 开放度     │ ★ 100% 开源      │ ★ 100% 开源  │ ★ 核心开源││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 写 vs 读    │ MOR: 写快读慢   │ 写中读快      │ 写快读慢 ││
│  │           │ COW: 写慢读快     │               │ (Log)    ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频   │ ★★★★★          │ ★★★★★        │ ★★★★★    ││
│  └──────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   高频 Update/Delete CDC → **Hudi MOR**（Delta Log 写零开销）│
│   数仓 Append-Only + 多引擎 → **Iceberg**（最开放、生态好）  │
│   Databricks 生态 → Delta Lake（功能可能领先）               │
│   数仓宽表只读报表 → Hudi COW / Iceberg（读快）             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（25 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Hudi MOR 模式核心特色？| **Delta Log（Avro 增量变更追加）+ 查询时 Base File + Delta Merge** |
| 2 | Hudi COW 模式核心？| Copy-On-Write：写入时重写整个 Parquet 文件；**读快（无 Merge）、写慢（重写）** |
| 3 | MOR vs COW 取舍？| MOR：写快读慢（高频 Update/CDC）；COW：写慢读快（数仓 Append-Only）|
| 4 | Hudi Timeline 是什么？| .hoodie/.timeline/ 下每个 Commit 一个文件（.commit / .deltacommit / .compaction / .savepoint）|
| 5 | Commit 文件内容？| JSON：operationType + writeStatuses（COMMITTED/DELETED 文件列表）+ stats |
| 6 | Delta Log 是什么？| MOR 模式专用；Avro 格式；和 Base File 同目录；记录 UPDATE/DELETE 变更 |
| 7 | MOR 查询 Merge 流程？| 读 Parquet Base File → 读所有关联 Delta Log → 按 Key 合并（Delta 覆盖/标记删除）→ 输出 |
| 8 | Read Optimized vs Snapshot Query？| RO 跳过 Delta Log 只读 Base（稍旧但快）；Snapshot 读最新 Base + 所有 Delta（最准）|
| 9 | Delta Compaction 触发条件？| Delta Log 数量 > threshold（默认 10）或总大小超阈值 |
| 10 | Delta Compaction 做什么？| Base File + Delta Log → 合并写新 Parquet；旧文件清理 |
| 11 | Full Compaction 做什么？| Partition 内所有 Base Files 合并成一个大 Parquet |
| 12 | Async Compaction？| MOR 模式 Compaction 默认异步；后台线程定期触发；不阻塞写入 |
| 13 | Clean Job 清理什么？| 被 Compaction 替代的旧文件 + 旧 .commit 文件 + 已完成 .rollback |
| 14 | Savepoint 作用？| 显式 Time Travel 锚点；Clean Job 不会清理 Savepoint 前的文件；Schema 大改动前先 Savepoint |
| 15 | ★ Hudi vs Iceberg vs Delta Lake 对比？| 见第五节对比表（12 维度）|
| 16 | 面试最高频考点排序？| **1. MOR/COW 取舍 2. Delta Log + Merge 3. Compaction 两种类型 4. Timeline 结构 5. 三格式对比** |
| 17 | 学习路径？| MOR 写入 → Timeline → Delta Log → MOR Merge 查询 → Compaction → Clean |
| 18 | 面试常见追问？| MOR 模式读时合并会不会很慢？| Delta Log 多了会慢 → Compaction 定期把 Delta 合并进 Parquet；Spark RO 查询直接跳过 Delta |
| 19 | 面试常见追问？| Delta Log 为什么不用 Parquet 用 Avro？| Avro 追加快（不需要 Parquet footer 写入完成）；Delta Log 写入量大且频繁 |
| 20 | 面试陷阱？| 说 Hudi 和 Iceberg 一样 → ❌ Hudi 原生支持 MOR（Delta Log），Iceberg 主要 Append-Only；Hudi Timeline 每个 Commit 单独文件，Iceberg 集中 metadata.json |
| 21 | 面试陷阱？| 说 MOR 查询需要重新写整个文件 → ❌ MOR 查询时**不写文件**，只在内存 Merge；写是 Compaction 时才做的 |
| 22 | MOR 读放大？| 每次查询要读 Base + 多个 Delta Log → IO 多；Spark RDD Cache 可以缓解 |
| 23 | Hudi 与 Flink CDC 集成？| Flink CDC → Hudi Sink（MOR 模式）→ 实时写 Delta Log → Compaction 定期合并；**无代码侵入，CDC 自动路由 MOR** |
| 24 | Iceberg vs Hudi Timeline 结构差异？| Iceberg 集中 metadata.json + manifest list 指向多个 manifest；Hudi 每个 Commit 单独文件 + 直接指向 data file |
| 25 | 典型生产用法？| 订单系统：Flink CDC 写 Hudi MOR（高频 Update 写快）→ 报表 Spark RO 查（只读 Base 快）→ T+1 全量 ETL Full Compaction |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：hudi-common/.../HoodieTimeline.java + HoodieTableFileSystemView.java + hudi-table/.../CompactionHelper.java  
**面试考点：25 题**  
**差异化点**：MOR Delta Log（Avro 增量）vs COW 重写、MOR 查询 Merge 流程、Delta vs Full Compaction 两种类型、**Hudi vs Iceberg vs Delta Lake 三格式对比**
