# Flink State Backend Heap vs RocksDB vs HashMap 源码级深度解析

> **源码基线**：Flink 1.18+（Java）  
> **核心位置**：flink-runtime/.../state + flink-statebackend-rocksdb + flink-statebackend-heap  
> **面试权重**：⭐⭐⭐⭐⭐（Flink Checkpoint 核心，补全 162 Flink SS）

---

## 一、State Backend 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink State Backend = 状态存储层（Checkpoint 基础）       │
│                                                               │
│  State Backend 存什么（面试必背）：                           │
│   ✅ ValueState / ListState / MapState / AggregatingState    │
│   ✅ KeyedState（每个 Key 有独立状态）                       │
│   ✅ OperatorState（算子级共享状态，如 Kafka Offset）        │
│   ✅ Window 状态 / Timer 状态                                 │
│                                                               │
│  ★ 三种核心 State Backend（Flink 1.18+）：                    │
│  ┌──────────────────┬──────────────────┬──────────────────┐ │
│  │ HeapStateBackend   │ HashMapStateBackend │ EmbeddedRocksDB │ │
│  │ (纯内存)           │ (纯内存，Flink 1.13+)│ StateBackend   │ │
│  ├──────────────────┼──────────────────┼──────────────────┤ │
│  │ 存储位置           │ TaskManager 堆内存 │ TaskManager 堆 │ 本地磁盘 RocksDB │ │
│  │ Checkpoint         │ 全量序列化        │ 全量序列化       │ ★ 增量 SST 硬链接 │ │
│  │ 重启恢复           │ Checkpoint 反序列化│ Checkpoint 反序列化│ SST 文件直读 │ │
│  │ 状态大小限制       │ ★ 小（几 GB）    │ ★ 中（几十 GB） │ ★ 大（TB 级）    │ │
│  └──────────────────┴──────────────────┴──────────────────┘ │
│                                                               │
│  旧 FsStateBackend（Flink 1.13 前）：已废弃，被 HashMap 替代  │
│  区别：FsStateBackend 把 Checkpoint 写磁盘；HeapStateBackend 写内存 |
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、三种 State Backend 源码级

### 2.1 HeapStateBackend / HashMapStateBackend

```java
// ★ HeapStateBackend / HashMapStateBackend 源码级
//
// flink-statebackend-heap/.../HeapStateBackend.java
// flink-runtime/.../state/HashMapStateBackend.java（1.13+ 推荐）
//
// // HashMapStateBackend 存储实现：
// // 所有 State 存 TaskManager JVM Heap 里
// // 结构：Map<StateName, Map<Key, StateValue>>
//
// public class HashMapStateBackend extends AbstractStateBackend {
//
//     // 内存状态存储
//     public <K, V> KeyedStateBackend<K> createKeyedStateBackend(...) {
//         return new HashMapKeyedStateBackend<>(
//             keySerializer,
//             new HashMap<StateName, Map<Key, StateValue>>()
//         );
//     }
//
//     // ★ Checkpoint 机制（全量）：
//     public Runnable snapshotState(StateSnapshotContext context) {
//         // ① 遍历所有 State
//         // ② 序列化为字节数组（DataOutputStream）
//         // ③ 整个 HashMap 序列化一次
//         // ④ 写 Checkpoint Storage（HDFS/S3/OSS）
//         // ⑤ 全程 Stop-The-World（Task 暂停处理数据）
//         //
//         // 问题：状态 10GB → Checkpoint 也要写 10GB → 慢！
//     }
//
//     // ★ 恢复：
//     public void restore(StateSnapshotContext context) {
//         // 从 Checkpoint Storage 读回整个 HashMap
//         // 反序列化 → 覆盖当前内存状态
//         // 全量反序列化（状态 10GB → 恢复也要 10GB IO）
//     }
// }
//
// // ★ HashMap vs Heap 核心区别（Flink 1.13 引入）：
// // HeapStateBackend：Checkpoint 走网络 Stream → 写目标存储
// // HashMapStateBackend：Checkpoint 直接写目标存储（效率更高）
// // 两者都是**纯内存存储**，Checkpoint 都是**全量序列化**
//
// ★ 适用场景（面试高频）：
//   - 开发/测试环境
//   - 状态很小（< 1GB）
//   - 不需要频繁 Checkpoint
//   - 内存够大（TM 堆内存 > 状态 × 2）
//
```

### 2.2 EmbeddedRocksDBStateBackend

```java
// ★ EmbeddedRocksDBStateBackend 源码级（面试核心）
//
// flink-statebackend-rocksdb/.../EmbeddedRocksDBStateBackend.java
// flink-statebackend-rocksdb/.../RocksDBKeyedStateBackend.java
//
// // 存储：每个 TM 启动 Embedded RocksDB 实例（JNI，本地磁盘）
// // State 存在 RocksDB 的 Column Family 里（Column Family = 一个 State 名称）
//
// public class EmbeddedRocksDBStateBackend extends AbstractStateBackend {
//
//     public KeyedStateBackend createKeyedStateBackend(...) {
//         // 创建 Embedded RocksDB 实例
//         RocksDB db = RocksDB.open(options, basePath);
//         // 每个 State 一个 Column Family
//         db.createColumnFamily(new ColumnFamilyDescriptor(stateName));
//         return new RocksDBKeyedStateBackend(db, keySerializer, stateSerializers);
//     }
//
//     // ★ Checkpoint：**增量 + SST 硬链接（O(1)）**
//     //
//     // RocksDB Checkpoint 原理（面试必背）：
//     // 1. 触发 Checkpoint 时，RockDB 后台 flush memtable → SST
//     // 2. 当前 SST 文件都是不可变的（LSM Tree 天然特性）
//     // 3. ★ 新 SST 用硬链接（hard link）指到 Checkpoint Storage
//     //    Linux hard link = 不复制文件（O(1) 操作）
//     //    → Checkpoint 速度极快！
//     //    → 只需要写新产生的 SST（增量 Checkpoint）
//     //
//     // 4. 旧 SST 可能被 Compaction 合并/删除
//     //    → Checkpoint Storage 用 hard link 保持引用不释放
//     //
//     // 5. 每次 Checkpoint 写一个 _metadata 文件：
//     //    记录当前所有 SST 的清单（文件名 + 位置 + 大小）
//     //    恢复时按这个清单加载 SST
//     //
//     public void checkpoint(String basePath, long checkpointId) {
//         // ★ 硬链接新 SST
//         for (SSTFile sst : rocksDB.getLiveSSTFiles()) {
//             if (sst.sinceLastCheckpoint()) {
//                 // O(1) hard link → checkpoint storage
//                 Files.createLink(sst.path, checkpointPath + "/" + sst.name);
//             }
//         }
//
//         // 写 _metadata 文件
//         writeMetadata(checkpointPath, rocksDB.getLatestSequenceNumber());
//     }
//
//     // ★ 恢复：**直读 SST 文件（O(1)，无需反序列化）**
//     public void restore(String checkpointPath) {
//         // 读 _metadata 文件 → SST 文件列表
//         // 从 Checkpoint Storage 拉 SST 到本地磁盘
//         // RocksDB 直接打开这些 SST 文件 → 恢复完成
//         // 比 HashMap 的全量反序列化快几个数量级！
//     }
//
//     // ★ 增量 Checkpoint：
//     //   RocksDB 支持 incremental checkpoint
//     //   只有自上次 Checkpoint 以来**新产生的 SST** 才同步
//     //   旧 SST 已经在 Checkpoint Storage 里了（硬链接还在）
//     //   → Checkpoint 时间从"全量"→"只同步增量"
//     //   → 超大数据集（TB 级）也能分钟级 Checkpoint
// }
//
```

---

## 三、Checkpoint 机制源码级

### 3.1 全量 vs 增量 Checkpoint

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink Checkpoint 核心机制（面试高频）                      │
│                                                               │
│  Checkpoint 触发（JobManager 侧）：                           │
│   1. JobManager 向所有 Task 发 Barrier                        │
│   2. Task 收到 Barrier → 停处理数据 → 开始 State Snapshot    │
│   3. State Backend snapshotState() → 写 Checkpoint Storage   │
│   4. ACK 给 JobManager → 所有 Task ACK → Checkpoint 完成     │
│                                                               │
│  ★ HashMap vs RocksDB Checkpoint 对比（核心差异）：          │
│                                                               │
│  ┌──────────────┬──────────────────┬──────────────────┐     │
│  │                │ HashMapState     │ RocksDB State      │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Checkpoint 类型 │ ★ 全量序列化      │ ★ 增量 SST 硬链接  │     │
│  │                │ 整个 HashMap 全写 │ 只有新 SST 同步  │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ Checkpoint 速度 │ 慢（状态 × 2 倍 IO）│ ★ 极快（O(1) 硬链接）│ │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 恢复速度       │ 慢（全量反序列化）│ ★ 快（直读 SST）  │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 状态大小上限   │ ★ 中（几十 GB）    │ ★ 大（TB 级）    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 磁盘依赖       │ 不需要            │ ★ 需要本地磁盘    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 内存占用       │ ★ 全状态在 Heap  │ 状态在 RocksDB   │     │
│  │                │                  │ + Block Cache      │     │
│  └──────────────┴──────────────────┴──────────────────┘     │
│                                                               │
│  ★ RocksDB 恢复流程（从 recalled Flink 生产经验提炼）：       │
│   1. JM 通知 TM 启动 Task + 指定 Checkpoint Path             │
│   2. TM 从 Checkpoint Storage 下载 SST 文件到本地磁盘         │
│   3. TM 启动 Embedded RocksDB（直接加载 SST 文件）           │
│   4. Task 启动 → 读 RocksDB State → 恢复完成                 │
│   5. 异步维表查询/Guava Cache 恢复（生产高频 Pitfall）       │
│      // Experience 539622: Guava Cache TTL 需要在恢复后重置  │
│      // // Cache TTL 参数化：通过作业入口 --dim.cache.ttlSeconds│
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Checkpoint 关键配置

```java
// ★ Flink Checkpoint 关键配置（面试高频）
//
// ExecutionCheckpointingOptions.java
// EmbeddedRocksDBStateBackend 配置
//
// // 全局 Checkpoint 配置：
// env.enableCheckpointing(60_000);  // 60s 触发一次 Checkpoint
//
// CheckpointConfig config = env.getCheckpointConfig();
// config.setCheckpointStorage("hdfs:///flink/checkpoints");  // 存储位置
// config.setMaxConcurrentCheckpoints(1);  // 并发 Checkpoint 数
// config.setMinPauseBetweenCheckpoints(5000);  // 两次 Checkpoint 最小间隔
// config.setCheckpointTimeout(10 * 60 * 1000);  // 单次 Checkpoint 超时
// config.setTolerableCheckpointFailureNumber(3);  // 容忍失败次数
// config.setExternalizedCheckpointCleanup(DELETE_ON_CANCELLATION);  // 取消时清理
//
// // RocksDB 专用配置：
// EmbeddedRocksDBStateBackend rocksBackend = new EmbeddedRocksDBStateBackend();
// rocksBackend.setRocksDBOptions(new RocksDBOptions() {
//     // Write Buffer 大小（MemTable 满了 flush）
//     .setWriteBufferSize(128 * 1024 * 1024)  // 128MB
//     // 合并线程数
//     .setBackgroundCompactionThreads(4)
//     // 本地磁盘路径
//     .setDbStoragePath("file:///tmp/flink/rocksdb")
//     // 增量 Checkpoint 开关
//     .setUseIncrementalCheckpoint(true)  // 默认 true
//     // Compaction Filter 加速
//     .enableCompactFilter();
// });
//
// // ★ 生产调优经验（来自 Experience 1602163 用户画像项目）：
// //
// // 问题 1：Flink 状态膨胀 → RocksDB 磁盘快满
// //   调优：
// //   a. 用 MapState 代替完整 POJO（减少序列化开销）
// //   b. 设置 state.ttl（过期状态自动清理）
// //   c. RocksDB Compaction Filter + tombstone 提前清理
// //
// // 问题 2：Checkpoint 超时
// //   调优：
// //   a. checkpointTimeout 调大（10min → 20min）
// //   b. RocksDB Compaction 放在 Checkpoint 间隙（background）
// //   c. Async Checkpoint（异步快照，不阻塞 Task）
// //
// // 问题 3：维表 JOIN / 异步查询恢复（Experience 539622）
// //   维表 JDBC 连接 Checkpoint 后需要重新建立
// //   Guava Cache TTL 参数化（作业级配置）
// //   Async MySQL Dim Lookup 在恢复后主动 flush 缓存
//
```

---

## 四、三种 State Backend 全维度对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种 State Backend 全维度对比                    │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐ │
│  │ 维度      │ Heap/HashMap  │ EmbeddedRocksDB │ 备注          │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 存储介质   │ ★ 纯 Heap    │ 本地磁盘      │ RocksDB JNI    │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ Checkpoint │ ★ 全量序列化  │ ★ 增量 SST    │ 硬链接 O(1)   │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 恢复速度   │ ★ 慢（反序列化）│ ★ 快（直读 SST）│ 状态越大优势越明显│ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 状态上限   │ ★ 几十 GB    │ ★ TB 级       │ 受磁盘限制     │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 内存占用   │ ★ 全状态在 Heap │ RocksDB +     │ Block Cache    │ │
│  │            │              │ Block Cache   │ 不在 Heap      │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 依赖       │ 无           │ 本地磁盘      │ + RocksDB 库  │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 生产推荐   │ ❌ 状态大    │ ★ 推荐        │ 流式生产首选   │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 开发测试   │ ★ 方便      │ 也可用        │ 视状态大小     │ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 异步快照   │ ❌ 不支持     │ ★ 支持        │ Backpressure 友好│ │
│  ├──────────┼──────────────┼──────────────┼──────────────┤ │
│  │ 面试高频   │ ★★★        │ ★★★★★        │ RocksDB 是核心 │ │
│  └──────────┴──────────────┴──────────────┴──────────────┘ │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   状态 < 1GB + 开发测试 → **HashMapStateBackend**（简单）    │
│   状态 1GB+ + 生产流式 → **EmbeddedRocksDBStateBackend**（必选）│
│   状态 > 100GB + 分钟级 Checkpoint → RocksDB 增量 Checkpoint  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 三种 State Backend 核心？| **HashMap（全内存 + 全量序列化）+ EmbeddedRocksDB（磁盘 + 增量 SST 硬链接）** |
| 2 | RocksDB Checkpoint 为什么快？| **SST 文件不可变（LSM Tree 特性）+ hard link 硬链接 O(1) 同步 + 增量 Checkpoint 只同步新 SST** |
| 3 | EmbeddedRocksDBStateBackend 存储结构？| 每个 TM 一个 Embedded RocksDB 实例；**每个 State 一个 Column Family**；State 存 RocksDB |
| 4 | HashMapStateBackend Checkpoint？| 整个 HashMap 全量序列化 → Checkpoint Storage；Stop-The-World |
| 5 | ★ Checkpoint 恢复流程（RocksDB）？| JM 指定路径 → TM 下载 SST → Embedded RocksDB 直加载 SST → Task 启动 → **O(1) 恢复（比反序列化快几个量级）** |
| 6 | State Backend 选型核心？| 状态大小 + Checkpoint 频率 + 内存/磁盘资源 |
| 7 | 增量 Checkpoint 是什么？| 只有自上次 Checkpoint 以来**新产生的 SST** 才同步；旧 SST 已在 Checkpoint Storage 保持硬链接 |
| 8 | 异步快照（Async Snapshot）？| RocksDB 支持异步快照 → Task 可以不暂停太久；HashMap 不支持 |
| 9 | FsStateBackend 废弃原因？| 和 HeapStateBackend 功能重叠；HashMapStateBackend（Flink 1.13+）替代 |
| 10 | Checkpoint Barrier 机制？| JobManager 发 Barrier → Task 收到 → 停处理 → snapshotState → ACK；所有 Task ACK → Checkpoint 完成 |
| 11 | 状态膨胀怎么处理？| **state.ttl（过期状态自动清理）+ 状态分桶 + MapState 代替完整 POJO + RocksDB Compaction Filter** |
| 12 | ★ Checkpoint 超时排查？| 状态太大 → Checkpoint 时间过长；调 checkpointTimeout + 优化状态结构 |
| 13 | Guava Cache TTL 恢复问题（Experience 539622）？| 异步维表查询的 Cache TTL 需要**参数化**（作业入口传 --dim.cache.ttlSeconds）；恢复后主动重置 |
| 14 | EmbeddedRocksDB 需要什么依赖？| **本地磁盘**（存储 RocksDB WAL + SST）+ RocksDB JNI 库 |
| 15 | RocksDB hard link 的前提？| Linux 文件系统支持 hard link；Checkpoint Storage 必须是**共享存储**（HDFS/S3/OSS）|
| 16 | 面试最高频考点排序？| **1. RocksDB SST 硬链接 Checkpoint 2. 三种 Backend 全维度对比 3. Checkpoint 恢复流程 4. 状态膨胀调优 5. 选型建议** |
| 17 | 学习路径？| Checkpoint 触发 → State Backend snapshot → RocksDB SST hard link → 恢复加载 |
| 18 | 面试常见追问？| RocksDB Checkpoint Storage 用 S3 可以吗？| 可以！Flink 支持 S3 Checkpoint Storage；hard link 原理在 S3 上等效（多 key 指向同对象）|
| 19 | 面试常见追问？| 状态 100GB，用哪种 Backend？| **必须 EmbeddedRocksDBStateBackend**（HashMap 全量序列化 Checkpoint 要几个小时）|
| 20 | 面试陷阱？| 说 "RocksDB 比 HashMap 快" → ❌ 内存查询 HashMap 更快；**Checkpoint 和恢复 RocksDB 快**（这是核心优势）|
| 21 | 面试陷阱？| 说 "RocksDB Checkpoint 不会失败" → ❌ S3/HDFS 可能临时不可用；配置 checkpoint_timeout + tolerableCheckpointFailureNumber |
| 22 | Checkpoint vs Savepoint 区别？| Checkpoint：作业恢复；Savepoint：状态迁移/版本升级/手动暂停（格式兼容）|
| 23 | EmbeddedRocksDBStateBackend 的 Embedded 是什么？| **RocksDB 作为 Flink 进程内嵌 JNI 库**；不是独立服务（区别于 ExternalRocksDBStateBackend）|
| 24 | Flink 1.14+ 默认 State Backend？| **HashMapStateBackend**；生产一般改成 EmbeddedRocksDBStateBackend |
| 25 | State Backend 集群级配置？| flink-conf.yaml：state.backend: rocksdb；state.checkpoints.dir: hdfs:///flink/checkpoints |
| 26 | 状态 Key Group 与 Checkpoint？| Flink 把 Key 哈希到 Key Group；Checkpoint 按 Key Group 分片 → 并行化 Checkpoint |
| 27 | 典型生产用法？| Flink CDC → EmbeddedRocksDBStateBackend + 10min Checkpoint + RocksDB 增量 + Guava Cache TTL 参数化（Experience 539622 解决方案）|

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：flink-statebackend-rocksdb/EmbeddedRocksDBStateBackend.java + flink-runtime/HashMapStateBackend.java  
**面试考点：27 题**  
**差异化点**：RocksDB SST 硬链接 O(1) Checkpoint、全量 vs 增量 Checkpoint 机制、**三种 Backend 全维度对比表**、生产调优（状态膨胀/超时/恢复）、Guava Cache TTL 参数化（Experience 539622 提炼）
