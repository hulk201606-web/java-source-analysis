# TiDB CDC / TiCDC 增量数据同步源码级深度解析

> **源码基线**：TiCDC v8.0.x（Go）+ TiKV v8.0.x  
> **核心位置**：pingcap/tiflow → cdc/ + tikv components/cdc  
> **面试权重**：⭐⭐⭐⭐（TiDB 生态日志订阅核心，数据集成高频）

---

## 一、TiCDC 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiCDC = TiDB Change Data Capture（增量数据捕获）         │
│                                                               │
│  作用：把 TiKV 的实时数据变更（行级变更事件）订阅出来        │
│   → 下沉到下游：MySQL、Kafka、Pulsar、S3、对象存储          │
│                                                               │
│  典型用途（面试必背）：                                        │
│   ① 实时数仓：TiDB → Kafka → Flink/Doris/Iceberg            │
│   ② 异构数据库实时同步（TiDB → MySQL 只读副本）              │
│   ③ 缓存失效/搜索索引更新（变更事件驱动 ES/Redis）           │
│   ④ 跨区域容灾（异步复制到异地 TiDB/MySQL）                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiCDC 架构组件                                            │
│                                                               │
│  ┌──────────┐  ChangeData 请求（订阅 Region Raft 日志）       │
│  │  TiKV    │◄──────────────┐                                │
│  │ (Raft)   │────RawData────┘                                │
│  │          │  CDC 在 TiKV 端是 Raft Learner/特殊副本 ★      │
│  └──────────┘                                                │
│       │ 变更事件（RowChangedEvent：PK+旧行+新行）            │
│       ↓                                                       │
│  ┌──────────────────────────────────────────────┐           │
│  │ TiCDC 集群（无状态节点，PD 调度 Changefeed）   │           │
│  │  ┌────────────┐  ┌────────────┐               │           │
│  │  │ Owner      │  │ Processor   │               │           │
│  │  │（调度者）   │  │（执行同步）  │               │           │
│  │  └────────────┘  └────────────┘               │           │
│  │  Sorter（排序器，解决多 Region 乱序）          │           │
│  └──────────────────────────────────────────────┘           │
│       │ MQ Protocol（canal-json / open-protocol / debezium） │
│       ↓                                                       │
│  ┌────────┬────────┬────────┬────────┐                      │
│  │ MySQL  │ Kafka  │ Pulsar │ S3/湖  │ 下游 Sink             │
│  └────────┴────────┴────────┴────────┘                      │
│
│ ★ TiCDC 节点无状态：进度和元数据存 PD（etcd）               │
│   节点宕机 → Owner 把 Changefeed 重新调度到其他节点         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、核心原理：Raft 日志订阅

### 3.1 CDC 作为特殊 Raft 副本

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiCDC 不扫 binlog（TiDB 无 binlog 概念），而是订阅 Raft Log│
│                                                               │
│  关键设计（面试核心）：                                        │
│   TiCDC 在 TiKV 端表现为一个特殊的 Raft 角色：               │
│   ★ CDC Learner / CDC Peer（不参与投票、不占法定人数）      │
│                                                               │
│  // tikv components/cdc/src/delegate.rs                      │
│  // TiKV CDC 端点（Endpoint）维护：                           │
│  //  - 每个被订阅 Region 一个 Capture/Delegate               │
│  //  - 从 Raftstore 拿到已 apply 的 Raw Change Data         │
│  //  - Old Value / Row 编码由 TiKV 侧组装                    │
│                                                               │
│  数据流：                                                      │
│   ① TiKV Raft apply 一条写请求（Prewrite/Commit）            │
│   ② CDC Delegate 从 apply 结果提取行变更                      │
│      - Commit 时产出 RowChangedEvent                         │
│      - 含 StartTS/CommitTS、PK、旧值(old)、新值(new)        │
│   ③ 通过 gRPC stream（ChangeData）批量推给 TiCDC Processor   │
│   ④ TiKV 端按 resolved_ts 推进水位（旧值可见性）            │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 resolved_ts 与顺序保证

```go
// ★ resolved_ts 是 TiCDC 一致性的核心（面试高频）
//
// // tiflow/cdc/.../resolved_ts
//
// 含义：该时间戳之前的所有事务变更都已经被收到
//      （不会再有 commit_ts ≤ resolved_ts 的事件迟到）
//
// 推进条件（类似 Flink Watermark）：
//   resolved_ts = min(
//       各被订阅 Region 的 resolved_ts,
//       所有未决事务的 min CommitTS,
//       下游确认进度
//   )
//
// 作用：
//   ① 下游可以放心输出 resolved_ts 之前的事件（顺序完整）
//   ② 多 Region 事件乱序到达 TiCDC → 靠 resolved_ts + Sorter 排序
//   ③ 支撑 sink 的事务一致性输出
//
```

### 3.3 Sorter 排序器

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 为什么需要 Sorter？                                       │
│                                                               │
│  一张表有多个 Region，分布在不同 TiKV：                       │
│   Region-1 事件 ts=105 先到                                  │
│   Region-2 事件 ts=101 后到（网络乱序）                      │
│   → 直接下发给下游会乱序                                     │
│                                                               │
│  Sorter（旧版独立 soror 集群；新版内建 DB sorter）：         │
//  // tiflow/cdc/sorter/                                       │
│   ① 按 key + commit_ts 排序变更事件                          │
│   ② 内存排序 + 落盘（RocksDB/LevelDB）                       │
│   ③ 按 resolved_ts 水位输出有序结果                          │
│                                                               │
│  新版（TiCDC 6.x+）：                                         │
│   Sorter 下沉到 TiCDC 节点内嵌（不再独立部署 soror）         │
│   基于 DB（本地 RocksDB）+ 内存 heap                         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Changefeed 任务模型

### 4.1 Owner + Processor

```go
// ★ TiCDC 任务调度（源码级）
//
// // tiflow/cdc/scheduler/ + cdc/capture.go
//
// Owner（同一时刻一个，通过 PD 竞选）：
//   - 管理所有 Changefeed 的生命周期
//   - 调度：把表（Table）分配给各 Processor（均衡）
//   - 收集心跳、处理节点故障转移
//
// Processor（每个 TiCDC 节点一个）：
//   - 接收被分配的表，对每张表启动一个"管道"
//   - 向 TiKV 建 ChangeData gRPC stream 拉变更
//   - Sink 到下游
//
// Changefeed 配置（cdc cli changefeed create）：
//   --sink-uri="kafka://broker/topic?protocol=canal-json"
//   --start-ts（从哪个 TSO 开始同步）
//   --config changefeed.toml（过滤规则、old-value、mounter 线程）
//
// 调度单位演进（面试加分）：
//   旧：以 Table 为单位调度（粗粒度，大表倾斜）
//   新（4.0+/TiDB 6+）：★ 以 Region 为单位调度
//      → 大表的多个 Region 可以分散到多个 TiCDC 节点并行
//      → 解决单大表同步瓶颈
//
```

### 4.2 Changefeed 状态机

```
┌─────────────────────────────────────────────────────────────┐
│  Normal（正常）→ 遇到可重试错误 → Stopped/Error              │
│                                                               │
│  关键状态：                                                    │
│   Normal：正常同步                                            │
│   Stopped：手动暂停（可 resume）                              │
│   Error：报错（gc-ttl 内可恢复）                              │
│   Failed：不可恢复（checkpoint 落后太多，被 GC 清了）         │
│   Finished：到指定 target-ts 完成                             │
│
│ ★ GC safepoint 与 TiCDC 的关系（面试高频）：                  │
│   TiCDC 注册自己的同步进度到 PD（作为 GC safepoint 限制者）  │
│   → TiKV GC 不会清理 TiCDC 还没读到的旧版本                  │
│   → 如果 TiCDC 长时间停滞超过 gc-ttl（默认 24h）：           │
│     GC 推进 → 需要的旧版本被删 → Changefeed Failed 不可恢复  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、Sink 与输出协议

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 下游 Sink 与消息协议（面试高频）                          │
│                                                               │
│  ① MySQL Sink：                                               │
│     把行变更转成 DML（INSERT/UPDATE/DELETE）写下游 MySQL     │
│     按 worker + 主键分发，保证同 key 顺序                    │
│                                                               │
│  ② Kafka/Pulsar Sink：                                        │
│     协议（protocol）：                                        │
│      - canal-json：兼容 Canal（生态广，最常用）              │
│      - open-protocol：TiCDC 原生（含 old/new/ts）            │
│      - debezium：兼容 Debezium 生态                           │
│      - avro：Schema Registry                                  │
│      - maxwell：兼容 Maxwell                                  │
│     分区策略：按主键 hash（同 key 有序）或按表                │
│                                                               │
│  ③ 云存储 Sink（S3/GCS/NFS）：                                │
│     Canal-JSON / CSV / Parquet，支持数据湖                    │
│                                                               │
│  消息内容（RowChangedEvent）：                                │
│   { startTs, commitTs, oldRow: {...}, newRow: {...}, type } │
│   type = INSERT / UPDATE / DELETE / PRECOMMIT...            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、TiCDC vs 其他 CDC 方案对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiCDC vs Flink CDC（Debezium）vs Canal（面试必背）        │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │            │ TiCDC        │ Flink CDC    │ Canal        │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 数据源     │ TiDB/TiKV    │ MySQL/PG/Mongo│ MySQL        │        │
│  │ 捕获原理   │ ★订阅 Raft Log│ 读 binlog    │ 伪装 slave   │        │
│  │            │（CDC Learner）│（Debezium）  │ 读 binlog    │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 顺序/水位  │ resolved_ts   │ watermark    │ 单线程解析   │        │
│  │ 部署       │ 独立集群      │ Flink 作业   │ 单服务       │        │
│  │ 下游       │ MySQL/Kafka/S3│ 任意（Flink）│ Kafka/MQ     │        │
│  │ 大表扩展   │ Region 级并行│ 并行 source  │ 弱           │        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│
│  ★ TiCDC 独特优势：原生感知 MVCC 时间戳，resolved_ts 保证顺序│
│    不依赖 binlog（TiDB 本无 binlog），Raft 级别低延迟        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiCDC 是什么？| 捕获 TiKV 实时行变更并下沉到 MySQL/Kafka/对象存储的增量同步组件 |
| 2 | 捕获原理？| **不读 binlog，订阅 Raft Log**；TiKV 端 CDC 特殊副本（不投票）从 apply 提取变更 |
| 3 | 事件内容？| RowChangedEvent：startTs/commitTs + 主键 + 旧行 + 新行 + 类型 |
| 4 | resolved_ts？| **该 ts 之前的变更都已收到**，下游可安全按序输出（类似 Watermark）|
| 5 | 为什么需要 Sorter？| 多 Region 乱序到达，按 key+commitTs 排序，按 resolved_ts 水位输出 |
| 6 | 架构角色？| Owner（调度）+ Processor（执行）；无状态，进度存 PD |
| 7 | 调度粒度演进？| 旧：Table 级（大表倾斜）；新：**Region 级并行**（大表分散多节点）|
| 8 | Changefeed 状态？| Normal/Stopped/Error/Failed/Finished |
| 9 | 与 GC 的关系？| TiCDC 把进度注册为 GC safepoint；停滞超 gc-ttl(24h) 旧版本被删 → Failed 不可恢复 |
| 10 | 下游协议？| canal-json（最常用）/ open-protocol / debezium / avro / maxwell |
| 11 | 消息怎么保序？| Kafka 按主键 hash 分区，同 key 进同分区有序 |
| 12 | TiCDC vs Canal？| TiCDC 订阅 Raft Log + resolved_ts；Canal 伪装 slave 读 MySQL binlog |
| 13 | TiCDC vs Flink CDC？| TiCDC 独立集群原生订阅 TiKV；Flink CDC 读 binlog（Debezium）跑在 Flink |
| 14 | 面试最高频考点排序？| **1. 订阅 Raft Log 原理 2. resolved_ts 3. Sorter 排序 4. GC safepoint 5. vs Canal/Flink CDC** |
| 15 | 学习路径？| TiKV cdc/delegate → ChangeData gRPC → resolved_ts → Owner/Processor → Sink 协议 |
| 16 | 面试常见追问？| TiCDC 延迟？→ 通常秒级；延迟高查 Sorter/下游反压/Region 调度 |
| 17 | TiCDC 节点挂了？| 无状态，Owner 把表/Region 重新调度，从 checkpoint-ts 恢复 |
| 18 | old value 哪来？| TiKV 侧根据 MVCC 旧版本组装（需开启 old-value，默认开）|
| 19 | Kafka Sink 一致性？| resolved_ts 水位 + 幂等/事务，至少一次；下游按 key 去重 |
| 20 | start-ts 作用？| Changefeed 从指定 TSO 开始全量+增量（需早于 GC 水位）|
| 21 | 面试高频场景？| 实时数仓（TiDB→Kafka→Doris）+ 异构同步 + 缓存/ES 更新 + 异地容灾 |
| 22 | 面试陷阱？| 说 TiCDC 读 TiDB binlog → ❌ TiDB 无 binlog，TiCDC 订阅 TiKV Raft apply 的变更 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：tiflow/cdc + tikv/components/cdc + resolved_ts + scheduler  
**面试考点：22 题**  
**差异化点**：Raft Log 订阅（CDC Learner）+ resolved_ts 水位 + Sorter 排序 + Region 级调度 + vs Canal/Flink CDC
