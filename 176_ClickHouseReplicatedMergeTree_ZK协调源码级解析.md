# ClickHouse ReplicatedMergeTree + ZooKeeper 协调源码级深度解析

> **源码基线**：ClickHouse 23.x / 24.x（C++）  
> **核心位置**：src/Storages/ReplicatedMergeTree + src/Interpreters + src/ZooKeeper  
> **面试权重**：⭐⭐⭐⭐⭐（ClickHouse 集群复制核心，高频）

---

## 一、ReplicatedMergeTree 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ReplicatedMergeTree = ClickHouse 集群级复制引擎           │
│                                                               │
│  核心机制（面试必背）：                                        │
│   ✅ ZooKeeper 协调（类似 MySQL MGR 的 Paxos）                 │
│   ✅ 异步复制（最终一致，不是强一致）                          │
│   ✅ 多主复制（多个 Replica 都能写入）                        │
│   ✅ Merge 也同步（不只是数据）                               │
│   ✅ 自动故障转移（ZK 选主）                                  │
│                                                               │
│  与 MergeTree（单机）的区别：                                 │
│   MergeTree：单节点、无复制、后台本地 Merge                   │
│   ReplicatedMergeTree：集群多节点、ZK 协调、跨节点同步       │
│                                                               │
│  ★ 与 TiKV Raft / MySQL MGR 的核心对比（第六节）：           │
│   ReplicatedMergeTree → ZK 协调 + 异步 + 列式存储            │
│   TiKV Raft → Raft CP 强一致 + KV 存储                       │
│   MySQL MGR → Paxos + GTID + 行式存储                       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、ZooKeeper 路径结构源码级

```cpp
// ★ ReplicatedMergeTree ZK 路径结构（面试核心源码级）
//
// src/Storages/ReplicatedMergeTree/ReplicatedMergeTree.cpp
// src/ZooKeeper/ZooKeeperClient.h
//
// // 每个 ReplicatedMergeTree 在 ZK 上有一个路径：
// // /clickhouse/tables/{shard_uuid}/
// //   这个路径下有多个 Replica（节点副本）
//
// ZK 目录树（完整结构，面试高频）：
//
// /clickhouse/tables/                       ← 所有 Replicated 表根
// ├── 01234567-89ab-cdef-0123-456789abcdef  ← Table UUID（每个 Replicated 表一个）
// │   ├── leader-election                   ← 选主节点
// │   │   ├── node_0001                     ← 节点1（临时 ZNode）
// │   │   └── node_0002                     ← 节点2
// │   ├── log/                              ← ★ 复制 LogEntry（核心！）
// │   │   ├── 000001                        ← Log Entry #1
// │   │   ├── 000002                        ← Log Entry #2
// │   │   ├── ...
// │   │   └── 000123                        ← Log Entry #123
// │   ├── columns.txt                       ← 列定义（Schema）
// │   ├── checksum.txt                      ← 数据校验和
// │   └── replicas/                         ← 各 Replica 状态
// │       ├── host1:9000/                   ← Replica 1（永久 ZNode）
// │       │   ├── queue/                    ← 已消费的 LogEntry 队列
// │       │   ├── marker                    ← 当前消费到的 LogEntry
// │       │   └── blocks/                   ← 已有 Part 列表
// │       └── host2:9000/                   ← Replica 2
// │           ├── queue/
// │           ├── marker
// │           └── blocks/
// └── 另一个 UUID/...
//
// ★ 关键机制理解（面试高频）：
//   - **LogEntry 是复制的核心数据**：每个写操作在 ZK 上产生一个 LogEntry
//   - **每个 Replica 有自己的 queue + marker**：记录消费进度
//   - **leader-election**：选主节点（负责产生 Merge 计划）
//   - **临时 ZNode**：Replica 挂掉 → ZNode 自动消失 → 触发故障转移
//
```

---

## 三、LogEntry 类型 + 复制流程源码级

### 3.1 四种 LogEntry 类型

```cpp
// ★ ReplicatedMergeTree LogEntry 四种类型（面试必背）
//
// src/Storages/ReplicatedMergeTree/ReplicatedMergeTreeLogEntry.h
//
// enum LogEntryType {
//     INSERT_BLOCK,    // ① 写入新 Part（最常见）
//     MERGE,           // ② Merge 多个 Part（后台合并也同步！）
//     MUTATE,          // ③ ALTER（加列/改类型/删除）
//     GET_PART,        // ④ 从其他 Replica 拉取缺失的 Part
// };
//
// // LogEntry 结构（ZK 上存储的内容）：
// struct LogEntry {
//     int64_t version;                 // 序列号（ZK 上按序递增）
//     LogEntryType type;               // INSERT_BLOCK / MERGE / MUTATE / GET_PART
//     String source_replica;           // 产生这个 Entry 的 Replica 地址
//     String destination_replica;      // GET_PART 时：向哪个 Replica 要 Part
//     String part_name;                // 相关 Part 名（INSERT_BLOCK 或 MERGE 结果）
//     String merge_from / merge_to;    // MERGE 类型：源 Part 列表 → 目标 Part
//     time_t create_time;              // 创建时间
//     int32_t num_tries;               // 重试次数
//     String exception;                // 上次失败原因
//     // ...
// };
//
```

### 3.2 写入触发复制全链路

```cpp
// ★ ReplicatedMergeTree INSERT 全链路（源码级，面试核心）
//
// src/Storages/ReplicatedMergeTree/ReplicatedMergeTree.cpp
// src/Storages/MergeTree/MergeTreeDataPartWriterOnDisk.cpp
//
// // 1. 用户向 Replica A 执行 INSERT INTO tbl VALUES (...)
// //    A 先在本地 MergeTree 写入 Part（和单机 MergeTree 一样）
// StorageMergeTree::write(...) {
//     writeDataPart();  // 本地写 Part（原子完成）
// }
//
// // 2. 本地 Part 写完 → 向 ZK 写 INSERT_BLOCK LogEntry
// //    这一步是"通知其他 Replica 有新 Part"
// ReplicatedMergeTree::writeLogEntry(LogEntry(INSERT_BLOCK, A.host, part_name));
// //    ZK 上 /clickhouse/tables/{uuid}/log/000123 = {type=INSERT_BLOCK, source=A, part=20241026_123456_7890_1}
//
// // 3. ★ 其他 Replica（B、C）监听 ZK 的 log/ 目录变化
// //    Watcher 触发 → 发现新 LogEntry → 检查自己有没有消费过
// ZooKeeper watcher 触发:
// for (auto replica : other_replicas) {
//     if (!replica.queue.contains(logEntry.version)) {
//         // ★ 自己没消费过 → 处理这个 LogEntry
//         replica.executeLogEntry(logEntry);
//     }
// }
//
// // 4. Replica B 处理 INSERT_BLOCK：从 A 拉 Part
// void ReplicaB::executeLogEntry(LogEntry entry) {
//     if (entry.type == INSERT_BLOCK) {
//         // ★ 不是重放 INSERT！而是直接拉 Part 文件
//         fetchPart(entry.source_replica, entry.part_name);
//         //    // HTTP 从 Replica A 拉 Part 文件到本地
//         //    // 写到自己的 MergeTree 目录
//     }
//
//     // 5. 标记已消费
//     this.queue.add(entry.version);
//     this.marker.set(entry.version);
// }
//
// // ★ 关键点：ReplicatedMergeTree 复制的是 Part 文件，不是 SQL 重放！
// //   为什么？
// //   ① MergeTree Part 是列式存储文件，直接 HTTP 拉高效
// //   ② Part 是不可变的，天然可复制
// //   ③ 比 SQL 重放更简单（不依赖 SQL 语义）
// //   ④ 这就是为什么 Merge 也能同步（MERGE LogEntry 也通知拉新 Part）
//
```

### 3.3 Merge + Mutate 也同步

```cpp
// ★ Merge + Mutate 同步流程（源码级）
//
// // Merge 同步（非 Replicated 节点的 Merge 只影响自己；Replicated 的 Merge 全局同步！）：
// Replica A 的后台线程触发 Merge：
//   Merge A 的 Part 1 + Part 2 → 新 Part 3
//   → 本地写 Part 3
//   → 向 ZK 写 MERGE LogEntry（merge_from=[part1, part2], merge_to=part3）
//   → Replica B/C 收到 MERGE LogEntry
//     → 从 A 拉 Part 3（不需要重跑 Merge，直接拉结果）
//     → 本地也有 Part 3 了
//     → B/C 本地也执行 Merge（也会各自写 MERGE LogEntry，自动去重）
//
// // ★ Mutate 同步：
// // ALTER TABLE tbl ADD COLUMN new_col UInt64 DEFAULT 0
// // → ZK 写 MUTATE LogEntry
// // → 所有 Replica 收到后同时执行 ALTER
// // → Schema 变化全局一致
//
// // ★ Get-Part 场景：
// // Replica C 因为重启漏了几个 Part → 启动后发现自己的 marker 落后
// //   → 主动向其他 Replica 发 GET_PART LogEntry
// //   → 从最新 Replica 拉所有缺失的 Part
// //   → 追上进度后 marker 对齐
//
// // ★ 复制进度对齐机制：
// // 每个 Replica 在 ZK 上维护自己的 marker（消费到的最新 LogEntry version）
// // 可以查出整个集群哪个 Replica 最慢：
// // SELECT * FROM system.replicated_logs;
// //   table_uuid | source_replica | marker | total_entries | diff
// //   01234...   | host1          | 120    | 123           | 3
// //   01234...   | host2          | 110    | 123           | ★ 10（慢！）
//
```

---

## 四、故障转移 + 选主源码级

```cpp
// ★ ReplicatedMergeTree 故障转移（源码级）
//
// // ZK 的 leader-election 临时 ZNode 机制：
// // ZNode: /clickhouse/tables/{uuid}/leader-election/node_{seq}
// // 每个 Replica 启动时创建一个临时 ZNode（序号递增）
// // 序号最小的成为 Leader（负责产生 Merge 计划）
// //
// // Leader 崩溃后：
// //   1. ZK Session 过期 → 临时 ZNode 自动消失
// //   2. 其他 Replica 监听 leader-election 目录变化
// //   3. 序号最小的 Replica 发现自己是 Leader
// //   4. 开始执行 Merge 计划（后台 Merge 线程接管）
// //
// // 这个过程完全自动、秒级完成
//
// // ★ 数据一致性：
// // ReplicatedMergeTree 是**异步最终一致**
// //   - 主 Replica 写完 Part → 立刻返回给用户（不等其他 Replica）
// //   - 其他 Replica 在后台异步拉 Part
// //   - 时间差一般毫秒级，但网络抖动时可能秒级
// //   - 这与 TiKV Raft 的强一致（等过半节点 ACK）完全不同
// //
// // ★ 脑裂避免：
// //   Leader 选举靠 ZK（CP 系统）→ 同一时刻只有一个 Leader
// //   不会出现"两个 Leader 同时写"的脑裂问题
// //   但写入是多主（任何 Replica 都能 INSERT）
// //   Leader 只负责 Merge 计划，不负责写入路由
//
```

---

## 五、TiKV Raft vs MySQL MGR vs ReplicatedMergeTree 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比：三大分布式复制方案（深入版）                 │
│  ┌──────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度      │ ReplicatedMergeTree │ MySQL MGR     │ TiKV Raft││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 协调协议  │ ★ ZooKeeper       │ Paxos         │ ★ Raft   ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 一致性    │ ★ 异步最终一致    │ ★ Paxos强一致 │ ★ Raft强一致││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 复制单元  │ Part 文件（列式）  │ GTID + Binlog │ KV Batch  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 复制方式  │ ★ HTTP 拉 Part    │ Dump 推 Binlog│ Raft Log  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Merge 同步│ ★ 全局同步        │ ❌ 不支持     │ ❌ 不支持 ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 写延迟    │ ★ 极低（异步）    │ 中（等 Paxos）│ 中（等 Raft）││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 脑裂保护  │ ZK Leader 选举    │ Paxos 自动    │ Raft 自动 ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 故障转移  │ ★ 秒级（ZK Watch）│ 秒级          │ 秒级      ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 数据模型  │ ★ 列式（MergeTree）│ 行式（InnoDB）│ KV（RocksDB）││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 适用场景  │ ClickHouse 数仓   │ OLTP 主从     │ TiDB HTAP ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频  │ ★★★★★            │ ★★★★★        │ ★★★★★    ││
│  └──────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   数仓/OLAP → **ReplicatedMergeTree**（Merge 全局同步、列式）│
│   OLTP 强一致 → TiKV Raft / MySQL MGR（Raft/Paxos CP 保证）│
│   跨 Region 数仓 → ClickHouse + Kafka/Distributed 异步同步   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（25 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ReplicatedMergeTree 是什么？| ClickHouse 集群级复制引擎；**ZK 协调 + Part 文件复制 + Merge 全局同步** |
| 2 | ★ ZK 路径结构？| `/clickhouse/tables/{uuid}/` → log/（LogEntry）+ replicas/{host}/ + leader-election + columns.txt |
| 3 | LogEntry 四种类型？| **INSERT_BLOCK + MERGE + MUTATE + GET_PART** |
| 4 | INSERT_BLOCK 复制流程？| 本地写 Part → ZK 写 LogEntry → 其他 Replica Watcher 触发 → **HTTP 拉 Part 文件**（不是 SQL 重放）|
| 5 | 为什么复制 Part 而不是 SQL 重放？| Part 是列式不可变文件 → HTTP 拉高效；比 SQL 重放简单；Merge 也能同步 |
| 6 | MERGE LogEntry 作用？| Replica 后台 Merge 后 → 写 MERGE LogEntry → 其他 Replica 也拉新 Part（Merge 全局同步）|
| 7 | Mutate（ALTER）同步？| ALTER 后写 MUTATE LogEntry → **所有 Replica 同时执行 ALTER** → Schema 全局一致 |
| 8 | GET_PART 场景？| Replica 重启后 marker 落后 → 主动发 GET_PART LogEntry → 从其他 Replica 拉缺失 Part |
| 9 | 复制进度对齐？| 每个 Replica 在 ZK 上维护 queue + marker；SELECT FROM system.replicated_logs 查谁最慢 |
| 10 | 故障转移怎么实现？| ZK leader-election 临时 ZNode 机制；Leader 崩溃 → ZNode 消失 → 序号最小的 Replica 成为新 Leader |
| 11 | 一致性级别？| **异步最终一致**；主 Replica 写完 Part 立刻返回（不等其他 Replica）；毫秒级延迟 |
| 12 | 脑裂保护？| Leader 选举靠 ZK（CP）→ 同一时刻只有一个 Leader；多主写入（不路由到 Leader）|
| 13 | ★ ReplicatedMergeTree vs TiKV Raft vs MySQL MGR？| 见第五节对比表（10 维度）|
| 14 | 面试最高频考点排序？| **1. ZK 路径结构 2. LogEntry 四种类型 3. INSERT_BLOCK 全链路 4. 故障转移 ZNode 机制 5. 三者对比** |
| 15 | 学习路径？| ZK 路径 → LogEntry 类型 → INSERT_BLOCK 流程 → Merge 同步 → 故障转移 |
| 16 | 面试常见追问？| 其他 Replica 拉 Part 失败怎么办？| Get-Part 自动重试（num_tries 计数器）；失败原因写 LogEntry.exception；超过阈值告警 |
| 17 | 面试常见追问？| 为什么 Merge 也要同步？| ClickHouse Merge 会产生新的大 Part → 如果不同步，查询时每个 Replica 各自 Merge 会产生不同的 Part（Schema 不一致）|
| 18 | 面试陷阱？| 说 ReplicatedMergeTree 是"同步强一致"→ ❌ 是**异步最终一致**；写入不等其他 Replica |
| 19 | 面试陷阱？| 说 ReplicatedMergeTree 是"主从复制"→ ❌ 是**多主复制**；任何 Replica 都能 INSERT；Leader 只负责 Merge 计划 |
| 20 | TiKV Raft 和 ReplicatedMergeTree 核心差异？| TiKV Raft 强一致（等过半 ACK）+ KV 写入；RM 异步 + Part 文件 + Merge 同步 |
| 21 | system.replicated_logs 能看什么？| 每个 Replicated 表的复制进度：LogEntry 总数 / 每个 Replica 的 marker / diff（落后多少条）|
| 22 | ClickHouse ZK Session 过期时间？| 默认 30s；调优 clickhouse.zookeeper.session_timeout_ms；网络差时调大 |
| 23 | 多 Region 怎么部署？| ClickHouse Distributed 表 + ReplicatedMergeTree 本地复制 + Kafka/Distributed 跨 Region 异步同步 |
| 24 | LogEntry 序号是什么？| ZK 的 version；每次写 ZNode +1；全局单调递增；所有 Replica 靠这个序号判断复制顺序 |
| 25 | 典型生产用法？| ClickHouse 3 节点 ReplicatedMergeTree 集群 + Kafka 异步写入 → 实时数仓多副本查询 + Merge 自动同步 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：src/Storages/ReplicatedMergeTree/ReplicatedMergeTree.cpp + ReplicatedMergeTreeLogEntry.h + src/ZooKeeper/ZooKeeperClient.h  
**面试考点：25 题**  
**差异化点**：ZK 路径完整结构、LogEntry 四种类型（INSERT/MERGE/MUTATE/GET_PART）、Part 文件复制 vs SQL 重放、Merge 全局同步、**ReplicatedMergeTree vs TiKV Raft vs MySQL MGR 三方案深入对比**
