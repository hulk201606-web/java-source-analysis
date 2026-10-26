# Nacos JRaft Snapshot 机制源码深度解析（本地 3.2.4）

> **源码基线**：Nacos 3.2.4（本地：`d:\GC_WorkSpaces\nacos-3.2.4`）  
> **核心模块**：`core/src/main/java/com/alibaba/nacos/core/distributed/raft/NacosStateMachine.java`  
> **面试权重**：⭐⭐⭐⭐（Nacos CP 模式 Log 太多时的关键机制）

---

## 一、Snapshot 机制解决什么问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Raft Log 无限增长？→ Snapshot 解决！                      │
│                                                               │
│  问题：                                                       │
│  Raft Leader 持续接收写请求 → Log Entry 无限增长              │
│  → Log Entry 太多 → 磁盘空间不足 + 新节点加入要复制全部 Log │
│                                                               │
│  Snapshot 解决方案（面试高频！）：                             │
│  ① 定期把 StateMachine 当前状态写成全量快照文件              │
│  ② 快照文件包含：                                             │
│     - 所有已 Commit Log 的 Apply 结果（完整状态）           │
│     - 快照对应的 Last Included Index & Term                 │
│  ③ 快照生成后 → 丢弃 Last Included Index 之前的旧 Log        │
│  ④ Follower 落后太多（Log 被丢弃了）→ Leader 发送 Snapshot   │
│                                                               │
│  ★ Nacos JRaft vs TiKV raft-rs vs ETCD Go Snapshot 对比：    │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 触发条件   │ Nacos JRaft   │ TiKV raft-rs │ ETCD Go       │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 定时      │ ✅ 默认 30min │ ✅ 默认 10min │ ✅ 默认 10min │   │
│  │ Log 数量   │ ✅ > 10000   │ ✅ > 10000   │ ✅ > 10000   │   │
│  │ 大小阈值   │ ❌ 不        │ ✅ > 256MB   │ ❌ 不        │   │
│  │ 压缩      │ ❌ 不        │ ✅ SSTable   │ ✅ Gzip      │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│  ★ Nacos Snapshot 触发条件（本地源码可验证）：                │
│     raft.snapshot-interval = 300000（30min）                 │
│     raft.snapshot-log-since = 10000（Log > 10000 触发）     │
│     raft.snapshot-size = 0（不检查大小）                     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、NacosStateMachine Snapshot 源码锚点

### 2.1 NacosStateMachine onSnapshotSave（★ 本地锚点）

```java
// 锚点：d:\GC_WorkSpaces\nacos-3.2.4\core\src\main\java\...\NacosStateMachine.java
//
// ★ onSnapshotSave 核心流程（本地源码可验证）
//
// JRaft 框架在以下情况触发 onSnapshotSave：
// ① 定时器到了（snapshot-interval）
// ② Log Entry 超过 snapshot-log-since
// ③ 主动调 RaftCore.snapshot()
//
// NacosStateMachine.onSnapshotSave() {
//     // ① 创建 SnapshotWriter（JRaft 库提供）
//     SnapshotWriter writer = snapshotMeta.newSnapshotWriter();
//
//     // ② 收集 RequestProcessor4CP 列表
//     for (RequestProcessor4CP processor : processors) {
//         // ③ 每个 processor 自己做 Snapshot
//         //    ConfigRequestProcessor → 把 MySQL 配置数据序列化
//         //    → 写入 Snapshot 文件
//         processor.saveSnapshot(writer);
//     }
//
//     // ④ 完成
//     writer.close();
//
//     // ⑤ JRaft 框架拿到快照文件
//     // → 同步给 Follower
//     // → 丢弃快照之前的旧 Log Entry
// }
//
// ★ RequestProcessor4CP.saveSnapshot() 抽象接口：
//   每个功能模块（Config/Naming）自己实现 Snapshot 逻辑
//   → ConfigRequestProcessor：从 MySQL 查所有配置 → Protobuf 序列化 → 写文件
//   → 这就是 Template Method 设计模式！
```

### 2.2 NacosStateMachine onSnapshotLoad

```java
// ★ NacosStateMachine.onSnapshotLoad（加载快照）
//
// Follower 落后太多 → 旧 Log Entry 被 Leader 丢弃了
// → Leader 发送 Snapshot 文件给 Follower
// → Follower 调 onSnapshotLoad() 恢复状态
//
// NacosStateMachine.onSnapshotLoad(SnapshotReader reader) {
//     // ① 清空当前状态
//     for (RequestProcessor4CP processor : processors) {
//         processor.resetContext();
//     }
//
//     // ② 读取快照文件
//     // 每个 processor 恢复自己的状态
//     for (RequestProcessor4CP processor : processors) {
//         processor.loadSnapshot(reader);
//         // ConfigRequestProcessor → 反序列化配置 → 写回 MySQL
//     }
// }
//
// ★ 和 onApply 关系：
//   Follower 加载 Snapshot 后 → 收到 Snapshot 之后的 Log Entry
//   → 继续 onApply（正常 Apply Log）
//   → 数据完全恢复（Snapshot + 后续 Log）
```

### 2.3 Snapshot 同步给 Follower

```java
// ★ JRaft Snapshot 同步流程（本地源码可验证）
//
// ① Leader 生成 Snapshot 文件
//    → NacosStateMachine.onSnapshotSave() → Snapshot 文件生成在磁盘
//    → JRaft 记录 LastIncludedIndex & LastIncludedTerm
//    → 丢弃 LastIncludedIndex 之前的旧 Log Entry
//
// ② Follower 落后太多（Log Entry 被丢弃）
//    → Follower AppendEntries Response：
//      status = ErrorLogTemporarilyUnavailable（我的日志断了）
//      next_index = 不知道（需要 Leader 发 Snapshot）
//
// ③ Leader 检测到 Follower 需要 Snapshot
//    → 发 InstallSnapshot RPC
//      InstallSnapshot(lastIncludedIndex, lastIncludedTerm, snapshotFilePath)
//
// ④ Follower 接收 Snapshot
//    → 保存快照文件到本地
//    → 调 NacosStateMachine.onSnapshotLoad()
//    → 恢复到 LastIncludedIndex 的状态
//
// ⑤ Follower 继续正常复制
//    → 从 LastIncludedIndex+1 开始正常 AppendEntries
//    → 和 Leader 日志完全对齐
//
// ★ InstallSnapshot RPC（JRaft 协议定义）：
//   message InstallSnapshotRequest {
//       string group_id = 1;
//       uint64 server_id = 2;
//       uint64 term = 3;
//       uint64 last_included_index = 4;
//       uint64 last_included_term = 5;
//       bytes data = 6;  // 快照文件数据
//   }
//   → 和 AppendEntries 不同（AppendEntries 发 Log Entry，InstallSnapshot 发快照文件）
```

---

## 三、TiKV raft-rs Snapshot 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV raft-rs Snapshot vs Nacos JRaft Snapshot           │
│                                                               │
│  TiKV Snapshot 流程（更优化）：                               │
│  ① 触发：Log > 10000 条 OR Region 大小 > 256MB              │
│  ② Snapshot 数据：RocksDB SSTable（已压缩好的列式存储）      │
│     → TiKV 底层 RocksDB 已经是 SSTable → 直接用！           │
│     → 不需要额外序列化（Nacos 需要序列化 MySQL 数据）      │
│  ③ 同步：发送 SSTable 文件（已经压缩 + 有序）              │
│     → 比 Nacos 纯 Protobuf 序列化快很多                    │
│  ④ 接收：Follower 直接把 SSTable 放进 RocksDB（无缝）      │
│     → 快！RocksDB 有 SSTable Ingest 功能                   │
│
│ ★ 本质差异：                                                 │
│   TiKV：Snapshot = RocksDB SSTable（底层就是 SSTable）     │
│   Nacos：Snapshot = 功能模块自己序列化 Config 数据          │
│
│ ★ 面试高频：                                                  │
│   TiKV Snapshot 为什么快？                                    │
│   → RocksDB 底层 SSTable + SSTable Ingest 直接导入          │
│   → 没有序列化/反序列化开销                                  │
│   Nacos Snapshot 为什么慢？                                   │
│   → MySQL 查配置 → Protobuf 序列化 → 写文件 → Follower 读 → 反序列化 → 写回 MySQL │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（12 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Snapshot 解决什么问题？| **Raft Log 无限增长** → Snapshot 全量快照 → 丢弃旧 Log → 磁盘省 + 新节点复制快 |
| 2 | Nacos Snapshot 触发条件？| **定时 30min OR Log Entry > 10000 条**；配置 `raft.snapshot-interval` + `raft.snapshot-log-since` |
| 3 | Nacos Snapshot 源码锚点？| `NacosStateMachine.onSnapshotSave()` / `onSnapshotLoad()` → `d:\GC_WorkSpaces\nacos-3.2.4\core\...\NacosStateMachine.java` |
| 4 | Snapshot 同步流程？| Leader 生成 Snapshot → Follower 落后太多（Log 被丢弃）→ Leader 发 InstallSnapshot RPC → Follower 调 onSnapshotLoad → 继续 AppendEntries |
| 5 | InstallSnapshot RPC？| JRaft 协议：`term + last_included_index + last_included_term + data` → 和 AppendEntries 是两个不同 RPC |
| 6 | TiKV Snapshot vs Nacos Snapshot？| TiKV：**RocksDB SSTable（直接导入，快！）**；Nacos：**Protobuf 序列化 MySQL 数据（慢）** |
| 7 | TiKV Snapshot 触发条件？| Log > 10000 OR Region > 256MB；比 Nacos 多一个大小阈值 |
| 8 | Snapshot 包含什么？| **完整 StateMachine 状态 + LastIncludedIndex + LastIncludedTerm** |
| 9 | Snapshot 之后的 Log？| 丢弃 **LastIncludedIndex 之前**的旧 Log；之后的 Log 继续正常保留 + 正常复制 |
| 10 | Snapshot 和 Follower 同步？| Follower Apply 完 Snapshot + LastIncludedIndex 之后的 Log = 完整恢复 |
| 11 | Snapshot 学习路径？| Nacos 本地源码 NacosStateMachine → onSnapshotSave/Load → JRaft InstallSnapshot RPC → TiKV RocksDB SSTable Ingest |
| 12 | Snapshot 面试高频考点？| **为什么需要 Snapshot？触发条件？同步流程？TiKV vs Nacos 性能差异？** |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Nacos 3.2.4（本地 `d:\GC_WorkSpaces\nacos-3.2.4`）+ TiKV v8.0.x  
**面试考点：12 题**  
**差异化点**：本地源码精确锚点 + TiKV/Nacos Snapshot 性能对比
