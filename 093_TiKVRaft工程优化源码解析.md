# TiKV Raft 工程优化深度源码解析

> **源码基线**：TiKV v8.0.x（Rust，raft-rs）  
> **核心仓库**：tikv/tikv + tikv/raft-rs  
> **面试权重**：⭐⭐⭐⭐⭐（工程优化，大厂面试高频）

---

## 一、为什么需要 Raft 工程优化？

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 标准 Raft 协议在教科书里很简单，但工程落地需要大量优化   │
│                                                               │
│  标准 Raft 教科书实现（伪代码）：                             │
│   while True {                                                │
│       sleep(50ms)                                             │
│       send_AppendEntries_to_all_followers()                  │
│       for f in followers:                                    │
│           if f.ACKs with matchIndex >= commitIndex:          │
│               commitIndex++                                  │
│   }                                                           │
│   → 问题：每条日志一次 AppendEntries → 吞吐极低！             │
│
│  TiKV 工程优化后的 Raft（面试高频！）：                       │
│  ┌──────────────────────────────────────────────────────┐    │
│  │ ① Pipeline AppendEntries（流水线，核心优化！）        │    │
│  │ ② Batch Write（批量写 WAL + 批量 Append）             │    │
│  │ ③ Async Apply（异步 Apply，不阻塞下一轮写）          │    │
│  │ ④ PreVote（预投票，减少无效竞选）                      │    │
│  │ ⑤ Lease Read（租约读，最快读优化）                    │    │
│  │ ⑥ Multi-Raft（每个 Region 一个 Group，并行度爆炸）   │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                               │
│ ★ 优化效果：                                                   │
│   教科书实现：~1000 ops/s                                    │
│   TiKV 优化后：~100000 ops/s（提升 100x！）                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Pipeline AppendEntries（★★★★★ 核心优化）

### 2.1 标准 AppendEntries vs Pipeline

```rust
// ★ TiKV Raft Pipeline AppendEntries（面试最高频优化！）
//
// 标准 Raft AppendEntries（串行，慢！）：
//
//  Leader:  send(entries_1) → wait ACK
//  Follower: recv → ACK
//  Leader:  recv ACK → send(entries_2) → wait ACK  ← 等！
//  Follower: recv → ACK
//  ...
//  → 每发一批日志就停下来等 ACK → 吞吐 = 1/RTT（极慢）
//
// ★ TiKV Pipeline AppendEntries（流水线，快！）：
//
//  Leader:  send(entries_1) → send(entries_2) → send(entries_3) → ...
//  Follower: recv entries_1 → ACK_1（但 Leader 已经发了 entries_2/3）
//  Leader:  recv ACK_1 → send(entries_4)（不停！）
//  Follower: recv entries_2 → ACK_2
//  ...
//  → 不等 ACK 就继续发下一批 → 吞吐 = 窗口大小/RTT（极快！）
//
// ★ Pipeline 核心变量（TiKV raft-rs）：
//   next_index[i]      → 给每个 Follower 下次发的起始 Index
//   match_index[i]     → 每个 Follower 已复制的最后 Index
//   inflight[i]        → Follower 正在复制但未 ACK 的日志队列
//   max_inflight_msgs  → 流水线窗口大小（默认 256）
//
//   Leader 内部循环：
//   for follower in followers {
//       if inflight[follower].len < max_inflight_msgs {
//           batch = entries[next_index[follower]..next_index+batch_size]
//           send(follower, batch)           // ★ 不等 ACK！
//           inflight[follower].push(batch)
//           next_index[follower] += len(batch)
//       }
//   }
//
//   收到 Follower ACK：
//   on_ack(follower, matched_index) {
//       inflight[follower].pop_upto(matched_index)
//       // 更新 CommitIndex（过半确认）
//       update_commit_index()
//   }
//
// ★ 性能提升（定量）：
//   RTT = 1ms（同机房）→ 标准 Raft 1000 ops/s → Pipeline 256000 ops/s
//   → 256x 提升！
```

### 2.2 Batch Write 批量写

```rust
// ★ TiKV Batch Write（批量写 WAL + 批量 Append）
//
// 标准 Raft 写流程（单条）：
//   for each log_entry:
//       wal.write(log_entry)        // 一次 fsync
//       append_entries_to_all()     // 一次网络发送
//       wait_acks()                 // 等待
//   → N 条日志 = N 次 fsync + N 次网络
//
// ★ TiKV Batch Write 流程：
//   攒一批日志（默认 16 条）：
//     batch = collect_new_entries() // 攒到 16 条或等 1ms
//     wal.write_batch(batch)        // ★ 一次 fsync！
//     append_entries_to_all(batch)  // ★ 一次网络发送！
//
//   Follower 也是 Batch 处理：
//     on_append_entries(batch) {
//         wal.write_batch(batch)     // 一次 fsync
//         ack(batch.last_index)       // 一次 ACK
//     }
//
// ★ fsync 为什么贵？
//   fsync 把页缓存刷到磁盘 → 强制等待磁盘完成 → ~1ms 延迟
//   → 批量 fsync 把 N 次 1ms 变成 1 次 1ms
//   → 吞吐提升 N 倍（N = batch_size）
```

### 2.3 Async Apply 异步 Apply

```rust
// ★ TiKV Async Apply（异步 Apply，不阻塞下一轮写）
//
// 标准 Raft Apply（同步，慢！）：
//   commit_index++
//   apply(last_applied+1..commit_index)  // ★ 阻塞！等 Apply 完成
//   → Apply 做完才开始下一轮复制
//   → 如果 Apply 慢（比如写 RocksDB Compaction）→ 整个 Raft 停
//
// ★ TiKV Async Apply：
//   commit_index++
//   send_to_apply_thread(last_applied+1..commit_index)  // ★ 丢给后台线程
//   // 不等 Apply 完成！继续下一轮 Pipeline Append
//
//   Apply Thread（后台）：
//     loop {
//         batch = apply_queue.recv()
//         for entry in batch:
//             state_machine.apply(entry)  // 写 RocksDB
//         last_applied += len(batch)
//     }
//
// ★ Async Apply 的关键（面试高频！）：
//   Raft Log 已经 Commit（过半确认）
//   → Apply 只是把 Log Entry 应用到状态机（写 RocksDB）
//   → Apply 失败不影响 Commit（Log Entry 已持久化）
//   → Apply 失败 → 重启后从 Log Entry 重放（Raft 保证）
//   → 所以可以异步！不需要同步等待
//
// ★ 效果：
//   Apply 延迟（RocksDB 写 + Compaction）→ 不影响 Raft 复制吞吐
//   Raft 层吞吐只受 Pipeline + Batch 限制（不受 Apply 限制）
```

---

## 三、其他工程优化

### 3.1 PreVote 预投票

```rust
// ★ TiKV PreVote（预投票，减少无效竞选）
//
// 标准 Raft 竞选流程（浪费 Term）：
//   Follower 心跳超时 → Term+1 → RequestVote RPC
//   → 如果网络分区 → Follower 选不上 → Term 反复膨胀
//   → 网络恢复后 Follower Term 很高 → 可能干扰正式 Leader
//
// ★ TiKV PreVote 流程：
//   ① PreVote 试探（不升 Term）：
//      Follower → PreVote RPC（用当前 Term，不+1）
//      Follower 回复：能当选 / 不能当选
//
//   ② 如果过半数说能当选 → 正式竞选：
//      Follower → Term+1 → RequestVote RPC（正式投票）
//
//   ③ 如果 PreVote 失败 → 放弃竞选，继续等
//
// ★ 效果：
//   网络分区恢复时 → 不会有很多无效 Term 膨胀
//   → Term 保持稳定 → 减少 Leader 干扰
```

### 3.2 Lease Read 租约读

```rust
// ★ TiKV Lease Read（租约读，最快读优化）
//
// 标准 Raft Read（强一致，慢！）：
//   Client → Leader：Read key
//   Leader → 发 ReadIndex RPC 给所有 Follower（过半确认 Leader 身份）
//   → 保证 Leader 身份有效（没有脑裂）
//   → 延迟 = 2 * RTT（一次 ReadIndex + 一次日志同步）
//
// ★ TiKV Lease Read（快！）：
//   Leader 心跳（默认 50ms）→ 知道自己 Leader 身份还在
//   在心跳间隔内 → Leader 直接本地读（不发 ReadIndex）
//   → 延迟 = 磁盘读延迟（~1ms）
//
// ★ Lease Read 边界条件（面试高频）：
//   ① 心跳间隔内才有效（默认 50ms）
//   ② 如果 Leader 不确定自己身份 → 降级为 ReadIndex
//   ③ 脑裂怎么防？心跳断了 → Leader 自动降级为 Follower
//   → Lease 过期后 Leader 自己知道 → 不提供 Lease Read 了
//
// ★ Lease Read vs ReadIndex vs Linearizable Read：
//   Lease Read → 最快（本地读），保证 Leader 身份有效
//   ReadIndex  → 次快（需要 Follower 过半确认）
//   Linearizable → 最慢（需要 log round-trip）
//   → 99% 场景用 Lease Read 就够快了
```

### 3.3 Multi-Raft 多 Raft Group

```rust
// ★ TiKV Multi-Raft（每个 Region 一个 Raft Group）
//
// 为什么每个 Region 一个？
//   TiKV 数据分片成几千个 Region（每个 ≈96MB）
//   → 每个 Region 有独立 Raft Group
//   → 几千个 Raft Group 并行工作 → 吞吐爆炸！
//
// ★ 单 Raft Group vs 多 Raft Group：
//   单 Group：所有写入串行 → 瓶颈！
//   多 Group：不同 Region 的写入并行 → 无瓶颈！
//
// ★ 线程模型（TiKV raftstore 模块）：
//   raftstore 有一个线程池：
//     每个线程处理多个 Region 的 Raft 消息
//     → 几千个 Region 被分散到几百个线程上
//     → 并行处理 Pipeline Append
//
// ★ PD 和 Multi-Raft 的关系：
//   PD 管理几千个 Region 的元数据（哪个 TiKV 有哪个 Region 的 Leader/Follower）
//   → PD Scheduler 定期调整 Region 分布（Balance + Hot Region）
//
```

---

## 四、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV Raft 核心工程优化？| **①Pipeline Append ②Batch Write ③Async Apply ④PreVote ⑤Lease Read ⑥Multi-Raft** |
| 2 | Pipeline Append 核心思想？| **不等 ACK 就发下一批** → 类似 CPU 流水线 → 窗口 256 → 吞吐提升 256x |
| 3 | Pipeline 核心变量？| **next_index（下次起始）/ match_index（已复制最后）/ inflight（未 ACK 队列）/ max_inflight_msgs（窗口）** |
| 4 | Batch Write 为什么快？| **攒一批日志一次 fsync** → 把 N 次磁盘刷写变成 1 次；1ms fsync → 16 条批量 = 62us/条 |
| 5 | Async Apply 为什么安全？| **Log Entry 已经 Commit（过半确认持久化）**→ Apply 只是写状态机；Apply 失败重启重放 Log Entry |
| 6 | PreVote 作用？| **竞选前先试探（不升 Term）**→ 网络分区恢复时减少无效 Term 膨胀 → 避免 Leader 干扰 |
| 7 | Lease Read 原理？| **Leader 心跳间隔内直接本地读**（~1ms）→ 心跳断了自动降级；比 ReadIndex（~2ms Follower 确认）快 2x |
| 8 | Multi-Raft 为什么？| **每个 Region ≈96MB 独立 Raft Group** → 几千个 Group 并行 → 解决单 Group 串行瓶颈 |
| 9 | 优化效果？| **教科书实现 ~1000 ops/s → TiKV 优化后 ~100000 ops/s**（100x 提升） |
| 10 | Pipeline vs Batch vs Async 关系？| Pipeline（发送端不等 ACK）→ Batch（攒一批写）→ Async（Apply 不阻塞）→ 三层独立优化叠加 |
| 11 | TiKV Raft vs ETCD Raft 优化？| TiKV：Pipeline + Batch + Async Apply + Multi-Raft；ETCD：Linearizable Read + Lease Read；TiKV 优化更极致 |
| 12 | TiKV Raft vs Nacos JRaft 优化？| TiKV：Pipeline + Batch + Async Apply；JRaft：共享 Timer + Closure；TiKV 优化更极致（多 Region 并行）|
| 13 | 标准 Raft 最大瓶颈？| **每发一条日志等一次 ACK** → 吞吐 = 1/RTT → Pipeline 解决；单 Group 串行 → Multi-Raft 解决 |
| 14 | 学习路径？| raft-rs 源码 → Pipeline Append 主循环 → Batch Write → Async Apply → TiKV raftstore 模块 |
| 15 | 面试最高频优化？| **Pipeline Append（流水线）+ Batch Write（批量写）** → 这两个是核心，其他都是锦上添花 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV v8.0.x（Rust，raft-rs）  
**面试考点：15 题**  
**差异化点**：大厂最极致 Raft 工程优化（从 1000 → 100000 ops/s 的完整路径）
