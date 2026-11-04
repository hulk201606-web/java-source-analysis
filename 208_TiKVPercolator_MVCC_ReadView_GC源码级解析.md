# TiKV Percolator MVCC + ReadView + 垃圾回收源码级深度解析

> **源码基线**：TiKV 7.x/8.0（Rust）  
> **核心位置**：components/storage/ + components/engine_rocks/ + src/server/gc_worker/  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 事务隔离核心，补全之前 Percolator 概览）

---

## 一、Percolator MVCC 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Percolator = Google Percolator 论文实现             │
│                                                               │
│  核心思想（面试必背）：                                        │
│   ★ 把每行数据变成多个版本（每个事务一个版本）                │
│   每个版本带 commit_ts（事务提交时间戳，全局递增）           │
│   读请求按 ReadView 选一个版本（快照隔离）                  │
│   不需要加锁！（乐观并发控制）                               │
│                                                               │
│  ★ TiKV 三个 CF（Column Family）—— MVCC 存储基础            │
│  ┌───────────────────────────────────────────────────┐      │
│  │ ① CF_DEFAULT：存储实际数据                          │      │
│  │    Key   = (user_key, commit_ts)                   │      │
│  │    Value = row value（最新版本的）                  │      │
│  ├───────────────────────────────────────────────────┤      │
│  │ ② CF_WRITE：记录每个 Key 的写入版本                 │      │
│  │    Key   = (user_key, commit_ts)                   │      │
│  │    Value = WriteType + start_ts + commit_ts        │      │
│  │    // WriteType: PUT / DELETE / LOCK               │      │
│  ├───────────────────────────────────────────────────┤      │
│  │ ③ CF_LOCK：未提交事务的锁                           │      │
│  │    Key   = (user_key, start_ts)                    │      │
│  │    Value = LockInfo（事务状态）                    │      │
│  └───────────────────────────────────────────────────┘      │
│                                                               │
│  ★ 和 MySQL InnoDB MVCC 核心区别：                          │
│   MySQL：Undo Log（逻辑回滚）+ Read View                   │
│   TiKV：**版本化 KV（物理多版本）+ CF_WRITE 记录版本链**    │
│   TiKV 每个版本都是独立的 KV，不需要 Undo Log 回滚         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiKV 三 CF 存储结构源码级

### 2.1 CF_DEFAULT + CF_WRITE + CF_LOCK

```rust
// ★ TiKV MVCC 三 CF 源码级（面试核心）
//
// components/storage/src/mvcc/
// components/engine_rocks/src/mvcc.rs
//
// // TiKV 底层用 RocksDB（三个 CF）：
// // CF_DEFAULT / CF_WRITE / CF_LOCK
// //
// // ★ Key 编码规则（源码级）：
// // user_key = "orders:1001"
// // start_ts = 100（事务开始时间戳）
// // commit_ts = 105（事务提交时间戳）
// //
// // CF_DEFAULT Key 编码：
// //   (user_key, MAX_U64 - commit_ts)
// //   // ★ 注意：commit_ts 取反！
// //   //   RocksDB 按 Key 排序
// //   //   MAX_U64 - commit_ts 越小 → commit_ts 越大
// //   //   → 同一个 user_key 的版本按 commit_ts 降序排列
// //   //   → 遍历时第一个就是最新版本（高频访问优化）
// //
// // CF_WRITE Key 编码：
// //   (user_key, commit_ts)
// //   // commit_ts 正序
// //   // 同一个 user_key 的写入版本按 commit_ts 升序
// //
// // CF_LOCK Key 编码：
// //   (user_key, start_ts)
// //   // start_ts 正序
// //   // 同一个 user_key 的锁按 start_ts 升序
// //
// // ★ 版本化写入流程源码级：
// //
// //   INSERT INTO orders VALUES (1001, 'Tom', 100);
// //   → start_ts = 100（TSO 分配）
// //   → commit_ts = 105（TSO 分配，PREWRITE 阶段获取）
// //
// //   // PREWRITE 阶段（第一步）：
// //   //   a. 检查 CF_LOCK 有没有锁（冲突检测）
// //   //   b. 写 CF_LOCK(user_key, start_ts) = LockInfo
// //   //   c. 写 CF_DEFAULT(user_key, MAX-105) = "Tom"（暂存）
// //   //
// //   // COMMIT 阶段（第二步）：
// //   //   a. 写 CF_WRITE(user_key, 105) = PUT(start_ts=100)
// //   //   b. 删除 CF_LOCK(user_key, 100)
// //   //   c. CF_DEFAULT 的暂存值变成正式版本
// //
// // ★ 版本链示例：
// //   orders:1001 被事务 100→105 写入（第一次）
// //   orders:1001 被事务 200→210 更新（第二次）
// //   orders:1001 被事务 300→315 更新（第三次）
// //
// //   CF_DEFAULT:
// //     (orders:1001, MAX-315) = "Alice"  ← 最新
// //     (orders:1001, MAX-210) = "Bob"    ← 第二新
// //     (orders:1001, MAX-105) = "Tom"    ← 最旧
// //
// //   CF_WRITE:
// //     (orders:1001, 105) = PUT(start_ts=100)
// //     (orders:1001, 210) = PUT(start_ts=200)
// //     (orders:1001, 315) = PUT(start_ts=300)
// //
```

### 2.2 TSO 时间戳分配

```rust
// ★ TSO（Timestamp Oracle）—— 全局时间戳分配（TiDB PD 提供）
//
// components/txn/helpers/tso.rs
//
// // Percolator 需要严格递增的时间戳：
// //   start_ts：事务开始时从 PD 获取
// //   commit_ts：事务 PREWRITE 成功后从 PD 获取
// //
// // ★ PD TSO 机制：
// //   PD 节点维护全局计数器（物理时间 + 逻辑计数器）
// //   客户端（TiKV/TiDB）批量预取 8192 个 TSO
// //   → 本地缓存，用完再去 PD 取
// //   → 减少 PD RPC 调用
// //   → PD 切换 Leader 时 sleep 2ms 防回退
// //
// // ★ 为什么需要 TSO 全局唯一？
// //   Percolator MVCC 靠 commit_ts 判断版本先后
// //   commit_ts 必须严格单调递增
// //   → 不能用本地时钟（会漂移）
// //   → 必须用中心化 Oracle（PD）
//
```

---

## 三、ReadView 快照读源码级

### 3.1 ReadView 核心结构

```rust
// ★ TiKV ReadView 源码级（面试核心）
//
// components/storage/src/mvcc/read_view.rs
//
// // ReadView 是什么？
// //   事务开始时从 PD 获取 snapshot_ts
// //   ReadView 告诉 TiKV："我要读 snapshot_ts 时刻的数据"
// //
// // ★ ReadView 核心字段（源码级）：
// pub struct ReadView {
//     pub snapshot_ts: TimeStamp,    // 快照时间戳（事务开始时）
//     pub start_ts: TimeStamp,       // 当前事务的 start_ts
//     pub committed: Vec<TimeStamp>, // 已提交事务的 commit_ts 集合（简化，实际更复杂）
//     pub min_ts: TimeStamp,         // 已提交事务的最小 start_ts
//     pub max_ts: TimeStamp,         // 已提交事务的最大 start_ts
// }
//
// // ★ ReadView 读数据流程（面试必背）：
// //
// // 场景：
// //   snapshot_ts = 215（我要读 215 时刻的数据）
// //   版本链：commit_ts = [105, 210, 315]
// //
// // 读流程：
// // 1. 遍历 CF_WRITE 里 orders:1001 的版本
// //    （按 commit_ts 升序：105 → 210 → 315）
// //
// // 2. 找 commit_ts ≤ snapshot_ts (215) 的**最后一个版本**
// //    → commit_ts=210（对应值 "Bob"）
// //    → commit_ts=315 > 215 → 不应该看到（未来的版本）
// //
// // 3. 检查这个版本对应的事务是否真的 commit 了？
// //    → 版本链里的 commit_ts=210 的 Write 记录是 PUT
// //    → 说明 start_ts=200 的事务已经 Commit
// //    → 可以安全读取这个版本
// //
// // 4. 返回 "Bob"
// //
// // ★ 为什么用 CF_WRITE 而不是 CF_DEFAULT？
// //   CF_WRITE 记录了每个版本的写入事务状态（PUT/DELETE/LOCK）
// //   → 能判断某个 commit_ts 的版本是否真的 Commit
// //   → CF_DEFAULT 只存值，不知道事务状态
// //
```

### 3.2 快照隔离 vs 当前读

```rust
// ★ TiKV 快照隔离 vs 当前读（面试高频）
//
// // 1. 快照读（Snapshot Read）：
// //   用 ReadView.snapshot_ts 去版本链找
// //   → 读到的是 snapshot_ts 时刻的一致快照
// //   → 不需要加锁！Percolator 核心优势
// //   → 隔离级别：Snapshot Isolation（SI）
// //
// // 2. 当前读（Current Read）：
// //   跳过 ReadView，直接读 CF_WRITE 最新版本
// //   → 读到的是数据库当前最新状态
// //   → 会加读锁（检查 CF_LOCK）
// //   → 用于 SELECT ... FOR UPDATE / SELECT ... LOCK IN SHARE MODE
// //
// // ★ Percolator 隔离级别：
// //   默认 Snapshot Isolation（SI）—— 不禁止快照读
// //   TiDB 层可以升级到 Serializability（S2PL 加锁）
// //
// // ★ Percolator 和 MySQL InnoDB MVCC 对比：
// //   维度           | TiKV Percolator              | MySQL InnoDB
// //   ──────────────┼────────────────────────────┼──────────────────────
// //   版本存储       | ★ 独立 KV（物理多版本）      | Undo Log（逻辑回滚）
// //   快照隔离       | ★ ReadView snapshot_ts       | Read View（m_ids/min_trx_id）
// //   崩溃恢复       | CF_WRITE + CF_LOCK 状态机     | Undo Log + Redo Log
// //   GC 触发       | ★ GC Worker 主动扫描 CF_WRITE | Purge 线程异步清理 Undo
// //   锁机制         | CF_LOCK Prewrite 阶段        | Next-Key Lock / Gap Lock
//
```

---

## 四、垃圾回收（GC）源码级

### 4.1 GC 核心问题

```rust
// ★ TiKV GC（Garbage Collection）源码级（面试核心！）
//
// src/server/gc_worker/gc_worker.rs
// components/storage/src/gc.rs
//
// // 问题：MVCC 多版本无限增长 → 磁盘爆了！
// //   每个写入都产生新版本
// //   CF_DEFAULT 每个版本一条 KV
// //   CF_WRITE 每个版本一条 KV
// //   → 必须定期清理旧版本
//
// // ★ TiKV GC 核心原则（面试必背）：
// //   清理所有 commit_ts < safe_ts 的版本
// //   safe_ts = 所有存活事务的**最小 snapshot_ts**
// //
// //   为什么？
// //   事务 A 的 snapshot_ts=100（还活着）
// //   事务 B 的 snapshot_ts=200（还活着）
// //   safe_ts = min(100, 200) = 100
// //   → commit_ts < 100 的版本可以安全清理
// //   → 不会影响任何活着的事务的快照读
// //
// // ★ safe_ts 怎么计算？
// //   TiDB Server 上报所有活跃事务的 snapshot_ts 给 PD
// //   PD 汇总 → 算出 min(snapshot_ts) → 推送给 TiKV
// //   TiKV 收到 safe_ts → 执行 GC
// //
// // ★ GC 触发时机：
// //   TiKV 每隔 gc.run-interval（默认 10min）执行一次
// //   或者 Region 被 merge/split 时也会触发
//
```

### 4.2 GC 源码级流程

```rust
// ★ TiKV GC 执行流程（源码级，面试核心）
//
// gc_worker.rs → run_gc()
//
// // Step 1: 从 PD 获取 safe_ts
// let safe_ts = pd_client.get_gc_safepoint();
//
// // Step 2: 扫描 CF_WRITE 中所有 commit_ts < safe_ts 的版本
// //   按 user_key 分组，按 commit_ts 排序
// //
// // Step 3: 对每个 user_key 做 GC 决策：
// //
// //   场景 A：一个 Key 有多个旧版本
// //   CF_WRITE: [commit_ts=105, 210, 315]
// //   safe_ts = 250
// //   → commit_ts=105 和 210 都 < 250
// //   → 可以保留最新的 210（safe_ts 以下）
// //   → 删除 105（最旧版本）
// //   → 保留 210（safe_ts 以下最新的，GC 安全边界）
// //   → 保留 315（> safe_ts，未来版本，不动）
// //
// //   场景 B：DELETE 标记
// //   CF_WRITE: [commit_ts=105 PUT, 210 DELETE]
// //   safe_ts = 250
// //   → 210 是 DELETE，说明这个 Key 被删了
// //   → 删除 commit_ts=105（PUT 的旧版本）
// //   → 删除 commit_ts=210（DELETE 标记本身）
// //   → 清理干净！（这个 Key 完全不存在了）
// //
// // Step 4: 同时清理 CF_DEFAULT 的旧值
// //   CF_DEFAULT 的 Key 和 CF_WRITE 的一一对应
// //   CF_WRITE 删除什么 commit_ts → CF_DEFAULT 也删什么
// //
// // ★ GC 源码级实现：
// fn run_gc(&self, safe_ts: TimeStamp) -> Result<()> {
//     // 遍历 CF_WRITE（按 user_key + commit_ts 升序）
//     for each user_key in CF_WRITE {
//         let mut versions = collect_commit_ts(user_key);
//         versions.sort(); // commit_ts 升序
//
//         // 找 safe_ts 以下的所有版本
//         let safe_versions: Vec<_> = versions.iter()
//             .filter(|commit_ts| **commit_ts < safe_ts)
//             .collect();
//
//         if safe_versions.len() <= 1 {
//             // 只有一个或没有 safe_ts 以下的版本 → 不动
//             continue;
//         }
//
//         // 保留 safe_ts 以下最新的，删除更旧的
//         let keep = safe_versions.last().unwrap();  // 保留
//         for old_version in safe_versions.iter().rev().skip(1) {
//             // 从旧到新删（除了 keep）
//             cf_write.delete(user_key, old_version);
//             cf_default.delete(user_key, old_version);
//         }
//     }
//     Ok(())
// }
//
```

### 4.3 GC 与压缩（Compaction）配合

```rust
// ★ TiKV GC + RocksDB Compaction 配合（面试高频，生产关键点）
//
// // TiKV GC 只是把 KV 标记为 Deleted（墓碑值）
// // 真正磁盘空间回收靠 RocksDB Compaction
// //
// // 流程：
// //   GC Worker → 写墓碑（Delete 操作到 CF_WRITE/CF_DEFAULT）
// //   → RocksDB 后台 Compaction 扫到墓碑
// //   → Compaction 时把墓碑对应的旧版本真正丢弃
// //   → 磁盘空间回收
// //
// // // 关键：GC 触发墓碑 → Compaction 回收空间
// // //   只 GC 不 Compaction → 磁盘不释放（墓碑占空间）
// // //   只 Compaction 不 GC → 旧版本还是在（GC 必须先标记）
// //
// // ★ TiKV gc.lifetime 参数：
// //   gc.lifetime = "10m"（默认 10 分钟）
// //   // safe_ts = current_ts - 10min
// //   // 10 分钟前的版本可以 GC
// //   // 太短：长事务会读到被 GC 的版本（错误！）
// //   // 太长：版本堆积，磁盘压力大
// //
// // ★ 长事务阻塞 GC（面试陷阱）：
// //   如果有事务 snapshot_ts 非常老（比如 1 小时前）
// //   safe_ts = min(snapshot_ts) = 很老的值
// //   → GC 只能清理更老的版本
// //   → 大部分版本都清理不了 → 磁盘爆满！
// //   → **生产必须监控长事务**（information_schema.tikv_store_status）
//
```

---

## 五、TiKV MVCC vs MySQL InnoDB MVCC vs CouchDB 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种 MVCC 实现核心对比                           │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ TiKV Percolator   │ MySQL InnoDB   │ CouchDB   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 版本存储         │ ★ 独立 KV（CF_*） │ Undo Log      │ 多文档    ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 快照机制         │ ★ ReadView ts    │ Read View     │ _rev      ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 锁机制           │ CF_LOCK Prewrite  │ Next-Key Lock │ MVCC CAS  ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ GC 触发         │ ★ GC Worker 主动  │ Purge 线程     │ Compaction││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 崩溃恢复         │ CF_WRITE + CF_LOCK │ Undo + Redo   │ Raft      ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 隔离级别         │ ★ SI + S2PL      │ RC + RR       │ MVCC      ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 分布式支持       │ ★ Raft Learner    │ ❌ 单节点      │ P2P       ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频         │ ★★★★★          │ ★★★★★        │ ★★★      ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   分布式 HTAP → **TiKV Percolator**（Raft + MVCC + Coprocessor）│
│   OLTP 单节点 → MySQL InnoDB（成熟 + Index Condition Pushdown）│
│   文档数据库 → CouchDB（P2P + 多版本）                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（28 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV 三个 CF 是什么？| **CF_DEFAULT（值）+ CF_WRITE（版本链）+ CF_LOCK（未提交锁）** |
| 2 | CF_DEFAULT Key 编码为什么 commit_ts 取反？| **MAX_U64 - commit_ts** → 同一个 user_key 版本按 commit_ts 降序 → 遍历时第一个就是最新版本（高频优化）|
| 3 | ★ Percolator 两阶段写入？| **Prewrite（写 CF_LOCK + CF_DEFAULT 暂存）→ Commit（写 CF_WRITE + 删 CF_LOCK）** |
| 4 | TSO 是什么？| **全局时间戳 Oracle（PD 提供）**；start_ts 事务开始时取；commit_ts Prewrite 后取；PD 批量预取 8192 个减少 RPC |
| 5 | ★ ReadView 核心？| **snapshot_ts（事务开始时间戳）**；按 commit_ts ≤ snapshot_ts 找最后一个版本；检查 Write 类型 |
| 6 | 快照隔离 vs 当前读？| 快照读：用 snapshot_ts 找版本（无锁）；当前读：跳过 ReadView 读最新版本（加锁）|
| 7 | CF_WRITE 的 WriteType 有哪些？| **PUT（写入）+ DELETE（删除）+ LOCK（锁）** |
| 8 | ★ TiKV GC 核心原则？| **清理所有 commit_ts < safe_ts 的版本**；safe_ts = 所有存活事务的 min(snapshot_ts) |
| 9 | safe_ts 怎么来的？| TiDB Server 上报活跃事务 snapshot_ts 给 PD → PD 算 min → 推给 TiKV |
| 10 | GC 清理一个 Key 多个旧版本？| 保留 safe_ts 以下**最新的一个**（GC 安全边界）→ 删更旧的 |
| 11 | DELETE 版本怎么 GC？| DELETE（commit_ts=210）在 safe_ts 以下 → 连同 PUT（commit_ts=105）一起删干净 |
| 12 | ★ GC + Compaction 配合？| GC 写墓碑 → RocksDB Compaction 真正回收磁盘；**只 GC 不 Compaction = 磁盘不释放** |
| 13 | gc.lifetime 默认？| **10m**；safe_ts = current_ts - 10min；太短→长事务读不到；太长→版本堆积 |
| 14 | ★ 长事务阻塞 GC？| **是的！** 有 snapshot_ts 很老的事务 → safe_ts 很老 → 大部分版本清理不了 → 磁盘爆满 |
| 15 | TiKV MVCC vs MySQL InnoDB MVCC 核心区别？| TiKV：独立 KV 物理多版本 + CF_WRITE；MySQL：Undo Log 逻辑回滚 |
| 16 | TiKV GC 什么时候触发？| 默认每 **10min**（gc.run-interval）；或 Region merge/split 时 |
| 17 | ★ 三种 MVCC 实现对比？| 见第五节对比表（8 维度）|
| 18 | 面试最高频考点排序？| **1. 三 CF + Key 编码 2. ReadView 快照读 3. GC safe_ts 核心原则 4. GC + Compaction 配合 5. Prewrite + Commit 两阶段** |
| 19 | 学习路径？| 三 CF → Key 编码 → Prewrite/Commit → ReadView → GC safe_ts → GC 流程 → Compaction 配合 |
| 20 | 面试常见追问？| Percolator 为什么不叫 2PC？| **不是经典 2PC（无 Coordinator）**；Prewrite + Commit 是 Percolator 论文的原生设计；Raft 保证每个 CF 写入原子 |
| 21 | 面试常见追问？| TiKV GC 和 RocksDB Compaction 是一回事吗？| **完全不是！** GC 是逻辑删除版本（写墓碑）；Compaction 是 RocksDB 物理合并 SST（真正释放磁盘）|
| 22 | 面试陷阱？| 说 "GC 会删除最新版本" → ❌ GC 只删 safe_ts 以下的旧版本；safe_ts 是所有活跃事务的最小 snapshot_ts → 不影响任何事务 |
| 23 | 面试陷阱？| 说 "CF_DEFAULT 直接存了版本链" → ❌ CF_WRITE 才是版本链的索引；CF_DEFAULT 只存值；ReadView 用 CF_WRITE 找版本 |
| 24 | GC Worker 挂了会怎么样？| 旧版本堆积 → 磁盘满 → TiKV 写入阻塞；必须保证 GC 正常运行（监控 gc duration）|
| 25 | TiKV 和 InnoDB 崩溃恢复差异？| TiKV：CF_WRITE/CF_LOCK 状态机（Commit 完成就是已提交）；InnoDB：Undo + Redo Log 重放 |
| 26 | 典型生产参数？| **gc.lifetime=10m + gc.run-interval=10m + 监控长事务（>5min 告警）** |
| 27 | TiDB ReadView 和 TiKV ReadView？| TiDB 层构建 ReadView → 下推到 TiKV；TiKV 执行实际的版本查找 |
| 28 | TiKV 和 CockroachDB MVCC？| 都是 Percolator 论文实现；TiKV 用 Raft；CockroachDB 用 Raft + Geo Replica；核心设计相同 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：components/storage/src/mvcc/ + gc_worker.rs + engine_rocks/mvcc.rs  
**面试考点：28 题**  
**差异化点**：三 CF 存储 + Key 编码 commit_ts 取反、ReadView snapshot_ts 找版本、**GC safe_ts = min(snapshot_ts) 核心原则**、GC 写墓碑 + Compaction 回收磁盘、长事务阻塞 GC 陷阱、**TiKV vs InnoDB vs CouchDB MVCC 三实现对比**
