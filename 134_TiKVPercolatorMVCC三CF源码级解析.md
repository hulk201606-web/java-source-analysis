# TiDB Percolator MVCC 三 CF 源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）  
> **核心仓库**：tikv/tikv → components/raftstore/ + storage/  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 事务核心，MVCC 数据模型高频必问）

---

## 一、Percolator 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Percolator = Google Spanner 论文提出的分布式事务模型      │
│                                                               │
│  TiDB = TiKV（Raft 存储）+ Percolator（事务）+ PD（调度）    │
│                                                               │
│  Percolator 核心思想：                                        │
│   把分布式事务转换成 2PC + MVCC（多版本并发控制）             │
│   → 利用时间戳（TSO）做版本控制                               │
│   → TiKV 存储三层 Column Family（CF）实现 MVCC              │
│
│ ★ TiDB 事务执行流程（面试必背！）：                            │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ ① 开启事务：PD 分配全局 Start TS（时间戳）              │  │
│  │ ② 执行读写：                                             │  │
│  │   读：带 Start TS 查 MVCC → 找最新版本 ≤ Start TS     │  │
│  │   写：带 Start TS 写入 Prewrite 日志 → 获取锁          │  │
│  │ ③ 提交：PD 分配 Commit TS                               │  │
│  │   → 写入 Commit 记录 → 删除 Prewrite 日志               │  │
│  │ ④ 回滚：Prewrite 超时 → 自动 Rollback                   │  │
│  └───────────────────────────────────────────────────────┘  │
│
│ ★ Percolator vs 经典 2PC（面试高频）：                        │
│   经典 2PC：Prepare 阶段必须持有锁直到 Commit/Rollback       │
│   Percolator：利用 MVCC + 时间戳 → 读不加锁                  │
│   → 读多写少场景性能极好                                     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、MVCC 三 CF 数据结构（面试核心！）

### 2.1 TiKV RocksDB 存储三层 CF

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Percolator MVCC 三 CF（面试必背全景！）              │
│                                                               │
│  TiKV RocksDB 三个 Column Family：                            │
│  ┌──────────┬──────────────┬──────────────┐                 │
│  │ CF         │ Key            │ Value          │ 作用          │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Lock CF**│ data_key      │ Lock{start_ts, │ 未提交的锁    │
│  │            │               │ primary, ttl} │               │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Data CF**│ data_key +    │ user_value    │ 已提交的数据  │
│  │            │ commit_ts     │               │ （多版本）    │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Write CF**│ data_key +   │ Write{commit_ts, │ 元信息       │
│  │            │ start_ts      │ WriteType}   │ （版本链）    │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│  ★ 具体例子：user:1 这条数据有两个版本                       │
│  用户 T1（start_ts=100）写了 name="Alice"                    │
│  用户 T2（start_ts=200）写了 name="Bob"（并提交）           │
│                                                               │
│  RocksDB 内部存储：                                           │
│  Lock CF:                                                     │
│    user:1 → (已空，因为都提交了)                              │
│                                                               │
│  Data CF:                                                     │
│    user:1 + 100 → "Alice"    ← T1 写入的 value               │
│    user:1 + 200 → "Bob"      ← T2 写入的 value               │
│                                                               │
│  Write CF:                                                    │
│    user:1 + 100 → Write{commit_ts=150, Put}                  │
│    user:1 + 200 → Write{commit_ts=250, Put}                  │
│
│  ★ 关键理解：                                                 │
│   Data CF 存实际 value（按 commit_ts 排序的多版本）          │
│   Write CF 存版本元信息（commit_ts + WriteType）             │
│   Lock CF 存未提交的 Prewrite 锁                              │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Prewrite + Commit + Rollback 源码级

```rust
// ★ TiKV Percolator Prewrite + Commit 源码级
//
// components/raftstore/src/raftstore/impl.rs
//
// // ★ Prewrite 阶段（2PC 第一阶段）
// pub fn prewrite(ctx: &WriteContext, mutations: Vec<Mutation>, start_ts: TimeStamp) {
//     for mutation in mutations {
//         let key = mutation.key();
//         let value = mutation.value();
//
//         // ① 写 Lock CF：获取行锁
//         // key → Lock{start_ts, primary_key, ttl}
//         write_lock(&ctx, key, start_ts, primary_key);
//
//         // ② 写 Data CF：Prewrite 的 value
//         // key + start_ts → value （临时写入，还没 commit_ts）
//         write_data(&ctx, key, start_ts, value);
//     }
//     // ★ 所有 Prewrite 完成 → 等 Raft Commit（Quorum 同步）
// }
//
// // ★ Commit 阶段（2PC 第二阶段）
// pub fn commit(ctx: &WriteContext, primary_key: Vec<u8>,
//               start_ts: TimeStamp, commit_ts: TimeStamp) {
//     // ① 写 Write CF：提交记录
//     // key + start_ts → Write{commit_ts, Put}
//     write_write_cf(&ctx, &primary_key, start_ts, commit_ts, WriteType::Put);
//
//     // ② 把 Data CF 的 key 从 data_key+start_ts → data_key+commit_ts
//     // （删除旧的临时 key + 写新的正式 key）
//     rewrite_data_key(&ctx, &primary_key, start_ts, commit_ts);
//
//     // ③ 删除 Lock CF：释放锁
//     delete_lock(&ctx, &primary_key, start_ts);
//
//     // ★ 所有 Commit 完成 → 等 Raft Commit
// }
//
// // ★ Rollback 阶段
// pub fn rollback(ctx: &WriteContext, start_ts: TimeStamp) {
//     // ① 删除 Lock CF
//     delete_lock_all(&ctx, start_ts);
//     // ② 删除 Data CF 的临时数据
//     delete_data_all(&ctx, start_ts);
//     // ③ 写 Write CF 标记为 Rollback
//     write_write_cf(&ctx, key, start_ts, TimeStamp::zero(), WriteType::Rollback);
// }
//
```

---

## 三、MVCC 读路径源码级

### 3.1 快照读（Snapshot Read）

```rust
// ★ TiKV MVCC 快照读（面试核心！）
//
// 场景：SELECT * FROM users WHERE id = 1 （Start TS = 250）
//
// pub fn snapshot_read(ctx: &ReadContext, key: &[u8], start_ts: TimeStamp) -> Option<Value> {
//     // ① 先查 Lock CF：有没有未提交的锁？
//     if let Some(lock) = read_lock_cf(ctx, key) {
//         if lock.start_ts < start_ts {
//             // 有更早的事务持有锁 → 等待（直到锁释放）
//             return wait_for_lock(ctx, &lock, start_ts);
//         }
//     }
//
//     // ② 查 Write CF：找最新的 commit_ts ≤ start_ts 的版本
//     // Write CF Key: key + start_ts（注意是 start_ts 不是 commit_ts）
//     let mut iter = ctx.snapshot.write_cf.iterator(key);
//
//     // ★ 从最大 start_ts 往下找，直到 start_ts ≤ start_ts
//     let mut best_write: Option<Write> = None;
//     while let Some((k, v)) = iter.next() {
//         let write_start_ts = extract_start_ts(k);
//         if write_start_ts <= start_ts {
//             best_write = Some(parse_write(v));
//             break;  // 找到了最新的可见版本
//         }
//     }
//
//     match best_write {
//         Some(write) if write.write_type == WriteType::Put => {
//             // ✅ 找到 → 用 write.commit_ts 查 Data CF
//             read_data_cf(ctx, key, write.commit_ts)
//         }
//         Some(write) if write.write_type == WriteType::Delete => {
//             // ❌ 被删除了
//             None
//         }
//         _ => {
//             // ❌ 没找到（数据不存在或版本太新）
//             None
//         }
//     }
// }
//
// ★ 面试高频：快照读怎么实现无锁？                              │
//   带 Start TS → 只看 commit_ts ≤ Start TS 的版本             │
//   → 不用加锁 → 读不阻塞写，写不阻塞读                        │
//   → 这就是 MVCC 的威力！                                     │
//
```

---

## 四、Percolator vs 经典事务模型对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Percolator vs 2PC/XA vs Raft 事务对比（面试高频！）        │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Percolator    │ 2PC/XA         │ 纯 Raft      │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 读模型      │ MVCC 快照读    │ 加锁读          │ 加锁读        │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 锁策略      │ 悲观锁 Prewrite│ Prepare 锁       │ 悲观锁        │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 时间戳      │ PD TSO       │ Coordinator      │ Leader 单调递增│   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 性能         │ ⭐⭐⭐⭐⭐      │ ⭐⭐             │ ⭐⭐⭐          │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 一致性       │ CP（Raft+MVCC）│ CP              │ CP             │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 锁竞争       │ 低（读不加锁） │ 高（读写都加锁）  │ 中             │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 生产代表     │ TiDB         │ Oracle/XA       │ etcd/ZK       │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ Percolator 核心优势（面试必背！）：                          │
│   ① 读不加锁 → MVCC 快照读（读写完全并行）                  │
│   ② PD TSO 全局单调时间戳 → 全局有序的事务版本              │
│   ③ TiKV Raft 副本 + MVCC 三 CF → CP 强一致 + 高可用       │
│   → TiDB 是目前分布式事务最优秀的开源实现之一！              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Percolator 核心？| **Google Spanner 论文 → 分布式 2PC + MVCC + 时间戳版本控制** |
| 2 | TiKV MVCC 三 CF？| **Lock CF（未提交锁）+ Data CF（多版本 value）+ Write CF（版本元信息）** |
| 3 | Prewrite 阶段做什么？| 写 Lock CF（获取锁）+ 写 Data CF（临时 value）→ 等 Raft Commit |
| 4 | Commit 阶段做什么？| 写 Write CF（commit_ts 元信息）+ 重写 Data CF 的 key（加 commit_ts）+ 删除 Lock CF |
| 5 | Rollback 做什么？| 删除 Lock CF + 删除 Data CF 临时数据 + 写 Write CF 标记 Rollback |
| 6 | MVCC 快照读怎么实现？| 带 Start TS → Write CF 找最新 commit_ts ≤ Start TS 的版本 → 读 Data CF |
| 7 | PD TSO 作用？| **全局单调递增时间戳** → 每个事务一个 Start TS 和 Commit TS |
| 8 | Percolator vs 2PC 核心区别？| Percolator 读不加锁（MVCC）；2PC 读写都加锁 → Percolator 性能高得多 |
| 9 | 面试最高频考点排序？| **1. 三 CF 核心结构 2. Prewrite→Commit 流程 3. 快照读实现 4. Percolator vs 2PC 5. PD TSO** |
| 10 | 学习路径？| raftstore impl → prewrite → commit → snapshot_read → MVCC 三 CF |
| 11 | 面试常见追问？| 为什么需要 Write CF？→ 版本链 + 元信息；Lock CF 锁超时怎么办？→ Prewrite 超过 TTL → 自动 Rollback |
| 12 | TiKV MVCC 三 CF 源码路径？| storage/ → txn/ prewrite/commit/rollback.rs |
| 13 | TiDB 事务隔离级别？| **快照隔离（Snapshot Isolation）** → MVCC 天然支持 |
| 14 | TiDB 幻读问题？| TiDB 用 next-key lock 解决幻读（间隙锁 + MVCC）|
| 15 | 2024 新特性？| TiDB 8.0+ **Async Commit**（异步提交，不等 Raft）+ **MVCC 压缩优化** |
| 16 | 面试高频场景？| 分布式订单事务（扣库存+扣余额+生成订单）+ 大量并发读（MVCC 无锁读）|
| 17 | Percolator vs Seata AT？| Percolator：底层存储 MVCC（TiKV 内置）；Seata AT：应用层 Undo Log |
| 18 | TiKV Percolator vs Kafka ISR？| Percolator：2PC + MVCC + Raft；Kafka ISR：最终一致同步 |
| 19 | Percolator vs TiKV Raft？| Percolator：事务层（2PC + MVCC）；Raft：复制层（多副本同步）|
| 20 | 面试陷阱？| 说 TiKV Percolator = 纯 Raft → ❌ Percolator 是 Raft + MVCC + 时间戳版本控制 |
| 21 | 锁冲突怎么处理？| 悲观锁：等待 Prewrite 锁释放；乐观锁：检测冲突后重试 |
| 22 | TiDB 事务源码完整路径？| parser SQL → Optimizer 生成执行计划 → TiKV Coprocessor 执行 → Percolator 事务控制 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：components/raftstore/src/raftstore/impl.rs + storage/txn/  
**面试考点：22 题**  
**差异化点**：MVCC 三 CF（Lock/Data/Write）+ Prewrite→Commit 全流程 + 快照读实现 + Percolator vs 2PC
