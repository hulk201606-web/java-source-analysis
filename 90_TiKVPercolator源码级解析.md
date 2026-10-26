# TiKV Percolator 2PC 源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust，txn 模块）  
> **核心仓库**：tikv/tikv + tikv/percolator  
> **面试权重**：⭐⭐⭐⭐⭐（分布式事务最高频源码级解析）

---

## 一、Percolator 核心角色回顾

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Percolator 三角色（源码级锚点）                     │
│                                                               │
│  ① Coordinator = TiDB Server                                 │
│     → txn::begin 事务入口                                    │
│     → txn::prewrite 发起 PreWrite                           │
│     → txn::commit 发起 Commit                               │
│
│  ② Participator = TiKV Region Raft Leader                    │
│     → 每个 Region 有独立 Percolator 处理                    │
│     → txn::acquire_pessimistic_lock（获取写锁）             │
│     → txn::pessimistic_prewrite（悲观锁下 PreWrite）        │
│
│  ③ Tso Provider = PD                                         │
│     → get_tso 获取全局递增时间戳                             │
│     → tso_cache 本地缓存（减少 PD RPC 调用）                │
│
│ ★ TiKV Percolator Rust 模块（源码路径）：                     │
│   tikv/src/storage/txn/                                       │
│   ├── txn.rs              ★ 事务入口                          │
│   ├── prewrite.rs         ★ PreWrite 核心                    │
│   ├── commit.rs           ★ Commit 核心                      │
│   ├── rollback.rs         ★ Rollback 核心                    │
│   ├── snapshot.rs         ★ MVCC 快照读                      │
│   └── writer.rs           ★ 写 RocksDB 封装                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、PreWrite 核心源码级流程（★★★★★ 面试最高频）

### 2.1 PreWrite 完整链路

```rust
// ★ TiKV Percolator PreWrite 源码级完整链路
//
// 场景：UPDATE orders SET status='paid' WHERE id=123
//       + UPDATE accounts SET balance=balance-100 WHERE id=456
//
// ① TiDB Server：txn::begin + get_tso()
//    start_ts = tso_cache.get_tso()  // 比如 1000
//    for each SQL执行：
//        key_versions = txn::snapshot_get(key, start_ts)  // MVCC 快照读
//        // 读到每个 key 在 start_ts 时的最新可见版本
//
// ② TiDB Server 组织 PreWrite 请求
//    primary_key = first_key  // 第一个 key 是 Primary Key！
//    secondary_keys = rest_keys
//    mutations = [Update(key_orders, value_paid), Update(key_accounts, value_new)]
//
// ③ Participator（TiKV Region Leader）处理 PreWrite
//
// txn::pessimistic_prewrite(mutations, start_ts, primary_key) {
//
//     // ★ Step 1: Lock CF 加写锁
//     for mutation in mutations {
//         // 检查 Lock CF 有没有锁
//         lock = writer.get_lock(mutation.key);
//         if lock exists and lock.start_ts > start_ts {
//             throw LockConflictError();  // 其他事务先加锁了 → 等待或重试
//         }
//         if lock exists and lock.start_ts < start_ts {
//             // 锁比我老 → 可能是 Coordinator 挂了 → 主动 Rollback
//             txn::cleanup(lock.start_ts);
//         }
//
//         // ★ Step 2: 写 Lock CF（加锁）
//         writer.put_lock(mutation.key, start_ts, lock_info)
//         // lock_info = { primary_key, ttl(3s), is_primary(key==primary_key) }
//
//         // ★ Step 3: 写 Data CF（写新版本）
//         writer.put_data(mutation.key, start_ts, mutation.value)
//         // Data CF 格式：key → (commit_ts, user_value)
//         // 此时 commit_ts 还不知道！先写 start_ts 版本
//     }
//
//     // ★ Step 4: 持久化 + Raft 复制
//     writer.write();
//     // 写 WAL +  AppendEntries → 过半确认
//     // → Raft 层保证副本一致
// }
//
// ★ PreWrite 关键：Data CF 里写的是 start_ts 版本（不是 commit_ts！）
//   → commit_ts 要等 Commit 阶段才知道
//   → MVCC 读时怎么判断这个版本是否可见？查 Write CF！
//
```

### 2.2 Lock CF 结构源码

```rust
// ★ TiKV Lock CF 源码级结构（面试高频！）
//
// Lock CF Key-Value 格式：
//
// Key:   data_key          （比如 key = orders:id=123）
// Value: {
//     start_ts: u64,       // 事务开始时间戳
//     primary: bytes,      // Primary Key（用于 Coordinator 挂了的恢复）
//     ttl: u64,            // 锁超时时间（默认 3000ms = 3s）
//     for_update_ts: u64,  // 悲观锁 for update 时间戳
//     min_commit_ts: u64,  // 最小提交时间戳（用于避免 TS 回退）
//     kind: WriteType,     // Put / Delete / Lock（加读锁）
// }
//
// ★ Primary Lock vs Secondary Lock：
// Primary Lock（第一个 key）：
//   → 有完整的 primary 信息（包含所有 secondary keys）
//   → 用于 Coordinator 挂了时恢复
//   → 超时后其他事务看到 Primary Lock → 知道有异常事务 → 主动 Rollback
//
// Secondary Lock：
//   → 只有 start_ts + ttl
//   → 超时后不需要主动 Rollback（Primary Lock 负责恢复）
//
// ★ TTL 锁机制（源码级）：
// Lock CF 里的 ttl = 3000ms（默认）
// → TiKV 后台线程定期扫描 Lock CF
// → 超过 TTL 的锁 → 自动清理（LockExpire）
// → 避免死锁（Coordinator 挂了锁自动释放）
//
```

---

## 三、Commit 核心源码级流程

```rust
// ★ TiKV Percolator Commit 源码级完整链路
//
// ① TiDB Server：get_tso() + commit 协调
//    commit_ts = tso_cache.get_tso()  // 必须 > start_ts
//    // commit_ts = 1001（比如）
//
// ② 先 Commit Primary Key
//    → 因为 Primary Lock 是恢复的关键
//    → Primary Commit 成功 → 表示事务整体成功
//
// ③ Participator（TiKV Region Leader）处理 Commit
//
// txn::commit_primary(primary_key, start_ts, commit_ts) {
//
//     // ★ Step 1: 检查 Lock CF 还有锁吗？
//     lock = writer.get_lock(primary_key);
//     if lock is None or lock.start_ts != start_ts {
//         // Primary Lock 已经被清理了？→ 事务可能已经 Rollback
//         throw CommitExpiredError();
//     }
//
//     // ★ Step 2: 删 Primary Lock
//     writer.delete_lock(primary_key);
//
//     // ★ Step 3: 写 Write CF（标记提交）
//     writer.put_write(primary_key, start_ts, commit_ts)
//     // Write CF Key-Value 格式：
//     // Key:   data_key + start_ts
//     // Value: { commit_ts: u64, write_type: WriteType::Commit }
//     // → MVCC 读时查 Write CF 判断版本是否可见！
//
//     // ★ Step 4: 持久化 + Raft 复制
//     writer.write();  // WAL + AppendEntries
// }
//
// ④ 再 Commit Secondary Keys（可以异步！）
//    for secondary_key in secondary_keys {
//        txn::commit_secondary(secondary_key, start_ts, commit_ts)
//        // 和 Primary 流程一样：删 Lock + 写 Write CF
//    }
//    // 可以并行发 RPC（不同 Secondary 可能在不同 TiKV Region）
//
// ★ Primary vs Secondary Commit 顺序：
// Primary 必须先 Commit！
// → 因为 Coordinator 挂了时其他事务靠 Primary Lock 恢复
// → Secondary Commit 可以异步（不影响事务成功判断）
//
```

---

## 四、TiKV 三 Column Family MVCC 可见性

```rust
// ★ TiKV 三 CF + MVCC 读可见性（源码级，面试高频！）
//
// MVCC 快照读：SELECT * FROM orders WHERE start_ts = 1000
//
// 读流程：
//
// ① 查 Lock CF（有没有锁？）
//    lock = lock_cf.get(key)
//    if lock exists {
//        if lock.start_ts < read_ts {
//            // 有更早的事务正在写 → 等待或返回 LockError
//            throw LockTimeoutError();
//        }
//        // lock.start_ts >= read_ts → 这个锁是我自己的（开始读前就有）
//        // → 跳过 Lock CF（继续查其他 CF）
//    }
//
// ② 查 Write CF（这个 Key 的版本历史）
//    // Write CF Key = data_key + start_ts
//    // 遍历找到 commit_ts ≤ read_ts 的最大版本
//    latest_write = write_cf.get_commit_ts_le(key, read_ts);
//    // 比如：
//    // start_ts=900, commit_ts=950（已提交）
//    // start_ts=1000, commit_ts=?（这个版本？）
//    // → 如果 commit_ts(950) ≤ read_ts(1000) → 可见
//
// ③ 查 Data CF（拿到数据）
//    data = data_cf.get(key, write.commit_ts)
//    // Data CF Key = data_key + commit_ts
//    // Value = user_value
//    return data;
//
// ★ 三 CF 完整关系图：
// ┌──────────────┬────────────────────┬────────────────────┐
// │ CF            │ Key                  │ Value              │
// ├──────────────┼────────────────────┼────────────────────┤
// │ Lock CF（锁） │ data_key            │ Lock(start_ts,      │
// │               │                     │  primary, ttl)      │
// ├──────────────┼────────────────────┼────────────────────┤
// │ Data CF（数据）│ data_key + commit_ts│ user_value          │
// ├──────────────┼────────────────────┼────────────────────┤
// │ Write CF（提交│ data_key + start_ts│ Write(commit_ts,    │
// │  标记）       │                     │  WriteType)         │
// └──────────────┴────────────────────┴────────────────────┘
//
// ★ 为什么三个 CF 分开？
// Lock CF：快速判断有没有锁（不需要遍历所有版本）
// Data CF：存多版本数据（RocksDB 多版本合并）
// Write CF：存版本间的提交关系（MVCC 可见性判断）
// → 三者分工明确，读路径高效
//
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Percolator 三角色？| **Coordinator=TiDB Server（发起事务）/ Participator=TiKV Raft Leader（执行）/ Tso Provider=PD（时间戳）** |
| 2 | PreWrite 三步骤？| **① Lock CF 加写锁 ② Data CF 写 start_ts 版本 ③ Raft 持久化复制** |
| 3 | Primary Lock 作用？| **第一个 Key 当 Primary** → 包含所有 Secondary 信息 → Coordinator 挂了时其他事务看到 Primary Lock → 主动 Rollback |
| 4 | Lock CF TTL？| **默认 3000ms** → 后台线程扫描 → 超过 TTL 自动清理锁 → 避免死锁 |
| 5 | Commit Primary vs Secondary 顺序？| **Primary 必须先 Commit**（恢复依赖 Primary Lock）→ Secondary 可以异步并行 |
| 6 | TiKV 三 CF MVCC？| **Lock CF（锁）+ Data CF（多版本数据）+ Write CF（提交标记）** → 三者分工明确 |
| 7 | MVCC 读流程？| ①查 Lock CF（有没有锁）→ ②查 Write CF（commit_ts ≤ read_ts 的最大版本）→ ③查 Data CF（拿数据）|
| 8 | Percolator vs Seata AT？| Percolator：**数据库内部 2PC（TiKV 原生）**；Seata AT：**跨服务 2PC + undo_log（微服务）** |
| 9 | PreWrite 怎么判断锁冲突？| Lock CF 查锁 → lock.start_ts > start_ts → 有其他事务先加锁 → 等待或重试 |
| 10 | 主从恢复（Coordinator 挂了）？| 其他事务看到 Primary Lock → 查 Write CF（有没有 Commit/Rollback Record）→ 没有 → 主动 Rollback 整个事务 |
| 11 | Percolator 源码路径？| **tikv/src/storage/txn/txn.rs / prewrite.rs / commit.rs / rollback.rs** |
| 12 | Data CF 格式？| Key = data_key + commit_ts → Value = user_value；每个版本一个 Entry |
| 13 | Write CF 格式？| Key = data_key + start_ts → Value = Write(commit_ts, WriteType)；标记版本可见性 |
| 14 | Lock CF 格式？| Key = data_key → Value = Lock(start_ts, primary, ttl, for_update_ts, min_commit_ts, kind) |
| 15 | Percolator vs Raft？| 两层分工：**Raft 做副本复制（CP）** + **Percolator 做事务 ACID**；Raft 不感知 Percolator 内部写 |
| 16 | TiKV 悲观锁 vs 乐观锁？| TiKV 默认 **乐观锁**（PreWrite 时加锁）；也支持 **悲观锁**（SELECT ... FOR UPDATE 先加锁再 PreWrite）|
| 17 | Percolator 性能瓶颈？| 跨 Region 事务需要 2 次 RTT（PreWrite + Commit）→ 尽量让事务只涉及一个 Region |
| 18 | min_commit_ts 作用？| 避免同一事务 start_ts == commit_ts（TS 回退极端情况）；commit_ts ≥ min_commit_ts |
| 19 | 面试最高频考点排序？| **1. PreWrite/Commit 两阶段 2. Primary Lock 3. 三 CF MVCC 4. 主从恢复 5. Percolator vs Seata AT** |
| 20 | 学习路径？| tikv/src/storage/txn/prewrite.rs → commit.rs → 三 CF 结构 → TiDB SQL 到 TiKV RPC 全链路 |

---

**文档版本：v1.0 · 2026-10-22**  
**面试考点：20 题**  
**源码路径**：tikv/src/storage/txn/（txn.rs / prewrite.rs / commit.rs / rollback.rs）  
**差异化点**：源码级三 CF 结构 + PreWrite/Commit 完整链路
