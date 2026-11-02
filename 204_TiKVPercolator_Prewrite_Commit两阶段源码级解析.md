# TiKV Percolator Prewrite + Commit 两阶段事务源码级深度解析

> **源码基线**：TiKV 7.x/8.0（Rust）  
> **核心位置**：components/txn/ + components/raftstore/src/store/  
> **面试权重**：⭐⭐⭐⭐⭐（TiDB 事务执行核心，198 MVCC 存储层的事务执行层补全）

---

## 一、Percolator 事务执行全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Percolator = Google 分布式事务论文实现                    │
│                                                               │
│  核心思想（面试必背）：                                        │
│   TiDB Server 不做事务一致性协调 → TiKV 本地做                │
│   → 每个 TiKV 节点自己负责 Key 范围的事务原子性                │
│   → **无全局 Coordinator**（和 Seata AT 不同）                │
│   → Raft 保证每个 CF_LOCK / CF_WRITE 写入的原子性             │
│                                                               │
│  ★ 事务执行三阶段（面试必背）：                                │
│  ┌───────────────────────────────────────────────────┐       │
│  │ ① Begin：获取 start_ts（TSO 分配）                 │       │
│  │   事务开始时间戳，全局唯一递增                       │       │
│  ├───────────────────────────────────────────────────┤       │
│  │ ② Prewrite：写锁 + 冲突检测（核心！）              │       │
│  │   - 检查 CF_LOCK 有没有锁（冲突？）                │       │
│  │   - 检查 CF_WRITE 最新 commit_ts（MVCC 冲突？）    │       │
│  │   - 写 CF_LOCK（加锁）+ CF_DEFAULT（暂存值）        │       │
│  ├───────────────────────────────────────────────────┤       │
│  │ ③ Commit / Rollback：提交或回滚                   │       │
│  │   Commit：获取 commit_ts → 写 CF_WRITE → 删 CF_LOCK│       │
│  │   Rollback：直接删 CF_LOCK（有锁）或写 CF_WRITE(DELETE)│   │
│  └───────────────────────────────────────────────────┘       │
│                                                               │
│  ★ 和经典 2PC 核心区别（面试高频）：                          │
│   经典 2PC：有全局 Coordinator → 阻塞等待所有 Participant     │
│   Percolator：**无 Coordinator** → 每个 TiKV 本地原子性       │
│   → 更简单、更快、更易扩展                                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Prewrite 阶段源码级

### 2.1 Prewrite 完整流程

```rust
// ★ TiKV Percolator Prewrite 源码级（面试核心！）
//
// components/txn/src/prewrite.rs
// components/txn/src/commands.rs
//
// // Prewrite 是什么？
// //   事务对某个 Key 做写操作前的"预写"阶段
// //   → 加锁 + 冲突检测 + 暂存新值
// //   → 多个 Key 的 Prewrite 可以并行（不同 TiKV 节点）
// //
// // ★ Prewrite 完整流程（源码级，面试高频）：
// //
// // 1. TiDB Server 解析 SQL → 生成写入 Key 列表
// //    → 按 TiKV Region 分组 → 发给对应的 TiKV
// //    // 批量 Prewrite（一次 RPC 多个 Key）
// //
// // 2. TiKV 收到 Prewrite Request（带 start_ts + mutations）：
// //
// //    for each mutation in request.mutations:
// //        let user_key = mutation.key;
// //        let start_ts = request.start_ts;
// //
// //        // ★ Step A：检查 CF_LOCK（有没有未提交事务的锁）
// //        if let Some(lock) = cf_lock.get(user_key, start_ts) {
// //            if lock.start_ts == start_ts {
// //                // 自己加的锁（重试）→ 跳过
// //            } else {
// //                // ★ 有其他事务的锁 → 冲突！
// //                // 判断是 Write Conflict 还是 Lock Wait？
// //                if lock.start_ts < start_ts {
// //                    // 旧事务锁 → 我应该等（Lock Wait）
// //                    return Err(TxnError::WriteConflict {
// //                        start_ts,
// //                        cause_start_ts: lock.start_ts,
// //                        // 返回给 TiDB Server → TiDB 决定等还是重试
// //                    });
// //                } else {
// //                    // 新事务锁 → 我太老了 → 直接失败
// //                    return Err(TxnError::KeyIsLocked { ... });
// //                }
// //            }
// //        }
// //
// //        // ★ Step B：检查 CF_WRITE（MVCC 冲突）
// //        if let Some(latest_write) = cf_write.get_latest(user_key) {
// //            if latest_write.start_ts >= start_ts {
// //                // 有事务在我之前就开始了 → MVCC 冲突
// //                return Err(TxnError::WriteConflict { ... });
// //            }
// //        }
// //
// //        // ★ Step C：写 CF_LOCK（加锁）
// //        cf_lock.put(
// //            user_key, start_ts,
// //            Lock {
// //                lock_type: LockType::Put,  // Put / Delete / Lock
// //                primary: primary_key,      // 主锁 Key（第一个写入的 Key）
// //                ttl: 3000,                 // 锁超时时间（默认 3s）
// //            }
// //        );
// //
// //        // ★ Step D：写 CF_DEFAULT（暂存值）
// //        //   MAX_U64 - start_ts 是 CF_DEFAULT 的 Key 编码
// //        cf_default.put(
// //            user_key.encode(),
// //            encode_ts(start_ts),
// //            mutation.value  // 新值暂存
// //        );
// //
// // // ★ Prewrite 必须是 Raft 原子操作！
// // //   同一个 Key 的 CF_LOCK + CF_DEFAULT 写入
// // //   要么都成功、要么都失败
// // //   → TiKV 用 Raft Log 保证（Prewrite 作为一个 Raft Entry）
// //
```

### 2.2 主锁机制 + Lock TTL

```rust
// ★ Percolator 主锁机制源码级（面试核心！）
//
// // 为什么需要主锁（Primary Lock）？
// //   事务写多个 Key → 每个 Key 都要 Prewrite
// //   Commit 时只需要 Commit 主锁 Key
// //   → 其他 Key 异步 Commit（由主锁 Commit 触发）
// //   → 减少 Raft RPC 次数！
// //
// // ★ 主锁选择规则：
// //   事务第一个 Prewrite 的 Key → 成为 Primary Lock
// //   后续 Prewrite 带上 primary_key 指向它
// //   // 所有 Secondary Lock 都记录 primary 的 start_ts
// //
// // ★ Commit 时主锁的作用：
// //   主锁 Key Commit 成功 → 通知其他 Secondary Lock
// //   Secondary Lock 收到 → 自己 Commit（写 CF_WRITE + 删 CF_LOCK）
// //   // 异步通知：TiKV 内部消息队列
// //   // 主锁 Commit 后 → Secondary 自己处理
// //
// // ★ Lock TTL（锁超时）：
// //   CF_LOCK 的 ttl = 3000（默认 3s）
// //   TiKV 后台线程定期扫描 CF_LOCK
// //   // 超过 TTL 的锁 → 认为持有者已崩 → 自动清理
// //   // 防止事务崩溃后锁永远不释放
// //
// // ★ 长事务 Lock TTL 续租：
// //   TiDB Server 定期给长事务发 Heartbeat
// //   // 更新 CF_LOCK 的 ttl（延长 3s）
// //   // 防止正常运行的长事务锁被清理
//
```

---

## 三、Commit 阶段源码级

### 3.1 Commit 完整流程

```rust
// ★ TiKV Percolator Commit 源码级（面试核心！）
//
// components/txn/src/commit.rs
//
// // Commit 什么时候触发？
// //   TiDB Server 收到客户端 COMMIT 请求
// //   → 检查所有 Prewrite 是否成功（有 CF_LOCK）
// //   → 从 PD 获取 commit_ts
// //   → 发送 Commit RPC 给 TiKV
// //
// // ★ Commit 完整流程（源码级）：
// //
// // 1. TiDB Server 从 PD TSO 获取 commit_ts
// //    commit_ts 必须 > start_ts（PD 保证）
// //
// // 2. TiDB Server 发 Commit RPC 给 Primary Lock 所在 TiKV
// //    // 只发给 Primary！（主锁机制的优势）
// //    CommitRequest { start_ts, commit_ts, primary_key }
// //
// // 3. TiKV 处理 Primary Commit：
// //    a. 检查 CF_LOCK 有没有主锁（确认 Prewrite 成功）
// //    b. 写 CF_WRITE（记录版本链）：
// //       cf_write.put(
// //           user_key, commit_ts,
// //           WriteType::Put,  // PUT / DELETE / LOCK
// //           start_ts
// //       );
// //    c. 删除 CF_LOCK（锁释放）
// //       cf_lock.delete(user_key, start_ts);
// //    // ★ 这两步是 Raft 原子操作
// //
// // 4. TiKV 触发 Secondary Lock 异步 Commit：
// //    // 主锁 Commit 成功后 → 通知 Secondary Lock
// //    // Secondary Lock 所在 TiKV 自己处理：
// //    a. 检查 CF_LOCK 有锁
// //    b. 写 CF_WRITE(commit_ts, start_ts)
// //    c. 删 CF_LOCK
// //    // 不需要 TiDB Server 再发 RPC！
// //    // TiKV 内部消息传递（快）
// //
// // 5. TiKV 给 TiDB Server 回 Commit ACK
// //    TiDB Server 给客户端回 OK
//
```

### 3.2 Rollback 流程 + Lock Wait 处理

```rust
// ★ TiKV Percolator Rollback + Lock Wait 源码级
//
// components/txn/src/rollback.rs
//
// // Rollback 什么时候触发？
// //   a. TiDB Server 显式 Rollback（客户端 ROLLBACK）
// //   b. Prewrite 检测到 MVCC 冲突 → 自动 Rollback
// //   c. 事务超时（超过 5min）→ TiDB Server 自动 Rollback
// //
// // ★ Rollback 流程：
// //   1. 发 Rollback RPC 给 Primary Lock 所在 TiKV
// //   2. TiKV 处理：
// //      a. CF_LOCK 有锁 → 直接删 CF_LOCK + 删 CF_DEFAULT 暂存值
// //      b. CF_LOCK 没锁（已经 Commit 了）→ 判断 Write 类型
// //         - 如果是 PREWRITE 类型的 WRITE（异常）→ 删 CF_WRITE
// //         - 如果是 PUT/DELETE 类型 → 已经 Commit，不能 Rollback → 报错
// //   3. Secondary Lock 异步 Rollback（同 Commit）
// //
// // ★ Lock Wait 处理（面试高频）：
// //
// //   事务 T1（start_ts=100）Prewrite 加锁成功
// //   事务 T2（start_ts=200）Prewrite 同一 Key → 发现 T1 的锁
// //   T1.start_ts=100 < T2.start_ts=200 → T2 应该等 T1
// //
// //   ★ 处理方式（两种）：
// //   a. TiDB Server 层 Wait：
// //      TiDB 收到 Lock 错误 → 等一段时间（lock_wait_timeout）
// //      → 重新去 TiKV 读 → 锁可能已经释放（T1 Commit/Rollback 了）
// //      → 没释放继续等 → 超时后返回 Lock Wait Timeout
// //
// //   b. TiKV 层优化（6.0+）：
// //      TiKV 返回 Lock 信息时带上 TTL 和 Primary Key
// //      TiDB 可以异步检测主锁状态 → 主动 Rollback 锁持有者（如果它崩了）
// //
// // ★ Deadlock 检测（面试高频）：
// //   TiKV Server 内部维护 Wait-for Graph
// //   // A 等 B、B 等 A → 形成环
// //   // 后台线程定期检测 → 发现环 → 回滚 start_ts 最大的事务
// //   // start_ts 最大的 = 最年轻的 → 回滚代价最小
//
```

---

## 四、Percolator vs 经典 2PC vs Seata AT 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试终极考点：三种分布式事务实现核心对比                  │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ TiKV Percolator   │ 经典 2PC      │ Seata AT   ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 协调者          │ ★ 无（TiKV 本地） │ ★ 有（全局）   │ TC（全局） ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 原子性保证       │ ★ Raft 每个 CF   │ Coordinator   │ Undo + XA  ││
│  │                │   写入原子        │ + Participant │ 兼容协议    ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 协议复杂度       │ ★ 低（两阶段简单）│ ★ 高（阻塞等待）│ 中          ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 锁粒度          │ Key 级别（CF_LOCK）│ 资源级别       │ 行级别      ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 崩溃恢复         │ ★ CF_WRITE + CF_LOCK│ Coordinator  │ Undo + TC ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 性能            │ ★ 高（Raft 本地）  │ 低（全局等待） │ 中          ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 典型应用         │ TiDB HTAP        │ 分布式数据库   │ Dubbo 微服务││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   分布式存储（TiDB/Percolator）→ **Percolator 无 Coordinator**│
│   跨服务事务（微服务）→ Seata AT / TCC                         │
│   严格 CP 分布式存储 → Percolator（Raft MVCC + 乐观锁）      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Percolator 三阶段？| **Begin（start_ts）→ Prewrite（加锁+暂存值）→ Commit/Rollback（提交/回滚）** |
| 2 | ★ Percolator vs 经典 2PC 核心区别？| **Percolator 无全局 Coordinator**；每个 TiKV 本地原子性（Raft 保证）；经典 2PC 有全局 Coordinator 阻塞等待 |
| 3 | ★ Prewrite 核心四步骤？| **Step A：检查 CF_LOCK（冲突检测）→ Step B：检查 CF_WRITE（MVCC 冲突）→ Step C：写 CF_LOCK（加锁）→ Step D：写 CF_DEFAULT（暂存值）** |
| 4 | CF_LOCK 冲突处理？| 有其他事务锁 → 比较 start_ts；旧事务锁 → Lock Wait；新事务锁（我太老了）→ 直接失败 |
| 5 | CF_WRITE MVCC 冲突？| latest_write.start_ts ≥ 我的 start_ts → 有事务在我之前开始 → 冲突 |
| 6 | ★ 主锁（Primary Lock）机制？| 事务第一个 Prewrite 的 Key 是主锁；Commit 只发给主锁；主锁 Commit 后 Secondary 异步 Commit |
| 7 | 主锁机制为什么快？| Commit 只发一个 RPC（主锁所在 TiKV）；Secondary Lock 内部消息传递 → 减少 TiDB→TiKV RPC 次数 |
| 8 | Lock TTL 默认值？| **3000ms（3s）**；超过 TTL 的锁认为持有者已崩 → 自动清理；长事务 Heartbeat 续租 |
| 9 | ★ Commit 核心流程？| TiDB 拿 commit_ts → 发 Commit RPC 给 Primary → TiKV 写 CF_WRITE + 删 CF_LOCK → Secondary 异步 Commit |
| 10 | Rollback 触发条件？| 显式 ROLLBACK / Prewrite 冲突自动回滚 / 事务超时（默认 5min）|
| 11 | Rollback 处理 CF_LOCK 和 CF_WRITE？| CF_LOCK 有锁 → 删 CF_LOCK + 删 CF_DEFAULT；CF_LOCK 没锁 → 看 CF_WRITE 类型（PREWRITE 删 / PUT 报错）|
| 12 | ★ Lock Wait 两种处理？| TiDB Server 层 Wait（lock_wait_timeout 内重试）；TiKV 层返回 Lock 信息 → TiDB 主动 Rollback 崩的锁持有者 |
| 13 | Deadlock 检测？| TiKV Wait-for Graph 检测环；发现环 → **回滚 start_ts 最大的事务**（最年轻 → 代价最小）|
| 14 | Prewrite 必须原子吗？| **是的！** 同一个 Key 的 CF_LOCK + CF_DEFAULT 写入是 Raft 原子 Entry |
| 15 | ★ Percolator vs 经典 2PC vs Seata AT 对比？| 见第四节对比表（7 维度）|
| 16 | 面试最高频考点排序？| **1. Prewrite 四步骤 2. 主锁机制 3. Commit/Rollback 流程 4. Lock Wait + Deadlock 5. Percolator vs 2PC** |
| 17 | 学习路径？| Begin start_ts → Prewrite 四步 → 主锁 → Commit/Rollback → Lock Wait → Deadlock → 对比 |
| 18 | 面试常见追问？| Percolator 为什么不叫 2PC？| **没有全局 Coordinator**；每个 TiKV 本地原子操作；经典 2PC 必须有全局 Coordinator 阻塞等待 |
| 19 | 面试常见追问？| Prewrite 和 Commit 能不在同一个 TiKV 吗？| **可以！** Primary Lock 在哪个 TiKV → Commit RPC 发给哪个；Secondary Lock 在其他 TiKV（异步 Commit）|
| 20 | 面试陷阱？| 说 "Percolator 是 XA 2PC 的优化版" → ❌ **完全不同的设计**；Percolator 乐观锁 + 无 Coordinator；XA 是悲观锁 + 全局 Coordinator |
| 21 | 面试陷阱？| 说 "Secondary Lock Commit 需要 TiDB 发 RPC" → ❌ **主锁 Commit 后 Secondary 自己处理**；TiKV 内部消息传递（快，不需要 TiDB 参与）|
| 22 | Lock Wait 超时时间？| innodb_lock_wait_timeout（TiDB 兼容 MySQL 参数，默认 50s）；超过返回 Lock Wait Timeout 错误 |
| 23 | Percolator 和 Snapshot Isolation 的关系？| **Percolator 实现 Snapshot Isolation（SI）**；MVCC ReadView + 乐观锁冲突检测 |
| 24 | TiDB 升级到 Serializability？| S2PL（Serializable 2PL）；TiDB Server 层加 SELECT ... FOR UPDATE 锁 → 升级到可串行化 |
| 25 | 典型生产事务配置？| **innodb_lock_wait_timeout=50s + 长事务 Heartbeat + Deadlock 检测开启** |
| 26 | CF_LOCK 和 CF_WRITE 的 Raft 原子性？| 同一个 Key 的 CF_LOCK + CF_DEFAULT 写入 / CF_WRITE + CF_LOCK 删除 → 都是 Raft Entry → 原子 |
| 27 | 事务 start_ts 和 commit_ts 关系？| PD TSO 保证 commit_ts > start_ts；每个事务的时间戳全局递增 → 保证 MVCC 版本链有序 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：components/txn/src/prewrite.rs + commit.rs + rollback.rs  
**面试考点：27 题**  
**差异化点**：Prewrite 四步骤源码级（CF_LOCK→CF_WRITE→CF_LOCK→CF_DEFAULT）、**主锁机制（Commit 只发主锁 RPC + Secondary 异步）**、Lock TTL + 长事务 Heartbeat、Deadlock Wait-for Graph 回滚最年轻事务、**Percolator vs 经典 2PC vs Seata AT 三方案 7 维度对比**（Percolator 无 Coordinator 核心优势）
