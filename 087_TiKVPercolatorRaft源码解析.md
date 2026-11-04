# TiKV Percolator 2PC + Raft 两层架构深度解析

> **源码基线**：TiKV v8.0.x（最新稳定，Rust 实现）  
> **核心仓库**：tikv/tikv + tikv/raft-rs  
> **面试权重**：⭐⭐⭐⭐⭐（分布式事务最高频实战架构）

---

## 一、TiKV 两层架构核心设计（★★★★★ 面试最高频）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB 架构：TiKV = **Raft（副本复制）+ Percolator（分布式事务）**│
│                                                               │
│  TiDB 四层架构：                                              │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ TiDB Server（SQL 层，Go 实现）                         │   │
│  │   → SQL 解析 + CBO 优化 + MPP 调度                    │   │
│  └──────────────────────┬────────────────────────────────┘   │
│                         ↓                                     │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ PD（Placement Driver，Go 实现）                       │   │
│  │   → Region 分片 + Raft Group 调度 + Tso 时间戳         │   │
│  └──────────────────────┬────────────────────────────────┘   │
│                         ↓                                     │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ TiKV（存储层，Rust 实现）                              │   │
│  │   ┌─────────────────────┬─────────────────────────┐   │   │
│  │   │ **Percolator 2PC**  │ **Raft Group**          │   │   │
│  │   │ 分布式事务 + MVCC    │ 副本复制 + 一致性保障    │   │   │
│  │   └─────────────────────┴─────────────────────────┘   │   │
│  │                    ↓                                      │
│  │   ┌───────────────────────────────────────────────┐   │   │
│  │   │ RocksDB LSM-Tree（存储引擎）                   │   │   │
│  │   └───────────────────────────────────────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│ ★ TiKV 为什么需要两层？（面试必问！）                        │
│                                                               │
│   Raft 层：保证**数据不丢 + 副本一致**（但没有事务）        │
│   Percolator 层：保证**分布式事务 ACID**（但不做副本）      │
│                                                               │
│   两层分工：                                                  │
│   - Raft 是 TiKV 的"文件系统" → 保证每个 Region 的副本一致   │
│   - Percolator 是 TiKV 的"事务管理器" → 保证跨 Region 事务   │
│                                                               │
│ ★ 和 TiKV vs CockroachDB 对比（面试高频）：                  │
│   TiKV：**Raft + Percolator 两层**（Rust 实现）             │
│   CockroachDB：**Raft + PostgreSQL 事务**（Go 实现）        │
│   → CockroachDB 事务更灵活（PostgreSQL MVCC）               │
│   → TiKV 事务更简洁（Google Spanner Percolator）            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Percolator 2PC 核心原理（★★★★★ Google Spanner 简化版）

### 2.1 Percolator 三角色

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Percolator 核心三角色（和标准 2PC 角色相同）             │
│                                                               │
│  ① Coordinator（协调者）= **TiDB Server**                    │
│     → 接收客户端 SQL → 发起/提交/回滚事务                     │
│     → 管理 Txn Start Timestamp + Commit Timestamp            │
│     → 遍历所有 Participator 完成 2PC                        │
│                                                               │
│  ② Participator（参与者）= **TiKV Region Raft Leader**      │
│     → 负责一个数据分片（Region）                              │
│     → 接收 Coordinator 的 PreWrite/Commit/Rollback 请求      │
│     → 写入 RocksDB + 加锁 + MVCC                             │
│                                                               │
│  ③ PD（Placement Driver）= **Tso Provider**                  │
│     → 提供全局递增时间戳（Tso）                               │
│     → 事务开始/提交时从 PD 拿时间戳                          │
│     → Tso = 物理时间（高位）+ 计数器（低位）→ 全局唯一递增  │
│                                                               │
│ ★ Percolator 简化了标准 2PC：                                │
│   → 标准 2PC 需要 Prepare（不写数据）+ Commit（写数据）      │
│   → Percolator 直接 PreWrite（写数据 + 加锁）+ Commit（解锁）│
│   → 把 Prepare 和 Commit 的逻辑合并简化                     │
│                                                               │
│ ★ 和 Seata AT 模式对比（面试高频！）：                       │
│   Percolator：**分布式事务 2PC + Raft 副本**（TiDB 原生）   │
│   Seata AT：**业务 SQL 拦截 + undo_log 自动回滚**（微服务）  │
│   → Percolator 是数据库内部事务；Seata AT 是跨服务事务      │
│   → 两者解决不同层次的分布式事务问题                         │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Percolator 完整事务流程（★★★★★ 面试最高频）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Percolator 2PC 完整事务流程（面试必背！）           │
│                                                               │
│  场景：UPDATE orders SET status='paid' WHERE id=123         │
│        + UPDATE accounts SET balance=balance-100 WHERE id=456│
│        → 涉及两个 TiKV Region → 分布式事务                  │
│                                                               │
│  ① 事务开始（获取 Start Timestamp）                          │
│    TiDB Server → PD.GetTso()                                  │
│    → PD 返回 start_ts = tso1（物理时间 + 计数器）           │
│    → 整个事务所有操作都用 start_ts 读快照（MVCC）           │
│                                                               │
│  ② 执行快照读（MVCC 可见性）                                  │
│    TiKV Region → RocksDB 按 start_ts 读最新可见版本         │
│    → 如果遇到被锁的 Key → 等待锁释放                         │
│    → 读到最新数据（其他已提交事务的 Commit TS < start_ts）   │
│                                                               │
│  ③ **PreWrite 阶段（2PC 阶段一）★ 核心！**                  │
│    TiDB Server 遍历所有 Participator（涉及的 Region）：      │
│    → 给每个 Region 发 PreWrite(start_ts, keys, values)      │
│                                                               │
│    TiKV Region 收到 PreWrite 后：                            │
│    a. 写数据到 RocksDB：data_key → (start_ts, value)         │
│       → 这时候数据已经写入了 RocksDB！                       │
│    b. **加写锁**到 Lock Column Family：lock_key → start_ts  │
│       → 锁存在 = 这个 Key 正在被事务修改                     │
│       → 其他事务读/写这个 Key 时看到锁 → 等待               │
│    c. 写入 Primary Key 标记（Primary Lock）：                │
│       → 一个事务选一个 Key 当 Primary（第一个 Key）          │
│       → Primary Lock 是 Percolator 的核心（防 Rollback 挂）│
│    d. 返回 OK 给 TiDB                                       │
│                                                               │
│    如果任何一个 PreWrite 失败 → 回滚（给所有已 PreWrite 的 Region 发 Cleanup）│
│                                                               │
│  ④ 提交（Commit）                                            │
│    TiDB Server → PD.GetTso() → commit_ts = tso2             │
│    → 必须 commit_ts > start_ts（时间不能倒流）               │
│                                                               │
│  ⑤ **Commit 阶段（2PC 阶段二）★ 核心！**                    │
│    TiDB Server 给 Primary Region 发 Commit：                 │
│    a. Primary Region 收到 Commit(start_ts, commit_ts)        │
│    b. **删除 Primary Lock**（从 Lock CF）                    │
│    c. **写 Commit Record**（写 RocksDB 标记事务已提交）       │
│    d. 返回 OK                                                │
│                                                               │
│    TiDB Server 给其他 Region 发 Commit：                    │
│    a. 非 Primary Region 收到 Commit(start_ts, commit_ts)      │
│    b. **删除 Lock**（从 Lock CF）                             │
│    c. 返回 OK                                                │
│                                                               │
│  ⑥ 事务完成                                                   │
│    → RocksDB 里数据变成：data_key → (commit_ts, value)       │
│    → commit_ts 是事务提交时间                                │
│    → 其他事务用 commit_ts 判断数据是否可见（MVCC）           │
│                                                               │
│ ★ Percolator 的 Primary Key 设计（面试核心！）             │
│   为什么需要 Primary Key？                                    │
│   → 防止 Commit 阶段 TiDB Server 挂了                        │
│   → 其他事务读到 Primary Lock → 知道有未完成的事务          │
│   → 超时主动 Rollback（写入 Rollback Record + 清除所有 Lock）│
│   → 解决 Coordinator 挂了的问题（和 Raft PreVote 思路类似）│
│                                                               │
│ ★ 和标准 2PC 对比（面试高频）：                              │
│   标准 2PC：Prepare（不写数据）→ Commit（写数据）            │
│   Percolator：PreWrite（写数据+加锁）→ Commit（解锁）        │
│   → Percolator 更简洁：PreWrite 就写数据，Commit 只是解锁  │
│   → 性能更好：PreWrite 就完成了数据写入（只锁一次）         │
│   → 但 Percolator 需要 Primary Lock 解决异常恢复           │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 Percolator MVCC 读可见性

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Percolator MVCC 读流程（面试高频）                       │
│                                                               │
│  事务用 start_ts 做快照读：                                   │
│  SELECT * FROM orders WHERE id=123                            │
│
│  TiKV Region 处理读请求：                                    │
│  a. 查 Lock Column Family：                                  │
│     key=id=123 有没有 Lock？                                 │
│     → 有 Lock 且 lock.start_ts < 当前 start_ts              │
│     → 说明有更早的事务正在写这个 Key → **等待锁释放**       │
│     → 超时返回 LockTimeout 或等自动 Rollback               │
│
│  b. 如果没 Lock 或 Lock 是其他事务的：                       │
│     查 Data Column Family：                                  │
│     key=id=123 的所有版本                                    │
│     → 找到 commit_ts ≤ 当前 start_ts 的最大版本              │
│     → 这个版本就是"快照可见"的版本                          │
│     → MVCC 的核心：用 commit_ts 做版本号                   │
│
│  c. 还查 Rollback Record：                                   │
│     有没有 Rollback Record？                                  │
│     → 有 Rollback Record → 这个 start_ts 的事务已回滚       │
│     → 跳过这个版本（用更早的版本）                           │
│
│ ★ TiKV RocksDB 三 Column Family（面试高频！）：             │
│                                                               │
│   ① Data CF（核心数据）：                                    │
│      key = (data_key, commit_ts)                             │
│      value = user_value                                       │
│      → 每个数据版本一个 Entry（commit_ts 做版本号）         │
│
│   ② Lock CF（锁）：                                          │
│      key = (primary_key, start_ts)                           │
│      value = lock_info（包含所有 Secondary Keys）            │
│      → 只有 Primary Lock 存在时表示事务未提交               │
│
│   ③ Write CF（提交/回滚记录）：                              │
│      key = (data_key, start_ts)                              │
│      value = commit_ts 或 Rollback                            │
│      → 记录事务对这个 Key 是 Commit 还是 Rollback          │
│
│ ★ 为什么三个 CF？                                              │
│   Data CF：存数据（多版本）                                  │
│   Lock CF：存锁（事务进行中）                                │
│   Write CF：存提交/回滚记录（MVCC 可见性判断）              │
│   → 三个 CF 分工明确：读 → 查 Lock → 查 Data → 查 Write      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 2.4 Percolator 异常恢复

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Percolator 异常恢复（面试高频！Primary Lock 设计的核心）│
│                                                               │
│  场景：TiDB Server 在 Commit 阶段挂了                         │
│  → 已 PreWrite 但还没 Commit（有 Primary Lock）              │
│  → 其他事务读到 Primary Lock → 知道有异常事务               │
│                                                               │
│  恢复流程（另一个 TiDB Server 发现 Primary Lock）：          │
│  ① 读取 Primary Lock 信息                                    │
│     → 拿到 start_ts + 所有 Secondary Keys                    │
│
│  ② 查 Primary Key 的 Write CF                                │
│     → 有没有 commit_ts？                                      │
│     → 有没有 Rollback Record？                                │
│     → **都没有** → 事务还在 PreWrite 阶段 → 需要 Rollback   │
│
│  ③ 发起主动 Rollback                                         │
│     → 给所有 Participator 发 Rollback(start_ts)              │
│     → 每个 Participator 删除 Lock + 写 Rollback Record      │
│
│  ★ 为什么用 Primary Lock 而不是 Leader Watch？               │
│   Primary Lock：**被动恢复**（其他事务发现 → 触发 Rollback） │
│   → 不需要 Leader Watch（简化实现）                          │
│   → 所有 Region 都能恢复异常事务（去中心化）                │
│   → 代价：其他事务读时可能碰到 Primary Lock → 等待/恢复    │
│                                                               │
│ ★ 和 Raft 恢复对比（面试高频）：                              │
│   Raft Leader 挂了 → Follower 选主 → 继续服务（自动）        │
│   Percolator Coordinator 挂了 → Primary Lock → 主动 Rollback │
│   → 两层各自处理各自的故障（分工清晰）                       │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 2.5 Percolator + Raft 双层完整协同

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV 双层完整协同（面试最高频！从 Client 到磁盘全链路）  │
│                                                               │
│  完整写入链路：UPDATE orders SET status='paid' WHERE id=123 │
│                                                               │
│  ① TiDB SQL 层：                                             │
│     TiDB Server → SQL 解析 → CBO 优化 → MPP 调度            │
│     → 确定 id=123 在哪个 TiKV Region（查 PD 元数据）       │
│     → 发起 Percolator 事务                                   │
│                                                               │
│  ② Percolator 事务层（2PC）：                                │
│     TiDB → PD.GetTso() → start_ts                           │
│     → 给 TiKV Region 发 PreWrite(start_ts, key, value)      │
│
│  ③ TiKV Percolator 处理 PreWrite：                           │
│     a. 写 Lock CF：lock_key → start_ts                       │
│     b. 写 Data CF：(data_key, start_ts) → value              │
│     c. 写 WAL（Raft 层需要）                                │
│
│  ④ TiKV Raft 层处理副本复制：                                │
│     TiKV Region Raft Leader：                                │
│     → 写 WAL（持久化）                                        │
│     → AppendEntries RPC 复制给 Follower                      │
│     → 过半确认 → Commit Log Entry                            │
│     → Apply Log Entry 到 RocksDB                             │
│     → 返回 OK 给 Percolator 层                               │
│
│  ⑤ Percolator 层收到 PreWrite OK：                          │
│     TiDB → PD.GetTso() → commit_ts                          │
│     → 给 TiKV Region 发 Commit(start_ts, commit_ts)          │
│
│  ⑥ TiKV Percolator 处理 Commit：                            │
│     a. 删除 Lock CF：移除 lock_key                           │
│     b. 写 Write CF：(data_key, start_ts) → commit_ts         │
│     c. 写 WAL（Raft 层需要）                                │
│
│  ⑦ TiKV Raft 层再次处理副本复制：                            │
│     和步骤④一样：WAL → AppendEntries → 过半确认 → Apply     │
│
│  ⑧ TiKV RocksDB LSM-Tree：                                   │
│     → 写入 MemTable（内存）                                  │
│     → MemTable 满了 → Flush → SSTable（磁盘）               │
│     → 后台 Compaction 合并小 SSTable                        │
│
│  ⑨ 返回 TiDB Client 成功                                    │
│                                                               │
│ ★ 从 Client 到磁盘走了三层：                                 │
│   Percolator（事务 ACID）→ Raft（副本复制）→ RocksDB（LSM） │
│   → 每层各有独立职责                                        │
│   → TiKV 设计的精华就是这三层清晰分工！                      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiKV 为什么两层架构？| **Percolator（事务 ACID）+ Raft（副本复制）**；两层分工：Raft 保证数据不丢（无事务）；Percolator 保证分布式事务（无副本）|
| 2 | Percolator 三角色？| **Coordinator=TiDB Server / Participator=TiKV Region Raft Leader / PD=Tso Provider** |
| 3 | PreWrite vs Commit 两阶段？| **PreWrite（写数据到 RocksDB + 加锁 Lock CF + Primary Lock）→ Commit（解锁 + 写 Write CF）**；Percolator 比标准 2PC 简化 |
| 4 | Percolator Primary Lock 作用？| **防 Coordinator 挂了** → 其他事务读到 Primary Lock → 超时主动 Rollback 整个事务 |
| 5 | Tso 是什么？| PD 提供的**全局递增时间戳** → start_ts + commit_ts 保证事务时间线；物理时间 + 计数器（高位时间 + 低位计数）|
| 6 | TiKV 三 Column Family？| **Data CF（多版本数据 + commit_ts）/ Lock CF（Primary Lock）/ Write CF（Commit/Rollback Record）** |
| 7 | MVCC 读可见性？| 用 start_ts 做快照 → 找 commit_ts ≤ start_ts 的最大版本 → 碰到 Lock 等待 → 查 Rollback Record |
| 8 | Percolator 异常恢复？| 发现 Primary Lock → 查 Write CF（Commit 还是 Rollback）→ 主动 Rollback 所有 Participator → 去中心化恢复 |
| 9 | Percolator vs Seata AT？| Percolator：**数据库内部事务（TiKV 原生）**；Seata AT：**跨服务事务（微服务）**；两层不同层次 |
| 10 | Raft 层在 TiKV 里做什么？| **每个 Region 一个 Raft Group** → 副本复制（AppendEntries）+ 过半确认 + Leader/Follower 选主；Percolator 操作底层通过 Raft |
| 11 | TiKV Multi-Raft？| **每个 ≈96MB Region 一个 Raft Group** → TiKV 几千个 Region → 几千个并行 Raft Group → 高并发写入 |
| 12 | TiKV vs CockroachDB？| TiKV：**Rust + Percolator 2PC**；CockroachDB：Go + PostgreSQL MVCC；Percolator 简洁但需要 Primary Lock 恢复 |
| 13 | TiKV RocksDB LSM-Tree？| **TiKV 最底层存储引擎** → WAL + MemTable + SSTable + 后台 Compaction；Percolator Lock/Data/Write CF 都存在这里 |
| 14 | TiKV 写路径三层？| **Percolator PreWrite → Raft AppendEntries → RocksDB MemTable**；每层各做自己的事（事务 → 副本 → 存储）|
| 15 | TiKV 读路径？| **Percolator MVCC（查 Lock → 查 Data CF → 查 Write CF）** → 可能需要从其他 Region Leader 读 |
| 16 | TiKV 学习路径？| 源码 tikv/tikv + tikv/raft-rs → Percolator 事务入口 → PreWrite/Commit 流程 → Raft AppendEntries → RocksDB CF 结构 |
| 17 | TiKV 热点问题？| TiKV Region 粒度 ≈96MB → **热点 Key 迁移**（PD 自动检测 + 热点 Region Split）→ 分散负载 |
| 18 | TiKV GC 过期版本？| **GC Worker** → 超过 safe point 的旧版本（Data CF + Write CF）被清理 → 释放磁盘 |
| 19 | TiKV vs Redis Cluster？| TiKV：**Raft + Percolator 事务**（支持分布式事务 + MVCC）；Redis Cluster：**Hash Slot + MOVED 重定向**（无事务 + 最终一致）|
| 20 | TiKV 面试最高频考点？| **两层架构（Percolator + Raft）+ PreWrite/Commit 两阶段 + Primary Lock + 三 Column Family + Tso 全局时间戳** |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiKV v8.0.x（Rust）+ tikv/raft-rs  
**面试考点：20 题**  
**差异化点**：分布式事务 + Raft 副本 两层架构（分布式系统最高频实战模式）
