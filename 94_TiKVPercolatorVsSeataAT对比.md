# TiKV Percolator vs Seata AT 深度对比

> **面试权重**：⭐⭐⭐⭐⭐（分布式事务最高频对比考点）  
> **核心参考**：TiKV Percolator（Google Spanner 简化版）+ Seata AT（阿里开源）

---

## 一、核心定位差异

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Percolator vs Seata AT 核心定位（面试必知！）        │
│                                                               │
│  ┌──────────────┬───────────────────┬───────────────────┐   │
│  │                │ TiKV Percolator     │ Seata AT          │   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 事务范围       │ **跨 TiKV Region**   │ **跨微服务 DB**   │   │
│  │                │ （单个 TiDB 实例）   │ （多个微服务）   │   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 事务实现       │ **数据库内部 2PC**   │ **微服务层 AT**   │   │
│  │                │ （TiKV 原生）       │ （代理 JDBC）    │   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 一致性协议     │ **Raft + 2PC**       │ 全局锁 + 回滚日志  │   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 业务代码侵入   │ **零（SQL 透明）**    │ **零（JDBC 代理）**│   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 事务管理器     │ TiDB Server          │ Seata TC         │   │
│  │                │ （内置，无外部组件）  │ （外部独立进程）  │   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 核心依赖       │ TiKV + PD            │ Seata TC + Nacos │   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 适用场景       │ **TiDB 分布式事务**  │ **微服务跨 DB 事务**│   │
│  ├──────────────┼───────────────────┼───────────────────┤   │
│  │ 开源项目       │ TiDB / TiKV         │ Seata（阿里）    │   │
│  └──────────────┴───────────────────┴───────────────────┘   │
│
│ ★ 一句话区别（面试最高频！）：                                │
│   TiKV Percolator：**单机 TiDB 跨 Region 事务**（数据库内部）│
│   Seata AT：**多微服务跨 DB 事务**（微服务层代理）           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、流程对比

### 2.1 TiKV Percolator 两阶段（源码级）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Percolator PreWrite + Commit 两阶段                 │
│                                                               │
│  场景：UPDATE orders SET paid WHERE id=1                     │
│        + UPDATE accounts SET balance=balance-100 WHERE id=2 │
│                                                               │
│  ① Begin：get_tso → start_ts = 1000                          │
│  ② MVCC Snapshot Read：start_ts=1000 快照读                  │
│  ③ PreWrite（两阶段第一阶段）                                │
│     TiDB → TiKV Region1(orders)：                            │
│       a. Lock CF 加锁（lock.start_ts=1000）                  │
│       b. Data CF 写新版本（key, start_ts=1000, value=paid）  │
│       c. Raft Persist（WAL + AppendEntries）                 │
│     TiDB → TiKV Region2(accounts)：同上                      │
│     → Primary Lock（第一个 key 当 Primary）                  │
│                                                               │
│  ④ Commit（两阶段第二阶段）                                   │
│     TiDB → TiKV Region1(Primary)：                           │
│       a. 检查 Lock CF（锁还在吗？是的）                      │
│       b. 删 Lock CF（释放锁）                                │
│       c. 写 Write CF（key, start_ts=1000 → commit_ts=1001） │
│       d. Raft Persist                                        │
│     TiDB → TiKV Region2(Secondary)：同样流程                 │
│     → commit_ts > start_ts（必须）                           │
│     → MVCC 读时查 Write CF 判断可见性                        │
│
│ ★ Primary Lock 作用（面试高频）：                              │
│   Coordinator 挂了 → 其他事务看到 Primary Lock                │
│   → 查 Write CF（有没有 Commit/Rollback Record）            │
│   → 没有 → 主动 Rollback 整个事务                            │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Seata AT 一阶段提交

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Seata AT 一阶段提交（核心设计！）                         │
│                                                               │
│  场景：微服务 A(order) 调微服务 B(account) 事务              │
│                                                               │
│  ① Begin：Seata TC → xid（全局事务 ID）                      │
│  ② Branch Register：每个微服务注册 Branch                   │
│     order 微服务 → Seata RM → TC（branch_id=1）             │
│     account 微服务 → Seata RM → TC（branch_id=2）           │
│
│  ③ 一阶段提交（和 PreWrite 类似但有 undo_log！）             │
│     order 微服务 UPDATE orders：                             │
│       a. 查 before image（SELECT * FROM orders WHERE id=1）  │
│       b. 执行业务 SQL（UPDATE）                               │
│       c. 查 after image（SELECT * FROM orders WHERE id=1）   │
│       d. **写 undo_log**（before image + after image）      │
│          → 同一本地事务（业务 SQL + undo_log 一起 Commit）   │
│          → ★ 关键：undo_log 和业务 SQL 在同一个本地事务！   │
│       e. **获取全局锁**（TC 锁管理器）                      │
│          → lock = key + branch_id → TC 记录                 │
│
│  ④ 一阶段 Commit（★ 核心！Seata AT 一阶段就提交！）         │
│     本地事务 Commit（业务 SQL + undo_log 一起提交）          │
│     → **业务数据已经落库了！**                                │
│     → TC 异步通知异步清理 undo_log（后台线程）               │
│
│  ⑤ 全局 Commit（TC 决定）                                   │
│     TC 收到所有 Branch 的成功 → ASYNC_COMMIT                │
│     → 每个 RM 异步清理 undo_log                              │
│     → 全局锁释放                                             │
│     → **二阶段不阻塞！一阶段已经提交完了！**                 │
│
│  ⑥ 全局 Rollback（TC 决定）                                  │
│     TC 收到某个 Branch 失败 → ROLLBACK                       │
│     → 每个 RM 查 undo_log → 用 before image 反向 SQL        │
│     → UPDATE orders SET ... WHERE id=1 AND after_image      │
│     → **乐观锁保护（防脏写）**                              │
│
│ ★ Seata AT 为什么一阶段就能提交？（面试最高频！）            │
│   ① undo_log 和业务 SQL 在同一个本地事务 → ACID 保证       │
│   ② 一阶段 Commit 后数据已经在 DB 里了（本地 ACID）        │
│   ③ 全局锁保证同一行不会被多个全局事务同时写（隔离性）     │
│   ④ Rollback 靠 undo_log 反向 SQL（补偿机制）              │
│   → **这是 Seata AT 最精妙的设计！**                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、核心差异深度对比（★ 面试必背）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 核心差异深度对比表（面试必背！）                          │
│                                                               │
│  ┌──────────────┬──────────────────┬──────────────────┐   │
│  │                │ TiKV Percolator    │ Seata AT           │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 两阶段 vs 一阶段│ **严格两阶段**       │ **一阶段提交**      │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 隔离性实现     │ Lock CF 写锁       │ 全局锁 + undo_log │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 回滚机制       │ 主动 Rollback      │ undo_log 反向 SQL  │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 乐观锁保护     │ Lock CF + Raft     │ after_image 比对  │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ Commit 时本地DB│ **没数据**（只写   │ **有数据**（一阶段│   │
│  │              │ Lock CF）          │ 已提交业务 SQL）  │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 外部依赖       │ TiKV + PD          │ Seata TC + Nacos  │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 全局时间戳     │ PD Tso             │ 不需要（本地 TS） │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 写锁粒度       │ TiKV Region 内     │ 微服务 DB 表内    │   │
│  ├──────────────┼──────────────────┼──────────────────┤   │
│  │ 对业务侵入     │ 零（SQL 透明）      │ 零（JDBC 代理）   │   │
│  └──────────────┴──────────────────┴──────────────────┘   │
│
│ ★ Seata AT 独有的优势：                                      │
│  ① 一阶段提交 → 性能比严格两阶段好（少一次 RTT）            │
│  ② 一阶段就落库 → 中间状态也可用（可用性好）                │
│  ③ 业务 SQL + undo_log 同事务 → ACID 保证 undo_log 原子性  │
│
│ ★ TiKV Percolator 独有的优势：                               │
│  ① Raft 副本复制 → CP 强一致（Seata AT 没有 Raft）         │
│  ② 三 CF MVCC → 成熟的多版本读（Seata 没有）              │
│  ③ 数据库内部实现 → 完全透明（Seata 是微服务代理）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、边界情况处理对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Coordinator 挂了怎么办？（面试高频！）                    │
│                                                               │
│ TiKV Percolator：                                            │
│   Coordinator（TiDB Server）挂了 → 其他事务看到 Primary Lock│
│   → Lock CF 有锁 → 查 Write CF（有没有 Commit/Rollback）    │
│   → 没有 Write CF 记录 → **主动 Rollback 整个事务**        │
│   → Lock TTL 超时（3s）→ TiKV 后台自动清理锁               │
│
│ Seata AT：                                                    │
│   TC 挂了 → 其他 RM 还能正常 Commit/Rollback                │
│   TC 恢复后重新从 Nacos 拉配置 → 恢复服务                   │
│   TC 不会丢数据（Branch/Global 关系持久化）                 │
│
│ ★ 业务回滚怎么办？（面试高频）                               │
│ TiKV Percolator：                                            │
│   Rollback → 直接删 Lock CF → 写 Write CF（WriteType=Rollback）│
│   → MVCC 读时跳过这个 start_ts 版本                         │
│   → 性能快（只写 CF，不修改 Data CF）                      │
│
│ Seata AT：                                                    │
│   Rollback → 查 undo_log → 用 before image 构造反向 SQL    │
│   → UPDATE ... WHERE pk AND after_image（乐观锁）           │
│   → 如果影响行数=0（数据被其他事务修改）→ **RollbackFailed** │
│   → 需要人工介入（这是 Seata AT 的一个风险点！）           │
│
│ ★ 热点 Key 怎么办？（面试高频）                              │
│ TiKV：热点 Key 所在 Region → PD HotRegionScheduler 迁移    │
│ Seata AT：全局锁热点 → TC 锁管理器压力大                    │
│  → Seata 6.x 引入 **Optimistic Branch Lock**（优化热点锁）  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | TiKV Percolator | Seata AT |
|---|------|----------------|----------|
| 1 | 事务范围 | **跨 TiKV Region** | **跨微服务 DB** |
| 2 | 两阶段 vs 一阶段 | **严格两阶段** | **一阶段提交（核心！）** |
| 3 | 隔离性 | Lock CF 写锁 | 全局锁 + undo_log |
| 4 | 回滚机制 | 删 Lock + 写 Write CF | undo_log 反向 SQL |
| 5 | 一阶段后数据状态 | **未落库**（Lock CF 里） | **已落库**（一阶段已提交） |
| 6 | 业务侵入 | **零（SQL 透明）** | **零（JDBC 代理）** |
| 7 | 外部依赖 | TiKV + PD | Seata TC + Nacos |
| 8 | Raft 副本复制？| **有！（CP 强一致）** | 无 |
| 9 | MVCC 多版本？| **有！三 CF（Lock/Data/Write）** | 无 |
| 10 | 全局时间戳？| PD Tso（必须）| 不需要 |
| 11 | TC 挂了怎么办？| Primary Lock + TTL 自动清理 | TC 无状态，恢复快 |
| 12 | Rollback 失败？| 不会（锁机制保证）| **乐观锁冲突 → 需人工介入** |
| 13 | 热点 Key 处理？| PD HotRegion Scheduler | TC 锁管理器压力大 |
| 14 | 对 MySQL 依赖？| 无（TiKV 自带） | 是（代理 MySQL）|
| 15 | 性能对比 | **好（Raft + 两阶段）** | 好（一阶段 + 异步）|
| 16 | 适用场景 | **TiDB 分布式事务** | **微服务跨 DB 事务** |
| 17 | 核心优势 | CP 强一致 + MVCC + 透明 | 一阶段提交 + 跨微服务 |
| 18 | 核心风险 | Coordinator 挂了需要 Rollback | Rollback 乐观锁冲突 |
| 19 | 面试最高频对比点？| ① 一阶段 vs 两阶段 ② Raft vs 无 ③ Coordinator 挂了处理 ④ Rollback 机制 |
| 20 | 选型建议 | TiDB → Percolator；微服务跨 DB → Seata AT |

---

**文档版本：v1.0 · 2026-10-22**  
**面试考点：20 题**  
**差异化点**：两种主流分布式事务方案深度对比（最完整的对比）
