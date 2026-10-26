# MySQL Redo Log + WAL + binlog 两阶段提交源码级深度解析

> **源码基线**：MySQL 8.0 / InnoDB（C++）  
> **核心位置**：storage/innobase/log/ + sql/binlog*  
> **面试权重**：⭐⭐⭐⭐⭐（crash-safe 与主从复制基石，100% 高频）

---

## 一、WAL 核心思想

```
┌─────────────────────────────────────────────────────────────┐
│  ★ WAL = Write-Ahead Logging（预写式日志）                  │
│                                                               │
│  核心规则（面试必背）：                                       │
│   ★ 数据页落盘前，必须先把对应的 Redo Log 落盘              │
│                                                               │
│  为什么需要 WAL？                                             │
│  InnoDB Buffer Pool 中的脏页不会每次修改都刷盘（随机 IO 慢）│
│  → 修改先顺序写 Redo Log（顺序 IO 快）                      │
│  → 即使脏页没刷盘，崩溃后也能用 Redo Log 重放恢复           │
│                                                               │
│  好处：                                                        │
│   ① 顺序写日志比随机写数据页快得多                           │
│   ② 崩溃安全（crash-safe）：已提交事务不丢                   │
│   ③ 可以用大 Buffer Pool 缓存更多脏页，攒批量刷盘           │
│                                                               │
│  ★ 两个核心 LSN（日志序列号）：                              │
│   redo log 自己的 LSN（日志写到哪了）                        │
│   数据页头部的 LSN（这个页最后被哪个日志修改过）             │
│   崩溃恢复：Redo Log 里 LSN > 数据页 LSN 的部分需要重放     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Redo Log 三层结构

### 2.1 Log Buffer + Redo Log File + 脏页

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redo Log 流转（面试必背链路）                             │
│                                                               │
│  事务修改数据：                                                │
│  ① 修改 Buffer Pool 中的数据页（变成脏页，内存中）           │
│  ② 同时写 Redo Log 到 Log Buffer（内存）                     │
│        innodb_log_buffer_size（默认 16MB）                   │
│                                                               │
│  ③ Log Buffer 刷到 Redo Log File（磁盘）的时机：             │
│    a. ★ 事务 COMMIT 时（innodb_flush_log_at_trx_commit 控制）│
│    b. Log Buffer 空间用满一半 / 每秒定时（master thread）    │
│    c. 脏页刷盘前（WAL：日志必须先落盘）                      │
│    d. Checkpoint 推进时                                     │
│                                                               │
│  ④ 脏页由后台线程异步刷到数据文件（.ibd）                    │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 innodb_flush_log_at_trx_commit（面试必背！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redo Log 刷盘策略三档（面试 100% 考）                     │
│                                                               │
│  =1（默认，最安全）：                                        │
│    每次 COMMIT 都把 Redo Log 【fsync 刷到物理磁盘】          │
│    → 崩溃不丢已提交事务，但性能最低（每次 fsync）            │
│                                                               │
│  =0（性能最高，最不安全）：                                  │
│    COMMIT 什么都不做，每秒由后台把 Log Buffer 写盘+fsync     │
│    → MySQL 崩溃/掉电最多丢 1 秒事务                          │
│                                                               │
│  =2（折中）：                                                │
│    每次 COMMIT 写到 OS Page Cache（write），每秒 fsync 一次  │
│    → MySQL 进程崩溃不丢（OS Cache 还在）                     │
│    → ★ 服务器掉电会丢 1 秒                                   │
│                                                               │
│  write（写 OS Cache）≠ flush（fsync 真正落盘）               │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.3 循环写 + Checkpoint

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redo Log 循环写入（面试高频）                             │
│                                                               │
│  Redo Log 是固定大小的一组文件（ib_logfile0/1，循环覆盖）   │
│  MySQL 8.0 默认 2 个文件，可动态调整                         │
│                                                               │
│       write pos（写入点）                                     │
│          ↓                                                    │
│  ┌───────────────────────┐                                  │
│  │ 新日志写这里 →         │                                  │
│  └───────────────────────┘                                  │
│          ↑                                                    │
│       checkpoint（检查点）                                    │
│                                                               │
│  write pos 到 checkpoint 之间 = 新日志可写空间               │
│  checkpoint 之前的日志对应的脏页都已刷盘 → 日志可覆盖        │
│                                                               │
│  ★ 写得太快、脏页刷太慢 → write pos 追上 checkpoint          │
│    → Redo Log 写满 → 数据库必须停写，强制刷脏页推进 CP       │
│    （生产监控 redo log checkpoint age！）                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、binlog 与两阶段提交（核心！）

### 3.1 Redo Log vs binlog 区别

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 为什么 MySQL 有两份日志？（面试必背对比表）               │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │            │ Redo Log      │ binlog        │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 所属层     │ InnoDB 引擎层 │ MySQL Server 层│               │
│  │ 记录内容   │ 物理日志       │ 逻辑日志       │                │
│  │            │（页号+偏移+改啥）│（SQL/行变更） │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 写入方式   │ 循环写（覆盖） │ 追加写（不覆盖）│               │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 作用       │ crash-safe   │ 主从复制+归档  │                │
│  │            │ 崩溃恢复       │ 数据恢复/PITR  │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ binlog 格式│ —             │ STATEMENT/ROW/ │               │
│  │            │               │ MIXED         │                │
│  └──────────┴──────────────┴──────────────┘                │
│
│  ★ 如果没有两阶段提交会怎样？（理解它存在的意义）             │
│                                                               │
│  情况 A：先写 Redo 后写 binlog                              │
│    Redo 写完 → binlog 还没写完 → Master 崩溃                │
│    → 崩溃恢复后事务提交成功                                 │
│    → 但 binlog 没这条变更 → Slave 少了数据 → ★主从不一致   │
│                                                               │
│  情况 B：先写 binlog 后写 Redo                              │
│    binlog 写完 → Redo 没写 → Master 崩溃                    │
│    → 崩溃恢复后事务回滚（没有 Redo）                        │
│    → 但 binlog 已发出去 → Slave 多了数据 → ★主从不一致     │
│                                                               │
│  → 必须用两阶段提交保证两份日志的【原子一致】               │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 两阶段提交流程（面试核心！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 内部 XA 两阶段提交（Redo Log × binlog）            │
│                                                               │
│  // storage/innobase/handler/ha_innodb.cc + binlog.cc        │
│                                                               │
│  事务 COMMIT 时：                                              │
│                                                               │
│  ══ 阶段一：Prepare ══                                       │
│  ① InnoDB 把事务的 Redo Log 刷盘                             │
│     Redo Log 里标记状态 = ★ PREPARE                          │
│     （记录一个 XID，事务唯一标识）                           │
│                                                               │
│  ══ 阶段二：写 binlog + Commit ══                            │
│  ② Server 层写 binlog（并 fsync 落盘）                      │
│     binlog event 里也记录同一个 XID                          │
│                                                               │
│  ③ InnoDB 写 Commit 标记到 Redo Log（提交完成）             │
│                                                               │
│  时序图：                                                      │
│  InnoDB: Redo(prepare) ──→ Server: binlog(fsync) ──→ Redo(commit)│
│                                                               │
│  ★ 崩溃恢复时如何裁决（面试必背）：                          │
│                                                               │
│  崩溃重启，扫描 Redo Log 里的 PREPARE 事务：                 │
│   a. 拿 XID 去 binlog 里查：                                  │
│      ★ binlog 里有这个 XID 且完整 → 提交事务（重放 Redo）  │
│      ★ binlog 里没有 / 不完整 → 回滚事务                    │
│   b. 因为 binlog 是完整写入的（一个 event 要么全有要么没有）│
│      → binlog 完整 = 它已经（或将要）发给 Slave              │
│      → Master 必须也提交，保证主从一致                      │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 sync_binlog 参数

```
┌─────────────────────────────────────────────────────────────┐
│  ★ binlog 刷盘策略（面试高频）                               │
│                                                               │
│  sync_binlog=0：每次写 binlog 只到 OS Cache，靠 OS fsync    │
│    → 性能高，掉电可能丢 binlog                               │
│                                                               │
│  sync_binlog=1（最安全）：                                   │
│    ★ 每次事务提交都 fsync binlog                             │
│    → 配合 innodb_flush_log_at_trx_commit=1 = "双 1"配置    │
│    → 生产最强一致性（主从不丢数据），性能最低                │
│                                                               │
│  sync_binlog=N：每 N 次事务 fsync 一次（折中）               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、崩溃恢复流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB Crash Recovery 完整流程（面试串联题）              │
│                                                               │
│  MySQL 异常宕机后重启：                                       │
│                                                               │
│  ① 找到最近的 Checkpoint LSN                                 │
│  ② 从该 LSN 开始重放 Redo Log（前滚 roll-forward）          │
│     → 把已提交但脏页没刷盘的修改恢复到数据页                │
│  ③ 处理未提交事务：                                          │
│     → 结合两阶段提交：binlog 完整的 PREPARE 事务 → 提交     │
│     → binlog 不完整的 → 用 Undo Log 回滚（rollback）        │
│  ④ 恢复完成，数据库正常服务                                  │
│                                                               │
│  ★ Redo（前滚）+ Undo（回滚）分工：                          │
│   Redo Log：保证已提交的不丢（持久性 durability）           │
│   Undo Log：保证未提交的可回滚（原子性 atomicity）+ MVCC     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | WAL 核心规则？| **数据页落盘前，Redo Log 必须先落盘**；顺序日志换随机 IO |
| 2 | Redo Log 作用？| **crash-safe 崩溃恢复**：重放已提交但没刷盘的脏页 |
| 3 | Redo vs binlog？| Redo 物理/引擎层/循环写/崩溃恢复；binlog 逻辑/Server 层/追加写/复制归档 |
| 4 | 为什么要两阶段提交？| 保证 Redo 和 binlog **原子一致**，避免崩溃后主从数据不一致 |
| 5 | 两阶段流程？| **① Redo prepare（带 XID）② 写 binlog fsync ③ Redo commit** |
| 6 | 崩溃怎么裁决 PREPARE 事务？| **binlog 有完整 XID → 提交；没有/不完整 → 回滚** |
| 7 | flush_log_at_trx_commit？| **1=每次 commit fsync（安全）；0=每秒刷（丢 1s）；2=写 OS Cache（掉电丢 1s）** |
| 8 | sync_binlog？| 1=每次 fsync（配合双 1 最强一致）；0=靠 OS；N=N 次刷一次 |
| 9 | 双 1 配置？| **innodb_flush_log_at_trx_commit=1 + sync_binlog=1**（生产最强一致）|
| 10 | Redo Log 结构？| Log Buffer（16MB）→ Redo File（循环写）；write pos / checkpoint |
| 11 | Redo 写满怎么办？| write pos 追上 checkpoint → 停写，强制刷脏页推进检查点 |
| 12 | binlog 三种格式？| STATEMENT（SQL，可能不一致）/ ROW（行镜像，安全但大）/ MIXED |
| 13 | 崩溃恢复流程？| 找 checkpoint → Redo 前滚 → PREPARE 事务按 binlog 裁决 → Undo 回滚未提交 |
| 14 | Redo vs Undo 分工？| Redo 前滚保证持久性；Undo 回滚保证原子性 + MVCC |
| 15 | 面试最高频考点排序？| **1. 两阶段提交流程 2. 崩溃裁决 3. 刷盘三参数 4. Redo vs binlog 5. WAL** |
| 16 | 学习路径？| log0log.cc Redo 写入 → binlog prepare/commit → 崩溃恢复 trx0trx → checkpoint |
| 17 | 面试常见追问？| 为什么 redo prepare 后还要 commit？→ prepare 是待定，binlog 成功才最终提交 |
| 18 | Log Buffer 何时刷盘？| commit / 用满一半 / 每秒定时 / 脏页刷盘前 / checkpoint |
| 19 | binlog 为什么追加写？| 要保留所有历史变更用于复制和时间点恢复（PITR）|
| 20 | 组提交 Group Commit？| 多个事务的 Redo/binlog 合并一次 fsync，减少 IO（MySQL 5.7+ 优化）|
| 21 | MySQL 8.0 Redo 改进？| Redo Log 可动态调整大小、多文件；日志架构重构 |
| 22 | write 和 fsync 区别？| write 到 OS Page Cache；fsync 才真正落物理盘 |
| 23 | 面试高频场景？| 主从丢数据排查 + 双 1 配置取舍性能 + 崩溃恢复原理 |
| 24 | 面试陷阱？| 把 Redo Log 和 binlog 混为一谈，或说两阶段提交是分布式事务 XA → 这是 MySQL 内部 XA（同一实例两日志）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：storage/innobase/log/ + handler/ha_innodb.cc + sql/binlog.cc  
**面试考点：24 题**  
**差异化点**：WAL/循环写/Checkpoint + Redo×binlog 两阶段 XID 裁决 + 双 1 配置 + 崩溃恢复前滚/回滚
