# MySQL 主从复制 + 半同步 + GTID 源码级深度解析

> **源码基线**：MySQL 8.0（C++）  
> **核心位置**：sql/rpl_binlog_sender.cc + sql/rpl_rli.cc + plugin/semisync/  
> **面试权重**：⭐⭐⭐⭐⭐（高可用基石，100% 高频）

---

## 一、复制架构总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 主从复制三大线程（面试必背）                        │
│                                                               │
│  Master：                                                      │
│   ① Binlog Dump 线程（每个 Slave 一个）                       │
│      接收 Slave 请求，把 binlog 事件发出去                     │
│                                                               │
│  Slave：                                                       │
│   ② I/O 线程：连 Master，接收 binlog 写本地 Relay Log        │
│   ③ SQL 线程（8.0 可多线程）：读 Relay Log 重放成数据         │
│                                                               │
│  ┌────────┐  binlog events   ┌────────────┐                  │
│  │ Master │ ───────────────→ │ I/O Thread │                  │
│  │ Dump   │                  └─────┬──────┘                  │
│  │ Thread │                        ↓ 写                       │
│  └────────┘                  ┌────────────┐                  │
│                              │ Relay Log  │                  │
│                              └─────┬──────┘                  │
│                                    ↓ 读并重放                  │
│                              ┌────────────┐                  │
│                              │ SQL Thread │ → 数据           │
│                              └────────────┘                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、异步复制全链路

### 2.1 binlog 事件格式与 Dump

```
┌─────────────────────────────────────────────────────────────┐
│  ★ binlog 事件（Event）结构（面试高频）                      │
│                                                               │
│  一个事务在 ROW 格式下产生多个 event：                        │
│   GTID Event（GTID 模式）                                    │
│   Query Event（BEGIN）                                       │
│   TableMap Event（表元信息）                                 │
│   ★ Write/Update/Delete Rows Event（行镜像，可含多行）       │
│   XID Event（COMMIT）                                        │
│                                                               │
│  每个 event 有固定 header：timestamp + type_code + server_id │
│    + event_size + next_position + flags                      │
│                                                               │
│  Master Dump 线程：                                           │
│  // sql/rpl_binlog_sender.cc                                  │
│  //  - Slave 发 COM_BINLOG_DUMP_GTID（带自己已有 GTID 集合） │
│  //  - Dump 线程定位 Slave 缺的第一个 event，顺序推送         │
│  //  - 新 event 产生后持续发送（长连接）                     │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Slave I/O + SQL 线程

```cpp
// ★ Slave 复制源码级
//
// sql/rpl_slave.cc（8.0 改名 replica）
//
// I/O 线程（queue_event）：
//  ① 连接 Master，注册 slave（带 server_uuid + 已执行 GTID 集合）
//  ② 接收 binlog event
//  ③ 写入 relay log（IO_CACHE 顺序写 + fsync 策略）
//  ④ 更新 master.info / mysql.slave_master_info（接收位点）
//
// SQL 线程（exec_event）：
//  ① 从 relay log 读 event
//  ② 应用到本地：
//     - Query event：执行 SQL（STATEMENT 格式）
//     - Row event：直接构造行操作应用（ROW 格式，更安全）
//  ③ 更新 relay-log.info / mysql.slave_relay_log_info（执行位点）
//
// ★ relaylog 与位点文件（崩溃恢复关键）：
//   slave_master_info：I/O 线程接收位点（master log + pos）
//   slave_relay_log_info：SQL 线程执行位点
//   slave_worker_info：并行复制每个 worker 的位点
//
```

### 2.3 异步复制的数据丢失窗口

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 纯异步复制的问题（面试必背）                              │
│                                                               │
│  Master COMMIT → 写本地 binlog → 返回客户端成功              │
│                 └─→ 异步发送 binlog 给 Slave（晚于返回！）    │
│                                                               │
│  如果 Master 在"返回成功后、binlog 到达 Slave 前"宕机：       │
│   → Slave 提升为新 Master，但缺少最后几个事务                │
│   → ★ 数据丢失 + 新老 Master 数据分歧                        │
│                                                               │
│  解决：半同步复制（semi-sync）                                │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、半同步复制（semi-sync）

### 3.1 AFTER_SYNC（增强半同步）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 半同步两种模式（面试核心！）                        │
│                                                               │
│  ① AFTER_SYNC（★5.7+ 默认，增强半同步 / lossless）          │
│                                                               │
│    Master 提交流程：                                          │
│      写 InnoDB Redo(binlog 两阶段提交)                       │
│        → 写 binlog                                           │
│        → ★ 等待至少一个 Slave ACK（确认收到 binlog）        │
│        → 收到 ACK 后才 InnoDB Commit（事务对用户可见）       │
│        → 返回客户端成功                                      │
│                                                               │
│    关键点：用户看到事务成功时，binlog 一定已到 Slave         │
│    → Master 宕机，新主（该 Slave）一定有这个事务 → 不丢      │
│    且 Slave 收到的是"还未在 Master 提交可见"的事务          │
│      （lossless：无损，不会读到旧主没有的脏数据）            │
│                                                               │
│  ② AFTER_COMMIT（5.5 老半同步）                              │
│                                                               │
│    Master 先 InnoDB Commit（事务已可见）→ 再等 Slave ACK      │
│    问题：等待 ACK 期间其他会话能读到数据，但 Master 若宕机    │
│         Slave 没有这条事务 → 用户读到了"丢失的数据"          │
│                                                               │
│  // plugin/semisync/semisync_master_plugin.cc                │
│  // rpl_semi_sync_master_wait_point 控制等待点：             │
│  //   AFTER_SYNC / AFTER_COMMIT                             │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 超时降级

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 半同步自动降级（面试高频）                                │
│                                                               │
│  rpl_semi_sync_master_timeout（默认 10s）：                   │
│    等待 Slave ACK 超时 → 自动降级为异步复制                  │
│    → 保证 Master 不被拖死                                   │
│    Slave 恢复后 ACK 跟上 → 自动恢复半同步                    │
│                                                               │
│  ACK 判定：                                                    │
│   rpl_semi_sync_master_wait_for_slave_count（默认 1）         │
│    需要几个 Slave ACK 才算成功                               │
│   ACK 是 Slave 把 binlog 写入 relay log（落盘）后发回        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、GTID 全局事务标识

### 4.1 为什么需要 GTID

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 传统复制位点（file + position）的问题                     │
│                                                               │
│  CHANGE MASTER TO MASTER_LOG_FILE='binlog.000123',           │
│                   MASTER_LOG_POS=4567;                       │
│   → 位点是文件+偏移，故障切换后要人工找位点，极易出错         │
│   → 一主多从切换、级联复制场景管理困难                        │
│                                                               │
│  GTID 解决：每个事务全局唯一 ID                              │
│   ★ GTID = server_uuid : transaction_id                     │
│   例：3E11FA47-71CA-11E1-9E33-C80AA9429562:23              │
│   server_uuid：主库唯一标识（首次启动生成，持久化 auto.cnf） │
│   transaction_id：该实例上事务单调递增序号                   │
│
└─────────────────────────────────────────────────────────────┘
```

### 4.2 GTID 自动定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ GTID 复制原理（面试必背）                                 │
│                                                               │
│  ① 每个事务提交时，Master 在 binlog 写一个 GTID Event        │
│     （gtid_mode=ON，enforce_gtid_consistency=ON）            │
│                                                               │
│  ② 每个实例维护 gtid_executed 集合：                          │
│     已经执行/提交的所有 GTID（如 uuid:1-23, uuid:1-5）       │
│     存 mysql.gtid_executed 表 + binlog 中                    │
│                                                               │
│  ③ Slave 连 Master 时：                                       │
│     CHANGE MASTER TO MASTER_AUTO_POSITION=1;                 │
│     → 把自己的 gtid_executed 集合发给 Master                 │
│     → Master Dump 线程做 ★ GTID 集合差集                     │
│       只发送 Slave 缺失的事务（自动定位，无需 file/pos）     │
│                                                               │
│  ④ Slave 重放前检查：                                         │
│     该 GTID 已在 gtid_executed → 跳过（幂等，防重复执行）   │
│     不在 → 执行并加入集合                                    │
│
└─────────────────────────────────────────────────────────────┘
```

### 4.3 GTID 故障切换优势

```
┌─────────────────────────────────────────────────────────────┐
│  ★ GTID 让故障切换自动化（面试高频场景）                     │
│                                                               │
│  主库 M 宕机，从库 S1/S2 提升：                               │
│   传统：人工算 binlog 位点，容易错/丢事务                    │
│   GTID：比较各从库 gtid_executed，选集合最全的提升为新主     │
│        其他从库用 AUTO_POSITION 自动连新主补齐缺失事务       │
│        → 复制链路自动收敛                                    │
│                                                               │
│  配合高可用工具：                                              │
│   MHA / Orchestrator / MGR / 自研 HA 都依赖 GTID 做自动切换 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、并行复制（MTS）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 8.0 多线程复制（Multi-Threaded Slaves）                  │
│                                                               │
│  问题：单 SQL 线程重放慢于主库写入 → 主从延迟                │
│                                                               │
│  演进：                                                        │
│  ① 5.6 库级并行（database）：不同库的事务并行（同库无效）    │
│  ② 5.7 ★ LOGICAL_CLOCK（基于组提交）：                       │
│     主库上【同一组提交】的事务说明互不冲突                    │
│     binlog 里用 last_committed / sequence_number 标记        │
│     last_committed 相同的事务可在 Slave 并行回放             │
│  ③ 8.0 增强：WRITESET 模式                                   │
│     即使不是同组提交，只要修改的行无冲突（写集合不相交）      │
│     也能并行 → 并行度更高                                    │
│                                                               │
│  配置：slave_parallel_type=LOGICAL_CLOCK                      │
│       slave_parallel_workers=N                                │
│       binlog_transaction_dependency_tracking=WRITESET（8.0） │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 复制三大线程？| Master Dump；Slave **I/O 线程**（收 binlog 写 relay log）+ **SQL 线程**（重放）|
| 2 | Relay Log 作用？| I/O 线程写入的中转日志，SQL 线程从中读事件重放 |
| 3 | 异步复制丢数据？| Master 返回成功后 binlog 未到 Slave 即宕机 → 丢事务 |
| 4 | 半同步两种模式？| **AFTER_SYNC（5.7+ 默认增强半同步，无损）+ AFTER_COMMIT（5.5 老模式）** |
| 5 | AFTER_SYNC 流程？| 写 binlog → **等 Slave ACK 后才 InnoDB Commit 可见** → 不丢不脏 |
| 6 | AFTER_COMMIT 问题？| 先 Commit 可见再等 ACK；宕机时用户读到了 Slave 没有的事务 |
| 7 | Slave ACK 时机？| Slave 把 binlog 写入 relay log 落盘后发回 ACK |
| 8 | 半同步超时？| rpl_semi_sync_master_timeout（默认10s）超时降级异步，恢复后自动回半同步 |
| 9 | wait_for_slave_count？| 需要几个 Slave ACK（默认 1）|
| 10 | GTID 格式？| **server_uuid:transaction_id**（如 uuid:23）|
| 11 | GTID 解决什么？| file+pos 位点难管理；GTID 全局唯一，故障切换自动定位 |
| 12 | gtid_executed？| 实例已执行 GTID 集合（gtid_executed 表 + binlog）|
| 13 | AUTO_POSITION？| Slave 上报 gtid_executed，Master 做差集只发缺失事务 |
| 14 | GTID 幂等？| 已在 gtid_executed 的事务直接跳过，防重复回放 |
| 15 | GTID 故障切换？| 选 gtid_executed 最全的从库提升，其他从库自动补齐 |
| 16 | ROW vs STATEMENT？| ROW 记行镜像（安全、大）；STATEMENT 记 SQL（小但函数/自增可能不一致）|
| 17 | 并行复制演进？| 库级并行 → 5.7 LOGICAL_CLOCK（组提交）→ 8.0 WRITESET（写集合无冲突）|
| 18 | LOGICAL_CLOCK 原理？| last_committed 相同的事务主库上同组提交，从库可并行 |
| 19 | WRITESET？| 8.0 按修改行的写集合判断冲突，不相交即可并行，并行度更高 |
| 20 | 面试最高频考点排序？| **1. 三线程 2. 半同步两模式 3. GTID 自动定位 4. 异步丢数据 5. 并行复制** |
| 21 | 学习路径？| rpl_slave → binlog sender → semisync plugin wait_point → GTID event/executed → MTS |
| 22 | 面试常见追问？| 半同步会拖慢主库吗？→ 等一个 ACK 的 RTT；可配超时降级 |
| 23 | 主从延迟原因？| 单 SQL 线程、大事务、从库硬件差、网络；用 MTS/避免大事务缓解 |
| 24 | 主从延迟怎么查？| SHOW SLAVE STATUS 的 Seconds_Behind_Master（8.0 看 GTID 差值更准）|
| 25 | 半同步 vs MGR？| 半同步仍是主从异步回放；MGR 基于 Paxos 组复制，多数派确认更强一致 |
| 26 | 面试陷阱？| 说半同步是强一致/不丢任何数据 → ❌ 超时会降级异步；AFTER_SYNC 保证"已确认事务不丢" |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：sql/rpl_slave.cc + rpl_binlog_sender.cc + plugin/semisync/  
**面试考点：26 题**  
**差异化点**：三线程/relay log + AFTER_SYNC vs AFTER_COMMIT + GTID 差集自动定位 + LOGICAL_CLOCK/WRITESET 并行
