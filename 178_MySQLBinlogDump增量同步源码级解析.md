# MySQL Binlog Dump 线程 + 增量同步源码级深度解析

> **源码基线**：MySQL 8.0.x（C++）  
> **核心位置**：sql/rpl_binlog_sender.cc + sql/rpl_replica.cc + sql/binlog.cc  
> **面试权重**：⭐⭐⭐⭐⭐（主从复制核心，高频）

---

## 一、Binlog Dump 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL 主从复制核心链路（面试必背）                        │
│                                                               │
│  从库连接主库后，通过 COM_BINLOG_DUMP / GTID_PROTOCOL      │
│  让主库启动一个 Dump 线程，把 binlog 流式推给从库           │
│                                                               │
│  主库侧（Dump 线程）：                                         │
│   ┌─────────────────────────────────────────────────┐         │
│   │ ① 从库发送 COM_BINLOG_DUMP_GTID / FILE_POS     │         │
│   │ ② 主库创建 Binlog_sender（Dump 线程）            │         │
│   │ ③ 按请求的 GTID / file:pos 定位 binlog 起点    │         │
│   │ ④ 循环：读取 binlog event → 压缩 → 发给从库    │         │
│   │ ⑤ 主库有新 binlog event → 主动推（长连接流）    │         │
│   └─────────────────────────────────────────────────┘         │
│                                                               │
│  从库侧（两线程）：                                             │
│   ① I/O Thread：收 Dump 线程发来的 event → 写 Relay Log       │
│   ② SQL Thread：读 Relay Log → 重放到从库执行                 │
│                                                               │
│  ★ 与 TiCDC 的核心区别（面试高频对比表见第六节）：            │
│   MySQL Binlog = 行格式事件 / 基于逻辑 SQL 重放            │
│   TiCDC = KV 变更 CDC / 基于 Percolator + Raft 增量流        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、主库侧：Binlog_sender（Dump 线程）源码级

### 2.1 Dump 线程创建

```cpp
// ★ Binlog_sender 主类（源码级，面试核心）
//
// sql/rpl_binlog_sender.cc  +  sql/rpl_binlog_sender.h
// sql/sql_repl.cc（Server 侧处理 COM_BINLOG_DUMP）
//
// // Server 接收到从库的 COM_BINLOG_DUMP：
// // mysql_sql.cc → dispatch_command() → case COM_BINLOG_DUMP:
// case COM_BINLOG_DUMP:
//   // 1. 校验权限（REPLICATION SLAVE / REPLICATION CLIENT）
//   // 2. 校验 binlog_format（必须 ROW / MIXED 才能完整复制）
//   // 3. 构造 Binlog_sender：
//   std::unique_ptr<Binlog_sender> sender = new Binlog_sender(
//       thd,                            // 当前连接的 THD
//       thd->get_catalog() != "mysql",  // 是否过滤 "mysql" 系统库（8.0+）
//       offset,                         // binlog file offset
//       server_id,                      // 从库 server_id
//       gtid_set,                       // ★ GTID 集合（8.0 推荐方式）
//       flag                            // 压缩/心跳 flags
//   );
//
//   // 4. 初始化：定位 binlog 起点
//   sender->init();
//
//   // 5. ★ 核心循环：阻塞等待 event → 发送给从库
//   sender->run();  // 这个函数会一直阻塞到连接断开
//
```

### 2.2 GTID vs File:Position 定位

```cpp
// ★ GTID 协议 vs 传统 File:Position（面试必背对比）
//
// // 传统 File:Position（5.6 及之前）：
// //   从库指定 mysql-bin.000001 : 4096
// //   主库 Dump 线程按这个物理位置定位
// //   问题：主库重建/切主后 file:pos 无意义
// //
// // GTID 协议（5.6.5+，8.0 推荐）：
// //   从库把自己"已执行的 GTID 集合"发给主库
// //   主库 Dump 线程找"GTID 集合之后"的 binlog event 起点
// //   主库可以：跳过已执行的、识别遗漏的
//
// // Binlog_sender::init() 定位起点：
// void Binlog_sender::init() {
//
//     if (gtid_set.is_empty()) {
//         // 传统 File:Position 模式
//         //   从库传了 file + offset
//         mysql_bin_log.find_log_pos(&log_file_name, &log_pos);
//         //   打开对应的 binlog 文件 → seek 到 offset
//     } else {
//         // ★ GTID 模式（MySQL 8.0 推荐）
//         //   找 binlog events 中 gtids > 从库已有 gtids 的起点
//         mysql_bin_log.find_next_pos_in_gtid_set(
//             &log_file_name, &log_pos, &gtid_set);
//         //   → 跳过从库已经执行过的 GTID
//         //   → 从"第一个缺失的 GTID 事件"开始发
//     }
//
//     // 打开 binlog file handler，准备读取
// }
//
// // ★ 8.0 还有 BINLOG_GTID_ENFORCE（会话变量）：
// // SET SESSION binlog_gtid_enforce = ON;
// // 之后只允许 GTID 协议连接（拒绝 File:Position）
//
// GTID vs File:Position 核心差异（面试高频）：
// ┌──────────────┬────────────────────┬─────────────────────┐
// │                │ File:Position         │ GTID Protocol        │
// ├──────────────┼────────────────────┼─────────────────────┤
// │ 定位方式       │ binlog file + 物理 offset│ GTID 集合（逻辑）  │
// │ 主库切换       │ ❌ 需要重新找 file:pos│ ★ 自动定位缺口   │
// │ 主库重建       │ ❌ 原 file:pos 失效  │ ★ GTID 全局唯一    │
// │ 多源复制       │ ❌ 难处理            │ ★ 天然支持          │
// │ 面试优先级     │ ★ 中                │ ★★★★★ 必背          │
// └──────────────┴────────────────────┴─────────────────────┘
//
```

### 2.3 Dump 线程核心 run() 循环

```cpp
// ★ Binlog_sender::run() 核心循环（源码级，面试核心）
//
// sql/rpl_binlog_sender.cc
//
// void Binlog_sender::run() {
//     // 初始化完成，进入主循环
//     while (!m_thd->killed) {  // 连接没被 kill
//
//         // ① 从 binlog 文件读取下一个 event
//         if (!read_next_event()) {
//             // binlog 文件读完了（暂时没新 event）
//
//             // ② ★ 阻塞等待：有新 binlog event 通知
//             //    mysql_bin_log.wait_for_new_event_to_read(...)
//             //    这里会阻塞最多 heartbeat_period 时间
//             //    期间主库有新 binlog → 立即被唤醒
//             //    超时（heartbeat）→ 发 HEARTBEAT 给从库
//             //    从库靠心跳判断主库还活着
//             wait_for_new_event();
//
//             continue;  // 再读一次
//         }
//
//         // ③ ★ 检查 event 的 GTID：跳过已执行的
//         //    GTID 模式下：如果 event 的 GTID 在从库已执行集合里 → 跳过
//         if (skip_event_due_to_gtid()) continue;
//         //    还可以检查是否跨 server_id（防止环形复制自传）
//         if (skip_event_due_to_server_id()) continue;
//         //    还可以检查 Replicate-Do-DB / Replicate-Do-Table（从库过滤）
//
//         // ④ 压缩 event（如果 COMPRESSION_ALGORITHM=ON）
//         //    mysql 8.0 支持 zstd / zlib / none
//         //    m_compressor.compress(event_data);
//
//         // ⑤ 把 event 发给从库（通过 socket）
//         //    格式：PacketHeader(4B) + EventBody(variable)
//         //    内部调用 net_safe_write()
//         if (send_event_to_client()) {
//             // 从库断开了 → 返回
//             break;
//         }
//
//         // ⑥ 更新 Show Master Status 的 binlog 位置
//         update_binlog_pos();
//     }
//
//     // 清理
// }
//
// ★ 阻塞等待事件的核心机制：
//   binlog.cc 里有一个条件变量 mysql_bin_log.m_events_cond
//   每次新 binlog event 写完 → signal m_events_cond
//   Dump 线程在 wait_for_new_event() 里 wait on 这个 cond
//   这是"写 binlog → Dump 线程立即唤醒 → 推给从库"的关键
//
```

---

## 三、从库侧：I/O + SQL 双线程源码级

```cpp
// ★ 从库双线程模型（源码级）
//
// sql/rpl_replica.cc （8.0 前叫 rpl_slave.cc）
//
// // 线程 1：I/O Thread（接收 Dump）
// void handle_slave_io() {
//     // ① 连接主库
//     //    -> real_connect()
//
//     // ② 发送 COM_BINLOG_DUMP_GTID（GTID 模式）
//     //    把自己已执行的 GTID 集合发给主库
//     master_com->send_gtid_identification(executed_gtid_set);
//     master_com->request_dump_binlog(...)
//
//     // ③ 收 event 循环
//     while (!slave_io_killed) {
//         //    从主库 socket 读下一个 event
//         //    可能是：HeartbeatEvent / GTIDEvent / XIDEvent / RowEvent
//         Log_event *ev = Log_event::read_log_event(
//             net, &ev_cache, ...);
//
//         if (!ev) {
//             // 心跳 event → 更新 last_io_heartbeat
//             // 表示主库还活着
//             continue;
//         }
//
//         // ④ ★ 写 Relay Log
//         //    Relay Log = 从库本地的 binlog 副本
//         //    // relay_log.index 记录有哪些 relay log 文件
//         //    // 文件名：mysql-relay-bin.000001
//         ev->write(relay_log_file, relay_log_pos);
//
//         // ⑤ 更新 Master_Log_File / Exec_Master_Log_Pos
//         //    以及 Relay_Log_File / Relay_Log_Pos
//         slave_state.update();
//     }
// }
//
// // 线程 2：SQL Thread（重放 Relay Log）
// void handle_slave_sql() {
//     while (!slave_sql_killed) {
//         // ① 读 Relay Log 的下一个 event
//         Log_event *ev = next_event();
//
//         if (!ev) {
//             // Relay Log 读完了 → 等 I/O Thread 写入
//             //   wait_for_next_event() 阻塞
//             continue;
//         }
//
//         // ② ★ 执行 event 里的行变更
//         //    Row-based replication：
//         //    WRITE_ROWS_EVENT_V2 / UPDATE_ROWS_EVENT_V2 / DELETE_ROWS_EVENT_V2
//         //    把行数据 INSERT/UPDATE/DELETE 到从库表
//         ev->apply_event(slave_exec_ctx);
//
//         // ③ 对于 GTID 模式：
//         //    更新 Retrieved_Gtid_Set + Executed_Gtid_Set
//         //    // 写入 mysql.generated_log_info（8.0 GTID 持久化表）
//     }
// }
//
// ★ 从库 relay log vs master binlog：
//   relay log 格式和 master binlog 完全相同
//   只是存储路径不同、文件头不同
//   从库可以有多级 relay log（级联复制）
//
```

---

## 四、Binlog Event 类型与行格式

```cpp
// ★ Binlog Event 类型（面试高频）
//
// sql/binlog_event.h
//
// enum Log_event_type {
//     UNKNOWN_EVENT        = 0,
//     START_EVENT_V3       = 1,   // binlog 文件头（版本信息）
//     QUERY_EVENT          = 2,   // SQL 语句（STATEMENT 格式）
//     STOP_EVENT           = 3,   // binlog 关闭
//     ROTATE_EVENT         = 4,   // binlog 文件切换
//     INTVAR_EVENT         = 5,   // 自增 ID 等
//     LOAD_EVENT           = 6,   // LOAD DATA（过时）
//     SLAVE_EVENT          = 7,   // 从库元信息（过时）
//     CREATE_FILE_EVENT    = 8,
//     // ...
//     GTID_EVENT           = 33,  // ★ GTID 事件（8.0 必备）
//     BEGIN_QUERY_EVENT    = 34,  // 事务开始标识
//     EXECUTE_LOAD_QUERY_EVENT = 35,
//     // ★ Row-based replication events（核心）：
//     WRITE_ROWS_EVENT_V0  = 13,  // INSERT 行变更
//     UPDATE_ROWS_EVENT_V0 = 24,  // UPDATE 行变更
//     DELETE_ROWS_EVENT_V0 = 4,   // DELETE 行变更
//     WRITE_ROWS_EVENT_V2  = 30,  // V2 版本（带分区信息）
//     UPDATE_ROWS_EVENT_V2 = 31,
//     DELETE_ROWS_EVENT_V2 = 32,
//     XID_EVENT            = 5,   // ★ 事务 COMMIT 标识
//     HEARTBEAT_EVENT      = 27,  // 心跳（无 payload）
//     PREVIOUS_GTIDS_LOG_EVENT = 36, // 之前 GTID 集合快照
//     ANNOTATE_ROWS_EVENT  = 16,  // 行事件注释
// };
//
// // ★ Row-based binlog event 结构：
// // WRITE_ROWS_EVENT_V2 = {
// //   header: timestamp, event_type, server_id, event_length
// //   post_header: table_id, flags, extra_row_info
// //   body:
// //     varint number_of_columns
// //     bitmap columns_in_image    // 哪些列有值
// //     row data 1... row data N   // 每行都是完整列值（和表 schema 对齐）
// // }
//
// ★ 事务事件序列（面试高频）：
//   BEGIN_QUERY_EVENT           ← 事务开始
//   GTID_EVENT                  ← 本次事务 GTID
//   QueryEvent("BEGIN")         ← 可选（STATEMENT 格式才会有）
//   WRITE_ROWS_EVENT_V2 ...     ← N 个行变更 event
//   UPDATE_ROWS_EVENT_V2 ...
//   DELETE_ROWS_EVENT_V2 ...
//   XID_EVENT                   ← ★ 事务提交标识（最后一个 event）
//
// 从库看到 XID_EVENT 才知道"这个事务完整了"
// 才会认为这批行变更可以安全执行
//
```

---

## 五、半同步 vs GTID vs 并行复制

```cpp
// ★ 三个高频复制增强（面试必背，之前已有覆盖但与 Dump 相关）
//
// // 1. 半同步复制（Semi-Sync Replication）：
// //    rpl_semi_sync_master.cc
// //    主库 Dump 线程等至少一个从库 ACK 收到 event → 再 COMMIT
// //    // 保证主库挂掉时从库不丢最后一个事务
// //    AFTER_SYNC：等 ACK 后才 COMMIT（最安全）
// //    AFTER_COMMIT：先 COMMIT 再等 ACK（最快但弱）
// //
// // 2. GTID 复制：
// //    每个事务有全局唯一 GTID = source_id:transaction_id
// //    从库已执行 GTID 持久化到 mysql.generated_log_info
// //    新从库连接时把 GTID 集合发给主库 → 主库跳过已执行事件
// //    ★ 主从切换（Failover）核心机制：
// //      新主库的 GTID 是旧主库 GTID 集合的超集 → 无缝切换
// //      不需要重新找 file:position
// //
// // 3. 并行复制（Parallel Replication）：
// //    rpl_parallel.cc
// //    从库 SQL Thread 不串行执行 event
// //    而是把 Row Event 按 PRIMARY KEY hash 分发给多个 worker thread
// //    // LOGICAL_CLOCK 模式（8.0 默认）：
// //    // 同一个主库事务内的 event → 同一个 parallel group → 串行
// //    // 不同事务但依赖相同 key → 自动串行
// //    // 不同事务、不同 key → 并行执行
// //    ★ 主库 Dump 线程要把 event 的 last_committed / sequence_number
// //      编码进 GTID_EVENT → 从库据此判断依赖关系
// //
// // 4. 心跳（Heartbeat）：
// //    Dump 线程在没有新 binlog event 时定期发 HEARTBEAT_EVENT
// //    从库靠心跳知道主库还活着、连接没断
// //    rpl_heartbeat_period（从库参数）：默认 30s
// //    心跳 event 极小（只有 header），基本不消耗网络
//
```

---

## 六、差异化对比：MySQL Binlog Dump vs TiCDC vs Kafka Connect Debezium

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比表：三大增量同步方案                         │
│  ┌──────────┬───────────────────┬──────────────┬──────────┐│
│  │ 维度      │ MySQL Binlog Dump    │ TiCDC         │ Debezium ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 数据源    │ MySQL Binlog       │ TiKV Raft Log │ MySQL/PG ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 数据格式  │ RowEvent（完整列值）│ KV CDC（MVCC）│ JSON/AVRO││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 定位机制  │ ★ GTID / File:Pos │ ★ TSO / Region│ LSN/GTID ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 一致性    │ 最终一致（异步）    │ ★ 严格一致   │ 最终一致  ││
│  │            │ 半同步强一致     │ Percolator    │          ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 高可用    │ 半同步 + GTID Failover │ TiKV Raft自动│ Kafka Connect│
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 输出目标  │ 从库 MySQL         │ Kafka / MQ    │ Kafka    ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 变更类型  │ 行级（ROW 格式）   │ KV 级（细粒度）│ 行级 JSON ││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ Schema变更│ DDL Event         │ DDL Log       │ Schema Change││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 运维复杂度│ 低（MySQL 内置）   │ 中（TiDB 生态）│ 中（Kafka Connect）││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 适用场景  │ MySQL 主从复制    │ TiDB 数仓实时 │ CDC → Kafka││
│  ├──────────┼───────────────────┼──────────────┼──────────┤│
│  │ 面试高频  │ ★★★★★            │ ★★★★★        │ ★★★      ││
│  └──────────┴───────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   MySQL 主从 → **Binlog Dump + GTID + 半同步**（MySQL 原生）│
│   TiDB 数仓实时 → **TiCDC**（KV 级、Percolator 一致）      │
│   MySQL CDC → Kafka → **Debezium**（开源生态完整）           │
│   三者配合：MySQL Binlog → Canal → Kafka → TiDB/ES（经典架构）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（28 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Binlog Dump 线程做什么？| 主库接从库 COM_BINLOG_DUMP → 定位起点 → **阻塞读 binlog event 流式推给从库** |
| 2 | GTID vs File:Position？| **GTID（全局唯一、主库切换/重建不受影响）vs File:Pos（物理定位、主库变动失效）** |
| 3 | Binlog_sender 核心 run() 循环？| init()定位 → while没kill: 读event → 跳过已执行GTID → 压缩 → send → 无event就wait_for_new_event() |
| 4 | wait_for_new_event 机制？| 阻塞在 mysql_bin_log.m_events_cond 条件变量；新 binlog 写完 signal 唤醒；超时发 HEARTBEAT |
| 5 | 从库双线程？| **I/O Thread（收 Dump → 写 Relay Log）+ SQL Thread（读 Relay Log → 重放）** |
| 6 | Relay Log 是什么？| 从库本地的 binlog 副本；格式和主库 binlog 相同；存储从主库收到的所有 event |
| 7 | Binlog Event 类型核心？| **GTID_EVENT + BEGIN_QUERY_EVENT + WRITE/UPDATE/DELETE_ROWS_EVENT_V2 + XID_EVENT** |
| 8 | 事务 event 序列？| BEGIN_QUERY → GTID_EVENT → N 个 RowEvent → XID_EVENT（事务提交标识） |
| 9 | XID_EVENT 作用？| 标识事务完整提交；从库只有看到 XID 才认为这批行变更可安全执行 |
| 10 | HEARTBEAT_EVENT 作用？| Dump 线程没新 event 时定期发；从库靠心跳判断主库还活着；极小不耗网络 |
| 11 | Row-based binlog event 结构？| header → post_header(table_id) → row count → columns bitmap → 每行完整列值（和表 schema 对齐） |
| 12 | GTID 复制 Failover 原理？| GTID 全局唯一；新主库 GTID 集合是旧主库的超集 → 从库直接发 GTID 集合定位；**不需要重新找 file:pos** |
| 13 | 半同步复制？| 主库 COMMIT 前等至少一个从库 ACK 收到 event；AFTER_SYNC 最安全（等 ACK 后才 COMMIT）|
| 14 | 并行复制？| 从库 SQL Thread 按 PRIMARY KEY hash 分发给多个 worker；LOGICAL_CLOCK 模式（8.0 默认）根据依赖关系判断并行度 |
| 15 | ★ MySQL Binlog vs TiCDC vs Debezium？| 见第六节对比表（10 维度）|
| 16 | 面试最高频考点排序？| **1. GTID vs File:Pos 2. Binlog_sender run() 循环 3. 双线程模型 4. Row Event + XID 5. 三者对比** |
| 17 | 学习路径？| COM_BINLOG_DUMP → Binlog_sender::init/run → handle_slave_io → handle_slave_sql → RowEvent 结构 |
| 18 | 面试常见追问？| Dump 线程会锁表吗？→ **不会**，Dump 线程只读 binlog（文件级锁），不加 InnoDB 表锁 |
| 19 | 面试常见追问？| Row-based vs Statement-based binlog？→ 8.0 默认 ROW；STATEMENT 有不确定性（RAND、NOW 等）风险 |
| 20 | 面试常见追问？| Show Master Status 里看到什么？| File / Position / Binlog_Do_DB / Binlog_Ignore_DB / Executed_Gtid_Set |
| 21 | 面试陷阱？| 说半同步复制是从库提交慢 → ❌ 半同步是主库"等 ACK 再提交"，保证主从数据一致 |
| 22 | 面试陷阱？| 说 XID_EVENT 是 DDL 事件 → ❌ XID_EVENT 是**事务提交标识**（DML 事务最后一个 event）|
| 23 | GTID 集合持久化在哪？| 主库：mysql.generated_gtids；从库：mysql.generated_log_info；8.0 支持 crash-safe（表级持久化）|
| 24 | Dump 线程能复制哪些？| INSERT/UPDATE/DELETE（ROW 格式）+ DDL（QUERY_EVENT）+ DML 事务边界（XID）|
| 25 | Binlog 压缩？| MySQL 8.0 支持 zstd / zlib / none；Dump 线程用 m_compressor 压缩后 send |
| 26 | 复制中断怎么续？| GTID 模式：从库重新连接时把已执行 GTID 集合发给主库 → 主库跳过已执行 event；File:Pos 模式：记住 Master_Log_File / Exec_Master_Log_Pos |
| 27 | Filter 机制？| 主库：binlog_do_db / ignore_db（在 Dump 线程里过滤）；从库：replicate_do_db / replicate_wild_do_table（SQL Thread 过滤）|
| 28 | 典型拓扑？| MySQL 主从（半同步）→ Canal（拉 binlog）→ Kafka → TiCDC/ES/Spring（消费） |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：sql/rpl_binlog_sender.cc + sql/rpl_replica.cc + sql/binlog.cc + sql/binlog_event.h  
**面试考点：28 题**  
**差异化点**：GTID vs File:Position 定位、Binlog_sender 阻塞等待机制、双线程模型、Row Event + XID 事务边界、MySQL Binlog vs TiCDC vs Debezium 三方案对比
