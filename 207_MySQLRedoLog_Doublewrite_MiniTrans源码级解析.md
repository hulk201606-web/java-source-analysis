# MySQL Redo Log + Doublewrite + Mini-transaction 源码级深度解析

> **源码基线**：MySQL 8.0.x（C++）  
> **核心位置**：storage/innobase/log/ + storage/innobase/buf/ + storage/innobase/trx/  
> **面试权重**：⭐⭐⭐⭐⭐（InnoDB 写路径核心，最高频面试考点）

---

## 一、InnoDB 写路径全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB 写路径 = Buffer Pool → Redo Log → Data Page        │
│                                                               │
│  核心问题（面试必背）：                                        │
│   为什么需要 Redo Log？                                       │
│   直接写 Data Page 不行吗？                                   │
│                                                               │
│   ① Data Page 随机写（慢）vs Redo Log 顺序追加（快 100x）    │
│   ② ACID 持久性需要：写完 Redo Log 就可以 Commit              │
│   ③ 崩溃恢复靠 Redo Log 重放 → 保证 Data Page 最终一致        │
│                                                               │
│  ★ 写路径三步骤（源码级）：                                    │
│                                                               │
│   Step 1: 修改 Buffer Pool 里的 Data Page（内存，很快）       │
│   Step 2: 写 Redo Log（顺序追加到 Log Buffer → 刷盘）         │
│   Step 3: Buffer Pool 里的脏页后台刷盘（Checkpoint）          │
│                                                               │
│   ★ 关键：Step 2 完成 = 事务持久化（可以 Commit）             │
│   Step 3 不影响事务 Commit 的延迟（后台异步）                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Redo Log 源码级

### 2.1 Redo Log 格式 + Block 结构

```c
// ★ InnoDB Redo Log 源码级（面试核心）
//
// storage/innobase/log/log0log.cc
// storage/innobase/log/log0recv.cc
//
// // Redo Log 物理位置：
// //   /var/lib/mysql/ib_logfile0
// //   /var/lib/mysql/ib_logfile1
// //   循环写（写满 ib_logfile0 → 写 ib_logfile1 → 覆盖 ib_logfile0）
// //
// // ★ Redo Log Block 结构（源码级，512 字节）：
// //   storage/innobase/include/log0types.h
// //
// struct log_block_t {
//     uint32_t hdr_no;          // 连续编号（递增）
//     uint16_t len;             // 数据长度
//     uint16_t checksum;        // Block 校验
//     uint32_t start_lsn;       // 起始 LSN（Log Sequence Number）
//     char     payload[...];    // Redo 记录数据
//     uint32_t end_lsn;         // 结束 LSN
// };
//
// // ★ LSN（Log Sequence Number）是什么？
// //   全局单调递增的 8 字节整数
// //   每个 Redo 记录一个 LSN
// //   // 例子：
// //   // LSN = 100000
//   // 追加一个 INSERT 记录 → LSN = 100080
//   // 追加一个 UPDATE 记录 → LSN = 100150
//   // Commit → LSN 继续递增
//   //
//   // LSN 核心用途：
//   //   a. 崩溃恢复的检查点（Checkpoint LSN 之前的都刷盘了）
//   //   b. 判断哪些 Redo 需要重放（> Checkpoint LSN 的）
//
// // ★ Redo 记录类型（高频）：
// //   MLOG_INSERT（B-tree 插入）
// //   MLOG_UPDATE（B-tree 更新）
// //   MLOG_DELETE（B-tree 删除）
// //   MLOG_COMP_REC_INSERT（Compact 格式插入）
// //   MLOG_UNDO_INIT（Undo Log 初始化）
// //   MLOG_COMP_PAGE_CREATE（创建新页）
// //   ... 30+ 种类型
//
```

### 2.2 Mini-transaction 源码级

```c
// ★ Mini-transaction（MTR）—— InnoDB Redo 写入的基本单元（面试核心）
//
// storage/innobase/mtr/mtr0mtr.cc
//
// // 为什么需要 Mini-transaction？
// //   一个 Buffer Pool 操作可能修改多个 Page
// //   例：B-tree 分裂 → 至少修改 3 个 Page（叶子 + 两个内部）
// //   → 这几个修改要么全部成功、要么全部回滚
// //   → 就是一个 Mini-transaction（小事务）
// //
// // ★ MTR 核心特性（源码级）：
// //   一个 MTR = 对一个或多个 Page 的一组修改
// //   所有修改作为一个整体写入 Redo Log
// //   Redo Log 要么包含整个 MTR、要么完全不包含（原子性）
// //
// // ★ MTR 执行流程：
// //
// // 1. 开始 MTR：
// //    mtr_start(&mtr);
// //    // 注册本线程的 MTR 对象
// //
// // 2. 在 MTR 里修改 Page：
// //    mtr_write(&mtr, block, page_offset, data, len);
// //    // 修改 Page 的同时，生成对应的 Redo 记录
// //    // Redo 记录追加到 Log Buffer（内存）
// //
// // 3. 提交 MTR：
// //    mtr_commit(&mtr);
// //    // 把本次 MTR 的所有 Redo 记录整体刷到 Log Buffer
// //    // （注意：还没 fsync 到磁盘！）
// //    // 记录本次 MTR 的结束 LSN
// //
// // 4. 事务 Commit 时刷盘：
// //    // 事务可能包含多个 MTR
// //    // trx_t::commit() → log_write_low() → log_flush()
// //    // 把 Log Buffer 里 Redo Log fsync 到磁盘
// //    // ★ 这一步完成 = 事务持久化
// //
// // ★ MTR 和事务的关系：
// //   一个数据库事务（trx_t）= 多个 MTR（mtr_t）
// //   每个 MTR 负责一小块原子修改
// //   事务 Commit 时把所有 MTR 的 Redo Log 一起刷盘
// //
```

---

## 三、Doublewrite 源码级

### 3.1 半页写问题

```c
// ★ Doublewrite 解决半页写问题（面试核心）
//
// storage/innobase/buf/buf0dblwr.cc
//
// // 什么是半页写（Torn Page Write）？
// //   InnoDB Page = 16KB（默认）
// //   SSD/HDD 扇区 = 512B 或 4KB
// //   → 一次写 Page 要写多个扇区
// //   → 如果写了一半（比如写了 8KB）突然断电
// //   → Page 损坏（前 8KB 是新数据，后 8KB 是旧数据）
// //   → Redo Log 重放也救不了（因为不知道原来的 Page 是什么）
// //
// // ★ Doublewrite 怎么解决？
// //   写 Page 之前先写备份位置！
// //   → 就算主 Page 半写损坏，备份 Page 是完整的
// //   → 崩溃恢复时从备份 Page 恢复完整 Page
//
// // ★ Doublewrite 源码级流程：
// //
// // 1. Buffer Pool 的脏页要刷盘时：
// //    buf_flush_do_batch() → buf_dblwr_write_single_page()
// //
// // 2. ★ 先写 Doublewrite Buffer（备份）：
// //    // Doublewrite Buffer 在共享表空间（ibdata1）里
// //    // 位置：ibdata1 的前 2MB + 之后每个 Extent 的前 2 页
// //    //
// //    // 写入流程：
// //    a. 把 Dirty Page 内容复制到 Doublewrite Buffer 内存
// //    b. Doublewrite Buffer 满了（默认 128 Page = 2MB）
// //       → 一次性 fsync 到 Doublewrite Buffer 磁盘位置
// //    c. ★ 这一步 fsync 完成 = 备份成功
// //
// // 3. 再写真正的 Data Page 位置：
// //    // 把 Doublewrite Buffer 里的 Page 写到真正的数据文件位置
// //    // （这一步就算半写损坏也不怕，因为 Doublewrite Buffer 有完整备份）
// //
// // 4. 两个位置都写成功 → Doublewrite Buffer 清空
//
// // ★ Doublewrite 崩溃恢复：
// //   崩溃后启动 → 扫描 Doublewrite Buffer
// //   → 检查每个备份 Page 和对应的数据文件 Page 校验和
// //   → 如果数据文件 Page 校验和不对（半写损坏）
// //   → 从 Doublewrite Buffer 复制回去修复
// //   → 然后 Redo Log 重放
//
```

### 3.2 Doublewrite 性能影响

```c
// ★ Doublewrite 性能影响（面试高频）
//
// // Doublewrite 代价：
// //   每次刷脏页 → 写两次磁盘（Doublewrite Buffer + 真正位置）
// //   → 理论上磁盘 IO 翻倍
// //   → 实际上：Doublewrite Buffer 先攒满 128 Page（2MB）再一次 fsync
// //   → 减少 fsync 次数 → 实际开销约 5-10%
// //
// // ★ 哪些情况可以关闭 Doublewrite？
// //   ① 使用 Fusion-IO（原子写 Page 的存储设备）
// //      → Fusion-IO 的 NVRAM 可以保证 16KB 原子写
// //      → Doublewrite 多余（但这种设备很少）
// //   ② 使用文件系统提供原子写（写条 ≥ Page Size）
// //      → ZFS / ext4 with 4KB blocks 不能保证 16KB 原子写
// //      → 基本不能关
// //
// // ★ 生产默认开启：
// //   innodb_doublewrite = ON（默认）
// //   // 绝大多数场景必须开（防止半页写损坏）
// //   // 关了可能 Page 损坏且恢复不了
//
// // ★ Redo Log 和 Doublewrite 的区别（面试高频）：
// //   Redo Log：逻辑操作记录（做什么修改）
// //   Doublewrite：物理 Page 备份（完整 Page 内容）
// //
// //   Redo Log 解决：**崩溃后重做已提交的事务**
// //   Doublewrite 解决：**半页写导致的 Page 物理损坏**
// //
// //   两者互补：
// //   Doublewrite 修复物理损坏 → Redo Log 重放逻辑操作 → 数据恢复
//
```

---

## 四、Redo Log + Undo Log + Binlog 两阶段提交

```c
// ★ Redo + Undo + Binlog 三件套 + 两阶段提交（面试终极考点）
//
// // InnoDB 事务流程（源码级简化）：
// //
// // INSERT/UPDATE/DELETE 开始：
// //   1. 写 Undo Log（记录反向操作，用于回滚）
// //      // Undo Log 在 ibdata1 或 undo tablespace
// //      // 每个事务一个 Undo Segments
// //
// //   2. 修改 Buffer Pool 里的 Data Page（内存）
// //      → 同时生成 Redo Log（物理修改记录）
// //      → Redo Log 追加到 Log Buffer（内存）
// //
// // ★ 事务 Commit 两阶段提交（源码级）：
// //
// // Stage 1: Prepare 阶段
// //   a. Undo Log 写盘（最后一个 Undo Record 标为 PREPARED）
// //   b. Redo Log 写盘（Log Buffer fsync 到 ib_logfile）
// //      // ★ 这一步保证：崩溃后 Redo 能恢复到 Prepare 状态
// //
// // Stage 2: Commit 阶段
// //   a. 写 Binlog（Server 层，逻辑格式）
// //      // Binlog 追加到 binlog_cache → fsync 到 mysql-bin.xxxxxx
// //   b. 写 Commit Record 到 Redo Log
// //      // 告诉崩溃恢复：这个事务 Commit 了，可以提交
// //   c. 返回给客户端 OK
// //
// // ★ 崩溃恢复时怎么用两阶段？
// //   崩溃后重启：
// //   1. 扫描最后一个 Checkpoint 之后的 Redo Log
// //   2. 遇到 Undo 状态 PREPARED 但没有 Commit Record 的：
// //      a. 检查 Binlog（有没有这个事务的 Binlog）
// //      b. Binlog 有 → 提交（Stage 2 可能没完成）
// //      c. Binlog 没有 → 回滚（Stage 1 刚完成，Stage 2 还没开始就崩了）
// //   → 这就是两阶段提交保证跨 Redo + Binlog 一致性！
//
```

---

## 五、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redo Log 核心作用？| **崩溃恢复：重放已提交事务的物理修改**；顺序追加（快）；Buffer Pool 脏页后台刷盘 |
| 2 | ★ Redo Log Block 大小？| **512 字节**（和磁盘扇区对齐）；含 hdr_no + len + checksum + start_lsn + payload + end_lsn |
| 3 | ★ LSN 是什么？| **Log Sequence Number，全局单调递增 8 字节整数**；每个 Redo 记录一个 |
| 4 | Mini-transaction 是什么？| **Redo 写入的基本单元**；一组对 Page 的原子修改；整个 MTR 作为一个整体写入 Redo |
| 5 | MTR 执行流程？| mtr_start → 修改 Page 生成 Redo → mtr_commit（Redo 追加到 Log Buffer）→ 事务 Commit 时 fsync |
| 6 | ★ Doublewrite 解决什么问题？| **半页写（Torn Page Write）**：Page 16KB，磁盘扇区 512B，写一半断电 → Page 物理损坏 |
| 7 | Doublewrite 源码级流程？| 脏页刷盘 → **先写 Doublewrite Buffer（备份）→ fsync → 再写真正数据位置** |
| 8 | Doublewrite Buffer 位置？| 共享表空间 ibdata1 的前 2MB + 每个 Extent 前 2 页；满 128 Page（2MB）一次性 fsync |
| 9 | Doublewrite 性能影响？| 额外一次 fsync；实际开销约 5-10%（攒批再写）；绝大多数场景**不能关** |
| 10 | ★ Redo + Doublewrite 核心区别？| Redo：**逻辑操作重放**（崩溃后重做事务）；Doublewrite：**物理 Page 备份**（修复半页写损坏）|
| 11 | ★ Redo + Undo + Binlog 两阶段提交？| Stage 1 Prepare：Undo Log + Redo Log fsync；Stage 2 Commit：写 Binlog + Redo Commit Record + 返回 OK |
| 12 | 崩溃恢复两阶段提交检查？| Undo 状态 PREPARED 但无 Commit Record → 查 Binlog：有 → 提交；无 → 回滚 |
| 13 | innodb_flush_log_at_trx_commit 参数？| **1=每次事务 fsync（默认，最安全）；0=每秒 fsync；2=刷到 OS 缓存** |
| 14 | Redo Log 循环写机制？| ib_logfile0 + ib_logfile1（8.0+改成 ib_redo 目录多文件）；写满 → 检查 Checkpoint 位置能不能覆盖 |
| 15 | ★ MySQL 8.0 Redo Log 新特性？| **Redo Log 改成 4M 小文件 + 动态扩容**；不再是固定 2 个大文件；支持在线调整 innodb_redo_log_capacity |
| 16 | 面试最高频考点排序？| **1. Doublewrite 半页写 2. MTR + LSN 3. Redo+Undo+Binlog 两阶段 4. Redo Log Block 5. fsync 参数** |
| 17 | 学习路径？| Redo Log Block → LSN → MTR → Doublewrite → 两阶段提交 → fsync 参数 |
| 18 | 面试常见追问？| Redo Log 和 Undo Log 都写了，为什么还需要 Binlog？| Redo 是物理格式（InnoDB 专用）；Binlog 是逻辑格式（Server 层，所有引擎通用）；**Binlog 用于主从复制 + Point-in-Time Recovery** |
| 19 | 面试常见追问？| Doublewrite 能用 ZFS 替代吗？| ZFS 保证 512B 原子写，但 **InnoDB Page = 16KB > 512B** → 还是可能半页写；除非 ZFS recordsize ≥ 16KB |
| 20 | 面试陷阱？| 说 "Redo Log 记录的是 SQL 语句" → ❌ **Redo Log 是物理格式（Page 的偏移量+值修改）**；Binlog 才是逻辑格式（SQL 或 Row Event）|
| 21 | 面试陷阱？| 说 "Doublewrite 可以随便关" → ❌ **绝大多数 SSD/HDD 不保证 16KB 原子写**；关了 → Page 半写损坏恢复不了 |
| 22 | Buffer Pool 脏页刷盘触发时机？| Checkpoint 定期触发（innodb_checkpoint_flush_target）；Buffer Pool 不够用触发 LRU 淘汰 |
| 23 | Redo Log 和 Checkpoint 的关系？| Checkpoint LSN 之前的 Redo Log 可以安全覆盖；崩溃恢复只重放 Checkpoint LSN 之后的 |
| 24 | innodb_flush_log_at_trx_commit=2 安全吗？| 不安全！OS 崩溃（断电）可能丢数据；**生产必须用 1** |
| 25 | 典型生产配置？| **innodb_flush_log_at_trx_commit=1 + sync_binlog=1 + innodb_doublewrite=ON** |
| 26 | Redo Log 刷盘异步优化？| MySQL 8.0 支持 Redo Log 异步刷盘（innodb_log_write_ahead_size + innodb_redo_log_flush_async）|
| 27 | Redo Log 崩溃恢复性能？| 只重放 Checkpoint 之后的；一般几秒内完成；如果 Checkpoint 落后太多 → 可能几分钟 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：storage/innobase/log/log0log.cc + mtr/mtr0mtr.cc + buf/buf0dblwr.cc  
**面试考点：27 题**  
**差异化点**：Redo Log 512B Block + LSN、**Mini-transaction 原子写入单元**、Doublewrite 半页写备份、**Redo + Undo + Binlog 两阶段提交**、fsync 参数安全级别
