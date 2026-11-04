# MySQL 源码深度解析（InnoDB 核心 + SQL 引擎）

> **源码基线**：MySQL 8.0.x（最新稳定）  
> **核心仓库**：https://github.com/mysql/mysql-server  
> **面试权重**：⭐⭐⭐⭐⭐（数据库最高频！）

---

## 一、MySQL 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  MySQL Server 分层架构                                       │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │   Connector 层（连接管理 + 用户认证）                  │    │
│  │   → TCP 监听 → 用户/密码认证 → ThreadPerConnection  │    │
│  │   → 每个连接一个线程（连接数多→线程爆炸）              │    │
│  └─────────────────────────────────────────────────────┘    │
│                          ↓                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │   SQL 引擎层（解析 + 优化 + 执行）★ 面试核心           │    │
│  │   ① Parser：SQL 解析（Yacc/Lexer）→ AST              │    │
│  │   ② Optimizer：查询优化器（基于成本的 CBO）           │    │
│  │   ③ Executor：执行计划（Handler 接口）                 │    │
│  │   ④ Table / Index / Buffer Pool 抽象接口              │    │
│  └─────────────────────────────────────────────────────┘    │
│                          ↓                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │   InnoDB 存储引擎层（★ 面试最高频！）                  │    │
│  │   ① Buffer Pool（内存缓存，LRU + 脏页刷新）            │    │
│  │   ② B+Tree Index（聚簇索引 + 二级索引）                │    │
│  │   ③ Transaction Manager（MVCC + Undo/Redo）           │    │
│  │   ④ Lock Manager（行锁 + Gap Lock + Next-Key Lock）    │    │
│  │   ⑤ Log Manager（Undo Log + Redo Log + Binlog）       │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、InnoDB 核心（★★★★★ 面试必问）

### 2.1 B+Tree 索引（聚簇索引 vs 二级索引）

```
// ★ B+Tree vs B Tree 区别（为什么用 B+Tree 不用 B Tree？）
//
// B Tree：每个节点都存数据 → 层数少但每个节点大 → 磁盘 IO 次数多
// B+Tree：只有叶子节点存数据 → 非叶子节点只存 key → 层数多一点但每个节点小 → 磁盘 IO 次数少
// 而且 B+Tree 叶子节点连成链表 → 范围查询极快！
//
// ★ InnoDB 聚簇索引（Clustered Index）：
//   PRIMARY KEY / UNIQUE INDEX → 聚簇索引
//   叶子节点存整行数据（PK + 所有列 + undo 指针 + trx_id + roll_ptr）
//
// ★ InnoDB 二级索引（Secondary Index）：
//   普通 INDEX → 二级索引
//   叶子节点存：索引列值 + PK 值（不是整行数据！）
//
// ★ 回表（Covering Index 反例，面试高频！）：
//   SELECT * FROM t WHERE name = '张三';
//   → name 是二级索引 → 查到 PK → 再去聚簇索引查完整数据（回表）
//   → 两次 B+Tree 查找
//
// ★ 覆盖索引（Covering Index）：
//   SELECT id, name FROM t WHERE name = '张三';
//   → name 是二级索引 → 二级索引叶子节点已有 id + name → 不用回表！
//   → EXPLAIN Extra = Using index
```

### 2.2 MVCC（多版本并发控制，★★★★★）

```
// ★ MVCC 核心：每行有两个隐藏字段
//   trx_id（最后修改的事务 ID）
//   roll_ptr（指向 Undo Log 的指针）
//
// ★ SELECT 怎么判断可见？
//   InnoDB Read View（快照读时生成）
//   m_ids：活跃事务列表
//   min_trx_id：最小活跃事务 ID
//   max_trx_id：下一个要分配的事务 ID
//
//   某行 trx_id 是否可见？
//   if row.trx_id == 当前事务 → 可见
//   if row.trx_id < min_trx_id → 可见（事务已提交）
//   if row.trx_id >= max_trx_id → 不可见（事务还没开始）
//   if min_trx_id <= row.trx_id < max_trx_id → 查 m_ids
//     if row.trx_id not in m_ids → 可见
//     if row.trx_id in m_ids → 不可见（事务还没提交）→ 继续追 Undo
//
// ★ 四种隔离级别（MVCC + Lock 组合实现）：
//   READ_UNCOMMITTED：不加 Lock，不查 Undo → 什么都能读（脏读）
//   READ_COMMITTED：每次 SELECT 都生成新 Read View → 避免不可重复读
//   REPEATABLE_READ（★ 默认）：**第一次 SELECT 生成 Read View，后续复用** → 避免不可重复读
//   SERIALIZABLE：加 Shared Lock → 读写串行化
//
// ★ RC 级别幻读问题：
//   幻读：同一个事务两次 SELECT ... WHERE age > 20 → 结果行数不一样
//   原因：第一次 Read View 后面插入了新行 → 第二次用新 Read View（RC 级别每次都生成新的）→ 能看到新行
//   解决：用 Gap Lock / Next-Key Lock（RR 级别）
```

### 2.3 Undo Log + Redo Log + Binlog（★★★★★）

```
// ★ 三大日志核心对比（面试必问区别）
//
// ┌──────────┬───────────────────────┬──────────────────────────────┬──────────────────────┐
// │   日志     │      Undo Log          │       Redo Log                │      Binlog          │
// ├──────────┼───────────────────────┼──────────────────────────────┼──────────────────────┤
// │ 存储内容  │ 数据修改前的旧版本        │ 物理操作（哪个页、改了什么）    │ 逻辑操作（执行了什么 SQL） │
// │ 作用      │ MVCC（多版本查询）+回滚  │ 崩溃恢复（刷盘前数据丢失恢复）  │ 主从复制 + 数据恢复     │
// │ 模式      │ 追加写（Append Only）    │ 循环写（ib_redo0/1/2...）    │ 追加写（binlog.000001）│
// │ 记录内容  │ INSERT: delete mark 行 │ "page 123, offset 45" 写入    │ "UPDATE t SET x=? WHERE y=?"│
// │ 引擎归属  │ InnoDB                 │ InnoDB                        │ Server 层（所有引擎都有）│
// │ 崩溃恢复  │ 不参与（只做回滚）        │ ★ 参与（redo → 恢复页）        │ ★ 参与（binlog → 重放） │
// │ 顺序      │ 事务开始时先写          │ 事务提交前必写                 │ 事务提交后写            │
// └──────────┴───────────────────────┴──────────────────────────────┴──────────────────────┘
//
// ★ Undo Log 两种类型：
//   insert_undo：INSERT 产生的 → 事务提交后立即删除（MVCC 不需要 undo 版本）
//   update_undo：UPDATE/DELETE 产生的 → 事务提交后保留（MVCC 需要）
//
// ★ Redo Log Group Commit（三阶段刷盘，保证 redo 和 binlog 原子性）：
//   ① Stage 1（Prepare）：InnoDB 写 Redo Log + 标记 prepare
//   ② Stage 2（Commit）：Server 写 Binlog
//   ③ Stage 3（Finish）：InnoDB 写 Redo Log 标记 commit
//   崩溃恢复：如果 Stage 2 崩溃 → Redo 是 prepare 状态 + Binlog 已存在 → 提交；否则 → 回滚
```

### 2.4 锁机制（★★★★★ 面试必问）

```
// ★ 四种 InnoDB 锁（从简单到复杂）
//
// ① Record Lock（行锁，索引记录锁）
//    SELECT * FROM t WHERE id = 1 FOR UPDATE;
//    → id 是主键 → 加 Next-Key Lock
//    → 唯一索引等值查询 → 降级为 Record Lock（不加 Gap Lock）
//
// ② Gap Lock（间隙锁，锁范围不锁行）
//    SELECT * FROM t WHERE id BETWEEN 10 AND 20 FOR UPDATE;
//    → 锁 (10, 20) 之间的间隙 → 不能插入新行
//    → 目的：防止幻读（Phantom Read）
//
// ③ Next-Key Lock（临键锁 = 行锁 + 间隙锁）
//    ★ InnoDB 默认行锁算法！
//    → 锁一个区间 (last_succesful, first_after]
//    → 唯一索引等值查询会降级成 Record Lock（因为等值查询不影响其他间隙）
//
// ④ Insert Intention Lock（插入意向锁）
//    INSERT 时加 → 表示要在某间隙插入
//    → 和 Gap Lock 冲突（防止并发插入导致幻读）
//
// ★ RR 级别 vs RC 级别锁区别：
//   RR：Next-Key Lock（防止幻读）
//   RC：只有 Record Lock（Gap Lock 关闭）→ 但 RC 级别本身不防幻读（用新 Read View）
//
// ★ 死锁检测：
//   InnoDB 自动检测 → 选择回滚代价最小的事务（undo 页数最少）
//   → 返回 Error 1213（Deadlock found）
```

### 2.5 Buffer Pool（内存缓存）

```
// ★ Buffer Pool（InnoDB 内存核心）
//
// 默认配置：innodb_buffer_pool_size（MySQL 8.0 默认 128MB，生产建议 物理内存 50-70%）
//
// 内存结构：
//   Buffer Pool = N 个 Instance（每个 Instance 独立锁，减少全局竞争）
//   每个 Instance 内部：
//     Hash Index：page_id → 控制块（快速查页是否在内存）
//     LRU List：new 区 + old 区（冷热分离，old 区 37/100）
//     Flush List：脏页链表（后台线程刷盘）
//     Free List：空闲页链表
//
// ★ LRU List 冷热分离：
//   new 区（头部）→ 热点数据
//   old 区（尾部）→ 冷数据
//   → 新访问的页放 new 头部（如果在 old 尾部，等 1s 后才升级到 new）
//   → old 区尾部被淘汰
//   → 解决：频繁全表扫描（Warm Buffer 操作）不会把热点数据挤出去！
//
// ★ 脏页刷新（后台线程）：
//   Dirty Page = Buffer Pool 中已修改但还没刷盘的页
//   Page Cleaner Thread（后台线程）
//   触发条件：
//     ① redo_log 满了（强制）
//     ② Buffer Pool free page 少了（强制）
//     ③ background flush（定时）
//     ④ 关闭 MySQL（全部刷盘）
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | B+Tree vs B Tree？| B+Tree **只有叶子节点存数据 + 叶子节点连成链表** → 磁盘 IO 次数少 + 范围查询快；B Tree 每个节点都存数据 |
| 2 | 聚簇索引 vs 二级索引？| 聚簇索引（PRIMARY KEY）：叶子节点存**整行数据**；二级索引（普通 INDEX）：叶子节点存**索引列值 + PK 值**（回表查整行）|
| 3 | 回表 vs 覆盖索引？| 回表：SELECT * 走二级索引 → 先查二级拿到 PK → 再查聚簇索引；覆盖索引：SELECT id, name WHERE name=? → 二级索引已有 id+name → 不用回表（Extra=Using index）|
| 4 | MVCC 核心？| 每行两个隐藏字段 **trx_id（最后修改事务 ID）+ roll_ptr（Undo Log 指针）** → Read View 判断可见性 |
| 5 | RR vs RC 隔离级别区别？| RR：**第一次 SELECT 生成 Read View，后续复用** → 避免不可重复读；RC：每次 SELECT 都生成新 Read View；RC 级别不防幻读（用 Gap Lock 防）|
| 6 | Undo/Redo/Binlog 区别？| Undo：修改前旧版本 + MVCC + 回滚；Redo：物理操作 + 崩溃恢复（刷盘前）；Binlog：逻辑 SQL + 主从复制 + 崩溃恢复；**Undo 属于 InnoDB，Redo 属于 InnoDB，Binlog 属于 Server 层**|
| 7 | 三大日志刷盘顺序？| Undo（事务开始先写）→ Redo（事务提交前必写）→ Binlog（事务提交后写）；Group Commit 三阶段保证 Redo + Binlog 原子性 |
| 8 | 四种 InnoDB 锁？| **Record Lock（行锁）/ Gap Lock（间隙锁）/ Next-Key Lock（行锁+间隙锁，默认）/ Insert Intention Lock（插入意向锁）** |
| 9 | Next-Key Lock 什么时候降级？| **唯一索引等值查询** → 降级为 Record Lock（不加 Gap Lock）；范围查询仍用 Next-Key Lock |
| 10 | RC 级别为什么不用 Gap Lock？| RC 级别用**新 Read View**（每次 SELECT 都生成新的）→ 本身就不防幻读 → Gap Lock 是多余的 |
| 11 | Buffer Pool 默认大小？| MySQL 8.0 默认 128MB；生产建议 **物理内存 50-70%**；N 个 Instance（每个独立锁，减少竞争）|
| 12 | LRU 冷热分离（37/100 原则）？| old 区占 37%，new 区占 63%；**新访问的页如果在 old 尾部，等 1s 才升级到 new 头部** → 防止全表扫描挤掉热点数据 |
| 13 | 死锁怎么处理？| **自动检测 → 回滚代价最小的事务**（Undo 页数最少）；返回 Error 1213 |
| 14 | SELECT * FROM LargeTable 会怎样？| 全表扫描 → Buffer Pool 被全表数据污染（old 区被扫描数据占满）→ 热点数据被挤出；**用 37/100 原则缓解** |
| 15 | InnoDB vs MyISAM？| InnoDB：事务 + 行锁 + 聚簇索引 + MVCC + 崩溃恢复；MyISAM：表锁 + 非聚簇索引 + 快写但不支持事务/崩溃恢复 |
| 16 | 索引失效场景？| 函数运算 / 类型转换 / LIKE 以 % 开头 / 隐式类型转换 / 联合索引最左前缀原则 |
| 17 | 主从复制原理？| ① Master 写 Binlog → ② Slave IO Thread 拉 Binlog → ③ 写 Relay Log → ④ Slave SQL Thread 重放 Relay Log |
| 18 | ACID 怎么保证？| **Atomicity**（Undo Log + GROUP COMMIT）/ **Consistency**（锁 + 约束）/ **Isolation**（MVCC + 锁）/ **Durability**（Redo Log + Binlog）|
| 19 | EXPLAIN 关键字段？| type（ALL/const/eq_ref/ref/range）/ key（实际用的索引）/ rows（预估扫描行数）/ Extra（Using index/Using where/Using filesort）|
| 20 | Buffer Pool 脏页触发刷新的 4 种情况？| ① redo_log 满（强制）② free page 少（强制）③ 后台定时 ④ 关闭 MySQL |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：MySQL 8.0.x（InnoDB C++ 源码 + 用户视角解析）  
**面试考点：20 题**
