# MySQL MVCC + Undo Log + ReadView 源码级深度解析

> **源码基线**：MySQL 8.0 / InnoDB（C++）  
> **核心位置**：storage/innobase/trx/ + storage/innobase/read/  
> **面试权重**：⭐⭐⭐⭐⭐（InnoDB 事务核心，面试 100% 高频）

---

## 一、MVCC 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MVCC = Multi-Version Concurrency Control（多版本并发控制）│
│                                                               │
│  解决的问题：                                                 │
│   读写互相阻塞 → 读加锁（共享锁）会阻塞写，写会阻塞读        │
│   MVCC：快照读不加锁 → 读旧版本 → 读写不互相阻塞            │
│
│  InnoDB MVCC 三大支柱（面试必背！）：                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ ① 隐藏字段：trx_id + roll_pointer（每行都有）          │  │
│  │ ② Undo Log：版本链（历史版本）                         │  │
│  │ ③ ReadView：一致性视图（判断哪个版本可见）            │  │
│  └───────────────────────────────────────────────────────┘  │
│
│  ★ 两种读的区别（面试高频！）：                               │
│   快照读（Snapshot Read）：普通 SELECT                       │
│     → 读历史版本，不加锁（MVCC）                            │
│   当前读（Current Read）：                                   │
│     → SELECT ... FOR UPDATE / LOCK IN SHARE MODE / UPDATE/DELETE│
│     → 读最新版本，加锁（Next-Key Lock 防幻读）             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、隐藏字段 + Undo Log 版本链

### 2.1 行记录隐藏字段

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB 聚簇索引每行记录的隐藏列（面试必背！）             │
│                                                               │
│  ┌────────────┬──────────────────────────────────────┐      │
│  │ DB_TRX_ID  │ 最近修改该行的事务 ID（6 字节）        │      │
│  ├────────────┼──────────────────────────────────────┤      │
│  │ DB_ROLL_PTR│ 回滚指针，指向 Undo Log（7 字节）     │      │
│  ├────────────┼──────────────────────────────────────┤      │
│  │ DB_ROW_ID  │ 隐藏行 ID（无主键时才生成，6 字节）    │      │
│  └────────────┴──────────────────────────────────────┘      │
│
│  ★ 版本链结构（Undo Log 串联）：                              │
│                                                               │
│  当前行（聚簇索引）                                            │
│  ┌──────────────────────────────────────┐                    │
│  │ name=Bob, trx_id=300, roll_ptr ──┐  │                    │
│  └───────────────────────────────────┼──┘                    │
│                                      ↓                        │
│  Undo Log (update undo) ┌──────────────────────────┐         │
│  ┌───────────────────────┤ name=Alice,trx_id=200    │         │
│  │                       │ roll_ptr ──┐             │         │
│  └───────────────────────┴────────────┼─────────────┘         │
│                                       ↓                       │
│                        ┌──────────────────────────┐          │
│                        │ name=Alice_old,trx_id=100│          │
│                        │ roll_ptr = NULL（链尾）  │          │
│                        └──────────────────────────┘          │
│                                                               │
│  每次 UPDATE：旧值写入 Undo Log + 新行 roll_ptr 指向旧 Undo  │
│  → 形成单向链表（越往链头版本越新）                          │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Undo Log 两种类型

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Undo Log 两类（面试高频！）                               │
│                                                               │
│  ① Insert Undo Log：                                         │
│     INSERT 产生；事务提交后可直接删除（新数据对别的事务本来就不可见）│
│                                                               │
│  ② Update Undo Log：                                         │
│     UPDATE/DELETE 产生；必须保留版本链                      │
│     → 供 MVCC 快照读 + 事务回滚使用                          │
│     → Purge 线程在没有任何 ReadView 需要旧版本时才清理       │
│
│  // storage/innobase/trx/trx0rec.cc                          │
│  // Undo Log 也写 Redo Log（防止 Undo 页本身损坏）           │
│  // Undo 段（Rollback Segment）管理：                        │
│  //   MySQL 5.6+ 128 个 Rollback Segment（支持高并发事务）   │
│
│  ★ Undo Log 三大作用：                                        │
│   ① 事务回滚（原子性）：失败时用 Undo 恢复旧值               │
│   ② MVCC 快照读（隔离性）：版本链提供历史版本                │
│   ③ Purge 清理：删除标记的行真正物理删除                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、ReadView 源码级（核心！）

### 3.1 ReadView 数据结构

```cpp
// ★ InnoDB ReadView 核心结构（源码级，面试核心！）
//
// // storage/innobase/include/read0types.h
//
// class ReadView {
//     // ★ ① 创建 ReadView 时，当前活跃（未提交）事务 ID 列表
//     trx_ids_t m_ids;
//
//     // ★ ② m_ids 中的最小值（活跃事务下界）
//     trx_id_t m_low_limit_id;
//
//     // ★ ③ 下一个将分配的事务 ID（系统最大 trx_id + 1，上界）
//     trx_id_t m_up_limit_id;
//
//     // ★ ④ 创建该 ReadView 的事务自身 ID
//     trx_id_t m_creator_trx_id;
//
//     // ★ 核心：判断某个版本（trx_id）对当前 ReadView 是否可见
//     bool changes_visible(trx_id_t id) const {
//         // 情况 A：id == 自己的事务 → 自己的修改，可见
//         if (id == m_creator_trx_id) return true;
//
//         // 情况 B：id < m_up_limit_id（比所有活跃事务都小）
//         //        → 该事务在创建视图前已提交 → 可见
//         if (id < m_up_limit_id) return true;
//
//         // 情况 C：id >= m_low_limit_id（比视图创建时所有事务都新）
//         //        → 视图创建后才开始的事务 → 不可见
//         if (id >= m_low_limit_id) return false;
//
//         // 情况 D：id 在 [up_limit, low_limit) 区间
//         //        → 在活跃列表里？未提交 → 不可见
//         //        → 不在活跃列表？已提交 → 可见
//         return !m_ids.contains(id);
//     }
// };
//
```

### 3.2 可见性判断规则（面试必背！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ReadView 版本可见性四规则（面试 100% 考！）               │
│                                                               │
│  沿版本链逐个检查 trx_id：                                    │
│                                                               │
│  ① trx_id == creator_trx_id（自己的事务）→ ✅ 可见          │
│  ② trx_id < up_limit_id（视图前已提交）→ ✅ 可见             │
│  ③ trx_id >= low_limit_id（视图后才开始）→ ❌ 不可见        │
│  ④ up_limit_id ≤ trx_id < low_limit_id：                     │
│       在 m_ids 活跃列表中 → ❌ 不可见（未提交）             │
│       不在 m_ids 中 → ✅ 可见（创建视图前已提交）          │
│                                                               │
│  不可见怎么办？→ 沿 roll_pointer 找 Undo Log 上一个版本      │
│  直到找到第一个可见版本 → 返回                                │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 RC vs RR 生成 ReadView 时机（面试最高频！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Read Committed vs Repeatable Read 本质区别（必背！）       │
│                                                               │
│  RC（读已提交）：                                             │
│   ★ 每条 SELECT 都重新生成一个 ReadView                      │
│   → 能看到别的事务已提交的最新数据（不可重复读）            │
│                                                               │
│  RR（可重复读，默认隔离级别）：                               │
│   ★ 事务中第一次 SELECT 时生成 ReadView，之后复用            │
│   → 整个事务看到的快照一致（可重复读）                      │
│                                                               │
│  // storage/innobase/trx/trx0trx.cc                          │
│  // 快照读入口：row_search_mvcc()                             │
│  //   RR：prebuilt->select_lock_type == LOCK_NONE 且         │
│  //        trx 首次读 → trx_assign_read_view()               │
│  //   RC：每次快照读都重新 trx_assign_read_view()            │
│
│  ★ RR 级别幻读怎么防？                                        │
│   快照读：MVCC（ReadView 复用，新插入的行版本不可见）       │
│   当前读：Next-Key Lock（Record Lock + Gap Lock 锁住范围）  │
│   → 两种读分别解决，InnoDB RR 下避免幻读                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、完整读流程示例

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MVCC 快照读完整流程（面试串联题）                         │
│                                                               │
│  事务 A（trx_id=300）执行：SELECT name FROM t WHERE id=1     │
│  RR 级别，首次读，生成 ReadView：                             │
│    m_ids = [200, 300]（200 未提交，300 是自己）              │
│    up_limit_id = 200，low_limit_id = 301                     │
│                                                               │
│  版本链：                                                     │
│  当前行：name=Bob,    trx_id=300 → 自己改的 → ✅ 可见        │
│    （如果是别的事务改的，继续沿链找）                        │
│  Undo1： name=Alice,  trx_id=200 → 在 m_ids → ❌ 不可见     │
│  Undo2： name=Alice_old,trx_id=100 → <up_limit(200) → ✅ 可见│
│                                                               │
│  执行步骤：                                                    │
│  ① row_search_mvcc 走聚簇索引找到当前行                     │
│  ② 读 DB_TRX_ID，changes_visible 判断                       │
│  ③ 不可见 → DB_ROLL_PTR 找 Undo Log 旧版本                  │
│  ④ 循环直到找到可见版本 → 返回                                │
│  ⑤ 全程不加锁（快照读）                                      │
│                                                               │
│  ★ 事务 B(trx_id=200) 此刻 COMMIT：                          │
│     事务 A 再 SELECT（RR，复用旧 ReadView）：                │
│     200 仍在旧 m_ids 中 → 旧版本 → 还是看不到 B 的修改      │
│     → 这就是可重复读！                                       │
│     （RC 级别会重建 ReadView，m_ids 不再含 200 → 能看到）   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | MVCC 是什么？| 多版本并发控制；**快照读历史版本不加锁，读写不互相阻塞** |
| 2 | MVCC 三大支柱？| **隐藏字段（trx_id/roll_ptr）+ Undo Log 版本链 + ReadView** |
| 3 | 三个隐藏字段？| **DB_TRX_ID（最近修改事务）+ DB_ROLL_PTR（Undo 指针）+ DB_ROW_ID（无主键才有）** |
| 4 | Undo Log 两类？| Insert Undo（提交即删）+ Update Undo（版本链，Purge 延迟清理）|
| 5 | Undo Log 作用？| **①回滚（原子性）②MVCC 快照读（隔离性）③Purge** |
| 6 | ReadView 四个字段？| **m_ids（活跃事务）+ low_limit_id + up_limit_id + creator_trx_id** |
| 7 | 可见性四规则？| 自己可见；<up_limit 可见；≥low_limit 不可见；中间看在不在 m_ids |
| 8 | RC vs RR 本质区别？| **RC 每条 SELECT 重建 ReadView；RR 首次 SELECT 生成后整个事务复用** |
| 9 | 快照读 vs 当前读？| 普通 SELECT 走 MVCC；FOR UPDATE/LOCK/UPDATE/DELETE 走当前读加锁 |
| 10 | RR 怎么防幻读？| **快照读靠 MVCC；当前读靠 Next-Key Lock（Record+Gap）** |
| 11 | 版本链怎么遍历？| 当前行 DB_TRX_ID 不可见 → DB_ROLL_PTR 找 Undo 旧版本 → 循环到可见 |
| 12 | 面试最高频考点排序？| **1. RC vs RR 的 ReadView 时机 2. 可见性四规则 3. 版本链 4. 防幻读 5. Undo 作用** |
| 13 | 学习路径？| 隐藏列 → undo 版本链 → read0types.h ReadView → row_search_mvcc → trx_assign_read_view |
| 14 | 面试常见追问？| Undo Log 什么时候删？→ 没有 ReadView 需要该版本时 Purge 线程清理 |
| 15 | ReadView 何时创建？| RR：事务首次快照读；RC：每次快照读（不是 BEGIN 时！）|
| 16 | Delete 在 MVCC 里？| DELETE 是打删除标记（delete mark），旧版本同样进 Undo，Purge 才物理删 |
| 17 | 事务 ID 哪来？| trx_sys 全局单调递增分配 |
| 18 | Undo 和 Redo 区别？| Undo 存旧值（回滚+MVCC）；Redo 存物理修改（崩溃恢复，WAL）|
| 19 | RR 能完全防幻读吗？| 同一事务内快照读+当前读混用可能出现；纯快照读/纯当前读可避免 |
| 20 | 长事务的危害？| 旧 ReadView 持有 → Undo 版本链不能 Purge → Undo 表空间膨胀 |
| 21 | 面试高频场景？| 余额查询不被更新阻塞 + 可重复读报表 + 并发扣减 |
| 22 | MySQL 8.0 Undo 改进？| Undo 表空间可独立配置/自动截断（innodb_undo_log_truncate）|
| 23 | Serializable 级别？| 所有 SELECT 隐式加共享锁，退化为当前读，无并发 |
| 24 | 面试陷阱？| 说 MVCC 只靠 Undo Log → ❌ 必须隐藏字段+Undo+ReadView 三者配合 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：storage/innobase/include/read0types.h + trx/trx0trx.cc + row/row0sel.cc  
**面试考点：24 题**  
**差异化点**：隐藏列+版本链 + ReadView 四规则源码 + RC/RR 时机对比 + 防幻读双机制
