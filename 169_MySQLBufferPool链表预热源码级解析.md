# MySQL Buffer Pool 预热 + Free/Flush/LRU 链表源码级深度解析

> **源码基线**：MySQL 8.0 / InnoDB（C++）  
> **核心位置**：storage/innobase/buf/buf0buf.cc + buf0lru.cc + buf0flu.cc  
> **面试权重**：⭐⭐⭐⭐⭐（InnoDB 内存管理核心，配合 LRU 37/63 必问）

---

## 一、Buffer Pool 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Buffer Pool = InnoDB 内存缓存区（磁盘页的镜像）          │
│                                                               │
│  缓存对象：                                                    │
│   ① 数据页（16KB，聚簇/二级索引页）                          │
│   ② Undo 页                                                   │
│   ③ 自适应哈希索引（AHI）                                     │
│   ④ Change Buffer（写缓冲，占 BP 一部分）                    │
│   ⑤ 锁信息、各种系统页                                        │
│                                                               │
│  所有页通过 buf_page_t / buf_block_t 描述                    │
│  用三个核心链表管理（面试必背！）：                           │
│   ★ LRU List（最近最少使用，管理缓存淘汰）                   │
│   ★ Free List（空闲页，刚启动/从未使用）                    │
│   ★ Flush List（脏页，等待刷盘）                            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、三大链表源码级

### 2.1 Free List（空闲链表）

```cpp
// ★ Free List 核心（源码级）
//
// storage/innobase/include/buf0buf.h
//
// struct buf_pool_t {
//     // ★ 空闲页链表：从没被用过的干净缓存块
//     UT_LIST_BASE_NODE_T(buf_page_t) free;
//
//     // ★ LRU 链表：所有正在被缓存的页
//     UT_LIST_BASE_NODE_T(buf_page_t) LRU;
//
//     // ★ 脏页链表：被修改过但还没刷盘的页
//     UT_LIST_BASE_NODE_T(buf_page_t) flush_list;
//
//     // 哈希表：space_id+page_no → buf_block（快速定位页是否在 BP）
//     buf_page_hash_t* page_hash;
// };
//
// ★ Free List 工作机制：
//  ① 数据库刚启动：所有 BP 内存块都在 free list
//  ② 需要读一个不在 BP 的页：
//     - free list 非空 → 从 free 取一个块
//     - free list 空 → 从 LRU 尾部淘汰一个干净页
//  ③ 页读入后挂到 LRU list
//
```

### 2.2 LRU List（冷热分离 5/8 + 3/8）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB LRU 改进版（面试必背 37/63 规则）                 │
│                                                               │
│  LRU List 分两段（midpoint insertion）：                    │
│  ┌──────────────────────┬──────────────────────────┐       │
│  │ Young（热数据，new）  │ Old（冷数据，old）          │       │
│  │ 5/8 ≈ 63%（头部）    │ 3/8 ≈ 37%（尾部）          │       │
│  └──────────────────────┴──────────────────────────┘       │
│                          ↑ midpoint（LRU_old 指针）          │
│                                                               │
│  ★ 新读入的页插入 midpoint（Old 区头部），不是 LRU 头部！  │
│                                                               │
│  为什么这样设计（面试高频）：                                 │
│   防全表扫描/预读污染热数据：                                 │
│   全表扫描一次性读入大量页，全插到 Old 头部                  │
│   真正的热点页在 Young 区不受影响                           │
│   扫描页如果只用一次 → 在 Old 区老化淘汰，不进 Young        │
│                                                               │
│  晋升规则：                                                    │
│   Old 区的页再次被访问，且停留时间 > innodb_old_blocks_time  │
│   （默认 1000ms）→ 移动到 Young 头部                        │
│   停留 < 1s 的二次访问不晋升（防止预读页瞬间污染 Young）    │
│
└─────────────────────────────────────────────────────────────┘
```

```cpp
// ★ LRU 淘汰与晋升（源码 buf0lru.cc）
//
// // 找一个可淘汰的 victim（buffer pool 满时）
// buf_page_t* buf_LRU_get_free_block(buf_pool_t* buf_pool) {
//     // ① 先从 LRU 尾部（Old 区尾部）找【干净页】
//     //    干净页直接覆盖（不用刷盘）
//     while (...) {
//         bpage = UT_LIST_GET_LAST(buf_pool->LRU);
//         if (!bpage->oldest_modification) {
//             // oldest_modification==0 表示不是脏页
//             buf_LRU_free_page(bpage);
//             return bpage;
//         }
//         // 脏页不能直接淘汰，跳过（让 flush 线程去刷）
//     }
//
//     // ② 单页刷新兜底：
//     //    如果尾部都是脏页，强制先刷一个脏页（用户线程也可能帮忙刷）
//     buf_flush_single_page_from_LRU(buf_pool);
// }
//
// // ★ 访问页时晋升（buf_page_make_young）
// void buf_page_make_young(buf_page_t* bpage) {
//     // 从 Old 移到 Young 头部（受 old_blocks_time 控制）
//     UT_LIST_REMOVE(LRU, bpage);
//     UT_LIST_ADD_FIRST(LRU, bpage);
// }
//
```

### 2.3 Flush List（脏页链表）

```cpp
// ★ Flush List 核心（源码 buf0flu.cc）
//
// 脏页 = BP 中被修改但还没写回磁盘的页
// 页第一次被修改（变脏）→ 加入 flush list（按 oldest_modification 排序）
//
// // oldest_modification（最早被修改的 LSN）
// // newest_modification（最近被修改的 LSN）
// // flush list 按 oldest_modification 升序（最老的脏页在头部）
//
// ★ 脏页同时存在于 LRU list 和 flush list（两个链表不互斥！）
//   LRU 管"淘汰"；Flush 管"刷盘"
//   刷盘后页变干净 → 从 flush list 移除，但仍在 LRU list
//
// ★ 刷脏页三大时机（面试必背）：
//  ① redo log 写满（write pos 追上 checkpoint）
//     → 必须强制刷最老的脏页推进 checkpoint（紧急！）
//  ② 后台线程定时刷（page_cleaner 线程，平滑刷）
//  ③ LRU 淘汰时尾部是脏页 → 单页刷（用户线程帮忙，影响延迟）
//  ④ 正常 shutdown / FLUSH TABLES
//
```

### 2.4 三个链表的关系（一张图收尾）

```
┌─────────────────────────────────────────────────────────────┐
│  新页读入：                                                   │
│    Free List ──取块──→ 挂到 LRU Old 头部                    │
│                                                               │
│  页被修改：                                                   │
│    在 LRU 中 + 加入 Flush List（按 oldest LSN）             │
│                                                               │
│  页被频繁访问：                                               │
│    Old ──晋升（>1s 再访问）──→ Young 头部                   │
│                                                               │
│  脏页刷盘：                                                   │
│    Flush List 头部（最老）→ 写磁盘 → 移出 Flush，留在 LRU  │
│                                                               │
│  BP 满需新块：                                               │
│    LRU 尾部找干净页淘汰；尾部脏页先刷后淘汰                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Buffer Pool 预热（Warmup）

### 3.1 为什么需要预热

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 冷启动问题（面试高频）                                    │
│                                                               │
│  MySQL 刚重启 / BP 刚扩容：                                   │
│   BP 是空的 → 所有查询都走磁盘（冷查询）                     │
│   → 高并发业务瞬间被慢查询打垮、磁盘 IO 飙升                │
│   → 预热期可能持续几十分钟到几小时                           │
│                                                               │
│  解决方案：                                                    │
│   ① 自动预热（Buffer Pool Preload，MySQL 5.6+）              │
│   ② 手动预热（全表/索引扫描）                                │
│   ③ 多 BP 实例并发预热                                       │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 自动预热：Dump + Load

```cpp
// ★ Buffer Pool 预热机制（源码级，面试核心）
//
// 通过两个参数控制：
//
// innodb_buffer_pool_dump_at_shutdown = ON
//   MySQL 正常关闭时：把 BP 中【页的引用信息】dump 到磁盘
//   （注意：只 dump 页的 space_id+page_no，不 dump 页数据，文件很小）
//   文件：ib_buffer_pool
//
// innodb_buffer_pool_load_at_startup = ON
//   MySQL 启动时：读 ib_buffer_pool，把这些页异步预读进 BP
//   → BP 恢复到关闭前的热页状态，预热自动完成
//
// // buf0dump.cc
// // Dump 过程：
// //   扫描 LRU list，记录每个页的 (space, page_no) 到文件
// //   可配置 dump 比例：innodb_buffer_pool_dump_pct（默认 25%）
// //   只 dump Young 区/最热部分（不是全量，节省时间）
//
// // Load 过程：
// //   后台预读线程批量发起 async read-ahead
// //   ibuf_read_ahead_random / linear 把页拉入 BP
// //   不阻塞启动（warmup 在后台进行）
//
// ★ 运行时手动操作：
//   SET GLOBAL innodb_buffer_pool_dump_now = ON;   // 立即 dump
//   SET GLOBAL innodb_buffer_pool_load_now = ON;   // 立即 load
//   SET GLOBAL innodb_buffer_pool_load_abort = ON; // 中止预热
//
// // 查看进度：
//   SHOW STATUS LIKE 'Innodb_buffer_pool_dump_status';
//   SHOW STATUS LIKE 'Innodb_buffer_pool_load_status';
//
```

### 3.3 手动预热与命中率监控

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 手动预热 SQL（面试实战）                                  │
│                                                               │
│  -- 把热表/索引读一遍（走 LRU Old 区，配合预热目的）        │
│  SELECT COUNT(*) FROM hot_table;                            │
│  -- 或显式扫索引（成本更低）                                 │
│  SELECT COUNT(id) FROM hot_table FORCE INDEX(PRIMARY);      │
│                                                               │
│  ★ 命中率监控（面试必背公式）：                               │
│   Innodb_buffer_pool_read_requests：逻辑读总次数（BP 查找） │
│   Innodb_buffer_pool_reads：物理读次数（BP miss 走磁盘）    │
│                                                               │
│   命中率 = (read_requests - reads) / read_requests × 100%   │
│   生产目标：99%+                                              │
│   命中率低 → BP 太小 / 冷启动 / 全表扫描污染                │
│                                                               │
│  其他状态：                                                    │
│   Innodb_buffer_pool_pages_total：总页数                     │
│   pages_dirty：脏页数                                        │
│   pages_free / pages_data：空闲/数据页数                    │
│   Innodb_buffer_pool_wait_free：等空闲块次数（应接近 0）    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、多 Buffer Pool 实例

```
┌─────────────────────────────────────────────────────────────┐
│  ★ innodb_buffer_pool_instances（面试高频）                 │
│                                                               │
│  BP 大于 1GB 时建议拆成多个实例（默认 8，8.0 自适应）       │
│  每个实例：独立的 free/LRU/flush list + 独立锁              │
│  页按 hash(space_id, page_no) 分配到固定实例                │
│                                                               │
│  好处：                                                        │
│   减少多线程访问 LRU/flush list 的锁竞争                    │
│   提升高并发吞吐（每个实例独立刷盘）                         │
│                                                               │
│  8.0 动态调整 BP 大小（online resize）：                     │
│   SET GLOBAL innodb_buffer_pool_size = ...（不用重启）      │
│   内部通过后台线程逐步迁移页                                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、刷盘策略 innodb_max_dirty_pages_pct

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 脏页比例控制（面试高频）                                  │
│                                                               │
│  innodb_max_dirty_pages_pct（默认 75，8.0 实际自适应更低）  │
│   脏页占比达到阈值 → page_cleaner 加快刷盘                  │
│                                                               │
│  innodb_io_capacity：普通 SSD 2000，NVMe 可设 10000+        │
│   告诉 InnoDB 磁盘 IO 能力，控制刷盘速度                     │
│  innodb_io_capacity_max：紧急刷盘（redo 满）时的上限       │
│                                                               │
│  8.0 自适应刷盘：根据 redo 推进速度动态调节                  │
│  flush 邻近页：刷一个脏页时顺带把相邻脏页一起刷（顺序 IO）  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Buffer Pool 三大链表？| **Free（空闲）+ LRU（缓存淘汰）+ Flush（脏页）** |
| 2 | Free List 作用？| 管理从未使用的空闲块；读页从这里取，空了去 LRU 尾部淘汰 |
| 3 | LRU 冷热比例？| **Young 5/8（63%）+ Old 3/8（37%）** |
| 4 | 新页插到哪？| **midpoint（Old 区头部），不是 LRU 头部** |
| 5 | 为什么不用朴素 LRU？| 全表扫描/预读会一次性冲掉热数据（缓冲池污染）|
| 6 | 页怎么晋升 Young？| Old 区页再次访问且停留 > innodb_old_blocks_time（默认 1000ms）|
| 7 | 为什么要有 1s 停留阈值？| 防止预读页/扫描页被瞬间访问两次就晋升污染 Young |
| 8 | Flush List 排序？| 按 **oldest_modification（最早脏 LSN）** 升序，最老脏页先刷 |
| 9 | 脏页在几个链表？| 同时在 LRU + Flush；刷盘后只移出 Flush，仍在 LRU |
| 10 | 刷脏页四时机？| **redo 满强制刷 + page_cleaner 后台刷 + LRU 淘汰单页刷 + shutdown** |
| 11 | LRU 尾部遇到脏页？| 不能直接淘汰，先单页刷盘再覆盖（用户线程也可能帮忙刷）|
| 12 | BP 预热方案？| **dump_at_shutdown + load_at_startup（ib_buffer_pool）** |
| 13 | dump 的是数据吗？| 不是，只存 space_id+page_no 引用（文件很小）|
| 14 | dump_pct？| innodb_buffer_pool_dump_pct 默认 25%，只 dump 最热部分 |
| 15 | 运行时预热命令？| innodb_buffer_pool_dump_now / load_now / load_abort |
| 16 | 命中率公式？| (read_requests - reads)/read_requests；目标 99%+ |
| 17 | 命中率低原因？| BP 太小 / 冷启动 / 全表扫描污染 |
| 18 | wait_free 高？| BP 频繁等空闲块（脏页刷不过来），BP 不足或 IO 瓶颈 |
| 19 | 多 BP 实例？| 大内存拆多实例，独立链表和锁，减少锁竞争 |
| 20 | 8.0 动态扩 BP？| SET GLOBAL 在线 resize，后台迁移页 |
| 21 | 脏页比例阈值？| innodb_max_dirty_pages_pct 默认 75（8.0 自适应）|
| 22 | io_capacity？| 告诉 InnoDB 磁盘能力（SSD 2000，NVMe 10000+），控制刷盘速度 |
| 23 | 面试最高频考点排序？| **1. 37/63 冷热 2. 三链表关系 3. 刷脏时机 4. 预热 dump/load 5. 命中率** |
| 24 | 学习路径？| buf0buf 三链表 → buf0lru midpoint 插入 → buf0flu 刷脏 → buf0dump 预热 |
| 25 | 面试常见追问？| Change Buffer 占 BP 吗？→ 是，默认最多 25%；AHI 呢？→ 也在 BP |
| 26 | 面试陷阱？| 说脏页刷盘后就从 BP 删除 → ❌ 刷盘只移出 Flush List，页还在 LRU 缓存 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：storage/innobase/buf/buf0buf.cc + buf0lru.cc + buf0flu.cc + buf0dump.cc  
**面试考点：26 题**  
**差异化点**：三链表协作关系 + midpoint 37/63 防污染 + 晋升 1s 阈值 + dump/load 预热 + 命中率公式
