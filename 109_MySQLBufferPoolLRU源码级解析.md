# MySQL InnoDB Buffer Pool + LRU 源码级解析

> **源码基线**：MySQL 8.0.x（C++）  
> **核心仓库**：mysql/mysql-server → storage/innobase/buf/  
> **面试权重**：⭐⭐⭐⭐⭐（MySQL 内存管理核心，高频必问）

---

## 一、Buffer Pool 核心地位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB Buffer Pool = MySQL 内存管理核心                    │
│                                                               │
│  为什么 Buffer Pool 重要？                                    │
│  磁盘 I/O 是 MySQL 最大性能瓶颈（磁盘 ~100x 比内存慢）       │
│  → Buffer Pool 把常用数据缓存在内存 → 减少磁盘 I/O           │
│  → Buffer Pool 大小直接决定 MySQL 性能（buffer_pool_size    │
│    通常设为物理内存的 50%-70%）                               │
│
│ ★ Buffer Pool 组成（面试高频！）：                             │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Buffer Pool（按 Instance 划分）                        │   │
│  │  ┌─────────────────────────────────────────────────┐ │   │
│  │  │ Buffer Pool Instance 1（1GB 默认）              │ │   │
│  │  │  ┌──────────┬──────────┬──────────┐             │ │   │
│  │  │  │ Free List │ LRU List │ FLUSH List │             │ │   │
│  │  │  │ 空闲页链表 │ 最近访问  │ 脏页链表   │             │ │   │
│  │  │  └──────────┴──────────┴──────────┘             │ │   │
│  │  └─────────────────────────────────────────────────┘ │   │
│  │  ┌─────────────────────────────────────────────────┐ │   │
│  │  │ Buffer Pool Instance 2（1GB）                     │ │   │
│  │  │  ...（同样结构）                                   │ │   │
│  │  └─────────────────────────────────────────────────┘ │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│ ★ 关键数据结构：                                              │
│   Page（页）= Buffer Pool 最小单位（16KB）                    │
│   Buffer Frame（帧）= 一个 Page + 元数据                       │
│   Free List：所有空闲 Buffer Frame（链表）                     │
│   LRU List：按最近访问时间排序（链表）                         │
│   FLUSH List：脏页（已修改，需刷盘）                          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Buffer Pool Instance 多实例设计

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Buffer Pool Instance 多实例设计（源码级）                  │
│                                                               │
│  MySQL 5.7+ 引入 Buffer Pool Instance（减少锁竞争）           │
│                                                               │
│  // storage/innobase/buf/buf0buf.cc                         │
│  srv_buffer_pool_size = 8GB                                  │
│  srv_buf_pool_instances = 8  // 默认 8 个实例                │
│  // → 每个 Instance 1GB                                     │
│
│  为什么多实例？                                                │
│  单 Instance：所有线程竞争一个 LRU List 锁 → 锁瓶颈          │
│  多 Instance：按 Page Hash 分散到不同 Instance → 锁粒度小    │
│  // buf_pool_t* buf_pool_get_from_page_id(page_id) {         │
│  //     ulint n = page_id % srv_buf_pool_instances;          │
│  //     return &buf_pool_arr[n];                             │
│  // }                                                        │
│
│ ★ 每个 Instance 内部结构（源码级）：                          │
│  typedef struct buf_pool_t {                                 │
│      ut_list_t free_list;        // LRU 空闲页                │
│      ut_list_t LRU;              // LRU List（访问排序）      │
│      ut_list_t flush_list;       // 脏页 List                │
│      // ...                                                  │
│      hash_table_t hash_index;    // page_id → Buffer Frame  │
│  } buf_pool_t;                                               │
│
│  // 每个 Instance 有自己的 mutex（锁粒度：单 Instance）      │
│  pthread_mutex_t LRU_list_mutex;                             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、LRU 37/100 冷热分离（★★★★★ 面试必背）

### 3.1 LRU 链表结构（源码级）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB LRU 37/100 冷热分离（源码级，面试必背！）           │
│                                                               │
│  普通 LRU 问题：                                              │
│  全表扫描 → 所有页被当作"新"放进 LRU 头部                     │
│  → 挤掉真正的热点页 → 热点数据命中率下降 → 性能抖动           │
│
│ ★ InnoDB 解决方案：LRU 分 old + new 两段                      │
│                                                               │
│  // storage/innobase/buf/buf0lru.cc                          │
│                                                               │
│  LRU List 总长 100%                                          │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ old 区（37%）           │ new 区（63%）               │   │
│  │ ← 冷数据（可能一次访问）│ → 热数据（高频访问）        │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│ ★ 关键阈值（源码级）：                                        │
│  #define BUF_LRU_OLD_SEGMENT_SIZE_RATIO 37                  │
│  // LRU List 中前 37% 是 old 区，后 63% 是 new 区           │
│
│ ★ Page 进入 LRU 的流程（源码级）：                             │
│   ① Buffer Pool 没空闲页 → LRU 淘汰一个页                    │
│   ② 从磁盘读新页 → **插入到 old 区尾部**（不是头部！）        │
│   ③ 如果这个页被再次访问 → 上移到 new 区头部                  │
│   ④ 如果这个页在 old 区待了很久没访问 → 从 old 区淘汰          │
│
│ ★ 这就防住了全表扫描！（面试核心）                             │
│   全表扫描读到的页 → 进入 old 区尾部                         │
│   → 不会挤掉 new 区的真正热点数据                             │
│   → old 区被这些冷数据填满后 → 冷数据自己淘汰自己             │
│   → new 区热点数据不受影响 → 命中率保持稳定                   │
│
```

### 3.2 LRU 淘汰核心逻辑

```c
// ★ LRU 淘汰核心逻辑（storage/innobase/buf/buf0lru.cc）
//
// Page 访问时：
// buf_page_t* buf_LRU_page_access(buf_page_t* page) {
//     // ★ 如果在 old 区（冷数据）：
//     if (page->in_old_segment) {
//         // ★ 上移到 new 区头部（变热了）
//         ut_list_remove(page, &buf_pool->LRU);
//         ut_list_insert_first(page, &buf_pool->LRU);
//         page->in_old_segment = FALSE;
//         return page;
//     }
//     // ★ 如果在 new 区（热数据）：
//     // 什么都不做（已经在热区了）
//     return page;
// }
//
// Buffer Pool 需要空闲页时：
// buf_page_t* buf_LRU_free_block(buf_pool_t* buf_pool) {
//     // ★ 优先从 old 区淘汰（冷数据优先淘汰）
//     buf_page_t* victim = ut_list_get_last(&buf_pool->LRU.old);
//     if (victim == NULL) {
//         // old 区空了 → 从 new 区淘汰（最后手段）
//         victim = ut_list_get_last(&buf_pool->LRU);
//     }
//
//     // ★ 如果是脏页 → 先刷盘（不能直接丢）
//     if (victim->is_dirty) {
//         buf_do_flush_list_batch(buf_pool, victim);
//     }
//
//     // ★ 释放 Page → 放回 Free List
//     ut_list_remove(victim, &buf_pool->LRU);
//     ut_list_insert_first(victim, &buf_pool->free_list);
//
//     return victim;
// }
//
// ★ 面试高频：37/100 为什么选 37？
//   经验值：
//   - 太小（比如 10%）：old 区太小 → 全表扫描还是会挤到 new 区
//   - 太大（比如 50%）：new 区太小 → 热点数据也被挤
//   - 37%：平衡 old（防扫描）和 new（保热点）→ 实测效果最好
//
```

---

## 四、脏页刷新（FLUSH List）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ InnoDB 脏页刷新策略（源码级）                              │
│                                                               │
│  脏页来源：Buffer Pool 中被修改过但还没刷盘的 Page             │
│  → 放在 FLUSH List（双链表）                                 │
│
│ ★ 脏页刷新三个时机（面试高频！）：                             │
│                                                               │
│  ① Redo Log 空间不足（最紧急！）                              │
│     innodb_log_files_in_group × innodb_log_file_size ≈ 48MB  │
│     Redo Log 写满了 → 不能再写了 → **强制刷脏页**            │
│     → 这是最紧急的刷新（否则 MySQL 停写！）                  │
│
│  ② Buffer Pool 空闲页不足                                   │
│     LRU 淘汰时发现是脏页 → 先刷盘再淘汰                     │
│     → 这是被动刷新（按需）                                    │
│
│  ③后台线程周期性刷新（后台任务）                              │
│     page_cleaner 线程每秒检查一次                             │
│     → 如果脏页比例 > innodb_max_dirty_pages_pct（默认 90%） │
│     → 批量刷新（innodb_io_capacity 控制刷盘速度）           │
│
│ ★ page_cleaner 线程（源码级）：                               │
│   srv_buf_pool_io_capacity = 200  // 默认 IO 容量            │
│   // page_cleaner 每秒最多刷 200 个脏页                      │
│
│   void buf_flush_page_cleaner() {                             │
│       // 每秒检查一次                                         │
│       for each buf_pool {                                    │
│           for (int i = 0; i < srv_buf_pool_io_capacity; i++) { │
│               buf_page_t* dirty = ut_list_pop_first(flush_list); │
│               if (dirty == NULL) break;                      │
│               buf_flush_do_page(dirty);  // 刷盘              │
│           }                                                  │
│       }                                                      │
│       os_aio_wait();  // 等 IO 完成                          │
│   }                                                          │
│
│ ★ Redo Log 空间不足时的紧急刷新（最严重）：                   │
│   Redo Log 剩余空间 < 10% → 日志写入阻塞                    │
│   → 强制刷脏页直到 Redo Log 空间恢复                         │
│   → 此时 MySQL 性能急剧下降（写入几乎停了）                  │
│   → 解决：增大 innodb_log_file_size（默认 48MB → 建议 512MB）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Buffer Pool 组成？| **Page（16KB）+ Free List + LRU List + FLUSH List** |
| 2 | Buffer Pool Instance？| **多实例按 page_id % instances 分散**→ 减少锁竞争；默认 8 个实例 |
| 3 | LRU 37/100 冷热带？| **old 37%（冷数据区）+ new 63%（热数据区）**→ 防全表扫描挤掉热点 |
| 4 | LRU 37/100 为什么选 37%？| 经验值：平衡 old 防扫描 + new 保热点；太小→扫描还是挤 new；太大→热点也被挤 |
| 5 | 新 Page 进入 LRU？| **插入 old 区尾部（不是头部）**→ 防止一次性访问数据挤掉热点 |
| 6 | Page 变热？| old 区访问 → 上移到 new 区头部 |
| 7 | LRU 淘汰优先级？| **优先 old 区（冷数据）**→ old 空了才淘汰 new 区 |
| 8 | 脏页刷新三时机？| **①Redo Log 不足（最紧急）②LRU 淘汰脏页（被动）③page_cleaner 后台（定期）** |
| 9 | page_cleaner 线程？| 每秒检查；受 innodb_io_capacity（默认 200）控制刷盘速度 |
| 10 | Redo Log 不足影响？| **写入阻塞（MySQL 停写风险）**→ 增大 innodb_log_file_size（512MB 建议）|
| 11 | Buffer Pool 源码路径？| **storage/innobase/buf/buf0buf.cc + buf0lru.cc** |
| 12 | innodb_buffer_pool_size 建议？| **物理内存 50%-70%**→ 是 MySQL 性能最关键参数 |
| 13 | LRU 源码关键函数？| buf_LRU_page_access()（访问处理）+ buf_LRU_free_block()（淘汰）|
| 14 | 全表扫描如何防住？| 冷数据进 old 区 → 不会挤掉 new 区热点 → 冷数据自己淘汰自己 |
| 15 | Buffer Pool 命中率？| **>99% 正常**；<90% → 增大 innodb_buffer_pool_size |
| 16 | Buffer Pool 学习路径？| buf0buf.cc → buf0lru.cc → page_cleaner 线程 → FLUSH List |
| 17 | 和 Redis 对比？| Redis：纯内存 KV；InnoDB Buffer Pool：磁盘 Page 缓存；两者都是 LRU 思想但 InnoDB 有 37/100 冷热带 |
| 18 | 面试最高频考点排序？| **1. LRU 37/100 防全表扫描 2. 脏页刷新三时机 3. Buffer Pool 组成 4. 多实例锁竞争** |
| 19 | Buffer Pool vs Linux Page Cache？| **Buffer Pool 自己管理 LRU + 脏页**→ 绕过 Linux Page Cache（innodb_flush_method=O_DIRECT）→ 双重缓存无意义 |
| 20 | O_DIRECT 作用？| MySQL 直接刷盘（绕过 Linux Page Cache）→ 减少 Page Cache 和 Buffer Pool 双重缓存冲突 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：storage/innobase/buf/buf0buf.cc + buf0lru.cc  
**面试考点：20 题**  
**差异化点**：LRU 37/100 冷热分离（面试必背）+ 脏页刷新三时机 + 源码级函数
