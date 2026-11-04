# Redis Sorted Set SkipList + Dict 哈希冲突源码级深度解析

> **源码基线**：Redis 7.x/8.0（C）  
> **核心位置**：src/server.h + src/t_zset.c + src/dict.c  
> **面试权重**：⭐⭐⭐⭐⭐（Redis 底层数据结构高频，补全 Cluster 178 + 持久化 187）

---

## 一、Redis Sorted Set 底层结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis Sorted Set = SkipList + Dict（双索引）              │
│                                                               │
│  核心设计（面试必背）：                                        │
│  ┌───────────────────────────────────────────────────┐       │
│  │ SortedSet（ZSET）底层：                             │       │
│  │                                                     │       │
│  │   dict *dict      ← Member → Score 快速查找        │       │
│  │   zskiplist *zsl  ← 按 Score 排序的跳跃表          │       │
│  │                                                     │       │
│  │   ★ 为什么双结构？                                  │       │
│  │   ZSCORE(member) → O(1)（Dict 查）                 │       │
│  │   ZRANGEBYSCORE  → O(logN)（SkipList 查）          │       │
│  │   ZRANK(member)  → O(logN)（SkipList 查排名）      │       │
│  │   // 两个结构各有优势，互为补充                      │       │
│  └───────────────────────────────────────────────────┘       │
│                                                               │
│  ★ Redis 各数据结构底层：                                      │
│   String → SDS（Simple Dynamic String）                      │
│   List   → quicklist（3.2+，双向链表+压缩列表）               │
│   Hash   → dict（小 Hash 用 ziplist）                         │
│   Set    → dict（小 Set 用 intset）                           │
│   ZSet   → **dict + zskiplist（小 ZSet 用 ziplist）**         │
│   Stream → rax（基数树）+ listpack                             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、SkipList 跳跃表源码级

### 2.1 SkipList 结构

```c
// ★ Redis SkipList 源码级（面试核心！）
//
// src/server.h → zskiplistNode + zskiplist
// src/t_zset.c
//
// // SkipList 节点结构（源码级）：
// typedef struct zskiplistNode {
//     sds ele;                        // Member 值（字符串）
//     double score;                   // 排序分数
//     struct zskiplistNode *backward; // 后退指针（Level 0 双向链表）
//     struct zskiplistLevel {
//         struct zskiplistNode *forward;  // 前进指针
//         unsigned long span;              // 跨度（当前节点到下一个节点之间有多少个 Level 0 节点）
//     } level[];                     // 层数组（柔性数组）
// } zskiplistNode;
//
// // SkipList 结构：
// typedef struct zskiplist {
//     struct zskiplistNode *header, *tail;  // 头尾
//     unsigned long length;                 // 节点数
//     int level;                            // 最大层数
// } zskiplist;
//
// // ★ SkipList 层设计（面试高频）：
// //   每个节点有 level[] 数组 → 多层前进指针
// //   header 有最大层数（64 层）
// //   新节点层数随机（概率平衡）：
// //   // 50% 概率 1 层，25% 概率 2 层...
// //   // 平均层数 = 1/(1-1/4) ≈ 1.33 层
// //   // 时间复杂度 O(logN)，空间复杂度 O(N)
// //
// // ★ SkipList 和红黑树对比（面试追问）：
// //   SkipList：实现简单 + 查询 O(logN) + 范围查询天然有序
// //   红黑树：严格平衡 + 查询 O(logN) + 实现复杂
// //   → Redis 选 SkipList 是因为**范围查询高效 + 实现简单**
//
```

### 2.2 SkipList 查询 + 插入源码级

```c
// ★ SkipList 插入 + 查询源码级（面试核心）
//
// // ZRANK member → 查排名（源码级）：
// //
// unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
//     unsigned long rank = 0;
//     zskiplistNode *x = zsl->header;
//
//     // 从最高层往下走
//     for (int i = zsl->level - 1; i >= 0; i--) {
//         // 往下一层之前，累加 span
//         while (x->level[i].forward &&
//                (x->level[i].forward->score < score ||
//                 (x->level[i].forward->score == score &&
//                  sdscmp(x->level[i].forward->ele, ele) <= 0))) {
//             rank += x->level[i].span;  // 累加跨度
//             x = x->level[i].forward;  // 前进
//         }
//     }
//     return rank;  // 找到节点，返回累计 span = 排名
// }
// // 时间复杂度：O(logN) → 高层快速跳过大量节点
// // span 累加 = 精确排名！
//
// // ZADD member score → 插入（源码级）：
// //
// // 1. 从最高层往下找插入位置（类似 GetRank 逻辑）
// // 2. 每层记录前驱节点（update[] 数组）
// // 3. 随机生成新节点层数（概率平衡算法）
// // 4. 创建新节点 → 修改每层指针 + 更新 span
// // 5. 更新 backward 指针（Level 0 双向链表）
// //
// // ★ span 维护：
// //   新节点插入后 → 所有层前驱的 span 要重新计算
// //   span = 前驱 span - (当前层前驱和新节点 span 差) + 1
// //   // span 精确维护是 ZRANK O(logN) 的关键！
//
```

---

## 三、Redis Dict 哈希冲突源码级

### 3.1 Dict 结构 + 渐进式 Rehash

```c
// ★ Redis Dict 哈希表源码级（面试核心！高频！）
//
// src/dict.h + src/dict.c
//
// // Dict 结构（源码级）：
// typedef struct dict {
//     dictType *type;         // 操作类型
//     void *privdata;         // 私有数据
//     dictht ht[2];           // ★ 两个哈希表（渐进式 Rehash）
//     long rehashidx;         // 当前 Rehash 到哪个桶；-1 表示没在 Rehash
//     unsigned long iterators;// 运行中的迭代器数量；不为 0 时暂停 Rehash
// } dict;
//
// typedef struct dictht {
//     dictEntry **table;      // 哈希桶数组
//     unsigned long size;     // 桶数量（必须是 2 的幂）
//     unsigned long sizemask; // size-1（用 & 代替 % 快速取模）
//     unsigned long used;     // 已用 entry 数
// } dictht;
//
// // ★ 渐进式 Rehash 核心（面试高频！）：
// //
// // 为什么渐进？
// //   普通 Rehash：一次性把 ht[0] 搬到 ht[1]
// //   如果有几百万 Key → 阻塞 Redis 很久
// //   渐进式：每次操作（增删改查）顺便搬 1 个桶
// //   → 把大任务拆成小碎片 → 不阻塞
// //
// // ★ Rehash 触发条件：
// //   Load Factor = ht[0].used / ht[0].size
// //   负载因子 > 1（正常） → 扩容（size 翻倍）
// //   负载因子 < 0.1（内存紧张）→ 缩容
// //   // Rehash 时 ht[1].size = ht[0].size * 2
// //
// // ★ Rehash 期间查询：
// //   查 ht[0] 和 ht[1]（可能 Key 在任一个）
// //   插入只往 ht[1]（ht[0] 不再新增）
// //   删除可能在 ht[0] 或 ht[1]
// //   // 等 ht[0] 全部搬完 → ht[1] 变成新 ht[0] → ht[1] 清空
//
```

### 3.2 哈希冲突解决

```c
// ★ Redis 哈希冲突解决（面试高频）
//
// // 1. Hash Function：
// //    Redis 用 MurmurHash / SipHash（字符串）
// //    uint64_t dictHashFunction(const void *key)
// //
// // 2. 冲突解决：★ 链地址法（拉链法）
// //    每个桶是一个链表（dictEntry 链）
// //    hash = MurmurHash(key) & sizemask → 桶号
// //    同桶的 Entry 用 next 指针串起来
// //    // 不是开放寻址法！开放寻址法删除复杂
// //
// // 3. 桶数组 size 必须是 2 的幂：
// //    // size = 2^n → sizemask = size - 1
// //    // hash & sizemask 等价于 hash % size
// //    // 但位运算比取模快得多！
// //    // 面试高频：为什么 size 必须 2 的幂？答：用 & 代替 %，更快
// //
// // ★ Dict 扩容 vs 缩容：
// //   扩容：used/size > 1 → size × 2（保证 Load Factor ≈ 1）
// //   缩容：used/size < 0.1 → size / 2（内存紧张时）
// //   // 默认只扩容不缩容（Redis 配置 dict-resize-enabled）
// //   // 7.0+ 自动缩容
//
```

---

## 四、Redis Sorted Set vs 其他有序结构对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：SkipList vs 红黑树 vs 有序数组                   │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ Redis SkipList  │ 红黑树          │ 有序数组 ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 查询            │ O(logN)          │ O(logN)        │ O(logN) ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 插入            │ O(logN)          │ O(logN)        │ O(N)    ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 删除            │ O(logN)          │ O(logN)        │ O(N)    ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 范围查询         │ ★ O(logN+k)      │ O(logN+k)      │ O(logN+k)││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 实现复杂度       │ ★ 低（约 150 行）  │ 高（约 1000+行）│ 最低     ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 内存             │ O(N)（多层指针）  │ O(N)（每个节点 3 指针）│ O(N)连续││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ Span 排名        │ ★ 原生支持       │ 不支持          │ 不支持   ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ Redis 选 SkipList 原因（面试追问）：                       │
│   1. 实现简单（比红黑树少 5-10 倍代码）                      │
│   2. Span 原生支持排名（ZRANK O(logN)）                      │
│   3. 范围查询高效（O(logN+k)）                               │
│   4. 插入/删除不需要旋转（红黑树要）                          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ★ Sorted Set 底层？| **dict（Member→Score 快速查）+ zskiplist（按 Score 排序+范围）** |
| 2 | SkipList 层数组设计？| header 有 64 层；新节点层数随机（概率平衡：50% 1层、25% 2层...）；平均 1.33 层 |
| 3 | ★ SkipList span 作用？| **精确排名**：每层 span 累加 = ZRANK 的 rank 值 |
| 4 | SkipList 查询/插入复杂度？| **O(logN)**；高层快速跳过大量节点 |
| 5 | SkipList vs 红黑树 Redis 为什么选 SkipList？| **实现简单 + Span 排名原生支持 + 范围查询高效 + 插入/删除无需旋转** |
| 6 | ★ Dict 渐进式 Rehash 核心？| 两个哈希表 ht[0]/ht[1]；每次操作顺便搬 1 个桶；不阻塞 Redis |
| 7 | 渐进式 Rehash 触发条件？| 负载因子 > 1 扩容（size×2）；负载因子 < 0.1 缩容 |
| 8 | Rehash 期间查询/插入/删除？| 查询查 ht[0]+ht[1]；插入只往 ht[1]；删除任一个 |
| 9 | ★ 哈希冲突解决？| **链地址法（拉链法）**；同桶 Entry 链表串联 |
| 10 | ★ Dict size 必须 2 的幂为什么？| sizemask=size-1；hash & sizemask 等价 hash % size；**位运算比取模快得多** |
| 11 | MurmurHash / SipHash 是什么？| Redis 用的哈希函数；MurmurHash3（高性能）；SipHash（防哈希碰撞攻击）|
| 12 | 面试最高频考点排序？| **1. Sorted Set = dict+skiplist 双结构 2. SkipList span 排名 3. 渐进式 Rehash 4. size 2 的幂用 & 5. 链地址法** |
| 13 | 学习路径？| Sorted Set → SkipList → span → Dict → Rehash → 冲突解决 → 对比 |
| 14 | 面试常见追问？| Rehash 期间会阻塞吗？| **不会！** 渐进式每次只搬 1 个桶；大 Key 数也要几千次才搬完；每次 O(1) → 不阻塞 |
| 15 | 面试常见追问？| SkipList 层数为什么是 64？| 64 层足以覆盖 2^64 个元素（O(logN) → 64 层够全宇宙）|
| 16 | 面试陷阱？| 说 "Redis 用开放寻址法解哈希冲突" → ❌ **用链地址法（拉链法）**；开放寻址删除复杂；拉链法简单 |
| 17 | 面试陷阱？| 说 "Rehash 期间新 Key 插入 ht[0]" → ❌ **插入只往 ht[1]**；ht[0] 不再新增；等全部搬完 ht[1] 变新 ht[0] |
| 18 | 小 Hash/Set/ZSet 底层？| 小 Hash/Set 用 ziplist/intset；ZSet 用 ziplist（元素 < 128 且 Member < 64B）→ 省内存 |
| 19 | Redis 7.0 Dict 改进？| 自动缩容；Dict 结构优化 |
| 20 | ZADD NX / XX / CH / INCR 选项？| NX：只加新 Member；XX：只更新已存在；CH：返回变化数；INCR：相当于 ZINCRBY |
| 21 | ZRANGEBYSCORE 源码级流程？| SkipList 最高层往下找 score 起点 → 累积 span → 从 Level 0 开始往后遍历 |
| 22 | Dict iterators 为什么暂停 Rehash？| 迭代器遍历期间如果 Rehash → 迭代器可能漏/重复；所以 iterators > 0 时暂停 Rehash |
| 23 | 典型生产配置？| **dict-resize-enabled=yes（7.0+ 默认自动缩容）+ maxmemory-policy=allkeys-lru** |
| 24 | Redis 哈希表负载因子？| 默认 1.0（used/size > 1 扩容）；和 Java HashMap（0.75）不同 |
| 25 | Sorted Set ZINCRBY 原子性？| 读 score → 计算新 score → 更新 dict + 更新 skiplist → 原子（Redis 单线程）|
| 26 | SkipList 内存开销？| 每个节点 64 层指针（柔性数组）→ 平均 1.33 层实际用 → 和红黑树差不多 |
| 27 | ZSet 小数据结构 ziplist 切换条件？| 元素 ≤ 128 且 Member 长度 ≤ 64B → 用 ziplist（压缩列表，省内存）；超过自动转 dict+skiplist |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：src/server.h + src/t_zset.c + src/dict.c  
**面试考点：27 题**  
**差异化点**：Sorted Set = dict+skiplist 双索引、**SkipList span 精确排名**、**渐进式 Rehash 两表+分桶搬移**、size 2 的幂用 & 替代 %、SkipList vs 红黑树 vs 有序数组对比
