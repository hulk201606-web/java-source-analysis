# Redis 源码深度解析（C 实现 + Java 用户视角）

> **源码基线**：Redis 7.4（最新稳定）  
> **核心仓库**：https://github.com/redis/redis  
> **面试权重**：⭐⭐⭐⭐⭐（缓存高频，5+ 种数据结构 + Cluster/Sentinel）

---

## 一、Redis 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：内存中的 Key-Value 存储（不仅是缓存！也是数据库）    │
│  为什么快？全内存（SSD/机械盘 1000 倍）+ 单线程模型（无锁）+ IO 多路复用（epoll）│
│                                                               │
│  单线程模型为什么没问题？                                      │
│  Redis 的瓶颈是内存和网络 IO，不是 CPU                         │
│  单线程避免锁竞争 + epoll 非阻塞 IO → 性能极高                 │
│  （Redis 6.0+ 引入 IO 多线程，但命令执行仍单线程）              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  五种核心数据结构（每种都有不同的底层编码！面试必问）           │
│                                                               │
│  String → int / embstr / raw（三种编码动态切换）               │
│  Hash   → ziplist / hashtable（两种编码）                     │
│  List   → quicklist（ziplist + 双链表，Redis 3.2+ 默认）      │
│  Set    → intset / hashtable                                  │
│  ZSet   → ziplist / skiplist + hashtable（★ 跳表！）          │
│                                                               │
│  ★ 编码切换（Redis 动态优化内存）：                             │
│  String 长度 < 44B → embstr（嵌入式）；> 44B → raw（动态字符串）│
│  Hash field 数 ≤ 512 → ziplist（紧凑数组）；> 512 → hashtable  │
│  ZSet 成员数 ≤ 128 → ziplist；> 128 → skiplist + hashtable    │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心数据结构源码（C 实现，但面试高频）

### 2.1 SDS（Simple Dynamic String，String 底层）

```c
// ★ Redis String 底层是 SDS（不是 C 原生 char*！）
//   C char* 不安全（\0 截断）+ 无长度记录
//   SDS 封装：
//
//   typedef struct sdshdr {
//       int len;      // 当前字符串长度
//       int free;     // 剩余空间
//       char buf[];   // 实际数据（柔性数组）
//   } sdshdr;
//
// ★ SDS 相比 C char* 的优势：
//   ① O(1) 取长度（C strlen 要 O(N) 扫到 \0）
//   ② 二进制安全（允许 \0 在中间）
//   ③ 空间预分配（防止频繁 realloc → 最多多分配一倍 → 接近幂等）
//   ④ 惰性释放（减少内存频繁分配）
//
// ★ embstr vs raw：
//   embstr：Redis Object + SDS 连续分配（内存碎片少，<= 44B 用）
//   raw：Redis Object + SDS 分开分配（> 44B 用）
//
// ★ 编码自动转换：
//   SET foo "hello" → embstr
//   APPEND foo "..." → 变长后自动转 raw
//   raw 字符串缩短到 44B 以下 → 不会自动转 embstr（避免频繁切换）
```

### 2.2 Skiplist（ZSet 跳表）

```c
// ★ ZSet（Sorted Set）底层是 SkipList（跳表）！不是红黑树！
//
// 为什么 Redis 用跳表不用红黑树？
//   ① 跳表实现简单（红黑树旋转/染色代码复杂）
//   ② 跳表范围查询快（天然链表结构，有序）
//   ③ 跳表内存比红黑树略大（但 ZSet 用 hashtable + skiplist 双索引，无所谓）
//
// 跳表结构（类似多级链表）：
//   ┌─────┬───┬───┬───┬───┐    L4 (最高层，稀疏)
//   │  H  │ 2 │   │   │   │
//   ├─────┼───┼───┼───┼───┤
//   │  H  │ 2 │ 4 │   │   │    L3
//   ├─────┼───┼───┼───┼───┤
//   │  H  │ 2 │ 4 │ 6 │   │    L2
//   ├─────┼───┼───┼───┼───┤
//   │  H  │ 2 │ 4 │ 6 │ 8 │    L1 (最底层，完整数据)
//   └─────┴───┴───┴───┴───┘
//   → 高层节点多为 skip 指针 → 查找从最高层开始 → O(logN)
//
// Redis skiplist 节点：
//   typedef struct zskiplistNode {
//       sds ele;                     // member（字符串）
//       double score;                // score（排序依据）
//       struct zskiplistNode *backward;  // 后退指针（最低层双向链表）
//       struct zskiplistLevel {      // 每一层的前进指针 + 跨度
//           struct zskiplistNode *forward;
//           unsigned long span;
//       } level[];                   // 柔性数组（最多 32 层）
//   } zskiplistNode;
//
// ★ ZSet 双索引：
//   skiplist：按 score 有序 → ZRANGEBYSCORE / ZRANK 快
//   hashtable：member → score 映射 → ZSCORE / ZREM 快（O(1)）
//   → 数据存一份，两份索引指向同一个 member 对象
```

### 2.3 quicklist（List 底层）

```c
// ★ Redis List 底层：quicklist（Redis 3.2+ 引入，替代 ziplist + adlist）
//
// quicklist = N 个 ziplist + 双向链表（每个 node 是一个 ziplist）
//
// ziplist（紧凑列表）：
//   连续内存数组（不 malloc 每个元素）
//   元素间用长度前缀编码（prevlen + len + value）
//   → 内存极小但插入/删除 O(N)
//
// quicklist 设计：
//   ┌──────┐ ┌──────┐ ┌──────┐
//   │ ziplist│ │ ziplist│ │ ziplist│
//   │  node 0│ ↔ │  node 1│ ↔ │  node 2│
//   └──────┘ └──────┘ └──────┘
//   每个 ziplist 最多包含 8KB（list-max-ziplist-size）
//
// ★ 为什么不用纯 ziplist？
//   ziplist 内存连续 → 头部插入 O(N)（要移动所有元素）
//   → quicklist 用双向链表 + ziplist 节点 → 头部插入只改 node 指针
//   → 既有 ziplist 的内存效率，又有链表的插入效率
//
// ★ 为什么不用纯 adlist（双向链表）？
//   每个元素一个 malloc → 内存碎片严重 + 指针开销大（每个 8B）
//
// ★ quicklist 是 Redis 最精妙的数据结构之一！
```

### 2.4 Cluster 模式（★ 高可用核心）

```
// ★ Redis Cluster 16384 个槽（slot）
//   key → CRC16(key) % 16384 → 某个 slot → 某个 Node
//
// 16384 槽分配：
//   Node A：slot [0, 5460]
//   Node B：slot [5461, 10922]
//   Node C：slot [10923, 16383]
//   → 每个 Node 知道所有 slot 的归属（MOVED 重定向）
//
// MOVED 重定向流程：
//   Client → Node A：GET user:123
//   Node A 计算 CRC16("user:123") % 16384 = 5678 → slot 在 Node B
//   → 返回 MOVED 5678 NodeB_IP:Port
//   Client 自动重定向到 Node B → Node B 处理
//
// ★ Gossip 协议（Cluster 节点间通信）：
//   每个 Node 每秒随机选 2-3 个其他 Node 发送 PING
//   → 同步 Cluster 拓扑信息（slot 分配、节点上下线）
//   → 最多需要 log(N) 轮次让全网感知新节点
//
// ★ 和 Sentinel 模式对比：
//   Sentinel：Master-Slave 模式 + Sentinel 自动故障转移（不支持分片）
//   Cluster：自动分片 + 自动故障转移（支持水平扩展）
//
// ★ Cluster 缺点：
//   不支持跨 slot 事务（MULTI/EXEC 要求所有 key 在同一个 slot）
//   → 用 Hash Tag 强制同 slot：SET {user}:123 / SET {user}:456
//     → CRC16 计算只看 {user} → 同 slot！
```

### 2.5 AOF / RDB 持久化

```
// ★ Redis 两种持久化方式（面试必问区别）
//
// 1. RDB（快照，Binary 格式）
//   → BGSAVE：fork 子进程 → 子进程 dump 内存到 .rdb 文件
//   → 优点：文件紧凑（Binary）+ 恢复快
//   → 缺点：最后一次快照后的数据丢失（如果改了数据还没 BGSAVE）
//   → save 配置：save 3600 1（3600s 内至少 1 个 key 变化触发）
//
// 2. AOF（追加写日志，Text 格式）
//   → 每次写命令追加到 appendonly.aof
//   → 恢复时重放所有命令（类似 MySQL binlog）
//   → 优点：数据安全（最多丢 1s）
//   → 缺点：文件比 RDB 大 + 恢复慢（要重放命令）
//   → appendfsync 三种配置：
//     always：每次命令都 fsync（最安全，最慢）
//     everysec：每秒 fsync（默认，生产推荐！）
//     no：不 fsync（靠 OS，最快但不安全）
//
// ★ 生产推荐：RDB + AOF 混合（Redis 4.0+）
//   → AOF 重写时先写 RDB 快照 + 后面紧跟增量 AOF
//   → 恢复快 + 数据安全
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis 为什么单线程？| **瓶颈在内存和网络 IO，不是 CPU**；单线程避免锁竞争 + epoll 非阻塞 IO → 性能极高（10w+ QPS）|
| 2 | String 底层 SDS？| **Simple Dynamic String** → 比 C char* 好：O(1) 取长度 + 二进制安全 + 空间预分配 |
| 3 | ZSet 底层？| **Skiplist（跳表）+ Hashtable 双索引** → skiplist 按 score 有序；hashtable 按 member 直接查 |
| 4 | 为什么用跳表不用红黑树？| 跳表**实现简单** + **范围查询快**（天然链表）+ ZSet 已有 hashtable 做快速 member 查询 |
| 5 | List 底层 quicklist？| **N 个 ziplist + 双向链表** → ziplist 内存紧凑；链表结构 → 头部插入 O(1)；Redis 3.2+ 默认 |
| 6 | Hash 编码切换？| field 数 **≤ 512 → ziplist（紧凑数组）**；> 512 → hashtable |
| 7 | Cluster 槽数？| **16384 个 slot** → CRC16(key) % 16384 → MOVED 重定向到对应 Node |
| 8 | Gossip 协议？| Cluster 节点间 **每秒随机 Ping 2-3 个节点** → 同步拓扑；最多 log(N) 轮全网感知 |
| 9 | Cluster 跨 slot 事务？| 不支持！**用 Hash Tag 强制同 slot**：key={user}:123 / {user}:456 → CRC16 只算 {user} → 同 slot |
| 10 | RDB vs AOF？| RDB：Binary 快照（文件小，恢复快，但丢最后一次快照后的数据）；AOF：追加日志（数据安全最多丢 1s，但文件大）|
| 11 | AOF 重写？| **fork 子进程 → 扫描内存 dump 成新 AOF**（不是重新 dump 旧 AOF，因为旧 AOF 中间可能有很多无效命令）|
| 12 | Redis 过期删除策略？| **惰性 + 定期**：惰性=访问时检查过期删除；定期=每 100ms 随机选 20 个设置过期的 key 检查删除 |
| 13 | 内存淘汰策略（maxmemory）？| **volatile-lru（默认）/ allkeys-lru / volatile-lfu / allkeys-lfu / volatile-ttl / noeviction** |
| 14 | 发布订阅 Pub/Sub？| **PUBLISH channel msg → SUBSCRIBE channel**；不持久化（Redis 重启丢）；适合实时通知（不如 MQ 可靠）|
| 15 | Redis Pipeline？| **批量命令打包发送** → 减少网络往返（RTT）→ 一次发 100 个命令比 100 次单独发快 10 倍；但不支持事务 |
| 16 | Lua 脚本原子性？| Redis 执行 Lua 脚本时**阻塞其他命令**（单线程）→ 保证原子性；EVAL / EVALSHA |
| 17 | Sentinel vs Cluster？| Sentinel：Master-Slave + 自动故障转移（**不支持分片**）；Cluster：自动分片 + 自动故障转移（**支持水平扩展**）|
| 18 | Redis 6.0+ IO 多线程？| **命令执行仍单线程**；但 IO（读写网络 Socket）用多线程 → 高并发下网络 IO 成为瓶颈 → 多线程优化 |
| 19 | 分布式锁 RedLock？| Redis 作者 antirez 提出的多 Node 分布式锁 → 但后来自己质疑了（GC pause 可能超时）→ 生产建议用 Zookeeper/etcd |
| 20 | BitMap 存用户活跃？| SETBIT user:login:20241022 1 → bit 数组 → 每天一个 key；BITCOUNT 统计活跃数（O(N) 但 N=天数，小） |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Redis 7.4（C 实现 + 用户视角解析）  
**面试考点：20 题**
