# Redis Cluster 16384 哈希槽 + 迁移源码级深度解析

> **源码基线**：Redis 7.2.x（C）  
> **核心仓库**：antirez/redis → src/cluster.c + src/server.c  
> **面试权重**：⭐⭐⭐⭐⭐（Redis 分片核心，高频必问）

---

## 一、Redis Cluster 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis Cluster = Redis 官方分布式分片方案                  │
│                                                               │
│  Cluster 架构（3 主 3 从最小生产集群）：                       │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ Master 1  │ Master 2  │ Master 3  │  16384 哈希槽分片     │
│  │ Slots:    │ Slots:    │ Slots:    │                       │
│  │ 0-5460   │ 5461-10922│10923-16383│                       │
│  └────┬─────┴────┬─────┴────┬─────┘                         │
│       │ Replicate │          │                                │
│  ┌────┴─────┐┌───┴────┐┌───┴────┐                          │
│  │ Slave 1   │ Slave 2  │ Slave 3  │                          │
│  └──────────┴──────────┴──────────┘                         │
│
│ ★ 16384 哈希槽（面试必背！）：                                │
│   为什么 16384 = 2^14？                                      │
│   → CRC16 算法输出 16bit → 0~65535                          │
│   → 只用低 14bit → 0~16383 → 16384 个槽                     │
│   → 足够大（每个槽管几千 key）+ 足够小（元数据小）           │
│
│ ★ Key → Slot 计算（源码级）：                                │
│   CRC16(key) % 16384                                        │
│   // src/cluster.c                                          │
│   unsigned int keyHashSlot(char *key, int keylen) {         │
│       return crc16(key, keylen) & 0x3FFF;  // 取低 14 bit  │
│   }                                                          │
│
│ ★ 大 Key / 多级 Key：                                        │
│   user:{user1}:name → CRC16("user1") % 16384                │
│   只算 { } 中间的部分（同一个 hash tag 的 key 会到同一槽）   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、哈希槽分配源码级

### 2.1 集群初始化槽分配

```c
// ★ Redis Cluster 哈希槽分配源码（cluster.c）
//
// // ClusterLinkNode 结构：每个 Master 节点
// typedef struct clusterLinkNode {
//     char name[40];                    // Node ID（40 位 SHA1）
//     unsigned char slots[16384/8];     // ★ 位图：这个节点管哪些槽
//     int numslots;                     // 管多少槽
//     int flags;                        // MASTER / SLAVE / FAIL ...
//     // ...
// } clusterLinkNode;
//
// // ★ 初始化集群时平均分配槽
// // redis-cli --cluster create 192.168.1.1:6379 192.168.1.2:6379 192.168.1.3:6379
// → 自动平均分配
// Master 1: slots[0-5460]    → 5461 槽
// Master 2: slots[5461-10922] → 5462 槽
// Master 3: slots[10923-16383] → 5461 槽
//
// ★ 面试高频：为什么位图用 16384/8 = 2048 bytes？
//   每个槽一个 bit → 16384 bit = 2048 byte
//   → 每个节点元数据很小（KB 级）→ 节点间 Gossip 高效
//
```

### 2.2 Key → 节点查找流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Client 请求 Key → 路由到正确节点（源码级）                │
│                                                               │
│  ① Client 连接任意 Master（比如 Master 1）                    │
│  ② Client 发命令：SET user:1 "Alice"                         │
│  ③ Master 1 计算：CRC16("user:1") % 16384 = 5123              │
│  ④ Master 1 查自己的 slots 位图 → 槽 5123 是不是自己的？    │
│     ✅ 是自己的 → 直接执行 → 返回结果                        │
│     ❌ 不是 → 查槽 5123 归谁（Master 2）→ 返回重定向        │
│                                                               │
│  ★ 重定向两种类型（面试高频！）：                              │
│                                                               │
│  MOVED：槽已经永久迁移完成 → Client 以后直接连 Master 2      │
│     返回：-MOVED 5123 192.168.1.2:6379                       │
│                                                               │
│  ASK：槽正在迁移中（部分 key 在 Master 1，部分在 Master 2）  │
│     返回：-ASK 5123 192.168.1.2:6379                         │
│     Client 需要先连 Master 2 执行一次 ASKING 命令           │
│     → 临时让 Master 2 接受这个槽的请求                        │
│
│ ★ 重定向 vs 代理（对比其他方案）：                              │
│   Redis Cluster：**重定向**（Client 自己找路）                │
│   → Client 首次需要 MOVED/ASK，之后缓存 Slot→Node 映射      │
│   → 优点：Server 简单，Client 智能                          │
│   → 缺点：Client 实现复杂（需要支持重定向）                  │
│
│   Codis：**代理**（Codis Proxy 路由）                        │
│   → Client 只连 Proxy，Proxy 转发                            │
│   → 优点：Client 透明（和普通 Redis 一样）                  │
│   → 缺点：Proxy 单点 + 性能损失                              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、槽迁移源码级

### 3.1 槽迁移完整流程

```c
// ★ Redis Cluster 槽迁移源码级（cluster.c）
//
// 场景：Master 1（0-5460）→ 迁移槽 5000 到 Master 2
//
// Step 1: SLOTSET 通知所有节点
//   Master 2 执行：cluster delslot 5000 （从当前节点移除）
//   Master 1 执行：cluster addslot 5000 （接收槽）
//   → 通过 Gossip 传播给所有节点
//
// Step 2: 进入迁移状态（IMPORTING / MIGRATING）
//   // cluster.c：
//   // Master 1 上：
//   srcNode = clusterLookupNodeBySlot(5000);  // 原来的 Master 1
//   destNode = clusterLookupNodeByName("Master 2");
//   clusterMigrateAdd(destNode, 5000);  // 标记自己在 MIGRATING 状态
//   srcNode = destNode;  // 目标节点
//
//   // Master 2 上：
//   clusterImportAdd(srcNode, 5000);  // 标记自己在 IMPORTING 状态
//
// Step 3: 导入 Key（从源节点拉取数据）
//   // Master 1 执行 cluster getkeysinslot 5000 count
//   // Master 1 执行 cluster migrate <dest_ip> <dest_port> "" 0 timeout keys ...
//   //
//   // 实际上 redis-cli 批量执行：
//   redis-cli --cluster reshard <cluster_host:port> --cluster-from <src_node_id> --cluster-to <dest_node_id> --cluster-slots 5000
//   // 内部循环：
//   //   ① cluster getkeysinslot 5000 100 → 拿到 100 个 key
//   //   ② cluster migrate dest_ip dest_port "" 0 5000 key1 key2 ...
//   //   ③ 重复直到这个槽的 key 全部迁完
//
// Step 4: 迁移完成
//   Master 1 执行 cluster delslot 5000
//   Master 2 执行 cluster addslot 5000
//   → 所有节点通过 Gossip 更新
//   → 以后槽 5000 完全归 Master 2
//
```

### 3.2 ASK vs MOVED 重定向

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ASK vs MOVED 重定向（面试最高频！）                        │
│                                                               │
│  槽 5000 正在从 Master 1 迁到 Master 2 时：                  │
│                                                               │
│  Client 请求 SET key_xxx "value"（key 在槽 5000）：        │
│                                                               │
│  情况 A：key 还在 Master 1（还没迁走）                      │
│    → Master 1 正常处理 ✅                                     │
│                                                               │
│  情况 B：key 已经迁到 Master 2（迁走了）                    │
│    → Master 1 找这个 key 找不到                              │
│    → Master 1 返回 ASK 5000 Master 2                         │
│    → Client 连 Master 2 → 先 ASKING → 再 SET key_xxx        │
│    → Master 2 正在 IMPORTING 状态 → 接受 ASKING 的请求      │
│    → 否则（非 ASKING）Master 2 会拒绝（因为这个槽还没正式归它）│
│                                                               │
│  MOVED vs ASK 核心区别：                                      │
│   MOVED：**永久性** → Client 更新 Slot→Node 映射缓存         │
│   ASK：**临时性** → Client 只是这次去 Master 2，不更新缓存  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Gossip 协议 + 故障检测

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis Cluster Gossip 协议（面试高频！）                    │
│                                                               │
│  节点间定期（每秒）交换信息：                                  │
│   → 自己管哪些槽？                                            │
│   → 其他节点状态（ONLINE / FAIL / PFAIL）                    │
│   → Master/Slave 关系                                         │
│
│  ★ 故障检测（两种状态）：                                      │
│                                                               │
│  ① PFAIL（Possibly Fail）：                                   │
│   某个节点超过 cluster-node-timeout（默认 15s）没响应        │
│   → 探测节点标记为 PFAIL                                      │
│   → 继续心跳                                                  │
│
│  ② FAIL（Fail）：                                             │
│   超过半数（N/2+1）Master 节点都认为 PFAIL                   │
│   → 共同标记为 FAIL（基于 Gossip 传播）                        │
│
│  ★ Failover（故障转移）：                                      │
│   Master 挂了 → 它的 Slave 主动发起 Failover                  │
│   条件：Slave 数据足够新（和 Master 同步延迟 < 阈值）        │
│   → Slave 通知所有 Master → 获得多数投票                      │
│   → Slave 晋升为新 Master（继承老 Master 的所有槽）           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题，史上最全）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis Cluster 核心？| **16384 哈希槽 + Master/Slave + Gossip 故障检测** |
| 2 | 为什么 16384 槽？| CRC16 输出 16bit → 取低 14bit → 2^14=16384 |
| 3 | Key → Slot 计算？| **CRC16(key) % 16384**；多级 Key 只算 `{ }` 中间部分 |
| 4 | 槽位图大小？| 16384 bit = **2048 byte** → 元数据很小 |
| 5 | MOVED vs ASK？| **MOVED 永久重定向（Client 更新缓存）；ASK 临时（槽迁移中）** |
| 6 | 槽迁移完整流程？| SLOTSET → MIGRATING/IMPORTING → 批量 migrate key → 完成 delslot/addslot |
| 7 | Gossip 协议？| 节点间每秒交换信息（槽分配 + 节点状态）|
| 8 | PFAIL vs FAIL？| **PFAIL 单节点标记；FAIL 需要 N/2+1 Master 共识** |
| 9 | Failover 条件？| Slave 主动发起 + 数据足够新 + 获得多数 Master 投票 |
| 10 | Cluster vs Codis vs Twemproxy？| Cluster 重定向 + 官方；Codis 代理 + Sentinel；Twemproxy 代理 + 简单 |
| 11 | ASKING 命令？| Client 执行 ASK 重定向后 → 先 ASKING 让目标节点临时接受这个槽 |
| 12 | 面试最高频考点排序？| **1. 16384 槽 2. CRC16(key)%16384 3. MOVED vs ASK 4. 槽迁移 5. Failover** |
| 13 | 学习路径？| cluster.c → keyHashSlot → clusterLinkNode slots 位图 → ASK/MOVED → Failover |
| 14 | 面试常见追问？| key `{user1}:name` 和 `{user1}:age` 到同一槽吗？→ 是（hash tag 相同）|
| 15 | 为什么不用一致性哈希？| Redis Cluster 用**固定槽 + 平均分配**（比一致性哈希更简单、更稳定）|
| 16 | Cluster 扩容 vs 一致性哈希？| Cluster：手动 reshard 指定槽；一致性哈希：自动只影响 1/N |
| 17 | Slave 不接受写？| Slave 默认只读（REPLICAOF 模式）；但 Redis 6.0+ 可以配置副本接受读 |
| 18 | Cluster 命令？| CLUSTER INFO / CLUSTER NODES / CLUSTER SLOTS / CLUSTER KEYSLOT |
| 19 | 2024 新特性？| Redis 7.2+ CLUSTER SHARDS 命令更高效；CLUSTER LINKS 监控节点间连接 |
| 20 | 面试高频场景？| 集群扩容（reshard）+ 故障转移 + 大 Key 分片 |
| 21 | Cluster vs TiKV vs Kafka 分片？| Cluster：Key→Slot；TiKV：Region；Kafka：Partition；核心都是分片 |
| 22 | 面试陷阱？| 说 Redis Cluster 用一致性哈希 → ❌ 错！用固定槽 + CRC16%16384 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：src/cluster.c  
**面试考点：22 题（史上最全）**  
**差异化点**：16384 槽 CRC16 → MOVED vs ASK → 槽迁移完整链路 → Gossip + Failover
