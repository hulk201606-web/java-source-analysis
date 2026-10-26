# Redis Cluster Slot 分片 + Gossip 协议源码级深度解析

> **源码基线**：Redis 7.x / 8.0（C）  
> **核心位置**：src/server.c + src/cluster/cluster.c + src/cluster/cluster.h  
> **面试权重**：⭐⭐⭐⭐⭐（分布式缓存核心，高频）

---

## 一、Redis Cluster 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis Cluster = 官方分布式 Redis 集群                      │
│                                                               │
│  核心机制（面试必背）：                                        │
│   ✅ 16384 Slot 哈希分片（CRC16 % 16384）                    │
│   ✅ Gossip 协议（节点间异步传播元数据）                     │
│   ✅ MOVED / ASK 重定向（客户端跟随路由）                    │
│   ✅ Master + Replica（主从复制，故障自动切换）               │
│   ✅ 无外部协调器（靠 Gossip + CRC16）                      │
│                                                               │
│  与 Codis / Twemproxy 对比（第六节）：                        │
│   Redis Cluster 原生分布式；Codis/Twemproxy 代理层分片       │
│   Redis Cluster 客户端智能路由；代理层对客户端透明但延迟高   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、16384 Slot + CRC16 源码级

```c
// ★ CRC16 + 16384 Slot 核心（源码级，面试核心）
//
// src/cluster/cluster.c
// src/cluster/crc16.c
//
// // CRC16 实现（CCITT 标准多项式 0x1021）：
// // src/cluster/crc16.c
// const uint16_t crc16tab[256] = { /* 预计算表 */ };
//
// uint16_t crc16(const char *buf, int len) {
//     int counter;
//     uint16_t crc = 0;
//     for (counter = 0; counter < len; counter++)
//         crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
//     return crc;
// }
//
// // ★ Key → Slot 映射：
// // // src/cluster/cluster.c → keyHashSlot()
// int keyHashSlot(char *key, int keylen) {
//     int s, e; /* start-end indexes of { and } */
//     // ① 特殊支持 hash tag：{user}:token
//     //    只对 {user} 里的 user 做 CRC16
//     //    → 多个 Key 可以强制在同一个 Slot
//     for (s = 0; s < keylen; s++)
//         if (key[s] == '{') break;
//     if (s < keylen) {
//         for (e = s+1; e < keylen; e++)
//             if (key[e] == '}') break;
//         if (e-s > 1) {
//             key += s+1;
//             keylen = e-s-1;
//         }
//     }
//     // ② 对 key（或 hash tag）做 CRC16 % 16384
//     return crc16(key, keylen) & 0x3FFF;  // % 16384 = & 0x3FFF
// }
//
// ★ 为什么 16384 Slot（面试高频答案）：
//   Redis 作者 antirez 说：
//   - 16384 是 2^14（方便 & 0x3FFF 快速取模）
//   - 每个 Slot 平均分布 Key（CRC16 均匀）
//   - 16384 足够大（避免单 Slot 太大）、又足够小（Gossip 传播快）
//   - 集群节点数一般 3-100；16384 Slot 每个节点 164-5461 个 Slot
//
```

---

## 三、Gossip 协议源码级

### 3.1 Gossip 三类消息

```c
// ★ Redis Cluster Gossip 协议（源码级，面试核心）
//
// src/cluster/cluster.c → clusterGossip()
//
// // 每个节点维护一个 Peers List（已知节点列表）：
// // typedef struct clusterNode {
// //     char name[CLUSTER_NAMELEN];  // 节点 ID（40 hex）
// //     char *ip; int port;
// //     unsigned char flags;        // PFAIL/FAIL/MASTER/SLAVE...
// //     uint64_t ping_sent;         // 上次发 PING 的时间
// //     uint64_t pong_received;     // 上次收 PONG 的时间
// //     clusterNode *slaveof;       // Replica 指向 Master
// //     unsigned char *slaves;      // Master 持有 Replica 列表
// //     int numslaves;
// //     uint8_t slots[CLUSTER_SLOTS/8];  // ★ 这个节点持有哪些 Slot（bitmap）
// //     uint64_t epoch;             // 配置版本（每次 Slot 迁移 +1）
// // } clusterNode;
//
// // ★ Gossip 三类核心消息：
//
// // 1. PING / PONG（心跳 + 元数据交换）
// //    cluster.c → clusterSendPing()
// //    随机选 10 个已知节点发 PING
// //    PING payload = 自己的状态 + 若干 Peers 的状态
// //    接收方：合并 Peers 列表 → 更新本地视图
// //
// //    频率：每秒 PING 3 次（cluster_ping_rate 默认 100ms 间隔）
// //    PONG 延迟 < cluster_node_timeout/2 → 标记节点 OK
// //    PONG 延迟 > cluster_node_timeout → 标记 PFAIL
// //
// // 2. MEET（节点初次加入集群）
// //    CLUSTER MEET ip port
// //    A → B 发 MEET；B 收到后把 A 加入 Peers → 开始 Gossip
// //    两个节点开始互相 PING → 通过 Gossip 传播给全集群
// //
// // 3. PUBLISH（Slot 变更广播）
// //    当 Slot 迁移完成（MOVED / IMPORT 操作）
// //    主动广播 clusterSetSlotMigrating / ... 给全集群
// //    比 Gossip 快（立即广播 vs 等待异步传播）
//
// // ★ Gossip 传播特性：
// //   每个节点随机选 10 个 Peers 发 PING
// //   每个 PING 带最多 3 个 Peers 的状态
// //   → 最终整个集群视图收敛（几秒内）
// //   → 不是严格广播（可能有丢），但最终一致
//
```

### 3.2 PFAIL → FAIL 故障检测

```c
// ★ PFAIL → FAIL 故障检测流程（源码级，面试必背）
//
// src/cluster/cluster.c
//
// // 单节点视角：
// // 我连续 cluster_node_timeout ms（默认 15s）没收到某节点 PONG
// // → 标记对方为 PFAIL（Possibly Fail，主观判断）
// //
// // 但 PFAIL 不能直接触发故障转移！
// // 因为可能是网络分区（我挂了但对方没挂）
// //
// // ★ FAIL 需要多数节点确认（客观判断）：
// // clusterFailReport()
// //
// // 1. 节点 A 标记 B 为 PFAIL
// //    → 通过 Gossip 把 A 的 PFAIL 状态传播给其他节点
// //
// // 2. 节点 C 也标记 B 为 PFAIL（C 也收不到 B 的 PONG）
// //    → 全集群 PFAIL 状态会被 Gossip 传播
// //
// // 3. 任意节点 D 收到：
// //    超过半数 Master 都标记 B 为 PFAIL
// //    → D 本地把 B 标记为 FAIL（客观确认）
// //    → D 发起故障转移：B 的 Replica 选一个升主
// //
// // 故障转移源码级（Replica 升主）：
// void clusterHandleSlaveFailover(clusterNode *slave) {
//     // 1. 收集所有认为 Master FAIL 的 Master 节点
//     clusterNode **masters = getMastersFailingMaster(slave->master);
//
//     // 2. 检查投票数是否达标（半数以上）
//     if (masters_count > masters_total/2) {
//         // 3. 选最合适的 Replica 升主
//         //    选 offset 最大的（最接近 Master 数据）
//         clusterNode *best = pickBestSlave(slave->master->slaves);
//
//         // 4. 升主：CLUSTER FAILOVER TAKEOVER FORCE
//         clusterFailoverReplaceMaster(best, slave->master);
//         //    best 变成新 Master → 承担旧 Master 的所有 Slot
//     }
// }
//
// ★ PFAIL vs FAIL 核心区别：
//   PFAIL = 单个节点主观判断（可能网络抖动）
//   FAIL  = 多数 Master 客观确认（真正挂了）
//   这是 Redis Cluster 防脑裂的关键
//
```

---

## 四、MOVED / ASK 重定向源码级

```c
// ★ MOVED vs ASK 重定向（源码级，面试核心）
//
// src/cluster/cluster.c → clusterRedirectClient()
//
// // 场景 1：Slot 已经迁移完成（确定归属）→ MOVED
// // 客户端请求 key=foo（Slot 1234）→ 发给 Node A
// // Node A 发现 Slot 1234 属于 Node B（Gossip 已更新）
// // → 返回 MOVED 1234 NodeB:port
// // → 客户端收到 MOVED → 更新本地 Slot → 下次直接发 Node B
// //
// // MOVED：永久重定向；客户端更新路由表
//
// // 场景 2：Slot 正在迁移（从 A → B，还没完成）→ ASK
// // 客户端请求 key=foo（Slot 1234）→ 发给 Node A
// // Node A 发现 Slot 1234 在迁移中 → 部分 Key 已到 B
// // → 返回 ASK 1234 NodeB:port
// // → 客户端收到 ASK → 临时去 Node B 查一次
// // → 不更新本地路由表（下次还是发 Node A，直到 MOVED）
// //
// // ASK：临时重定向；客户端不更新路由表
//
// ★ 客户端处理流程（面试高频）：
//   1. 初次请求 → 发任意节点
//   2. 收到 MOVED → 更新本地 Slot→Node 映射 → 重发
//   3. 收到 ASK → 临时指定 Node → 重发 → 不更新映射
//   4. 收到 TRYAGAIN → 重试（Slot 迁移冲突）
//
// // ASK 迁移过程细节：
// // SLOTIMPORT 目标节点 B：把 Slot 相关 Key 从 A 拉到本地
// // SLOTDEL   源节点 A：迁移完成后删除本地 Key
// // 迁移期间：A 和 B 都响应 Slot 的请求
// //   - 已迁移的 Key → B 返回
// //   - 未迁移的 Key → A 返回
// //   → 客户端可能需要 ASK 两个节点（概率低）
//
```

---

## 五、Redis Cluster vs Codis vs Twemproxy 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种 Redis 分片方案对比                          │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐  │
│  │ 维度      │ Redis Cluster │ Codis         │ Twemproxy     │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 架构       │ 原生分布式    │ Proxy + Codis│ Proxy + Redis │  │
│  │            │ 客户端路由    │ 自定义 Redis │ 原生 Redis    │  │
│  │            │ 16384 Slot   │ 协议实现     │ 163 Slots     │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 分片机制   │ CRC16 % 16384│ 预分片 1024  │ Pre-sharding  │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 故障转移   │ ★ 内置 Gossip │ ZK 协调       │ ❌ 无         │  │
│  │            │ PFAIL→FAIL   │ Sentinel 感知 │ 靠代理层      │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 动态扩缩   │ ★ Slot 迁移   │ 重新分片      │ ❌ 不支持      │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ Pipeline    │ ★ 原生支持   │ ✅ 代理透传    │ ✅ 代理透传    │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 性能       │ ★ 最高（直连）│ 中（多一跳）  │ 中（多一跳）  │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 双写       │ ★ Hash Tag   │ ❌ 复杂        │ ❌ 不支持      │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 维护成本   │ 低（官方）    │ 中（自研）    │ 低（开源）    │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 面试高频   │ ★★★★★        │ ★★★          │ ★★★          │  │
│  └──────────┴──────────────┴──────────────┴──────────────┘  │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   新项目 → **Redis Cluster**（官方、Gossip、Slot 迁移、客户端直连）│
│   存量 Codis → 迁移成本高可暂时保留；新项目不推荐              │
│   Twemproxy → 简单代理层、无故障转移能力、现代项目不推荐      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis Cluster 核心分片机制？| **CRC16(key) % 16384 → 哈希 Slot** |
| 2 | 为什么 16384 Slot？| 2^14，方便 & 0x3FFF 快速取模；均匀+传播快 |
| 3 | Hash Tag 作用？| `{user}:token` → 只对 `user` 做 CRC16；**强制多 Key 落同一 Slot**（支持 Multi-Key 操作）|
| 4 | Gossip 三类核心消息？| **PING/PONG（心跳+元数据）+ MEET（初次加入）+ PUBLISH（Slot 变更广播）** |
| 5 | Gossip 传播机制？| 随机选 10 个 Peer 发 PING；每个 PING 带最多 3 个 Peer 状态；**最终一致** |
| 6 | ★ PFAIL vs FAIL 故障检测？| PFAIL=主观判断（单节点）；**FAIL=多数 Master 客观确认**（防脑裂）|
| 7 | Replica 升主条件？| 收到半数以上 Master 标记 Master 为 FAIL；选 offset 最大的 Replica |
| 8 | ★ MOVED vs ASK 重定向？| MOVED=Slot 迁移完成（客户端更新路由表）；**ASK=Slot 迁移中（临时重定向，不更新路由）** |
| 9 | 客户端收到 MOVED/ASK 怎么处理？| MOVED：更新本地 Slot→Node 映射 → 重发；ASK：临时指定 Node → 重发 → **不更新映射** |
| 10 | Slot 迁移过程？| SLOTIMPORT（目标拉 Key）+ SLOTDEL（源删 Key）；**迁移期间 A/B 都响应** |
| 11 | Redis Cluster vs Codis vs Twemproxy？| 见第五节对比表（9 维度）|
| 12 | 面试最高频考点排序？| **1. CRC16 + 16384 Slot 2. PFAIL→FAIL 故障检测 3. MOVED vs ASK 4. Gossip 三类消息 5. 三方案对比** |
| 13 | 学习路径？| CRC16 → Gossip PING/PONG → PFAIL→FAIL → MOVED/ASK → Slot 迁移 |
| 14 | 面试常见追问？| 为什么 Gossip 不直接广播？| 随机选 10 个 Peer → 每个再带 3 个 Peer 状态 → 几轮全集群收敛；节省带宽（O(N)）|
| 15 | 面试常见追问？| PING 频率？| cluster_ping_rate 默认 100ms 间隔；**每秒 PING 3 次** |
| 16 | 面试陷阱？| 说"MOVED 是临时重定向"→ ❌ MOVED 是**永久重定向**（Slot 已归属）；ASK 才是临时 |
| 17 | 面试陷阱？| 说"PFAIL 就能触发故障转移"→ ❌ 必须**多数 Master 标记为 FAIL**才触发 |
| 18 | Redis Cluster 支持 Multi-Key 操作吗？| 只要所有 Key 落在同一 Slot（用 Hash Tag）就支持；跨 Slot 的事务会报错 |
| 19 | cluster_node_timeout 默认？| 15s；超过没收到 PONG → 标记 PFAIL；网络差时调大 |
| 20 | Cluster 健康检查 CLUSTER INFO？| cluster_state（ok/fail）+ slots_ok 数量 + 各节点状态 + epoch |
| 21 | Codis 和 Redis Cluster 核心差异？| Codis Proxy 层分片（客户端透明）；Redis Cluster 原生分布式（客户端路由）；Codis 维护成本高 |
| 22 | ASK 迁移期间客户端应该怎么做？| 先发给源节点 → 源节点返回 ASK → 临时去目标节点查；两次概率低但存在 |
| 23 | 节点重启后 Slot 状态？| 从 RDB/AOF 恢复 Key → Gossip 接收最新 Slot→Node 映射 → 如果是 Master 可能需要故障转移回 |
| 24 | Redis Cluster 支持哪些命令？| 大部分 Redis 命令；但 Lua Script 必须确保所有 Key 在同一 Slot；BITMAP/HLL 跨 Slot 不支持 |
| 25 | 如何手动 Slot 迁移？| CLUSTER SETSLOT slot MIGRATING / IMPORTING / STABLE；CLUSTER MEET；CLUSTER FAILOVER TAKEOVER |
| 26 | 典型生产用法？| 6 节点 Cluster（3 Master × 2 Replica）→ Cache + 分布式锁 + 计数器；客户端带 Hash Tag 保证多 Key 落同 Slot |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：src/cluster/cluster.c + src/cluster/crc16.c + src/server.c  
**面试考点：26 题**  
**差异化点**：CRC16+16384 Slot、PFAIL→FAIL 多数 Master 确认防脑裂、MOVED vs ASK 重定向、**Redis Cluster vs Codis vs Twemproxy 三方案对比**
