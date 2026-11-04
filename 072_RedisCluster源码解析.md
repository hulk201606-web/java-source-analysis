# Redis Cluster 源码深度解析（Redis 分布式集群）

> **源码基线**：Redis 7.4.x（Cluster 模式）  
> **核心仓库**：https://github.com/redis/redis  
> **面试权重**：⭐⭐⭐⭐⭐（Redis 分布式最高频）

---

## 一、Redis Cluster 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：Redis 分布式集群（分片 + 高可用）                  │
│  解决什么问题？单节点 Redis 内存有限 + 单点故障                │
│                                                               │
│  四种 Redis 高可用方案对比（面试必问！）                      │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │ Redis Master-  │ Redis Cluster  │ Twemproxy    ││
│  │                │ Slave + Sentinel│              │ / Codis      ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 分片           │ ❌ 不支持     │ ✅ Hash Slot    │ ✅ 代理层     ││
│  │ 自动故障转移   │ ✅ Sentinel   │ ✅ 内置         │ ❌ 手动       ││
│  │ 客户端支持     │ Jedis/Lettuce │ Jedis Cluster  │ 代理透明     ││
│  │ 运维复杂度     │ ★★ 简单      │ ★★★ 中等      │ ★★★★ 高      ││
│  │ 社区推荐       │ 小型/测试    │ ★★★★★ 生产   │ 已过时       ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ Redis Cluster 是 Redis 官方推荐的生产分布式方案！          │
│ ★ Codis 已停止维护 → 用 Redis Cluster 或 Redis Proxy         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Redis Cluster 架构                                          │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │  Master Node 1 │ Master Node 2 │ Master Node 3 │         │
│  │ (Slot 0-5460)  │ (Slot 5461-10922) │ (Slot 10923-16383)  │ │
│  │   └─ Slave A   │   └─ Slave A   │   └─ Slave A   │         │
│  │                │                │                │         │
│  └──────────────┴──────────────┴──────────────┘             │
│                                                               │
│  ★ 16384 Hash Slot（固定！面试高频）                        │
│    CRC16(key) % 16384 → 算出 slot → slot 在哪个 Master → 数据在哪│
│  ★ 每个 Master 负责一段 Slot（可以不均匀，重新平衡）           │
│  ★ 每个 Master 至少一个 Slave（故障时自动提升）                │
│  ★ 奇数节点选主（Paxos-like 协议）                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Hash Slot 路由（★ 面试最高频）

```c
// ★ Redis Cluster Hash Slot 核心
//
// 固定 16384 个 Slot（0 ~ 16383）
// 计算方式：
//   slot = CRC16(key) % 16384
//
// 特殊 Key 路由（Hash Tag）：
//   user_id:{order_123} → 只对 {order_123} 部分算 CRC16
//   → {order_123} 相关的所有 key 都在同一个 Slot
//   → 可以做跨 key 的操作（MULTI/EXEC pipeline）
//
// 示例：
//   key = "user:1"     → CRC16("user:1") % 16384 = 5293 → Node 1
//   key = "user:2"     → CRC16("user:2") % 16384 = 8756 → Node 2
//   key = "user:{123}:name" → CRC16("123") % 16384 = 5293 → 和 user:1 同 Slot！
//   key = "user:{123}:age"  → CRC16("123") % 16384 = 5293 → 和上面同 Slot！
//   → 可以 MULTI 批量操作这两个 key
//
// ★ 客户端实现（Jedis Cluster / Lettuce）：
//   客户端缓存 Slot → Node 映射表
//   → 请求发往对应 Node
//   → 如果 Node 回复 MOVED/ASK → 更新缓存重试
//   → 直连模式（不是代理模式！）
```

### 2.2 MOVED / ASK 重定向

```c
// ★ Redis Cluster 两种重定向（面试高频！）
//
// ① MOVED（永久重定向）
//    场景：Slot 已经从 Node A 迁移到 Node B（迁移完成）
//    Node A 返回：-MOVED 5293 10.0.0.2:6379
//    → 客户端更新 Slot→Node 缓存
//    → 后续直接发往 Node B
//
// ② ASK（临时重定向）
//    场景：Slot 正在从 Node A 迁移到 Node B（迁移中）
//    Node A 返回：-ASK 5293 10.0.0.2:6379
//    → 客户端这次发往 Node B，但**不更新缓存**
//    → 后续继续发 Node A（等迁移完成收到 MOVED 再更新）
//
// ★ 为什么分两种？
//   MOVED：迁移完成 → 永久换节点
//   ASK：迁移中 → 临时去另一个节点，原节点还没完全放弃这个 Slot
```

### 2.3 Gossip 协议 + ClusterState

```c
// ★ Redis Cluster 用 Gossip 协议同步节点状态
//   不是 Raft/Paxos（简化版）
//
// 每个节点维护 ClusterState：
//   struct clusterState {
//     clusterNode *nodes[16384];  // Slot → Node 映射
//     clusterNode *myself;       // 自己
//     int epoch;                 // 当前 Epoch（类似 Raft term）
//   };
//
// 节点间 Gossip：
//   每个节点周期性（每秒）向随机 5 个节点发送 PING
//   → PONG 返回自己的 ClusterState
//   → 接收方合并 → 更新自己的视图
//   → 最终一致性（几秒内收敛）
//
// ★ 和 Raft/ZAB 对比：
//   Raft/ZAB：强一致（Leader 强推）→ 延迟低但只写 Leader
//   Redis Gossip：最终一致（节点间互相传）→ 高可用但可能短暂不一致
//   → Redis Cluster 用 Gossip 是因为简单 + Redis 场景容忍短暂不一致
```

### 2.4 故障转移（Failover）

```c
// ★ Redis Cluster 故障转移流程（面试高频）
//
// 1. 故障检测：
//    Master A 心跳超时（10s）→ 所有节点把 A 标记为 PFAIL（Possibly Fail）
//    → 超过半数 Master 确认 → 标记为 FAIL
//
// 2. 选新 Master：
//    Master A 的 Slave 们投票
//    → 选 Slave replication offset 最大的（数据最新）
//    → Epoch +1（避免脑裂）
//
// 3. 广播：
//    新 Master 广播自己成为 Master + 接管 A 的 Slots
//    → 所有节点更新 ClusterState
//
// 4. 旧 Master 回来：
//    旧 Master 发现有新 Master（Epoch 更高）
//    → 自动降级为 Slave（不强制要求手动 reset）
//    → 开始同步新 Master 数据
//
// ★ 和 Sentinel 选主对比：
//   Sentinel：第三方进程监控 + 选主
//   Cluster：**内置选主 + Gossip 同步**（无额外组件）
```

### 2.5 限制与坑点

```c
// ★ Redis Cluster 限制（面试必问！）
//
// ① 多 Key 操作限制：
//    - 必须所有 key 在同一个 Slot（用 Hash Tag {xxx}）
//    - MSET / MGET / MULTI / EXEC / EVAL 等
//    - 不同 Slot → CROSSSLOT 错误！
//
// ② Pub/Sub 广播问题：
//    - PUBLISH 消息发到所有节点（广播）
//    - 不推荐在 Cluster 里用 Pub/Sub（会搞慢集群）
//    - 用 Redis Streams / Kafka 替代
//
// ③ Lua Script 限制：
//    - EVAL/EVALSHA 如果涉及多个 key → 必须同 Slot
//    - 用 {xxx} Hash Tag 保证
//
// ④ Key 数量上限：
//    单 Slot 不限制 key 数量，但**不要让某个 Slot 特别热**
//    → 大 Key / 热 Key → 迁移 Slot 时卡住
//
// ⑤ 批量操作分片：
//    MSET k1 v1 k2 v2 ... → 客户端先按 Slot 分片
//    → 对每个 Slot 单独 MSET
//    → Lettuce/Jedis Cluster 自动做
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis Cluster Hash Slot 数？| **固定 16384**（不是 2^14？对，就是 16384）→ `CRC16(key) % 16384` |
| 2 | Hash Tag 是什么？| **{xxx} 花括号内部分算 CRC16** → 多个 key 路由到同一个 Slot → 可以跨 key 操作（MULTI/MSET）|
| 3 | MOVED vs ASK 重定向？| **MOVED（永久，迁移完成）→ 更新缓存**；**ASK（临时，迁移中）→ 不更新缓存**；客户端实现必须处理这两种 |
| 4 | Redis Cluster 用什么一致性协议？| **Gossip（最终一致）** → 每秒向随机节点 PING → 交换 ClusterState → 几秒收敛；不是 Raft/Paxos |
| 5 | Failover 流程？| Master 心跳超时 → 标记 FAIL → Slave 投票选 offset 最大的 → Epoch+1 → 广播 → 旧 Master 回来自动降级为 Slave |
| 6 | Redis Cluster vs Sentinel？| Sentinel：**第三方进程 + Master-Slave + 无分片**；Cluster：**内置选主 + 16384 Slot 分片 + 无额外组件** |
| 7 | Cluster 多 Key 操作限制？| **所有 key 必须同 Slot** → 用 Hash Tag `{xxx}` 保证；不同 Slot → `CROSSSLOT` 错误 |
| 8 | 客户端实现？| **直连模式**（Jedis Cluster / Lettuce）→ 缓存 Slot→Node 表 → MOVED 更新缓存；不是代理模式 |
| 9 | Redis Cluster 最大节点数？| **官方不超过 1000 个节点**（Gossip 复杂度 O(N²)）；生产推荐 32-64 个 |
| 10 | Cluster 槽位失衡？| redis-cli --cluster rebalance → 自动平衡 Slot；大 Key 先拆分再迁移 |
| 11 | Cluster vs Twemproxy/Codis？| **Cluster 官方推荐（直连 + 无单点）**；Codis 已停维护（代理层）；Twemproxy 也过时 |
| 12 | Cluster Pub/Sub？| **广播到所有节点** → 会搞慢集群；生产用 Redis Streams / Kafka 替代 |
| 13 | Cluster Lua Script？| EVAL/EVALSHA **多 key 必须同 Slot**；用 `{xxx}` 保证 |
| 14 | Cluster 脑裂？| Epoch（任期）机制 → 旧 Master 回来发现新 Master Epoch 更高 → 自动降级 Slave → 不会双写 |
| 15 | Cluster 写入瓶颈？| 单 Slot 写入还是单 Master → **避免热点 Key**（热 Slot 无法分片）；用 Hash Tag 但要注意分布 |
| 16 | Cluster 学习路径？| Docker 起 3 Master + 3 Slave → hash tag 实验 → MOVED/ASK 理解 → 故障转移模拟（kill 节点） |
| 17 | Cluster 扩容缩容？| redis-cli --cluster add-node / del-node → 迁移 Slot；扩容时自动平衡 |
| 18 | Cluster vs Redis Proxy？| 企业级代理（Predixy / twemproxy / Codis）：**代理层 + 分片规则自定义**；Cluster：官方方案 + Hash Slot |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Redis 7.4.x Cluster 模式  
**面试考点：18 题**
