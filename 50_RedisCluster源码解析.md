# Redis Cluster 源码深度解析（Redis 分布式集群）

> **源码基线**：Redis 7.4.x（Cluster 模式）  
> **核心仓库**：https://github.com/redis/redis  
> **面试权重**：⭐⭐⭐⭐⭐（Redis 分布式最高频）

---

## 一、Redis Cluster 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：Redis 官方分布式集群（分片 + 高可用）              │
│  解决什么问题？单节点 Redis 内存有限 + 单点故障                │
│                                                               │
│  四种 Redis 高可用方案（面试必问对比）：                       │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │ Master-Slave │ Redis Cluster  │ Twemproxy/  ││
│  │                │ + Sentinel   │                │ Codis        ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 分片           │ ❌ 不支持     │ ✅ 16384 Slot  │ ✅ 代理层     ││
│  │ 自动故障转移   │ ✅ Sentinel   │ ✅ 内置         │ ❌ 手动       ││
│  │ 额外组件       │ Sentinel 进程│ 无（内置）     │ 代理进程      ││
│  │ 数据一致性     │ 异步复制     │ 最终一致（Gossip）│ 依赖分片规则││
│  │ 运维复杂度     │ ★★ 简单      │ ★★★ 中等      │ ★★★★ 高      ││
│  │ 社区推荐       │ 小型/测试    │ ★★★★★ 生产   │ 已过时       ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ Redis Cluster 是 Redis 官方推荐的生产分布式方案！          │
│ ★ Codis 已停维护 → 用 Redis Cluster 或 Redis Proxy         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Redis Cluster 架构（★ 面试高频图解）                        │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │  Master Node 1 │ Master Node 2 │ Master Node 3 │         │
│  │ (Slot 0-5460)  │ (Slot 5461-10922) │ (Slot 10923-16383)  │ │
│  │   └─ Slave A   │   └─ Slave A   │   └─ Slave A   │         │
│  └──────────────┴──────────────┴──────────────┘             │
│                                                               │
│  ★ 固定 **16384 Hash Slot**（面试必背！不是 2^14，就是 16384）│
│  ★ 计算：slot = CRC16(key) % 16384                           │
│  ★ 每个 Master 负责一段 Slot → 可以不均匀（redis-cli rebalance）│
│  ★ 每个 Master 至少一个 Slave（故障时自动提升）                │
│  ★ Gossip 协议最终一致（不是 Raft/Paxos，简化实现）          │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Hash Slot 路由（★★★★★ 面试最高频）

```c
// ★ Redis Cluster Hash Slot 路由（面试必背）
//
// 固定 16384 个 Slot（0 ~ 16383）
// 计算公式：slot = CRC16(key) % 16384
//
// ★ Hash Tag 机制（面试高频！）
//   如果 key 包含 {xxx} → 只对花括号内部分算 CRC16
//   → 保证多个 key 路由到同一个 Slot
//   → 实现跨 key 操作（MULTI/EXEC pipeline）
//
// 示例：
//   "user:1"              → CRC16("user:1") % 16384 = 5293 → Node 1
//   "user:{123}:name"     → CRC16("123") % 16384 = 5293 → 和上一个同 Slot！
//   "user:{123}:age"      → CRC16("123") % 16384 = 5293 → 同 Slot
//   → 这两个可以 MULTI/EXEC 批量操作！
//
// ★ 客户端实现（Jedis Cluster / Lettuce）：
//   客户端缓存 Slot → Node 映射表 → 请求直连对应 Node
//   → 如果 Node 返回 MOVED/ASK → 更新缓存或临时重定向
//   → **直连模式**（不是代理模式！Codis 是代理模式）
```

### 2.2 MOVED vs ASK 重定向（★★★★ 面试高频）

```c
// ★ Redis Cluster 两种重定向（面试必问！区别是什么）
//
// ① MOVED（永久重定向）
//    场景：Slot **迁移完成**（已经从 Node A 搬到 Node B）
//    Node A 返回：-MOVED 5293 10.0.0.2:6379
//    → 客户端**更新 Slot→Node 缓存**
//    → 后续所有该 Slot 请求直接发 Node B
//
// ② ASK（临时重定向）
//    场景：Slot **迁移中**（还没完全从 Node A 搬到 Node B）
//    Node A 返回：-ASK 5293 10.0.0.2:6379
//    → 客户端**这次发 Node B，但不更新缓存！**
//    → 后续继续发 Node A（等收到 MOVED 再更新）
//
// ★ 为什么要分两种？
//   ASK = 迁移中 → 临时去新节点，但原节点还没完全放弃这个 Slot
//   MOVED = 迁移完成 → 永久换节点
//   → 保证迁移期间数据不丢失
```

### 2.3 Gossip 协议（★ 面试高频）

```c
// ★ Redis Cluster 用 Gossip 协议同步节点状态
//   不是 Raft/Paxos（简化版最终一致）
//
// 节点间 Gossip 机制：
//   每秒每个节点向**随机 5 个节点**发送 PING
//   → PONG 返回自己的 ClusterState（所有节点视图）
//   → 接收方合并 → 更新自己的视图
//   → 最终一致性（几秒内收敛）
//
// ★ 为什么用 Gossip 不用 Raft？
//   Gossip：实现简单 + 高可用 + Redis 场景容忍短暂不一致
//   Raft：强一致 + 延迟低 + 但只写 Leader（不适合 Hash Slot 多写）
//   → Redis Cluster 选 Gossip 是因为简单够用
//
// ★ 和 Raft/ZAB 对比：
//   Raft/ZAB：强一致（Leader 强推）→ 延迟低但只写 Leader
//   Redis Gossip：最终一致（节点间互相传）→ 高可用但可能短暂不一致
//   → Redis 场景容忍短暂不一致（Key-Value 最终一致即可）
```

### 2.4 故障转移（★ 面试高频）

```c
// ★ Redis Cluster 故障转移流程（面试高频）
//
// ① 故障检测：
//    Master A 心跳超时（10s）
//    → 所有节点把 A 标记为 PFAIL（Possibly Fail）
//    → 超过半数 Master 确认 → 标记为 FAIL
//
// ② 选新 Master：
//    Master A 的 Slave 们投票
//    → 选 **replication offset 最大的**（数据最新的 Slave）
//    → Epoch +1（避免脑裂，类似 Raft term）
//
// ③ 广播 + 更新：
//    新 Master 广播自己成为 Master + 接管 A 的 Slots
//    → 所有节点更新 ClusterState
//
// ④ 旧 Master 回来：
//    旧 Master 发现有新 Master（Epoch 更高）
//    → **自动降级为 Slave**（不强制手动 reset）
//    → 开始同步新 Master 数据
//
// ★ 和 Sentinel 选主对比：
//   Sentinel：第三方进程监控 + 选主（额外组件）
//   Cluster：**内置选主 + Gossip 同步**（无额外组件）
```

### 2.5 限制与坑点（★ 面试必问）

```c
// ★ Redis Cluster 限制（面试必问！坑点）
//
// ① 多 Key 操作限制：
//    - 所有 key 必须**同一个 Slot**（用 Hash Tag {xxx} 保证）
//    - MSET/MGET/MULTI/EXEC/EVAL 等跨 Slot → CROSSSLOT 错误！
//    - 解决方案：Hash Tag {order_123} 让同业务 key 同 Slot
//
// ② Pub/Sub 广播问题：
//    - PUBLISH 消息**发到所有节点**（广播）
//    - 会搞慢整个集群
//    - 生产用 Redis Streams / Kafka 替代
//
// ③ Lua Script 限制：
//    - EVAL/EVALSHA 多 key 必须同 Slot
//    - 用 Hash Tag 保证
//
// ④ Key 数量上限：
//    单 Slot 不限制 key 数量 → 但**不要让某个 Slot 特别热**
//    → 大 Key / 热 Key → 迁移 Slot 时卡住（要搬很多数据）
//
// ⑤ 节点数上限：
//    Redis 官方不超过 **1000 个节点**（Gossip 复杂度 O(N²)）
//    → 生产推荐 32-64 个
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis Cluster Hash Slot 数？| **固定 16384**（不是 2^14？对，就是 16384）→ `CRC16(key) % 16384` |
| 2 | Hash Tag 是什么？| **`{xxx}` 花括号内部分算 CRC16** → 多个 key 同 Slot → 可以跨 key 操作（MULTI/MSET）；Redis Cluster 多 key 操作的核心解决方案 |
| 3 | MOVED vs ASK 重定向？| **MOVED（永久，Slot 迁移完成 → 更新缓存）**；**ASK（临时，迁移中 → 不更新缓存）**；客户端必须处理这两种 |
| 4 | Redis Cluster 一致性协议？| **Gossip（最终一致）** → 每秒向随机节点 PING → 交换 ClusterState → 几秒收敛；不是 Raft/Paxos |
| 5 | Failover 流程？| Master 心跳超时 → 标记 FAIL → Slave 投票选 offset 最大的 → Epoch+1 → 广播 → 旧 Master 回来自动降级 Slave |
| 6 | Cluster vs Sentinel？| Sentinel：**第三方进程 + Master-Slave + 无分片**；Cluster：**内置选主 + 16384 Slot 分片 + 无额外组件**；Cluster 是官方推荐 |
| 7 | Cluster 多 Key 操作限制？| **所有 key 必须同 Slot** → CROSSSLOT 错误；用 Hash Tag 保证；Lettuce/Jedis Cluster 自动按 Slot 分片批量操作 |
| 8 | Cluster 客户端实现？| **直连模式**（不是代理）→ 缓存 Slot→Node 表 → MOVED 更新缓存；Jedis Cluster / Lettuce 实现 |
| 9 | Cluster vs Twemproxy/Codis？| Cluster：**官方推荐 + 直连 + 无单点**；Codis：已停维护（代理层）；Twemproxy：也过时；选 Cluster |
| 10 | Cluster Pub/Sub？| **广播到所有节点** → 会搞慢集群；生产用 Redis Streams / Kafka 替代 |
| 11 | Cluster 脑裂？| Epoch（任期）机制 → 旧 Master 回来发现新 Master Epoch 更高 → 自动降级 Slave → 不会双写 |
| 12 | Cluster 节点数上限？| **官方不超过 1000 个**（Gossip O(N²)）；生产推荐 32-64 |
| 13 | Cluster 扩容缩容？| `redis-cli --cluster add-node / del-node` → 自动迁移 Slot；扩容时自动平衡 |
| 14 | Cluster 槽位失衡？| `redis-cli --cluster rebalance` → 自动平衡 Slot；大 Key 先拆分再迁移 |
| 15 | Cluster vs Proxy？| 企业级代理（Predixy / twemproxy）：代理层 + 自定义分片规则；Cluster：官方 Hash Slot + 直连 |
| 16 | Cluster 写入瓶颈？| 单 Slot 还是单 Master → **避免热点 Key**；Hash Tag 能同 Slot 但不能解决热点 |
| 17 | Cluster 学习路径？| Docker 起 3 Master + 3 Slave → Hash Tag 实验 → MOVED/ASK 理解 → kill 节点看故障转移 |
| 18 | Cluster Lua Script？| EVAL/EVALSHA **多 key 必须同 Slot**；用 Hash Tag 保证 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Redis 7.4.x Cluster 模式  
**面试考点：18 题**
