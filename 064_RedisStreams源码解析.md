# Redis Streams 源码深度解析（Redis 5.0+ 消息队列）

> **源码基线**：Redis 7.4（最新稳定）  
> **核心仓库**：https://github.com/redis/redis  
> **面试权重**：⭐⭐⭐⭐（Redis 进阶高频）

---

## 一、Redis Streams 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：Redis 内置消息队列（Redis 5.0+ 新增）             │
│  为什么比 Pub/Sub 好？                                       │
│  Pub/Sub：不持久化 → Consumer 离线消息丢失                   │
│  Streams：持久化 + Consumer Group + ACK + 死信队列           │
│  → 接近 Kafka 功能，但 Redis 单节点性能更高                   │
│                                                               │
│  和其他消息队列对比：                                          │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │ Redis Streams │ RabbitMQ      │ Kafka         ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 依赖           │ 无（Redis 自带）│ 独立 Broker   │ 独立 Broker + ZK││
│  │ 持久化         │ ✅ AOF/RDB     │ ✅            │ ✅            ││
│  │ Consumer Group │ ✅            │ ✅ 队列模型    │ ✅ Partition   ││
│  │ 死信队列       │ ✅ XCLAIM     │ ❌            │ ❌            ││
│  │ 吞吐量         │ ★★★★★ 极高   │ ★★★ 中等      │ ★★★★★ 最高   ││
│  │ 持久容量       │ 受 Redis 内存 │ 磁盘          │ 磁盘          ││
│  │ 运维复杂度     │ ★ 极低       │ ★★★★ 高      │ ★★★★ 高      ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ 生产场景：低消息量（< 10w/s）+ 已有 Redis 基础设施 → 选 Streams│
│ ★ 高频场景：高消息量（100w/s+）+ 需要持久化 → 选 Kafka/RocketMQ│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Stream 核心数据结构                                          │
│                                                               │
│  Stream = Append Only Log（有序追加日志）                    │
│  ├── Stream Entry（消息条目）                                │
│  │   ├── ID（时间戳 + 序列号：1709876543210-0）             │
│  │   └── Field-Value Pairs（{"name":"张三","age":"20"}）    │
│  └── Consumer Group（消费者组）                               │
│      ├── Consumer（消费者）                                  │
│      ├── PEL（Pending Entries List，待确认消息）             │
│      └── Last Delivered ID（已消费到的位置）                 │
│                                                               │
│ ★ 每个 Stream 可配 MAXLEN（最大消息数，超出 FIFO 删除旧的）   │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 写入（XADD）

```c
// ★ XADD 写入消息核心流程
//
// XADD mystream MAXLEN 10000 * name 张三 age 20
//
// ① 生成 Entry ID
//    * → 当前时间戳 + 序列号 → 1709876543210-0
//    手动 → 可以指定 ID，但必须递增
//    后缀 -0/-1/... → 同一毫秒内的序列号
//
// ② 写 Radix Tree + ListPack
//    Redis Stream 底层是 Radix Tree（前缀树）
//    Key = Stream Name
//    Value = ListPack（紧凑列表，存所有 Entry）
//    → 类似 Redis 其他数据结构（Hash/List 底层也用 ListPack）
//
// ③ 更新 Consumer Group 游标
//    如果有 Consumer Group
//    → 更新 Last Delivered ID
//
// ④ 持久化
//    写 Redis 内存 → AOF 追加 → RDB 定期快照
//    和 Redis 其他数据结构一样的持久化
//
// ★ MAXLEN 近似裁剪（面试高频）：
//   XADD mystream MAXLEN ~ 10000
//   ~ 表示近似（不精确）→ Redis 可以批量删除旧消息
//   → 比精确 MAXLEN 快很多（O(1) vs O(N)）
//   → 生产强烈推荐用 ~
```

### 2.2 Consumer Group 消费（★ 核心）

```c
// ★ Consumer Group 是 Redis Streams 最重要的功能（面试高频）
//
// 创建 Consumer Group：
//   XGROUP CREATE mystream mygroup $
//   $ → 从最新消息开始消费（也可指定具体 ID）
//
// 消费消息：
//   XREADGROUP GROUP mygroup consumer1 COUNT 10 STREAMS mystream >
//   → consumer1 从 mygroup 消费 mystream 的新消息
//   → 每条返回：ID + Field-Value
//   → 自动加入 PEL（Pending Entries List）
//
// ★ PEL（Pending Entries List，面试高频！）：
//   消息投递给 Consumer 后进入 PEL
//   → 表示"这个消息被投递了，但还没 ACK"
//   → 防止 Consumer 挂了消息丢失
//
// 确认消息（ACK）：
//   XACK mystream mygroup 1709876543210-0
//   → 消息从 PEL 删除
//
// 自动死信转移（XCLAIM）：
//   XCLAIM mystream mygroup consumer2 6000 1709876543210-0
//   → 消息投递超过 6000ms（6s）没被 ACK
//   → 转移给 consumer2（其他消费者处理）
//   → 类似 RocketMQ 重试队列 → 死信队列
//
// ★ Consumer Group 核心特性：
//   ① 同一 Group 内：消息只投递给一个 Consumer（类似 Kafka Partition）
//   ② 不同 Group 之间：独立消费（每个 Group 都能看到所有消息）
//   ③ 消息持久化：Stream 存在 Redis 内存/磁盘，Consumer 离线不丢消息
```

### 2.3 死信处理 + 监控

```c
// ★ 监控 Stream 状态：
//
// XLEN mystream           → Stream 总消息数
// XINFO STREAM mystream   → 详细信息（first ID / last ID / 总长度 / 组数）
// XINFO GROUPS mystream   → Consumer Group 列表
// XINFO CONSUMERS mystream mygroup → Group 内 Consumer 列表
// XINFO PEL mystream mygroup consumer1 → PEL 列表（待确认消息）
// XINFO DELAYED mystream mygroup → 被 XCLAIM 转移的消息
//
// ★ XAUTOCLAIM（Redis 6.2+，自动转移死信）：
//   XAUTOCLAIM mystream mygroup consumer2 6000 COUNT 10 STREAMS mystream
//   → 自动扫描 PEL → 超时消息转移给 consumer2
//   → 比手动 XCLAIM 更方便
//
// ★ Trimming 三种方式：
//   ① XADD MAXLEN N / MINID id（写入时 FIFO 删除）
//   ② XTRIM mystream MAXLEN N（显式裁剪）
//   ③ XADD + ~ 近似裁剪（推荐，批量删除）
//
// ★ Stream vs Redis List（LPUSH/RPOP）：
//   List：简单队列（POP 后消失，无法 Consumer Group）
//   Stream：消息队列（持久 + Consumer Group + ACK + 死信）
//   → List 是 FIFO（先入先出）；Stream 是 Log（可回读历史）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis Streams 是什么？| **Redis 5.0+ 内置消息队列** → Append Only Log + Consumer Group + PEL + XCLAIM 死信；比 Pub/Sub 持久化好 |
| 2 | Stream Entry ID？| **时间戳 + 序列号**（如 1709876543210-0）；同一毫秒内用 -0/-1/-2 递增；可用 * 让 Redis 自动生成 |
| 3 | Consumer Group 作用？| **消息投递给 Group 内一个 Consumer + PEL 待确认 + 死信转移**；类似 Kafka Consumer Group；不同 Group 独立消费 |
| 4 | PEL（Pending Entries List）？| **消息投递给 Consumer 后进入 PEL** → 未 ACK 期间一直在；Consumer 挂了不丢消息；XACK 后删除 |
| 5 | XACK / XCLAIM / XAUTOCLAIM？| XACK：确认消息（PEL 删除）；XCLAIM：超时消息转移给其他 Consumer；**XAUTOCLAIM（6.2+）：自动扫描 PEL 转移** |
| 6 | MAXLEN 近似裁剪（~）？| XADD **MAXLEN ~ 10000** → 近似裁剪（O(1) 批量删除）；精确 MAXLEN 是 O(N)；生产推荐 ~ |
| 7 | XREADGROUP 和 XREAD 区别？| XREADGROUP：**有 Group 上下文**（PEL + Consumer 归属）；XREAD：无 Group（每次从头读/从上次读） |
| 8 | Redis Streams vs Kafka？| Streams：**低延迟 + 低运维 + 小容量（内存限制）**；Kafka：**高吞吐 + 磁盘 + 分布式 + 高运维**；按消息量级选 |
| 9 | Stream 底层存储？| **Radix Tree（前缀树）+ ListPack** → Stream Name → Entry List；和 Redis Hash 底层类似 |
| 10 | Stream 持久化？| **Redis 标准持久化**（AOF + RDB）；和 String/List 一样；不是单独的 WAL |
| 11 | Stream vs Pub/Sub？| Pub/Sub：**不持久化 + Consumer 离线丢消息 + 无 ACK**；Stream：**持久化 + Consumer Group + PEL + ACK + 死信** |
| 12 | Stream vs RabbitMQ？| Streams：**简单 + 无 Broker + 高性能**；RabbitMQ：**AMQP 协议 + 死信队列 + 路由规则**；Streams 适合轻量场景 |
| 13 | Stream 最大容量？| 受 Redis 内存限制（maxmemory）→ 用 MAXLEN 控制 Stream 大小 → 磁盘模式 Redis 可以到 GB 级 |
| 14 | Stream 监控？| XLEN / XINFO STREAM / XINFO GROUPS / XINFO CONSUMERS / XINFO PEL → 完整监控链路 |
| 15 | Stream 学习路径？| XADD → XREADGROUP → XACK → XCLAIM → Consumer Group 场景（订单创建 → 支付 → 通知）→ MAXLEN 裁剪 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Redis 7.4  
**面试考点：15 题**
