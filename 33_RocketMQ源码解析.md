# Apache RocketMQ 源码深度解析（阿里系消息中间件）

> **源码基线**：Apache RocketMQ 5.3.x（最新稳定）  
> **核心仓库**：https://github.com/apache/rocketmq  
> **面试权重**：⭐⭐⭐⭐⭐（消息中间件高频）

---

## 一、RocketMQ 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  四大角色                                                      │
│  NameServer（注册中心）→ Broker（消息存储）→ Producer（生产者）→ Consumer（消费者）│
│                                                               │
│  ┌──────────────┐             ┌──────────────┐              │
│  │  NameServer   │             │   Broker     │              │
│  │   无状态      │ ←注册/心跳→ │ Master/Slave  │              │
│  │  路由信息缓存 │             │  消息存储/转发 │              │
│  └──────┬───────┘             └──────┬───────┘              │
│         │ 查询路由                      │ 发送/拉取              │
│    ┌────┴────┐                    ┌───┴─────┐               │
│    ↓         ↓                    ↓         ↓               │
│  Producer  Consumer           Producer  Consumer             │
│  发消息     消费消息           发消息     消费消息             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  两种消息模式                                                  │
│                                                               │
│  1. Push 模式 ★推荐！Broker 主动推                             │
│     DefaultMQPushConsumer                                     │
│       → Consumer 定时 PullAPIWrapper.pull（长轮询）            │
│       → Broker 有消息就返回（没消息最多等 30s）                │
│       → 回调 MessageListenerConcurrently.consumeMessage       │
│       → 优点：低延迟；缺点：Consumer 端并发控制                 │
│                                                               │
│  2. Pull 模式（旧，手动拉取）                                   │
│    DefaultMQPullConsumer                                      │
│      → 自己调 pull API 拉消息                                  │
│      → 优点：完全控制；缺点：开发复杂 + 延迟高                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Broker 消息存储结构（★ 面试高频！）                            │
│                                                               │
│  ① CommitLog（★ 核心！顺序写，性能高）                         │
│     → 所有 Topic 的消息统一写一个 CommitLog                    │
│     → 顺序追加写入（Append Only）→ 磁盘顺序 IO 性能极高       │
│     → 消息格式：topic + body + tags + keys + timestamp         │
│                                                               │
│  ② ConsumeQueue（消费索引）                                    │
│     → 每个 Topic-Queue 一个 ConsumeQueue 文件                  │
│     → 存 CommitLog 的 offset + size + tags（定长 20B/条）      │
│     → Consumer 拉消息时先查 ConsumeQueue 拿到 CommitLog offset │
│       → 再去 CommitLog 读消息内容                              │
│                                                               │
│  ③ IndexFile（可选索引）                                       │
│     → 按 msgKey 哈希索引 → 快速查消息                          │
│     → MySQL 主键索引风格                                       │
│                                                               │
│  ★ 三级存储 = 顺序写 CommitLog（高性能）+ 索引 ConsumeQueue（灵活）│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Producer 发送消息（★★★★★）

```java
// ★ DefaultMQProducer.send 完整流程
//
// 1. send(msg, timeout)
//    → this.send(msg, timeout, communicationMode)
//      → communicationMode 默认 SYNC（同步）
//        可选 ASYNC（异步回调）/ ONEWAY（只管发不关心结果）
//
// 2. 路由查找
//    → 从 NameServer 拉 Topic 路由（RouteManager）
//      → TopicRouteData（queueTable + brokerDatas）
//    → 本地缓存 → 30 秒刷新一次
//
// 3. 选择 Queue（SendQueueSelector）
//    → 默认 SendQueueSelectorInstance
//    → 无 key：随机选 Queue
//    → 有 key（orderId）：hash(key) % queueSize → **顺序消息！**
//
// 4. 网络发送（Netty）
//    → RemotingClient.invokeSync(brokerAddr, request, timeout)
//    → Broker Master 处理
//    → Broker 写 CommitLog + 返回 SendResult
//
// 5. 返回 SendResult
//    → sendStatus(SEND_OK)
//    → msgId（消息唯一 ID）
//    → queueId + queueOffset（Queue 内偏移）
//
// ★ 顺序消息原理（生产者端选 Queue）：
//   hash(orderId) % queueSize → 同一个 orderId 永远进同一个 Queue
//   → Consumer 消费时同一个 Queue 单线程消费 → 保证顺序
//
// ★ 事务消息：
//   ① 发送 Half Message（Broker 暂存，对 Consumer 不可见）
//   ② 执行本地事务
//   ③ 成功 → COMMIT（Broker 让消息可见）
//      失败 → ROLLBACK（Broker 删除 Half Message）
//      超时 → Broker 回查本地事务状态 → COMMIT / ROLLBACK
```

### 2.2 Push Consumer 消费（★★★★）

```java
// ★ DefaultMQPushConsumer（推荐模式！Broker 主动推）
//
// 内部其实是 **Pull + 长轮询**（不是真的 Netty 推送）
//
// 完整流程：
//
//   1. 启动：DefaultMQPushConsumer.start()
//      → 从 NameServer 拉路由
//      → RebalanceImpl.doRebalance() ★ 负载均衡分配 Queue
//        → 同一个 ConsumerGroup 的实例平分 Queue
//        → 10 个 Queue / 3 个 Consumer = Consumer1 拿 Queue[0-3]
//      → 为每个分配到的 Queue 启动 PullAPIWrapper 任务
//
//   2. PullAPIWrapper.pull（长轮询）
//      → 发 PULL 请求到 Broker
//      → 如果 Broker 有消息 → 立即返回
//      → 如果没有消息 → Broker 最多等待 30s（可配置）
//        → 期间有新消息到达 → 立即返回
//        → 超时返回空
//      → 客户端拿到 → MessageExt 列表
//
//   3. 并发消费（ConsumeMessageConcurrently）
//      → 默认 consumeMessageBatchMaxSize=1（一条一条消费）
//      → 可以设 N → 批量消费提升吞吐
//      → Consumer 提交到 ThreadPoolExecutor 并发处理
//      → 处理完返回 CONSUME_SUCCESS 或 RECONSUME_LATER
//
//   4. Broker 端处理
//      → 收到 PULL 请求
//      → 查 ConsumeQueue（从 offset 开始读）
//      → 如果有消息 → 从 CommitLog 读消息内容 → 返回
//      → 如果没消息 → 挂起请求（长轮询）→ 有新消息唤醒
//
// ★ 为什么叫 Push？
//   对开发者像 Push（Broker 主动推，Consumer 只管收）
//   底层其实是 Pull + 长轮询（Broker 不维护长连接状态，复杂度低）
```

### 2.3 Rebalance 负载均衡（★★★★）

```java
// ★ Rebalance 是 Consumer 端的核心！
//
// RebalanceImpl.doRebalance() 流程：
//   1. 从 NameServer 拉 Topic 路由
//   2. 拉 ConsumerGroup 所有在线 Consumer 实例列表
//   3. 每个 Consumer 拿到自己分配到的 Queue 列表
//      → 默认 AllocateMessageQueueAveragely（平均分配）
//      → 算法：queueIndex % consumerCount == consumerIndex
//      → 例：10 Queue / 3 Consumer → Consumer1: [0,1,2,3]; Consumer2: [4,5,6]; Consumer3: [7,8,9]
//
// ★ 什么时候触发 Rebalance？
//   ① Consumer 启动时
//   ② Broker 上下线
//   ③ Consumer 上下线
//   ④ 定时任务（20 秒一次检查）
//
// ★ Rebalance 后 offset 怎么处理？
//   Consumer 本地保存 offset（每个 Queue 当前消费进度）
//   定期同步到 Broker（offset 管理）
//   Rebalance 后如果拿到新 Queue → 从 Broker 查这个 Queue 的 offset → 继续消费
//
// ★ Rebalance 导致什么问题？
//   消费中断短暂（新 Queue 分配过来）→ 但 RocketMQ 用 offset 管理 → 不会重复/丢失
```

### 2.4 CommitLog 顺序写（★★★★★ 面试高频）

```java
// ★ CommitLog 是 RocketMQ 性能核心！
//
// 为什么顺序写快？
//   磁盘 IO 随机写：约 100~200 IOPS（7200 RPM 机械盘）
//   磁盘 IO 顺序写：约 10000+ IOPS
//   → 差 50~100 倍！
//
// RocketMQ CommitLog 设计：
//
//   ① 所有 Topic 消息统一追加写一个文件
//      → CommitLog 数据结构：Append Only（只追加不修改）
//      → 任何 Topic 消息都往当前文件尾部追加
//
//   ② 文件滚动
//      → 当前文件到默认 1GB → 创建新 commitlog-yyyymmddhhmmss.dat
//      → 老文件保留 → 定期清理
//
//   ③ 读取
//      → Consumer 不能直接查 CommitLog（不知道 offset）
//      → 先查 ConsumeQueue（按 Topic-Queue 建的索引）
//      → ConsumeQueue 存 commitLogOffset + size + tags
//      → 知道 offset 后去 CommitLog 随机定位读
//
//   ★ CommitLog 格式（每条消息固定前缀）：
//      ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┐
//      │ size │ magic│ CRC  │ flag │ qid  │ off  │ ...  │
//      │ 4B   │ 4B   │ 4B   │ 1B   │ 8B   │ 8B   │      │
//      └──────┴──────┴──────┴──────┴──────┴──────┴──────┘
//      → 定长前缀 → 快速跳过
//      → 消息体可变长（body 长度在 size 里）
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RocketMQ 四大角色？| **NameServer（注册中心，无状态）/ Broker（消息存储，Master/Slave）/ Producer（生产者）/ Consumer（消费者）** |
| 2 | Broker 消息存储三级？| **CommitLog（★ 所有 Topic 顺序写，1GB 文件滚动）+ ConsumeQueue（Topic-Queue 索引，定长 20B/条）+ IndexFile（msgKey 哈希索引）** |
| 3 | Push 模式原理？| 底层是 **Pull + 长轮询**：Consumer 发 PULL → Broker 有消息立即返回 → 没消息挂起最多 30s（期间有新消息立即返回）|
| 4 | 顺序消息怎么实现？| **Producer 选 Queue 时 hash(orderId) % queueSize** → 同一个 key 永远进同一个 Queue → Consumer 同一 Queue 单线程消费 → 保证顺序 |
| 5 | 事务消息三阶段？| **① 发送 Half Message（Broker 暂存不可见）→ ② 执行本地事务 → ③ COMMIT/ROLLBACK + Broker 回查** |
| 6 | Rebalance 是什么？| **Consumer 负载均衡分配 Queue** → 同一个 ConsumerGroup 实例平分 Queue；触发时机：启动/上下线/定时 20s；算法：平均分配 |
| 7 | CommitLog 为什么顺序写快？| **顺序追加写磁盘 ≈ 10000+ IOPS，比随机写快 50~100 倍**；缺点：不能按 Topic 直接读（要靠 ConsumeQueue 索引）|
| 8 | 消息积压怎么处理？| ① 紧急扩容 Consumer（临时多开实例）② 跳过不重要消息（maxReconsumeTimes）③ 临时写脚本用 admin 工具移到临时 Topic |
| 9 | 重复消费怎么办？| Consumer 端 **幂等处理**（业务侧保证）；RocketMQ 不保证 exactly-once（只保证 at-least-once）|
| 10 | NameServer 无状态？| **是的！** Broker 定期 10s 向所有 NameServer 发心跳；NameServer 不做选主；一个挂了其他 NameServer 继续 |
| 11 | Master/Slave 同步？| **同步双写（Broker 配置 brokerRole=SYNC_MASTER）** 或异步刷盘 + Slave 主动拉取 |
| 12 | Consumer 消费模型？| **DefaultMQPushConsumer（Push 模式，1.8.0 后推荐）** + DefaultMQPullConsumer（Pull 模式，旧版）；还有 LitePullConsumer 等 |
| 13 | 重试队列 vs DLQ？| 消费失败 → 进入 **%RETRY%GroupName** 重试队列（最多 16 次，间隔递增）→ 16 次还失败 → 进入 **%DLQ%GroupName** 死信队列 |
| 14 | Tags 怎么过滤？| Producer 发消息时 msg.setTags("A")；Consumer 订阅时 expression = "A \|\| B"；Broker 拉消息时 ConsumeQueue 存 tagsHashCode → 快速过滤 |
| 15 | RocketMQ vs Kafka？| RocketMQ：**顺序消息 + 事务消息 + 中文文档完善**；Kafka：**吞吐量更高（百万级 QPS）+ 更成熟的流式生态** |
| 16 | Broker 刷盘策略？| **SYNC_FLUSH（同步刷盘，保证消息不丢但慢）/ ASYNC_FLUSH（异步刷盘，快但可能丢）** |
| 17 | 消息轨迹 Trace？| traceTopic=true → Producer/Broker/Consumer 全链路追踪 → 类似 Sleuth 的 TraceId 跨组件传播 |
| 18 | RocketMQ 5.x 新特性？| **POP 消费模式**（Broker 主动推 + ACK）+ gRPC 新客户端 + 存储改造 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache RocketMQ 5.3.x  
**面试考点：18 题**
