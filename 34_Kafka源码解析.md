# Apache Kafka 源码深度解析（分布式消息流平台）

> **源码基线**：Apache Kafka 3.8.x（最新稳定）  
> **核心仓库**：https://github.com/apache/kafka  
> **面试权重**：⭐⭐⭐⭐⭐（消息中间件高频，和 RocketMQ 对比必问）

---

## 一、Kafka 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式事件流平台（Event Streaming Platform）       │
│  高吞吐 / 低延迟 / 持久化日志流（Log）                        │
│                                                               │
│  三大组件：                                                    │
│  Broker（消息存储节点）→ Producer（生产者）→ Consumer（消费者）  │
│                                                               │
│  Topic / Partition 模型：                                     │
│  Topic：逻辑主题（如 order-events）                           │
│  Partition：物理分片（Topic 创建时指定 N 个 Partition）       │
│  → 每个 Partition 是一个有序、不可变、可追加的消息日志（Log）    │
│  → 每个 Partition 有 1 Leader + N-1 Follower（副本）          │
│  → 同一 Topic 内消息按分区有序（不同分区间无序）               │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                        Topic                            │ │
│  │   ┌──────────┬──────────┬──────────┬──────────┐        │ │
│  │   │Partition 0│Partition 1│Partition 2│Partition 3│        │ │
│  │   │  Leader+RS │  Leader+FS │  Leader+RS │  Leader+FS │        │ │
│  │   └──────────┴──────────┴──────────┴──────────┘        │ │
│  │                                                         │ │
│  │   ConsumerGroup 内每个 Partition 只能被一个 Consumer 消费 │ │
│  │   → 10 Partition / 3 Consumer = Consumer[0,1] Consumer[2,3] ...│
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Producer 发送消息（★★★★★ 面试高频）

```java
// ★ KafkaProducer.send 完整流程（主线程 + Sender 线程双线程模型）
//
//  ① 主线程调用 producer.send(record)
//     → 拦截器链（ProducerInterceptor）
//     → 序列化（Serializer）
//     → 分区器（Partitioner）★ 选 Partition
//       → 有 key：hash(key) % partitionCount
//       → 无 key：Sticky Partitioner（粘性分区，提高批量效率）
//     → 加入 RecordAccumulator（消息累加器）
//       → 按 Partition 分组 → ProducerBatch 缓冲
//       → batch.size（默认 16KB）+ linger.ms（默认 0）控制发送时机
//       → 主线程到此返回（不等 Broker 响应，异步）
//
//  ② Sender 后台线程（单独启动）
//     → RecordAccumulator.drain(readyPartitions)
//       → 取出 ready 的 ProducerBatch（batch.size 满或 linger.ms 到）
//       → 按 Broker 分组（一个 ProducerBatch 发一个 Broker）
//     → NetworkClient.send(brokerNode, produceRequest)
//       → 建立 TCP 连接（首次）
//       → 发送 ProduceRequest（包含多个 Partition 的 batch）
//     → 收到 ProduceResponse
//       → 成功：回调 ProducerCallback.onCompletion
//       → 失败：按 max.retries 重试
//
// ★ RecordAccumulator 核心结构：
//   Map<TopicPartition, Deque<ProducerBatch>> batches
//   → 每个 Partition 一个队列
//   → ProducerBatch = MemoryRecords（序列化后的消息）
//   → batch.size=16KB → 攒够一批就发 → 提高吞吐
//
// ★ 和 RocketMQ Producer 对比：
//   Kafka：异步批量（RecordAccumulator + Sender 线程）→ 高吞吐
//   RocketMQ：同步/异步/Oneway → 灵活但吞吐略低
```

### 2.2 Broker 端处理（Kafka Server 核心）

```java
// ★ Broker 收到 ProduceRequest 处理流程
//
// 1. SocketServer（NIO 模型，基于 Java NIO Selector）
//    → Acceptor Thread：接收新连接
//    → Processor Thread（N 个，默认 CPU*2）：处理请求
//    → RequestQueue：请求入队
//
// 2. KafkaRequestHandlerPool（业务处理线程池）
//    → 从 RequestQueue 取请求
//    → 按 API Key 分发（ProduceRequest / FetchRequest / OffsetCommitRequest ...）
//
// 3. ReplicaManager（★ Partition 副本管理核心）
//    → produceRequest → appendRecords(topicPartition, records)
//      → 找到 Leader Partition
//      → 写入本地 Log（Log.append → LogSegment.append）
//      → ISR（In-Sync Replicas）同步：通知 Follower 拉取
//      → 按 acks 配置决定响应时机：
//        acks=0  → 收到请求立即响应（不保证写入）
//        acks=1  → Leader 写入本地即响应（默认）
//        acks=all → Leader + 所有 ISR Follower 都写入才响应（最安全）
//
// 4. Log 存储结构
//    → 每个 Partition 一个 Log
//    → LogSegment（.log + .index + .timeindex）：滚动文件，默认 1GB
//    → .log：消息内容（顺序追加，Append Only）
//    → .index：稀疏索引（offset → position 映射，2 个 entry 之间差 N 字节）
//    → .timeindex：时间索引（timestamp → offset 映射）
//
// ★ 和 RocketMQ CommitLog 对比：
//   Kafka：每个 Partition 独立 Log（按 Partition 有序）
//   RocketMQ：所有 Topic 共享 CommitLog（全局顺序写，性能极高）
//
// ★ Kafka Index 结构（稀疏索引）：
//   每 4096 字节写一个 index entry（offset → file position）
//   → 查 offset 先二分 index 找到近似 position → 线性扫描 .log
//   → 避免全量 .index 膨胀（如果每条消息一个 index entry 会爆）
```

### 2.3 Consumer 消费流程

```java
// ★ KafkaConsumer（单线程模型，和 Producer 双线程不同）
//
// consumer.poll(Duration.ofMillis(100)) 流程：
//
//   1. 加入 ConsumerGroup
//      → 发送 JoinGroupRequest
//      → GroupCoordinator（Broker 端组件）执行 Rebalance
//        → RangeAssignor（默认）：10 Partition / 3 Consumer 平均分配
//        → 或 CooperativeStickyAssignor（粘性分配，Rebalance 影响最小）
//
//   2. FetchRequest 批量拉取
//      → 为自己分配到的每个 Partition 发 FetchRequest
//      → FetchRequest 中带 offset（上次消费进度）
//      → Broker 从 Log.offset 开始读消息返回
//      → max.poll.records（默认 500）+ fetch.max.bytes（默认 50MB）控制批量
//
//   3. 业务处理
//      → ConsumerRecords<K,V> records = consumer.poll(...)
//      → for (ConsumerRecord<K,V> record : records) { 业务逻辑 }
//
//   4. 提交 offset
//      → enable.auto.commit=true（默认，定时提交 5s）
//      → 或手动 consumer.commitSync() / commitAsync()
//      → offset 存在 __consumer_offsets Topic（特殊 Topic，50 个 Partition）
//
// ★ 和 RocketMQ Consumer 对比：
//   Kafka：手动 poll + offset 提交（灵活，at-least-once 靠业务保证）
//   RocketMQ：Push 模式（Pull + 长轮询）+ ACK（Broker 管理 offset）
```

### 2.4 ISR（In-Sync Replicas）副本同步

```java
// ★ ISR 是 Kafka 高可用核心！
//
// Partition 副本分类：
//   Leader：处理读写请求
//   ISR Follower：和 Leader 保持同步的 Follower 集合
//   OSR（Out-of-Sync Replicas）：落后太多的 Follower（被踢出 ISR）
//
// ISR 维护规则：
//   Follower 落后 Leader 超过 replica.lag.time.max.ms（默认 30s）
//   → 踢出 ISR → 变成 OSR
//   OSR 追平 Leader → 重新加入 ISR
//
// Leader 选举：
//   Leader 挂了 → Controller 监听 ZK/ kraft event
//     → 从 ISR 中选第一个 Follower 作为新 Leader
//     → 如果 ISR 为空 → 从 OSR 选（但数据可能丢失）
//
// ★ 和 RocketMQ Master/Slave 对比：
//   Kafka：ISR 动态维护 + Controller 选举（高可用自动故障转移）
//   RocketMQ：Master/Slave 配置化 + 异步同步（5.x 开始有自动故障转移）
//
// ★ Kafka acks=all 配合 min.insync.replicas：
//   acks=all + min.insync.replicas=2
//   → 写入必须 Leader + 至少 2 个 ISR Follower 确认
//   → 保证最多 1 个 Broker 挂了数据不丢
//   → 生产推荐！
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Kafka 核心模型？| **Topic → Partition → Log（有序、不可变、追加写）**；Producer 按 Partition 发；Consumer 按 ConsumerGroup 消费 |
| 2 | Producer 双线程模型？| **主线程（send → RecordAccumulator）+ Sender 线程（drain → NetworkClient）**；主线程不等 Broker 响应，异步提高吞吐 |
| 3 | RecordAccumulator 作用？| **消息累加器** → Map<TopicPartition, Deque<ProducerBatch>> → 按 Partition 分组 → batch.size + linger.ms 控制批量发送 |
| 4 | Partition 选择策略？| **有 key：hash(key) % partitionCount → 保证同一 key 进同一 Partition**；无 key：Sticky Partitioner 粘性分区 |
| 5 | Broker Log 滚动文件结构？| **.log（消息内容）+ .index（稀疏索引，offset→position）+ .timeindex（时间索引）**；默认 1GB 滚动 |
| 6 | 稀疏索引作用？| 避免每条消息一个 index entry（爆炸式增长）→ 每 4096B 写一个 index entry → 查 offset 先二分 index → 线性扫描 .log |
| 7 | acks 三种配置？| **acks=0（不保证写入）/ acks=1（Leader 写入即响应，默认）/ acks=all（Leader + 所有 ISR Follower 确认，最安全）** |
| 8 | ISR 是什么？| **In-Sync Replicas** → 和 Leader 保持同步的 Follower 集合；落后超过 30s 踢出；Leader 挂了从 ISR 选新 Leader |
| 9 | Consumer Rebalance？| **同一个 ConsumerGroup 实例平分 Partition**；触发时机：Consumer 上下线 / Topic 扩 Partition；算法：RangeAssignor（默认）/ CooperativeStickyAssignor（影响最小）|
| 10 | offset 存哪里？| **__consumer_offsets 特殊 Topic**（50 个 Partition，Kafka 内部 Topic）；enable.auto.commit=true 定时 5s 自动提交 |
| 11 | Kafka vs RocketMQ？| Kafka：**高吞吐百万级 + 流式生态（Flink/Spark）**；RocketMQ：**顺序消息 + 事务消息 + 中文文档完善 + 延迟消息** |
| 12 | Kafka 为什么快？| **顺序追加写 + 稀疏索引 + 异步批量（RecordAccumulator）+ 零拷贝（sendfile）+ 页缓存** |
| 13 | 零拷贝？| Kafka 用 **sendfile 系统调用** → 数据从页缓存直接到网卡 → 绕过用户态 → 减少 CPU 拷贝 |
| 14 | Consumer 单线程？| 是的！KafkaConsumer 是单线程 poll 模型 → 避免复杂并发；要提高消费吞吐 → 多 Consumer 实例（同 ConsumerGroup）|
| 15 | Kafka 事务？| **Kafka Exactly-Once 语义** → Producer 幂等（enable.idempotence=true）+ 事务（transactional.id）→ 跨 Partition 原子写入 |
| 16 | 消息积压？| ① 增加 Consumer 实例（同 ConsumerGroup）② 增加 Partition 数（需要 Rebalance）③ 换更快消费逻辑 |
| 17 | Kafka 2.8+ KRaft？| **去掉 ZooKeeper 依赖** → Kafka 自带 Raft 协议做 Controller 选举；简化部署架构 |
| 18 | 消费重复？| Kafka 保证 **at-least-once** → Consumer 业务侧做幂等（唯一 ID + Redis/DB 去重）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Kafka 3.8.x  
**面试考点：18 题**
