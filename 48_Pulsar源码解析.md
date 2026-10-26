# Apache Pulsar 源码深度解析（云原生消息中间件）

> **源码基线**：Apache Pulsar 4.2.x（最新稳定）  
> **核心仓库**：https://github.com/apache/pulsar  
> **面试权重**：⭐⭐⭐⭐（云原生消息中间件高频）

---

## 一、Pulsar 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：云原生分布式消息中间件（RocketMQ/Kafka 替代品）    │
│  三大组件：                                                  │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   Broker       │ BookKeeper   │  ZooKeeper    │             │
│  │  无状态消息代理  │ 持久化存储    │ 元数据存储     │             │
│  │  REST API +    │ Ledger WAL   │ 命名空间/分区   │             │
│  │  TCP Dispatcher│              │ 管理           │             │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         ↓              ↓              ↓                       │
│  Producer/Consumer   Ledger（消息日志）  Topic 路由/分区信息     │
│                                                               │
│ ★ 和 Kafka / RocketMQ 架构核心区别：                          │
│  Kafka：Broker 有状态（自己存消息）→ 数据和计算耦合            │
│  RocketMQ：Broker 有状态（自己存 CommitLog）                  │
│  **Pulsar：Broker 无状态 → 存储和计算分离** ← 核心设计！      │
│  → Broker 只做路由和转发，消息存在 BookKeeper                  │
│  → 好处：Broker 可以水平扩展（无状态）；BookKeeper 专门做 WAL  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  消息模型对比                                                 │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │   Kafka        │  RocketMQ      │  Pulsar      ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 架构           │ Broker 有状态  │ Broker 有状态  │ **Broker 无状态**││
│  │ 存储引擎       │ 自身磁盘       │ 自身磁盘       │ BookKeeper   ││
│  │ 持久化模型     │ Log（文件）     │ CommitLog      │ Ledger WAL   ││
│  │ 水平扩展       │ 加 Broker      │ 加 Broker      │ 加 Broker/Bookie││
│  │ 吞吐           │ ★★★★★ 最高     │ ★★★★ 高      │ ★★★★ 高      ││
│  │ 延迟           │ ★★★★          │ ★★★★★        │ ★★★★★        ││
│  │ 分层存储       │ ❌              │ ❌              │ ✅ 冷热分离   ││
│  │ 函数计算       │ Kafka Streams   │ ❌              │ ✅ Pulsar Functions││
│  │ Schema 管理    │ ❌              │ ❌              │ ✅ Built-in  ││
│  │ 跨地域复制     │ ❌ 需 MirrorMaker│ ❌              │ ✅ Geo Replication││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Broker（消息代理）

```java
// ★ Broker 是无状态组件（Pulsar 核心设计！）
//   内部两个服务：
//   ① REST API Server：Topic 查询 + 管理操作
//   ② TCP Dispatcher：Producer/Consumer 数据传输（自定义二进制协议）
//
// Broker 写消息流程：
//   Producer → Broker（Dispatcher 接收）
//     → 创建 ManagedLedger（Topic 的消息日志句柄）
//     → BookKeeper Client → 写 Ledger（WAL 追加）
//     → 同步写入 BookKeeper 集群（Quorum 确认）
//     → 返回 Producer ack
//
// Broker 读消息流程：
//   Consumer → Broker
//     → ManagedLedger（读 Ledger）
//     → BookKeeper Client 拉 Ledger 数据
//     → 返回 Consumer
//
// ★ 为什么 Broker 无状态好？
//   ① Broker 挂了 → 立刻可以加新 Broker（无状态不需要数据恢复）
//   ② 扩展 Broker 只需要加机器（不需要迁移分片）
//   ③ BookKeeper 专门做 WAL → 存储更可靠（多副本 + 自动平衡）
```

### 2.2 BookKeeper（★ 持久化核心）

```java
// ★ BookKeeper 是 Pulsar 的 WAL 存储层（和 RocketMQ/Kafka 自存不同）
//   独立的分布式日志系统（HDFS 风格，但更低延迟）
//
// BookKeeper 存储模型：Ledger（分类账）
//   Ledger = 一段有序追加写的日志（类似 Kafka LogSegment）
//   每个 Ledger 有一个 Bookie（BookKeeper 存储节点）作为 Leader
//   + N 个 Bookie 作为 Follower（Quorum 副本）
//
// 写入流程（Bookie Client）：
//   ① 写请求到达 Bookie Leader
//   ② 本地写 journal（WAL，类似 MySQL Redo Log）
//   ③ 向其他 Bookie Follower 同步
//   ④ 收到 Quorum 确认 → 返回 ack
//
// ★ BookKeeper vs Kafka/RocketMQ 自存：
//   优点：Broker 无状态 → 灵活扩展；BookKeeper 专门优化 WAL
//   缺点：多一层组件（运维复杂）；额外网络开销（Broker → BookKeeper）
//
// ★ Ledger 滚动：
//   ManagedLedger 满了（默认 128MB）→ 创建新 Ledger
//   旧 Ledger 不变（immutable）→ 类似 Kafka LogSegment / ES Segment
```

### 2.3 Producer / Consumer 核心

```java
// ★ ProducerImpl 核心类（消息发送）
//
// 发送流程：
//   producer.send(msg)
//     → ① 路由（选 Partition）
//        RoundRobin / KeyHash / 自定义路由
//     → ② 获取 Broker 连接（TCP）
//     → ③ 打包消息（自定义二进制协议）
//     → ④ 发送到 Broker
//     → ⑤ Broker 写 BookKeeper → Quorum 确认
//     → ⑥ 返回 ack
//
// ★ ConsumerImpl 核心类（消息消费）
//
// 消费模式：
//   ① 消费模型：Exclusive（独占）/ Shared（共享）/ Failover（自动故障转移）
//   ② 订阅模式：多 Topic / 多 Partition
//   ③ Offset 管理：BookKeeper 持久化（不丢）
//
// ★ Pulsar 的 Reader 模式：
//   类似 Kafka Consumer + 可以从头开始读
//   → 适合回溯消费场景
```

### 2.4 Geo Replication（跨地域复制，★ 特色）

```java
// ★ Pulsar 跨地域复制（Geo Replication）是特色！
//   Kafka MirrorMaker 是独立工具；Pulsar 内置
//
// 架构：
//   Region A Broker ←→ Geo Replication Plugin → Region B Broker
//
// 三种模式：
//   ① Async（异步）：本地 Broker 先 ack → 异步复制 → 低延迟但可能丢
//   ② Sync（同步）：等跨地域复制完成再 ack → 强一致但高延迟
//   ③ Geo Sync（混合）：本地 Quorum ack 后立即 ack → 后台异步复制
//                       → 适合需要低延迟但保证数据到达的场景
//
// ★ 为什么 Pulsar Geo Replication 比 Kafka 好？
//   Kafka MirrorMaker 是独立进程 + 需要额外存储
//   Pulsar：Broker 内置 Geo Replication Plugin → 轻量 + 透明
```

### 2.5 Pulsar Functions（★ Serverless 消息处理）

```java
// ★ Pulsar Functions 是消息中间件原生 Serverless（类似 Kafka Streams）
//
// 场景：不用自己写 Consumer + 部署应用 → 直接在 Broker 上跑 Function
//   输入 Topic → Function（Java/Python/Go）→ 输出 Topic
//   → 类似 AWS Lambda 但基于消息驱动
//
// 示例：
//   PulsarFunction<OrderEvent, Result> function = (event, context) -> {
//       // 处理订单事件
//       Result result = process(event);
//       return result;  // 发到输出 Topic
//   };
//
// ★ 和 Kafka Streams 对比：
//   Kafka Streams：需要独立部署应用 + Kafka 集群
//   Pulsar Functions：Broker 内置（轻量）+ 支持 Go
//   → 但 Kafka Streams 功能更全（窗口/Join/聚合）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Pulsar 核心架构？| **Broker（无状态，REST + TCP Dispatcher）+ BookKeeper（WAL 持久化）+ ZooKeeper（元数据）**；**Broker 无状态 → 存储和计算分离** |
| 2 | BookKeeper 作用？| **WAL 持久化层**（Ledger 追加写日志）→ 多副本 Quorum → 类似 Kafka Log 但专门做 WAL |
| 3 | Pulsar vs Kafka 架构？| Pulsar：**Broker 无状态 + BookKeeper WAL**；Kafka：Broker 有状态 + 自存磁盘 → Pulsar 扩展更灵活但运维复杂 |
| 4 | Geo Replication？| **内置跨地域复制**（Async/Sync/GeoSync 三种模式）；Kafka 要 MirrorMaker 独立工具 |
| 5 | Pulsar Functions？| **Broker 内置 Serverless** → 输入 Topic → Function（Java/Python/Go）→ 输出 Topic；类似 Kafka Streams 但更轻量 |
| 6 | Producer 路由？| RoundRobin（默认）/ KeyHash（顺序）/ 自定义路由；类似 Kafka Partition 选择 |
| 7 | Consumer 消费模式？| **Exclusive（独占）/ Shared（共享）/ Failover（自动故障转移）**；Reader 模式可以从头读 |
| 8 | Pulsar Schema？| **内置 Schema 管理**（JSON/Avro/Protobuf）→ Producer/Consumer 自动做序列化校验；Kafka 无内置 Schema |
| 9 | Pulsar 分层存储？| **热数据在 BookKeeper SSD + 冷数据自动归档到 S3/GCS**（Kafka/RocketMQ 无此功能） |
| 10 | 为什么 Pulsar 云原生？| **Broker 无状态**（K8s 上自动伸缩）+ 分层存储（冷热分离省钱）+ Geo Replication（全球部署）+ Schema（兼容多语言）|
| 11 | Ledger 滚动？| ManagedLedger 满 128MB → 创建新 Ledger → 旧 Ledger immutable → 类似 Kafka LogSegment / ES Segment |
| 12 | Pulsar vs RocketMQ？| 都是云原生友好；RocketMQ：事务消息 + 顺序消息 + 中文社区；Pulsar：**Broker 无状态 + 分层存储 + Geo Replication** |
| 13 | Bookie Leader/Follower？| Ledger 写入：Bookie Leader 接收请求 → 本地写 journal → 同步 Follower → Quorum ack 返回 Producer |
| 14 | Pulsar 适合什么场景？| 金融级可靠性（Geo Replication）+ 多语言生态（Schema）+ 云原生（Broker 无状态自动伸缩）|
| 15 | Pulsar 学习路径？| Producer/Consumer API → BookKeeper 理解 → Geo Replication → Schema → Functions |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Pulsar 4.2.x  
**面试考点：15 题**
