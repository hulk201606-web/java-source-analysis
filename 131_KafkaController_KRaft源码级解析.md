# Apache Kafka Controller + KRaft 源码级深度解析

> **源码基线**：Kafka 3.8.x（Scala + Java）  
> **核心仓库**：apache/kafka → core/src/main/scala/kafka/controller/ + metadata/src/main/java/org/apache/kafka/controller/  
> **面试权重**：⭐⭐⭐⭐⭐（去 ZooKeeper 后的新控制面，2024-2025 最高频）

---

## 一、Kafka 控制面演进

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Kafka 控制面三代演进（面试必背！）                         │
│                                                               │
│  第一代：ZooKeeper 模式（0.x ~ 2.x）                         │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Controller（从 Broker 中选举）                         │   │
│  │   ↓ 读写元数据                                          │   │
│  │ ZooKeeper（/brokers, /controller, /admin, ...）       │   │
│  │   ↓ Watch 通知                                          │   │
│  │ 所有 Broker                                             │   │
│  └───────────────────────────────────────────────────────┘   │
│  问题：                                                        │
│   ① Controller 元数据缓存和 ZK 双写，容易不一致              │
│   ② 故障转移慢（Controller 重连 ZK + 全量加载元数据，分钟级）│
│   ③ ZK 集群规模瓶颈（百万 Partition 扛不住）                 │
│   ④ 运维两套系统（Kafka + ZK）                              │
│
│  第二代：KRaft 模式（Kafka 2.8 预览，3.3 生产可用）          │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Controller 节点（专用或混合）                          │   │
│  │   内置 Raft（KRaft 协议，基于 Apache Raft 论文）        │   │
│  │   元数据日志（Metadata Log，本质是一个内部 Topic）      │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│  第三代：KRaft GA（Kafka 3.3+，3.8 完全移除 ZK 代码）        │
│  ★ Kafka 4.0 将彻底删除 ZooKeeper 支持                        │
│
│ ★ KRaft = Kafka Raft                                         │
│   用 Raft 协议管理集群元数据，元数据以事件日志形式存储       │
│   → 元数据就是一条只追加的 Raft Log（__cluster_metadata）    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、KRaft 架构核心

### 2.1 Controller + Broker 角色

```
┌─────────────────────────────────────────────────────────────┐
│  ★ KRaft 集群节点角色（面试高频！）                          │
│                                                               │
│  process.roles 配置三种角色：                                 │
│   process.roles=broker        → 纯 Broker                    │
│   process.roles=controller    → 纯 Controller               │
│   process.roles=broker,controller → 混合模式（小规模用）     │
│   （不配置 process.roles → 认为是 ZK 模式，3.x 兼容）        │
│
│  专用部署（生产推荐，大规模）：                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │ Controller │  │ Controller │  │ Controller │  Raft Quorum│
│  │  (Active)  │←→│  (Standby) │←→│  (Standby) │  3/5 节点   │
│  └─────┬──────┘  └────────────┘  └────────────┘            │
│        │ Metadata Log（Raft 复制）                            │
│        ↓                                                      │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ Broker 1  │ Broker 2  │ Broker 3  │  数据面（只管消息）   │
│  └──────────┴──────────┴──────────┘                         │
│
│ ★ 和 ZK 模式最大区别：                                        │
│   Broker 不再向 Controller 拉取元数据 RPC                    │
│   → 而是作为 Raft Learner **直接复制 Metadata Log**          │
│   → 每个 Broker 本地都有完整元数据副本（最终一致）           │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 元数据即日志（Metadata as Log）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ KRaft 核心设计：元数据事件溯源（Event Sourcing）          │
│                                                               │
│  内部 Topic：__cluster_metadata（单分区多副本）              │
│                                                               │
│  Log 里的每条记录都是一个元数据事件：                         │
│  ┌──────┬──────────────────────────────────────────┐        │
│  │ Offset│ Record                                    │        │
│  ├──────┼──────────────────────────────────────────┤        │
│  │ 0    │ RegisterBrokerRecord(brokerId=1, ...)     │        │
│  │ 1    │ RegisterBrokerRecord(brokerId=2, ...)     │        │
│  │ 2    │ TopicRecord(name=order, id=1001, CREATE)  │        │
│  │ 3    │ PartitionRecord(topic=1001, p0, leader=1) │        │
│  │ 4    │ PartitionRecord(topic=1001, p1, leader=2) │        │
│  │ 5    │ TopicRecord(name=order, id=1001, DELETE)  │        │
│  └──────┴──────────────────────────────────────────┘        │
│                                                               │
│  Controller/Broker 启动时：                                   │
│   从 Offset 0 开始 Replay 所有事件 → 在内存中重建元数据镜像   │
│   （MetadataImage / MetadataCache）                          │
│                                                               │
│  之后：增量 Apply 新事件 → 更新内存镜像                      │
│  + 定期 Snapshot 压缩日志（避免日志无限增长）                │
│
│ ★ 面试高频：为什么这样设计？                                  │
│   ① Raft 天然保证元数据强一致 + 高可用                       │
│   ② Broker 复制日志 = 元数据推送，无 Controller RPC 风暴    │
│   ③ 事件重放可重建任意时刻状态（Snapshot + Log）            │
│   ④ 支撑百万 Partition（ZK Watch 风暴问题彻底消失）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Controller 源码级

### 3.1 QuorumController 核心

```java
// ★ KRaft Active Controller 核心（源码级）
//
// // metadata/src/main/java/org/apache/kafka/controller/QuorumController.java
//
// public class QuorumController implements Controller {
//
//     // ★ 单线程事件队列：所有元数据操作串行化（无锁设计！）
//     private final Queue<ControllerEvent> queue;
//
//     // ★ 元数据镜像（Replay 日志构建的内存状态）
//     private final MetadataImage image;
//
//     // ★ Raft 客户端（提交元数据记录）
//     private final RaftClient raftClient;
//
//     // ★ 各功能子控制器（Composition 组合）
//     private final ReplicationControlManager replication;  // Topic/Partition/ISR
//     private final ClusterControlManager cluster;          // Broker 注册/心跳
//     private final ConfigurationControlManager configuration;
//     private final FeatureControlManager features;
//
//     // ★ 创建 Topic 的处理链路
//     public CompletableFuture<CreateTopicsResponseData> createTopics(
//             CreateTopicsRequestData request) {
//         ControllerResult<CreateTopicsResponseData> result =
//             replication.createTopics(request, ...);
//
//         // ★ 把元数据变更打包成 Raft 记录提交
//         // 不是直接改内存！而是先写 Raft Log
//         long offset = appendRecords(result.records());
//
//         // 等 Raft Commit 后 → Apply → Future 完成
//         return result.response();
//     }
//
//     // ★ Raft Log Commit 后回调：Apply 记录到内存镜像
//     public void applyCommittedRecords(List<ApiMessageAndVersion> records) {
//         for (ApiMessageAndVersion record : records) {
//             replay(record);  // 每条记录更新对应的内存状态
//         }
//     }
// }
//
// ★ 关键设计：写流程 = 生成事件 → 提交 Raft → Commit 后 Apply
//   Active Controller 只有一个，所有写操作走它的单线程队列
//   → 天然串行，不需要锁
//
```

### 3.2 Controller 选举

```java
// ★ KRaft Leader 选举（基于 Raft，源码级）
//
// // raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java
//
// // Controller 节点和 Broker 都参与 Raft 协议
// // 但只有 Voter（Controller 节点）有投票权
// // Broker 以 Learner 身份复制日志（不投票）
//
// // 选举状态机
// enum LeaderState {
//     UNATTACHED,   // 还没跟上 Leader
//     ATTRACTED,    // 已连接 Leader
//     CANDIDATE,    // 候选（发起投票）
//     LEADER,       // Leader（Active Controller）
//     FOLLOWER      // Follower（Standby Controller）
// }
//
// // ★ 选举触发（和标准 Raft 一致 + PreVote 优化）
// // ① Follower electionTimeout（随机化，默认 1-2s）没收到心跳
// // ② 先 PreVote（不增加 Term 探测能否当选）
// // ③ 收到多数 Voter 支持 → Term+1 正式投票
// // ④ 多数票 → 成为 Active Controller
//
// ★ KRaft 选举 vs ZK 选举（面试高频）：
//   ZK 模式：Controller 在 Broker 里选（/controller 临时节点）
//            Broker 还要连 ZK，双层选举
//   KRaft：Controller 集群直接 Raft 选举（Term + RequestVote）
//          故障转移秒级（不需要重新加载 ZK 全量元数据）
//
```

---

## 四、Broker 元数据同步源码级

### 4.1 Broker 作为 Raft Learner

```java
// ★ Broker 元数据发布机制（源码级，面试核心！）
//
// // metadata/src/main/java/org/apache/kafka/image/MetadataPublisher.java
//
// public interface MetadataPublisher {
//     // 元数据镜像更新后回调
//     void publish(MetadataDelta delta, MetadataImage newImage);
// }
//
// // Broker 端核心：MetadataLoader（加载/跟随元数据日志）
// // metadata/src/main/java/org/apache/kafka/metadata/MetadataLoader.java
//
// public class MetadataLoader implements RaftClient.Listener<ApiMessageAndVersion> {
//
//     // ★ 新的 Raft 记录 Commit → 批量 Apply → 发布新镜像
//     @Override
//     public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
//         MetadataDelta delta = new MetadataDelta(image);
//         while (reader.hasNext()) {
//             ApiMessageAndVersion record = reader.next();
//             delta.replay(record);  // 增量重放
//         }
//         MetadataImage newImage = delta.apply();
//
//         // ★ 原子切换镜像 + 通知所有 Publisher
//         for (MetadataPublisher publisher : publishers) {
//             publisher.publish(delta, newImage);
//         }
//         this.image = newImage;
//     }
// }
//
// ★ Broker 拿到新镜像后更新：
//   ① Partition 状态（谁是 Leader、ISR 列表）→ 更新本地副本角色
//   ② Topic 配置（删除标记、参数）
//   ③ Broker 存活列表
//   ④ ACL / 配额 / 动态配置
//
// // Broker 发现自己被选为某 Partition 的新 Leader：
// // → makeLeaders() / makeFollowers() 切换本地副本状态
//
```

### 4.2 Leader 选举（Partition 级别）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Partition Leader 选举（KRaft 下的变化，面试高频！）       │
│                                                               │
│  ZK 模式：                                                    │
│   Broker 上的 Controller 监听 ISR 变化                       │
│   Leader 挂了 → Controller 从 ISR 选新 Leader                │
│   → 写 ZK /controller_epoch + 通知 Broker                   │
│
│  KRaft 模式：                                                 │
│   Broker 检测到 Leader 失败 → 向 Active Controller 请求      │
│   Controller 在 Raft 日志中写 PartitionChangeRecord          │
│   → 日志 Commit 后所有 Broker 看到新 Leader                  │
│
│  // replication 控制管理器中的选举逻辑                         │
│  // ReplicationControlManager.java                            │
│  // ① 只能从 ISR 中选新 Leader（unclean 关闭时）             │
│  // ② 生成 PartitionChangeRecord(partitionId, newLeader, newIsr)│
│  // ③ 提交 Raft → 原子生效                                   │
│
│ ★ ISR 变更也走元数据日志：                                    │
│   Broker 副本落后/追上 → AlterIsr 请求 → Controller          │
│   → PartitionChangeRecord 更新 ISR → Raft Commit            │
│   → 比 ZK 模式的 ISR 通知可靠得多（不会丢 ISR 变更）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、Snapshot 机制

```
┌─────────────────────────────────────────────────────────────┐
│  ★ KRaft Metadata Snapshot（元数据快照，面试高频）           │
│                                                               │
│  问题：元数据日志无限增长，新节点/重启节点从头 Replay 太慢    │
│                                                               │
│  解决（和 Raft Snapshot 一致）：                              │
│   Active Controller 定期把当前内存镜像序列化成 Snapshot      │
│   Snapshot 之前的日志可以被压缩删除                          │
│                                                               │
│  新节点加入流程：                                             │
│   ① 先拉最新 Snapshot（一次拿到完整元数据镜像）              │
│   ② 再从 Snapshot 之后的 Offset 复制增量日志                 │
│   → 快速追上集群（秒级）                                     │
│
│  // metadata/.../MetadataImageShutdownWriter / SnapshotWriter
│  // 触发条件（默认）：                                         │
│  //   metadata.log.snapshot.record.count（日志记录数阈值）   │
│  //   Controller 优雅停机时也会生成 Snapshot                 │
│
│ ★ KRaft Snapshot vs ZK：                                      │
│   ZK 模式 Controller 故障转移要从 ZK 全量读元数据（分钟级）  │
│   KRaft 用 Snapshot + 增量日志（秒级故障转移）              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、KRaft vs ZooKeeper 全景对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ KRaft vs ZooKeeper 面试必背对比表                         │
│                                                               │
│  ┌──────────┬────────────────┬────────────────┐             │
│  │ 维度       │ KRaft           │ ZooKeeper      │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ 元数据存储  │ Raft Log        │ ZK ZNode 树     │             │
│  │            │ __cluster_meta  │                │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ 一致性协议  │ Raft（内置）    │ ZAB            │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ Broker 获取 │ Raft Learner   │ Controller RPC  │             │
│  │ 元数据     │ 复制日志（推送）  │ + ZK Watch     │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ 故障转移    │ 秒级（Snapshot）│ 分钟级（全量加载）│             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ Partition  │ 200w+（官方）   │ 20w 量级        │             │
│  │ 规模上限    │                │                │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ 运维组件    │ 只有 Kafka      │ Kafka + ZK     │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ ISR 变更    │ Raft Log 原子   │ ZK 写入+通知    │             │
│  ├──────────┼────────────────┼────────────────┤             │
│  │ Controller │ 独立 controller  │ Broker 兼任     │             │
│  │ 部署       │ 角色（可混合）   │                │             │
│  └──────────┴────────────────┴────────────────┘             │
│
│ ★ 官方数据（KRaft 优势）：                                    │
│   ① Partition 数量提升 10x+（支持 200 万分区）               │
│   ② Controller 故障恢复从分钟级 → 秒级                       │
│   ③ 元数据传播延迟大幅降低（日志复制 vs Watch 回调）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | KRaft 是什么？| **Kafka Raft**：用内置 Raft 协议管理元数据，替代 ZooKeeper |
| 2 | Kafka 控制面演进？| ZK 模式 → KRaft 预览（2.8）→ KRaft GA（3.3）→ 4.0 彻底移除 ZK |
| 3 | ZK 模式四大问题？| Controller/ZK 双写不一致 + 故障转移慢（分钟级）+ ZK 规模瓶颈 + 两套系统运维 |
| 4 | process.roles？| broker / controller / broker,controller（混合）；不配置 = ZK 模式 |
| 5 | 生产 Controller 部署？| **专用 controller 节点 3/5 个**（Raft Quorum），大规模不和 Broker 混合 |
| 6 | 元数据存在哪？| 内部 Topic **__cluster_metadata**（单分区多副本 Raft Log）|
| 7 | 元数据即日志？| **Event Sourcing**：RegisterBroker/Topic/PartitionChange 等事件追加日志，Replay 重建状态 |
| 8 | Active Controller 核心？| **QuorumController**：单线程事件队列串行处理 + 无锁设计 |
| 9 | Controller 写流程？| 生成元数据记录 → 提交 Raft Log → Commit 后 Apply 更新内存镜像 |
| 10 | Controller 选举？| Controller 节点 Raft 选举（RequestVote + PreVote + Term），Broker 不投票 |
| 11 | Broker 怎么拿元数据？| 作为 **Raft Learner 复制 Metadata Log**，MetadataLoader 增量 Apply 更新 MetadataImage |
| 12 | 为什么 Broker 用 Learner？| 只复制不投票，不影响 Controller Quorum |
| 13 | KRaft 故障转移快？| Snapshot + 增量日志，新 Active 秒级就绪；ZK 模式要全量读 ZNode |
| 14 | Snapshot 作用？| 内存镜像序列化快照；新节点先拉 Snapshot 再追增量日志；压缩旧日志 |
| 15 | Partition Leader 怎么选？| Broker 请求 → Active Controller 写 PartitionChangeRecord 到 Raft → Commit 后全体生效 |
| 16 | ISR 变更？| AlterIsr 请求 → Controller → **PartitionChangeRecord 原子提交 Raft Log** |
| 17 | KRaft vs ZK 规模？| KRaft 官方支持 **200w+ Partition**；ZK 约 20w 量级 |
| 18 | 面试最高频考点排序？| **1. 元数据即日志 2. Broker Learner 同步 3. 选举与故障转移 4. vs ZK 对比 5. Snapshot** |
| 19 | 学习路径？| QuorumController → KafkaRaftClient 选举 → MetadataLoader/Image → ReplicationControlManager → Snapshot |
| 20 | 面试常见追问？| Broker 本地元数据一致吗？→ Raft Commit 后最终一致；Controller 全挂？→ 集群元数据只读但数据收发仍可继续 |
| 21 | Controller 全挂影响？| 无法创建 Topic/选举等元数据操作；已有 Producer/Consumer 数据读写不受影响（Broker 有元数据镜像）|
| 22 | KRaft 用的是标准 Raft 吗？| 基于 Raft 论文的 Kafka 实现，含 PreVote、日志压缩、Learner 等工程优化 |
| 23 | MetadataImage 是什么？| Replay 日志构建的内存只读元数据镜像；原子切换；MetadataPublisher 订阅更新 |
| 24 | 面试陷阱？| 说 KRaft 把元数据存在 Kafka 数据 Topic 里 → ❌ 是专用内部 Topic __cluster_metadata，走独立 Raft Quorum |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：metadata/src/main/java/org/apache/kafka/controller/QuorumController.java + raft/KafkaRaftClient.java  
**面试考点：24 题**  
**差异化点**：三代演进 + 元数据即日志（Event Sourcing）+ Broker Learner 同步 + Snapshot 秒级故障转移 + vs ZK 全景
