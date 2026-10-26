# Apache Kafka Rebalance + Consumer Group 协调协议源码级深度解析

> **源码基线**：Kafka 3.8.x（Java）  
> **核心仓库**：apache/kafka → clients/src/main/java/org/apache/kafka/clients/consumer/ + group-coordinator/  
> **面试权重**：⭐⭐⭐⭐⭐（Kafka 消费端最高频，重平衡是生产头号痛点）

---

## 一、Consumer Group 核心模型

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Consumer Group = 消费组（水平扩展 + 容错）                │
│                                                               │
│  Topic-A: P0  P1  P2  P3（4 个分区）                         │
│     ↑      ↑   ↑   ↑                                          │
│  ┌──────┐┌──────┐┌──────┐                                    │
│  │ C1   ││ C2   ││ C3   │  同组 3 个消费者                    │
│  │ P0,P1││ P2   ││ P3   │  每个分区只被组内一个消费者消费     │
│  └──────┘└──────┘└──────┘                                    │
│                                                               │
│  ★ 两条铁律（面试必背）：                                     │
│   ① 一个 Partition 只能被同一个 Group 里的一个 Consumer 消费 │
│   ② 一个 Consumer 可以消费多个 Partition                     │
│                                                               │
│  ★ 消费者数 > 分区数：多余消费者空闲（分不到分区）           │
│    → 扩容消费者不能超过分区数                                │
│                                                               │
│  ★ Group Coordinator（协调者）：                             │
│   Broker 端一个特殊组件（KRaft 模式在 Controller 里）        │
│   管理 Group 成员 + 分区分配 + offset 提交                  │
│   内部 Topic：__consumer_offsets（存消费位移）               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Rebalance 触发与协议

### 2.1 三大触发条件

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Rebalance（重平衡）三大触发条件（面试必背！）             │
│                                                               │
│  ① 组成员变化：                                              │
│     新 Consumer 加入（扩容）                                 │
│     Consumer 崩溃 / 主动 leave（缩容、宕机）                 │
│                                                               │
│  ② 订阅 Topic 数变化：                                       │
│     正则订阅匹配到新 Topic                                   │
│     Topic 被创建/删除                                        │
│                                                               │
│  ③ 订阅 Topic 的分区数变化：                                 │
│     Topic 扩容增加分区（partition expansion）               │
│                                                               │
│  ★ Rebalance 的代价（生产头号痛点，面试高频）：              │
│   Rebalance 期间：                                           │
│   → ★ 整个 Group 的所有 Consumer 停止消费（Stop-The-World）│
│   → 撤销分区、重新分配、恢复消费                            │
│   → 频繁 Rebalance 导致消费延迟尖刺                         │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 两代成员协调协议

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Kafka 成员协调协议两代演进（面试核心！）                  │
│                                                               │
│  ── 老协议：Eager Rebalance（急切式，Kafka 0.x-2.3）────────  │
│                                                               │
│  两轮 JoinGroup + SyncGroup：                                │
│  ① 所有 Consumer 先【全部撤销】自己的分区                    │
│  ② 一起向 Coordinator 发 JoinGroup                          │
│  ③ Leader Consumer（组内第一个）执行分配算法算出分配方案    │
│  ④ SyncGroup 把方案下发给所有成员                           │
│  ⑤ 全员重新【分配】分区                                      │
│                                                               │
│  问题：所有分区先全部释放再重新分配 → 全部暂停消费           │
│        即使分区本来还归你，也要撤销重来                      │
│                                                               │
│  ── 新协议：Cooperative Sticky（协作式/增量式，2.4+）────── │
│                                                               │
│  ★ 增量重平衡（Incremental Cooperative Rebalancing）：      │
│  ① 成员只撤销【需要移交】的分区（保留自己仍持有的）         │
│  ② JoinGroup 请求里携带：                                    │
│       revokedPartitions（要撤销的）                           │
│       ownedPartitions（要保留的）                            │
│  ③ 分配器尽量维持现有分配，只迁移变化部分                   │
│  ④ 没受影响的 Consumer ★完全不停止消费                       │
│                                                               │
│  配置启用：                                                    │
│   partition.assignment.strategy=                            │
│     org.apache.kafka.clients.consumer.CooperativeStickyAssignor│
│
└─────────────────────────────────────────────────────────────┘
```

### 2.3 分配策略 Assignor

```java
// ★ Kafka 四种分区分配策略（面试高频！）
//
// clients/.../consumer/PartitionAssignor.java
//
// ① RangeAssignor（默认，范围分配）
//    按 Topic 维度，每个 Topic 的分区按序号范围分给 Consumer
//    Topic 数多时第一个 Consumer 总是多拿 → 不均
//
// ② RoundRobinAssignor（轮询）
//    所有 Topic 的所有分区打散，轮询分配 → 更均匀
//
// ③ ★ StickyAssignor（黏性分配，2.3+ 推荐）
//    两个目标：a) 分区分配尽量均匀 b) 重平衡时尽量保持原分配（黏性）
//    → Rebalance 时分区移动最少
//
// ④ ★ CooperativeStickyAssignor（协作黏性，2.4+ 最新）
//    在 Sticky 基础上支持增量协作重平衡（不全员撤销）
//
// // 分配在 Leader Consumer 上执行（不是 Coordinator！）
// // JoinGroupResponse 返回 Leader 的成员列表
// // Leader 调用 assignor.assign(partitionsPerTopic, subscriptions)
// // → 算出 Map<MemberId, List<TopicPartition>>
// // → 通过 SyncGroup 上报 Coordinator 转发
//
```

---

## 三、心跳与会话保活

### 3.1 心跳与 Poll 分离

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Consumer 保活双机制（面试高频！）                         │
│                                                               │
│  ① heartbeat 线程（独立后台线程）：                          │
│     定期发 HeartbeatRequest（session.timeout.ms，默认 45s）  │
│     Coordinator 在此时间内没收到心跳 → 认为成员死亡           │
│     → 触发 Rebalance                                        │
│                                                               │
│  ② max.poll.interval.ms（默认 5min）：                       │
│     ★ 两次 poll() 之间的最大间隔                            │
│     如果业务处理一批消息超过 5min（比如处理卡住、GC）        │
│     → 即使心跳正常，Coordinator 也认为这个 Consumer 挂了     │
│     → 把它踢出 Group，触发 Rebalance                        │
│                                                               │
│  ★ 为什么需要两个机制？                                      │
│   心跳只证明"进程活着"，不证明"在正常消费"                   │
│   → 业务线程死循环/卡死时心跳还在发 → 假活                  │
│   → max.poll.interval.ms 兜底检测"消费能力"                 │
│                                                               │
│  ③ 静态成员 Static Membership（KIP-345，2.3+）：            │
│     group.instance.id 配置固定实例 ID                        │
│     成员短暂重启不立即触发 Rebalance                         │
│     → 在 session.timeout 内重启回来，分区分配保持不变       │
│     → K8s 滚动发布场景大幅减少 Rebalance                    │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Rebalance 状态机

```java
// ★ Consumer 端 Rebalance 状态（源码级）
//
// clients/.../consumer/internals/ConsumerCoordinator.java
//
// 两个核心回调（业务可监听）：
// ① onPartitionsRevoked：分区被撤销前触发（提交 offset 的时机！）
// ② onPartitionsAssigned：新分区分配后触发（初始化资源）
//
// // 监听器接口
// public interface ConsumerRebalanceListener {
//     // ★ Eager：全部撤销时调用；Cooperative：只撤销移交分区时调用
//     void onPartitionsRevoked(Collection<TopicPartition> partitions);
//
//     // 新分区分配完成
//     void onPartitionsAssigned(Collection<TopicPartition> partitions);
// }
//
// ★ Rebalance 期间 offset 提交（面试坑！）：
//   必须在 onPartitionsRevoked 里同步提交已处理完的 offset
//   否则分区被移走后，新 Owner 从旧 offset 开始 → 重复消费
//
```

---

## 四、Offset 管理

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 消费位移 Offset 管理（面试高频）                          │
│                                                               │
│  存储：内部 Topic __consumer_offsets（50 分区，压缩 Topic） │
│   Key = group.id + topic + partition                        │
│   Value = offset + metadata + timestamp                      │
│                                                               │
│  提交方式：                                                    │
│  ① 自动提交（默认）：enable.auto.commit=true                 │
│     每 auto.commit.interval.ms（默认 5s）提交 poll 到的最大 offset│
│     → "at-least-once"：可能重复（处理完没提交就崩）         │
│                                                               │
│  ② 手动提交：                                                 │
│     commitSync()（同步阻塞，可靠）                           │
│     commitAsync()（异步，不阻塞但可能失败）                  │
│     → 处理完业务再提交 → 更精确                              │
│                                                               │
│  ★ Rebalance 后的起始位置：                                  │
│     有已提交 offset → 从 offset 继续                         │
│     无 offset（新组）→ auto.offset.reset：                   │
│       earliest（最早）/ latest（默认，最新）/ none（报错）   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、KRaft 模式下的 Group Coordinator

```
┌─────────────────────────────────────────────────────────────┐
│  ★ KRaft 时代的 Group Coordination 变化（面试加分）          │
│                                                               │
│  ZK 模式：GroupCoordinator 跑在某个 Broker 上                │
│  KRaft 模式（KIP-848，3.7+ 新一代消费组协议）：              │
│   ① Coordinator 主动推送（Push）替代 Consumer 轮询心跳      │
│   ② Rebalance 变成服务端驱动（Consumer 不再做 Leader 分配） │
│   ③ 心跳里直接带 offset/分配反馈，减少请求                  │
│   ④ 真正的增量重平衡 + 更快的故障检测                        │
│                                                               │
│  // group-coordinator/src/main/java/org/apache/kafka/        │
│  //    coordinator/group/GroupCoordinatorService.java        │
│  // 消费组状态以 __consumer_offsets 事件 + Raft 管理         │
│                                                               │
│  Consumer 状态机（新协议）：                                  │
│   UNJOINED → JOINING → RECONCILING（调整分区）→ STABLE       │
│   → 只有成员/分配真正变化时才进入 RECONCILING                │
│   → STABLE 状态下消费不中断                                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Consumer Group 两条铁律？| **一个 Partition 组内只被一个 Consumer 消费；一个 Consumer 可消费多 Partition** |
| 2 | Coordinator 作用？| 管理成员 + 分区分配协调 + offset 存储（__consumer_offsets）|
| 3 | Rebalance 三触发条件？| **成员变化 + 订阅 Topic 变化 + Topic 分区数变化** |
| 4 | Rebalance 代价？| **Stop-The-World：整个 Group 停止消费**；频繁重平衡导致延迟尖刺 |
| 5 | Eager vs Cooperative？| Eager 全员撤销再重分；**Cooperative 增量只移交变化分区，其他成员不停止** |
| 6 | 四种分配策略？| Range（默认）/ RoundRobin / Sticky（黏性）/ CooperativeSticky（最新）|
| 7 | 分配算法谁执行？| **组内 Leader Consumer**（不是 Coordinator），SyncGroup 上报下发 |
| 8 | 心跳 vs max.poll.interval？| 心跳证明进程活；**poll 间隔证明在正常消费**（防业务卡死假活）|
| 9 | 静态成员？| group.instance.id 固定 ID；短重启不触发 Rebalance（K8s 滚动发布）|
| 10 | session.timeout 默认？| 45s；超时没心跳 → 判定死亡 |
| 11 | offset 存哪？| **__consumer_offsets（50 分区压缩 Topic）** |
| 12 | 自动提交风险？| 5s 周期提交 poll 最大 offset；崩溃可能重复消费（at-least-once）|
| 13 | Rebalance 何时提交 offset？| **onPartitionsRevoked 回调里同步提交**，否则新 Owner 重复消费 |
| 14 | auto.offset.reset？| 无已提交 offset 时：earliest / latest（默认）/ none |
| 15 | 消费者数 > 分区数？| 多余消费者空闲分不到分区 |
| 16 | 面试最高频考点排序？| **1. Rebalance 触发+STW 2. Eager vs Cooperative 3. 心跳/poll 双机制 4. 分配策略 5. offset 提交** |
| 17 | 学习路径？| ConsumerCoordinator → JoinGroup/SyncGroup → AbstractConsumerRebalanceListener → __consumer_offsets |
| 18 | 面试常见追问？| 怎么避免频繁 Rebalance？→ CooperativeSticky + 静态成员 + 调大 poll.interval + 避免长处理 |
| 19 | Rebalance 能避免吗？| 用 CooperativeStickyAssignor + Static Membership；K8s 优雅停机先发 close() |
| 20 | 处理消息太慢怎么办？| 增加分区/消费者 + 批量处理 + 异步处理，注意别超 max.poll.interval.ms |
| 21 | KIP-848 新协议？| KRaft 时代服务端驱动重平衡 + Push 心跳 + 增量，Consumer 不再做 Leader 分配 |
| 22 | commitSync vs commitAsync？| Sync 可靠但阻塞；Async 不阻塞但失败不重试（可能提交乱序）|
| 23 | 面试高频场景？| K8s 滚动发布 Rebalance 风暴 + 消费积压扩容 + 僵尸消费者 |
| 24 | 面试陷阱？| 说心跳正常就不会被踢 → ❌ 超 max.poll.interval.ms 照样被踢 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：clients/.../consumer/internals/ConsumerCoordinator.java + group-coordinator/  
**面试考点：24 题**  
**差异化点**：Eager vs Cooperative 增量协议 + 心跳/poll 双保活 + 静态成员 + KIP-848 KRaft 新协议
