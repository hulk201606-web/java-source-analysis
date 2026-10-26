# Kafka Consumer Rebalance KIP-429 Eager vs Cooperative 源码级深度解析

> **源码基线**：Apache Kafka 3.6+（Java）  
> **核心位置**：clients/src/main/java/.../rebalance/internals/ + assignors/CooperativeStickyAssignor  
> **面试权重**：⭐⭐⭐⭐⭐（消费者 Rebalance 高频，补全 169 KIP-848 边界明确）

---

## 重要边界：本文覆盖范围

```
┌─────────────────────────────────────────────────────────────┐
│  ⚠️ 本文边界：KIP-429（客户端 Rebalance 协议）              │
│                                                               │
│  ★ NOT KIP-848（doc 169 已覆盖 Broker 端新 Coordinator）   │
│                                                               │
│  三代 Rebalance 协议（面试必背时间线）：                      │
│                                                               │
│  ① Eager Rebalance（Kafka < 2.4）—— 完全 STW                │
│     客户端端，所有消费者同时停 → 全量重新分配                │
│     2.4 之前默认协议                                          │
│                                                               │
│  ② KIP-429 Cooperative Rebalance（2.4+ 默认）—— 增量        │
│     客户端端，只有变动消费者触发；Sticky Assignor 最小转移    │
│     本文★核心覆盖                                              │
│                                                               │
│  ③ KIP-848 新 Group Coordinator（3.7+ 可选）—— Broker 端    │
│     Broker 做分配；Server-side Assignor；KRaft 原生          │
│     doc 169 已覆盖 → 本文第三阶段会交叉对比                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Eager Rebalance vs Cooperative Rebalance 核心流程

```java
// ★ Eager vs Cooperative Rebalance 源码级（Kafka clients）
//
// clients/.../rebalance/internals/ConsumerRebalanceListener.java
// clients/.../rebalance/internals/AbstractCoordinator.java
// clients/.../assignors/CooperativeStickyAssignor.java
//
// // ============ Eager Rebalance（2.4 之前）============
// //
// // 触发：任何消费者加入/离开/Topic Partition 变化
// //
// // 流程（完全 STW）：
// // 1. Consumer Coordinator 通知所有消费者"Rebalance 了"
// //    → 发 JoinGroupRequest（session timeout 内）
// //
// // 2. 所有消费者必须 STOP（不再 poll()）
// //    Consumer 进入 waitingForRebalance 状态
// //    这期间 Poll() 抛 REBALANCE_IN_PROGRESS
// //
// // 3. Coordinator 等所有消费者 Join（等 group.instance.id 的全部成员）
// //    任何一个没到 → Coordinator 等至 rebalance.timeout.ms
// //
// // 4. Leader Consumer 执行 Assignor.assign()
// //    用 RoundRobin / Range 等策略全量重新分配
// //    结果返回 Coordinator → 分发给所有消费者
// //
// // 5. 所有消费者收到新 assignment → 恢复 poll()
// //
// // ★ 问题：每次 Rebalance 都是全量停止！
// //   消费者 A 重启 → 整个 Group 都停 → 全量分配 → 恢复
// //   Group 大（1000+ 消费者）→ Rebalance 可能几分钟
//
// // ============ ★ KIP-429 Cooperative Rebalance（2.4+ 默认）============
// //
// // 触发：同上（消费者变动/Partition 变化）
// //
// // ★ 核心区别：Cooperative Assignor 做"增量分配"
// //   不会让所有消费者全停！
// //
// // 流程（增量 Rebalance）：
// // 1. 触发的消费者（比如新加的 B）发 JoinGroup
// //    Coordinator 让 B 成为 Leader（因为新加入的需要分配）
// //
// // 2. Leader B 执行 CooperativeStickyAssignor.assign()
// //    // ★ CooperativeStickyAssignor 核心：
// //    // a. 先看当前所有消费者的 assignment
// //    //    Consumer A: [P0, P1]（不变）
// //    //    Consumer C: [P2, P3]（不变）
// //    //    Consumer B: []（新加入的，需要分配）
// //    //
// //    // b. 找有"移动空间"的 Consumer（不是所有消费者都停！）
// //    //    只有 A 或 C 可能需要放弃一个 Partition 给 B
// //    //    让 B 接管 → B 发 JoinGroup 接收新 assignment
// //    //
// //    // c. Coordinator 通知：
// //    //    A: "你让出 P1" → A 停止消费 P1 → 提交 Offset → 让出
// //    //    B: "你接管 P1, P4" → B 开始消费 P1, P4
// //    //    C: "不变" → C 继续正常 poll()！
// //    //
// // ★ 只有需要让渡/接管的消费者暂停
// // ★ 其他消费者继续正常消费（没有 STW！）
//
```

---

## 三、CooperativeStickyAssignor 核心算法

```java
// ★ CooperativeStickyAssignor 源码级（Kafka clients）
//
// clients/.../assignors/CooperativeStickyAssignor.java
//
// // 核心设计目标：minimize Partition movement（最小化移动）
//
// public Map<String, List<TopicPartition>> assign(
//     Map<String, Integer> partitionsPerTopic,
//     Map<String, Subscription> subscriptions,
//     Map<String, List<TopicPartition>> currentAssignment  // ★ 关键：当前 assignment！
// ) {
//     // 1. 初始：把每个 Topic 的 Partition 均匀分给消费者
//     //    Round-Robin 但按 topic 分组（一个消费者尽量拿到同 topic 的连续 Partition）
//     Map<String, List<TopicPartition>> assignment = initialAssign();
//
//     // 2. ★ Sticky 优化：尽量保持当前 assignment 不变
//     //    把当前 assignment 的 Partition 标记为"已保留"
//     for (Map.Entry<String, List<TopicPartition>> entry : currentAssignment.entrySet()) {
//         String consumerId = entry.getKey();
//         for (TopicPartition tp : entry.getValue()) {
//             if (assignment.get(consumerId).contains(tp)) {
//                 // ★ 这个 Partition 保留！不移动
//                 assigned.add(tp);
//             }
//         }
//     }
//
//     // 3. 处理新增消费者（空 assignment）
//     //    找"拥有 Partition 最多"的消费者 → 让它释放一些给新消费者
//     for (String newConsumer : newConsumers) {
//         String donor = findDonor(assignment);  // 找最多 Partition 的
//         List<TopicPartition> movable = assignment.get(donor).stream()
//             .filter(tp -> !assigned.contains(tp))
//             .collect(Collectors.toList());
//         // 让 donor 让出一些 Partition → 给 newConsumer
//         assignment.get(newConsumer).addAll(selectSubset(movable, donor.getMax() / 2));
//     }
//
//     // 4. 处理删除消费者（死了的）
//     //    死了的消费者的 Partition → 分给其他消费者
//     //    → 尽量分给"最少 Partition"的消费者
//     for (String deadConsumer : deadConsumers) {
//         for (TopicPartition tp : assignment.remove(deadConsumer)) {
//             String receiver = findReceiver(assignment);  // 找最少 Partition 的
//             assignment.get(receiver).add(tp);
//         }
//     }
//
//     return assignment;
// }
//
// ★ StickyAssignor vs CooperativeStickyAssignor（重要）：
//   StickyAssignor（KIP-429 前，配合 Eager Rebalance）：
//     - 客户端级分配
//     - 最小化 Partition movement（但还是全量 Rebalance）
//   CooperativeStickyAssignor（KIP-429）：
//     - 配合 Cooperative Rebalance 协议
//     - 真正增量 Rebalance（只有变动消费者暂停）
//     - Partition movement 可能为 0（如果消费者没变）
//
```

---

## 四、Rebalance 触发条件 + 状态机

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Rebalance 触发条件（面试高频）                             │
│                                                               │
│  类型 1：消费者变动                                            │
│   - 新消费者加入 Group                                        │
│   - 消费者心跳超时（session.timeout.ms 默认 45s）              │
│   - 消费者主动 leaveGroup                                     │
│                                                               │
│  类型 2：Topic Partition 变化                                  │
│   - AdminClient.createPartitions() → Partition 增加           │
│   - Topic 被删除                                              │
│                                                               │
│  类型 3：Subscription 变化                                     │
│   - 消费者订阅/取消订阅 Topic（metadata 变化通知）            │
│                                                               │
│  ★ Rebalance 状态机（Coordinator 视角）：                    │
│  ┌──────────────────────────────────────────────────┐        │
│  │ STABLE → PREPARING_REBALANCE → COMPLETING_REBALANCE│      │
│  │                                                      │    │
│  │ STABLE:           消费者正常 poll                    │      │
│  │ PREPARING:        Coordinator 通知"准备 Rebalance" │      │
│  │   Eager: 等所有消费者 STOP                           │      │
│  │   Cooperative: 等需要让渡的消费者 STOP              │      │
│  │ COMPLETING:       Leader Assignor 执行分配          │      │
│  │   Eager: 分配给所有人                                │      │
│  │   Cooperative: 只给变动消费者                        │      │
│  └──────────────────────────────────────────────────┘        │
│                                                               │
│  ★ Rebalance 超时参数（面试高频）：                            │
│   rebalance.timeout.ms（默认 60s）：消费者 Join 等待超时     │
│   session.timeout.ms（默认 45s）：心跳超时触发 Rebalance      │
│   heartbeat.interval.ms（默认 3s）：心跳间隔                  │
│   partition.assignment.strategy: CooperativeStickyAssignor   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、三代 Rebalance 协议全维度对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试最高频：三代 Rebalance 协议全维度对比                 │
│  ┌──────────┬──────────────────┬──────────────────┬──────────┐│
│  │ 维度      │ Eager Rebalance   │ KIP-429 Cooperative │ KIP-848  ││
│  │            │ (< 2.4)          │ (2.4+ 默认)       │ (3.7+)   ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 协调者     │ ★ Client Leader  │ ★ Client Leader   │ ★ Broker  ││
│  │            │ Assignor         │ Assignor           │ (新 Coord)│
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ Rebalance │ ★ 完全 STW       │ ★ 增量（仅变动）   │ ★ 增量   ││
│  │            │ 所有消费者停    │ 仅变动消费者停    │ Broker端 ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ Partition │ ★ 全量重新分配   │ ★ 最小化移动      │ Broker   ││
│  │ movement  │ 可能 100%         │ 可能 0%           │ 最优     ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ Assignor  │ RoundRobin/Range  │ CooperativeSticky │ Server-  ││
│  │            │ Sticky（旧版）   │ Assignor           │ side     ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 消费者感知  │ 必须 STOP 等    │ 仅需让渡时暂停    │ 透明     ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 大规模 Group│ ❌ STW 分钟级   │ ★ 秒级恢复       │ ★ 秒级   ││
│  │ (1000+)   │                  │                   │          ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 依赖       │ ConsumerGroup    │ ConsumerGroup     │ KRaft    ││
│  │            │ Coordinator     │ Coordinator       │ 新 Coord ││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 运维复杂度 │ 低（协议简单）    │ 中（需配 Strategy）│ 中（双写）││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 默认版本   │ Kafka 2.3-       │ Kafka 2.4-3.6+   │ Kafka 3.7+││
│  ├──────────┼──────────────────┼──────────────────┼──────────┤│
│  │ 面试高频   │ ★★★            │ ★★★★★           │ ★★★★★   ││
│  └──────────┴──────────────────┴──────────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   生产 Kafka 3.0+ → **KIP-429 CooperativeStickyAssignor**（默认且好用）│
│   Kafka 3.7+ 测试环境 → KIP-848（新 Broker 端协调，未来默认）│
│   旧集群（2.3 以前）→ 必须升级，Eager Rebalance 大规模 Group 慢│
│   关键：partition.assignment.strategy=CooperativeStickyAssignor│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Eager vs Cooperative Rebalance 核心区别？| **Eager 完全 STW（所有消费者停）+ 全量重分配；Cooperative 增量（仅变动消费者停）+ 最小化移动** |
| 2 | ★ KIP-429 是什么？| Cooperative Rebalance 协议 + CooperativeStickyAssignor；**增量 Rebalance 的客户端协议** |
| 3 | ★ 重要边界：KIP-429 vs KIP-848？| **KIP-429=客户端端 Rebalance 协议（Leader Consumer 分配）；KIP-848=Broker 端新 Coordinator（Broker 分配）** |
| 4 | CooperativeStickyAssignor 核心目标？| **Minimize Partition movement（最小化 Partition 移动）** |
| 5 | CooperativeStickyAssignor 怎么最小化移动？| 先保留当前 assignment 不变；新增消费者从最多 Partition 的消费者让渡；删除消费者给最少 Partition 的接收者 |
| 6 | Eager Rebalance 为什么 STW？| Coordinator 要求所有消费者必须 STOP 才能开始；任何一个没停 → 等 rebalance.timeout.ms |
| 7 | Cooperative Rebalance 谁 Leader？| **触发 Rebalance 的新加入消费者当 Leader**（需要为自己分配 Partition）|
| 8 | Cooperative Rebalance 消费者角色？| STABLE 时继续 poll；REBALANCE 时如果是变动者 → STOP → 让出/接管 → 恢复 poll；**不变者全程正常 poll** |
| 9 | Rebalance 三种触发条件？| **消费者变动 + Topic Partition 变化 + Subscription 变化** |
| 10 | Rebalance 三阶段状态机？| **STABLE → PREPARING_REBALANCE → COMPLETING_REBALANCE** |
| 11 | partition.assignment.strategy 默认值？| **Kafka 2.4+ 默认：CooperativeStickyAssignor**；之前是 RangeAssignor |
| 12 | Rebalance 高频参数？| session.timeout.ms=45s + rebalance.timeout.ms=60s + heartbeat.interval.ms=3s |
| 13 | ★ 三代 Rebalance 全维度对比？| 见第五节对比表（9 维度）|
| 14 | 面试最高频考点排序？| **1. Eager vs Cooperative 核心区别 2. KIP-429 vs KIP-848 边界 3. CooperativeStickyAssignor 算法 4. Rebalance 触发条件 5. 三代对比** |
| 15 | 学习路径？| Eager STW → KIP-429 Cooperative → CooperativeStickyAssignor → Rebalance 触发条件 → 三代对比 |
| 16 | 面试常见追问？| Cooperative Rebalance 时不变的消费者会不会停？| **不会！** 只有需要让渡/接管 Partition 的消费者会停；不变的全程正常 poll |
| 17 | 面试常见追问？| 消费者心跳超时会触发 Rebalance 吗？| **会**；session.timeout.ms（默认 45s）没收到心跳 → Coordinator 认为消费者挂了 → 触发 Rebalance |
| 18 | 面试陷阱？| 说 KIP-429 就是 KRaft → ❌ KIP-429 是**客户端 Rebalance 协议**；KIP-848 才是 Broker 端新 Coordinator（KRaft-native）|
| 19 | 面试陷阱？| 说 Cooperative Rebalance 比 Eager Rebalance 快所以应该升级 Broker → ❌ 是客户端配置 partition.assignment.strategy 改就生效；不需要 Broker 升级 |
| 20 | StickyAssignor vs CooperativeStickyAssignor？| StickyAssignor（Eager 协议下也能最小化移动，但还是 STW）；**CooperativeStickyAssignor（KIP-429 协议下增量 Rebalance）** |
| 21 | rebalance.timeout.ms 太小会怎样？| 消费者 Stop 太慢（提交 Offset + 让渡 Partition 耗时）→ 超时 → Rebalance 失败 → 触发新一轮 Rebalance |
| 22 | 典型生产调优？| 大规模 Group（>100 消费者）→ CooperativeStickyAssignor + session.timeout.ms=60s + rebalance.timeout.ms=120s |
| 23 | 如何禁用 Rebalance 时自动让渡？| 无（是 Cooperative 协议设计）；如果不想增量 → 用 RangeAssignor（Eager 协议）|
| 24 | 监控 Rebalance 指标？| Consumer Group Metrics：rebalance_total / rebalance_rate / rebalance_latency |
| 25 | Kafka Streams Rebalance？| Streams 底层用 KIP-429；Partition 状态存储恢复也在 Rebalance 后（State Restore）|
| 26 | 典型生产用法？| 点击流 Consumer Group 1000+ 消费者 → CooperativeStickyAssignor；消费者重启不会触发全量 Rebalance |
| 27 | Rebalance 与 Offset Commit 顺序？| 消费者 STOP 时先提交 Offset → 让渡 Partition（防止丢失）；Cooperative 协议保证这个顺序 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：clients/.../rebalance/internals/ConsumerRebalanceListener.java + assignors/CooperativeStickyAssignor.java  
**面试考点：27 题**  
**差异化点**：Eager STW vs KIP-429 增量、CooperativeStickyAssignor 最小移动算法、**KIP-429（客户端）vs KIP-848（Broker端）明确边界**、三代 Rebalance 协议对比表
