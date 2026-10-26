# Kafka MirrorMaker 2.0 跨集群同步源码级深度解析

> **源码基线**：Apache Kafka 3.7+（Scala/Java）  
> **核心位置**：connect/mirror/ + MirrorSourceConnector + MirrorCheckpoint  
> **面试权重**：⭐⭐⭐⭐（云原生跨区域同步必备，MM1 vs MM2 必问）

---

## 一、MirrorMaker 2.0 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MirrorMaker 2.0 = Kafka 官方跨集群同步（基于 Connect）    │
│                                                               │
│  ★ MirrorMaker 1.0 vs 2.0（面试必背对比）：                  │
│  ┌──────────┬──────────────────┬──────────────────┐        │
│  │ 维度      │ MM1（Consumer+Producer）│ MM2（Connect 框架）│        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ 架构      │ 简单 Consumer   │ ★ Kafka Connect  │        │
│  │           │ + Producer      │ + 专用 Connector │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ Topic 发现│ 手动配置         │ ★ 自动正则匹配  │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ 偏移同步   │ ❌ 不支持        │ ★ Checkpoint  同步  │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ 心跳同步   │ ❌              │ ★ Heartbeat   同步  │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ ACL/配置   │ ❌              │ ★ Config Sync  │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ Exactly-Once│ ❌             │ ★ 事务生产者    │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ 多集群    │ 一对一定制        │ ★ 多对多拓扑  │        │
│  ├──────────┼──────────────────┼──────────────────┤        │
│  │ 运维      │ 轻量简单         │ 依赖 Connect 集群 │        │
│  └──────────┴──────────────────┴──────────────────┘        │
│                                                               │
│  ★ MM2 内部核心组件（源码级）：                               │
│   ① MirrorSourceConnector：源集群消费 → 目标集群生产        │
│   ② MirrorSinkConnector：目标集群反向同步（回环场景）       │
│   ③ MirrorCheckpointConnector：偏移量定期同步               │
│   ④ MirrorHeartbeatConnector：心跳 offset 同步（延迟监控）  │
│   ⑤ MirrorConfigSync：Topic/ACL/Broker Config 同步          │
│
│  ★ MM2 专用内部 Topic（面试高频）：                          │
│   mm2-offset-syncs.<cluster-alias> → 源到目标 offset 映射   │
│   mm2-checkpoints.<cluster-alias>   → 消费进度快照          │
│   mm2-status.<cluster-alias>         → 运行状态             │
│   mm2-configs.<cluster-alias>        → Broker/Topic 配置同步│
│   mm2-heartbeats                    → 心跳 offset            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、MirrorSourceConnector 核心源码级

### 2.1 Connect 框架集成

```java
// ★ MirrorSourceConnector（源码级）
//
// connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConnector.java
//
// public class MirrorSourceConnector extends SourceConnector {
//
//     // ★ 入口：Connect Worker 启动时调用 start()
//     @Override
//     public void start(Map<String, String> props) {
//         // ① 解析配置（源集群 + 目标集群 + Topic pattern）
//         String sourceClusterAlias = props.get("source.cluster.alias");
//         String targetClusterAlias = props.get("target.cluster.alias");
//         Pattern topicPattern = Pattern.compile(props.get("topics"));
//         //    topics=.*  → 同步所有
//         //    topics=foo|bar  → 只同步这两个
//         //    topics=[a-z].*  → 正则
//
//         // ② 初始化源集群 AdminClient + Consumer
//         sourceAdmin = AdminClient.create(sourceProperties);
//         sourceConsumer = new KafkaConsumer<>(sourceConsumerProperties);
//
//         // ③ 初始化目标集群 AdminClient + Producer
//         targetAdmin = AdminClient.create(targetProperties);
//         targetProducer = new KafkaProducer<>(targetProducerProperties);
//         //    ★ 使用事务生产者（Exactly-Once）
//         //    producer.enable.idempotence=true
//         //    producer.transactional.id="mirror-...-txn"
//
//         // ④ 后台线程：定期发现新 Topic
//         topicDiscovery = new TopicDiscovery(sourceAdmin, topicPattern);
//         topicDiscovery.start();
//     }
//
//     // ★ 创建任务（Connect 模型：Connector → N 个 Task）
//     @Override
//     public List<Map<String, String>> taskConfigs(int maxTasks) {
//         // 把 TopicPartition 列表分配给 N 个 Task
//         // 类似 Kafka Consumer 的 partition assignment
//         List<TopicPartition> partitions = sourceConsumer.listTopics();
//         return MirrorUtils.splitPartitions(partitions, maxTasks);
//     }
//
//     @Override
//     public SourceTask task() {
//         return new MirrorSourceTask();
//     }
// }
//
```

### 2.2 MirrorSourceTask 数据复制核心

```java
// ★ MirrorSourceTask（源码级，面试核心）
//
// connect/mirror/.../MirrorSourceTask.java
//
// public class MirrorSourceTask extends SourceTask {
//
//     @Override
//     public void start(Map<String, String> props) {
//         // ① 订阅分配给自己的 TopicPartition
//         sourceConsumer.assign(assignedPartitions);
//
//         // ② 从 checkpoint 恢复上次同步进度
//         //    读 mm2-checkpoints 里自己的 offset
//         Map<TopicPartition, Long> checkpoints =
//             checkpointReader.readCheckpoints(consumerGroupId);
//         sourceConsumer.seek(checkpoints);  // ★ 断点续传
//     }
//
//     // ★ 核心循环：poll → convert → produce → commit
//     @Override
//     public List<SourceRecord> poll() {
//         List<SourceRecord> records = new ArrayList<>();
//
//         // ① 从源集群 poll 数据
//         ConsumerRecords<byte[], byte[]> sourceRecords =
//             sourceConsumer.poll(Duration.ofMillis(500));
//
//         for (ConsumerRecord<byte[], byte[]> record : sourceRecords) {
//             // ② ★ Topic 重映射（面试高频）
//             //    source topic: "orders"
//             //    → target topic: "orders"（默认同名）
//             //    也可以配置前缀：mm2.prefix=source. → "source.orders"
//             //    也可以配置后删除：mm2.regex=...
//             String newTopic = MirrorUtils.renameTopic(
//                 record.topic(), config);
//
//             // ③ ★ Offset 转换（面试核心）
//             //    源集群 offset 和目标集群 offset 不同！
//             //    目标集群有自己的日志段和 offset
//             //    MirrorUtils 维护 offset 映射表
//             //    offset mapping = mm2-offset-syncs 内部 topic
//             long sourceOffset = record.offset();
//             long targetOffset = MirrorUtils.remapOffset(
//                 newTopic, record.partition(), sourceOffset);
//             //    如果是新 Topic → remapOffset = 从 0 开始
//             //    如果已有历史 → remapOffset = 上次目标 offset + (sourceOffset - sourceStartOffset)
//
//             // ④ 构造 SourceRecord（Connect 模型）
//             records.add(new SourceRecord(
//                 sourcePartition(record),
//                 sourceOffset(record),
//                 newTopic,          // 目标 Topic
//                 record.key(),
//                 record.value()
//             ));
//         }
//
//         return records;  // Connect Framework 会调用 SourceTask
//     }
//
//     // ★ Commit 阶段（Connect 模型自动触发）
//     // SourceTask.commit() 被 Connect Framework 在 Sink 写入成功后调用
//     @Override
//     public void commit() {
//         // ① 把 Consumer 的 committed offset 写回源集群
//         sourceConsumer.commitSync();
//
//         // ② ★ 写 checkpoint 到 mm2-checkpoints topic
//         //    记录"我已经同步到源集群 offset X"
//         //    下次重启从这里续传
//         checkpointWriter.writeCheckpoint(
//             consumerGroupId, committedOffsets);
//     }
// }
//
```

---

## 三、Offset 同步与 Checkpoint 核心

### 3.1 Offset 同步流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MM2 Offset 同步全链路（面试核心，MM1 不支持！）           │
│                                                               │
│  场景：应用在源集群 consumer 组消费到 offset 1000           │
│  切换到目标集群后怎么续上？                                    │
│                                                               │
│  ① 应用在目标集群创建同名 consumer group                     │
│  ② 但目标集群 Topic 是 MM2 复制过来的，offset 完全不同       │
│  ③ 需要一张【源 offset → 目标 offset】的映射表              │
│                                                               │
│  ★ MirrorCheckpointConnector 负责同步 offset 映射：          │
│                                                               │
│  A. 定期（默认 1 分钟）从源集群消费组读 committed offset     │
│     // 用 ConsumerGroupCommand / AdminClient.listConsumerGroupOffsets
│     Map<TopicPartition, Long> sourceCommittedOffsets =
│         sourceAdmin.listConsumerGroupOffsets(groupId);
│                                                               │
│  B. 查 mm2-offset-syncs 内部 topic，把 sourceOffset 转 targetOffset
│     long targetOffset = offsetMapping.getTargetOffset(
│         targetTopic, partition, sourceOffset);
│                                                               │
│  C. 把转换后的 targetOffset 写到目标集群的 checkpoints topic  │
│     checkpointWriter.write(groupId, targetTopic, targetOffset);
│                                                               │
│  ★ 应用续消费流程（面试高频）：                               │
│     应用在目标集群 consumer 启动：
│     → Connect Reader 从 mm2-checkpoints 读自己的 checkpoint │
│     → seek 到 targetOffset 继续消费                         │
│     → 效果：无缝切换，源集群消费进度完全同步到目标集群        │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Checkpoint 源码级

```java
// ★ MirrorCheckpointConnector（源码级）
//
// connect/mirror/.../MirrorCheckpointConnector.java
//
// public class MirrorCheckpointConnector extends SourceConnector {
//
//     // ★ 定期（checkpoint.interval.seconds，默认 60s）同步
//     @Override
//     public List<SourceRecord> poll() {
//         List<SourceRecord> checkpoints = new ArrayList<>();
//
//         // ① 发现源集群所有 consumer group
//         List<String> groups = sourceAdmin.listConsumerGroups();
//
//         for (String group : groups) {
//             // ② 读该 group 在源集群的 committed offset
//             Map<TopicPartition, Long> sourceOffsets =
//                 sourceAdmin.listConsumerGroupOffsets(group);
//
//             for (Map.Entry<TopicPartition, Long> entry : sourceOffsets.entrySet()) {
//                 TopicPartition tp = entry.getKey();
//                 long sourceOffset = entry.getValue();
//
//                 // ③ 把源 Topic 映射到目标 Topic
//                 String targetTopic = MirrorUtils.renameTopic(tp.topic(), config);
//
//                 // ④ ★ 核心转换：用 offset mapping 找目标 offset
//                 //    mapping 存在 mm2-offset-syncs.<source-cluster> 内部 topic
//                 long targetOffset = offsetMapping.offsetFor(
//                     targetTopic, tp.partition(), sourceOffset);
//                 //    // offsetMapping 的实现：
//                 //    //   Consumer<CheckpointEnvelope> + 二分查找
//                 //    //   因为每条复制数据都会在 offset-syncs 里记录一条映射
//                 //    //   格式：{targetTopic, partition, sourceOffset, targetOffset}
//
//                 // ⑤ 构造 checkpoint record 写入目标集群 checkpoint topic
//                 checkpoints.add(new SourceRecord(
//                     checkpointTopic + "-" + group,  // key = checkpoint topic + group
//                     null,
//                     targetTopic,                     // value target topic
//                     MirrorUtils.toCheckpointValue(targetOffset, group, sourceOffset)
//                 ));
//             }
//         }
//
//         return checkpoints;
//     }
// }
//
```

---

## 四、MirrorSink + 多集群双向同步

### 4.1 MirrorSink 反向同步

```java
// ★ MirrorSinkConnector（源码级）
//
// connect/mirror/.../MirrorSinkConnector.java
//
// MirrorSinkConnector = MirrorSourceConnector 的反向
//   目标集群 → 源集群（回环场景：active-active 多活）
//
// public class MirrorSinkConnector extends SinkConnector {
//
//     @Override
//     public void put(Collection<SinkRecord> records) {
//         // ① 从目标集群（Sink 侧）读到的数据
//         //    可能是：MirrorSourceConnector 复制过来的数据
//         //    也可能是应用直接写目标集群的数据
//
//         // ② ★ 关键：避免回环（Source 复制 → Sink 又写回 → 无限循环）
//         //    方案 A：Topic 前缀区分
//         //      MirrorSource 复制 "source.order" → 目标 "target.order"
//         //      MirrorSink 只复制 "target.order" 回源集群
//         //      应用写目标集群 → 写到 "target.order"（Sink 会同步）
//         //
//         //    方案 B：配置 exclusion pattern
//         //      MirrorSink 排除 mm2- 开头的内部 topic
//         //
//         //    方案 C：MirrorSource 写入时打 tag（header）
//         //      Sink 检测到是 Source 复制来的就跳过
//
//         for (SinkRecord record : records) {
//             String targetTopic = record.topic();
//             String sourceTopic = MirrorUtils.renameToSourceTopic(targetTopic);
//
//             // 写入源集群
//             sourceProducer.send(new ProducerRecord<>(
//                 sourceTopic, record.key(), record.value()));
//         }
//     }
// }
//
```

### 4.2 多集群拓扑

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MM2 支持的多集群拓扑（面试高频）                          │
│                                                               │
│  ① 单向同步（备份/灾备）                                     │
│     DC-A (active) → DC-B (standby)                           │
│     MirrorSource(DC-A → DC-B)                                │
│                                                               │
│  ② 双向同步（active-active 多活）                            │
│     DC-A ↔ DC-B                                              │
│     MirrorSource(DC-A→DC-B) + MirrorSource(DC-B→DC-A)       │
│     ★ 必须 Topic 前缀区分：A.topic → B.aTopic；B.topic → A.bTopic│
│     ★ 必须 MirrorSink 排除 mm2- 内部 topic                  │
│     ★ 用 MirrorHeartbeat 监控端到端延迟                      │
│                                                               │
│  ③ 星型拓扑（多区域 → 集中集群）                              │
│     DC-A → Hub / DC-B → Hub / DC-C → Hub                    │
│     每个源集群独立 MirrorSource → Hub                        │
│     Hub 内部 topic 按来源前缀区分                              │
│                                                               │
│  ④ 链式拓扑（DC-A → DC-B → DC-C，需要 offset 转换）          │
│     A.offset=100 → B.offset=98（A→B 转换）                  │
│     B.offset=98  → C.offset=95（B→C 转换）                  │
│     offset-mapping 必须链式传递                               │
│                                                               │
│  ★ MM2 vs Confluent Replicator 对比（面试扩展）：            │
│  ┌──────────┬──────────────┬──────────────────┐              │
│  │            │ MM2 (开源)     │ Replicator (商业)   │              │
│  ├──────────┼──────────────┼──────────────────┤              │
│  │ 架构       │ Connect 框架  │ 独立服务            │              │
│  │ 吞吐       │ 高（并行）    │ 更高（多实例）      │              │
│  │ 双向同步    │ ✅ 手动配置  │ ✅ 内置            │              │
│  │ 冲突处理    │ ❌ 无        │ ✅ Timestamp 合并  │              │
│  │ Schema 同步 │ ❌ 无        │ ✅ 集成 SR         │              │
│  │ MirrorMaker 2.0 缺点：                                      │
│  │  - 双向 active-active 冲突处理靠应用层保证                   │
│  │  - Schema Registry 不内置同步                                │
│  │  - Connect 运维成本（Worker 集群）                           │
│  └──────────┴──────────────┴──────────────────┘              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、Exactly-Once 与心跳延迟监控

### 5.1 Exactly-Once 保证

```java
// ★ MM2 Exactly-Once 实现（源码级，面试高频）
//
// MM2 的 Exactly-Once = Connect 框架事务 + 内部 checkpoint
//
// // MirrorSourceTask 使用事务生产者：
// producer.setTransactionId(transactionalId);
// producer.initTransactions();
//
// // poll → commit 之间：
// producer.beginTransaction();
// for (SourceRecord record : records) {
//     producer.send(record);  // 生产者内部事务队列
// }
// producer.commitTransaction();
// // 如果 poll 循环中途挂了 → 事务回滚 → 目标集群无脏数据
//
// // Connect Framework + SourceTask.commit()：
// // SourceTask.commit() 成功后才提交 offset
// // 保证"目标集群写入成功" ↔ "源集群 offset 推进" 原子性
// // 不会出现：目标写了但 offset 没提交 → 重启重复消费
// // 也不会：offset 提交了但目标没写 → 数据丢失
//
// ★ MM2 的语义级别：
//  - 端到端 Exactly-Once：从源集群消费到目标集群生产
//  - 但跨 Consumer Group 的 Exactly-Once 依赖应用自己保证
//  - MirrorCheckpoint 保证 offset 同步，但消费到应用层面可能还要再做
//
```

### 5.2 MirrorHeartbeat 延迟监控

```java
// ★ MirrorHeartbeatConnector（源码级）
//
// connect/mirror/.../MirrorHeartbeatConnector.java
//
// // 作用：周期性写入心跳消息，用于监控端到端同步延迟
//
// public class MirrorHeartbeatConnector extends SourceConnector {
//
//     // 定期（heartbeat.interval.seconds）写入
//     // 写入时间戳 + 心跳序号到目标集群的 mm2-heartbeats topic
//     // 心跳消息包含：
//     //   - 写入源集群的时间戳 T1
//     //   - 被 MirrorSource 复制到目标集群的时间戳 T2
//     //   - 端到端延迟 = T2 - T1
//     //
//     // 应用/运维可以消费 mm2-heartbeats 监控延迟
//     // 告警规则：延迟 > 5s 触发告警
// }
//
// ★ MM2 内部 Topic 清单（面试常考）：
// ┌───────────────────────────────┬──────────────────────┐
// │ Topic 名称                     │ 作用                  │
// ├───────────────────────────────┼──────────────────────┤
// │ mm2-offset-syncs.<source>     │ ★ 源→目标 offset 映射 │
// │ mm2-checkpoints.<source>      │ ★ 消费进度快照        │
// │ mm2-status.<source>           │ 运行状态             │
// │ mm2-configs.<source>          │ Broker/Topic 配置同步 │
// │ mm2-heartbeats                │ 心跳延迟监控          │
// │ mm2-cluster-mappings          │ 多集群映射关系        │
// └───────────────────────────────┴──────────────────────┘
//
```

---

## 六、差异化对比表

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比：MM2 vs MM1 vs Replicator vs Flink CDC      │
│  ┌──────────┬────────────┬────────────┬────────────┬────┐
│  │ 维度      │ MM2 (Kafka) │ MM1        │ Confluent   │Flink│
│  │           │             │            │ Replicator  │ CDC │
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 架构      │ Connect    │ Consumer+  │ 独立服务    │Flink│
│  │           │            │ Producer   │            │ Job │
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ Topic 发现│ ★ 正则自动 │ ❌ 手动配置 │ ★ 自动     │自动│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ Offset 同步│ ★ Checkpoint│ ❌          │ ★ 完整     │可配│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 心跳延迟   │ ★ Heartbeat│ ❌          │ ✅          │可配│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ Exactly-Once│ ★ 事务   │ ❌ 至少一次 │ ★ 完整     │★ 完整│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 双向同步   │ ✅ 手动配置 │ ❌          │ ★ 内置     │✅ 可│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 冲突处理   │ ❌ 无      │ ❌          │ ★ 内置     │❌ 无│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ Schema 同步│ ❌ 无      │ ❌          │ ★ SR 集成  │❌ 无│
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 多集群拓扑│ ★ 多对多   │ 一对一定制 │ ★ 多对多   │多  │
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 运维复杂度 │ 中（Worker）│ ★ 低       │ 高（商业） │中  │
│  ├──────────┼────────────┼────────────┼────────────┼────┤
│  │ 面试高频   │ ★★★ 高     │ ★ 低       │ ★★ 中      │★★★│
│  └──────────┴────────────┴────────────┴────────────┴────┘
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   Kafka 跨集群同步 → **MirrorMaker 2.0**（开源、Connect 生态）│
│   简单单向同步、不想运维 Connect → **MM1**（轻量）            │
│   多活双向 + 冲突处理 + Schema → **Replicator**（商业）      │
│   需要实时 ETL + CDC + 转换 → **Flink CDC**（非纯同步）       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | MirrorMaker 2.0 是什么？| Kafka 官方**基于 Connect 框架**的跨集群同步工具 |
| 2 | MM1 vs MM2 核心区别？| MM1 简单 Consumer+Producer；**MM2 基于 Connect，支持自动 Topic 发现、Offset Checkpoint、心跳、Exactly-Once** |
| 3 | MM2 内部核心组件？| **MirrorSourceConnector + MirrorSinkConnector + MirrorCheckpointConnector + MirrorHeartbeat + ConfigSync** |
| 4 | mm2 开头的内部 Topic 有哪些？| **offset-syncs + checkpoints + status + configs + heartbeats + cluster-mappings** |
| 5 | MirrorSourceConnector 核心流程？| Connect start → 发现 Topic → Task 循环：poll → rename → offset remap → produce → checkpoint |
| 6 | Topic 重映射？| 默认同名；可配置前缀/正则（mm2.prefix / mm2.regex） |
| 7 | ★ Offset Remap 怎么实现？| **源 offset → 查 mm2-offset-syncs → 转目标 offset**（每条复制数据都记录一条映射） |
| 8 | 为什么需要 offset remap？| 源集群和目标集群的 Topic 是不同物理 topic，offset 独立 |
| 9 | Checkpoint 什么时候写？| SourceTask.commit() 时定期写入 mm2-checkpoints；**重启从 checkpoint 恢复** |
| 10 | MirrorCheckpointConnector 做什么？| **定期同步 consumer group 的 committed offset：源 offset → remap → 目标 offset → 写 checkpoint topic** |
| 11 | 应用怎么续消费？| 在目标集群启动 → Connect Reader 读 checkpoint → seek 到目标 offset 继续 |
| 12 | MirrorSinkConnector 作用？| **反向同步**（active-active 多活场景：目标集群写的数据回同步到源集群） |
| 13 | MirrorSink 怎么防回环？| Topic 前缀区分 / exclusion pattern / header tag 检测 |
| 14 | MM2 支持哪些拓扑？| **单向备份 / 双向 active-active / 星型多区域 / 链式多跳** |
| 15 | ★ MM2 Exactly-Once 实现？| **事务生产者（init/begin/commit）+ Connect Framework SourceTask.commit() 原子提交 offset** |
| 16 | MirrorHeartbeat 做什么？| 定期写心跳消息，监控**端到端同步延迟**（目标时间戳 - 源时间戳） |
| 17 | MirrorConfigSync 做什么？| 同步 Topic Config / Broker Config / ACL 到目标集群 |
| 18 | MM2 vs Confluent Replicator 对比？| Replicator 商业版更强：内置双向、冲突处理、Schema 同步；MM2 开源但需手动配置 |
| 19 | 面试最高频考点排序？| **1. MM1 vs MM2 区别 2. Offset Remap 核心 3. Checkpoint 断点续传 4. Exactly-Once 事务 5. 双向同步防回环** |
| 20 | 学习路径？| MirrorSourceConnector → MirrorSourceTask → offset-syncs → CheckpointConnector → HeartbeatConnector |
| 21 | 面试常见追问？| MM2 同步 Topic 怎么创建？→ MirrorSourceTask 在目标集群自动创建同名 Topic（adminClient.createTopic） |
| 22 | 面试常见追问？| MM2 同步新 Topic 延迟？→ Topic Discovery 后台线程定期扫描（topic.regeneration.interval.seconds），默认 5 分钟 |
| 23 | 面试陷阱？| 说 MM2 能跨 Schema 同步 → ❌ 开源版不行（Replicator 商业版才有）；说 MM2 是 MM1 的升级版 → ❌ 架构完全不同（Connect vs 原生 Consumer） |
| 24 | 选型建议？| Kafka 跨集群首选 MM2；简单同步用 MM1；多活+冲突用 Replicator |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：connect/mirror/MirrorSourceConnector.java + MirrorSourceTask.java + MirrorCheckpointConnector.java + MirrorSinkConnector.java + MirrorHeartbeatConnector.java  
**面试考点：24 题**  
**差异化点**：MM1 vs MM2 架构差异、Offset Remap 核心机制、Checkpoint 断点续传、Exactly-Once 事务保证、双向同步防回环、MM2 vs Replicator vs Flink CDC 对比
