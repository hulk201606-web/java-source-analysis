# Apache Pulsar Function + Schema Registry 源码级深度解析

> **源码基线**：Pulsar 3.2.x（Java）  
> **核心位置**：pulsar-functions/ + pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/  
> **面试权重**：⭐⭐⭐⭐（云原生消息流计算 + 数据契约，差异化考点）

---

## 一、Pulsar Functions 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar Functions = 轻量级流处理（Function as a Service）│
│                                                               │
│  定位（面试必背）：                                            │
│   类似 AWS Lambda：                                           │
│   写一个简单函数（Java/Python/Go），消费一个 Topic           │
│   处理后输出到另一个 Topic，不用搭 Flink/Spark 集群          │
│                                                               │
│  ┌────────┐  消息   ┌──────────────┐  结果  ┌────────┐      │
│  │ Input  │ ─────→  │ User Function│ ─────→ │ Output │      │
│  │ Topic  │         │ (处理逻辑)    │        │ Topic  │      │
│  └────────┘         └──────────────┘        └────────┘      │
│
│  vs Flink（面试高频对比）：                                    │
│   Pulsar Functions：轻量、单条/小批处理、内置 Pulsar         │
│   Flink：重量级、复杂窗口/Join/状态/CEP、独立集群            │
│   简单 ETL/过滤/转换 → Functions；复杂流计算 → Flink        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Function 运行时架构

### 2.1 三种运行模式

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar Function 三种运行模式（面试高频）                 │
│                                                               │
│  ① LocalRun（本地模式）：开发调试用，本机 JVM 跑             │
│                                                               │
│  ② Cluster 模式（生产）：                                     │
│    Function 提交到 Pulsar 集群，由 ★ Function Worker 调度   │
│    Function Worker：                                          │
│      - 可嵌入 Broker（默认，functionsWorkerEnabled=true）    │
│      - 也可独立部署                                           │
│      - 负责 Function 的分配、调度、容错、元数据              │
│                                                               │
│  ③ 进程/K8s 运行时：                                          │
│    - Process Runtime：每个 Function 实例一个独立进程         │
│    - ★ Kubernetes Runtime：每个实例一个 Pod（隔离最好）      │
│    - Thread Runtime：同进程多线程（最轻，隔离差）            │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 核心处理链路

```java
// ★ Pulsar Function 处理链路（源码级）
//
// pulsar-functions/instance/src/main/java/.../JavaInstanceRack.java
//
// 一个 Function 实例（Instance）核心组件：
//  - Consumer（消费 Input Topic）
//  - 用户 Function 对象
//  - Producer（写 Output Topic）
//  - Context（上下文：metrics、state、logger）
//
// // 消息处理主循环
// public class JavaInstance {
//     public void processMessage(Message<byte[]> msg) {
//         // ① 反序列化输入（Schema）
//         Object input = serde.deserialize(msg.getData());
//
//         // ② 调用用户函数
//         Object output = userFunction.process(input, context);
//
//         // ③ 输出结果（带相同 key，保持路由）
//         if (output != null) {
//             producer.newMessage()
//                 .key(msg.getKey())
//                 .value(serde.serialize(output))
//                 .send();
//         }
//
//         // ④ ACK 输入消息（至少一次）
//         consumer.acknowledge(msg);
//     }
// }
//
// 用户接口（最简单形式）：
// public class FilterFunction implements Function<Order, Order> {
//     public Order process(Order order, Context ctx) {
//         return order.amount > 100 ? order : null;  // null = 过滤掉
//     }
// }
//
```

### 2.3 语义保证与状态

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Function 投递语义（面试高频）                              │
│   At-most-once：发送即 ACK（可能丢）                         │
│   At-least-once（默认）：处理+输出成功再 ACK（可能重复）     │
│   Effectively-once：依托 Pulsar 事务（事务性输出+ACK）      │
│                                                               │
│  状态管理（State Storage）：                                  │
// pulsar-functions/state/                                      │
│   - 内置 KV 状态接口：context.putState/getState             │
│   - 后端：BookKeeper（Pulsar 2.4+，基于 TableService）       │
│   - 每个 key 的状态按路由存储                                 │
│   - 适合简单计数/去重；复杂状态用外部存储                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Schema Registry 源码级（重点！）

### 3.1 为什么需要 Schema

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 问题：Pulsar/Kafka 原生只存 byte[]，生产者消费者各说各话 │
│                                                               │
│  Producer 发 JSON/POJO，Consumer 不知道怎么反序列化         │
│  → 字段变了、类型错了，运行时才炸                            │
│                                                               │
│  Schema Registry 解决：                                       │
│   ① Schema（数据结构）和 Topic 绑定，集中存储管理            │
│   ② Producer 发送时注册/校验 Schema                          │
│   ③ Consumer 按 Schema 自动序列化/反序列化                   │
│   ④ ★ Schema 版本管理 + 兼容性检查（数据契约）              │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Schema 存储结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar Schema 存储（源码级，面试核心）                    │
│                                                               │
│  Schema 存在哪：                                              │
│   // pulsar-broker/.../schema/BookkeeperSchemaStorage.java   │
│   - 持久化在 BookKeeper（schemaLedger），和消息同存储        │
│   - 也可配关系型存储                                          │
│                                                               │
│  每个 Topic 的 Schema 是一条版本链：                         │
│   Topic: public/default/order-topic                          │
│     version 0: {type: AVRO, schema: {...fields v1...}}       │
│     version 1: {type: AVRO, schema: {...fields v2...}}       │
│     version 2: {...}                                          │
│   - 新版本追加，旧版本永久保留（可回溯）                     │
│   - Schema 位置：__transactionlogs 类似的专用 ledger         │
│                                                               │
│  // SchemaData 结构：                                         │
│  //  type: AVRO / JSON / PROTOBUF / PROTOBUF_NATIVE         │
│  //        / KEY_VALUE / STRING / BOOLEAN / INT64 ...        │
│  //  schema: schema 定义字节（AVRO 是 JSON schema）          │
│  //  properties: 自定义属性                                  │
│  //  timestamp / version                                     │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 Schema 上传与兼容性检查

```java
// ★ Producer 发送时的 Schema 流程（源码级）
//
// ProducerBuilder<Order> builder = client.newProducer(
//     Schema.AVRO(Order.class))           // 声明 Schema
//     .topic("order-topic");
//
// 发送第一条消息时：
//  ① Producer 把 POJO → Avro SchemaInfo
//  ② Broker 收到 CreateProducer + SchemaInfo
//  ③ Broker 调 SchemaService：
//     - Topic 还没有 schema → 存为 version 0
//     - 已有 schema → ★ 做兼容性检查（compatibility checker）
//     - 兼容 → 追加为新版本；不兼容 → 拒绝，报错
//
// // 兼容性策略（CompatibilityCheck，面试高频）：
// //  - BACKWARD（默认，向后兼容）：
// //      新 schema 能读旧数据（如只加可选字段/删字段）
// //      Consumer 用新 schema 读旧 message ✅
// //  - FORWARD（向前兼容）：
// //      旧 schema 能读新数据（如加可选字段）
// //  - FULL：BACKWARD + FORWARD（只能加/删可选字段）
// //  - ALWAYS_COMPATIBLE / ALWAYS_INCOMPATIBLE：不检查
// //  - BACKWARD_TRANSITIVE / FORWARD_TRANSITIVE / FULL_TRANSITIVE
// //      （和所有历史版本都要兼容，而非只和最新版本）
//
// Consumer 端：
//  订阅时 Broker 返回最新（或指定）版本 Schema
//  → 自动反序列化 byte[] 为 POJO，类型安全
//
```

### 3.4 Pulsar vs Confluent Schema Registry

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Pulsar Schema vs Confluent Schema Registry（Kafka）对比  │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │            │ Pulsar        │ Confluent(Kafka)│              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 存储       │ 内置 BookKeeper│ 独立 SR 服务   │              │
│  │            │ （无额外组件） │ + Kafka/KRaft │              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ Schema 位置│ 绑定 Topic    │ 按 subject     │              │
│  │ 类型       │ AVRO/JSON/PB/ │ AVRO/JSON/Protobuf│           │
│  │            │ KeyValue      │                │              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 兼容性配置│ Topic/Namespace│ Subject 级    │              │
│  │            │ 级             │（TOPIC/RECORD）│             │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 运维       │ 无需额外部署   │ 需部署 SR 集群 │              │
│  └──────────┴──────────────┴──────────────┘                │
│  核心价值相同：数据契约 + 版本演进 + 兼容性守门              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Pulsar Functions 定位？| 类 Lambda 的轻量流处理；消费 Input Topic → 处理 → Output Topic |
| 2 | Functions vs Flink？| Functions 轻量单条/小批、内置 Pulsar；Flink 重量级窗口/Join/状态/CEP |
| 3 | 三种运行模式？| LocalRun（调试）+ Cluster（Worker 调度）+ K8s/Process/Thread 运行时 |
| 4 | Function Worker？| 调度/容错/元数据管理组件，可嵌入 Broker 或独立部署 |
| 5 | K8s Runtime？| 每个 Function 实例一个 Pod，隔离性最好 |
| 6 | 处理链路？| Consumer 收消息 → Schema 反序列化 → process() → Producer 输出 → ACK |
| 7 | 投递语义？| at-most-once / **at-least-once（默认）** / effectively-once（Pulsar 事务）|
| 8 | 状态存储？| context.putState/getState，后端 BookKeeper TableService |
| 9 | Schema Registry 解决什么？| byte[] 无类型 → Schema 绑定 Topic + 自动序列化 + 版本兼容校验 |
| 10 | Pulsar Schema 存哪？| **内置 BookKeeper（schemaLedger）**，无需额外组件 |
| 11 | Schema 版本？| 每次变更追加新版本，旧版本永久保留可回溯 |
| 12 | 支持类型？| AVRO / JSON / PROTOBUF / KEY_VALUE / 原生类型 |
| 13 | 兼容性策略？| **BACKWARD（默认）/ FORWARD / FULL + TRANSITIVE 变体** |
| 14 | BACKWARD 含义？| 新 schema 能读旧数据（加可选字段/删字段），Consumer 升级安全 |
| 15 | FORWARD 含义？| 旧 schema 能读新数据（Producer 先升级场景）|
| 16 | Pulsar vs Confluent SR？| Pulsar 内置 BK 无需额外部署；Confluent 独立 SR 服务按 subject 管理 |
| 17 | 不兼容会怎样？| Broker 拒绝 Producer 注册，发送失败（契约守门）|
| 18 | 面试最高频考点排序？| **1. Schema 兼容性 2. Schema 存 BK 3. Functions vs Flink 4. 运行模式 5. 投递语义** |
| 19 | 面试常见追问？| Schema 不兼容但必须发怎么办？→ 改兼容性策略或新 Topic；复杂流处理怎么办？→ Flink |
| 20 | 面试陷阱？| 说 Pulsar Schema 要单独部署 Registry → ❌ 内置在 Broker + BookKeeper，无额外组件 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：pulsar-functions/instance + pulsar-broker/.../service/schema/BookkeeperSchemaStorage.java  
**面试考点：20 题**  
**差异化点**：Function 处理链路/运行时/语义 + Schema 内置 BookKeeper + BACKWARD/FORWARD/FULL 兼容 + vs Confluent
