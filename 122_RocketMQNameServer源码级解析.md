# Apache RocketMQ NameServer + Broker 注册中心源码级解析

> **源码基线**：RocketMQ 5.2.x（Java）  
> **核心仓库**：apache/rocketmq → name/ + broker/  
> **面试权重**：⭐⭐⭐⭐（MQ 第二巨头，和 Kafka 对比高频）

---

## 一、RocketMQ 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ = 阿里开源分布式消息队列                        │
│                                                               │
│  核心组件：                                                   │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ NameServer（注册中心 ★ 本文重点！）                     │   │
│  │   ├── Broker 注册管理                                   │   │
│  │   ├── Topic 路由管理                                    │   │
│  │   └── 无状态！任意一个挂了都不影响                      │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ 心跳 + 注册                                         │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ Broker Master │ Broker Master │ Broker Slave │              │
│  │ Topic-A     │ Topic-B     │ 数据同步      │                │
│  └──────────┴──────────┴──────────┘                         │
│         ↓                                                     │
│  ┌──────────┬──────────┐                                     │
│  │ Producer  │ Consumer  │                                    │
│  └──────────┴──────────┘                                     │
│
│ ★ RocketMQ vs Kafka 核心区别（面试必背！）：                  │
│   ┌──────────┬──────────────┬──────────────┐                 │
│   │            │ RocketMQ     │ Kafka          │              │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 注册中心    │ **NameServer** │ ZooKeeper/KRaft │          │
│   │            │ 无状态 + 简单 │ 强一致 + 复杂  │             │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 模型        │ Topic + Queue│ Topic + Partition │           │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 写模型      │ 同步刷盘/异步│ 顺序追加       │              │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 事务消息    │ ✅ 支持      │ ❌ 不支持       │              │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 延迟消息    │ ✅ 18 个等级  │ ❌ 原生不支持   │              │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 顺序消息    │ ✅ Partition内│ ✅ Partition内 │              │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 生产推荐    │ ★★★★☆      │ ★★★★★        │              │
│   └──────────┴──────────────┴──────────────┘                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、NameServer 核心源码级

### 2.1 NameServer 启动 + 注册逻辑

```java
// ★ RocketMQ NameServer 核心源码级
//
// // name/NameServer.java
//
// public class NamesrvStartup {
//
//     public static void main(String[] args) {
//         // ① 启动 RPC Server
//         NamesrvController controller = createNamesrvController(args);
//         controller.initialize();
//
//         // ② 注册 Processor（处理各种请求）
//         // Broker 注册 / 取消注册
//         controller.registerProcessor(new BrokerManageProcessor(controller));
//
//         // Topic 路由查询
//         controller.registerProcessor(new RouteInfoManagerProcessor(controller));
//
//         // 心跳处理
//         controller.registerProcessor(new HAProcessor(controller));
//
//         // ③ 启动定时任务
//         // 10s 扫描一次：超过 120s 没心跳的 Broker → 下线
//         controller.getScheduledExecutorService().scheduleAtFixedRate(
//             () -> removeExpiredBrokers(), 10, 10, TimeUnit.SECONDS);
//     }
// }
//
// ★ NameServer 核心数据结构（源码级）：
//
// public class RouteInfoManager {
//     // ★ Broker 存活表（brokerId → LiveBrokerInfo）
//     private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
//
//     // ★ Topic 路由表（topic → List<QueueData>）
//     private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
//
//     // ★ Broker 状态表（brokerAddr → BrokerStatus）
//     private final HashMap<String/* brokerAddr */, BrokerStatus> brokerStatusTable;
//
//     // ★ 心跳时间：120s 过期（超过这个时间没心跳 → Broker 下线）
//     static final long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
// }
//
```

### 2.2 Broker 注册全链路

```java
// ★ Broker 向 NameServer 注册（源码级）
//
// // broker/BrokerController.java
//
// public class BrokerController {
//     // ★ Broker 启动后定时（10s）向所有 NameServer 发心跳
//     public void start() {
//         this.scheduledExecutorService.scheduleAtFixedRate(
//             new Runnable() {
//                 @Override
//                 public void run() {
//                     // 向每个 NameServer 发心跳（带 Topic 路由信息）
//                     for (String namesrvAddr : nameServerAddrs) {
//                         try {
//                             BrokerHeartbeatData heartbeatData = buildHeartbeatData();
//                             brokerOuterAPI.sendHeartbeatToNameServer(namesrvAddr, heartbeatData);
//                         } catch (Exception e) {
//                             // NameServer 挂了 → 下次再试
//                         }
//                     }
//                 }
//             }, 10, 10, TimeUnit.SECONDS);
//     }
// }
//
// ★ 心跳数据包含：
//   ① Broker IP + Port + ID（Master/Slave）
//   ② Broker 上所有 Topic 的 Queue 数量
//   ③ Broker 状态（正常/只读/禁止写入）
//
// ★ NameServer 处理心跳：
//   // BrokerManageProcessor.processRequest()
//   收到 Broker 心跳 → 更新 brokerLiveTable → 更新心跳时间戳
//   → 把 Topic 路由更新到 topicQueueTable
//   → 返回 OK
//
```

---

## 三、NameServer 过期 Broker 清理

```java
// ★ NameServer 扫描过期 Broker（源码级）
//
// // name/RouteInfoManager.java
//
// public void removeExpiredBroker() {
//     long now = System.currentTimeMillis();
//     List<String> expiredBrokers = new ArrayList<>();
//
//     synchronized (brokerLiveTable) {
//         for (Map.Entry<String, BrokerLiveInfo> entry : brokerLiveTable.entrySet()) {
//             long lastHeartbeat = entry.getValue().getLastUpdateTimestamp();
//             // ★ 超过 120s 没心跳 → Broker 过期
//             if (now - lastHeartbeat > BROKER_CHANNEL_EXPIRED_TIME) {
//                 expiredBrokers.add(entry.getKey());
//             }
//         }
//
//         // ★ 批量移除过期 Broker
//         for (String brokerAddr : expiredBrokers) {
//             brokerLiveTable.remove(brokerAddr);
//             // 同时从 topicQueueTable 里移除这个 Broker 的 Queue
//             removeBrokerFromTopicRoute(brokerAddr);
//         }
//     }
// }
//
// ★ 面试高频：NameServer vs ZK vs Etcd？
//   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
//   │            │ NameServer    │ ZooKeeper     │ Etcd          │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 一致性模型  │ **最终一致**   │ CP（ZAB 强一致）│ CP（Raft）    │   │
//   │ 无状态      │ ✅ 完全无状态 │ ❌ Leader选举  │ ❌ Leader选举 │   │
//   │ 注册发现    │ ✅ 心跳+过期  │ ✅ Watch + Ephemeral │ ✅ Watch + Lease │
//   │ 典型应用    │ RocketMQ      │ Kafka + Dubbo │ K8s + TiDB    │
//   │ 复杂度      │ ⭐ 最低      │ ⭐⭐⭐ 高       │ ⭐⭐ 中        │
//   └──────────┴──────────────┴──────────────┴──────────────┘   │
//
// ★ 为什么 MQ 选择 NameServer 而不是 ZK？
//   ① 消息队列吞吐优先 → 最终一致足够
//   ② ZK 是 CP → 集群大了 ZK 成为瓶颈
//   ③ NameServer 无状态 → 可以任意水平扩展
//   ④ Broker 心跳 10s + 过期 120s → 足够快发现 Broker 挂了
//
```

---

## 四、Broker Master/Slave 同步

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ Broker 主从同步（对比 Kafka ISR）                │
│                                                               │
│  Kafka：Leader 写 + Follower 同步 ISR → acks=all 等所有 ISR  │
│                                                               │
│  RocketMQ：                                                  │
│   ① Broker Master 接收 Producer 写入                         │
│   ② Master 同步给所有 Slave（异步）                          │
│   ③ Producer 默认不等 Slave（acks=0）→ 吞吐高              │
│   ④ 可以配置同步刷盘（同步等 Slave + 同步刷盘）              │
│
│  ★ RocketMQ 三种刷盘策略（面试高频）：                        │
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │ 策略       │ 同步/异步     │ 可靠性         │ 吞吐量         │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ ASYNC_FLUSH│ 异步刷盘      │ 中             │ 高（默认）    │   │
│   │ SYNC_FLUSH │ 同步刷盘      │ 高（刷盘后返回）│ 低             │   │
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│  ★ RocketMQ vs Kafka 可靠性：                                 │
│   Kafka：acks=all + ISR + min.insync.replicas → 强一致       │
│   RocketMQ：SYNC_FLUSH + Broker Slave 同步 → 接近强一致     │
│   → Kafka CP 更强；RocketMQ 吞吐更高                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RocketMQ 核心组件？| **NameServer（注册中心）+ Broker（消息存储）+ Producer + Consumer** |
| 2 | NameServer 核心职责？| **Broker 注册 + Topic 路由管理 + 心跳过期清理** |
| 3 | NameServer vs ZK vs Etcd？| NameServer 无状态 + 最终一致 + 复杂度最低；ZK CP + 强一致；Etcd CP + Raft |
| 4 | Broker 注册流程？| Broker 启动 → 每 10s 向所有 NameServer 发心跳（带 Topic 路由）→ NameServer 更新表 |
| 5 | NameServer 过期清理？| **超过 120s 没心跳 → 从 brokerLiveTable 移除 + 从 topicQueueTable 移除** |
| 6 | Broker 主从同步？| Master 写 + 异步同步给 Slave；Producer 默认不等 Slave（异步刷盘）|
| 7 | 三种刷盘策略？| ASYNC_FLUSH（默认，快但可能丢）+ SYNC_FLUSH（可靠但慢）|
| 8 | RocketMQ vs Kafka 核心区别？| RocketMQ 有事务/延迟/顺序消息；Kafka ISR 强一致；RocketMQ 用 NameServer；Kafka 用 ZK/KRaft |
| 9 | 面试最高频考点排序？| **1. NameServer 无状态+心跳 2. Broker 注册 3. 过期清理 4. 刷盘策略 5. RocketMQ vs Kafka** |
| 10 | 学习路径？| NamesrvStartup → RouteInfoManager → BrokerManageProcessor → 心跳定时任务 |
| 11 | 面试常见追问？| NameServer 挂了怎么办？→ 所有 NameServer 挂了 Producer 找不到 Broker → 发不了消息；通常部署 3+ 个 NameServer |
| 12 | Producer 怎么找 Broker？| Producer 启动时 → 从配置的 NameServer 列表查 Topic 路由 → 缓存路由 → 定时刷新 |
| 13 | Consumer 怎么找 Broker？| 和 Producer 一样 → 从 NameServer 拉 Topic 路由 |
| 14 | Topic 路由表结构？| topic → List<QueueData>（每个 Queue 有 BrokerAddr + QueueId + ReadWrite 权限）|
| 15 | RocketMQ 事务消息？| **半消息（Half Message）→ 事务执行 → 提交/回滚**；Broker 定时回查未提交的半消息 |
| 16 | RocketMQ 延迟消息？| 18 个等级（1s/5s/10s/30s/1m/.../2h）；5.x 支持任意延迟 |
| 17 | 2024 新特性？| RocketMQ 5.2+ DLedger 模式（Raft 同步刷盘）+ 自动扩容 |
| 18 | 面试高频场景？| 电商下单（事务消息）+ 延迟任务（定时关单）+ 日志收集 |
| 19 | NameServer 为什么无状态？| 路由信息全在 Broker 心跳里 → NameServer 不需要持久化 → 任意重启/挂了都不影响 |
| 20 | Kafka 用 ZK/KRaft vs RocketMQ NameServer？| Kafka 需要强一致的元数据管理（Controller 选举、ISR 同步）；RocketMQ 只需要路由发现（最终一致足够）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：name/NamesrvStartup.java + name/RouteInfoManager.java  
**面试考点：20 题**  
**差异化点**：NameServer 无状态 + Broker 注册全链路 + 过期清理 120s + Kafka vs RocketMQ + 刷盘策略
