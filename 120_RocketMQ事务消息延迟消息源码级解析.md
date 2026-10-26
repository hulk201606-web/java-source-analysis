# Apache RocketMQ 事务消息 + 延迟消息源码级深度解析

> **源码基线**：RocketMQ 5.2.x（Java）  
> **核心仓库**：apache/rocketmq → client/ + broker/  
> **面试权重**：⭐⭐⭐⭐（RocketMQ 特色，Kafka 没有的能力）

---

## 一、RocketMQ 特色功能总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ 相比 Kafka 的三大独家特色                        │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 特色       │ 核心机制       │ 谁有           │ 面试高频     │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **事务消息** │ 半消息 + 本地事务 +回查│ ✅ RocketMQ 独家│ ⭐⭐⭐⭐⭐      │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **延迟消息** │ 18 等级定时器    │ ✅ RocketMQ      │ ⭐⭐⭐⭐      │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **顺序消息** │ Queue 有序消费 │ ✅ Kafka 也有    │ ⭐⭐⭐        │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ RocketMQ vs Kafka 全景对比（面试高频）：                     │
│   ┌──────────┬──────────────┬──────────────┐                 │
│   │            │ RocketMQ     │ Kafka          │               │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 注册中心    │ NameServer    │ ZooKeeper/KRaft │             │
│   │ 事务消息    │ ✅ 原生支持   │ ❌ 不支持       │               │
│   │ 延迟消息    │ ✅ 18 等级    │ ❌ 不原生支持     │               │
│   │ 刷盘策略    │ 同步/异步     │ 顺序追加        │               │
│   │ 适用场景    │ 电商/金融    │ 实时流/日志      │               │
│   │ 一致性模型    │ AP           │ CP（Raft/ISR）  │               │
│   └──────────┴──────────────┴──────────────┘                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、事务消息源码级

### 2.1 半消息 + 本地事务 + 回查

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ 事务消息完整流程（面试必背！）                    │
│                                                               │
│  场景：订单创建 + 消息通知下游（扣库存/发优惠券）             │
│  要求：订单创建和消息通知必须一致 → 要么都成功要么都失败      │
│                                                               │
│  ① 发送半消息（Half Message / Prepare）                     │
│     Producer → Broker：SEND_TRANSACTION_MESSAGE（半消息）   │
│     Broker 保存半消息 → 返回成功 → **消费者看不到半消息！**  │
│
│  ② 执行本地事务                                               │
│     Producer 收到 Broker 成功 → 执行本地事务（INSERT INTO orders）│
│     → 本地事务成功？返回 COMMIT → 失败？返回 ROLLBACK        │
│
│  ③ 提交/回滚消息                                              │
│     Producer → Broker：COMMIT_MESSAGE / ROLLBACK_MESSAGE    │
│     Broker 收到 COMMIT：半消息变正常消息 → Consumer 可见    │
│     Broker 收到 ROLLBACK：半消息删除 → Consumer 看不到      │
│
│  ④ 事务回查（补偿机制 ★）                                    │
│     Broker 定期（默认 60s）扫描半消息超过事务检查时间的消息   │
│     → 向 Producer 发起事务状态查询（PRODUCER_INNER_MESSAGE）│
│     Producer 根据本地事务记录返回 COMMIT/ROLLBACK/UNKNOWN    │
│     → Broker 根据返回处理半消息                               │
│
│ ★ 面试高频：半消息为什么消费者看不到？                          │
│   Broker 内部有特殊标记 → Consumer 拉取时自动过滤掉         │
│   → 只有 COMMIT 后消息才对 Consumer 可见                     │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 事务消息源码级

```java
// ★ RocketMQ 事务消息 Producer 源码级
//
// // producer/TransactionProducer.java
//
// public class OrderProducer implements TransactionListener {
//
//     // ★ Step 1: 发送半消息
//     public TransactionSendResult sendMessage(String orderId) {
//         Message msg = new Message("order_topic", "create", orderId);
//         return transactionProducer.sendMessageInTransaction(
//             msg, null  // 传入一个 arg（可以传订单信息）
//         );
//     }
//
//     // ★ Step 2: 执行本地事务（在 sendMessageInTransaction 内部调用）
//     @Override
//     public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
//         String orderId = new String(msg.getBody());
//
//         try {
//             // ★ 执行本地事务
//             orderService.createOrder(orderId);
//             return LocalTransactionState.COMMIT_MESSAGE;  // ✅ 成功 → 提交
//         } catch (Exception e) {
//             log.error("create order failed", e);
//             return LocalTransactionState.ROLLBACK_MESSAGE;  // ❌ 失败 → 回滚
//         }
//     }
//
//     // ★ Step 4: 事务回查（Broker 定期调用）
//     @Override
//     public LocalTransactionState checkLocalTransaction(MessageExt msg) {
//         String orderId = new String(msg.getBody());
//
//         // 根据本地事务记录判断订单是否存在
//         Order order = orderService.findByOrderId(orderId);
//         if (order != null) {
//             return LocalTransactionState.COMMIT_MESSAGE;  // 已创建 → 提交
//         } else {
//             return LocalTransactionState.ROLLBACK_MESSAGE;  // 没创建 → 回滚
//         }
//     }
// }
//
```

### 2.3 事务消息 Broker 处理

```java
// ★ RocketMQ 事务消息 Broker 端处理源码级
//
// // broker/transaction/TransactionMessageCheckService.java
//
// public class TransactionMessageCheckService {
//
//     // ★ Broker 定期扫描半消息（默认 60s）
//     public void start() {
//         this.scheduledExecutorService.scheduleAtFixedRate(
//             () -> {
//                 // 1. 扫描所有待检查的半消息
//                 List<MessageExt> halfMessages = brokerStore.selectHalfMessageByTime(
//                     System.currentTimeMillis() - 10 * 60 * 1000  // 超过 10min 没提交/回滚
//                 );
//
//                 for (MessageExt msg : halfMessages) {
//                     // 2. 向 Producer 发起回查请求
//                     // 发送 PRODUCER_INNER_MESSAGE 到 Producer 的回调端口
//                     TransactionState state = producerProxy.checkTransaction(msg);
//
//                     // 3. 根据 Producer 返回处理
//                     if (state == COMMIT) {
//                         // ✅ 提交：把半消息变正常消息
//                         brokerStore.commitHalfMessage(msg);
//                     } else if (state == ROLLBACK) {
//                         // ❌ 回滚：删除半消息
//                         brokerStore.rollbackHalfMessage(msg);
//                     } else {
//                         // UNKNOWN：暂不处理，下次再查
//                     }
//                 }
//             }, 60, 60, TimeUnit.SECONDS);
//     }
// }
//
// ★ 面试高频：回查机制解决什么问题？
//   Producer 执行本地事务时挂了 → Broker 不知道该提交还是回滚
//   → Broker 定期回查 Producer → Producer 根据本地事务记录返回状态
//   → 保证最终一致（最多 60s 延迟）
//
```

---

## 三、延迟消息源码级

### 3.1 18 等级定时消息

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ 延迟消息（旧版本 18 等级，源码级）                 │
│                                                               │
│  18 个固定等级（ms）：                                         │
│   1s / 5s / 10s / 30s / 1m / 2m / 3m / 4m / 5m / 6m / 7m     │
│   / 8m / 9m / 10m / 20m / 30m / 1h / 2h                     │
│
│  Producer 发送：                                               │
│   msg.putProperty("KEYS", "ORDER_CLOSE_123");                 │
│   msg.putProperty("DELAY", "100");  // 100 = 10min 延迟      │
│
│  Broker 处理核心机制（源码级）：                                │
│
│  // broker/schedule/ScheduleMessageService.java              │
│  public class ScheduleMessageService {
//
//     // ★ Broker 内部有 18 个定时 Topic（SCHEDULE_TOPIC_0 ~ _17）
//     // SCHEDULE_TOPIC_3 → 对应 30s 延迟
//     // SCHEDULE_TOPIC_16 → 对应 1h 延迟
//
//     public void putMessage(MessageExt msg, int delayLevel) {
//         // ① 消息先写到对应的定时 Topic（不是用户的业务 Topic）
//         String scheduleTopic = "SCHEDULE_TOPIC_" + delayLevel;
//         Message scheduleMsg = msg.clone();
//         scheduleMsg.setTopic(scheduleTopic);
//         brokerStore.putMessage(scheduleMsg);
//
//         // ② 定时器到时间后再转发到用户 Topic
//         this.deliverTaskExecutor.schedule(
//             () -> deliverMessage(msg, delayLevel),
//             getDelayMs(delayLevel), TimeUnit.MILLISECONDS
//         );
//     }
//
//     public void deliverMessage(MessageExt msg, int delayLevel) {
//         // ③ 到时间后：从定时 Topic 拉消息 → 投递到用户业务 Topic
//         String userTopic = msg.getProperty("REAL_TOPIC");
//         Message realMsg = msg.clone();
//         realMsg.setTopic(userTopic);
//         brokerStore.putMessage(realMsg);
//     }
// }
//
// ★ 面试高频：18 等级延迟消息缺点？                               │
//   固定等级 → 不能精确延迟（比如 99s）                          │
//   定时 Topic 18 个 → 每个 Topic 都有单独的消费者线程            │
//
└─────────────────────────────────────────────────────────────┘
```

### 3.2 RocketMQ 5.x 任意延迟（新方案）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ 5.1+ 任意延迟消息（新方案，源码级）                │
│                                                               │
│  5.1+ 支持任意延迟时间（不再限制 18 等级）                    │
│  实现机制：基于时间轮（Time Wheel）                           │
│                                                               │
│  // broker/schedule/ScheduleTimeWheelService.java             │
│  public class ScheduleTimeWheelService {
//
//     private final TimeWheel wheel;  // ★ 时间轮数据结构
//
//     // ★ 把消息放到时间轮的对应槽位
//     public void scheduleMessage(MessageExt msg, long delayMs) {
//         long deliverTime = System.currentTimeMillis() + delayMs;
//
//         // 时间轮：按时间粒度分层
//         // 比如：秒级、分钟级、小时级、天级
//         wheel.addTask(deliverTime, () -> deliverMessage(msg));
//     }
//
//     // ★ 时间轮到了 → 执行任务（投递到用户 Topic）
//     public void tick() {
//         List<ScheduledTask> tasks = wheel.advance();
//         for (ScheduledTask task : tasks) {
//             task.execute();  // deliverMessage
//         }
//     }
// }
//
// ★ 时间轮 vs 18 等级：                                          │
//   18 等级：固定延迟 + 18 个定时 Topic                          │
//   时间轮：任意延迟 + 高效 O(1) 插入和执行                       │
//   → 5.x 时间轮是更优雅的实现                                   │
//
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RocketMQ 三大独家特色？| **事务消息 + 延迟消息 + 顺序消息**（Kafka 没有事务和延迟）|
| 2 | 事务消息四步流程？| **① 半消息 ② 本地事务 ③ 提交/回滚 ④ 回查补偿** |
| 3 | 半消息是什么？| Broker 保存但 Consumer 不可见；Commit 后才可见 |
| 4 | 事务回查机制？| Broker 定期（60s）扫描未提交半消息 → 回查 Producer → Producer 根据本地事务返回状态 |
| 5 | 事务消息源码核心？| TransactionProducer + TransactionListener（executeLocalTransaction + checkLocalTransaction）|
| 6 | 延迟消息 18 等级？| 1s~2h 共 18 个固定延迟；SCHEDULE_TOPIC_0~17 内部 Topic |
| 7 | RocketMQ 5.x 延迟消息？| **时间轮（Time Wheel）** 实现任意延迟 |
| 8 | 事务消息 vs 分布式事务？| RocketMQ 事务消息：**本地事务 + 消息原子**（不是跨服务分布式事务）|
| 9 | 面试最高频考点排序？| **1. 事务消息四步 2. 回查机制 3. 半消息 4. 延迟消息 18 等级 5. RocketMQ vs Kafka** |
| 10 | 学习路径？| TransactionProducer → TransactionListener → Broker TransactionService → ScheduleMessageService → TimeWheel |
| 11 | 面试常见追问？| 事务消息能保证分布式事务吗？→ 只能保证本地事务和消息发送原子；跨服务需要 Seata/TCC |
| 12 | Producer 执行本地事务挂了？| Broker 回查 → Producer 根据本地事务记录返回状态 → 最终一致 |
| 13 | 事务消息 COMMIT 后 Broker 挂了？| 消息可能丢？→ 不丢（RocketMQ 有刷盘 + 主从同步）|
| 14 | 延迟消息能不能延迟到具体时间？| 旧版不能（18 等级）；RocketMQ 5.1+ 用时间轮支持任意延迟 |
| 15 | 2024 新特性？| RocketMQ 5.2+ **事务消息回查增强** + **任意延迟消息时间轮优化** |
| 16 | 面试高频场景？| 电商订单超时关单（延迟消息）+ 订单支付通知（事务消息）+ 优惠券发放 |
| 17 | 事务消息 vs Seata TCC？| 事务消息：本地事务+消息原子；TCC：跨服务 Try/Confirm/Cancel；两者解决问题不同 |
| 18 | RocketMQ 事务消息源码路径？| client/TransactionProducer.java + broker/transaction/TransactionService.java |
| 19 | 回查间隔？| 默认 60s 扫描一次半消息；最多查 15 次（15 次后丢弃）|
| 20 | 面试陷阱？| 把 RocketMQ 事务消息 = 分布式事务 → ❌ 只是本地事务+消息原子；跨服务需要 Seata/TCC |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：client/TransactionProducer.java + broker/schedule/  
**面试考点：20 题**  
**差异化点**：事务消息四步 + 回查补偿 + 延迟消息 18 等级 + 时间轮新方案
