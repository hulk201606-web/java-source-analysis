# Apache RocketMQ CommitLog + ConsumeQueue 存储源码级深度解析

> **源码基线**：RocketMQ 5.2.x（Java）  
> **核心位置**：store/src/main/java/org/apache/rocketmq/store/  
> **面试权重**：⭐⭐⭐⭐⭐（RocketMQ 高性能存储核心，对比 Kafka 高频）

---

## 一、RocketMQ 存储架构总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ 存储：CommitLog + ConsumeQueue 分离设计         │
│                                                               │
│  和 Kafka 的本质区别（面试必背）：                            │
│   Kafka：一个 Partition 一个独立日志文件                     │
│          → 分区多 = 大量文件同时写 = 随机 IO                │
│   RocketMQ：★ 所有 Topic 的消息都顺序写【同一个 CommitLog】  │
│          → ConsumeQueue 只是消息的"索引/投递队列"           │
│
│  写入链路：                                                    │
│  Producer 消息 → CommitLog（实际消息体，全局顺序写）        │
│                      ↓ 后台线程异步构建索引                  │
│                 ConsumeQueue（每 Topic-Queue 一个，逻辑队列）│
│                      ↓                                        │
│                 Consumer 按 ConsumeQueue 定位消息再读 CL     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、CommitLog 源码级

### 2.1 顺序写 + 内存映射

```java
// ★ CommitLog 核心（源码级，面试核心！）
//
// store/.../CommitLog.java
//
// // 一个 Broker 上所有 Topic 的消息都写到这里
// // 文件固定大小（mappedFileSizeCommitLog，默认 1GB）
// // 文件名 = 该文件第一条消息的全局偏移量（20 位补零）
// //   00000000000000000000
// //   00000000001073741824
//
// public class CommitLog {
//     private final MappedFileQueue mappedFileQueue;  // 一组 1GB 的 MappedFile
//
//     // ★ 消息追加（顺序写）
//     public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
//         MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
//         // 写入 CommitLog 条目（含消息体 + 元数据）
//         AppendMessageResult result = mappedFile.appendMessage(msg, this.appendCallback);
//         return putMessageResult;
//     }
// }
//
// // MappedFile：用 mmap 把文件映射到内存
// // store/.../MappedFile.java
// // fileChannel.map(MapMode.READ_WRITE, 0, fileSize)
// // → 写 mappedByteBuffer 等价于写页缓存（顺序追加）
// // → 配合 TransientStorePool（堆外内存池）可先写 DirectByteBuffer
// //   再 commit 到 FileChannel（write）
//
```

### 2.2 CommitLog 消息条目结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ CommitLog 单条消息物理格式（面试高频）                    │
│                                                               │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │
│  │ 总长度     │ 魔数      │ 队列 ID   │ 队列偏移  │ 消息体    │  │
│  │ TOTALSIZE │ MAGICCODE│ QUEUEID  │ QUEUEOFFSET│ BODY ... │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
│  还包含：StoreTimestamp（存储时间）、Topic 长度+Topic、       │
│         Properties（tag/key/delay level）、CRC 校验          │
│                                                               │
│  每条消息在 CommitLog 有全局唯一 offset（物理偏移）          │
│  ConsumeQueue 索引项就指向这个 offset                         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、ConsumeQueue 源码级

### 3.1 逻辑消费队列（索引）

```java
// ★ ConsumeQueue 核心（源码级，面试核心！）
//
// store/.../ConsumeQueue.java
//
// // 每个 Topic 的每个 MessageQueue 对应一个 ConsumeQueue 目录
// // 路径：store/consumequeue/{topic}/{queueId}/
// //   00000000000000000000
// //   00000000000005000000  （每个文件默认 30W 条目 × 20 字节）
//
// // ★ 每个索引条目固定 20 字节（面试必背！）
// //  ┌──────────────┬──────────────┬──────────────┐
//  │ CommitLog Offset（8 字节）│ 消息大小（4字节）│ Tag HashCode（8字节）│
//  └──────────────┴──────────────┴──────────────┘
//
// public class ConsumeQueue {
//     // ★ ReputMessageService 后台线程不断从 CommitLog 分发构建索引
//     // 读到新消息 → 根据 topic/queueId 追加一条 20 字节索引到对应 CQ
// }
//
```

### 3.2 ReputMessageService 异步分发

```java
// ★ CommitLog → ConsumeQueue 的异步构建（源码级）
//
// store/.../ReputMessageService.java（CommitLog 内部线程）
//
// // 后台线程循环：
// //  ① 从 CommitLog 的 dispatchedOffset 开始顺序读新消息
// //  ② 解析出 topic、queueId、commitLogOffset、size、tagHash
// //  ③ 异步分发：
// //     - 写 ConsumeQueue 索引（20 字节定长条目）
// //     - 写 IndexFile（可选，按 key/时间查消息用）
// //  ④ 推进 dispatchedOffset
//
// ★ 设计意义：
//   Producer 写消息只需要顺序写 CommitLog（极快）
//   构建消费索引是后台异步的，不阻塞写入主路径
//
```

### 3.3 消费读取链路

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Consumer 拉取消息的读取路径（面试必背）                  │
│                                                               │
│  ① Consumer 按 queue 的 consumeOffset（逻辑位点）拉取        │
│  ② 计算 ConsumeQueue 文件和条目位置：                        │
│      物理位置 = consumeOffset × 20 字节                      │
│  ③ 读出 20 字节索引：                                        │
│      CommitLog Offset + Size + TagHash                       │
│  ④ Tag 过滤（SQL92/Tag 表达式先比对 TagHash，省得读正文）   │
│  ⑤ 通过 CommitLog Offset + Size 去 CommitLog 读消息体       │
│  ⑥ 返回给 Consumer                                            │
│                                                               │
│  ★ 两级索引好处：                                             │
│   消费队列文件极小（定长 20 字节，可全放 Page Cache/mmap）   │
│   真正的大消息体集中在 CommitLog 顺序组织                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、IndexFile（辅助索引）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ IndexFile = 按业务 Key / 时间查消息（面试高频）          │
│                                                               │
│  路径：store/index/  文件名是创建时间戳                      │
│                                                               │
│  结构（类似 HashMap 的哈希索引）：                           │
│  ┌────────────┐                                              │
│  │ Header      │ 起始/结束时间戳、begin/end phyOffset       │
│  ├────────────┤                                              │
│  │ Slot Table  │ 500w 个哈希槽（key hash → 链表头）         │
│  ├────────────┤                                              │
│  │ Index Linked│ 2000w 个索引项（keyHash + phyOffset +      │
│  │ List        │  timeDiff + 前一项指针，解决哈希冲突）      │
│  └────────────┘                                              │
│                                                               │
│  用途：                                                        │
│   messageSelectorOrderByKey / 按 key 查询消息                │
│   消息轨迹、事务回查（根据 key 找消息）                      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、刷盘 + 零拷贝对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ 刷盘与读取（面试高频）                           │
│                                                               │
│  刷盘两策略：                                                  │
│   ASYNC_FLUSH（默认）：写 Page Cache 即返回，后台 flush      │
│   SYNC_FLUSH：Group Commit 攒批，真正 fsync 后才返回        │
│                                                               │
│  读消息零拷贝：                                                │
│   Consumer 拉 CommitLog → FileChannel.transferTo → sendfile │
│   和 Kafka 一样用零拷贝（之前 Kafka 文档讲过机制）          │
│                                                               │
│  内存映射写：                                                  │
│   CommitLog 用 mmap 写（MappedByteBuffer）                  │
│   可选 TransientStorePool 堆外内存 + write + flush 分离     │
│   （写堆外 DirectBuffer → commit 到 FileChannel，写读分离）│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、RocketMQ vs Kafka 存储对比（核心！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocketMQ CommitLog vs Kafka Partition Log（面试必背表）  │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │ 维度       │ RocketMQ      │ Kafka          │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 消息写入   │ 全局单 CommitLog│ 每 Partition 独立日志│         │
│  │ 文件组织   │（所有 Topic 混写）│（按分区独立文件）│         │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 消费索引   │ ★ ConsumeQueue│ offset 就在日志内│              │
│  │           │（20B 定长索引）│（+稀疏索引）   │              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ Topic/分区多│ 仍单文件顺序写│ 文件多→随机 IO │              │
│  │           │（优势明显）   │（分区数受限）   │              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 读取       │ CQ 索引→CL 读│ 直接读分区日志 │              │
│  │           │（多一次索引）  │（更直接）      │              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 消息查找   │ IndexFile 支持│ 时间戳索引     │              │
│  │           │按 key/时间    │                │              │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 适用       │ 海量 Topic/队列│ 分区数适中     │              │
│  └──────────┴──────────────┴──────────────┘                │
│                                                               │
│  ★ 一句话总结：                                                │
│   RocketMQ 用"单一 CommitLog 顺序写 + 异步建多份逻辑索引"    │
│   换来了海量 Topic/Queue 下依然保持磁盘顺序写                │
│   代价是消费多读一次索引（但 CQ 极小，常驻缓存）            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RocketMQ 存储核心？| **CommitLog（消息体，全局顺序写）+ ConsumeQueue（逻辑消费索引）** |
| 2 | 与 Kafka 存储最大区别？| RocketMQ 所有 Topic 混写单 CommitLog；Kafka 每 Partition 独立日志 |
| 3 | 为什么用单 CommitLog？| 海量 Topic/Queue 时仍保持磁盘**顺序写**，避免多文件随机 IO |
| 4 | CommitLog 文件大小？| 默认 1GB 一个 MappedFile，文件名=起始全局 offset |
| 5 | CommitLog 怎么写？| mmap（MappedByteBuffer）顺序追加；可选 TransientStorePool 堆外写 |
| 6 | ConsumeQueue 是什么？| 每 Topic-Queue 一个的逻辑队列，存指向 CommitLog 的索引 |
| 7 | CQ 索引条目结构？| **固定 20 字节：CommitLog Offset(8) + Size(4) + TagHash(8)** |
| 8 | CQ 索引谁构建？| **ReputMessageService 后台线程异步从 CommitLog 分发构建** |
| 9 | 消费读取链路？| consumeOffset×20 定位 CQ → 读 20B 索引 → TagHash 过滤 → 按 offset+size 读 CommitLog |
| 10 | 为什么 TagHash 在索引里？| 先比对 hash 做 Tag 过滤，不用读消息体，省 IO |
| 11 | IndexFile 作用？| 按消息 Key/时间范围查消息（哈希槽+链表，事务回查、消息轨迹）|
| 12 | 刷盘策略？| ASYNC_FLUSH（默认，写缓存返回）/ SYNC_FLUSH（Group Commit fsync）|
| 13 | 读消息零拷贝？| transferTo → sendfile（同 Kafka）|
| 14 | ConsumeQueue 文件大小？| 默认 30W 条 × 20B ≈ 5.72MB，小到常驻 Page Cache |
| 15 | 消息写入后立即可见吗？| CommitLog 写完后要等 ReputMessageService 异步建好 CQ 索引才可被消费 |
| 16 | 面试最高频考点排序？| **1. CommitLog/CQ 分离 2. 20B 索引 3. 异步分发 4. vs Kafka 5. 消费读链路** |
| 17 | 学习路径？| CommitLog.putMessage → MappedFile mmap → ReputMessageService → ConsumeQueue → IndexFile |
| 18 | 面试常见追问？| 异步建索引会丢消息吗？→ 消息已持久化在 CommitLog，索引重建可恢复 |
| 19 | ConsumeQueue vs Kafka offset？| CQ 是独立定长索引文件；Kafka offset 直接是分区日志内偏移（配稀疏索引）|
| 20 | TransientStorePool？| 堆外 DirectBuffer 先写，再 commit 到 FileChannel，写读分离减少页缓存竞争 |
| 21 | 多个 ConsumerGroup 怎么独立消费？| 各自维护消费进度，共享同一份 ConsumeQueue 索引（offset 单独存）|
| 22 | 消费进度存哪？| 集群模式存 Broker（offsets.json/inner topic）；广播模式存客户端 |
| 23 | 面试高频场景？| 海量 Topic 高吞吐选型 + 消息堆积原理 + 按 key 查消息 |
| 24 | 面试陷阱？| 说 RocketMQ 一个 Topic 一个物理文件 → ❌ 所有 Topic 共享 CommitLog，Topic 只是 CQ 逻辑划分 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：store/.../CommitLog.java + ConsumeQueue.java + ReputMessageService.java + IndexFile.java  
**面试考点：24 题**  
**差异化点**：CommitLog 单文件顺序写 + CQ 20 字节定长索引 + 异步分发 + 两级读取 + vs Kafka 存储本质差异
