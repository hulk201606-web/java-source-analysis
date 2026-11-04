# Apache Flink Exactly-Once Sink 两阶段提交源码级深度解析

> **源码基线**：Flink 1.19.x（Java）  
> **核心仓库**：apache/flink → flink-streaming-java/.../io/TwoPhaseCommitSinkFunction.java  
> **面试权重**：⭐⭐⭐⭐⭐（端到端 Exactly-Once 核心，Checkpoint 高频延伸）

---

## 一、端到端 Exactly-Once 难题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink 内部 Exactly-Once ≠ 端到端 Exactly-Once            │
│                                                               │
│  Checkpoint 只保证 Flink **内部状态** Exactly-Once           │
│  但 Sink 写外部系统（Kafka/MySQL）时：                        │
│   ① Checkpoint barrier 对齐前写了一次                        │
│   ② 故障重启从上一个 Checkpoint 恢复 → 又写一次              │
│   → ★ 外部系统收到重复数据（At-Least-Once）                 │
│
│  例子：Sink 写 Kafka                                          │
│   处理 offset 100 的数据 → 已发送到 Kafka                    │
│   Checkpoint 在 offset 99 → 故障重启                         │
│   从 offset 99 恢复 → offset 100 再发一次 → Kafka 重复       │
│
│  ★ 解决方案：两阶段提交（2PC）Sink                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 阶段一（Pre-Commit）：数据先写外部系统"暂存区"         │  │
│  │   外部事务 BEGIN → 写数据 → 不 COMMIT                  │  │
│  │   （Kafka：开事务 writeTransaction；MySQL：XA Prepare）│  │
│  │                                                         │  │
│  │ Checkpoint 完成回调：                                    │  │
│  │ 阶段二（Commit）：Checkpoint 成功 → 正式提交           │  │
│  │ Checkpoint 失败/废弃 → Abort（回滚，外部看不到数据）   │  │
│  └───────────────────────────────────────────────────────┘  │
│
│  ★ 端到端 EOS 三个必要条件（面试必背！）：                    │
│   ① Source 可重放（Kafka 能按 offset 重读）                 │
│   ② Flink Checkpoint（内部状态一致）                        │
│   ③ Sink 两阶段提交 / 幂等写入（外部不重复）               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TwoPhaseCommitSinkFunction 源码级

### 2.1 核心抽象类

```java
// ★ Flink 两阶段提交 Sink 核心（源码级，面试核心！）
//
// // flink-streaming-java/.../api/functions/sink/TwoPhaseCommitSinkFunction.java
//
// public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT>
//         extends RichSinkFunction<IN>
//         implements CheckpointedFunction, CheckpointListener {
//
//     // ★ 当前正在写入的事务（每个 Checkpoint 周期一个）
//     protected transient TXN currentTransaction;
//
//     // ★ 已 PreCommit、等待 Checkpoint 完成通知的事务
//     private final List<TransactionHolder<TXN>> pendingCommitTransactions;
//
//     // ====== 子类必须实现的四个抽象方法 ======
//
//     /** 开启一个新事务（外部系统 BEGIN） */
//     protected abstract TXN beginTransaction() throws Exception;
//
//     /** 把数据写入事务（但还没提交，外部不可见） */
//     protected abstract void invoke(TXN transaction, IN value, Context context);
//
//     /** 阶段一：预提交（PREPARE，事务必须能被恢复和重试） */
//     protected abstract void preCommit(TXN transaction) throws Exception;
//
//     /** 阶段二：正式提交（COMMIT，数据对外可见） */
//     protected abstract void commit(TXN transaction);
//
//     /** 中止/回滚事务（ABORT） */
//     protected abstract void abort(TXN transaction);
// }
//
```

### 2.2 与 Checkpoint 的完整时序

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 两阶段提交 × Checkpoint 完整时序（面试必背！）            │
│                                                               │
│  时间轴：                                                      │
│                                                               │
│  T1: Sink 开始处理数据                                        │
│      → beginTransaction() 开启事务 txn-A                      │
│      → invoke(txn-A, records) 数据写入事务（未提交）        │
│                                                               │
│  T2: ★ Checkpoint Barrier n 到达 Sink                        │
│      snapshotState() 被调用：                                  │
│        ① preCommit(txn-A)        ← 阶段一：PREPARE           │
│        ② 把 txn-A 的句柄存进 OperatorState（待提交列表）    │
│        ③ beginTransaction() 开启 txn-B（给下个周期用）       │
│      → Barrier 带状态发往 JM（Checkpoint 继续）             │
│                                                               │
│  T3: ★ JM 通知整个 Checkpoint n 完成（所有算子都成功）       │
│      notifyCheckpointComplete(n) 被调用：                     │
│        ④ commit(txn-A)           ← 阶段二：COMMIT           │
│           → txn-A 的数据对消费者可见                         │
│                                                               │
│  T4: 如果 Checkpoint n 失败 / 任务废弃：                      │
│        abort(txn-A)             ← 回滚，外部看不到数据       │
│
│  ★ 关键点（面试高频）：                                       │
│   preCommit 和 commit 之间有"时间差"                          │
│   （等整个 Checkpoint 完成）→ 数据短暂不可见                 │
│   如果在 commit 前 JM 挂了：                                   │
│   重启后从 OperatorState 恢复 pending 的 txn-A               │
│   → 收到 CheckpointComplete 通知就重试 commit               │
│   → ★ commit 必须幂等（重试提交不能产生重复）               │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.3 状态恢复逻辑

```java
// ★ 故障恢复时的悬挂事务处理（源码级，面试高频！）
//
// // initializeState()：从 Checkpoint 恢复
// public void initializeState(FunctionInitializationContext context) {
//     // ① 恢复所有"已 preCommit 但不确定是否 commit"的事务句柄
//     List<State<TXN, CONTEXT>> restored =
//         operatorState.get();
//
//     for (State<TXN, CONTEXT> state : restored) {
//         // ★ 恢复后必须恢复这些事务，之后：
//         //   收到 CheckpointComplete → commit
//         //   收到 CheckpointAborted → abort
//         pendingCommitTransactions.add(state.transaction);
//
//         // ★ 关键：commit/abort 都要重试直到成功
//         // 因为崩溃可能发生在 commit 发出后、确认前
//         // 外部系统可能已经提交，也可能没有 → 必须靠幂等
//     }
// }
//
// // Checkpoint 废弃回调（Flink 1.11+）
// public void notifyCheckpointAborted(long checkpointId) {
//     // ★ Checkpoint 确定失败 → 回滚对应事务
//     abort(pendingTransactions.remove(checkpointId));
// }
//
```

---

## 三、Kafka 事务 Sink 实战

```
┌─────────────────────────────────────────────────────────────┐
│  ★ FlinkKafkaProducer 两阶段提交（面试高频案例）              │
│                                                               │
│  Kafka 0.11+ 原生事务 API：                                   │
│   producer.initTransactions();                              │
│   producer.beginTransaction();                               │
│   producer.send(records);                                    │
│   producer.sendOffsetsToTransaction(offsets, group);  // 连带 offset│
│   producer.commitTransaction();  / abortTransaction();       │
│
│  Flink Kafka Sink 映射：                                      │
│   beginTransaction() → KafkaProducer.beginTransaction()      │
│   invoke()            → producer.send()（写目标 Topic）     │
│   preCommit()         → flush() + sendOffsetsToTransaction() │
│   commit()            → commitTransaction()                  │
│   abort()             → abortTransaction()                   │
│
│  ★ 精妙之处：把 Kafka Source 的消费 offset 也纳入同一事务    │
│   → 输出数据 + offset 提交原子化                             │
│   → 即使故障也不会"发了数据但 offset 没提交"                 │
│
│  配置：                                                        │
│   KafkaSink.<String>builder()                                │
│       .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)  │
│       .setTransactionalIdPrefix("flink-txn-")  // ★ 事务 ID 前缀│
│       .build();
│
│  ★ 事务 ID 重要性（面试坑）：                                 │
│   transactional.id 必须稳定 + 带子任务编号                   │
│   → 恢复后用相同 ID 才能 abort 掉旧 Producer 的悬挂事务     │
│   → Kafka 通过 Producer Epoch（fencing）防止僵尸 Producer   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、幂等 Sink（两阶段提交的替代方案）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 实现端到端 EOS 的第二条路：幂等写入                       │
│                                                               │
│  有些外部系统不支持事务（如 Redis、部分 HTTP API）           │
│  → 用幂等代替 2PC：                                          │
│                                                               │
│  ① 主键 UPSERT：                                             │
│     INSERT ... ON DUPLICATE KEY UPDATE                       │
│     重复写同一主键 → 结果一样，不产生重复                    │
│                                                               │
│  ② 去重表 / 唯一键：                                         │
│     用业务唯一 ID（订单号 + 操作类型）建唯一索引            │
│     重复插入 → 抛唯一键冲突 → 忽略                          │
│                                                               │
│  ③ 带版本号 CAS：                                            │
│     UPDATE SET value=?, version=version+1                    │
│     WHERE id=? AND version=?                                 │
│                                                               │
│  ★ 2PC vs 幂等对比（面试高频）：                              │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │            │ 两阶段提交     │ 幂等写入       │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 外部要求    │ 必须支持事务   │ 有唯一键即可   │                │
│  │ 延迟       │ commit 前不可见│ 立即可见       │                │
│  │ 实现复杂度  │ 高            │ 低             │                │
│  │ 典型       │ Kafka 事务/XA │ UPSERT/去重表 │                │
│  └──────────┴──────────────┴──────────────┘                │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、Exactly-Once 语义级别辨析

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 三种投递语义（面试必背）                                  │
│                                                               │
│  At-Most-Once（最多一次）：可能丢，不重复                    │
│    → 不做 Checkpoint 或直接发送不等确认                      │
│                                                               │
│  At-Least-Once（至少一次）：不丢，但可能重复（默认）        │
│    → 有 Checkpoint 但 Sink 没有 2PC/幂等                    │
│                                                               │
│  Exactly-Once（精确一次）：不丢不重                          │
│    → Source 可重放 + Checkpoint + Sink 2PC/幂等             │
│
│  ★ 面试坑：Flink Checkpoint = Exactly-Once？                 │
│   ❌ 不准确！Checkpoint 只保证**算子状态** EOS               │
│   端到端 EOS 还需要 Sink 事务/幂等                           │
│   这是面试官最爱追问的点！                                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 为什么 Checkpoint 还不够 EOS？| Checkpoint 只管内部状态；Sink 故障重试会向外部系统重复写 |
| 2 | 端到端 EOS 三条件？| **①Source 可重放 ②Checkpoint ③Sink 两阶段提交/幂等** |
| 3 | 两阶段提交 Sink 四方法？| **beginTransaction + invoke + preCommit + commit（+abort）** |
| 4 | preCommit 何时调用？| **Checkpoint Barrier 到达 Sink 的 snapshotState() 时** |
| 5 | commit 何时调用？| **JM 通知整个 Checkpoint 完成的 notifyCheckpointComplete() 时** |
| 6 | preCommit 和 commit 之间？| 等整个 Checkpoint 完成，数据短暂不可见 |
| 7 | commit 前 JM 挂了？| 从 OperatorState 恢复 pending 事务 → 重试 commit（**必须幂等**）|
| 8 | 核心抽象类？| TwoPhaseCommitSinkFunction（实现 CheckpointedFunction + CheckpointListener）|
| 9 | Kafka Sink 怎么实现？| Kafka 原生事务；preCommit 时 flush + sendOffsetsToTransaction |
| 10 | 为什么 offset 也放进事务？| 输出数据+offset 提交原子化，杜绝"发了但 offset 没提交" |
| 11 | transactional.id 要求？| 稳定且带子任务编号；恢复后用同 ID abort 悬挂事务（Producer Epoch fencing）|
| 12 | notifyCheckpointAborted？| Checkpoint 确定失败 → abort 对应事务（Flink 1.11+）|
| 13 | 幂等 Sink 方案？| 主键 UPSERT + 唯一键去重表 + 版本号 CAS |
| 14 | 2PC vs 幂等？| 2PC 需要外部支持事务（延迟可见）；幂等只需唯一键（立即可见，实现简单）|
| 15 | 三种投递语义？| At-Most-Once / At-Least-Once / Exactly-Once |
| 16 | 面试最高频考点排序？| **1. 端到端三条件 2. 四方法时序 3. 悬挂事务恢复 4. Kafka 事务 5. 2PC vs 幂等** |
| 17 | 学习路径？| TwoPhaseCommitSinkFunction → snapshotState/notifyCheckpointComplete → FlinkKafkaProducer → 幂等 |
| 18 | 面试常见追问？| commit 为什么必须幂等？→ 崩溃可能在 commit 发出后确认前，重试不能产生重复 |
| 19 | pendingCommitTransactions？| 已 preCommit 等 Checkpoint 完成通知的事务；存 OperatorState 可恢复 |
| 20 | EOS 性能开销？| 事务使数据延迟到 Checkpoint 完成才可见；Checkpoint 间隔越短延迟越低但开销越大 |
| 21 | Flink 新 Sink API？| 1.14+ Sink/CommittingSinkWriter 接口（TwoPhaseCommitSinkFunction 的现代化替代）|
| 22 | 面试陷阱？| 说"开了 Checkpoint 就是 Exactly-Once" → ❌ 必须说明是内部状态，端到端还需 Sink 配合 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：flink-streaming-java/.../functions/sink/TwoPhaseCommitSinkFunction.java  
**面试考点：22 题**  
**差异化点**：Barrier×2PC 完整时序 + 悬挂事务恢复 + Kafka 事务 offset 原子化 + 2PC vs 幂等 + 三语义辨析
