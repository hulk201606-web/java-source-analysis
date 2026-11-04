# Apache Kafka 分区副本再分配（Reassignment）源码级深度解析

> **源码基线**：Kafka 3.8.x（Scala + Java）  
> **核心位置**：core/src/main/scala/kafka/server/ + tools/ + metadata/  
> **面试权重**：⭐⭐⭐⭐（集群扩缩容/负载再均衡运维核心，区分度高）

---

## 一、为什么需要副本再分配

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Partition Reassignment = 在线迁移分区副本                 │
│                                                               │
│  触发场景（面试必背）：                                        │
│  ① 集群扩容：新增 Broker 是空的，需要把部分副本迁过去        │
│     ★ Kafka 不会自动均衡！新建 Broker 不会自动接管分区       │
│  ② 集群缩容：下线 Broker 前要把它上面的副本全部迁走         │
│  ③ 副本分布不均：某些 Broker Leader/流量过载                │
│  ④ 修改副本因子：3 副本改成 5 副本                            │
│  ⑤ 更换磁盘/机架：调整副本的物理位置                         │
│
│  ★ 关键认知（面试坑）：                                        │
│   Rebalance（Consumer Group 重平衡）≠ Reassignment（副本迁移）│
│   Rebalance：消费者之间重新分配分区的"消费权"                │
│   Reassignment：分区副本在 Broker 之间的"物理迁移"          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、再分配核心原理

### 2.1 副本集合变更（RAR + AR）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 再分配本质：修改分区的副本分配（Assigned Replicas）      │
│                                                               │
│  例子：Topic-P0 当前副本 [1,2,3]，要迁到 [3,4,5]             │
│                                                               │
│  Controller 生成 Reassignment：                              │
│   目标 RAR（Reassigned Replicas）= [3,4,5]                   │
│   当前 AR （Assigned Replicas）   = [1,2,3]                  │
│                                                               │
│  迁移策略：                                                    │
│   ① 新增副本：4、5（在 AR 里没有）→ 作为新 Follower 同步     │
│   ② 保留副本：3（新旧集合交集）→ 已有数据，继续同步          │
│   ③ 删除副本：1、2（在 RAR 里没有）→ 新副本追平后删除        │
│                                                               │
│  ★ 为什么不能直接删 1、2？                                    │
│   新副本 4、5 数据是空的，立刻切走会丢数据                    │
│   → 必须等 4、5 追平 Leader（进入 ISR）后，才能删 1、2       │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 迁移执行流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Reassignment 完整执行阶段（面试核心！）                   │
│                                                               │
│  阶段 ①：启动新副本（New Replica / Initial Fetch）           │
│   Controller 通过 LeaderAndIsr 请求                          │
│   → Broker4/5 创建副本，状态 NewReplica                      │
│   → 从 Leader（Broker3）开始全量复制                          │
│      - 落后太多：走 TruncatedLogRecovery / 快照 + 增量       │
│      - 新副本不参与 ISR、不参与 Leader 选举、不对外服务      │
│                                                               │
│  阶段 ②：等待新副本追平（Caught Up）                         │
│   Controller 定期 Watch 新副本的 LEO                         │
│   → 判断是否"追上"：新副本 LEO ≥ 触发时刻 Leader 的 LEO      │
│     （或落后时间 < replica.lag.time.max.ms）                 │
│   → 追平后新副本加入 ISR                                     │
│                                                               │
│  阶段 ③：切换副本分配（Reassign Partitions）                 │
│   所有新副本进 ISR 后                                        │
│   → Controller 更新 ZooKeeper/KRaft 中的 AR = RAR           │
│   → 通过 LeaderAndIsr 通知所有 Broker                        │
│                                                               │
│  阶段 ④：删除旧副本（Remove Old Replicas）                   │
│   Broker1/2 上的副本进入 OfflineReplica → 删除日志           │
│   迁移完成                                                    │
│                                                               │
│  // core/.../controller/ReassignPartitionsStateMachine.scala│
│  // NewReplica → NewReplicaReceivedDuringIsr                 │
│  //   → OnlineReplica（追平后）→ 旧副本 OfflineReplica       │
│  //   → ReplicaDeletionStarted → ReplicaDeletionSuccessful   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、KRaft 模式下的再分配（3.x 新方式）

### 3.1 不再依赖 ZooKeeper

```
┌─────────────────────────────────────────────────────────────┐
│  ★ KRaft 时代再分配的变化（面试加分）                        │
│                                                               │
│  ZK 模式：                                                     │
│   kafka-reassign-partitions.sh 生成 JSON                     │
│   → 写 /admin/reassign_partitions ZNode                      │
│   → Controller Watch 触发，更新 /brokers/topics 状态         │
│                                                               │
│  KRaft 模式：                                                  │
│   通过 AlterPartitionReassignments API（KIP-455）            │
│   → Active Controller 写 PartitionReassignmentRecord         │
│   → 进入元数据 Raft Log（__cluster_metadata）                │
│   → Commit 后所有 Broker 收到，Controller 执行迁移           │
│                                                               │
│  ★ KIP-455 的改进：                                          │
│   ① 声明式 API：可以中途取消（--cancel）                     │
│   ② 单分区粒度操作，不用一次性提交全量 JSON                  │
│   ③ 再分配进度可查询（ListPartitionReassignments）           │
│   ④ Controller 故障转移后迁移自动恢复（状态在 Raft Log）    │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 执行限流（Crucial！）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 副本迁移限流（面试高频，生产必问）                        │
│                                                               │
│  问题：迁移时新副本全量复制 → 占满网卡/磁盘 → 影响线上流量   │
│                                                               │
│  解决：quotas.replication 限流                                │
│   broker.quota.replication.throttled.rate（Broker 入站）     │
│   leader/follower 复制带宽上限（如 50MB/s）                  │
│                                                               │
│  // 被限流的副本复制走单独的 Quota 通道                       │
│  // 正常 ISR 同步不受影响                                    │
│  // ReplicaQuotaManager 统计流量，超阈值延迟 FETCH 响应      │
│                                                               │
│  注意点：                                                      │
│   限流太严：迁移非常慢、ISR 长期不收敛                       │
│   限流太松：迁移期间业务抖动                                  │
│   → 迁移完成后务必移除限流配置（否则正常复制也被限）        │
│                                                               │
│  磁盘 IO 也有限制：replica.fetch 限流 + 限速复制             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Leader 迁移与 preferred leader

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Preferred Replica Leader Election（优先副本选举）        │
│                                                               │
│  AR 列表的第一个副本 = preferred replica（优先 Leader）      │
│  经历故障转移后 Leader 可能不在首选副本上（负载倾斜）        │
│                                                               │
│  kafka-leader-election.sh --election-type PREFERRED          │
│   → Controller 把 Leader 切回 AR[0]（前提它在 ISR 且存活）  │
│   → 平衡 Leader 分布（Leader 承担读写，Follower 只读）       │
│                                                               │
│  auto.leader.rebalance.enable=true（默认）                    │
│   Controller 定期检查 preferred Leader 失衡比例              │
│   超阈值自动触发优先选举（可能造成短暂抖动，生产常关闭）     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Reassignment 是什么？| 分区副本在 Broker 间的**物理迁移**（改副本分配 AR）|
| 2 | Reassignment vs Rebalance？| Reassignment 副本物理迁移；Rebalance 消费者重新分配消费权 |
| 3 | 触发场景？| 扩容（★不自动均衡）/缩容/分布不均/改副本因子/换机 |
| 4 | 新 Broker 会自动接管分区吗？| **不会**，必须手动/工具触发 Reassignment |
| 5 | 迁移四阶段？| 启动新副本→等追平进 ISR→切 AR=RAR→删旧副本 |
| 6 | 为什么不能立刻删旧副本？| 新副本数据为空，必须先追平 Leader 进 ISR 否则丢数据 |
| 7 | 新副本追平判断？| 新副本 LEO ≥ 触发时 Leader LEO 或落后 < lag.time.max.ms |
| 8 | 新副本能选 Leader 吗？| 不能；NewReplica 不参与 ISR/选举，追平后才转 Online |
| 9 | KRaft 下怎么做？| **AlterPartitionReassignments API（KIP-455）** 写 PartitionReassignmentRecord 到 Raft |
| 10 | KIP-455 改进？| 声明式可取消 + 单分区粒度 + 进度可查 + 故障自动恢复 |
| 11 | 迁移限流？| replication quota 限制复制带宽，防止迁移打满网卡影响业务 |
| 12 | 限流注意事项？| 迁移完成必须移除限流配置，否则正常复制也被限 |
| 13 | Preferred Leader？| AR[0]；故障后 Leader 漂移，PREFERRED 选举切回首选副本平衡负载 |
| 14 | 自动 Leader 平衡？| auto.leader.rebalance.enable，生产常关闭避免抖动 |
| 15 | 面试最高频考点排序？| **1. 迁移四阶段 2. 新副本追平才删旧 3. KRaft KIP-455 4. 限流 5. vs Rebalance** |
| 16 | 学习路径？| ReassignPartitionsStateMachine → NewReplica 状态 → KIP-455 API → Quota 限流 |
| 17 | 面试常见追问？| 迁移中 Controller 挂了？→ ZK 靠 ZNode，KRaft 状态在 Raft Log，恢复后继续 |
| 18 | 迁移会丢数据吗？| 不会；先复制追平进 ISR 才切换删除，期间正常生产消费 |
| 19 | 迁移期间生产写哪？| 写当前 Leader；新 Follower 从 Leader 同步，对客户端透明 |
| 20 | 缩容 Broker 步骤？| 先 Reassignment 迁走所有副本 → 等 ISR 收敛 → 再关闭/下线 Broker |
| 21 | 迁移慢的原因？| 限流太严 + 数据量大 + 新副本落盘慢 + 带宽瓶颈 |
| 22 | 面试陷阱？| 说加机器 Kafka 自动均衡 → ❌ 必须显式 Reassignment |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：core/.../controller/ReassignPartitionsStateMachine.scala + KIP-455 AlterPartitionReassignments  
**面试考点：22 题**  
**差异化点**：RAR/AR 副本集合 + 四阶段状态机 + 追平才切换 + KRaft KIP-455 + 复制限流
