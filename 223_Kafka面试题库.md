# 223 · Kafka 高频面试题库（45 题 · 详解）

> **方向**：Kafka 3.x/4.x 架构、存储、生产消费、副本与一致性、KRaft  
> **面试权重**：⭐⭐⭐⭐⭐（消息/流处理必问）  
> **配套源码文档**：[038_Kafka源码解析.md](038_Kafka源码解析.md)、[203_KafkaKRaftController元数据即日志源码级解析.md](203_KafkaKRaftController元数据即日志源码级解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问；不写未经验证的行号。

---

## 一、架构与副本（Q1–Q6）

### Q1. Kafka 的整体架构由哪些部分组成？

**结论**：Producer、Broker、Consumer、Controller/元数据协调、Topic/Partition/Replica 组成，按分区实现水平扩展。

**机制**：
- Topic 逻辑上是消息类别，物理上分为多个 Partition。
- 每个 Partition 是一个有序、追加写的日志。
- 每个 Partition 有多个 Replica，Leader 读写，Follower 同步。
- Controller 负责分区/副本分配与故障转移。
- Consumer Group 按分区并行消费。

**追问**：Kafka 为什么吞吐高？顺序写、PageCache、零拷贝、批量、分区并行。

### Q2. Controller 的职责是什么？

**结论**：管理集群元数据、分区分配、Leader 选举、Broker 上下线处理与副本状态机。

**机制**：
- 监听 Broker 变化与分区状态变更。
- Broker 故障时选择新 Leader，并通知所有相关 Broker。
- 维护 Topic、Partition、Replica 的分配关系。
- 传统模式依赖 ZooKeeper，KRaft 模式用元数据日志。

**追问**：Controller 挂了怎么办？ZK 模式会重新选举 Controller；KRaft 模式由 Controller Quorum 选举。

### Q3. KRaft 模式与 ZooKeeper 模式有什么区别？

**结论**：KRaft 把元数据写入内部日志，由 Controller Quorum 管理，去掉 ZooKeeper 依赖。

**机制**：
- 元数据变更以日志形式追加，Snapshot 持久化。
- Controller 节点组成 Raft Quorum，保证一致性。
- Broker 只负责数据，元数据从 Controller 获取。
- 部署更简单，元数据扩展性更好，故障恢复更快。

**追问**：KRaft 一定更好吗？新集群推荐 KRaft；存量 ZK 集群迁移需评估版本与工具链。

### Q4. Topic、Partition、Replica 的关系是什么？

**结论**：一个 Topic 分为多个 Partition，每个 Partition 有多个 Replica，Replica 分布在不同的 Broker。

**机制**：
- Partition 是有序日志，消息追加到末尾。
- Replica 分为 Leader 与 Follower，Leader 处理读写。
- 副本因子决定可靠性，通常 3。
- 分区数决定并行度，但过多会增加元数据与文件句柄压力。

**追问**：分区数能减少吗？不能直接减少，通常新建 Topic 迁移或重建。

### Q5. ISR、HW、LEO 分别是什么？

**结论**：ISR 是与 Leader 保持同步的副本集合；HW 是已提交可见位置；LEO 是日志末尾偏移。

**机制**：
- Follower 追赶上 Leader 后进入 ISR，落后太多被移出。
- HW = ISR 中最小 LEO，Consumer 只能读到 HW 之前的消息。
- LEO 是每个副本当前写入末尾。
- `min.insync.replicas` 控制 acks=all 时的最小同步副本数。

**追问**：ISR 越大越好吗？不是，ISR 大表示副本健康，但同步成本高；关键是最小副本数满足可靠性。

### Q6. 分区数如何设计？

**结论**：按吞吐、并发消费能力与 Broker 数量设计，避免过少限制并行、过多增加开销。

**机制**：
- 分区数决定 Producer 并行写与 Consumer 并行读的上限。
- 每个分区对应一个 Leader，分布到 Broker。
- 分区过多会增加 Controller 元数据、文件句柄与 Rebalance 成本。
- 经验：单分区吞吐按业务压测，再乘以目标吞吐与副本因子。

**追问**：分区数超过 Consumer 数会怎样？部分 Consumer 空闲，资源浪费；少于 Consumer 数则部分实例无分区可消费。

---

## 二、存储机制（Q7–Q13）

### Q7. Kafka 的日志存储结构是怎样的？

**结论**：每个 Partition 对应一个目录，内部按 LogSegment 切分，包含 `.log`、`.index`、`.timeindex` 等文件。

**机制**：
- Segment 默认按大小（如 1GB）或时间滚动。
- `.log` 存消息，`.index` 是偏移稀疏索引，`.timeindex` 按时间索引。
- 消息按追加写，偏移单调递增。
- 旧 Segment 按保留策略删除或压缩。

**追问**：为什么要分段？便于删除旧数据、控制文件大小与并行恢复。

### Q8. Kafka 的稀疏索引如何加速查找？

**结论**：`.index` 每隔一定字节记录“偏移 → 物理位置”，查找时二分定位再顺序扫描。

**机制**：
- 索引不是每条消息都记录，而是稀疏采样。
- 先按目标 offset 二分索引，找到最近的物理位置。
- 再从该位置顺序扫描 `.log`，定位精确消息。
- 时间索引支持按时间戳查找。

**追问**：索引文件会很大吗？不会，稀疏索引只占日志很小比例，且分段管理。

### Q9. Kafka 的零拷贝是怎么实现的？

**结论**：Consumer 读取时用 `sendfile` 把 PageCache 数据直接送到网卡，避免用户态拷贝。

**机制**：
- 消息写入 PageCache，读取时命中缓存。
- `FileChannel.transferTo` 走 sendfile，减少内核/用户态切换。
- 批量传输减少系统调用。
- Producer 端也有批量与压缩优化。

**追问**：零拷贝适用于所有场景吗？需要文件到 Socket 的直传；若需应用层转换/加密，零拷贝会被打断。

### Q10. Kafka 的刷盘策略是怎样的？

**结论**：Kafka 默认依赖操作系统 PageCache 异步刷盘，不强制每条消息 fsync，以吞吐优先。

**机制**：
- 消息写入 PageCache 即返回。
- 操作系统按脏页策略回写磁盘。
- 可靠性依赖副本机制，而不是单机 fsync。
- 可通过 `flush.messages`/`flush.ms` 调整，但代价高。

**追问**：Kafka 会丢消息吗？单机掉电可能丢 PageCache；多副本 + acks=all 可保证集群级可靠。

### Q11. Log Compaction 是什么？适合什么场景？

**结论**：按 key 保留每个 key 的最新值，删除旧值，适合 changelog/状态存储场景。

**机制**：
- `cleanup.policy=compact`。
- 后台 Cleaner 扫描 Segment，保留每个 key 的最后一条。
- 支持 tombstone（null 值）删除 key。
- 常见于 Kafka Streams 状态存储、CDC 快照。

**追问**：Compaction 与删除策略能同时用吗？可以，`cleanup.policy=delete,compact`，但语义更复杂。

### Q12. Kafka 的消息保留策略有哪些？

**结论**：按时间（`retention.ms`）和大小（`retention.bytes`）删除，或按 key 压缩保留。

**机制**：
- 默认按时间删除，常见 7 天。
- 超过大小限制时删除最旧 Segment。
- 删除以 Segment 为单位，不是单条消息。
- 保留策略可按 Topic 配置。

**追问**：消费者落后会被删消息吗？会，超过保留期的消息被删除后无法消费，需要监控 lag。

### Q13. Kafka 日志清理会阻塞读写吗？

**结论**：删除是异步的，通常影响很小；Compaction 会重写 Segment，可能产生磁盘与 IO 压力。

**机制**：
- 删除旧 Segment 由后台线程执行。
- Compaction 需要读取、合并、写新 Segment，再替换。
- 大 Segment 或高写入量时清理可能跟不上。
- 监控磁盘、Cleaner 延迟与日志段数量。

**追问**：如何减少清理压力？合理设置 Segment 大小、保留时间，避免过多小文件与频繁滚动。

---

## 三、生产者（Q14–Q22）

### Q14. Producer 发送一条消息的完整流程是什么？

**结论**：拦截器 → 序列化 → 分区器 → 累加器批处理 → Sender 发送 → 响应与重试。

**机制**：
- `ProducerRecord` 经拦截器处理。
- key/value 序列化。
- 分区器决定目标 Partition。
- 消息进入 RecordAccumulator，按 batch 聚合。
- Sender 线程按 linger/batch 条件发送。
- 响应回调处理成功或重试。

**追问**：发送是异步的吗？默认异步批量发送，`send` 返回 Future，可同步等待。

### Q15. Kafka 的分区器如何工作？

**结论**：有 key 时按 key 哈希取模，无 key 时轮询/粘性分区，保证分布均匀。

**机制**：
- 默认 `DefaultPartitioner`：key 哈希 → 固定分区。
- 无 key 时使用粘性分区，减少切换批次。
- 可自定义 Partitioner 实现业务路由。
- 分区数变化会影响 key 的映射结果。

**追问**：增加分区后同一 key 会换分区吗？会，哈希取模结果变化，顺序性可能受影响。

### Q16. acks 参数有哪几种？如何选择？

**结论**：`acks=0` 不等确认，`acks=1` 等 Leader 落盘，`acks=all/-1` 等 ISR 全部确认。

**机制**：
- 0：吞吐最高，可能丢消息。
- 1：Leader 确认即可，Leader 故障可能丢。
- all：所有 ISR 确认，可靠性最高，延迟最大。
- 常配合 `min.insync.replicas` 使用。

**追问**：acks=all 一定不丢吗？还要副本数、ISR 健康与磁盘可靠；副本不足时会拒绝写入。

### Q17. Producer 的重试机制是怎样的？

**结论**：发送失败按 `retries` 重试，配合退避与幂等避免重复；部分错误不可重试。

**机制**：
- 可重试错误：网络抖动、Leader 切换、超时。
- 不可重试错误：消息过大、序列化失败、权限错误。
- `retry.backoff.ms` 控制退避。
- 开启幂等后重试不会产生重复。

**追问**：重试会导致顺序问题吗？可能，尤其多分区/多 in-flight；幂等与顺序发送可缓解。

### Q18. Kafka 的幂等生产者如何实现？

**结论**：通过 PID + 序列号去重，Broker 记录每个分区的已写序列，重复消息被丢弃。

**机制**：
- `enable.idempotence=true`。
- Producer 获取 PID，每条消息带递增序列号。
- Broker 按 PID + Partition 校验序列。
- 要求 `acks=all`、`max.in.flight.requests.per.connection <= 5`。
- 只保证单分区、单会话内不重复。

**追问**：跨会话/跨分区能保证吗？不能，需要事务或业务幂等。

### Q19. Kafka 事务如何保证 Exactly-Once？

**结论**：事务把多个分区写与 offset 提交绑定为原子操作，配合幂等与 read_committed 实现端到端精确一次。

**机制**：
- `transactional.id` 初始化事务。
- 写消息带事务标记，暂不可见。
- 提交后消息可见，回滚则丢弃。
- Consumer 设置 `isolation.level=read_committed`。
- 事务超时与 abort 处理未完成事务。

**追问**：事务能跨 Topic 吗？可以，同一事务可写多个 Topic/Partition，但成本更高。

### Q20. batch.size、linger.ms 如何影响性能？

**结论**：增大 batch 与 linger 提升吞吐、降低请求数，但增加延迟与内存占用。

**机制**：
- `batch.size` 控制单批最大字节。
- `linger.ms` 控制等待更多消息的时间。
- 批量发送减少网络与 Broker 写入次数。
- 延迟敏感场景应减小 linger。

**追问**：batch 没满会一直等吗？到 linger 时间即发送，不会无限等待。

### Q21. Kafka 支持哪些压缩方式？如何选择？

**结论**：gzip、snappy、lz4、zstd；lz4/zstd 通常兼顾压缩比与 CPU，gzip 压缩比高但慢。

**机制**：
- 压缩在 Producer 端做，Broker 存储压缩后的 batch。
- Consumer 端解压，减少网络与磁盘。
- 压缩对批量消息效果最好。
- 不同版本支持不同算法与级别。

**追问**：压缩会影响消费 CPU 吗？会，解压需要 CPU；高压缩比换 CPU，需压测。

### Q22. Producer 拦截器有什么用？

**结论**：在发送前与响应后插入自定义逻辑，用于埋点、脱敏、动态路由与监控。

**机制**：
- `ProducerInterceptor.onSend` 可修改消息。
- `onAcknowledgement` 处理成功/失败回调。
- 多个拦截器按配置顺序执行。
- 不应做阻塞操作，避免拖慢发送线程。

**追问**：拦截器能改分区吗？可以在消息进入分区器前修改 key/value，但直接改分区需自定义分区器。

---

## 四、消费者与消费组（Q23–Q31）

> **主题归属**：本节讲 Kafka 消费细节；MQ 通用可靠性/幂等/顺序见 [232 Q14–Q16](232_分布式与微服务面试题库.md)。

### Q23. Consumer Group 的消费语义是什么？

**结论**：同一 Group 内每个分区只被一个 Consumer 消费，实现负载均衡与并行消费。

**机制**：
- 分区分配给 Group 内实例。
- 实例数大于分区数时部分实例空闲。
- 不同 Group 独立消费全量消息。
- offset 按 Group 记录。

**追问**：如何提高消费并行度？增加分区数或增加实例，但上限是分区数。

### Q24. Rebalance 的流程是什么？

**结论**：Group 协调器检测成员/订阅变化，触发分区重新分配，Consumer 暂停消费并提交 offset。

**机制**：
- 成员加入/离开、心跳超时、订阅变更都会触发。
- Coordinator 组织 JoinGroup/SyncGroup。
- 分配策略计算分区归属。
- 迁移期间可能重复消费或短时停滞。

**追问**：如何减少 Rebalance？稳定心跳、避免频繁上下线、合理 session timeout、使用静态成员。

### Q25. Eager 与 Cooperative Rebalance 有什么区别？

**结论**：Eager 会先撤销所有分区再整体重分配；Cooperative 只迁移需要变动的分区，减少停顿。

**机制**：
- Eager：所有 Consumer 暂停、释放分区，再重新分配。
- Cooperative：增量撤销/分配，未变动的分区继续消费。
- Cooperative 需要客户端与服务端支持，可能出现多轮协调。
- KIP-429 引入 cooperative-sticky。

**追问**：Cooperative 一定更快吗？大多数场景停顿更小，但协调轮次可能更多。

### Q26. KIP-848 的新消费组协议有什么改进？

**结论**：把组协调逻辑更多放到 Broker，客户端只维护状态，减少 Rebalance 停顿与全局同步。

**机制**：
- 服务端管理成员与目标分配。
- 客户端按增量指令更新分区。
- 减少 JoinGroup/SyncGroup 的全局屏障。
- 支持更平滑的扩缩容与故障恢复。

**追问**：KIP-848 完全替代旧协议了吗？需要版本与服务端支持，存量客户端仍兼容旧协议。

### Q27. offset 提交有哪几种方式？

**结论**：自动提交、同步手动提交、异步手动提交，按业务一致性选择。

**机制**：
- `enable.auto.commit=true` 按间隔提交，可能重复或漏提交。
- `commitSync` 阻塞等待，可靠性高。
- `commitAsync` 异步，性能好但失败需处理。
- 常见组合：处理成功后同步提交，或异步 + 定期同步兜底。

**追问**：先提交还是先处理？必须先处理成功再提交，否则失败会丢消息。

### Q28. 自动提交 offset 有什么风险？

**结论**：提交时机与业务处理不同步，可能重复消费或消息丢失，生产建议手动提交。

**机制**：
- 自动提交按时间间隔，不保证处理完成。
- Rebalance 时可能提交未处理完的 offset。
- 消费者崩溃会重复消费。
- 关闭自动提交并显式管理更可控。

**追问**：手动提交就万无一失吗？仍需幂等与异常处理，提交失败要重试。

### Q29. Kafka Consumer 是线程安全的吗？如何多线程消费？

**结论**：Consumer 非线程安全，一个实例对应一个线程；多线程需每线程一个 Consumer 或分区级并行。

**机制**：
- `KafkaConsumer` 不是线程安全的，多线程共享会异常。
- 方案一：每线程独立 Consumer。
- 方案二：单 Consumer 拉取，业务线程池处理，但 offset 管理复杂。
- 分区数限制并行度。

**追问**：`wakeup()` 有什么用？用于从其他线程唤醒阻塞的 poll，实现优雅退出。

### Q30. Kafka 如何保证消息顺序？

**结论**：单分区内按写入顺序消费；跨分区不保证，需要业务按 key 路由到同一分区。

**机制**：
- Producer 按 key 哈希到固定分区。
- 分区日志追加有序。
- Consumer 单分区顺序拉取。
- 多线程消费同一分区会破坏顺序。

**追问**：增加分区会影响顺序吗？同一 key 的分区映射可能变化，历史顺序与新的写入可能错位。

### Q31. 如何避免重复消费？

**结论**：Kafka 至少一次语义，业务必须幂等；可用唯一键、去重表、幂等状态机。

**机制**：
- 消息可能因重试、Rebalance、提交失败重复。
- 消费前按业务唯一键查重。
- 处理与记录状态放在同一事务/幂等操作中。
- 数据库唯一约束兜底。

**追问**：幂等键用什么？订单号、请求 ID、业务主键；不要只用 offset。

---

## 五、高可用、运维与选型（Q32–Q35）

### Q32. Kafka 的副本分配策略是什么？

**结论**：尽量均匀分布副本到 Broker，同时考虑机架感知，避免所有副本在同一故障域。

**机制**：
- 创建 Topic 时按 Broker 轮询分配。
- 第一个副本为 Leader，后续为 Follower。
- `rack` 配置实现机架感知。
- 副本因子不超过 Broker 数。

**追问**：副本因子 3 够吗？通常够，但跨机房/跨机架需结合故障域设计。

### Q33. 分区再分配（Reassignment）怎么做？

**结论**：通过 `kafka-reassign-partitions` 生成计划，迁移副本数据，再校验与切换。

**机制**：
- 生成 JSON 分配计划。
- 执行后 Broker 复制数据到目标。
- 观察迁移进度与流量。
- 完成后清理旧副本。
- 可用 Cruise Control 自动均衡。

**追问**：再分配会影响性能吗？会，产生大量网络与磁盘 IO，应限速并避开高峰。

### Q34. HW 与 LEO 在副本同步中如何变化？

**结论**：Follower 追赶 Leader 的 LEO，Leader 依据 ISR 最小 LEO 推进 HW，Consumer 只能读到 HW 之前。

**机制**：
- Leader 写入后 LEO 增加。
- Follower 拉取并追加，LEO 追上。
- ISR 中最小 LEO 决定 HW。
- Follower 落后会被移出 ISR，HW 可能回退/停滞。

**追问**：为什么 Consumer 不能读 HW 之后？那些消息尚未被所有 ISR 确认，可能因 Leader 故障丢失。

### Q35. Kafka 与 RocketMQ 的差异及选型建议？

**结论**：Kafka 偏日志/流处理与高吞吐，RocketMQ 偏业务消息与事务/延迟/轨迹能力。

**机制**：
- Kafka：分区日志、批量、零拷贝、Streams/Connect 生态。
- RocketMQ：CommitLog + ConsumeQueue、事务消息、18 级延迟、死信与轨迹。
- Kafka 顺序按分区，RocketMQ 可按队列。
- 选型看业务语义、吞吐、延迟、运维与团队经验。

**追问**：Kafka 能做事务消息吗？能，Exactly-Once 事务针对 Kafka 读写；跨外部系统仍需业务补偿。RocketMQ/Kafka/Pulsar 的三方选型见 [222 Q35/Q40](222_RocketMQ面试题库.md)。

---

## 六、进阶补充（Q36–Q40）

### Q36. Kafka 分层存储（Tiered Storage）是什么？

**结论**：热数据留在 Broker 本地磁盘，冷数据下沉到对象存储/HDFS，按保留策略分层，降低存储成本。

**机制**：
- 基于 KIP-405，Segment 可按策略迁移到远程存储。
- 本地保留近期数据，远程保存历史。
- 读取冷数据需回源，延迟更高。
- 适合日志/事件长期留存场景。

**追问**：分层存储影响消费吗？冷数据读取延迟上升，需评估消费 SLA。

### Q37. 从 ZooKeeper 迁移到 KRaft 的步骤是什么？

**结论**：先升级到支持迁移的版本，开启双写/迁移模式，校验元数据，再切换为 KRaft-only。

**机制**：
- 部署 KRaft Controller Quorum。
- 以迁移模式运行，ZK 与 KRaft 同步元数据。
- 校验 Topic/分区/ACL 一致性。
- 切换 Controller 并移除 ZK 依赖。
- 回滚窗口内保留 ZK 数据。

**追问**：迁移期间能写吗？可以，但需按官方流程与版本约束，避免双写不一致。

### Q38. Kafka 的 Share Groups（KIP-932）是什么？

**结论**：提供队列式共享消费，同一分区可被多个消费者协作消费，按消息级确认。

**机制**：
- 传统 Consumer Group 一个分区只能一个消费者。
- Share Group 允许共享，适合任务队列。
- 消息需显式 acknowledge，失败可重投。
- 与消费组语义不同，注意顺序与重复。

**追问**：Share Group 能保证顺序吗？共享消费天然弱化顺序，强顺序仍用单分区单消费者。

### Q39. Kafka 4.0 有哪些重要变化？

**结论**：KRaft 成为唯一元数据模式，ZooKeeper 被移除；客户端与服务端 API 继续演进。

**机制**：
- 新集群只能使用 KRaft。
- 旧 ZK 集群需先迁移。
- 部分旧客户端/工具需升级。
- 新版本继续增强共享消费、分层存储等能力。

**追问**：4.0 兼容 3.x 客户端吗？多数兼容，但需核对版本矩阵与废弃 API。

### Q40. Kafka 生产环境的调优顺序是什么？

**结论**：先定位瓶颈（生产/消费/Broker/磁盘/网络），再按 batch、linger、压缩、fetch、分区与 GC 逐层调。

**机制**：
- 生产：`batch.size`、`linger.ms`、压缩、acks。
- 消费：`fetch.min.bytes`、`max.poll.records`、并发消费者。
- Broker：分区数、副本、IO、PageCache、网络。
- JVM：GC 与堆外。
- 先监控 lag、ISR、请求耗时，再改参数。

**追问**：调参前要做什么？压测基线与容量评估，避免盲目复制参数。

---

## 七、进阶补充（Q41–Q45）

### Q41. Kafka 的关键监控指标有哪些？

**结论**：关注 lag、ISR、请求延迟、磁盘、网络、GC、Controller 状态与副本同步。

**机制**：
- Consumer lag 与消费速率。
- ISR 收缩/副本落后。
- Produce/Fetch 请求 P99。
- 磁盘使用、PageCache、网络。
- UnderReplicatedPartitions、OfflinePartitions。

**追问**：lag 上涨先查什么？消费耗时、下游、分区分配与 rebalance。

### Q42. 跨机房复制怎么做？

**结论**：用 MirrorMaker 2 做集群间复制，按主题/消费组映射，处理延迟、冲突与位点转换。

**机制**：
- MM2 基于 Connect，双向/单向复制。
- 自动创建 Topic 与消费组映射。
- 位点转换与副本策略。
- 跨机房网络与带宽成本高。

**追问**：跨机房复制能保证一致吗？异步复制，存在延迟与重复，需业务幂等。

### Q43. Kafka 客户端升级要注意什么？

**结论**：核对兼容矩阵、废弃 API、序列化与配置变更，先灰度客户端再升级 Broker。

**机制**：
- 客户端与服务端版本兼容范围。
- 配置项默认值可能变化。
- 序列化/反序列化兼容。
- 监控错误率与延迟。

**追问**：能直接升 Broker 吗？建议先升客户端，再滚动升级 Broker，保留回滚。

### Q44. 增加分区会带来什么影响？

**结论**：提升并行度，但会改变 key 的分区映射，可能影响顺序与已有数据分布。

**机制**：
- 新分区加入后 key 哈希取模结果变化。
- 同一 key 的历史消息与新消息可能在不同分区。
- 需要评估顺序性与消费逻辑。
- 分区数不能直接减少。

**追问**：如何避免顺序问题？按新分区数重新规划或使用业务路由表；分区与 key 机制见 [Q14–Q15](223_Kafka面试题库.md)。

### Q45. Kafka 容量如何规划？

**结论**：按消息大小、保留时间、副本因子、峰值吞吐与增长趋势估算磁盘、网络与分区数。

**机制**：
- 日增数据 × 保留天数 × 副本因子。
- 峰值吞吐决定网络与磁盘带宽。
- 分区数决定并行度上限。
- 预留 30% 以上余量。

**追问**：只按日均估算够吗？不够，需考虑峰值、重放、压缩比与副本放大；调优顺序见 [Q40](223_Kafka面试题库.md)。

---

## 八、速记清单

| 主题 | 必背一句话 |
|---|---|
| 架构 | Topic → Partition → Replica，Leader 读写、Follower 同步 |
| 存储 | Segment + 稀疏索引 + PageCache + 零拷贝 |
| 可靠 | acks=all + min.insync.replicas + 多副本 |
| 幂等 | PID + 序列号，单分区单会话 |
| 事务 | 多分区写 + offset 原子提交，read_committed |
| 消费 | Group + Rebalance + offset 提交 |
| 顺序 | 单分区有序，按 key 路由 |
| KRaft | 元数据日志 + Controller Quorum，去 ZK |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：45 题 · 详解**
