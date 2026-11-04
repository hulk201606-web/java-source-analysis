# ZooKeeper ZAB 协议 + Watch + 数据树源码级深度解析

> **源码基线**：ZooKeeper 3.9+（Java）  
> **核心位置**：src/main/java/org/apache/zookeeper/server + server/quorum/Zab  
> **面试权重**：⭐⭐⭐⭐⭐（分布式协调核心，独立高频）

---

## 一、ZooKeeper 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ ZooKeeper = CP 分布式协调服务（ZAB 协议）                │
│                                                               │
│  核心特性（面试必背）：                                        │
│   ✅ ZAB 协议（原子广播 + 崩溃恢复）                          │
│   ✅ 顺序一致性（所有客户端看到相同更新顺序）                 │
│   ✅ Watch 机制（数据变化通知）                               │
│   ✅ 层级数据树（znode 永久/临时/有序）                       │
│   ✅ ACL 权限（world/auth/digest/sasl）                      │
│                                                               │
│  典型用途：                                                    │
│   - 服务发现（Dubbo/Nacos 早期）                              │
│   - 分布式锁（Curator / Redisson）                            │
│   - 配置中心（Spring Cloud）                                  │
│   - 集群协调（Kafka Controller 选举、ClickHouse 复制）       │
│                                                               │
│  与 Raft/Paxos 对比（第六节）：                                │
│   ZAB ≈ 简化版 Paxos（为 ZK 场景优化）                       │
│   Raft 更简洁（Leader 强中心化）                              │
│   但 ZK 是首个工业级 CP 协调系统，生态最广                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、ZAB 协议源码级

### 2.1 ZAB 两阶段：Broadcast + Recovery

```java
// ★ ZAB（ZooKeeper Atomic Broadcast）核心源码级
//
// src/main/java/org/apache/zookeeper/server/quorum/Zab.java
// src/.../server/quorum/FastLeaderElection.java
// src/.../server/RequestProcessor.java 链
//
// // ZAB 分两阶段（面试必背）：
// // 阶段 1：Broadcast（正常运行）
// // 阶段 2：Recovery（Leader 崩溃后恢复）
//
// // ★ Broadcast（Leader 原子广播提案）：
// //
// // Leader 收到写请求：
// //
// // Step 1: Leader 把提案（Proposal）写入本地 Log
// //         并广播给所有 Follower
// //
// // Step 2: Leader 等 Follower 回复 ACK（过半）
// //         过半 ACK → Commit 提案
// //
// // Step 3: Leader 广播 Commit 给所有 Follower
// //         Follower 执行提案 → 更新内存 → 回 ACK
// //
// // Step 4: Leader 收到所有 Follower 的 ACK
// //         → 给客户端回 OK
// //
// // // 源码级：
// // LeaderZooKeeperServer.proposeRequest() →
// //    broadcastProposal() →
// //    // QuorumPeer 收到后：
// //    //   a. 本地 Log 写入 Proposal
// //    //   b. 回 Leader ACK
// //    // Leader 收到过半 ACK →
// //    //   commit() →
// //    //   broadcastCommit() →
// //    // Follower 执行 → 回 Leader ACK →
// //    // Leader 回客户端
// //
// // ★ ZAB vs Raft 关键区别（面试高频）：
// //   ZAB：Proposal 和 Commit 分两次广播（两步）
// //   Raft：Leader 直接 AppendEntries（一次，隐式 Commit）
// //   ZAB 更接近两阶段提交；Raft 更简洁
// //
// // ★ ZAB 对 Follower 数量的要求：
// //   总节点数 N，过半 = N/2 + 1
// //   例：3 节点 → 2 过半；5 节点 → 3 过半
// //   Leader 自己算一个 → 实际 Follower 需要 过半 - 1
// //
```

### 2.2 Recovery 阶段 + Leader 选举

```java
// ★ ZAB Recovery：Leader 崩溃后重新选举 + 同步
//
// src/.../server/quorum/FastLeaderElection.java
// src/.../server/quorum/LeaderZooKeeperServer.java
//
// // Recovery 三阶段（源码级）：
// //
// // Phase 1: Election（Leader 选举）
// //   所有 Peer 进入 LOOKING 状态
// //   每个 Peer 广播自己的 Zxid（事务 ID）
// //   选 Zxid 最大的 Peer 为新 Leader
// //   // Zxid = 64bit（epoch 高 32bit + counter 低 32bit）
// //   // Zxid 大 = 事务最新 → 不会丢提案
// //
// // Phase 2: Sync（同步）
// //   新 Leader 选出后 → 和每个 Follower 同步
// //   - Leader 检查 Follower 的 Log（Proposal 列表）
// //   - 差异部分：Leader 把缺失的 Proposal 发给 Follower
// //   - 同时 Leader 也清理自己已经 Commit 但没 Follower 确认的
// //   - Follower 收到新事务列表 → 更新状态为 OBSERVING → 然后 FOLLOWING
// //
// // Phase 3: Broadcast（恢复正常广播）
// //   Leader 开始处理新请求 → 进入 Broadcast 阶段
// //
// // ★ FastLeaderElection 算法（ZAB 核心）：
// //   Peer 启动 → LOOKING
// //   LOOKING → 发出 Vote（myId, myLastZxid）
// //   收到过半 Vote → LEADING（我当 Leader）
// //   收到更高 Zxid → FOLLOWING（放弃选举）
// //
// // // Zxid 比较规则：
// // //   先比 epoch（高 32bit）
// // //   epoch 相同再比 counter（低 32bit）
// // //   Zxid 大的 = 事务最新 = 当 Leader
// //
// // ★ Leader 为什么是 Zxid 最大的？（面试核心）
// //   Leader A 崩溃前 Commit 了 Proposal P1（Zxid=100）
// //   Follower B 也 Commit 了 P1
// //   Follower C 还没收到 P1（网络延迟）
// //   → 选 B 当 Leader（Zxid=100 > C 的 Zxid=99）
// //   → B 作为 Leader 可以把 P1 广播给 C（不会丢）
// //   → 如果选 C 当 Leader → C 没有 P1 → 可能丢数据！
//
```

---

## 三、数据树 + Znode 源码级

### 3.1 四种 Znode 类型

```java
// ★ ZooKeeper 数据树源码级
//
// src/main/java/org/apache/zookeeper/server/Tree.java
// src/.../persistence/DataTree.java
// src/.../data/NodeType.java
//
// // 四种 Znode 类型（面试必背）：
// //
// // 1. PERSISTENT（永久）
// //    创建后一直存在，除非显式 delete
// //
// // 2. PERSISTENT_SEQUENTIAL（永久有序）
// //    自动追加单调递增序号
// //    例：create /lock/seq- → 创建 /lock/seq-0000000001
// //    典型用法：分布式锁、唯一 ID 生成
// //
// // 3. EPHEMERAL（临时）
// //    会话（Session）断开自动删除
// //    典型用法：服务发现、Leader 选举
// //
// // 4. EPHEMERAL_SEQUENTIAL（临时有序）
// //    Session 断开自动删除 + 自动追加序号
// //    典型用法：服务发现临时节点、分布式锁（临时有序锁）
// //
// // // Znode 数据结构：
// public class DataNode {
//     byte[] data;              // 数据内容（≤ 1MB）
//     int version;              // 数据版本（每次写 +1）
//     int cversion;             // 子节点版本（子节点变化 +1）
//     long acl;                 // ACL 版本
//     long ephemeralOwner;      // Session ID（仅临时节点）
//     long dataLength;          // 数据长度
//     long numChildren;         // 子节点数
//     long pzxid;               // 最后修改的 Zxid
//     List<String> children;   // 子节点名（有序）
// }
//
// ★ Tree 存储结构（源码级）：
//   ConcurrentHashMap<String, DataNode> nodes
//   // Key = znode path（如 /clickhouse/tables/uuid）
//   // Value = DataNode
//   // 所有节点在内存里（ZK 内存数据库）
//   // 快照定期写磁盘 + 事务日志持续追加
//
```

### 3.2 Session + ACL

```java
// ★ ZooKeeper Session（会话）核心
//
// src/.../server/ServerCnxn.java
//
// // Session 本质：客户端和 ZK 集群之间的长连接
// // Session 关联：
// //   ① 临时节点：Session 断 → 临时节点自动删除
// //   ② Watch 回调：Session 断 → Watch 自动清除
// //   ③ ACL 权限：基于 Session 的认证信息
// //
// // Session 超时（session.timeout）：
// //   默认 30s；客户端心跳（session.tick）每 1/3 timeout 一次
// //   连续 3 次没心跳 → ZK 认为 Session 已死 → 清理临时节点
// //
// // Session 转移：
// //   客户端断连后在 timeout 内连上另一个 Follower
// //   Session 状态（临时节点/Watch）保留 → 无缝恢复
// //   如果超过 timeout → Session 清理 → 临时节点被删
// //
// // ★ ACL 四种 Scheme（面试高频）：
// //   world:anyone        → 开放给所有人
// //   auth:user:password  → 认证用户（digest）
// //   ip:192.168.1.0/24  → IP 白名单
// //   sasl:user@domain    → Kerberos
// //
// // ACL 权限位：
// //   CREATE | DELETE | READ | WRITE | ADMIN
// //   // CREATE = 能创建子节点
// //   // WRITE = 能改 data
// //   // READ = 能读 data
// //   // DELETE = 能删子节点
// //   // ADMIN = 能改 ACL
//
```

---

## 四、Watch 机制源码级

```java
// ★ ZooKeeper Watch 机制（源码级，面试核心）
//
// src/.../server/WatchManager.java
// src/.../server/Watcher.java
//
// // Watch 是什么？
// //   客户端对某个 Znode 注册 Watch
// //   → 当该 Znode 变化时，ZK 主动推送事件给客户端
// //   → 一次性（触发后需要重新注册）
// //
// // // Watch 类型（面试必背）：
// //   1. Data Watch：监听 Znode 数据变化
// //      getData(path, watch=true)
// //      // 触发：setData / create / delete
// //
// //   2. Children Watch：监听子节点变化
// //      getChildren(path, watch=true)
// //      // 触发：create child / delete child
// //
// //   3. Exists Watch：监听节点存在性
// //      exists(path, watch=true)
// //      // 触发：create / delete
// //
// // ★ Watch 注册源码级：
// public class WatchManager {
//     // watchTable: path → Set<Watcher>
//     ConcurrentHashMap<String, Set<Watcher>> watchTable;
//     // watchPaths: Watcher → Set<path>
//     ConcurrentHashMap<Watcher, Set<String>> watchPaths;
//
//     public void addWatch(String path, Watcher watcher) {
//         watchTable.computeIfAbsent(path, k -> new HashSet<>()).add(watcher);
//         watchPaths.computeIfAbsent(watcher, k -> new HashSet<>()).add(path);
//     }
//
//     public void removeWatcher(Watcher watcher) {
//         Set<String> paths = watchPaths.remove(watcher);
//         for (String path : paths) {
//             watchTable.get(path).remove(watcher);
//         }
//     }
// }
//
// // ★ Watch 触发流程（源码级）：
// // 1. Leader 处理写请求（create / setData / delete）
// // 2. Broadcast + Commit 完成
// // 3. Leader 查 WatchManager：这个 path 上有哪些 Watcher
// // 4. 把 Watch 事件广播给所有 Follower（带事务 Zxid）
// // 5. Follower 收到后 → 查自己的 WatchManager
// // 6. 找到匹配的 Watcher → 触发回调（session 断了就跳过）
// //
// // ★ Watch 一致性保证（面试核心）：
// //   Watch 事件带 Zxid（事务 ID）
// //   客户端必须按 Zxid 顺序处理事件
// //   如果收到 Watch 事件后再读 → 一定读到 Watch 事件之后的数据
// //   → 这就是"顺序一致性"
// //
// // ★ Watch 为什么是一次性？
// //   - 简化服务器端实现（不需要维护长周期状态）
// //   - Watch 注册是客户端操作，容易丢失
// //   - 一次性保证 Watch 触发后客户端一定重新评估状态
// //   - 生产一般配合 Read 操作循环注册 Watch
//
```

---

## 五、ZooKeeper vs Raft vs Paxos 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三大 CP 协调协议对比                             │
│  ┌──────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度      │ ZooKeeper ZAB    │ Raft          │ Paxos     ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Leader 选举 │ FastLeader      │ Leader         │ 多 Leader ││
│  │            │ Election (Zxid)  │ Transfer       │ 可能冲突 ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 广播模型   │ ★ 两阶段 Proposal│ ★ 一次 Append │ Paxos 提案││
│  │            │ + Commit         │ Entries        │ 两阶段   ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 日志同步   │ Sync Phase       │ AppendEntries  │ Accept/   ││
│  │            │ (Recovery)       │ (日志匹配)     │ Learn     ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 成员变更   │ ❌ 静态（难）    │ ★ 动态 Joint   │ ❌ 复杂   ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 读路径     │ ★ Follower 本地 │ Follower Read  │ 任意节点  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ Watch 机制 │ ★ 原生支持      │ ❌ 需自行实现  │ ❌ 需自行  ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 实现复杂度 │ 中（ZAB 复杂）  │ ★ 低（清晰）   │ ★ 高     ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 工程生态   │ ★ 最广          │ 广泛（TiDB等） │ 有限     ││
│  ├──────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频   │ ★★★★★          │ ★★★★★        │ ★★★★    ││
│  └──────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   需要 Watch + 服务发现 → **ZooKeeper**（原生 Watch）        │
│   分布式存储 + 动态成员 → **Raft**（TiKV/ETCD/KRaft）      │
│   需要 Paxos 语义的特定系统 → 原生 Paxos（一般不用）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ZAB 两阶段？| **Broadcast（正常广播）+ Recovery（Leader 崩溃恢复）** |
| 2 | ★ ZAB Broadcast 流程？| Leader 写本地 Log → 广播 Proposal → 过半 ACK → Commit 广播 → Follower 执行 → 回客户端 OK |
| 3 | Zxid 是什么？| **64bit：高 32bit epoch + 低 32bit counter**；事务全局单调递增 |
| 4 | Recovery 三阶段？| **Election（Leader 选举，Zxid 最大的当选）+ Sync（差异同步）+ Broadcast（恢复正常）** |
| 5 | ★ 为什么 Zxid 最大的当 Leader？| Zxid 最大 = 事务最新 → 不会丢 Commit 过的 Proposal |
| 6 | ZAB vs Raft 关键区别？| **ZAB Proposal+Commit 两次广播；Raft AppendEntries 一次隐式 Commit** |
| 7 | 四种 Znode 类型？| **PERSISTENT / PERSISTENT_SEQUENTIAL / EPHEMERAL / EPHEMERAL_SEQUENTIAL** |
| 8 | 临时节点什么时候删？| **Session 断开 + session.timeout 超时**；或客户端主动 delete |
| 9 | Watch 三种类型？| **Data Watch（监听数据）+ Children Watch（监听子节点）+ Exists Watch（监听存在性）** |
| 10 | Watch 为什么一次性？| 简化服务器实现 + 保证一致性（触发后重新评估状态）|
| 11 | ★ Watch 一致性保证？| 事件带 Zxid；**按 Zxid 顺序处理；收到 Watch 后再读一定读到事件之后的数据** |
| 12 | Watch 注册源码级结构？| WatchManager：watchTable(path → Set<Watcher>) + watchPaths(Watcher → Set<path>) |
| 13 | Session 转移？| 客户端断连后在 session.timeout 内连上另一个 Follower → 状态保留 → 无缝恢复 |
| 14 | ACL 四种 Scheme？| **world / auth(digest) / ip / sasl** |
| 15 | Session timeout 默认？| 30s；心跳每 1/3 timeout 一次；3 次没心跳 → 清理临时节点 |
| 16 | ZAB 过半规则？| 总节点 N，过半 = N/2 + 1；Leader 自己算一个 |
| 17 | ★ ZAB vs Raft vs Paxos 全维度对比？| 见第五节对比表（10 维度）|
| 18 | 面试最高频考点排序？| **1. ZAB Broadcast 两阶段 2. Recovery + Zxid 最大选 Leader 3. Watch 一次性 + 一致性 4. Znode 四种类型 5. ZAB vs Raft vs Paxos** |
| 19 | 学习路径？| ZAB Broadcast → Recovery → FastLeaderElection → WatchManager → DataTree |
| 20 | 面试常见追问？| ZAB 是 Raft 的前身吗？| 不是！ZAB 是 ZK 自己的协议；Raft 是 Stanford 2014 提的；两者设计目标不同（ZAB 为 ZK 场景优化）|
| 21 | 面试常见追问？| Follower 能直接响应读请求吗？| 可以！**Follower 本地有最新快照** → 直接读（比去 Leader 快）；但写必须走 Leader |
| 22 | 面试陷阱？| 说 "ZK 是强一致所以读也要走 Leader" → ❌ ZK 读请求可以走 Follower（顺序一致性保证足够）；写必须走 Leader |
| 23 | 面试陷阱？| 说 "Watch 会丢" → ❌ Watch 事件带 Zxid；如果丢了客户端可以对比 Zxid 知道缺了；重新注册 Watch 保证后续能触发 |
| 24 | ZK 内存数据树结构？| ConcurrentHashMap<String, DataNode> nodes；Key=znode path；Value=DataNode |
| 25 | ZK 写性能瓶颈？| 写请求都走 Leader → Leader 单点瓶颈；一般 ZK 集群不超过 10 节点 |
| 26 | ZK 启动流程？| Loading → 选 Leader/Election → Recovery Sync → Broadcasting |
| 27 | 典型生产用法？| Kafka Controller 选举（Zxid=epoch+counter）+ ClickHouse ReplicatedMergeTree LogEntry 协调 + Curator 分布式锁（临时有序 Znode + Watch）|

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：src/.../quorum/Zab.java + FastLeaderElection.java + WatchManager.java + DataTree.java  
**面试考点：27 题**  
**差异化点**：ZAB Broadcast 两阶段源码级、Recovery Zxid 最大选 Leader、Watch 一次性 + Zxid 一致性、**ZAB vs Raft vs Paxos 三协议全维度对比**
