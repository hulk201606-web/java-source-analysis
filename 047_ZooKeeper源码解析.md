# Apache ZooKeeper 源码深度解析（分布式协调）

> **源码基线**：Apache ZooKeeper 3.9.x（最新稳定）  
> **核心仓库**：https://github.com/apache/zookeeper  
> **面试权重**：⭐⭐⭐⭐⭐（分布式高频，Nacos/Eureka 替代方案）

---

## 一、Zookeeper 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式协调服务（配置中心 + 注册中心 + 分布式锁 + 选主）│
│  数据模型：ZNode（类似 Unix 文件系统，树状结构）               │
│                                                               │
│  ZNode 四种类型：                                             │
│  ① Persistent（持久，默认）→ 创建后一直存在直到删除            │
│  ② Ephemeral（临时）→ 客户端断开自动删除                      │
│  ③ Persistent Sequential → 持久 + 自动加递增序号              │
│  ④ Ephemeral Sequential → 临时 + 自动加递增序号               │
│                                                               │
│  ★ ZNode 核心数据：                                          │
│  data（数据，最大 1MB）+ children（子节点列表）+ Stat（元数据）│
│  Stat：cZxid（创建事务 ID）+ mZxid（修改事务 ID）+ ctime + mtime│
│        + version（修改次数）+ ephemeralOwner（临时节点所有者）│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  集群角色（ZAB 协议 + FastLeaderElection 选主）                │
│                                                               │
│  Leader：处理写请求 + 协调集群 + 广播 Propose/Commit          │
│  Follower：处理读请求 + 接收 Leader 广播 + 投票选主           │
│  Observer：处理读请求 + 不参与投票（扩展读性能，不影响写性能）  │
│                                                               │
│  ZAB 协议核心流程（写请求）：                                  │
│  Client → Follower（写请求不能直接发 Follower）               │
│    → FollowerRequestProcessor → 转发给 Leader                │
│      → LeaderRequestProcessor                                │
│        → Propose（向所有 Follower 发 PROPOSE）               │
│        → Follower 收到 PROPOSE → 本地写 Pre-Log → ACK        │
│        → Leader 收到 Quorum（半数以上）ACK                   │
│        → Commit（向所有 Follower 发 COMMIT）                 │
│        → Follower 本地 Commit → ACK                          │
│        → Leader 返回 Client OK                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 FastLeaderElection（★★★★★ 选主核心）

```java
// ★ FastLeaderElection 是 ZK 默认的选主算法
//   触发时机：集群启动 / Leader 挂了 / 主动重启
//
// 投票规则（两个比较维度）：
//   ① 优先选 zxid（事务 ID）最大的节点 → 保证选的 Leader 有最新数据
//   ② zxid 相同 → 选 myid（服务器 ID）最小的节点 → 确定性选择
//
// 投票流程：
//   1. 每个节点先给自己投票（vote）
//   2. 通过 QuorumCnxManager（TCP，3.5+ 之前用 UDP）广播给所有其他节点
//   3. 收集其他节点的 vote → 更新自己的 vote
//   4. 检查是否 Quorum（半数以上）同意选自己
//   5. 是 → 当选 Leader；否 → 继续等下一轮投票
//
// ★ 为什么要优先选 zxid 最大的？
//   如果选了 zxid 小的 → 它没有最新的事务数据 → 其他 Follower 有 → 集群数据丢失！
//   → zxid 最大的节点一定有最新数据 → 选它不丢数据
//
// ★ 选主参数：
//   finalizeWait（收集投票最多等多久）
//   initLimit（Follower 启动时等 Leader 同步的时间）
//   syncLimit（运行时 Leader 和 Follower 同步超时时间）
```

### 2.2 ZAB 协议（★★★★ 共识核心）

```java
// ★ ZAB（ZooKeeper Atomic Broadcast）是 ZK 的核心共识协议
//   基于 Paxos 的简化版，但比 Paxos 简单可靠
//
// ZAB 三个阶段：
//   ① Discovery（发现）：节点启动时发现 Leader
//   ② Recovery（恢复）：新 Leader 当选 → Follower 同步数据
//   ③ Broadcast（广播）：正常运行 → Leader 广播写请求
//
// ★ Recovery 阶段（新 Leader 当选后）：
//   Leader 向所有 Follower 发送 NEWLEADER
//   Follower 收到 → 回 ACK
//   Leader 收集 Quorum ACK → 选 NEWLEADER 阶段完成
//   Leader 开始同步数据给 Follower（按 zxid 对齐）
//   → Follower 数据落后多少，同步多少
//   → 同步完 → 开始接受 Client 请求
//
// ★ Broadcast 阶段的 Propose 流程（保证一致性）：
//   Leader 收到写请求
//     ① 本地写 Pre-Log（事务前日志）
//     ② 向所有 Follower 发 PROPOSE
//     ③ Follower 写 Pre-Log → ACK Leader
//     ④ Leader 收 Quorum ACK → 本地 Commit
//     ⑤ 向所有 Follower 发 COMMIT
//     ⑥ Follower 本地 Commit → 事务正式生效
//     ⑦ Leader 返回 Client OK
//
// ★ ZAB vs Raft vs Paxos：
//   ZAB：简化版 Paxos（两阶段提交 + 强制顺序）→ ZK 专用
//   Raft：工程化 Paxos（选主/日志复制/安全）→ Etcd/consul 使用
//   Paxos：学术版（灵活但实现复杂）→ 工业界很少直接实现
```

### 2.3 Watch（★ 监听机制核心）

```java
// ★ Watch 是 ZK 最独特的功能！
//
// Watch 核心特性：
//   ① **一次性触发**（One-time trigger）：事件触发后 Watch 自动删除 → 要重新注册
//   ② **客户端**：Watch 注册在 Client 本地（不是 Server！）→ Server 只负责通知 Client
//   ③ **异步通知**：Watch 事件通过 WatchEvent 回调通知（不是同步阻塞）
//
// Watch 触发场景：
//   NodeDataChanged（节点数据变）
//   NodeDeleted（节点删除）
//   NodeCreated（节点创建）
//   NodeChildrenChanged（子节点列表变）
//   DataWatch（父节点的 Watch，子节点变化也触发）
//
// 内部实现：
//   Client：WatchManager（维护本地 Watch 列表）
//   Server：WatchManager（维护 Server 端 Watch 列表 + 会话）
//
// Server 端 WatchManager 结构：
//   HashMap<String, HashSet<Watcher>> watchTable  ← path → Watchers
//   HashMap<Watcher, HashSet<String>> watch2Paths  ← Watcher → paths
//
// 触发流程：
//   ① Client 调 getData(path, watch=true)
//   ② Server 收到请求 → 查 WatchManager → 注册 Watcher
//   ③ 后来 Client 调 setData(path)
//   ④ Server → WatchManager.triggerWatch(path, NodeDataChanged)
//   ⑤ 找到该 path 对应的所有 Watcher → 异步通知
//   ⑥ Client 收到通知 → WatchEvent 回调 → 重新注册 Watch（因为一次性）
//
// ★ Watch 一个经典坑：
//   while (true) {
//       getData(path, watch=true);
//       wait();  // ← WatchEvent 会 notify
//   }
//   → 必须在回调里重新注册 Watch（因为一次性触发）
//   → 忘记注册 → 下次变化收不到通知！
```

### 2.4 RequestProcessor 链（★ 处理链）

```java
// ★ RequestProcessor 链：ZK 请求处理核心（类似 Sentinel Slot 链 / Dubbo Filter 链）
//
// Leader 端处理链：
//   PrepRequestProcessor（预处理）
//     → ProposalRequestProcessor（提交事务）
//       → CommitRequestProcessor（提交）
//
// Follower 端处理链：
//   FollowerRequestProcessor（普通请求）
//     → SyncRequestProcessor（写请求同步到磁盘）
//       → SendAckRequestProcessor（ACK Leader）
//
// Observer 端处理链：
//   ObserverRequestProcessor（只处理读请求）
//     → SendAckRequestProcessor（ACK Leader）
//
// ★ ZK 写请求必须经过 Leader？
//   Follower / Observer 收到写请求 → FollowerRequestProcessor 检测
//   → 转发给 Leader（FollowerForwardingRequestProcessor）
//   → Leader 处理 → Broadcast → 返回
//   → 保证写请求串行化（Leader 单线程处理写请求）
```

### 2.5 分布式锁（面试高频应用）

```java
// ★ ZK 分布式锁常见实现（面试常考！）
//
// 方案一：节点临时 + 顺序（推荐！）
//   1. 在 /lock 目录下创建 EPHEMERAL_SEQUENTIAL 节点
//      /lock/lock-00001
//      /lock/lock-00002
//   2. 取自己的序号 → 检查是不是最小的
//      if (mySeq == minSeq) → 获取锁成功！
//      else → watch 前一个节点（/lock/lock-00001）
//   3. 前一个节点删除 → Watch 触发 → 重新检查自己是不是最小
//   4. 释放锁：删除自己的临时节点
//
// ★ 为什么用临时节点？
//   Client 挂了 → 临时节点自动删除 → 下一个节点 Watch 触发 → 自动获取锁
//   → 不会死锁！
//
// ★ Redisson 分布式锁原理（基于 ZK）：
//   RedissonLock → Lua 脚本 + ZK EPHEMERAL_SEQUENTIAL + Watch
//   → 还有 RedLock（多 ZK 节点同时加锁，防止单点）
//
// ★ ZK vs Redis 分布式锁对比：
//   ZK：**强一致性**（Paxos/ZAB）+ Watch 机制自动唤醒 → 但性能低
//   Redis：高性能 + Lua 脚本原子性 → 但 RedLock 有争议（GC pause 可能失效）
//   → 金融类选 ZK；高并发选 Redis
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ZK 集群角色？| **Leader（处理写 + 广播）/ Follower（读 + 投票）/ Observer（只读，不投票，扩展读性能）** |
| 2 | FastLeaderElection 选主规则？| **优先选 zxid（事务 ID）最大的节点**（保证数据最新）→ zxid 相同选 myid 最小的；通过 TCP（3.5+）广播投票 |
| 3 | ZAB 协议三阶段？| **Discovery（发现 Leader）/ Recovery（恢复，新 Leader 同步数据给 Follower）/ Broadcast（广播写请求，两阶段 Propose + Commit）** |
| 4 | Watch 核心特性？| **一次性触发**（事件触发后自动删除 → 必须重新注册）+ 客户端本地管理 + 异步回调 |
| 5 | ZK 分布式锁实现？| **EPHEMERAL_SEQUENTIAL 临时顺序节点** → watch 前一个节点 → 最小序号获取锁 → 释放锁删除节点 |
| 6 | ZK vs Nacos？| ZK：**只有注册 + 配置 + 锁**；Nacos：注册 + 配置 + 集群（更丰富）+ 控制台；但 ZK 是工业界事实标准 |
| 7 | ZK 写请求必须经过 Leader？| 是的！Follower/Observer 收到写请求 → 转发 Leader（FollowerForwardingRequestProcessor）→ Leader 串行化处理 |
| 8 | zxid 是什么？| **ZooKeeper Transaction ID**（事务 ID）→ 单调递增；每个写请求 +1；**选主时优先选 zxid 最大的**（保证数据不丢）|
| 9 | Session 机制？| Client 和 ZK 保持长连接 → 心跳（10s）→ Session 过期（30s 默认）→ 临时节点自动删除 |
| 10 | ZNode 四种类型？| **Persistent（持久）/ Ephemeral（临时）/ Persistent Sequential / Ephemeral Sequential**（后两种自动加递增序号）|
| 11 | Propose 两阶段？| ① Leader 广播 PROPOSE → Follower 写 Pre-Log → ACK；② Leader 收 Quorum ACK → 广播 COMMIT → Follower 本地 Commit |
| 12 | Quorum 是什么？| **半数以上节点**（N/2 + 1）；ZK 集群不能超过半数节点挂掉才能工作 |
| 13 | Watch 注册位置？| **客户端本地（WatchManager）+ Server 端（WatchManager）**；Server 负责存储 Watch 列表并触发事件；Client 收到通知执行回调 |
| 14 | RequestProcessor 链（Leader）？| **PrepRequestProcessor → ProposalRequestProcessor → CommitRequestProcessor** → ZK 请求处理的责任链模型 |
| 15 | ZK 最大 ZNode 数据量？| **单个 ZNode 最大 1MB 数据**；每个 ZNode 数据是不可分割的整体（修改用 setData 替换整块）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache ZooKeeper 3.9.x  
**面试考点：15 题**
