# MySQL Group Replication Paxos + GTID 同步源码级深度解析

> **源码基线**：MySQL 8.0.x（C++）  
> **核心位置**：plugin/group_replication/ + plugin/group_replication/libmysqlgcs/  
> **面试权重**：⭐⭐⭐⭐⭐（MySQL 高可用核心，补全主从复制深度）

---

## 一、Group Replication 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MySQL Group Replication（MGR）= Paxos 多主复制集群         │
│                                                               │
│  核心机制（面试必背）：                                        │
│   ✅ 基于 Paxos 协议的分布式共识                                │
│   ✅ 乐观锁（Write Set）冲突检测                              │
│   ✅ GTID 自动定位 + 自动故障转移                              │
│   ✅ Single-Primary（单主）+ Multi-Primary（多主）两种模式     │
│   ✅ Built-in 插件（8.0 原生支持，不需要 Galera）             │
│                                                               │
│  与传统主从复制的核心区别（第六节对比表）：                    │
│   传统主从：Leader-Follower + Binlog 推 + 可能脑裂           │
│   MGR：    Paxos 共识 + 自动选主 + 防脑裂                    │
│                                                               │
│  ★ 核心优势：                                                 │
│   高可用：Primary 挂了自动选新 Primary（秒级）                │
│   防脑裂：Paxos 过半确认 → 不可能出现两个 Primary 同时写      │
│   多活：Multi-Primary 模式下多个节点都能写（但有冲突检测）    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、MGR 架构 + Paxos 共识源码级

### 2.1 MGR 三层架构

```cpp
// ★ Group Replication 三层架构（源码级）
//
// plugin/group_replication/
// ├── GCS（Group Communication Service）—— 通信层
// │   └── libmysqlgcs/plugin/group_replication/libmysqlgcs/
// │       → Paxos 协议实现（MGR 核心！）
// │
// ├── Certification Layer —— 冲突检测层
// │   → 乐观锁 + Write Set 比较
// │
// └── Binary Log —— 数据层
//     → 标准 MySQL Binlog 格式
//     → GTID 标识每个事务
//
// // ★ 写入流程（源码级，面试核心）：
// //
// // 1. 客户端向 Primary 节点发写请求
// //    Primary 执行 SQL → 生成 Binlog + GTID + Write Set
// //
// // 2. Primary 把 Write Set（乐观锁标识）发给所有其他节点
// //    // GCS Paxos 广播：broadcast_write_set(write_set, gtids)
// //
// // 3. 每个 Secondary 节点收到 Write Set → **Certification 检查冲突**
// //    // 乐观锁：检查 Write Set 中引用的行有没有被其他并发事务改过
// //    // 如果冲突 → 回滚 Primary 的该事务
// //    // 如果没冲突 → 记录下来（应用时使用）
// //
// // 4. 所有节点（包括 Primary）Binlog 广播 + 应用
// //    Primary：Commit 事务 → Binlog 发送给自己的 Relay
// //    Secondary：Apply Relay Log（应用 Primary 的 Binlog）
// //
// // 5. Paxos 过半确认 → Commit 成功
// //    （至少 N/2 + 1 个节点确认没冲突）
//
```

### 2.2 Paxos 协议实现

```cpp
// ★ MGR Paxos 协议源码级（面试必背）
//
// plugin/group_replication/libmysqlgcs/src/bindings/cpp/
//   interface/gcs_interface.h
//   plugin/gcs_tag.cpp
//
// // MGR Paxos 简化流程（源码级抽象）：
// //
// // 1. Primary 提出提案（Proposal）：
// //    Proposal = { write_set, gtids, binlog_events }
// //    → 向所有 GCS Member 广播 PREPARE
// //
// // 2. GCS Member 收到 PREPARE：
// //    → Certification 层检查 Write Set 冲突
// //    → 没冲突 → 广播 ACCEPT（带上自己的决策）
// //    → 有冲突 → 广播 REJECT
// //
// // 3. 过半 ACCEPT（≥ N/2 + 1）：
// //    → GCS Leader 广播 COMMIT
// //    → 所有节点 Commit 事务 + 应用 Binlog
// //    // Primary 本地也 Commit
// //
// // 4. 不足过半 ACCEPT：
// //    → GCS Leader 广播 ABORT
// //    → Primary 回滚该事务
// //
// // ★ Write Set 冲突检测核心（源码级）：
// // // plugin/group_replication/src/rpl_group_replication.cc
// //
// // WriteSet = 事务修改过的行集合（主键+列哈希）
// //   例：UPDATE orders SET status='paid' WHERE id=1001;
// //   → WriteSet = { (table=orders, pk=1001, col_hash=hash('status')) }
// //
// // Certification 检查：
// //   Secondary 收到 WriteSet
// //   → 对比 WriteSet 里的 (table, pk) 对
// //   → 有没有在自己本地的 "已接受 WriteSet" 里出现过？
// //   → 出现过 = 冲突（乐观锁失败）→ REJECT
// //   → 没出现 = OK → ACCEPT
// //
// // ★ 冲突检测时机：Paxos PREPARE 阶段
// //   冲突在事务 Commit 前就能检测到
// //   → 提前回滚，不需要等 Binlog Apply 时发现
// //
```

---

## 三、GTID + 自动故障转移源码级

### 3.1 GTID 标识

```cpp
// ★ MGR GTID 与传统主从 GTID 的核心区别（源码级）
//
// // 传统主从 GTID：
// //   server_uuid:transaction_id
// //   每个事务一个 GTID
// //   主从 GTID 集合：Executed_Gtid_Set
// //
// // MGR GTID 特殊之处：
// //   - MGR 所有节点共享同一个 group_replication_group_name（UUID 形式）
// //   - GTID 格式：group_replication_group_name:seq_no
// //     // group_replication_group_name = "aaaa-bbbb-cccc-dddd"
// //     // seq_no = 全局递增序号（所有节点共享）
// //     // GTID = "aaaa-bbbb-cccc-dddd:1,aaaa-bbbb-cccc-dddd:2"
// //   - 每个事务在整个 Group 里唯一标识
// //   - GTID 集合在所有节点上同步一致
// //
// // ★ GTID 自动定位（故障转移核心）：
// //
// // Primary 节点挂了：
// // 1. GCS Paxos 选举新 Primary
// //    // 选哪个？选 GTID 最大的（事务最新）
// //
// // 2. 新 Primary 选举流程：
// //    a. 所有存活节点广播自己的 Executed_Gtid_Set
// //    b. GTID Set 最完整（覆盖最多 seq_no）的成为新 Primary
// //    c. 如果多个 GTID Set 相同 → 选 server_uuid 最大的
// //    d. Paxos 过半确认新 Primary（防止脑裂）
// //
// // 3. 旧 Primary（如果没死透，网络分区后恢复）：
// //    // 可能还以为自己是 Primary
// //    // GCS Paxos 检测到网络分区恢复
// //    // → 旧 Primary 的 server UUID 不再是 Leader
// //    // → GCS 给它发送 PRIMARY STEP DOWN 请求
// //    // → 旧 Primary 自动降级为 Secondary
// //    // （这就是 Paxos 防脑裂的核心：只有过半节点选举的才是合法 Leader）
// //
// // ★ 脑裂防护源码级机制：
// //   网络分区：3 节点集群分成 [A, B] 和 [C]
// //   → [A, B] 能过半（2/3）→ 选新 Primary（假设 A 是 Primary）
// //   → [C] 不能过半（1/3）→ 不能选举新 Primary → C 变为只读
// //   → 网络恢复后 C 加入 → A 继续是 Primary
// //   → **不可能出现 [A, B] 选 A 当主、[C] 选 C 当主**
//
```

### 3.2 自动故障转移流程

```cpp
// ★ MGR 自动故障转移完整流程（源码级，面试核心）
//
// // plugin/group_replication/src/gr_autorejoin.cc
// // plugin/group_replication/src/rpl_group_replication.cc
//
// // 场景：3 节点 Single-Primary 集群
// //   Node A = Primary；Node B, C = Secondary
// //   Node A 突然宕机
// //
// // Step 1: GCS 检测到 Node A 失联
// //   Paxos 心跳超时 → GCS 将 Node A 标记为 UNREACHABLE
// //   // GCS 成员列表更新 → [B, C] 是存活节点
// //
// // Step 2: 存活节点触发 Primary 选举
// //   // Node B 和 C 都进入 ELECTING 状态
// //   // 对比各自的 Executed_Gtid_Set：
// //   //   Node B: aaaa:1-100
// //   //   Node C: aaaa:1-100（和 B 一样）
// //   // → GTID 相同 → 选 server_uuid 更大的 → Node B 当选
// //
// // Step 3: Paxos 过半确认新 Primary
// //   Node B 发出 PRIMARY STEP UP 提案
// //   → [B, C] 都 ACCEPT（2/3 过半）
// //   → GCS Commit：Node B 成为新 Primary
// //
// // Step 4: Node B 变成 Primary
// //   // 状态机：ELECTING → PRIMARY
// //   // Node C 继续做 Secondary
// //   // 应用之前 Node A 已写入但未同步到 C 的 Binlog
// //   // GTID Set 补齐（GTID=100 后如果 A 还有更高的 GTID → 等 A 恢复后补齐）
// //
// // Step 5: 客户端自动重连
// //   // MySQL 8.0 Connector/J 支持 Group Replication 感知
// //   //   Connector 缓存所有节点地址
// //   //   检测到 Primary 下线 → 自动切换到新 Primary
// //   // 无感知 Failover（秒级）
// //
// // ★ Multi-Primary 模式故障转移：
// //   Multi-Primary 没有单一 Primary → 每个节点都能写
// //   节点挂了 → 其他节点继续正常写（写其他 Table/Row）
// //   // 但挂的节点写过的数据 → 其他节点通过 GTID 自动补齐
//
```

---

## 四、Single-Primary vs Multi-Primary 源码级

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：MGR 两种模式对比                                 │
│  ┌──────────────┬──────────────────┬──────────────────┐     │
│  │ 维度            │ Single-Primary   │ Multi-Primary     │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 写入节点         │ ★ 只有 1 个     │ ★ 多个都能写      │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 冲突检测         │ ❌ 不需要（单点写） │ ★ 必须（多写冲突）│    │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 脑裂防护         │ ★ Paxos 选主    │ ★ Paxos 过半确认  │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 适用场景         │ 大多数业务       │ 多 Region 多活    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 优点            │ 简单、没冲突      │ ★ 高可用、分散写压力│    │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 缺点            │ Primary 单点压力  │ 写冲突可能回滚    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 生产推荐         │ ★ 首选           │ 谨慎使用（冲突多）│     │
│  └──────────────┴──────────────────┴──────────────────┘     │
│                                                               │
│  ★ Multi-Primary 写冲突处理：                                 │
│   两个 Primary 同时写同一行：                                 │
│   A 写 id=1001 status='paid'                                │
│   B 写 id=1001 status='refunded'                            │
│   → Paxos 先确认一个（A 的）→ B 的被拒绝（Write Set 冲突）│
│   → B 的事务回滚 → 应用层收到 ERROR 1062（ER_LOCK_DEADLOCK）│
│   → 应用层需要重试或改用 Single-Primary                      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、Group Replication vs 传统主从 vs Semi-Sync 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种 MySQL 高可用方案对比                        │
│  ┌──────────────┬──────────────────┬──────────────┬──────────┐│
│  │ 维度            │ MGR (Paxos)       │ 传统主从       │ Semi-Sync  ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 协议            │ ★ Paxos 共识      │ 异步 Binlog   │ Semi-Sync  ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 一致性          │ ★ Paxos 强一致  │ 最终一致       │ AFTER_SYNC ││
│  │                │                  │               │ 强一致     ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 自动选主        │ ★ 内置 Paxos    │ ❌ 需 MHA/Orchestrator│ 需 MHA  ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 脑裂防护        │ ★ Paxos 过半    │ ❌ 可能脑裂     │ ❌ 可能脑裂 ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 多主支持        │ ★ Multi-Primary │ ❌ 单主        │ ❌ 单主     ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ GTID 优势       │ 共享 group UUID │ 普通 UUID      │ 普通 UUID ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 性能            │ Paxos 开销      │ ★ 最低         │ 中        ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 运维复杂度       │ 中              │ ★ 最低         │ 低        ││
│  ├──────────────┼──────────────────┼──────────────┼──────────┤│
│  │ 面试高频         │ ★★★★★          │ ★★★★         │ ★★★★★     ││
│  └──────────────┴──────────────────┴──────────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   新项目优先：**MGR Single-Primary**（Paxos + 自动选主 + 防脑裂）│
│   存量项目：保留 Semi-Sync + MHA（简单稳定）                  │
│   多 Region 多活：MGR Multi-Primary（但冲突要处理）          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | MGR 核心机制？| **Paxos 协议 + Write Set 乐观锁冲突检测 + GTID 自动定位 + 自动故障转移** |
| 2 | ★ Paxos 两阶段在 MGR 里的流程？| PREPARE（广播 Write Set）→ Certification 检查 → ACCEPT/REJECT → 过半 Commit/ABORT |
| 3 | Write Set 冲突检测原理？| **WriteSet = (table, pk, col_hash)**；Secondary 收到后对比本地已接受的 WriteSet；重复 = 冲突 = REJECT |
| 4 | MGR GTID 和传统 GTID 区别？| **共享 group_replication_group_name:seq_no**（全局递增序号）；所有节点 GTID Set 同步一致 |
| 5 | ★ 故障转移自动选主规则？| **GTID Set 最完整（覆盖最多 seq_no）的存活节点 → 当选新 Primary** |
| 6 | MGR 怎么防脑裂？| **网络分区后 [N/2+1] 才能过半选举 → 不可能两个分区各选一个 Primary** |
| 7 | 网络分区恢复后的旧 Primary 怎么处理？| GCS 检测 → 旧 Primary 的 server_uuid 不再是 Leader → **自动降级为 Secondary** |
| 8 | Single-Primary vs Multi-Primary 核心区别？| **Single 只有 1 个能写（无冲突）；Multi 多个都能写（必须 Write Set 冲突检测）** |
| 9 | Multi-Primary 冲突处理？| Paxos 先确认的事务成功 → 后到的被拒绝（Write Set 冲突）→ 回滚 |
| 10 | ★ MGR vs 传统主从 vs Semi-Sync 三方案对比？| 见第五节对比表（10 维度）|
| 11 | 面试最高频考点排序？| **1. Paxos 两阶段 + Write Set 冲突检测 2. 自动选主 GTID 最完整 3. 防脑裂机制 4. Multi-Primary 冲突 5. 三方案对比** |
| 12 | 学习路径？| Paxos PREPARE → WriteSet → Certification → GTID Set → 选主流程 → 脑裂防护 |
| 13 | 面试常见追问？| 为什么 MGR Write Set 冲突检测在 Paxos PREPARE 阶段？| **提前发现冲突**（在 Commit 前）；避免 Binlog Apply 时才发现冲突（代价大）|
| 14 | 面试常见追问？| GTID Set 怎么判断"最完整"？| Executed_Gtid_Set 包含更大的 seq_no 范围 → 覆盖更多事务 → 选它 |
| 15 | 面试陷阱？| 说 "MGR 就是 Galera Cluster" → ❌ MGR 是 **MySQL 8.0 内置**；Galera 是 MariaDB 开源实现；都是 Paxos 但代码不同 |
| 16 | 面试陷阱？| 说 "MGR Multi-Primary 不会回滚事务" → ❌ **Write Set 冲突检测会主动回滚冲突事务**（ER_LOCK_DEADLOCK 错误码）|
| 17 | MGR GCS 是什么？| **Group Communication Service**；MGR 通信层；基于 Paxos 协议实现成员管理 + 共识 |
| 18 | MGR 要求的环境？| GTID 必须开启（gtid_mode=ON）；Binlog 必须是 ROW 格式（binlog_format=ROW）；最小 3 节点（防脑裂过半）|
| 19 | 网络分区 [A,B] vs [C] 恢复后 C 怎么同步？| C 的 GTID Set 落后 → GCS 从 Primary（A）补齐缺失的 GTID → 应用 Primary 的 Binlog |
| 20 | MGR 性能开销？| Paxos 两阶段 → 每次事务要等过半确认；比传统主从慢 10-30%；但换来一致性 |
| 21 | MGR 和 Semi-Sync 的差异？| Semi-Sync 只保证 Binlog 写到 Relay Log；**MGR Paxos 保证 Binlog 被过半节点 Commit** |
| 22 | 典型生产配置？| **3 节点 Single-Primary + GTID=ON + Binlog ROW + semi-sync + group_replication_group_name** |
| 23 | MGR 和 ProxySQL 配合？| ProxySQL 检测 MGR Primary → 写流量路由到 Primary → 故障自动切换 |
| 24 | MGR 的 group_replication_group_name 怎么来？| 唯一 UUID（类似 server_uuid）；所有节点必须一样 |
| 25 | 旧 Primary 恢复后会再当主吗？| **不会自动**；需要人为触发 Primary Switch（除非新 Primary 也挂了）|
| 26 | MGR 最大节点数？| 理论上 Paxos 不限制；实际生产推荐 ≤ 9 节点（Paxos 两阶段广播复杂度随 O(N) 增加）|
| 27 | MGR 和 Percona XtraDB Cluster 对比？| 都是 Paxos 多主；PXC 用 Galera；MGR 是 MySQL 内置；功能类似但生态不同 |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：plugin/group_replication/src/rpl_group_replication.cc + libmysqlgcs/src/ + gr_autorejoin.cc  
**面试考点：27 题**  
**差异化点**：Paxos PREPARE→ACCEPT→Commit 两阶段 + WriteSet (table,pk,col_hash) 冲突检测、**GTID 全局递增 seq_no 最完整选主**、脑裂过半防护、Single vs Multi-Primary、**MGR vs 传统主从 vs Semi-Sync 三方案 10 维度对比**
