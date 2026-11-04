# TiKV Raft Learner + Joint Consensus 成员变更源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）+ etcd/raft 库  
> **核心仓库**：tikv/tikv → components/raftstore/；etcd-io/raft  
> **论文来源**：Diego Ongaro《In Search of an Understandable Consensus Algorithm》§6  
> **面试权重**：⭐⭐⭐⭐（Raft 工程进阶，区分度高）

---

## 一、Raft 成员变更核心问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Raft 成员变更为什么不能直接改配置？（面试必背！）          │
│                                                               │
│  错误做法：Leader 直接把集群从 3 节点改成 5 节点              │
│                                                               │
│  问题：旧节点和新节点**各自看到不同的多数派**                 │
│   旧配置 C_old（3 节点）多数派 = 2                            │
│   新配置 C_new（5 节点）多数派 = 3                            │
│                                                               │
│   分裂时刻：                                                  │
│   旧 Leader 拿到 old 的 2 票（old 多数派）继续当 Leader      │
│   新节点中某节点拿到 new 的 3 票也当 Leader                  │
│   → ★ 脑裂！两个 Leader 同时存在！                           │
│                                                               │
│  根本原因：配置切换不是原子的，传播有延迟                     │
│  → 存在一个中间状态：部分节点用 old，部分用 new              │
│  → old 多数派 和 new 多数派可能不相交（各自为政）            │
│
│ ★ Raft 论文给了两种安全方案：                                 │
│  ① 单节点变更（One Server Change / 一次加一个）             │
│  ② 联合共识（Joint Consensus / C_old,new 两阶段）           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Learner 角色源码级

### 2.1 Learner 是什么

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Learner（学习者）= Non-voting Member（无投票权副本）      │
│                                                               │
│  Raft 标准三角色：Leader / Follower / Candidate              │
│  TiKV/etcd 增加第四角色：Learner                             │
│                                                               │
│  Learner 特点：                                                │
│   ✅ 从 Leader 接收日志复制（正常同步数据）                  │
│   ❌ 不参与投票（Leader 选举、Commit 确认都不算它）          │
│   ❌ 不能当选 Leader                                          │
│                                                               │
│ ★ 为什么加新节点要先当 Learner？（面试高频！）                │
│   新节点刚加入时数据是空的（落后很多）                        │
│   如果直接给它投票权：                                        │
│     ① Leader 要等它复制日志 → 它太慢拖慢整个集群提交         │
│     ② 更糟：旧 Leader 挂了，它日志最新被选成 Leader？        │
│        它数据不全 → 可能影响可用性                            │
│   → 先当 Learner 默默追数据                                   │
│   → 追平后再提升为 Voter（正式投票成员）                     │
│
│ // TiKV Region Peer 角色                                      │
// pub enum PeerRole {
//     Voter    = 0,  // 正式成员（有投票权）
//     Learner  = 1,  // 学习者（无投票权，只同步）
//     IncomingVoter = 2,  // 变更中：Learner → Voter
//     DemotingVoter = 3,  // 变更中：Voter → Learner
// }
│
│ ★ PD 添加副本标准流程（面试必背）：                           │
│   AddLearner（先加 Learner）                                  │
│     → 等 Learner 日志追平 Leader                             │
│     → PromoteLearnerToVoter（提升为正式成员）                │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 TiKV ConfChange 操作类型

```rust
// ★ TiKV/etcd Raft 成员变更操作类型（源码级）
//
// // raft/proto/confchange.proto
// enum ConfChangeType {
//     AddNode         = 0;  // 添加 Voter（旧接口，内部拆成 Learner+Promote）
//     RemoveNode      = 1;  // 删除节点
//     AddLearnerNode  = 2;  // ★ 添加 Learner
// }
//
// // etcd/raft 更细粒度的单步变更（Joint Consensus 用）
// enum ConfChangeSingleType {
//     AddNode         = 0;  // 直接加 Voter
//     RemoveNode      = 1;  // 删 Voter
//     UpdateNode      = 2;
//     AddLearnerNode  = 3;  // 加 Learner
//     PromoteLearner  = 4;  // ★ Learner 提升 Voter
//     RemoveLearner   = 5;
// }
//
// // PD 调度添加副本的 Operator Step 顺序（之前文档提到过）
// // ① AddLearner(store_id)        先以 Learner 加入
// // ② 等 apply 后检查进度追平
// // ③ PromoteLearnerToVoter       追平后提升
//
```

---

## 三、两种成员变更算法

### 3.1 单节点变更（One Server Change）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 方案一：单节点变更（etcd/TiKV 默认，简单高效）            │
│                                                               │
│  核心规则（Raft 论文证明）：                                  │
│   每次只增加或减少**一个节点**                                │
│   → C_old 和 C_new 的多数派必然有交集                        │
│   → 不可能出现两个不相交多数派 → 不会脑裂                    │
│                                                               │
│  证明（3 节点加 1 节点）：                                    │
│   old={A,B,C} 多数派需要 2 票                                │
│   new={A,B,C,D} 多数派需要 3 票                              │
│   任何 old 多数派（2 票）和 new 多数派（3 票）在 4 节点中     │
│   必然至少共享一个节点（鸽巢原理）→ 不会分裂                 │
│                                                               │
│  要从 3 节点扩到 5 节点：                                     │
│   ① AddLearner D → Promote D（3→4）                          │
│   ② AddLearner E → Promote E（4→5）                          │
│   每次单节点，串行执行                                       │
│                                                               │
// // etcd/raft 处理单节点变更
// func (r *raft) applyConfChange(cc pb.ConfChangeV2) {
//     if cc.Transition == pb.ConfChangeTransition_AUTO {
//         // 只有一个变更且离开 Joint 状态 → 直接单步生效
//         if len(changes) == 1 {
//             r.applyConfChangeSingle(changes[0])
//         }
//     }
// }
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Joint Consensus 联合共识

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 方案二：Joint Consensus（联合共识，一次变多个）           │
│                                                               │
│  适用场景：一次要加/减多个节点（比如跨机房扩容一次加 3 个）  │
│                                                               │
│  两阶段：                                                      │
│                                                               │
│  阶段 ①：进入联合配置 C_old,new                                │
│   Leader 创建一条日志，配置 = C_old ∪ C_new                  │
│   关键规则：                                                    │
│   ★ 日志提交需要【同时】满足 old 多数派 和 new 多数派        │
│   （两边都复制成功才算提交）                                  │
│   → 这保证了联合配置期间不可能脑裂                            │
│                                                               │
│  阶段 ②：离开联合配置，切到 C_new                             │
│   Leader 看到 C_old,new 日志已提交                            │
│   → 再发一条日志把配置切成纯 C_new                            │
│   → C_new 日志提交后，变更完成                                │
│
// // etcd/raft Joint 状态机
// func (r *raft) applyConfChange(cc pb.ConfChangeV2) {
//     if len(changes) > 1 {
//         // ★ 多个变更 → 进入 Joint 状态
//         // 配置同时包含 incoming（新）和 outgoing（旧）
//         r.prs.Config.Voters = MakeVoters(
//             Joint(leftVoters, rightVoters))  // C_old,new
//     }
// }
//
// // ★ Joint 配置下的投票确认：需要两个多数派都确认
// func (voters JointVoters) quorum() int {
//     // left(C_old) 和 right(C_new) 各自的多数派都要满足
// }
//
// // Leader 提交规则（raft.go）
// // 日志 index 被提交当且仅当：
// // ① C_old 的多数派复制了该 index
// // ② C_new 的多数派也复制了该 index
//
│ ★ Joint 状态下选举：                                          │
│   Candidate 必须同时获得 old 多数派和 new 多数派的选票        │
│   → 保证新 Leader 在两边都合法                                │
│
│ ★ TiKV 默认用单节点变更；复杂批量变更走 Joint Consensus      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、成员变更完整时序

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV 扩容添加一个 TiKV 节点完整时序（面试串联题！）        │
│                                                               │
│  ① 新 TiKV 启动 → 向 PD 注册（StoreHeartbeat）               │
│                                                               │
│  ② PD balance-region 调度器发现需要补副本                     │
│     生成 Operator：AddLearner(Store4) → PromoteLearner       │
│                                                               │
│  ③ PD 给 Region Leader 下发 AddLearner ConfChange            │
│     Leader 写一条 ConfChange 日志（AddLearner）              │
│     → 复制到 old 多数派 → Commit → Apply                    │
│     → 新 Peer 以 Learner 身份加入，开始追日志                │
│                                                               │
│  ④ Learner 追数据（Leader 持续发 Snapshot/Append）           │
│     PD 通过 RegionHeartbeat 观察 Learner 的 match index      │
│     → 等到追平 Leader（落后 < 阈值）                         │
│                                                               │
│  ⑤ PD 下发 PromoteLearner ConfChange                         │
│     Leader 写 ConfChange（Learner→Voter）日志                │
│     → 复制到新的多数派 → Commit → Apply                     │
│     → 新节点正式成为 Voter，成员变更完成                     │
│                                                               │
│  ⑥ 之后 PD 可以安全地把旧节点的 Peer 迁走/下线               │
│
│ ★ 下线旧节点（缩容）：                                        │
│   evict-leader 驱逐 Leader + RemoveNode 逐个删 Peer          │
│   删到最后一个 Peer 前 Region 必须有足够其他副本             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Raft 为什么不能直接改配置？| 新旧配置多数派可能不相交 → **脑裂（两个 Leader）** |
| 2 | 两种安全变更方案？| **单节点变更（一次一个）+ Joint Consensus（联合共识两阶段）** |
| 3 | Learner 角色？| **无投票权副本**：接收日志同步但不投票、不能当 Leader |
| 4 | 为什么先加 Learner？| 新节点数据空，直接给投票权会拖慢提交 + 可能被选成 Leader 影响可用 |
| 5 | 单节点变更为什么安全？| 一次加/减一个 → 新旧多数派**必然有交集**（鸽巢原理）|
| 6 | Joint Consensus 两阶段？| ① 进 C_old,new（**两个多数派都确认才提交**）② 切纯 C_new |
| 7 | Joint 状态选举规则？| Candidate 必须同时获得 old 多数派和 new 多数派选票 |
| 8 | TiKV 添加副本流程？| **AddLearner → 等追平 → PromoteLearnerToVoter** |
| 9 | ConfChange 怎么传播？| 作为一条特殊 Raft 日志复制 → Commit → Apply 后生效 |
| 10 | etcd ConfChange 类型？| AddNode/RemoveNode/AddLearner/**PromoteLearner**/RemoveLearner |
| 11 | 面试最高频考点排序？| **1. Learner 作用 2. 直接改配置脑裂 3. 单节点变更安全性 4. Joint 两阶段 5. 扩容时序** |
| 12 | 学习路径？| PeerRole 枚举 → ConfChangeType → etcd raft applyConfChange → JointVoters |
| 13 | 面试常见追问？| Learner 追数据怎么追？→ Leader 发 InstallSnapshot 快照 + Append 日志 |
| 14 | Learner 算法定人数吗？| **不算**；选举和提交都只数 Voter |
| 15 | 3→5 节点怎么做？| 单节点变更分两次（3→4→5）；或 Joint 一次完成 |
| 16 | PD 怎么知道 Learner 追平？| RegionHeartbeat 上报 Learner 的 match/region 状态 |
| 17 | 下线节点流程？| evict-leader 驱逐 Leader → RemoveNode 逐个删 Peer |
| 18 | Joint 配置长什么样？| Voters 包含两组：left（旧）和 right（新），提交需两组各自多数派 |
| 19 | 成员变更期间 Leader 挂了？| 新选举在 Joint 下需双多数派；日志中未提交的 ConfChange 不生效 |
| 20 | TiKV vs etcd vs ZK 成员变更？| TiKV/etcd 用 Raft Learner + Joint；ZK ZAB 用不同机制（观察者 Observer）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：components/raftstore/ + raft/proto/confchange.proto + etcd/raft  
**论文**：Diego Ongaro Raft 论文 §6 Cluster Membership Changes  
**面试考点：20 题**  
**差异化点**：配置变更脑裂根因 + Learner 追平再提升 + 单节点 vs Joint 两方案 + 扩容完整时序
