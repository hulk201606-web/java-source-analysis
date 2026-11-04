# Elasticsearch Cross-Cluster Search + Shard Allocation 源码级深度解析

> **源码基线**：Elasticsearch 8.x（Java）  
> **核心位置**：server/src/main/java/.../cluster/ + action/indices/ + IndicesShardAllocate  
> **面试权重**：⭐⭐⭐⭐⭐（ES 集群管理核心，高频）

---

## 一、ES Shard Allocation 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Shard Allocation = ES 如何决定把 Shard 放哪台节点上       │
│                                                               │
│  核心目标（面试必背）：                                        │
│   ✅ 数据均衡（节点间 Shard 数量 / 磁盘占用尽量均匀）         │
│   ✅ 高可用（Primary + Replica 不落在同一节点 / 同一 Rack）   │
│   ✅ 性能隔离（冷热节点区分）                                  │
│   ✅ 磁盘水位保护（Disk Watermark 自动挪走 Shard）            │
│   ✅ 感知 Rack（Rack Awareness）                              │
│                                                               │
│  Allocation 触发时机：                                         │
│   ① 新建 Index（按 mapping 规则分配 Primary）                 │
│   ② 节点加入/离开集群（Gossip 感知）                         │
│   ③ 磁盘水位超限（Disk Watermark）                            │
│   ④ 手动 Reroute（/_cluster/reroute）                        │
│   ⑤ Shard 迁移完成（Replica 同步）                           │
│                                                               │
│  核心代码路径：                                                │
│   server/.../cluster/metadata/IndexMetadata.java             │
│   server/.../cluster/routing/IndexShardRoutingTable.java      │
│   server/.../cluster/allocation/Allocator.java               │
│   server/.../cluster/allocation/ShardsAllocator.java         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Allocator 决策链源码级

### 2.1 AllocationDecider 决策器链

```java
// ★ ES AllocationDecider 决策器链（源码级，面试核心）
//
// server/src/main/java/org/elasticsearch/cluster/allocation/Allocator.java
// server/.../allocation/decider/*.java（8.x 有 10+ Decider）
//
// // Allocator 主循环（每个 Period 执行一次，默认 10s）：
// public void allocate(RoutingAllocation allocation) {
//     // ① 遍历所有需要分配的 Shard（Unassigned Shards）
//     for (ShardRouting shard : unassignedShards) {
//         // ② 对每个候选节点，跑所有 AllocationDecider
//         for (DiscoveryNode node : candidateNodes) {
//             Decision decision = decideAllocation(shard, node, allocation);
//             //   Decider 链模式：所有 Decider 都返回 YES 才是最终 YES
//             //   任何一个返回 NO / THROTTLE → 整体不通过
//             if (decision == Decision.YES) {
//                 // 选这个节点
//                 assign(shard, node);
//                 break;
//             }
//         }
//     }
// }
//
// // ★ 核心 AllocationDecider 清单（面试高频）：
// //
// // 1. ThrottlingDecider（节流控制）
// //    - concurrent_recoveries_limit：并发恢复上限（默认节点数 × 2）
// //    - Throttled → 暂时不能分配（等恢复腾出槽位）
// //
// // 2. FilterAllocationDecider（手动过滤）
// //    - cluster.routing.allocation.include/exclude/require
// //    - index.routing.allocation.include/exclude/require
// //    - 例：index.routing.allocation.include._name=node_hot_*
// //
// // 3. DiskThresholdDecider（磁盘水位保护）★ 面试核心
// //    - high watermark: 90%（默认）
// //    - flood stage watermark: 95%（默认，更严格）
// //    - disk_watermark_enabled: true（默认）
// //    - 磁盘超 high watermark → 该节点 **不能再接收新 Shard**
// //    - 磁盘超 flood watermark → 该节点所有 Index **只读**（禁止写）
// //
// // 4. RackAwareAllocationDecider（Rack 感知）
// //    - cluster.routing.allocation.awareness.attributes=rack_id
// //    - Primary + Replica 必须落在不同 rack_id
// //    - 防止整个 Rack 断电导致数据丢失
// //
// // 5. ShardsLimitAllocationDecider（节点最大 Shard 数）
// //    - cluster.routing.allocation.total_shards_per_node（默认 -1 无限制）
// //    - index.routing.allocation.total_shards_per_node
// //    - 防止单节点 Shard 过多压垮
// //
// // 6. NewVersionInitializingDecider（新版本节点）
// //    - 节点刚启动还没加入任何 Shard → 先不给分配（等 1 分钟）
// //
// // 7. SameShardAllocationDecider（防自环）
// //    - Primary 和 Replica 不能在同一节点
// //    - 一个节点不能同时有一个 Shard 的 Primary 和 Replica
//
```

### 2.2 Disk Watermark 自动迁移

```java
// ★ DiskThresholdDecider 源码级（面试高频）
//
// server/.../allocation/decider/DiskThresholdDecider.java
//
// // 磁盘水位分级：
// enum DiskThreshold {
//     NORMAL,          // 磁盘正常（< high watermark）
//     HIGH,            // 超 high watermark（90%）→ 拒绝新分配
//     FLOOD_STAGE      // 超 flood watermark（95%）→ 只读模式
// }
//
// // 决策流程：
// public Decision canAllocate(ShardRouting shard, DiscoveryNode node, ...) {
//     DiskUsage usage = getDiskUsage(node);
//     //    usage.totalBytes, usage.freeBytes, usage.usedBytes, usage.path
//     DiskThreshold threshold = evaluateThreshold(usage);
//
//     switch (threshold) {
//         case FLOOD_STAGE:
//             // ★ 节点进入 Flood Stage → 不允许任何写入
//             //    不允许分配任何 Shard
//             //    标记所有 Index 只读（Cluster state 更新）
//             return Decision.NO;
//
//         case HIGH:
//             // ★ 节点超 high watermark → 拒绝新分配
//             //    但已有的 Shard 不强制搬走（除非有其他节点可用）
//             return Decision.THROTTLE;
//
//         case NORMAL:
//             return Decision.YES;
//     }
// }
//
// // ★ 磁盘水位触发迁移：
// // 后台 DiskMonitor 线程（默认 30s 检查一次）：
// //
// // 节点进入 HIGH → Allocator 尝试把该节点上的 Replica 搬走
// //   → 找其他磁盘正常的节点 → 发起 Reallocate Replica
// //   → 迁走后该节点磁盘压力下降
// //
// // 节点进入 FLOOD_STAGE → 强制只读
// //   → 所有 Index 禁止写请求 → 运维告警
// //   → 必须加磁盘容量或手动 Reroute 搬走 Shard
//
// ★ Disk Watermark 核心参数（面试高频）：
//   cluster.routing.allocation.disk.watermark.low = 85%
//   cluster.routing.allocation.disk.watermark.high = 90%
//   cluster.routing.allocation.disk.watermark.flood_stage = 95%
//   cluster.routing.allocation.disk.watermark.enable_for_single_data_node = true
//
```

### 2.3 Rack Awareness

```java
// ★ RackAwareAllocationDecider 源码级
//
// // 配置：
// PUT /_cluster/settings {
//   "persistent": {
//     "cluster.routing.allocation.awareness.attributes": "rack_id",
//     "cluster.routing.allocation.force.rack_id.values": "rack_a,rack_b,rack_c"
//   }
// }
//
// // 决策逻辑：
// public Decision canAllocate(ShardRouting shard, DiscoveryNode node, ...) {
//     String rack = node.attributes().get("rack_id");
//     // 统计这个 Shard 已经在哪些 Rack 上有副本
//     Set<String> existingRacks = getExistingRacks(shard);
//
//     if (existingRacks.contains(rack)) {
//         // ★ 本 Rack 已经有这个 Shard 的副本 → 拒绝
//         return Decision.NO;
//     }
//     return Decision.YES;
// }
//
// ★ Rack Awareness 效果：
//   - 防止整个 Rack 断电导致数据全丢
//   - 典型 3 台 Rack，3 Primary + 3 Replica → Rack 交叉分布
//   - 与跨 Zone 部署配合（AWS/Azure/GCP）
//
// ★ 与 Force Awareness 区别：
//   Awareness：尽力而为（找不到跨 Rack 节点就还是分配）
//   Force：强制（找不到 → 直接不分配，等 Rack 节点加入）
//   cluster.routing.allocation.force.rack_id.values = "rack_a,rack_b,rack_c"
//
```

---

## 三、Cross-Cluster Search 源码级

### 3.1 CCS 核心机制

```java
// ★ Cross-Cluster Search（CCS）核心源码级
//
// server/.../action/search/ccs/...（ES 7.3+ 引入）
//
// // CCS 注册远程集群：
// PUT /_cluster/settings {
//   "persistent": {
//     "cluster.remote.us-east.seeds": ["host1:9300", "host2:9300"],
//     "cluster.remote.us-west.seeds": ["host3:9300"]
//   }
// }
//
// // CCS 查询语法：
// GET /orders,us-east:orders,us-west:orders/_search { ... }
//
// // CCS 核心原理（源码级）：
// // 旧版：Federated Search → 每个查询节点都去远程集群拉结果 → 性能差
// // 新版 CCS：
// //   1. 本地节点作为协调节点（Coordinating Node）
// //   2. 把查询 Fan-Out 到本地 Shard + 远程集群 Shard
// //   3. 远程集群用 Transport Client 直接发查询请求
// //   4. 各集群结果拉回来 → 协调节点 Merge + 分页
//
// // // 核心代码路径：
// // CCSRequest → RemoteClusterConnection → TransportSearchAction
// //
// public class CcsRequest {
//     List<Clusters> clusters;  // 本地 + 远程集群列表
//     SearchRequest searchRequest;  // 统一查询
//     Coordination coordination;    // 同步/异步/最小耗时协调
// }
//
// public CcsResponse execute(CcsRequest request) {
//     // 1. 本地集群执行：本地 TransportSearchAction
//     Future<CcsResponse> localFuture = asyncLocalSearch(request);
//
//     // 2. 远程集群执行：每个远程集群并发发 TransportSearch
//     List<Future<CcsResponse>> remoteFutures = request.getRemoteClusters().stream()
//         .map(cluster -> asyncRemoteSearch(cluster, request))
//         .collect(toList());
//
//     // 3. 等所有完成（coordination 决定等待策略）
//     List<CcsResponse> allResponses = waitForAll(localFuture, remoteFutures);
//
//     // 4. Merge 结果（按 Sort 排序 + 分页）
//     return mergeResults(allResponses, request.getSort(), request.getFrom(), request.getSize());
// }
//
```

### 3.2 CCS 协调策略

```java
// ★ CCS 三种协调策略（源码级，面试高频）
//
// // 1. Synchronous（默认，普通查询）
// //    所有集群返回后才返回
// //    最快集群也要等最慢的 → 最差情况延迟
// //    "所有集群都查完 → 统一排序分页"
//
// // 2. Simplified（异步，ES 8.x 新增）
// //    远程集群用 Transport Client 直接查
// //    不等待最慢的 → 更快但可能丢数据
// //    适合实时 Dashboard（允许几秒延迟）
//
// // 3. MinimizeLatency（最小延迟）
// //    先返回快的集群结果
// //    慢集群超时（默认 30s）后返回部分结果
// //    适合用户体验优先的场景
//
// // CCS 分页挑战（面试高频）：
// // 各集群的排序是独立的
// // 本地 [1, 3, 5] + 远程 [2, 4, 6]
// // 协调节点要 Merge 成 [1, 2, 3, 4, 5, 6]
// // 深分页 from=10000 → 每个集群拉 10000+size → 协调节点 Merge
// // → 深分页跨集群性能差（各集群都得拉全量前 N 条）
// // → 建议：PIT（Point In Time）+ Search After
//
// ★ CCS vs 传统 Federation Search：
// ┌──────────┬──────────────┬──────────────┐
// │            │ CCS (ES 7.3+) │ Federation     │
// ├──────────┼──────────────┼──────────────┤
// │ 查询方式   │ 远程 Transport  │ 每节点都代理  │
// │ 并发       │ ★ 原生并发    │ 单节点串行    │
// │ 排序 Merge │ ★ 协调节点    │ 各代理各自    │
// │ 性能       │ ★ 高          │ 低            │
// │ 实现复杂度 │ 中            │ 高（多代理）  │
// └──────────┴──────────────┴──────────────┘
//
```

---

## 四、ES CCS vs ClickHouse Distributed Table 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比：跨集群查询方案                               │
│  ┌──────────┬──────────────────┬──────────────────┐         │
│  │            │ ES Cross-Cluster │ ClickHouse Distributed  │  │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 架构       │ Transport Client │ Distributed Engine │        │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 节点角色   │ 协调节点 Merge  │ 本地引擎分发      │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ Shard 粒度 │ Primary 直查   │ Replica 直查      │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 排序 Merge │ ★ 协调节点    │ 引擎层 Merge      │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 分页       │ 深分页差        │ 深分页差（相同）   │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 同步/异步   │ 三种协调策略    │ 同步              │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 元数据同步   │ 注册远程集群    │ 本地 Distributed   │         │
│  │            │ 不共享元数据     │ Table 指向远程     │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 适用场景   │ 跨 Region 搜索  │ 跨 Region 数仓   │         │
│  ├──────────┼──────────────────┼──────────────────┤         │
│  │ 面试高频   │ ★★★★★          │ ★★★★★          │         │
│  └──────────┴──────────────────┴──────────────────┘         │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   跨 Region 搜索 → ES CCS（Transport Client + 协调节点 Merge）│
│   跨 Region 数仓 → ClickHouse Distributed（引擎层分发）     │
│   两者配合：ES 做搜索 + ClickHouse 做聚合 + CCS/Distributed 跨 Region │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（26 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ES Shard Allocation 核心目标？| **数据均衡 + 高可用（Primary/Replica 不同节点）+ 性能隔离 + 磁盘水位保护** |
| 2 | AllocationDecider 链模式？| 所有 Decider 依次检查；**任何一个返回 NO/THROTTLE → 整体不通过** |
| 3 | ★ 核心 AllocationDecider 清单？| **Throttling + Filter + DiskThreshold + RackAware + ShardsLimit + NewVersion + SameShard** |
| 4 | Disk Watermark 三级？| **NORMAL（<85%）→ HIGH（85-90%，拒绝新分配）→ FLOOD_STAGE（>90%，只读）** |
| 5 | Disk Watermark 触发什么操作？| HIGH → 迁走 Replica；FLOOD_STAGE → 所有 Index 只读 |
| 6 | Rack Awareness 作用？| **Primary 和 Replica 必须落在不同 Rack**；防整个 Rack 断电数据丢 |
| 7 | Rack Awareness vs Force Awareness？| Aware 尽力而为；**Force 强制（找不到 Rack 就不分配，等 Rack 节点加入）** |
| 8 | ShardsLimit 是什么？| cluster.routing.allocation.total_shards_per_node；**防单节点 Shard 过多压垮** |
| 9 | ★ CCS（Cross-Cluster Search）是什么？| ES 7.3+ 跨集群查询；协调节点 Fan-Out 本地+远程 → Merge → 返回 |
| 10 | CCS vs 传统 Federation Search？| **CCS 原生并发 + Transport Client；Federation 单节点串行代理** |
| 11 | CCS 三种协调策略？| **Synchronous（全等）+ Simplified（异步快）+ MinimizeLatency（先快后慢）** |
| 12 | CCS 分页挑战？| 各集群独立排序；协调节点 Merge；**深分页 from=10000 各集群都得拉前 N 条 → 性能差** |
| 13 | CCS 注册远程集群？| `cluster.remote.{alias}.seeds = ["host:port", ...]`；不共享元数据，运行时发现 |
| 14 | Allocator 执行频率？| 默认每 10s（cluster.routing.allocation.node_concurrent_recoveries）|
| 15 | 面试最高频考点排序？| **1. AllocationDecider 链 2. Disk Watermark 三级 3. Rack Awareness 4. CCS 核心机制 5. CCS vs Distributed** |
| 16 | 学习路径？| Allocator → Decider 链 → DiskThresholdDecider → RackAwareDecider → CCS 源码 |
| 17 | 面试常见追问？| 为什么不允许 Primary 和 Replica 在同一节点？| 节点挂 → Primary+Replica 全丢 → 无副本 → 数据丢失 |
| 18 | 面试常见追问？| Disk Watermark 怎么触发节点级 Read-Only？| cluster state 更新 → 所有 Index 禁止写请求 → 写入返回 `cluster read-only / allow delete` |
| 19 | 面试陷阱？| 说 CCS 和 Federation 是一回事 → ❌ CCS 是 ES 7.3+ 新实现，原生并发；Federation 是 ES 5.x 代理层，单节点串行 |
| 20 | 面试陷阱？| 说"Disk 超限会立即删除 Shard" → ❌ 是**迁走 Replica 到其他节点**；不会删除数据 |
| 21 | CCS 和 PIT 配合？| 跨集群查询 + Point In Time → 每个集群先 PIT → 协调节点 Merge；保证快照一致性 |
| 22 | 节点加入集群后 Allocator 做什么？| 感知新节点 → 把积压的 Unassigned Shard 分配过去；同时平衡现有节点的 Shard |
| 23 | Index 创建时怎么分配 Primary？| 按 IndexMetadata（number_of_shards）+ Allocator → 每个 Primary 选 Decider 通过的节点 |
| 24 | Replica 同步完成后？| Replica 变成 In-Sync Shard（ISR）；Cluster State 更新；故障转移从 ISR 中选 |
| 25 | Allocator 手动触发？| POST /_cluster/reroute；POST /_cluster/reroute?retry_failed（重试失败的 Shard）|
| 26 | 典型生产用法？| 6 节点 2 Rack 集群（Rack Awareness）+ Disk Watermark 自动保护 + CCS 跨 Region US-East / US-West 联合查询 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：server/.../cluster/allocation/Allocator.java + decider/DiskThresholdDecider.java + RackAwareAllocationDecider.java + action/search/ccs/  
**面试考点：26 题**  
**差异化点**：AllocationDecider 链模式决策、Disk Watermark 三级防护、Rack Awareness、**CCS vs Federation vs ClickHouse Distributed 三方案对比**
