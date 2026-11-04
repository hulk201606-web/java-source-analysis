# Apache Hadoop YARN ResourceManager 源码级深度解析

> **源码基线**：Hadoop 3.3.x（Java）  
> **核心仓库**：apache/hadoop → hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server-resourcemanager  
> **面试权重**：⭐⭐⭐⭐（大数据调度核心，高频必问）

---

## 一、YARN 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ YARN = Hadoop 3.x 统一资源管理 + 调度框架               │
│                                                               │
│  YARN 架构：                                                  │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ ResourceManager（RM） ★ 核心！                         │   │
│  │   ├── Scheduler（调度器）                              │   │
│  │   ├── ResourceTracker（NodeManager 管理）              │   │
│  │   ├── ApplicationManager（AM 管理）                    │   │
│  │   └── ContainerAllocationExpirer（过期 Container）     │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ 心跳 + Container 报告                               │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ NodeManager│ NodeManager│ NodeManager│  ...                │
│  │ 管理 Containers │ 管理 Containers │ 管理 Containers │       │
│  └──────────┴──────────┴──────────┘                         │
│
│ ★ YARN 核心职责：                                             │
│   ① 资源管理：统一管理集群 CPU/Memory                        │
│   ② 任务调度：分配 Container 给应用                          │
│   ③ 容错：NodeManager 挂了自动恢复 Container                 │
│
│ ★ YARN vs Kubernetes（面试高频对比）：                        │
│   ┌──────────┬──────────────┬──────────────┐                 │
│   │            │ YARN          │ K8s            │              │
│   ├──────────┼──────────────┼──────────────┤                 │
│   │ 抽象单元    │ Container    │ Pod            │              │
│   │ 资源粒度    │ vcore + MB   │ CPU + Memory    │              │
│   │ 调度器      │ Capacity/Fair│ Default/PDB    │              │
│   │ 容器运行时  │ Docker       │ containerd     │              │
│   │ 服务发现    │ 无（应用自管理）│ K8s Service    │              │
│   │ 网络模型    │ 端口映射      │ CNI            │              │
│   │ 适用场景    │ 大数据离线    │ 云原生全场景    │              │
│   └──────────┴──────────────┴──────────────┘                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、ResourceManager 核心组件

### 2.1 ResourceManager 内部结构

```java
// ★ YARN ResourceManager 内部组件（源码级）
//
// // hadoop-yarn-server-resourcemanager/.../ResourceManager.java
//
// public class ResourceManager {
//     // ★ Scheduler（核心调度器）
//     private ResourceScheduler scheduler;
//     // 可选：CapacityScheduler / FairScheduler / FifoScheduler
//
//     // ★ ResourceTracker（NM 管理）
//     private ResourceTracker resourceTracker;
//     // 管理所有 NodeManager 心跳 + 可用资源
//
//     // ★ ApplicationManager（AM 管理）
//     private ApplicationMasterService applicationMasterService;
//     // AM 注册 + AM 心跳 + AM 生命周期
//
//     // ★ ContainerAllocationExpirer
//     private ContainerAllocationExpirer containerAllocationExpirer;
//     // 过期的 Container（分配了但 AM 没领）
//
//     // ★ RM 启动流程
//     public void start() {
//         // ① 启动 RPC Server（NM/AM/App 通信）
//         registerService(new ResourceManagerRPCService());
//         registerService(new ApplicationMasterRPCService());
//
//         // ② 启动 Scheduler
//         scheduler = CapacityScheduler.newInstance(conf);
//         scheduler.start();
//
//         // ③ 启动 ResourceTracker
//         resourceTracker = new ResourceTracker();
//         resourceTracker.start();
//
//         // ④ 启动 Web UI
//         startWebApp();
//     }
// }
//
```

### 2.2 CapacityScheduler 核心源码

```java
// ★ YARN CapacityScheduler（企业最常用调度器，源码级）
//
// // capacityscheduler/CapacityScheduler.java
//
// public class CapacityScheduler implements ResourceScheduler {
//
//     // ★ Queue 树（层级队列，面试高频！）
//     private final CSQueue root;  // 根队列
//     // 结构：root → default + biz + dev
//     //       default（70%） + biz（20%） + dev（10%）
//
//     // ★ 调度核心循环
//     @Override
//     public List<Container> assignContainers(NodeId nodeId) {
//         CSNode csNode = nodes.get(nodeId);
//         Resource available = csNode.getAvailableResource();
//         List<Container> allocated = new ArrayList<>();
//
//         // ★ 从根队列开始，向下递归分配
//         root.assignContainers(csNode, available, allocated);
//         return allocated;
//     }
// }
//
// // ★ CSQueue 递归分配（源码级）
// public abstract class CSQueue {
//     public void assignContainers(CSNode node, Resource available, List<Container> allocated) {
//         if (this instanceof CSLeafQueue) {
//             // ★ 叶子队列：分配 Container 给里面的应用
//             CSLeafQueue leaf = (CSLeafQueue) this;
//             for (CSApp app : leaf.apps) {
//                 if (app.hasPendingRequest()) {
//                     // 给应用分配一个 Container
//                     allocated.add(app.allocateContainer(node, available));
//                 }
//             }
//         } else {
//             // ★ 父队列：按容量比例分配给子队列
//             CSParentQueue parent = (CSParentQueue) this;
//             for (CSQueue child : parent.children) {
//                 child.assignContainers(node, available, allocated);
//                 if (ResourceUtils.isNone(available)) break;
//             }
//         }
//     }
// }
//
```

---

## 三、Container 分配完整流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Container 分配完整链路（源码级）                          │
│                                                               │
│  ① Client → RM：submit Application（提交 MapReduce/Spark）│
│     → RM 创建 Application → 返回 ApplicationID              │
│
│  ② RM → RM Scheduler：分配 AM Container（每个应用一个 AM）   │
│     → AM Container 启动 NodeManager 上的 AM 进程            │
│
│  ③ AM → RM：注册 + 心跳 + 申请业务 Containers              │
│     AM 启动后 → 向 RM 注册 + 汇报 AM 状态                    │
│     AM 调用 allocate(containerRequest) → 申请业务 Containers │
│
│  ④ RM Scheduler 调度业务 Containers                         │
│     a. ResourceTracker：知道每个 NM 可用资源                │
│     b. Scheduler.assignContainers()：给 AM 分配 Container   │
│     c. 返回 Container（包含 ContainerID + NodeId + Resource）│
│
│  ⑤ AM → NodeManager：领取 Container + 执行任务              │
│     AM 拿到 Container → 去对应 NM 领取                       │
│     NM 启动 Container（Map Task / Spark Executor）          │
│
│  ⑥ NM → RM：Container 完成报告                              │
│     Container 跑完 → NM 报告完成 → RM 更新应用状态          │
│     AM 任务全部完成 → 应用完成                               │
│
│ ★ 完整时序（面试高频！）：                                    │
│  Client → RM → 分配 AM → NM 启 AM → AM 注册 → AM 申请      │
│  Containers → RM 调度 → 分配给 AM → AM 去 NM 领 → 执行 →   │
│  完成 → 应用结束                                             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、CapacityScheduler vs FairScheduler

```
┌─────────────────────────────────────────────────────────────┐
│  ★ YARN 三种调度器对比（面试高频！）                         │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 调度器     │ CapacityScheduler │ FairScheduler │ FifoScheduler │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 核心思想   │ **层级容量保证**    │ **公平共享**  │ **先来先服务** │   │
│  │ 队列模型   │ 树状层级队列        │ 层级队列      │ 无队列       │   │
│  │ 典型应用   │ 企业（部门隔离）     │ 团队共享      │ 简单集群      │   │
│  │ 资源隔离   │ 严格（cap/queue）   │ 灵活（weight）│ 无隔离       │   │
│  │ 生产推荐   │ ★★★★★           │ ★★★           │ ★           │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ CapacityScheduler 核心参数（面试高频）：                    │
│  yarn.scheduler.capacity.root.default.capacity = 70          │
│  → default 队列占 70% 容量（软限制，空闲时可以超）          │
│  yarn.scheduler.capacity.root.default.maximum-capacity = 90  │
│  → default 队列最多能占 90%（硬限制，防一个队列占满集群）    │
│
│ ★ 软限制 vs 硬限制（面试高频）：                              │
│  capacity = 70% → 软限制（空闲时可以超）                     │
│  maximum-capacity = 90% → 硬限制（绝不超过）                │
│  → 空闲时队列可以用别人不用的资源                            │
│  → 别人需要时归还（资源抢占）                                │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | YARN 核心组件？| **ResourceManager + NodeManager + ApplicationMaster + Container** |
| 2 | ResourceManager 核心职责？| ①统一资源管理 ②任务调度 ③容错恢复 |
| 3 | RM 三大子组件？| **Scheduler（调度器）+ ResourceTracker（NM 管理）+ ApplicationManager（AM 管理）** |
| 4 | 三种调度器？| **CapacityScheduler（层级容量，企业首选）+ FairScheduler（公平）+ FifoScheduler（简单）** |
| 5 | CapacityScheduler 队列模型？| **树状层级队列**（root → biz/dev/default）+ capacity/maximum-capacity |
| 6 | Capacity 软/硬限制？| capacity = 70%（软，空闲可超）；maximum-capacity = 90%（硬，绝不超）|
| 7 | Container 分配完整链路？| Client submit → RM 分配 AM → AM 注册 → AM 申请 Containers → RM 调度 → AM 领 → NM 执行 |
| 8 | YARN vs K8s？| YARN：大数据离线（Container + Queue）；K8s：云原生全场景（Pod + CNI + Service）|
| 9 | AM 职责？| 每个应用一个 AM；管理业务 Containers + 汇报 RM |
| 10 | NM 职责？| 管理本机 Containers + 心跳 RM + 执行 Container |
| 11 | YARN HA？| RM Active + Standby + ZK 选主；ZKFC 监控；Fencing 防脑裂（和 HDFS HA 同设计）|
| 12 | 面试最高频考点排序？| **1. 三种调度器对比 2. CapacityScheduler 队列模型 3. Container 分配链路 4. YARN vs K8s** |
| 13 | 学习路径？| ResourceManager → CapacityScheduler → CSQueue 递归 → ContainerAllocationExpirer |
| 14 | YARN vs 分布式调度？| YARN 是应用级调度（MapReduce/Spark 调度器）；TiDB PD 是 Region 级调度（更细粒度）|
| 15 | 面试常见追问？| CapacityScheduler 如何防止一个队列占满集群？→ maximum-capacity 硬限制 |
| 16 | YARN 生产推荐？| **CapacityScheduler**（企业部门隔离场景最成熟）|
| 17 | FairScheduler 特点？| 公平共享 + weight 权重；团队共享集群用 |
| 18 | YARN 资源单位？| vcore（CPU）+ MB（内存）；调度时按 Container 规格分配 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server-resourcemanager  
**面试考点：18 题**  
**差异化点**：大数据调度核心 + CapacityScheduler 递归源码 + Container 全链路
