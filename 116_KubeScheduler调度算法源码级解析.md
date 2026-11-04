# Kubernetes kube-scheduler 调度算法源码级解析

> **源码基线**：Kubernetes 1.31.x（Go）  
> **核心仓库**：kubernetes/kubernetes → pkg/scheduler/  
> **面试权重**：⭐⭐⭐⭐（K8s 核心，云原生必问）

---

## 一、kube-scheduler 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ kube-scheduler = K8s 默认调度器                          │
│                                                               │
│  Pod 调度链路：                                                │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ 用户 apply pod.yaml → API Server                      │   │
│  │   Pod Spec: nodeSelector / nodeAffinity / tolerations │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ Pending Pod 进入调度队列                            │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ kube-scheduler ★ 本文重点                             │   │
│  │   ① Filter（过滤）：哪些 Node 能放这个 Pod             │   │
│  │   ② Score（打分）：能放的 Node 按优先级排序            │   │
│  │   ③ Bind（绑定）：选择最优 Node → 绑定到 Pod          │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ Bind 更新 Pod.NodeName                            │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ kubelet 看到 Pod 分配给自己 → 启动容器                │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ scheduler 核心代码路径（源码级）：                          │
│   pkg/scheduler/                                             │
│   ├── scheduler.go          ★ Scheduler 主循环              │
│   ├── framework.go          ★ Framework 接口                │
│   ├── core/                                                  │
│   │   ├── scheduling_queue.go    调度队列                    │
│   │   ├── scheduling_cycle.go    Filter + Score 主循环      │
│   │   └── bind.go                Bind 核心                   │
│   ├── framework/plugins/                                    │
│   │   ├── node_name.go           NodeName Filter（指定 Node）│
│   │   ├── node_affinity.go       NodeAffinity Filter/Score  │
│   │   ├── node_ports.go          NodePorts Filter           │
│   │   ├── node_resources.go      NodeResources Filter/Score │
│   │   ├── node_unschedulable.go  NodeUnschedulable Filter   │
│   │   ├── pod_node_affinity.go   PodNodeAffinity Filter/Score│
│   │   ├── taint_toleration.go    TaintToleration Filter/Score│
│   │   ├── node_balanced_resource.go  BalancedResourceAllocation│
│   │   └── ...                                                 │
│   └── plugins/                                                │
│       ├── defaults.go             默认插件集                 │
│       └── legacy.go               V1 调度器兼容              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Filter → Score → Bind 源码级

### 2.1 Filter 过滤插件链

```go
// ★ kube-scheduler Filter 阶段源码级
//
// // pkg/scheduler/core/scheduling_cycle.go
//
// func (s *schedulingCycle) ScheduleOne(pod *v1.Pod) (string, error) {
//     // ★ Phase 1: Filter（过滤能放的 Node）
//     feasibleNodes := make([]*v1.Node, 0, len(s.nodeLister))
//
//     for _, node := range s.nodeLister.List() {
//         fit, reasons := s.filterPlugins.Filters(pod, node)
//         if fit {
//             feasibleNodes = append(feasibleNodes, node)
//         } else {
//             // Node 不适合：记录原因（资源不够 / 污点不匹配 / 端口冲突...）
//             addNodeToFailedList(node, reasons)
//         }
//     }
//
//     if len(feasibleNodes) == 0 {
//         return "", fmt.Errorf("no feasible nodes")
//     }
//
//     // ★ Phase 2: Score（打分排序）
//     // ★ Phase 3: Bind（选择最优 Node 并绑定）
//     // ...
// }
//
// // ★ Filter 插件实现（源码级：NodeUnschedulable）
// func (pl *NodeUnschedulable) Filter(pod *v1.Pod, node *v1.Node) bool {
//     // Node.Spec.Unschedulable = true → 这个 Node 不接受任何 Pod
//     return !node.Spec.Unschedulable
// }
//
// // ★ Filter 插件实现（源码级：NodeResources）
// func (pl *NodeResources) Filter(pod *v1.Pod, node *v1.Node) bool {
//     nodeInfo := pl.nodeLister.GetNodeInfo(node)
//     podRequest := calculatePodRequest(pod) // CPU + Memory 需求
//     available := nodeInfo.Allocatable().Sub(nodeInfo.Requested())
//     return available.GreaterThanOrEqual(podRequest)
// }
//
// // ★ Filter 插件实现（源码级：TaintToleration）
// func (pl *TaintToleration) Filter(pod *v1.Pod, node *v1.Node) bool {
//     // Node 有污点 → Pod 必须有对应的 Toleration
//     for _, taint := range node.Spec.Taints {
//         if !podHasToleration(pod, taint) {
//             return false;  // Pod 不能忍受这个污点 → 过滤掉
//         }
//     }
//     return true;
// }
//
```

### 2.2 Score 打分插件链

```go
// ★ kube-scheduler Score 阶段源码级
//
// // pkg/scheduler/core/scheduling_cycle.go
//
// func (s *schedulingCycle) ScheduleOne(pod *v1.Pod) (string, error) {
//     // Phase 1: Filter → feasibleNodes
//
//     // ★ Phase 2: Score（对 feasibleNodes 打分）
//     for _, node := range feasibleNodes {
//         scores[node.Name] = 0
//     }
//
//     for _, scorePlugin := range s.scorePlugins {
//         pluginScores := scorePlugin.Score(pod, feasibleNodes)
//         for nodeName, score := range pluginScores {
//             scores[nodeName] += score * scorePlugin.Weight
//         }
//     }
//
//     // ★ 加权汇总（每个 Score 插件有 weight）
//     // NodeResourcesBalancedAllocation.Weight = 1
//     // NodeImageLocalityScore.Weight = 1
//     // ...
//
//     // ★ 按 score 从高到低排序
//     sort.Slice(feasibleNodes, func(i, j int) bool {
//         return scores[feasibleNodes[i].Name] > scores[feasibleNodes[j].Name]
//     })
//
//     // ★ Phase 3: Bind
//     bestNode := feasibleNodes[0]  // 最高分
//     return s.bind(pod, bestNode)
// }
//
// // ★ NodeResourcesBalancedAllocation Score 插件（默认最重要）
// func (pl *NodeResourcesBalancedAllocation) Score(pod *v1.Pod, node *v1.Node) int64 {
//     nodeInfo := pl.nodeLister.GetNodeInfo(node)
//     // 计算 CPU 和 Memory 的资源均衡程度
//     // 每个 Node CPU/Memory 剩余比例越接近 → Score 越高
//     cpuRatio := float64(nodeInfo.Allocatable().Cpu().MilliValue() -
//                         nodeInfo.Requested().Cpu().MilliValue()) /
//                 float64(nodeInfo.Allocatable().Cpu().MilliValue())
//     memRatio := float64(nodeInfo.Allocatable().Memory().Value() -
//                         nodeInfo.Requested().Memory().Value()) /
//                 float64(nodeInfo.Allocatable().Memory().Value())
//     return int64((1 - math.Abs(cpuRatio - memRatio)) * 100)
// }
//
```

### 2.3 Bind 核心

```go
// ★ kube-scheduler Bind 核心（源码级）
//
// // pkg/scheduler/core/bind.go
//
// func (s *scheduler) bind(pod *v1.Pod, node *v1.Node) error {
//     // ★ 方案 1: SubResource（推荐）
//     // K8s 1.21+ 引入 NodeBinding 子资源 → 事务性绑定
//     binding := &v1.Binding{
//         ObjectMeta: metav1.ObjectMeta{Name: pod.Name},
//         Target: v1.ObjectReference{
//             Kind: "Node",
//             Name: node.Name,
//         },
//     }
//     return s.client.Bind(binding)  // PUT /api/v1/namespaces/{ns}/pods/{name}/binding
//     // API Server 更新 Pod.Spec.NodeName
//     // kubelet watch Pod 变更 → 启动容器
// }
//
// // ★ 多调度器并发绑定怎么办？
// // 如果两个调度器同时给同一个 Pod 绑定不同 Node？
// // → 只有一个会成功（API Server 乐观锁）
// // → 另一个收到 Conflict 错误 → 重试
//
```

---

## 三、Taint/Toleration + NodeAffinity

### 3.1 Taint/Toleration（污点/容忍）源码级

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Taint/Toleration（面试官必问！）                          │
│                                                               │
│  Node 打污点：                                                 │
│  $ kubectl taint nodes node1 dedicated=special-user:NoSchedule│
│  key=dedicated, value=special-user, effect=NoSchedule        │
│
│  Pod 加容忍：                                                  │
│  spec.tolerations:                                            │
│  - key: "dedicated"                                           │
│    operator: "Equal"                                          │
│    value: "special-user"                                      │
│    effect: "NoSchedule"                                       │
│
│  ★ Effect 三种（面试高频）：                                   │
│   NoSchedule：Pod 不能被调度到这个 Node                        │
│   PreferNoSchedule：尽量不要调度（调度器尽量避开）             │
│   NoExecute：**已经在这个 Node 上的 Pod 会被驱逐**（立即生效）│
│
│  ★ 典型场景：                                                  │
│   GPU 节点：taint=gpu=true:NoSchedule → 只有 GPU Pod 能调度  │
│   专用节点：taint=dedicated=app1:NoExecute → app1 独占       │
│   隔离节点：taint=restricted=true:NoSchedule → 只有指定 Pod  │
│
```

### 3.2 NodeAffinity（节点亲和）源码级

```yaml
# ★ NodeAffinity 例子
apiVersion: v1
kind: Pod
spec:
  affinity:
    nodeAffinity:
      # ★ required（硬要求，必须满足 → Filter）
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: zone
            operator: In
            values: ["east", "west"]
          - key: gpu
            operator: Exists
      # ★ preferred（软偏好，优先调度 → Score）
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        preference:
          matchExpressions:
          - key: ssd
            operator: In
            values: ["true"]
```

---

## 四、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | kube-scheduler 核心职责？| **Filter（过滤）→ Score（打分）→ Bind（绑定）** 三阶段调度 |
| 2 | Filter 阶段做什么？| 遍历所有 Node → 检查每个 Filter 插件 → 不满足过滤掉 |
| 3 | Score 阶段做什么？| 对 Filter 后的 Node 打分 → 加权汇总 → 按分数排序 |
| 4 | Bind 阶段做什么？| 选最高分 Node → PUT Binding API → API Server 更新 Pod.NodeName |
| 5 | Filter vs Score 插件？| Filter：能/不能（硬约束）；Score：优先级（软偏好）|
| 6 | NodeUnschedulable Filter？| Node.Spec.Unschedulable=true → 不接受任何 Pod |
| 7 | NodeResources Filter？| 检查 Node 剩余 CPU/Memory ≥ Pod 需求 |
| 8 | Taint/Toleration 三种 Effect？| **NoSchedule（不能调度）/ PreferNoSchedule（尽量避开）/ NoExecute（驱逐已存在 Pod）**|
| 9 | NodeAffinity required vs preferred？| required（硬要求 → Filter）；preferred（软偏好 → Score）|
| 10 | PodAffinity vs NodeAffinity？| PodAffinity：Pod 和 Pod 亲和（一起调度）；NodeAffinity：Pod 和 Node 亲和 |
| 11 | 默认 Score 插件？| **NodeResourcesBalancedAllocation**（CPU/Memory 均衡）+ ImageLocality + NodeAffinityScore |
| 12 | kube-scheduler 源码路径？| **pkg/scheduler/**（核心）+ **pkg/scheduler/framework/plugins/**（插件）|
| 13 | 面试最高频考点排序？| **1. Filter→Score→Bind 三阶段 2. Taint/Toleration 3. NodeAffinity 4. 常见 Filter/Score 插件** |
| 14 | 学习路径？| scheduler.go 主循环 → NodeUnschedulable Filter → BalancedAllocation Score → Bind 核心 |
| 15 | 多调度器并发？| 乐观锁（ResourceVersion）；一个成功，另一个 Conflict → 重试 |
| 16 | Scheduler vs PD vs YARN？| Scheduler：Pod 级；PD：Region 级；YARN：Container 级；三种不同粒度调度 |
| 17 | 面试常见追问？| Pod 调度失败怎么办？→ 检查 FailedScheduling 事件；看哪些 Filter 失败了 |
| 18 | 2024 新特性？| **scheduling.gating**（调度门控）+ **Pod 动态调度**（WorkloadGroup）|
| 19 | Scheduler 扩展？| 写自定义 Plugin → 实现 Filter/Score 接口 → 注册到 Scheduler |
| 20 | 面试最高频场景？| GPU 调度（Taint/Toleration）+ 跨 Zone 高可用（NodeAffinity）+ 资源均衡（BalancedAllocation）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：pkg/scheduler/ + pkg/scheduler/framework/plugins/  
**面试考点：20 题**  
**差异化点**：Filter→Score→Bind 三阶段源码级 + Taint/Toleration + NodeAffinity
