# Kubernetes 源码深度解析（容器编排）

> **源码基线**：Kubernetes 1.31.x（最新稳定）  
> **核心仓库**：https://github.com/kubernetes/kubernetes  
> **面试权重**：⭐⭐⭐⭐⭐（云原生最高频）

---

## 一、Kubernetes 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：容器编排平台（Docker 等容器的自动化部署/扩展/运维）│
│  解决什么问题？微服务架构下 N 个服务的容器管理                  │
│                                                               │
│  两大平面：                                                    │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Control Plane（控制平面）                             │   │
│  │  ① kube-apiserver ★ 唯一入口（所有组件只和它通信）     │   │
│  │  ② etcd（元数据存储，强一致）                           │   │
│  │  ③ kube-scheduler（调度：选 Node 放 Pod）              │   │
│  │  ④ kube-controller-manager（控制器循环）                │   │
│  │  ⑤ kube-cloud-controller-manager（云平台集成）          │   │
│  └──────────────────────────────────────────────────────┘   │
│                          ↓                                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Data Plane（数据平面）                               │   │
│  │  Worker Node：                                         │   │
│  │  ① kubelet（容器生命周期管理）                          │   │
│  │  ② kube-proxy（Service 网络转发）                      │   │
│  │  ③ Container Runtime（Docker/containerd/CRI-O）        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Pod 生命周期 + Controller 模式（★ 面试核心）                 │
│                                                               │
│  ① 用户 kubectl apply -f deployment.yaml → API Server       │
│  ② API Server 存到 etcd                                      │
│  ③ Deployment Controller 监听到 Deployment 创建               │
│     → 创建 ReplicaSet（副本集）                               │
│     → ReplicaSet Controller → 创建 N 个 Pod                  │
│  ④ kube-scheduler 发现未调度的 Pod → 选合适的 Node            │
│     → 写 Pod.spec.nodeName                                   │
│  ⑤ kubelet 发现分配给自己的 Pod → 拉镜像 + 启动容器           │
│  ⑥ Pod Ready → Service Controller 更新 Endpoints             │
│                                                               │
│ ★ Controller 模式（Reconciliation Loop）：                    │
│   for {                                                       │
│     desired = 获取期望状态（etcd 里的配置）                    │
│     actual = 获取实际状态（从 API Server/Watcher）              │
│     if desired != actual {                                    │
│       执行操作让 actual → desired                              │
│     }                                                         │
│   }                                                           │
│   → 所有 Controller 都是这个模式！（Deployment/Service/Node...）│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 kube-apiserver（★★★★★ 唯一入口）

```go
// ★ kube-apiserver 是整个 K8s 的唯一入口！
//   所有组件（scheduler/controllers/kubelet/kubectl）都只和它通信
//   内部不直接操作 etcd → 通过 API Server（保证权限校验 + 版本控制）
//
// 请求 pipeline（面试高频）：
//   Client Request
//     ↓
//   ① Authentication（认证）→ Bearer Token / x509 Client Cert
//     ↓
//   ② Authorization（授权）→ RBAC（Role-Based Access Control）
//     ↓
//   ③ Admission Control（准入控制）→ MutatingWebhook / ValidatingWebhook
//     ↓
//   ④ Schema Validation（校验 YAML/JSON 合法性）
//     ↓
//   ⑤ Storage Layer（存储层）→ etcd 写
//     ↓
//   返回结果
//
// ★ Watch 机制（K8s 核心！类似 ZK Watch）：
//   Client → API Server.Watch("/api/v1/pods")
//   → kube-apiserver 从 etcd 拉 List + 启动 Watch
//   → etcd 有变更 → API Server 收到事件 → 推给 Client
//   → 事件类型：ADDED / MODIFIED / DELETED
//   → Controller 用 Watch 监听资源变化 → 触发 Reconciliation Loop
```

### 2.2 kube-scheduler（调度核心）

```go
// ★ kube-scheduler 负责给未调度的 Pod 选 Node
//
// 调度流程（面试高频）：
//   ① 触发：kube-apiserver Watch 发现 Pod.spec.nodeName 为空
//
//   ② Filter Phase（过滤）：
//      遍历所有 Node → 用 Predicates（谓词）过滤不合适的 Node
//      内置 Predicates：
//        - NodeUnschedulable（节点是否可调度）
//        - PodFitsResources（资源是否够 CPU/Memory）
//        - PodMatchedNodeAffinities（Node Affinity）
//        - PodToleratesNodeTaints（污点容忍）
//        - MatchNodeSelector（节点选择器）
//        - NoDiskConflict（磁盘冲突）
//      → 过滤后留下一组可用 Node
//
//   ③ Score Phase（打分）：
//      对可用 Node 打分（0-100）
//      内置 Priorities（优先级）：
//        - LeastRequestedPriority（资源最少剩余优先）
//        - BalancedResourceAllocation（资源均衡）
//        - NodeAffinity（亲和性打分）
//        - ImageLocalityPriority（本地有镜像优先）
//      → 分数高的 Node 优先
//
//   ④ Bind Phase（绑定）：
//      选分数最高的 Node → 写回 Pod.spec.nodeName
//      → API Server 存 etcd
//      → kubelet 发现 Pod 分配给自己 → 启动
//
// ★ 自定义调度器扩展：
//   ① 写自定义 Predicate / Priority
//   ② 实现 Scheduler Framework（1.15+，更灵活的扩展点）
//      PreFilter / Filter / PostFilter / PreScore / Score / Reserve / Permit / PreBind / Bind / PostBind
```

### 2.3 Controller Manager（控制器循环）

```go
// ★ Controller Manager 包含 N 个 Controller，每个都是 Reconciliation Loop
//
// 核心模式：
//   for each resource {
//     desired := 期望状态（从 etcd / spec 拿）
//     actual := 实际状态（从 API Server / status 拿）
//     if desired != actual {
//       reconcile(desired, actual)
//     }
//   }
//
// ★ 核心 Controller：
//
// ① Deployment Controller
//   Deployment.spec.replicas = 3
//   → 创建 ReplicaSet（replicas=3）
//   → ReplicaSet Controller 创建 3 个 Pod
//   → 如果 Deployment 更新镜像 → 创建新 ReplicaSet → 滚动更新
//
// ② ReplicaSet Controller
//   保证实际 Pod 数 = spec.replicas
//   → 多了 → 删除；少了 → 创建
//
// ③ Service Controller
//   Service.spec.selector = app:order
//   → 找到所有 label app=order 的 Pod
//   → 更新 Endpoints（Pod IP + Port）
//   → kube-proxy 根据 Endpoints 做流量转发
//
// ④ Node Controller
//   监控 Node 健康状态
//   → Node NotReady 超过 40s → 标记 Node Unschedulable
//   → Node 恢复 Ready → 标记可调度
//   → 如果 Node 永久故障 → 驱逐 Pod
//
// ★ Controller 间关系：
//   Deployment → ReplicaSet → Pod
//   Service → Endpoints → kube-proxy 流量转发
//
// ★ Informer（Watcher 封装）：
//   Controller 不直接调 API Server Watch
//   → 用 Informer 封装 Reflector（Watch）+ Indexer（索引缓存）
//   → 本地缓存 → 减少 API Server 请求
//   → DeltaQueue：存 Watch 事件（ADDED/MODIFIED/DELETED）
```

### 2.4 kubelet + Pod 生命周期

```go
// ★ kubelet 运行在每个 Worker Node 上
//   负责容器生命周期管理（CRI 接口调用 Docker/containerd）
//
// kubelet 核心循环：
//   for {
//     ① 从 API Server 同步分配给自己的 Pod 列表
//     ② 对比本地运行的容器 vs 期望 Pod 列表
//     ③ 缺失 → 创建；多余 → 停止；变化 → 更新
//     ④ Pod 状态回写 API Server（status）
//   }
//
// ★ Pod 状态机（面试高频）：
//   Pending → Running → Succeeded / Failed
//   子状态：
//   - PodScheduled（Scheduler 已选 Node）
//   - ContainersReady（所有容器 Ready）
//   - Initialized（Init Container 完成）
//   - Ready（可接收流量）
//
// ★ Init Container vs Sidecar Container：
//   Init Container：在主容器启动前必须完成 → 做初始化任务（拉数据/配置文件）
//   Sidecar Container：和主容器并行运行 → 辅助功能（日志收集/代理/监控）
//
// ★ Liveness / Readiness / Startup Probe：
//   Liveness（存活探针）：失败 3 次 → 重启容器（防止容器死锁）
//   Readiness（就绪探针）：失败 → Service 把这个 Pod 从 Endpoints 摘掉（不接流量）
//   Startup（启动探针）：容器启动慢 → 给足够时间启动后再用 Liveness 检查
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | K8s Control Plane 组件？| **kube-apiserver（★ 唯一入口）/ etcd（元数据存储）/ kube-scheduler（选 Node）/ kube-controller-manager（控制器循环）**|
| 2 | kube-apiserver 为什么是唯一入口？| **所有组件只和它通信** → 保证统一认证/授权/版本控制；内部不直接碰 etcd → 简化 Controller 实现 |
| 3 | Controller Reconciliation 模式？| **for 循环：对比 desired（期望状态）vs actual（实际状态）→ 不等则 reconcile**；所有 Controller（Deployment/Service/Node）都是这个模式 |
| 4 | Scheduler 调度流程？| **① Filter（Predicate 过滤不合适的 Node）→ ② Score（Priority 打分）→ ③ Bind（写回 Pod.spec.nodeName）**|
| 5 | 核心 Predicates？| **NodeUnschedulable / PodFitsResources / PodMatchedNodeAffinities / PodToleratesNodeTaints / MatchNodeSelector** |
| 6 | Deployment → ReplicaSet → Pod？| Deployment Controller 创建 ReplicaSet；ReplicaSet Controller 创建 N 个 Pod；滚动更新：新 ReplicaSet 逐步替代旧的 |
| 7 | Service → Endpoints？| Service Controller 根据 selector 匹配 Pod → 更新 Endpoints（Pod IP + Port）→ kube-proxy 根据 Endpoints 做流量转发 |
| 8 | Informer 作用？| **封装 API Server Watch** → Reflector（Watch 事件）+ DeltaQueue（存事件）+ Indexer（本地索引缓存）→ 减少 API Server 请求 |
| 9 | Node 故障怎么处理？| Node Controller → Node NotReady 超过 40s → 标记 Unschedulable → 驱逐 Pod → Pod 被重新调度到其他 Node |
| 10 | Liveness vs Readiness Probe？| **Liveness 失败 → 重启容器**（防死锁）；**Readiness 失败 → Service 摘掉 Pod（不接流量）但不重启** |
| 11 | Init Container vs Sidecar？| Init Container：**主容器前必须完成**（初始化任务）；Sidecar：**并行运行**（日志收集/代理）|
| 12 | K8s 和 Docker 关系？| Docker 是容器运行时；K8s 是容器编排（通过 CRI 接口调用 Docker/containerd/CRI-O）；K8s 1.24 开始默认 containerd（弃用 Docker shim）|
| 13 | RBAC 授权模型？| **Role（namespace 内）/ ClusterRole（全局）+ RoleBinding / ClusterRoleBinding** → 给用户/ServiceAccount 授予资源的 CRUD 权限 |
| 14 | Admission Webhook？| **准入控制**：MutatingWebhook（改资源，如自动注入 sidecar）+ ValidatingWebhook（校验）；在 API Server 存 etcd 之前触发 |
| 15 | Ingress vs Service？| Service：集群内部 DNS 解析 + kube-proxy 负载均衡；Ingress：入口层（HTTP/HTTPS 路由 + TLS 终止）→ Controller 实现 Nginx/Traefik |
| 16 | Watch vs Poll？| K8s 用 **Watch（长轮询 + 事件推送）** → Controller 通过 Informer 订阅变化 → 比 Poll 高效 |
| 17 | StatefulSet vs Deployment？| StatefulSet：**有稳定网络标识 + 稳定持久化存储 + 有序部署/删除**（DB/中间件）；Deployment：无状态服务 |
| 18 | K8s 学习路径？| Pod → Service → Deployment → Scheduler → Controller → Custom Resource（CRD + Operator）→ Service Mesh（Istio）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Kubernetes 1.31.x  
**面试考点：18 题**
