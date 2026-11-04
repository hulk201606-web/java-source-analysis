# 238 · Kubernetes 与云原生高频面试题库（40 题 · 详解）

> **方向**：K8s 架构、工作负载、网络、调度、资源、存储、发布与运维  
> **面试权重**：⭐⭐⭐⭐⭐（云原生与运维必问）  
> **配套源码文档**：[053_Kubernetes源码解析.md](053_Kubernetes源码解析.md)、[062_Helm源码解析.md](062_Helm源码解析.md)、[065_ArgoCD源码解析.md](065_ArgoCD源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问；以 K8s 1.25+ 为主线。

---

## 一、架构与工作负载（Q1–Q10）

### Q1. Kubernetes 的整体架构是什么？

**结论**：控制面（API Server、etcd、Scheduler、Controller Manager） + 工作节点（kubelet、kube-proxy、容器运行时）。

**机制**：
- API Server 统一入口与认证授权。
- etcd 存集群状态。
- Scheduler 调度 Pod 到节点。
- Controller Manager 运行控制器循环。
- kubelet 管理 Pod 与容器。

**追问**：kube-proxy 负责什么？Service 的流量转发与负载均衡规则。

### Q2. Pod 是什么？为什么不是直接管理容器？

**结论**：Pod 是最小调度单元，可包含一个或多个共享网络/存储的容器。

**机制**：
- 同一 Pod 内容器共享 Network Namespace。
- 可共享 Volume。
- 调度、生命周期以 Pod 为单位。
- 容器作为 Pod 内的运行单元。

**追问**：一个 Pod 多容器常见吗？常见于 Sidecar、Init Container、日志采集。

### Q3. Deployment、ReplicaSet 与 Pod 的关系是什么？

**结论**：Deployment 管理 ReplicaSet，ReplicaSet 管理 Pod 副本，提供滚动更新与回滚。

**机制**：
- Deployment 声明期望副本数与模板。
- 创建/更新 ReplicaSet。
- ReplicaSet 确保 Pod 数量。
- 版本历史支持回滚。

**追问**：为什么不直接用 Pod？裸 Pod 无副本与自愈能力。

### Q4. StatefulSet 适合什么场景？

**结论**：需要稳定网络标识、稳定存储与有序部署的有状态服务，如数据库、消息队列。

**机制**：
- Pod 名称有序（-0、-1）。
- 使用 Headless Service + 稳定 DNS。
- PVC 模板为每个副本绑定存储。
- 支持有序扩缩容。

**追问**：StatefulSet 能保证数据一致吗？不能，需应用层复制与选主。

### Q5. DaemonSet、Job 与 CronJob 有什么区别？

**结论**：DaemonSet 每个节点运行一个 Pod；Job 一次性任务；CronJob 定时任务。

**机制**：
- DaemonSet 适合日志/监控/网络代理。
- Job 保证完成次数。
- CronJob 按 Cron 表达式调度。
- 都需处理失败与重试。

**追问**：DaemonSet 更新会怎样？滚动更新，逐个节点替换。

### Q6. 容器镜像与容器运行时是什么关系？

**结论**：镜像是只读模板，容器是镜像的运行实例；运行时（containerd/CRI-O）负责拉取与运行。

**机制**：
- 镜像分层，复用层。
- 容器层可写。
- CRI 接口对接运行时。
- kubelet 通过 CRI 管理。

**追问**：镜像越小越好吗？启动快、攻击面小，但需包含依赖。

### Q7. Namespace 的作用是什么？

**结论**：逻辑隔离资源与权限，如多环境/多团队，但不提供网络与内核级强隔离。

**机制**：
- 资源按 Namespace 分组。
- RBAC 可按 Namespace 授权。
- 资源配额可限制。
- 网络默认互通。

**追问**：Namespace 能隔离网络吗？不能，需 NetworkPolicy 或网络方案。

### Q8. Label 与 Selector 有什么用？

**结论**：Label 是键值标签，Selector 按标签筛选资源，是 K8s 解耦与编排的核心。

**机制**：
- Service 用 Selector 找 Pod。
- Deployment 用 Selector 管理 Pod。
- 支持等式与集合选择。
- 标签可动态变更。

**追问**：Label 与 Annotation 区别？Label 用于选择，Annotation 存非标识元数据。

### Q9. K8s 的三种探针是什么？

**结论**：liveness（存活）、readiness（就绪）、startup（启动），分别控制重启、流量接入与启动宽限。

**机制**：
- liveness 失败重启容器。
- readiness 失败从 Service 端点摘除。
- startup 成功后才启用其他探针。
- 支持 HTTP/TCP/Exec。

**追问**：探针配置不当会怎样？误重启或流量打到未就绪实例。

### Q10. Pod 的重启策略有哪些？

**结论**：Always、OnFailure、Never，控制容器退出后的重启行为。

**机制**：
- Always 总是重启。
- OnFailure 非 0 退出才重启。
- Never 不重启。
- 受重启退避限制。

**追问**：Pod 一直 CrashLoopBackOff 怎么查？看事件、日志、探针与资源限制。

---

## 二、网络与服务（Q11–Q18）

### Q11. Service 的类型有哪些？

**结论**：ClusterIP、NodePort、LoadBalancer、ExternalName，按访问方式选择。

**机制**：
- ClusterIP 集群内虚拟 IP。
- NodePort 在节点暴露端口。
- LoadBalancer 云厂商负载均衡。
- ExternalName 映射外部 DNS。

**追问**：Service 如何实现负载均衡？kube-proxy 维护 iptables/IPVS 规则。

### Q12. ClusterIP、NodePort 与 LoadBalancer 有什么区别？

**结论**：ClusterIP 仅集群内，NodePort 通过节点端口暴露，LoadBalancer 由云厂商提供外部入口。

**机制**：
- ClusterIP 稳定虚拟地址。
- NodePort 占用宿主端口。
- LoadBalancer 创建外部 LB。
- 生产常 Ingress + ClusterIP。

**追问**：NodePort 有什么缺点？端口管理复杂、安全暴露面大。

### Q13. Ingress 的作用是什么？

**结论**：七层入口，按 Host/Path 路由到不同 Service，提供 TLS 与统一入口。

**机制**：
- Ingress 资源声明规则。
- Ingress Controller 实现转发。
- 支持 TLS 终止。
- 减少 NodePort/LB 数量。

**追问**：Ingress 与 Service 区别？Service 是四层抽象，Ingress 是七层路由。

### Q14. CNI 是什么？

**结论**：容器网络接口规范，负责给 Pod 分配网络、配置路由与跨节点通信。

**机制**：
- kubelet 调用 CNI 插件。
- 常见 Calico、Flannel、Cilium。
- 负责 IPAM 与网络策略。
- 跨节点需 Overlay 或路由。

**追问**：Overlay 与 Underlay 区别？Overlay 封装隧道，Underlay 直接路由，性能与复杂度不同。

### Q15. K8s 集群内的 DNS 如何工作？

**结论**：CoreDNS 为 Service/Pod 提供 DNS 解析，Service 名称解析为 ClusterIP。

**机制**：
- Service 有稳定 DNS 名。
- Headless Service 返回 Pod IP 列表。
- Pod 可配置 DNS 策略。
- 解析失败影响服务发现。

**追问**：DNS 解析慢会怎样？服务调用超时，需检查 CoreDNS 与网络。

### Q16. NetworkPolicy 能做什么？

**结论**：按 Pod/Namespace/端口限制进出流量，实现网络隔离。

**机制**：
- 选择 Pod 并定义 ingress/egress 规则。
- 依赖 CNI 支持。
- 默认拒绝或允许。
- 需谨慎避免误封。

**追问**：所有 CNI 都支持吗？不是，需确认插件能力。

### Q17. 服务发现有哪些方式？

**结论**：DNS、环境变量、Service、Headless Service、外部注册中心。

**机制**：
- ClusterIP Service 提供稳定入口。
- Headless 返回 Pod 列表。
- 环境变量注入 Service 信息。
- 微服务常用 Nacos/Consul。

**追问**：Headless 适合什么？StatefulSet 与客户端直连 Pod。

### Q18. Pod 之间如何通信？

**结论**：同节点走网桥/CNI，跨节点走 Overlay 或路由，Service 提供抽象。

**机制**：
- 每个 Pod 有独立 IP。
- 同 Pod 内容器共享网络。
- Service 负载均衡到后端 Pod。
- NetworkPolicy 控制流量。

**追问**：为什么 Pod IP 会变？Pod 重建会分配新 IP，Service 提供稳定访问。

---

## 三、调度与资源（Q19–Q26）

### Q19. K8s 的调度流程是什么？

**结论**：过滤（Predicates）→ 打分（Priorities）→ 选择节点 → 绑定 Pod。

**机制**：
- 过滤不满足资源/亲和/污点的节点。
- 对候选节点打分。
- 选择最高分。
- 更新 Pod 的 nodeName。

**追问**：调度失败会怎样？Pod 处于 Pending，需看事件与资源。

### Q20. 节点亲和与 Pod 亲和有什么区别？

**结论**：节点亲和控制 Pod 调度到哪些节点；Pod 亲和控制 Pod 与已有 Pod 靠近/远离。

**机制**：
- nodeAffinity 按节点标签。
- podAffinity 按同拓扑域。
- 支持 required/preferred。
- 影响调度与数据局部性。

**追问**：强亲和会导致什么？节点资源不足时 Pending。

### Q21. 污点与容忍是什么？

**结论**：污点让节点排斥 Pod，容忍让 Pod 允许被调度到有污点的节点。

**机制**：
- NoSchedule/PreferNoSchedule/NoExecute。
- 节点打污点。
- Pod 声明容忍。
- NoExecute 会驱逐不兼容 Pod。

**追问**：Master 节点为什么有污点？防止业务 Pod 调度到控制面。

### Q22. requests 与 limits 有什么区别？

**结论**：requests 用于调度预留，limits 是运行时上限；超过内存 limit 会被 OOMKilled。

**机制**：
- 调度按 requests 计算资源。
- CPU 超过 limit 被限流。
- 内存超过 limit 被杀死。
- 影响 QoS 等级。

**追问**：只设 limits 不设 requests 会怎样？默认 requests=limits，调度可能过重。

### Q23. K8s 的 QoS 等级有哪些？

**结论**：Guaranteed、Burstable、BestEffort，资源紧张时按等级驱逐。

**机制**：
- Guaranteed：requests=limits。
- Burstable：部分有 requests/limits。
- BestEffort：未设置。
- 驱逐优先级与 OOM 分数相关。

**追问**：核心服务应该设什么？Guaranteed 或合理 Burstable，避免被优先驱逐。

### Q24. HPA 如何工作？

**结论**：根据 CPU/内存/自定义指标调整副本数，在 min/max 范围内伸缩。

**机制**：
- 定期采集指标。
- 计算期望副本数。
- 更新 Deployment/StatefulSet。
- 需设置 requests 作为基准。

**追问**：HPA 扩容不生效？检查 metrics-server、requests、maxReplicas 与资源。

### Q25. VPA 与 PDB 是什么？

**结论**：VPA 自动调整 Pod 资源请求；PDB 保证自愿中断时最小可用副本。

**机制**：
- VPA 调整 requests/limits。
- PDB 限制驱逐数量。
- 与滚动更新/节点维护配合。
- 避免容量骤降。

**追问**：VPA 与 HPA 能同时用吗？同一维度可能冲突，需谨慎。

### Q26. 节点资源不足时会发生什么？

**结论**：触发驱逐（Eviction），按 QoS 与优先级回收 Pod，严重时节点不可调度。

**机制**：
- kubelet 监控内存/磁盘/PID。
- 超过阈值驱逐低优先级 Pod。
- 节点压力污点。
- 调度器避开压力节点。

**追问**：磁盘压力如何缓解？清理日志/镜像，扩容或迁移。

---

## 四、存储与配置（Q27–Q32）

### Q27. Volume、PV 与 PVC 的关系是什么？

**结论**：Volume 是 Pod 内挂载点，PV 是集群存储资源，PVC 是用户对存储的申请。

**机制**：
- Pod 引用 PVC。
- PVC 绑定 PV。
- PV 由管理员/动态供应创建。
- 生命周期独立于 Pod。

**追问**：emptyDir 与 PVC 区别？emptyDir 随 Pod 删除，PVC 可持久化。

### Q28. StorageClass 与动态供应是什么？

**结论**：StorageClass 定义存储类型与供应器，PVC 可自动创建 PV。

**机制**：
- PVC 指定 StorageClass。
- Provisioner 创建后端存储。
- 支持扩容与回收策略。
- 减少手工 PV 管理。

**追问**：动态供应失败怎么查？看 Provisioner 日志与 StorageClass 参数。

### Q29. ConfigMap 与 Secret 有什么区别？

**结论**：ConfigMap 存非敏感配置，Secret 存敏感数据并做编码/加密与访问控制。

**机制**：
- 都可挂载或注入环境变量。
- Secret 默认 Base64，不是加密。
- 需配合 RBAC 与加密存储。
- 更新后 Pod 需重启/热加载。

**追问**：Secret 安全吗？Base64 不等于安全，需加密与最小权限。

### Q30. 有状态服务如何持久化？

**结论**：StatefulSet + PVC 模板 + 稳定 DNS，每个副本绑定独立存储。

**机制**：
- 副本有稳定身份。
- 存储不随 Pod 漂移。
- 有序启动/终止。
- 应用层负责复制与一致性。

**追问**：StatefulSet 能自动选主吗？不能，需应用或 Operator 实现。

### Q31. 数据持久化有哪些方案？

**结论**：本地盘、网络存储（NFS/Ceph）、云盘、对象存储，按性能与可靠性选择。

**机制**：
- 本地盘性能好但不跨节点。
- 网络存储支持共享与迁移。
- 云盘由 CSI 驱动。
- 需考虑备份与快照。

**追问**：数据库能用 NFS 吗？需验证 IO 语义与性能，很多数据库不建议。

### Q32. 多租户如何隔离？

**结论**：Namespace + RBAC + ResourceQuota + NetworkPolicy + 节点池隔离。

**机制**：
- 逻辑隔离资源与权限。
- 配额限制资源使用。
- 网络策略隔离流量。
- 敏感业务用独立集群/节点。

**追问**：Namespace 隔离足够吗？不够，内核与网络仍需额外隔离。

---

## 五、发布、运维与生态（Q33–Q40）

### Q33. 滚动更新是如何工作的？

**结论**：逐步替换旧 Pod，配合 readiness 探针与 maxSurge/maxUnavailable 控制可用性。

**机制**：
- 创建新 ReplicaSet。
- 按策略扩缩容。
- 新 Pod 就绪后摘除旧 Pod。
- 失败可暂停/回滚。

**追问**：滚动更新会中断服务吗？配置得当可零中断，但需就绪探针与 PDB。

### Q34. 如何回滚 Deployment？

**结论**：`kubectl rollout undo` 回到上一版本，依赖 ReplicaSet 历史。

**机制**：
- Deployment 保存 revision。
- 回滚创建旧版 ReplicaSet。
- 可指定 revision。
- 需验证镜像与配置兼容。

**追问**：回滚会丢数据吗？不会影响持久化数据，但可能不兼容新 schema。

### Q35. Helm 的作用是什么？

**结论**：K8s 包管理器，用 Chart 模板化资源，支持版本、依赖与值覆盖。

**机制**：
- Chart 包含 templates/values。
- 渲染为 YAML。
- 支持 install/upgrade/rollback。
- 可管理依赖与 hooks。

**追问**：Helm 与 Kustomize 区别？Helm 模板化与包管理，Kustomize 基于覆盖与 patch。

### Q36. Operator 是什么？

**结论**：用 CRD + 控制器封装领域知识，自动管理复杂有状态应用。

**机制**：
- 自定义资源描述期望状态。
- 控制器监听并调谐。
- 实现备份、升级、故障恢复。
- 常见 Prometheus/MySQL Operator。

**追问**：Operator 与 Helm 区别？Helm 部署一次，Operator 持续调谐。

### Q37. ArgoCD 与 GitOps 是什么？

**结论**：以 Git 为唯一事实源，ArgoCD 监听变更并同步到集群，实现声明式持续交付。

**机制**：
- Git 存 K8s 清单。
- ArgoCD 对比期望与实际。
- 自动/手动同步。
- 支持多集群与回滚。

**追问**：GitOps 如何处理密钥？用 Sealed Secrets/External Secrets，不提交明文。

### Q38. K8s 的监控与日志怎么做？

**结论**：Prometheus + Grafana 监控指标，EFK/Loki 收集日志，事件与追踪补充。

**机制**：
- kubelet/应用暴露指标。
- Prometheus 抓取与告警。
- 日志采集到集中存储。
- 事件用于排障。

**追问**：如何监控 Pod 重启？看 restart count、事件与探针失败。

### Q39. K8s 常见故障如何排查？

**结论**：按 Pod 状态、事件、日志、资源、网络、存储逐层定位。

**机制**：
- Pending：调度/资源/亲和。
- CrashLoop：日志/探针/配置。
- ImagePull：镜像/凭据/网络。
- 网络：DNS/Service/CNI。
- 存储：PVC/PV/Provisioner。

**追问**：`kubectl describe` 看什么？Events 通常直接给出根因。

### Q40. 云原生最佳实践有哪些？

**结论**：无状态优先、声明式、最小权限、资源限制、健康探针、可观测、自动化发布与回滚。

**机制**：
- 12-Factor 与容器友好。
- requests/limits 与 QoS。
- 探针与 PDB。
- GitOps 与渐进发布。
- 监控、日志、追踪齐全。

**追问**：所有服务都要无状态吗？尽量无状态，有状态用 StatefulSet/Operator。

---

## 六、速记清单

| 主题 | 必背一句话 |
|---|---|
| 架构 | API Server + etcd + Scheduler + Controller + kubelet |
| 工作负载 | Pod 最小单元，Deployment 管副本，StatefulSet 管身份 |
| 网络 | Service 抽象，Ingress 七层，CNI 管 Pod 网络 |
| 调度 | 过滤 + 打分 + 绑定，亲和/污点控制 |
| 资源 | requests 调度，limits 限制，QoS 决定驱逐 |
| 存储 | PV/PVC/StorageClass，StatefulSet 绑存储 |
| 发布 | 滚动更新 + 回滚 + Helm/Operator/GitOps |
| 排障 | 状态 → 事件 → 日志 → 资源 → 网络 → 存储 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：40 题 · 详解**
