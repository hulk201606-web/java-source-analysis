# Argo CD 源码深度解析（GitOps 持续交付）

> **源码基线**：Argo CD v2.13.x（最新稳定）  
> **核心仓库**：https://github.com/argoproj/argo-cd  
> **面试权重**：⭐⭐⭐⭐（云原生 GitOps 高频）

---

## 一、Argo CD 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：K8s 的 GitOps 持续交付工具                        │
│  GitOps = Git 作为单一可信源 → 自动同步到 K8s              │
│                                                               │
│  解决什么问题？                                              │
│  传统 CICD：构建 → push image → 手动 kubectl apply           │
│  GitOps：Git 仓库存 Helm Chart/Kustomize → Argo CD 自动同步  │
│  → 声明式 + 可审计 + 可回滚（Git revert 即回滚）             │
│                                                               │
│  核心组件：                                                  │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   API Server   │ Application Controller │ Repo Server  │ │
│  │  REST API + UI │ 核心 Reconciliation   │ Git/Helm 解析  │ │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         ↓              ↓              ↓                       │
│    Web UI/CLI     K8s API Server   Git/Helm Repo              │
│                                                               │
│ ★ Argo CD 是 CNCF 毕业项目 + 云原生 GitOps 事实标准          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Argo CD vs FluxCD vs Kustomize                              │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │  Argo CD       │  FluxCD       │ Kustomize    ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 方式           │ **Pull + UI**  │ **Pull 纯 CLI** │ **Push（CLI）** ││
│  │ 可视化         │ ★★★★★ 强    │ ❌ 无          │ ❌ 无          ││
│  │ Webhook 触发   │ ✅            │ ✅            │ ❌ 手动 apply  ││
│  │ 多集群         │ ✅            │ ✅            │ ❌ 单集群      ││
│  │ 自动自愈       │ ✅（detect + sync）│ ✅          │ ❌            ││
│  │ Helm/Kustomize │ ✅ 都支持     │ ✅ 都支持     │ ✅ Kustomize  ││
│  │ 社区           │ CNCF 毕业     │ CNCF 毕业     │ K8s 官方      ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ 生产推荐：Argo CD（可视化 + UI）+ FluxCD（纯 CLI）          │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Application（★ 核心资源）

```yaml
# ★ Application 是 Argo CD 的核心 CRD 资源
#   定义 Git 仓库 + 目标 K8s 集群 + 同步策略
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: myapp
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/example/k8s-manifests.git
    targetRevision: main          # Git 分支 / Tag / Commit SHA
    path: overlays/production     # Helm Chart 目录 / Kustomize 目录
    helm:
      valueFiles:
        - values-prod.yaml
      parameters:
        - name: replicaCount
          value: "5"
  destination:
    server: https://kubernetes.default.svc  # 目标集群
    namespace: production
  syncPolicy:
    automated:                   # ★ 自动同步
      prune: true                # 自动删除已删除的资源
      selfHeal: true             # 自动自愈（手动 kubectl delete 后自动重建）
    syncOptions:
      - CreateNamespace=true
      - PrunePropagationPolicy=foreground
```

### 2.2 Reconciliation Loop（★ 核心机制）

```go
// ★ Application Controller 的核心：Reconciliation Loop
//   和 K8s Controller 模式一样（desired vs actual → reconcile）
//
// 完整流程（面试高频）：
//
// ① 触发：
//    - Git 仓库有新 commit（Webhook / Poll 5min）
//    - Application CRD 变化
//    - 定时 sync（--sync-timeout 3min）
//
// ② Source（Git 仓库）解析：
//    Repo Server → git clone → helm template / kustomize build
//    → 生成目标 K8s YAML（desired state）
//
// ③ Live（K8s 集群）获取：
//    直接查 K8s API Server → 获取当前运行的 YAML（actual state）
//
// ④ Diff（对比 desired vs actual）：
//    用三方 diff：desired (Git) ↔ actual (K8s)
//    → 检测变化（create/update/delete）
//    → 生成 Sync Plan（要做什么操作）
//
// ⑤ Prune（裁剪/删除）：
//    如果 Git 里删掉了某个资源
//    → Live State 里还存在 → prune=true 时自动 delete
//
// ⑥ Sync（同步到 K8s）：
//    kubectl apply（或 Server-Side Apply）
//    → 创建 / 更新 / 删除资源
//    → 顺序管理（按 Sync Wave）
//
// ⑦ Health Check（健康检查）：
//    检查 Pod 是否 Ready
//    检查 Deployment 是否 Available
//    → 更新 Application CRD 的 Health 状态
//
// ★ Sync Wave（同步 waves，面试高频）：
//    资源上加注解：argocd.argoproj.io/sync-wave: "0"
//    → Wave 0 先执行，Wave 1 后执行
//    → 适合 CRD → Deployment 顺序（先注册 CRD 再创建用 CRD 的资源）
//
// ★ Sync Options（同步选项，面试高频）：
//    CreateNamespace=true → 自动创建 namespace
//    PrunePropagationPolicy=foreground → 删除时先删子资源
//    ServerSideApply=true → 用 K8s Server-Side Apply（推荐）
//    Replace=true → 用 Replace 而不是 Apply
```

### 2.3 Repo Server（Git/Helm 解析）

```go
// ★ Repo Server 负责解析 Git 仓库
//   拉代码 → 渲染 Helm/Kustomize → 输出 YAML
//
// Repo Server 核心功能：
//   ① Git 操作：clone / fetch / checkout / log
//   ② Helm：helm dependency update + helm template
//   ③ Kustomize：kustomize build
//   ④ Jsonnet：jsonnet 渲染
//
// ★ targetRevision 四种类型：
//   ① 分支：main / develop
//   ② Tag：v1.2.0
//   ③ Commit SHA：a1b2c3d（精确到 commit）
//   ④ HEAD：最新 commit（默认）
//
// ★ Helm + Argo CD：
//   Argo CD 直接用 helm template（不执行 install）
//   → 渲染成 K8s YAML → 同步到 K8s
//   → 和 Helm CLI 类似，但 Argo CD 管版本（Git commit SHA）
//   → Helmfile 也支持（批量多个 Chart）
```

### 2.4 Webhook 触发 + K8s Event

```yaml
# ★ Git Webhook 自动触发 Argo CD 同步
#   Git 仓库 push → Webhook → Argo CD /api/webhook → 触发 sync
#
# 各 Git 平台 Webhook：
#   GitHub / GitLab / Bitbucket / Gitea
#
# Webhook 配置：
#   URL: https://argocd.example.com/api/webhook
#   Secret: <随机密钥>
#   Events: push
#
# ★ K8s Event 触发：
#   K8s 集群资源变化（手动 kubectl delete）
#   → Argo CD 检测到 Live State 变化
#   → 如果 selfHeal=true → 自动修复（re-apply）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | GitOps 是什么？| **Git 作为单一可信源** → 代码 + 基础设施都在 Git → Argo CD 自动同步 Git 状态到 K8s；声明式 + 可审计 + 可回滚 |
| 2 | Argo CD 三大组件？| **API Server（REST + UI）/ Application Controller（Reconciliation Loop）/ Repo Server（Git/Helm/Kustomize 解析）** |
| 3 | Application CRD？| 核心资源：**source（Git 仓库/分支/路径）+ destination（K8s 集群/namespace）+ syncPolicy（自动同步/裁剪/自愈）** |
| 4 | Reconciliation Loop？| **① Source 解析（Git→YAML）② Live 获取（K8s API）③ Diff 对比 ④ Sync（kubectl apply）⑤ Health Check**；定时 3min + Webhook 触发 |
| 5 | Sync Wave？| **注解 argocd.argoproj.io/sync-wave: "0"** → Wave 0 先执行 → Wave 1 后执行；适合 CRD 先注册再用 |
| 6 | automated prune + selfHeal？| prune=true → Git 里删掉的资源自动从 K8s 删除；**selfHeal=true → 手动 kubectl delete 后自动重建**（自动自愈）|
| 7 | Argo CD vs FluxCD vs Kustomize？| Argo CD：**Pull + UI + 多集群**；FluxCD：Pull + 纯 CLI；Kustomize：Push + K8s 官方 |
| 8 | targetRevision 四种？| **分支（main）/ Tag（v1.2.0）/ Commit SHA（a1b2c3d）/ HEAD（最新）**；精确到 SHA → 可重现 |
| 9 | Argo CD 和 Helm？| Argo CD 用 **helm template**（渲染成 YAML 再同步）；不是 helm install；Git commit SHA 就是版本 |
| 10 | Webhook 触发？| Git push → Webhook → Argo CD /api/webhook → 触发 sync；比 5min Poll 快很多 |
| 11 | Health Check？| Argo CD 内置：Pod Ready → Healthy；Deployment Available → Healthy；也可自定义（Lua 脚本）|
| 12 | Argo CD 学习路径？| Docker 一键部署 → 写第一个 Application（Helm）→ UI 看 Diff → Sync Wave → Kustomize → 多集群 |
| 13 | Argo CD 和 CI/CD？| Git push → CI 构建镜像 push registry → GitOps commit 更新 image tag → Argo CD Webhook 触发 sync；CI 做构建，Argo CD 做部署 |
| 14 | Argo CD 多集群？| **Application.destination.server 配置多个集群**；Argo CD Agent 模式（远端集群跑 Agent）|
| 15 | Argo CD 权限控制？| RBAC（argocd.argoproj.io/role）+ Git 仓库权限（读）+ K8s 权限（目标集群 admin）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Argo CD v2.13.x  
**面试考点：15 题**
