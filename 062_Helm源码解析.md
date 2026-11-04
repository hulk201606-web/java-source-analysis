# Helm 源码深度解析（K8s 包管理器）

> **源码基线**：Helm 3.16.x（最新稳定）  
> **核心仓库**：https://github.com/helm/helm  
> **面试权重**：⭐⭐⭐⭐（云原生高频）

---

## 一、Helm 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：Kubernetes 包管理器（apt-get for K8s）           │
│  解决什么问题？                                              │
│  → 手动写 K8s YAML 太繁琐（RBAC + Deployment + Service + ConfigMap）│
│  → 版本管理混乱（不知道哪个应用部署了什么版本）                 │
│  → 回滚困难（手动 delete 再 apply）                          │
│                                                               │
│  Helm 3 vs Helm 2 关键变化（面试高频）：                       │
│  ┌──────────────┬──────────────────────┬──────────────────────┐│
│  │                │     Helm 2             │     Helm 3          ││
│  ├──────────────┼──────────────────────┼──────────────────────┤│
│  │ 组件           │ Tiller（集群内进程）   │ **纯 CLI**（无集群内组件）││
│  │ 存储           │ ConfigMap             │ **Secret**（默认）  ││
│  │ RBAC          │ Tiller 需要集群权限     │ 用户自己有 K8s 权限    ││
│  │ 依赖           │ requirements.yaml     │ **Chart.yaml dependencies** ││
│  │ CRD           │ ❌ 不支持              │ ✅ 支持               ││
│  │ 命令风格       │ helm install --name   │ helm install RELEASE   ││
│  └──────────────┴──────────────────────┴──────────────────────┘│
│                                                               │
│ ★ Helm 3 是主流（Tiller 被移除，简化架构 + 安全）             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Chart 目录结构（面试高频）                                    │
│                                                               │
│  mychart/                                                     │
│  ├── Chart.yaml          # 元数据（name/version/description）  │
│  ├── values.yaml         # 默认值（模板变量）                  │
│  ├── templates/          # 模板文件（Go Template）            │
│  │   ├── deployment.yaml  # Deployment 模板                   │
│  │   ├── service.yaml     # Service 模板                      │
│  │   ├── configmap.yaml   # ConfigMap 模板                     │
│  │   └── _helpers.tpl     # 辅助模板（define/block）           │
│  ├── charts/             # 子 Chart（依赖）                    │
│  │   └── redis/           # 依赖的 Redis Chart                │
│  └── crds/               # CRD 定义（Helm 3+）                │
│                                                               │
│ ★ 模板引擎：Go html/template + Sprig 函数库                    │
│ ★ 渲染结果：K8s YAML → 直接 apply 到 K8s API Server           │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Helm 3 核心组件

```go
// ★ Helm 3 架构极简：纯 CLI（没有集群内组件！）
//
// 关键组件：
//   ① Helm CLI（Go 实现，单二进制）
//   ② K8s API（和 kubectl 一样用 client-go）
//   ③ Storage Driver（存 Release 状态到 K8s Secret/ConfigMap）
//
// 命令 → 核心流程：
//   helm install mychart -n myapp
//     ↓
//   ① Load Chart（解压 + 校验）
//   ② Load Values（values.yaml + -f 覆盖）
//   ③ Render Templates（Go Template → YAML）
//   ④ Validate（YAML 合法性 + K8s 准入）
//   ⑤ Install（调 K8s API 创建资源）
//   ⑥ Store Release（写 K8s Secret 存 Release 状态）
//
// ★ Storage Driver（Helm 3 默认 Secret）：
//   Release 信息存在 K8s Secret：
//   kubectl get secret -n helm
//     → sh.helm.release.v1.myapp.v1
//   → 每次 install/upgrade/rollback 都创建新版本 Secret
```

### 2.2 Chart Template 渲染（★ 面试高频）

```yaml
# ★ Helm 模板 = Go Template + Sprig 函数
#   deployment.yaml 示例：
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}-{{ .Chart.Name }}
  labels:
    app: {{ .Release.Name }}
    version: {{ .Chart.Version | quote }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app: {{ .Release.Name }}
  template:
    metadata:
      labels:
        app: {{ .Release.Name }}
    spec:
      containers:
        - name: {{ .Chart.Name }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            - name: http
              containerPort: {{ .Values.service.port }}
              protocol: TCP
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          {{- if .Values.resources }}
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          {{- end }}
---
# 条件判断（只有 replicas > 1 才创建 HPA）
{{- if gt .Values.replicaCount 1 }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ .Release.Name }}-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ .Release.Name }}-{{ .Chart.Name }}
  minReplicas: {{ .Values.autoscaling.minReplicas }}
  maxReplicas: {{ .Values.autoscaling.maxReplicas }}
{{- end }}

# ★ 常用模板内置变量（面试高频）：
# .Chart      → Chart.yaml 字段（name/version/description）
# .Values     → values.yaml + -f 覆盖的变量
# .Release    → Release 元数据（Name/Namespace/Revision/IsUpgrade）
# .Capabilities → K8s 集群能力（APIVersions/KubeVersion）
# .Template   → 当前模板信息（Name/BasePath）
# .Files      → 读 Chart 内任意文件（.Files.Get "config.ini"）
# .Secrets    → 访问加密 Values（helm install --values=secrets.yaml）
# .Required   → 值必须存在，否则渲染失败

# ★ 常用 Sprig 函数（面试高频）：
# quote / toYaml / nindent    → 格式控制
# default                     → 默认值
# required                    → 必填值
# include / define / block    → 模板复用（_helpers.tpl）
# range                       → 循环
# if / else / end             → 条件
# indent                      → 缩进
# b64enc / b64dec             → Base64 编码（ConfigMap/Secret 需要）
# now / date                  → 日期
```

### 2.3 Release 生命周期

```go
// ★ Release 生命周期（Helm 核心概念）
//   helm install → 创建 Release revision v1
//   helm upgrade → 创建 revision v2（保留 v1 Secret）
//   helm rollback v1 → 回到 revision v1
//   helm uninstall → 删除 Release（保留 revision Secret）
//   helm history → 看所有 revision
//
// Release 对象结构：
//   Name: myapp
//   Namespace: default
//   Chart: mychart-1.0.0
//   Config: { replicas: 3, image: xxx }
//   Info: { Status: deployed, Description: "Install complete" }
//   Version: 1  // revision 版本
//   Templates: 渲染后的 YAML（完整）
//   Manifest: 合并后的完整 YAML
//   Hooks: 生命周期钩子
//
// ★ Helm 升级流程（面试高频）：
//   helm upgrade mychart -n myapp
//     ↓
//   ① Load 新 Chart + 合并新 Values
//   ② Render → 得到新 Manifest
//   ③ **Diff** 新 Manifest vs 旧 Manifest
//      → 检测变化的资源（只更新变化的，不删没变化的）
//   ④ 先执行 pre-upgrade hooks
//   ⑤ 更新变化的资源（用 kubectl apply）
//   ⑥ 执行 post-upgrade hooks
//   ⑦ Store 新 revision Secret
//
// ★ Helm vs kubectl apply（面试高频）：
//   kubectl apply：简单直接，自己管理 YAML
//   Helm：模板化 + 版本管理 + Release 状态 + 依赖管理 + 回滚
//   → Helm 是 kubectl 的高级封装，不是替代
```

### 2.4 Helm Hooks

```yaml
# ★ Helm Hooks 生命周期钩子（面试高频）
#   在 Chart.yaml 或单独文件（templates/hooks.yaml）定义
#
# 七种 Hook 类型（按执行时机）：
#   pre-install     → install 之前
#   post-install    → install 之后
#   pre-upgrade     → upgrade 之前
#   post-upgrade    → upgrade 之后
#   pre-rollback    → rollback 之前
#   post-rollback   → rollback 之后
#   test            → helm test 执行（验证用）
#
# hooks.yaml 示例：
apiVersion: batch/v1
kind: Job
metadata:
  name: "{{ .Release.Name }}-db-migration"
  annotations:
    "helm.sh/hook": post-install       # install 完成后执行
    "helm.sh/hook-weight": "5"         # 同 hook 按 weight 顺序
    "helm.sh/hook-delete-policy": before-hook-creation  # 下次前先删
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: migration
          image: {{ .Values.migration.image }}
          command: ["/bin/sh", "-c", "run-db-migration"]
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Helm 2 vs Helm 3 核心变化？| **移除 Tiller（集群内组件）→ 纯 CLI**；默认存储从 ConfigMap → Secret；依赖声明移到 Chart.yaml |
| 2 | Helm 核心概念？| **Chart（应用模板）/ Release（实例，每次 install/upgrade 一个新 revision）/ Repository（仓库）** |
| 3 | Chart 目录结构？| Chart.yaml（元数据）+ values.yaml（默认值）+ templates/（Go Template）+ charts/（子 Chart）+ crds/（Helm 3+ CRD）|
| 4 | 模板引擎是什么？| **Go html/template + Sprig 函数库**；.Chart/.Values/.Release 三大内置变量 |
| 5 | Release 生命周期？| install → v1；upgrade → v2（保留 v1）；rollback v1 → 回到 v1；uninstall → 删除但保留历史 |
| 6 | Helm 怎么管理版本？| **每次 install/upgrade 创建新 revision**（存在 K8s Secret）；helm history 看所有 revision；helm rollback 回到旧版本 |
| 7 | Helm 升级流程？| Load 新 Chart → Render → **Diff 新旧 Manifest**（只更新变化的）→ 执行 pre-upgrade hooks → apply → post-upgrade hooks → Store revision |
| 8 | Helm vs kubectl apply？| kubectl apply：简单，自己写 YAML；Helm：**模板化 + 版本管理 + Release 状态 + 依赖 + 回滚**；Helm 是 kubectl 的高级封装 |
| 9 | Helm Hooks？| **7 种 Hook 类型**：pre/post install/upgrade/rollback + test；注解 helm.sh/hook 指定时机；helm.sh/hook-weight 控制顺序 |
| 10 | Helm Values 三层覆盖？| ① Chart 内 values.yaml（默认）② -f 指定文件（环境覆盖）③ --set key=value（命令行临时覆盖）→ 后覆盖前 |
| 11 | Helm 依赖管理？| **Chart.yaml dependencies** 声明依赖 → helm dependency update 下载 → vendor/ 目录管理 |
| 12 | Helm 加密 Values？| **helm secrets** 插件 + sops 加密；或 helm install --values=secrets.yaml + 外部 Vault |
| 13 | Helm 学习路径？| 写一个简单 Chart → Values + Templates → Helper Tpl → Hooks → 子 Chart 依赖 → Helmfile 批量管理 |
| 14 | Helm 为什么是 K8s 标配？| K8s YAML 繁琐 + Helm 模板化 + 版本管理 + 社区生态（Artifact Hub 几万 Chart）→ CNCF 毕业项目 |
| 15 | Helm 模板安全？| 模板渲染在本地（不是 K8s 端）；可信 Helm Chart 才能用（执行任意 YAML）；不要直接执行网上下载的 Chart |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Helm 3.16.x  
**面试考点：15 题**
