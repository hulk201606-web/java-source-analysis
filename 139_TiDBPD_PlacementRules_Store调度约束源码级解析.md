# TiDB PD Placement Rules + Store 调度约束源码级深度解析

> **源码基线**：PD v8.0.x（Go）  
> **核心仓库**：tikv/pd → pkg/schedule/labeler/ + placement/ + core/  
> **面试权重**：⭐⭐⭐⭐（多副本选址与高级调度，区分度高）

---

## 一、Replica 选址基本问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ PD 调度器在决定"副本放哪"时必须满足一堆约束              │
│                                                               │
│  最基本：3 副本不能全在一台机器/一个机架上                    │
│  进阶：异地多活、SSD/HDD 混部、TiFlash 列存、指定节点隔离    │
│
│  ★ PD 三代副本放置规则演进（面试主线）：                     │
│                                                               │
│  第一代：replication.location-labels（层级标签）             │
│    只能表达"副本按层级打散"（zone→host）                     │
│                                                               │
│  第二代：Rule Checker（副本数量+Label 约束）                 │
│    max-replicas + 各种 role/label 硬性检查                   │
│                                                               │
│  第三代：★ Placement Rules（PD 4.0+，本文重点）              │
│    声明式规则，支持任意精细的副本放置策略                    │
│    → 可以按 Key 范围给不同数据指定不同副本布局               │
│    → 多规则共存、引擎隔离、TiFlash/TiKV 混合                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Store Label 层级标签

### 2.1 Label 体系

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Store Label（物理拓扑标签，面试必背）               │
│                                                               │
│  TiKV 配置（tikv.toml）：                                     │
│  [server]                                                     │
│  labels = { zone = "z1", host = "host-1", disk = "ssd" }     │
│                                                               │
│  PD 配置副本打散的层级：                                      │
│  config set replication.location-labels ["zone","host"]      │
│  config set max-replicas 3                                   │
│                                                               │
│  含义：                                                        │
│   3 个副本（Peer）必须分布在：                                │
│   ① 不同 host（不能同机）                                    │
│   ② 尽量不同 zone（不同可用区）                              │
│                                                               │
│  拓扑层级（从大到小）：                                       │
│  zone（可用区）→ rack（机架）→ host（主机）→ store（实例）  │
│  PD 保证副本在每个配置的 label 层级上尽量唯一                │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 旧版 Rule Checker

```go
// ★ PD 副本约束检查（源码级）
//
// pkg/schedule/checker/rule_checker.go
//
// type RuleChecker struct {
//     cluster     Cluster
//     // 检查 Region 的副本是否满足规则
// }
//
// func (c *RuleChecker) Check(region *core.RegionInfo) *Operator {
//     // ① 检查副本数量是否符合 max-replicas
//     // ② 检查副本分布是否满足 location-labels 层级隔离
//     // ③ 检查是否有孤立/多余/死亡的 Peer
//     // ④ 不满足 → 生成修复 Operator
//     //    比如在符合 Label 的 Store 上 AddLearner → Promote
// }
//
// // 计算"最合适的新副本 Store"
// // pkg/schedule/filter/filters.go
// // 多层过滤器串联：
// //   ① 排除已有 Peer 的 Store
// //   ② 排除宕机/下线/满盘的 Store
// //   ③ ★ Label 隔离过滤（保证不和现有 Peer 同 host/zone）
// //   ④ 分数最低（最空闲）的 Store 优先
//
// func distinctScore(region *RegionInfo, store *StoreInfo,
//                    replicas int, isolationLevel string) float64 {
//     // 比较候选 Store 和现有 Peer 的 Label 相似度
//     // 共享的 label 层级越少得分越高（越隔离越优先）
// }
//
```

---

## 三、Placement Rules 声明式规则（核心！）

### 3.1 规则数据结构

```go
// ★ Placement Rule 核心结构（源码级，面试核心！）
//
// pkg/schedule/labeler/rules.go + api/.../placement.go
//
// type Rule struct {
//     // ① 规则标识
//     GroupID string  `json:"group_id"`   // 规则组
//     ID      string  `json:"id"`         // 规则 ID
//     Index    int    `json:"index"`       // 优先级（多规则匹配时）
//     Override bool   `json:"override"`    // 是否覆盖后续规则
//
//     // ② ★ 作用的数据范围（Key Range！）
//     StartKey  []byte  `json:"-"`
//     EndKey    []byte  `json:"-"`
//     // 表/索引级规则可以通过 pd-ctl 用 tableID 设置
//
//     // ③ ★ 副本布局（核心！）
//     Role      string  `json:"role"`       // leader / follower / learner
//     Count     int     `json:"count"`       // 这种副本要几个
//
//     // ④ Label 约束（副本必须/不能/优先满足）
//     LabelConstraints []LabelConstraint `json:"label_constraints"`
//     //   key: "disk", op: "in", values: ["ssd"]
//     //   op 类型：in / notIn / exists / notExists
//
//     // ⑤ 位置隔离层级
//     LocationLabels []string `json:"location_labels"`
//
//     // ⑥ 存储引擎（tikv / tiflash）
//     Engine string `json:"engine"`  // "tiflash" 指定列存副本
//
//     // ⑦ 是否强制要求满足（不满足会持续尝试，不降级）
//     IsolationLevel string `json:"isolation_level"`
// }
//
```

### 3.2 默认规则示例

```json
// ★ PD 默认 Placement Rule（JSON）
// pd-ctl config placement-rules show
//
[
  {
    "group_id": "pd",
    "id": "default",
    "role": "voter",
    "count": 3,
    "location_labels": ["zone", "host"],
    "engine": "tikv"
  }
]
//
// 含义：每个 Region 3 个 Voter（TiKV），按 zone/host 层级打散
//
```

### 3.3 高级场景规则

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Placement Rules 五大典型场景（面试高频！）                │
│                                                               │
│  场景 ①：SSD/HDD 混部                                        │
│   3 副本全部放 SSD：                                         │
│   { "role":"voter", "count":3,                              │
│     "label_constraints":[{"key":"disk","op":"in","values":["ssd"]}] }│
│                                                               │
│  场景 ②：★ 2 个 TiKV + 1 个 TiFlash 列存副本                │
│   [                                                          │
│     {"role":"voter","count":2,"engine":"tikv",              │
│      "location_labels":["zone","host"]},                    │
│     {"role":"learner","count":1,"engine":"tiflash"}         │
│   ]                                                          │
│                                                               │
│  场景 ③：三地五中心（金融级异地多活）                        │
│   [                                                          │
│     {"role":"voter","count":3,                              │
│      "label_constraints":[                                  │
│        {"key":"zone","op":"in","values":["z1","z2","z3"]}]},│
│     {"role":"leader","count":1,                             │
│      "label_constraints":[                                  │
│        {"key":"zone","op":"in","values":["z1"]}]}  // Leader 固定 z1│
│   ]                                                          │
│                                                               │
│  场景 ④：按表/Key 范围放置（★ Placement Rules 独有能力）    │
│   给某张热点表单独指定 5 副本 + 独立物理机                    │
│   → 规则带 StartKey/EndKey，只作用于该表的 Region           │
│                                                               │
│  场景 ⑤：节点隔离（独占/驱逐）                               │
│   label_constraints notIn 某 label → 数据不放这些节点       │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.4 规则匹配与调度

```go
// ★ RuleManager 匹配 Region（源码级）
//
// pkg/schedule/labeler/labeler.go
//
// type RuleManager struct {
//     // 按 Key Range 组织的规则（用于快速匹配 Region）
//     rules map[string]*Rule
// }
//
// // ★ 根据 Region 的 StartKey 找到所有适用规则
// func (m *RuleManager) GetRulesByRegion(region *core.RegionInfo) []*Rule {
//     key := region.GetStartKey()
//     // 遍历规则，匹配 StartKey/EndKey 覆盖该 Region 的
//     // 按 Index 排序；Override=true 的规则截断后续
// }
//
// // ★ 调度器如何使用规则：
// // 1. ReplicaChecker / RuleChecker 周期检查每个 Region
// // 2. 用规则计算"期望副本布局"（expected layout）
// // 3. 和 Region 当前 Peers 对比：
// //    - 缺符合的副本 → AddLearner + Promote（在满足 Label 的 Store）
// //    - 多不符合的副本 → RemovePeer
// //    - Leader 位置不对 → TransferLeader
// // 4. 生成 Operator 下发 TiKV
//
// // ★ 和调度器协作：
// //   硬约束（Checker）：必须满足规则，不满足持续修复
// //   软偏好（balance Scheduler）：在满足规则前提下做负载均衡
//
```

---

## 四、Store 状态与调度过滤

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV Store 生命周期状态（面试高频）                       │
│                                                               │
│  Up（正常）→ 可调度副本                                       │
│  Disconnected（失联）→ 短暂容忍                              │
│  Down（心跳超时）→ 触发副本补建                              │
│  Offline（下线中）→ 正在迁走 Region（仍服务）                │
│  Tombstone（已下线）→ 完全移除                               │
│
│  ★ Store 评分（balance 调度依据）：                          │
│   Leader 调度看：leader_count / leader_weight               │
│   Region 调度看：region_count / region_weight               │
│   空间调度看：used_size / capacity（磁盘使用率）             │
│   → 优先从高分 Store 迁出、迁入低分 Store                   │
│
│  ★ 调度过滤器链（候选 Store 过滤）：                         │
│   StoreStateFilter   排除 Down/Offline/满盘                  │
│   StoreAttributeFilter 排除引擎不符（tikv/tiflash）          │
│   ★ LabelConstraintFilter 排除 Label 不符（ssd/zone）        │
│   RegionFitter       排除已有 Peer、违反隔离层级             │
│   最后用 score 选最空闲的                                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | PD 三代副本放置演进？| location-labels 层级 → Rule Checker → **Placement Rules 声明式（PD4.0+）** |
| 2 | Label 层级？| zone→rack→host→store；PD 保证副本在配置的 label 层级唯一 |
| 3 | replication.location-labels？| 指定按哪些 label 打散副本，如 ["zone","host"] |
| 4 | Placement Rule 核心字段？| **StartKey/EndKey（范围）+ Role + Count + LabelConstraints + Engine + IsolationLevel** |
| 5 | Rule 按 Key 范围？| 规则带 StartKey/EndKey，可给单表/索引指定独立布局（独有能力）|
| 6 | TiFlash 副本怎么配？| 规则 role=learner、engine=tiflash、count=1 |
| 7 | LabelConstraint 操作？| in / notIn / exists / notExists |
| 8 | 三地五中心？| 3 voter 跨 zone + leader 固定主 zone；金融异地多活 |
| 9 | SSD/HDD 混部？| label_constraints key=disk in [ssd] |
| 10 | 规则怎么生效？| Checker 周期对比"期望布局"vs 当前 Peers → 生成 Add/Remove/TransferLeader Operator |
| 11 | 硬约束 vs 软偏好？| Checker 强制满足规则；balance 调度器在满足前提下做负载均衡 |
| 12 | Store 状态？| Up / Disconnected / Down / Offline / Tombstone |
| 13 | Store 评分？| Leader 看 leader_count/weight；Region 看 count/weight；空间看 used/capacity |
| 14 | 调度过滤器链？| 状态过滤 → 引擎过滤 → Label 约束过滤 → 隔离层级 → score 选最空闲 |
| 15 | 面试最高频考点排序？| **1. Placement Rule 字段 2. Label 层级隔离 3. TiFlash 规则 4. 按 Key 范围 5. Store 状态/评分** |
| 16 | 学习路径？| rules.go Rule 结构 → labeler GetRulesByRegion → rule_checker → filters.go |
| 17 | 面试常见追问？| 规则不满足会怎样？→ Checker 持续生成 Operator 直到满足（可用 IsolationLevel 控制严格度）|
| 18 | Override/Index 作用？| 多规则匹配时按 Index 排序；Override 截断后面规则 |
| 19 | 下线节点流程？| Store 标 Offline → balance/ReplicaChecker 迁走所有 Region → 变 Tombstone |
| 20 | 面试陷阱？| 说 Placement Rules 只能控制副本数 → ❌ 还能按 Key 范围、引擎、Role、Label 精细控制 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：pkg/schedule/labeler/ + placement/ + checker/rule_checker.go  
**面试考点：20 题**  
**差异化点**：三代规则演进 + Rule 字段源码 + TiFlash/三地五中心/按表规则 + 硬约束 vs 软偏好
