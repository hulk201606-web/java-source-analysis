# Sentinel 源码深度解析（熔断限流）

> **源码基线**：Sentinel 1.8.x（Spring Cloud 2021.0.8 配套）  
> **核心仓库**：alibaba/Sentinel  
> **大纲来源**：19_SpringCloud源码调研大纲.md P0-3  
> **面试权重**：⭐⭐⭐⭐⭐（阿里系必问）

---

## 一、Sentinel 核心概念

```
┌─────────────────────────────────────────────────────────────┐
│  Sentinel 四大核心概念                                       │
│                                                               │
│  Resource：资源（要保护的对象）→ 接口/方法/下游服务           │
│  Rule：规则怎么管 → 限流规则/熔断规则/热点参数/系统自适应    │
│  Context：调用上下文（不同调用链统计分开）                    │
│  Entry：入口（SphU.entry() 返回的对象）→ 退出时 exit()       │
│                                                               │
│  ★ 四大功能                                                   │
│   ① 流量控制（FlowRule）：QPS / 线程数限流                   │
│   ② 熔断降级（DegradeRule）：异常比例/慢调用比例/异常数      │
│   ③ 热点参数（ParamFlowRule）：按参数值做限流                 │
│   ④ 系统自适应（SystemRule）：CPU/Load/入口 QPS              │
│                                                               │
│  ★ 核心处理器链（责任链模式，大纲锚点 ProcessorSlotChain）   │
│    NodeSelectorSlot → ClusterBuilderSlot → StatisticSlot      │
│    → FlowSlot → DegradeSlot → SystemSlot → ParamFlowSlot     │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心处理链路（★ 大纲锚点：SphU.entry 完整调用栈）

### 2.1 SphU.entry → Slot 链（★★★★★ 大纲要求核心）

```java
// ★ SphU.entry 完整调用栈（Sentinel 源码最核心的执行路径）
//
// 用户代码：
//   Entry entry = SphU.entry("api/order/get");
//   try {
//       // 业务代码（被保护的资源）
//   } finally {
//       entry.exit();
//   }
//
// 完整源码链路（大纲锚点）：
//
// SphU.entry(resourceName)  [sentinel-core API]
//   → Env.sph.entry(resourceName, EntryType.IN)
//     → CtSph.entryWithType(resourceName, EntryType.IN, 1)
//       [★ 大纲锚点 L50-90]
//
//       ① 构建 Resource：new StringResourceWrapper
//       ② 创建 Context：Constants.CONTEXT → 每个调用链独立 Context
//       ③ 创建 Entry：new CtEntry(resourceWrapper, ...)
//       ④ ★ 执行 Slot 链（ProcessorSlotChain.entry）
//          [★ 大纲锚点 ProcessorSlotChain.entry]
//
// ★ ProcessorSlotChain（Slot 责任链，大纲锚点）：
//   按顺序执行每个 Slot.entry()：
//
//   1. NodeSelectorSlot
//      作用：为不同 Context 创建不同 ClusterNode
//      → 统计跨 Context 的集群数据
//      → 核心：Map<Context, Node> clusterNodeMap
//
//   2. ClusterBuilderSlot
//      作用：创建 ClusterNode（聚合多个 Node 的数据）
//      → 同一个 Resource 不管从哪个 Context 进来 → 同一个 ClusterNode
//
//   3. StatisticSlot [★ 大纲锚点 L50-90]
//      作用：计数器 +1（pass）
//      → StatisticNode.addPass()
//      → LeapArray 滑动窗口计数器（核心数据结构）
//
//   4. FlowSlot [★ 大纲锚点]
//      作用：限流规则检查
//      → FlowRuleManager.getRules() → 遍历 FlowRule
//      → FlowChecker.checkFlow(resource, rule, context)
//      → 如果限流 → 抛出 FlowException
//
//   5. DegradeSlot [★ 大纲锚点]
//      作用：熔断降级规则检查
//      → DegradeRuleManager.getRules() → 遍历 DegradeRule
//      → DegradeRuleManager.checkDegrade(resource, rule)
//      → 如果熔断 → 抛出 DegradeException
//
//   6. SystemSlot
//      作用：系统自适应检查（CPU/Load/QPS）
//      → SystemRuleManager.checkSystem()
//
//   7. ParamFlowSlot
//      作用：热点参数限流
//      → ParamFlowRuleManager.check(resource, ...)
//
// ⑤ 如果没有 BlockException → 返回 Entry
// ⑥ 如果 BlockException → 重试（重试策略）或直接抛出
//
// ★ entry.exit() 反向遍历 Slot 链（每个 Slot.exit()）：
//   StatisticSlot.exit() → 计数器 -1（pass）
//   ClusterBuilderSlot.exit() → 更新 ClusterNode
//   NodeSelectorSlot.exit() → 更新 Node
//   → 确保统计准确
```

### 2.2 限流规则 FlowRule（★ 大纲锚点：FlowChecker.checkFlow）

```java
// ★ FlowRule 核心字段（大纲要求）
//   resource        → 保护的资源名
//   grade           → 限流类型：0=线程数，1=QPS（默认）
//   count           → 阈值（线程数上限 / QPS 上限）
//   strategy        → 直接 / 关联 / 链路 / 集群
//   limitApp        → 只对特定调用方限流
//   clusterMode     → 是否集群模式
//
// ★ FlowChecker.checkFlow（大纲锚点）
//   → 根据 grade 类型判断：
//
//   grade=0（线程数）：
//     StatisticNode.curThreadNum() 原子计数器
//     → 当前活跃线程数 >= count → 限流
//     → 原理：ThreadLocal + CAS 计数
//
//   grade=1（QPS，默认）：
//     StatisticNode.pass / passWindow
//     → 滑动窗口计数器（LeapArray<MetricBucket>）
//     → 当前窗口 pass 数 >= count → 限流
//
// ★ 滑动窗口计数器（LeapArray，大纲锚点）：
//
//   LeapArray<MetricBucket> 核心结构：
//   ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
//   │ 0 │ 1 │ 2 │ 3 │...│...│...│...│58 │59 │  ← 60 个桶（1 秒一个）
//   └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
//     ↑ 当前桶                                        ↑ 前 59 秒
//
//   LeapArray.currentWindow() [大纲锚点]：
//     当前时间戳 % 1000 → 当前桶下标
//     → 当前桶的 MetricBucket.addPass()
//     → 需要统计 QPS 时：当前桶 pass + 前 1 秒桶 pass
//     → 近似 QPS（足够精确）
//
// ★ MetricBucket 字段：
//   long pass;       // 通过数
//   long block;      // 被拦截数
//   long success;    // 成功数
//   long exception;  // 异常数
//   long rt;         // RT 总和
//   int minRt;       // 最小 RT
```

### 2.3 熔断规则 DegradeRule（★★★★★ 熔断状态机）

```java
// ★ DegradeRule 熔断三种规则（大纲锚点）
//
// grade=0：慢调用比例
//   threshold → 慢调用 RT 阈值（ms）
//   count     → 统计窗口内慢调用数
//   minRequestAmount → 最小请求数（不够不开熔断）
//   timeWindow → 熔断时间窗口（秒）
//
// grade=1：异常比例
//   ratioThreshold → 异常比例阈值（0.0-1.0）
//   minRequestAmount → 最小请求数
//   timeWindow → 熔断时间窗口
//
// grade=2：异常数
//   count → 异常数阈值
//   timeWindow → 熔断时间窗口
//
// ★ CircuitBreaker 熔断状态机（大纲锚点 tryPass/revertToClosed）
//
//   ┌──────────────────────────────────────┐
//   │          CLOSED（正常）               │
//   │   触发规则（异常/慢调用太多）        │
//   └──────────────┬───────────────────────┘
//                  ↓
//   ┌──────────────────────────────────────┐
//   │          OPEN（熔断打开）             │
//   │   timeWindow 秒后 → HALF_OPEN        │
//   │   所有请求直接 Block                  │
//   └──────────────┬───────────────────────┘
//                  ↓
//   ┌──────────────────────────────────────┐
//   │       HALF_OPEN（半开）               │
//   │   放行一个探测请求                   │
//   │   成功 → CLOSED                     │
//   │   失败 → OPEN                       │
//   └──────────────────────────────────────┘
//
// ★ 状态转换源码（CircuitBreaker）：
//   CLOSED → OPEN：
//     currentDegrade.getStats() → 触发条件 → updateState(OPEN)
//
//   OPEN → HALF_OPEN：
//     after(timeWindow, now) → updateState(HALF_OPEN)
//
//   HALF_OPEN → CLOSED / OPEN：
//     tryPass() 放行探测请求
//     detectSuccess() 成功 → CLOSED
//     detectError() 失败 → OPEN
//
// ★ 和 Resilience4j 对比：
//   Sentinel：**三状态（CLOSED/OPEN/HALF_OPEN）** + LeapArray 滑动窗口
//   Resilience4j：**六状态（含 DISABLED/FORCED_OPEN/METRICS_ONLY）** + Bucket4j
//   → Sentinel 更专注限流熔断 + 控制台；Resilience4j 轻量功能多
```

### 2.4 Node 层级结构

```java
// ★ Sentinel Node 层级（大纲锚点）
//
// Node（接口）→ 三种实现：
//
// ① StatisticNode
//   基础统计节点 → pass/block/success/exception/rt
//   用 LeapArray<MetricBucket> 实现滑动窗口
//   → 每个 Resource 一个 StatisticNode
//
// ② ClusterNode
//   集群统计节点 → 聚合多个 StatisticNode
//   → 同一个 Resource 不管从哪个 Context 进来 → 同一个 ClusterNode
//   → 用于统计跨调用方的全局数据
//
// ③ EntranceNode
//   入口节点 → 统计调用链入口数据
//   → Context 里的入口节点
//
// ★ 层级关系：
//   Context.entranceNode（EntranceNode）
//     → 第一个 Node（NodeSelectorSlot 创建）
//       → 每经过一个 Slot 创建一个 Node
//         → StatisticNode（最终计数）
//         → ClusterNode（ClusterBuilderSlot 创建）
```

---

## 三、面试考点（20 题，★ 大纲要求）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Sentinel 核心处理器链？| **Slot 责任链 7 个**：NodeSelectorSlot → ClusterBuilderSlot → StatisticSlot → FlowSlot → DegradeSlot → SystemSlot → ParamFlowSlot |
| 2 | Sentinel 限流 vs 熔断？| **限流预防性**（流量高就挡）→ FlowSlot；**熔断故障后**（异常/慢调用多了断开）→ DegradeSlot；两者都用 LeapArray 统计 |
| 3 | 熔断三种规则？| **grade=0 慢调用比例 / grade=1 异常比例 / grade=2 异常数**；都有 timeWindow 熔断时间 |
| 4 | 熔断状态机？| **CLOSED → OPEN → HALF_OPEN → CLOSED/OPEN**（timeWindow 后触发 HALF_OPEN）；CircuitBreaker 实现 tryPass/detectSuccess/detectError |
| 5 | 滑窗计数器？| **LeapArray<MetricBucket>**（60 个 1 秒桶）；当前桶 + 前 59 秒桶 = QPS；比 Fixed Window 精准 |
| 6 | 热点参数限流怎么实现？| **ParamFlowSlot**（第 7 个 Slot）→ 按方法参数值做限流 → HashMap<paramValue, QPS>；常用在秒杀/抽奖参数 |
| 7 | 规则持久化？| **数据源 SPI**：FileDataSource（文件）/ NacosDataSource（Nacos）/ ApolloDataSource（Apollo）；热更新不需要重启 |
| 8 | Sentinel vs Resilience4j？| Sentinel：**完整限流熔断框架 + 控制台 + 热点参数**；Resilience4j：**轻量（只做熔断/隔离/重试）+ 六状态机 + Bucket4j**；选哪个看场景 |
| 9 | Sentinel vs Hystrix？| Hystrix 已停更（2018）→ **Sentinel 官方推荐替代**；Hystrix 只有熔断降级；Sentinel 限流+熔断+热点+系统自适应 |
| 10 | Slot 链为什么用责任链？| 每个 Slot 功能独立 → 新增 Slot 不需要改已有 Slot → 可扩展（自定义 Slot）|
| 11 | QPS 限流实现？| LeapArray 滑动窗口 → 当前窗口 pass 数 + 前窗口 pass 数 ≈ QPS；线程数限流器用 AtomicInteger |
| 12 | Node 计数器？| **StatisticNode + LeapArray** → 四种指标：pass/block/success/exception；每个 Resource 一个 StatisticNode |
| 13 | ClusterNode 作用？| **聚合多个 StatisticNode** → 跨 Context 统计；同一个 Resource 不管从哪个入口调 → 同一个 ClusterNode |
| 14 | Sentinel 集群模式？| **Cluster Mode + Token Server** → 全局限流令牌池；需要额外部署 Token Server |
| 15 | Sentinel 学习路径？| @SentinelResource 标注 → 加限流规则 → 看 Slot 链 → 熔断三状态 → LeapArray 源码 → 热点参数 |
| 16 | FlowRule 四种 strategy？| **DIRECT（直接）/ RELATE（关联）/ CHAIN（链路）/ CLUSTER（集群）**；默认 DIRECT |
| 17 | 降级策略？| 默认：抛 BlockException → 调用方 catch；自定义：@SentinelResource(fallback="方法名") → 降级方法返回默认值 |
| 18 | Sentinel Web Servlet 拦截器？| sentinel-web-servlet → SentinelFilter（Filter Chain 入口）→ SphU.entry |
| 19 | Sentinel 注解 @SentinelResource？| 标注资源 → value（资源名）+ blockHandler（限流处理）+ fallback（降级处理）|
| 20 | Sentinel 规则 API？| FlowRuleManager.loadRules(List\<FlowRule\>) / DegradeRuleManager.loadRules()；生产用 Nacos 持久化 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Sentinel 1.8.x  
**面试考点：20 题**  
**大纲来源**：19_SpringCloud源码调研大纲.md P0-3
