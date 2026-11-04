# 分布式限流 Sentinel FlowSlot + Dubbo LoadBalance 源码级解析

> **源码基线**：Sentinel 1.8.x + Dubbo 3.x（Java）  
> **面试权重**：⭐⭐⭐⭐⭐（限流完整实现 + Dubbo 负载均衡高频必问）

---

## 一、Sentinel 限流核心总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Sentinel = 阿里开源流量控制熔断降级框架                    │
│                                                               │
│  Sentinel 核心设计模式：ProcessorSlotChain 责任链              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ NodeSelectSlot（选择集群节点）                         │   │
│  │   ↓                                                     │   │
│  │ ClusterBuilderSlot（构建集群节点）                     │   │
│  │   ↓                                                     │   │
│  │ FlowSlot ★ 限流（本文重点！）                         │   │
│  │   ↓                                                     │   │
│  │ DegradeSlot 熔断降级                                    │   │
│  │   ↓                                                     │   │
│  │ SystemSlot 系统自适应                                   │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ FlowSlot 四种限流模式（面试必背！）：                        │
│  ┌──────────┬──────────────┬──────────────┐                 │
│  │ 模式       │ 核心机制       │ 适用场景       │               │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ QPS 限流   │ LeapArray 滑动窗口 │ 每秒请求数限制  │               │
│  │ 线程数限流 │ 并发线程计数    │ 防止线程爆满    │               │
│  │ 匀速排队   │ 漏桶算法       │ 严格匀速       │               │
│  │ 系统自适应 │ TCP 队列 + RT  │ System 维度    │               │
│  └──────────┴──────────────┴──────────────┘                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Sentinel FlowSlot 源码级

### 2.1 LeapArray 滑动窗口实现

```java
// ★ Sentinel FlowSlot + LeapArray 滑动窗口（面试核心！）
//
// // slots/block/flow/FlowSlot.java
//
// public class FlowSlot extends AbstractProcessorSlot<FlowRule> {
//
//     @Override
//     public void entry(Context context, ResourceWrapper resourceWrapper,
//                       DefaultNode node, int count, boolean prioritized, Object... args)
//             throws Throwable {
//         // ★ 检查所有限流规则
//         List<FlowRule> rules = FlowRuleManager.getRules();
//         for (FlowRule rule : rules) {
//             if (!canPassCheck(rule, context, node, count)) {
//                 // ❌ 限流拒绝 → 抛出 FlowException
//                 throw new FlowException(rule, "Flow limit exceeded");
//             }
//         }
//     }
//
//     private boolean canPassCheck(FlowRule rule, Context context,
//                                   DefaultNode node, int count) {
//         // ★ 根据限流模式选择不同检查器
//         switch (rule.getGrade()) {
//             case FlowRule.GRADE_QPS:
//                 // QPS 限流：LeapArray 滑动窗口
//                 return canPassByQps(rule, node);
//             case FlowRule.GRADE_THREAD:
//                 // 线程数限流
//                 return canPassByThread(rule, node);
//             case FlowRule.GRADE_BEHAVIOR_UNIFORM:
//                 // 匀速排队（漏桶）
//                 return canPassByUniform(rule, node);
//         }
//     }
// }
//
// // ★ LeapArray 滑动窗口核心（面试必背源码！）
// // slots/statistic/LeapArray.java
//
// public class LeapArray<T> {
//     private final int sampleCount;        // 窗口分成 N 格（比如 10）
//     private final long intervalInMs;      // 窗口时间（比如 1000ms）
//     private final long windowLengthInMs;   // 每格时间 = intervalInMs / sampleCount
//     private AtomicReferenceArray<WindowWrap<T>> array; // 环形数组
//
//     // ★ 核心：当前窗口的统计对象
//     public WindowWrap<T> currentWindow(long timeMillis) {
//         long time = timeMillis - start;
//         int idx = (int)(time / windowLengthInMs) % sampleCount;
//
//         WindowWrap<T> old = array.get(idx);
//         if (old == null || old.isTimeOut(timeMillis)) {
//             // 窗口过期 → 重置
//             WindowWrap<T> newWindow = new WindowWrap<>(timeMillis);
//             array.compareAndSet(idx, old, newWindow);
//         }
//         return array.get(idx);
//     }
//
//     // ★ 遍历所有有效窗口统计总量
//     public T getWindowData() {
//         long currentTime = System.currentTimeMillis();
//         T result = new T();
//         for (WindowWrap<T> window : array) {
//             if (window != null && !window.isTimeOut(currentTime)) {
//                 result.merge(window.getValue());  // 合并有效窗口
//             }
//         }
//         return result;
//     }
// }
//
// ★ 面试高频：滑动窗口 vs 固定窗口？                               │
//   固定窗口：1s 边界 → 可能 2x 流量突发（请求 A 在后半秒 B 在前半秒）│
//   滑动窗口：10 格 × 100ms → 无边界突发 → 准确                 │
//
```

### 2.2 LeapArray 10 格窗口演示

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Sentinel LeapArray 10 格窗口可视化（面试必背！）           │
│                                                               │
│  intervalInMs = 1000（1s 窗口）                               │
│  sampleCount = 10（分 10 格）                                 │
│  windowLengthInMs = 100ms（每格 100ms）                      │
│                                                               │
│  时间轴：                                                     │
│  |--0--|--1--|--2--|--3--|--4--|--5--|--6--|--7--|--8--|--9--| │
│  0-100 100-200 ...                         900-1000           │
│                                                               │
│  当前时间 1500ms → 统计窗口 = 500ms ~ 1500ms                 │
│  → 遍历 5-9 格（有效窗口）合并统计 → 总请求数                │
│                                                               │
│  10 格窗口 = 10 个时间小格子 × 100ms = 1s 总窗口            │
│  → 任何时刻统计都是最近 1s 的请求（准确，无边界问题）         │
│
│ ★ Sentinel QPS 限流源码完整链路：                               │
│   请求 → FlowSlot.entry()                                    │
│        → canPassByQps(rule, node)                            │
│        → LeapArray.currentWindow() + getWindowData()         │
│        → 统计最近 1s 的请求数                                 │
│        → 如果 < threshold → 通过                              │
│        → 如果 ≥ threshold → 拒绝（FlowException）             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Dubbo LoadBalance 负载均衡源码级

### 3.1 Dubbo 五种负载均衡策略

```java
// ★ Dubbo 3.x LoadBalance 负载均衡源码级（面试高频！）
//
// // org/apache/dubbo/rpc/cluster/loadbalance/
//
// public class LoadBalanceFactory {
//     // ★ Dubbo 五种负载均衡策略（面试必背！）
//     public static final String RANDOM = "random";              // ① 随机（默认）
//     public static final String ROUND_ROBIN = "roundrobin";    // ② 轮询
//     public static final String LEAST_ACTIVE = "leastactive";  // ③ 最少活跃数
//     public static final String CONSISTENT_HASH = "consistenthash"; // ④ 一致性哈希
//     public static final String SHORTEST_RESPONSE = "shortestresponse"; // ⑤ 最短响应
// }
//
// // ★ ① RandomLoadBalance（默认，源码级）
// public class RandomLoadBalance implements LoadBalance {
//     public Invoker doSelect(List<Invoker> invokers, URL url, Invocation invocation) {
//         // 简单的加权随机
//         int totalWeight = invokers.stream().mapToInt(i -> getWeight(i)).sum();
//         Random random = new Random();
//         int offset = random.nextInt(totalWeight);
//
//         for (Invoker invoker : invokers) {
//             offset -= getWeight(invoker);
//             if (offset <= 0) {
//                 return invoker;
//             }
//         }
//         return invokers.get(invokers.size() - 1);
//     }
// }
//
// // ★ ② RoundRobinLoadBalance（加权轮询）
// public class RoundRobinLoadBalance implements LoadBalance {
//     // 轮询算法：所有 Invoker 维护一个 currentWeight
//     // 每次选 weight 最大的那个
//     public Invoker doSelect(...) {
//         // Nginx 加权轮询算法（平滑加权轮询）
//         // 每个 Invoker 有 currentWeight
//         // 每次选择后 currentWeight -= totalWeight / weight
//     }
// }
//
// // ★ ③ LeastActiveLoadBalance（最少活跃数）
// public class LeastActiveLoadBalance implements LoadBalance {
//     // 选择活跃请求数最少的 Invoker
//     public Invoker doSelect(...) {
//         // activeCount 来自 RpcStatus.getActive()
//         return invokers.stream()
//             .min(Comparator.comparingInt(i -> RpcStatus.getActive(i)))
//             .orElse(invokers.get(0));
//     }
// }
//
// // ★ ④ ConsistentHashLoadBalance（一致性哈希）
// public class ConsistentHashLoadBalance implements LoadBalance {
//     // 按 Invoker + VirtualNode 构建哈希环
//     // 调用方根据 invocation.getParameter() 选 Key
//     // → 同一个 Key 总是路由到同一个 Invoker
// }
//
```

### 3.2 Dubbo Cluster 容错策略

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Dubbo 十种集群容错策略（面试高频！）                        │
│                                                               │
│  ┌──────────┬──────────────┬──────────────────────────────┐ │
│  │ 策略       │ 核心机制       │ 适用场景                       │ │
│  ├──────────┼──────────────┼──────────────────────────────┤ │
│  │ Failover  │ 失败自动切换    │ 默认；读写都可用                │ │
│  │ Failfast  │ 失败立即报错    │ 写操作（不想重复执行）            │ │
│  │ Failsafe  │ 失败忽略       │ 心跳/日志等不重要的             │ │
│  │ Failback  │ 失败异步重试    │ 通知类服务                     │ │
│  │ Forking   │ 并行多个 Invoker │ 实时性要求高                   │ │
│  │ Broadcast │ 广播所有 Invoker │ 通知所有节点（配置中心）        │ │
│  │ Available │ 选第一个可用的   │ 简单场景                     │ │
│  │ Mergeable │ 合并多个结果    │ MapReduce 类型                │ │
│  │ RegistryAware │ 注册中心感知 │ 动态服务发现                 │ │
│  │ ThreadLocalReferencedCache │ 本地缓存   │ 缓存服务                     │ │
│  └──────────┴──────────────┴──────────────────────────────┘ │
│
│ ★ Dubbo Cluster + LoadBalance + Failover 完整链路：          │
│   Consumer → Failover Cluster                                │
│            → Random LoadBalance（选 Provider）                │
│            → 调用 Provider                                    │
│            → 失败？→ 自动切换下一个 Provider（最多 retries 次）│
│            → 全部失败？→ 抛出 Exception                      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Sentinel FlowSlot 核心？| **ProcessorSlotChain 责任链**；FlowSlot 是限流 Slot；四种模式（QPS/线程数/匀速排队/系统自适应）|
| 2 | LeapArray 滑动窗口？| **N 格环形数组 × intervalInMs/sampleCount**；遍历有效窗口合并统计 → 无边界突发 |
| 3 | LeapArray 核心方法？| **currentWindow()**（找当前窗口）+ **getWindowData()**（遍历合并）+ **isTimeOut()**（过期检查）|
| 4 | Dubbo 五种负载均衡？| **Random（默认）+ RoundRobin + LeastActive + ConsistentHash + ShortestResponse** |
| 5 | Dubbo 十种容错？| **Failover（默认）+ Failfast + Failsafe + Failback + Forking + Broadcast + Available + Mergeable + RegistryAware + ThreadLocalReferencedCache** |
| 6 | Failover vs Failfast？| Failover：失败自动切换（读操作）；Failfast：失败立即报错（写操作，避免重复执行）|
| 7 | LeastActive 负载均衡？| **选择活跃请求数最少的 Invoker**（RpcStatus.getActive()）|
| 8 | 一致性哈希负载均衡？| **按参数 Key 哈希环路由**；相同 Key 总是到同一个节点；避免缓存穿透 |
| 9 | 面试最高频考点排序？| **1. LeapArray 滑动窗口 2. FlowSlot 四种模式 3. Dubbo 五种负载均衡 4. 容错策略 5. Failover 链路** |
| 10 | 学习路径？| FlowSlot → LeapArray → RandomLoadBalance → RoundRobinLoadBalance → FailoverCluster |
| 11 | 面试常见追问？| Sentinel LeapArray 为什么准？→ 10 格窗口无边界突发；Dubbo Failover 重试几次？→ 默认 2 次 |
| 12 | Sentinel 规则类型？| FlowRule（限流）+ DegradeRule（熔断）+ SystemRule（系统）+ ParamFlowRule（参数限流）|
| 13 | Dubbo 负载均衡源码路径？| org/apache/dubbo/rpc/cluster/loadbalance/ |
| 14 | Sentinel vs Guava RateLimiter？| Sentinel：滑动窗口 + 四种模式 + Dashboard；Guava：令牌桶 + 简单 |
| 15 | 2024 新特性？| Sentinel 1.8+ **Adaptive Sentinel**（自适应限流）+ Dubbo 3.x **Triple（gRPC 协议）** |
| 16 | 面试高频场景？| 秒杀（Sentinel 限流 + Dubbo LeastActive）+ 高并发网关（Failover 容错）|
| 17 | Dubbo 权重怎么设？| provider.weight 参数；Random 和 RoundRobin 都支持加权 |
| 18 | Sentinel Dashboard 功能？| 实时监控 + 规则配置 + 集群模式 + 流控效果实时显示 |
| 19 | Dubbo Cluster 容错源码？| FailoverClusterInvoker + FailfastClusterInvoker |
| 20 | 面试陷阱？| 说 Sentinel 是令牌桶 → ❌ Sentinel 默认用滑动窗口 LeapArray |
| 21 | LeapArray vs 令牌桶 vs 漏桶？| LeapArray 无突发；令牌桶允许突发；漏桶绝不突发 |
| 22 | Sentinel 集群模式？| 多个实例共享限流规则 + Dashboard 统一管理 + ClusterSlot 统计全局流量 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：slots/block/flow/FlowSlot.java + slots/statistic/LeapArray.java  
**面试考点：22 题**  
**差异化点**：LeapArray 10 格滑动窗口 + Dubbo 五种负载均衡 + 十种容错策略
