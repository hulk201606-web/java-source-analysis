# Resilience4j 熔断降级源码深度解析

> **源码基线**：Resilience4j 1.7.1 + 1.8.0（Spring Boot 2.7.x 配套）  
> **核心仓库**：https://github.com/resilience4j/resilience4j  
> **面试权重**：⭐⭐⭐⭐（P2 组件 · Hystrix 替代方案）

---

## 一、Resilience4j 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  和 Sentinel / Hystrix 定位对比                                │
│                                                               │
│  ┌────────────┬──────────────┬──────────────┬─────────────┐  │
│  │   功能      │ Resilience4j │  Sentinel   │   Hystrix    │  │
│  ├────────────┼──────────────┼──────────────┼─────────────┤  │
│  │ 熔断降级    │ ✅ 核心功能   │ ✅ 核心功能   │ ✅ 核心功能   │  │
│  │ 流量控制    │ ❌ 无         │ ✅ QPS/线程数  │ ❌ 无        │  │
│  │ 热点参数    │ ❌ 无         │ ✅ 核心功能   │ ❌ 无        │  │
│  │ 系统保护    │ ❌ 无         │ ✅ 核心功能   │ ❌ 无        │  │
│  │ 线程隔离    │ ✅ Bulkhead  │ ✅ 线程隔离   │ ✅ 线程隔离   │  │
│  │ 信号量隔离  │ ✅ Bulkhead  │ ❌ 无         │ ✅ 信号量隔离 │  │
│  │ 重试        │ ✅ Retry     │ ❌ 无         │ ✅           │  │
│  │ 限流        │ ✅ RateLimiter│ ❌ 无         │ ❌ 无        │  │
│  │ 轻量级      │ ✅ 仅熔断隔离  │ ❌ 全功能框架  │ ❌ 重         │  │
│  │ 控制台      │ ❌ 无         │ ✅ Dashboard  │ ❌ Turbine   │  │
│  └────────────┴──────────────┴──────────────┴─────────────┘  │
│                                                               │
│ ★ Resilience4j 定位：轻量级熔断库（只做熔断/隔离/重试/限流）    │
│ ★ Sentinel 定位：全功能流控框架（熔断+限流+热点+系统保护+控制台）│
│ ★ 生产推荐：**Sentinel**（功能最全）或 **Resilience4j**（轻量无控制台依赖）│
└─────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────┐
│  五大核心模块                                                  │
│                                                               │
│  ① CircuitBreaker（★ 熔断状态机）                              │
│     → 滑动窗口统计（time-based / count-based）                │
│     → 三状态：CLOSED → OPEN → HALF_OPEN                       │
│                                                               │
│  ② Bulkhead（隔离）                                           │
│     → ThreadPoolBulkhead：每个熔断独立线程池（真正线程隔离）    │
│     → SemaphoreBulkhead：信号量隔离（总并发数限制）             │
│                                                               │
│  ③ RateLimiter（限流 · 令牌桶）                                │
│     → fillRate（每秒补充令牌数）                               │
│     → limitForPeriod（每个周期令牌上限）                        │
│                                                               │
│  ④ Retry（重试）                                              │
│     → maxAttempts + waitDuration + retryExceptions             │
│                                                               │
│  ⑤ Fallback（降级）                                           │
│     → @CircuitBreaker(fallback = "methodName")                │
│                                                               │
│  ★ 叠加使用：circuitBreaker + bulkhead + retry 组合            │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 CircuitBreaker 六状态（★★★★★ 面试必问）

```java
// ★ Resilience4j CircuitBreaker 有 6 种状态！比 Sentinel 多 3 个
//   （Sentinel 只有 CLOSED / OPEN / HALF_OPEN 3 个）
//
// ┌────────────────────────────────────────────────────────────┐
// │                                                              │
// │  ① CLOSED（正常）                                            │
// │     → 所有请求正常通过 + 滑动窗口统计 success/failure         │
// │     → 触发 OPEN 条件：                                       │
// │       failureRate >= failureRateThreshold（默认 50%）        │
// │       或 slowCallRate >= slowCallRateThreshold               │
// │       且 slidingWindowPassed >= minimumNumberOfCalls         │
// │                                                              │
// │  ② OPEN（熔断打开）                                          │
// │     → 所有请求直接抛 CallNotPermittedException               │
// │     → 等待 waitDurationInOpenState（默认 60s）               │
// │     → 到期后转 HALF_OPEN                                     │
// │                                                              │
// │  ③ HALF_OPEN（探测）                                         │
// │     → 允许 maxPermittedNumberOfCallsInHalfOpenState 个请求   │
// │       探测（默认 10 个）                                     │
// │     → 探测全部成功 → CLOSED                                  │
// │     → 探测中有 failure → 回到 OPEN（重新计时）               │
// │                                                              │
// │  ④ DISABLED（特殊状态：禁用熔断）                              │
// │     → 永远不触发熔断，所有请求正常通过                        │
// │     → CircuitBreakerConfig.disabled(true) 配置               │
// │                                                              │
// │  ⑤ FORCED_OPEN（特殊状态：强制熔断）                           │
// │     → 永远熔断，所有请求抛 CallNotPermittedException         │
// │     → 测试时用（模拟熔断场景）                                │
// │                                                              │
// │  ⑥ METRICS_ONLY（特殊状态：只统计不拦截）                      │
// │     → 正常通过请求但收集 metrics 数据                         │
// │     → 生产监控用（看统计但不熔断）                             │
// │                                                              │
// └────────────────────────────────────────────────────────────┘
```

### 2.2 CircuitBreakerImpl 状态切换

```java
// ★ CircuitBreakerImpl 是 CircuitBreaker 的核心实现
//   状态切换由状态机管理（用枚举 + AtomicReference）
//
// 关键字段：
//   AtomicReference<State> state          → 当前状态（原子切换防止并发）
//   CircuitBreakerConfig config           → 配置（阈值、时间窗口等）
//   SlidingWindow slidingWindow           → 滑动窗口（统计 success/failure）
//   long transitionToOpenAtMillis         → OPEN 状态开始时间（用于计算到期）
//
// tryAcquirePermission()（★ 请求进来第一步）：
//   State current = state.get();
//   switch (current) {
//     case CLOSED:
//       // 检查滑动窗口是否触发熔断
//       if (isFailureRateExceeded() || isSlowCallRateExceeded()) {
//         transitionToOpen();  // → OPEN
//         return false;
//       }
//       return true;  // 允许通过
//     case OPEN:
//       // 检查 waitDuration 是否到期
//       if (System.currentTimeMillis() - transitionToOpenAtMillis
//             >= config.getWaitDurationInOpenState().toMillis()) {
//         transitionToHalfOpen();  // → HALF_OPEN
//         return true;  // 允许探测通过
//       }
//       return false;  // 还在冷却期，拒绝
//     case HALF_OPEN:
//       // 检查探测配额
//       if (halfOpenCalls.incrementAndGet()
//             > config.getMaxPermittedNumberOfCallsInHalfOpenState()) {
//         return false;  // 配额用完，拒绝
//       }
//       return true;  // 允许探测
//     case DISABLED:
//       return true;   // 永远放行
//     case FORCED_OPEN:
//     case METRICS_ONLY:
//       return false;  // 永远拒绝
//   }
//
// 注意：transitionToXxx() 用 AtomicReference.compareAndSet 原子切换
//   → 防止多线程同时触发状态切换
//   → Sentinel 的 tryPass 也是类似 CAS 原子操作
```

### 2.3 SlidingWindow（滑动窗口统计）

```java
// ★ Resilience4j SlidingWindow 两种模式
//
// 1. CountBasedSlidingWindow（基于调用次数）
//    → 最近 N 次调用（默认 100）
//    → 只保留最后 N 个调用结果
//    → 优点：简单；缺点：不考虑时间
//
// 2. TimeBasedSlidingWindow（基于时间，★ 推荐默认！）
//    → 最近 T 秒内的调用（默认 60s）
//    → 内部用 Ring Buffer（数组 + Mod 索引）
//    → 每个 Bucket 代表一个时间片段（1s 一个）
//    → total buckets = slidingWindowDurationSeconds
//
// SlidingWindow 统计内容：
//   → successCount（成功次数）
//   → failureCount（失败次数）
//   → slowCallCount（慢调用次数）
//   → totalCount（总调用次数）
//   → failureRate = failureCount / totalCount
//   → slowCallRate = slowCallCount / totalCount
//
// ★ 和 Sentinel LeapArray 的区别：
//   Sentinel LeapArray：60 个 1s 窗口，每个窗口有独立 MetricBucket
//   Resilience4j SlidingWindow：同样 Ring Buffer 但实现更简洁（Resilience4j 不区分 pass/block/success/exception 四种计数器，只区分 success/failure）
```

### 2.4 Bulkhead 隔离（★ 两种模式）

```java
// ★ Bulkhead 解决熔断后的资源耗尽问题
//   如果不隔离 → 某个慢服务导致整个线程池被占满 → 级联故障
//
// 1. ThreadPoolBulkhead（★ 线程隔离，和 Hystrix 类似）
//    → 每个熔断方法有独立线程池
//    → maxThreadPoolSize（最大线程数）+ queueCapacity（队列）
//    → 优点：真正隔离（慢服务线程不影响主业务）
//    → 缺点：线程开销大（每个熔断一个线程池）
//
// 2. SemaphoreBulkhead（信号量隔离，默认）
//    → 总并发数限制（permits 个令牌）
//    → 优点：轻量（不创建新线程）
//    → 缺点：没有真正隔离（慢调用占住令牌阻塞主线程）
//
// 用法：
//   @Bulkhead(name = "orderService", type = Bulkhead.Type.THREADPOOL)
//   public Order getOrder(Long id) { ... }
//
//   // ThreadPoolBulkhead 自动用独立线程池执行
//   // 主业务线程不会被阻塞
```

### 2.5 Retry 重试

```java
// ★ Retry 配置 + CircuitBreaker 叠加
//
// CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
//     .failureRateThreshold(50)          // 失败率 50% 触发熔断
//     .waitDurationInOpenState(Duration.ofSeconds(30))  // OPEN 后等 30s
//     .slidingWindowType(SlidingWindowType.TIME_BASED)  // 时间窗口
//     .slidingWindowSize(60)             // 窗口大小 60s
//     .minimumNumberOfCalls(10)           // 至少 10 次调用才判断
//     .permittedNumberOfCallsInHalfOpenState(5)  // HALF_OPEN 放 5 个探测
//     .build();
//
// RetryConfig retryConfig = RetryConfig.custom()
//     .maxAttempts(3)                    // 最多 3 次（原调用 + 2 次重试）
//     .waitDuration(Duration.ofMillis(500))  // 重试间隔 500ms
//     .retryExceptions(RuntimeException.class)
//     .build();
//
// // 组合使用：先重试 → 重试全失败再触发熔断
// CircuitBreaker circuitBreaker = CircuitBreaker.of("orderService", circuitBreakerConfig);
// Retry retry = Retry.of("orderService", retryConfig);
//
// Supplier<String> decoratedSupplier = CircuitBreaker.decorateSupplier(
//     retry.decorateSupplier(originalSupplier), circuitBreaker);
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Resilience4j 定位？| **轻量级熔断库**（只做熔断/隔离/重试/限流）→ Sentinel 是全功能流控框架（还有流量控制/热点参数/系统保护/控制台）|
| 2 | CircuitBreaker 六状态？| **CLOSED / OPEN / HALF_OPEN（正常三状态）+ DISABLED / FORCED_OPEN / METRICS_ONLY（特殊三状态）** → Sentinel 只有前三个 |
| 3 | OPEN 转 HALF_OPEN 条件？| **waitDurationInOpenState 到期**（默认 60s）→ 不是固定次数，是时间触发 |
| 4 | HALF_OPEN 最多放行几个？| **maxPermittedNumberOfCallsInHalfOpenState**（默认 10 个）→ 全部成功才回 CLOSED，一个失败回到 OPEN |
| 5 | 熔断触发条件？| **failureRate >= threshold（默认 50%）或 slowCallRate >= threshold** → 且 totalCall >= minimumNumberOfCalls（默认 10）|
| 6 | SlidingWindow 两种模式？| **CountBased（最近 N 次调用，默认 100）/ TimeBased（最近 T 秒，默认 60s）** → 推荐 TimeBased（更贴合实际时间窗口）|
| 7 | Bulkhead 隔离两种？| **ThreadPoolBulkhead（线程隔离，真正隔离但线程开销大）/ SemaphoreBulkhead（信号量隔离，轻量但无真正隔离）**|
| 8 | CircuitBreaker 和 Retry 叠加顺序？| **Retry 先 → CircuitBreaker 后** → 重试全失败再触发熔断（避免一次临时失败就熔断）|
| 9 | 和 Hystrix 区别？| Resilience4j：轻量 + 状态机六状态 + 模块化；Hystrix：重 + 线程池隔离为主 + 已停更 |
| 10 | 和 Sentinel 区别？| Sentinel 功能最全（限流+熔断+热点+系统保护+控制台）；Resilience4j 轻量（只做熔断/隔离/重试）|
| 11 | 线程隔离 vs 信号量隔离？| 线程隔离：每个熔断独立线程池 → 真正隔离（主业务不被阻塞）→ 线程开销大；信号量隔离：总并发数限制 → 无隔离 → 轻量 |
| 12 | RateLimiter 令牌桶参数？| **limitForPeriod（每个周期令牌上限）+ limitRefreshPeriod（补充周期）** → 例：每秒 100 个令牌 = QPS 100 |
| 13 | Resilience4j 和 Spring Cloud 集成？| spring-cloud-circuitbreaker-resilience4j → @CircuitBreaker(name, fallback) → AOP 切面自动包装方法 |
| 14 | 多 CircuitBreaker 共享？| CircuitBreakerRegistry → 可以配置全局默认配置 + 每个熔断方法独立配置 |
| 15 | HALF_OPEN 探测失败会怎么样？| **回到 OPEN 状态**（不是 HALF_OPEN 永远探测）→ 重新等 waitDurationInOpenState 后再进入 HALF_OPEN |

---

## 四、核心类速查表

| 类 | 职责 | 关键方法 |
|----|------|---------|
| **CircuitBreakerImpl** | 熔断核心实现 | tryAcquirePermission / onSuccess / onError |
| **CircuitBreakerConfig** | 熔断配置 | failureRateThreshold / waitDurationInOpenState / slidingWindowSize |
| **State** | 六状态枚举 | CLOSED / OPEN / HALF_OPEN / DISABLED / FORCED_OPEN / METRICS_ONLY |
| **SlidingWindow** | 滑动窗口统计 | call / onSuccess / onError |
| **TimeBasedSlidingWindow** | 时间窗口实现 | Ring Buffer + Bucket |
| **CountBasedSlidingWindow** | 次数窗口实现 | 环形队列 + 固定容量 |
| **Bulkhead** | 隔离接口 | acquire/tryAcquire |
| **ThreadPoolBulkhead** | 线程隔离 | ExecutorService 独立线程池 |
| **SemaphoreBulkhead** | 信号量隔离 | Semaphore permits |
| **Retry** | 重试 | maxAttempts / waitDuration / retryExceptions |
| **RateLimiter** | 令牌桶限流 | limitForPeriod / limitRefreshPeriod |
| **CircuitBreakerRegistry** | CircuitBreaker 管理 | find / create / getAll |

---

**文档版本：v1.0 · 2026-10-21**  
**源码基线**：Resilience4j 1.7.1 + 1.8.0  
**面试考点：15 题**  
**状态**：⭐ P2 组件（Resilience4j）完成
