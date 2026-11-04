# Spring Cloud LoadBalancer 源码深度解析（Ribbon 官方替代）

> **源码基线**：Spring Cloud LoadBalancer 3.1.8（Spring Boot 2.7.x 配套）  
> **核心仓库**：https://github.com/spring-cloud/spring-cloud-commons  
> **面试权重**：⭐⭐⭐⭐  
> **版本说明**：Spring Cloud 2020+（Ilford）**默认弃用 Ribbon**，切换为 Spring Cloud LoadBalancer 内置；OpenFeign / Gateway / RestTemplate 自动集成

---

## 一、LoadBalancer 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  和 Ribbon 定位对比                                           │
│                                                               │
│  ┌──────────┬──────────────────────┬──────────────────────┐ │
│  │   特性    │   Ribbon（Netflix）   │ Spring Cloud LoadBalancer │
│  ├──────────┼──────────────────────┼──────────────────────┤ │
│  │ 线程模型  │ 同步阻塞              │ ★ Reactive 响应式       │ │
│  │ 协议支持  │ 仅 HTTP              │ HTTP + WebFlux Mono 风格│ │
│  │ 内置实现  │ 7 种 IRule           │ 3 种（可扩展）           │ │
│  │ Zone感知  │ ZoneAvoidanceRule    │ ZoneBasedLoadBalancer   │ │
│  │ 粘性会话  │ 需自定义             │ ★ 内置 RequestBasedSticky│ │
│  │ 权重      │ WeightedResponseTime │ WeightedLoadBalancer    │ │
│  │ Spring Cloud│ 2020+ 官方弃用     │ ★ 2020+ 默认启用        │ │
│  └──────────┴──────────────────────┴──────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  核心 API 体系                                                 │
│                                                               │
│  ReactiveLoadBalancerClient（Gateway 用）                      │
│    ↓                                                          │
│  LoadBalancerClient（RestTemplate / Feign 用）                 │
│    ↓                                                          │
│  ReactiveLoadBalancer<ServiceInstance>（核心接口）             │
│    choose(request) → Response<ServiceInstance>               │
│                                                               │
│  ★ 三种内置实现（比 Ribbon 7 种少，但更简洁高效）：            │
│    ① RoundRobinLoadBalancer ★ 默认！轮询                       │
│    ② ZoneBasedLoadBalancer Zone 感知（多机房推荐）             │
│    ③ WeightedLoadBalancer 权重随机（按 MetaData 权重）         │
│                                                               │
│  ★ 两种附加能力：                                               │
│    ① RequestBasedStickySession 粘性会话（Cookie 记忆实例）     │
│    ② SameInstancePreference 优先选上次选中的实例               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 LoadBalancerClient 选择实例（RestTemplate / Feign 用）

```java
// ★ LoadBalancerClientFactory
//   → 每个服务名一个 LoadBalancer 实例（按 serviceId 缓存）
//   → 和 Ribbon 的 SpringClientFactory 逻辑完全一样

// LoadBalancerClient.choose("order-service") 流程：
//
//   1. LoadBalancerClientFactory.getInstance(serviceId, ReactiveLoadBalancer.class)
//      → 返回 RoundRobinLoadBalancer（默认）
//
//   2. ReactiveLoadBalancer.choose(DefaultRequest.create(request))
//      ↓
//      RoundRobinLoadBalancer.choose(request)
//        → ServiceList.getServerList() → 从 DiscoveryClient 拿实例列表
//        → 轮询逻辑：AtomicLong currentIndex
//          index = currentIndex.getAndIncrement() % instances.size()
//          → 返回 instances.get(index)
//        → 比 Ribbon RoundRobinRule 更简洁！
//
//   3. 返回 ServiceInstance（host, port, metadata）
//      → RestTemplate / Feign 用这个拼接真实 URL
```

### 2.2 RoundRobinLoadBalancer 轮询核心

```java
// ★ RoundRobinLoadBalancer 核心实现（比 Ribbon 更简洁）
//
// 关键字段：
//   AtomicLong position → 当前轮询位置（线程安全）
//   List<ServiceInstance> instances → 当前服务实例列表
//
// choose(request) 流程：
//   1. getReachableInstances(request) → 过滤：只留健康 + Zone 匹配的
//   2. if (instances.isEmpty()) → 返回 empty
//   3. int pos = (int) Math.abs(position.getAndIncrement()) % instances.size()
//   4. return instances.get(pos)
//
// ★ 和 Ribbon RoundRobinRule 对比：
//   Ribbon：AtomicInteger nextServerIndex → 复杂一些，还要处理 null serverList
//   LB：AtomicLong position → 更简洁，直接用 List.size() 取模
//
// ★ LB 自动做健康过滤！Ribbon RoundRobinRule 默认不做（要 ZoneAvoidanceRule 才做）
```

### 2.3 ZoneBasedLoadBalancer（多机房推荐）

```java
// ★ ZoneBasedLoadBalancer = Zone 感知 + RoundRobin
//
// choose(request) 流程：
//   1. 从当前 ServiceInstance 的 metadata 取 zone（如 zone=cn-east-1）
//   2. 优先选同 zone 的实例
//   3. 如果同 zone 全挂了 → fallback 到其他 zone
//   4. zone 内用 RoundRobinLoadBalancer 轮询
//
// ★ 等价于 Ribbon 的 ZoneAvoidanceRule
//   但实现更简洁（内部包装 RoundRobinLoadBalancer）
```

### 2.4 粘性会话（RequestBasedStickySession）

```java
// ★ LB 内置粘性会话！Ribbon 要自己写
//
// 配置：
//   spring.cloud.loadbalancer.sticky-session.enabled=true
//   spring.cloud.loadbalancer.sticky-session.add-service-instance-cookie=true
//
// 原理：
//   1. 第一次请求：RoundRobin 选一个实例 → Cookie 记录选中实例 ID
//      → Cookie 名：sc-lb-instance-id（可配置）
//   2. 后续请求：从 Cookie 读 instanceId → 优先选这个实例
//   3. 选中的实例挂了 → 跳过粘性，重新 RoundRobin 选新的
//
// ★ 秒杀 / 会话亲和场景必备！不需要自己写拦截器
```

### 2.5 RestTemplate / Feign 自动集成

```java
// ★ 和 Ribbon 集成方式几乎一样
//
// RestTemplate：
//   @Bean @LoadBalanced RestTemplate restTemplate() { ... }
//   → LoadBalancerAutoConfiguration 注入 LoadBalancerInterceptor
//   → 拦截请求 → LoadBalancerClient.choose → 改写 URL → 执行
//
// OpenFeign：
//   FeignClient → LoadBalancerFeignClient（或 ReactiveLoadBalancerFeignClient）
//   → 内部 LoadBalancerClient.choose → 选实例 → 执行请求
//
// Gateway：
//   ReactiveLoadBalancerClientFilter
//   → lb://order-service → LoadBalancerClient.choose → http://ip:port
//
// ★ 如果同时引了 Ribbon + LB？
//   Spring Cloud 2020+ 默认禁用 Ribbon（spring.cloud.loadbalancer.ribbon.enabled=false）
//   要回退 Ribbon：spring.cloud.loadbalancer.ribbon.enabled=true
```

---

## 三、面试考点（12 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | LB vs Ribbon？| LB 是 Spring Cloud Commons 内置（Reactive 支持更好）；Ribbon Netflix 出品（已逐步弃用）；LB 默认在 Spring Cloud 2020+ 启用 |
| 2 | LB 默认轮询算法？| **RoundRobinLoadBalancer** → AtomicLong position → getAndIncrement() % instances.size() |
| 3 | 三种内置实现？| **RoundRobinLoadBalancer（默认）/ ZoneBasedLoadBalancer（多机房）/ WeightedLoadBalancer（权重随机）** |
| 4 | LB 自动健康过滤？| 是的！RoundRobinLoadBalancer 内部 getReachableInstances 自动过滤不健康实例 + Zone 不匹配实例 |
| 5 | 粘性会话？| **RequestBasedStickySession** → Cookie(sc-lb-instance-id) 记忆实例 → 后续请求优先选同一个；实例挂了自动切换 |
| 6 | Gateway 怎么用 LB？| ReactiveLoadBalancerClientFilter → ReactiveLoadBalancer.choose → Mono<ServiceInstance> → 改写 lb:// 为 http://ip:port |
| 7 | RestTemplate 怎么切 LB？| 引 spring-cloud-starter-loadbalancer（排除 ribbon）→ @LoadBalanced RestTemplate → 自动用 LB |
| 8 | 自定义 LB 配置？| @LoadBalancerClient(name = "order-service", configuration = CustomConfig.class) → 每个服务不同策略 |
| 9 | SameInstancePreference？| 上次选的实例优先 → 适合会话亲和性（和粘性会话类似但不写 Cookie）|
| 10 | LB 的 Reactive 优势？| Gateway 用 WebFlux（Reactive）→ LB 也用 Mono/Flux → 全程响应式非阻塞；Ribbon 只能同步 |
| 11 | 多机房配置？| ZoneBasedLoadBalancer + ServiceInstance.metadata.zone=cn-east-1 → 优先同 zone，fallback 跨 zone |
| 12 | 回退 Ribbon？| spring.cloud.loadbalancer.ribbon.enabled=true + 排除 spring-cloud-starter-loadbalancer → 继续用 Ribbon |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Spring Cloud LoadBalancer 3.1.8（Spring Cloud Commons 内置）  
**面试考点：12 题**
