# Spring Cloud Ribbon 客户端负载均衡源码解析

> **源码基线**：Spring Cloud Netflix Ribbon 2021.0.8（Spring Boot 2.7.x 配套）  
> **核心仓库**：Netflix/ribbon + spring-cloud/spring-cloud-netflix  
> **面试权重**：⭐⭐⭐⭐（P0 组件补漏）  
> **版本说明**：Spring Cloud 2021+ 官方逐步弃用 Ribbon，推荐 **Spring Cloud LoadBalancer** 替代，但 Ribbon 仍被 OpenFeign/Gateway 默认集成

---

## 一、Ribbon 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心概念（四件套）                                           │
│  LoadBalancer → ServerList → IRule → IPing                    │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  三种使用方式                                                  │
│                                                               │
│  ① RestTemplate（手动 + @LoadBalanced 注解）                    │
│     RestTemplate.getForObject("http://order-service/orders") │
│     → Interceptor → LoadBalancerClient.choose("order-service")│
│                                                               │
│  ② OpenFeign（自动集成）                                       │
│     FeignClient → LoadBalancerFeignClient                     │
│     → LoadBalancerClient.choose                               │
│                                                               │
│  ③ Gateway（ReactiveLoadBalancerClientFilter）                 │
│     → ReactiveLoadBalancerClientFilter.choose                 │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  LoadBalancerClient.choose("order-service")                   │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 1. SpringClientFactory.getLoadBalancer("order-service")│  │
│  │    → 返回 ZoneAwareLoadBalancer（默认）                  │  │
│  │                                                         │  │
│  │ 2. ZoneAwareLoadBalancer.chooseServer(key)             │  │
│  │    ├─ ServerList 拉取服务列表（DiscoveryClient → Nacos） │  │
│  │    ├─ IPing 健康检查（PingUrl / NoOpPing）              │  │
│  │    ├─ ServerListFilter 过滤（ZoneAvoidancePredicate）    │  │
│  │    └─ IRule.choose(key, serverList) → ★ 选一个 Server   │  │
│  │                                                         │  │
│  │ 3. 返回 Server（host:port）→ 拼接真实 URL 执行请求       │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 LoadBalancerClient.choose → ZoneAwareLoadBalancer.chooseServer

```java
// ★ LoadBalancerClient（Spring Cloud 抽象接口）
//   Spring Cloud Netflix 默认实现：RibbonLoadBalancerClient
//   Spring Cloud Commons 也有 ReactiveLoadBalancer（Gateway 用）
//
// RibbonLoadBalancerClient.choose("order-service") 流程：
//
//   1. SpringClientFactory.getLoadBalancer("order-service")
//      → 每个服务名对应一个 LoadBalancer 实例（不共享）
//      → LoadBalancer 内部维护：serverList + rule + ping
//
//   2. ZoneAwareLoadBalancer.chooseServer(key)
//      ↓
//      // ① ServerList 获取服务列表
//      List<Server> allServers = serverList.getUpdatedListOfServers()
//        → DiscoveryEnabledNIWSServerList（默认）
//          → DiscoveryClient.getInstances("order-service")
//          → 底层用 Nacos / Eureka 拉取注册实例
//      // 定时刷新：DynamicServerListLoadBalancer
//        → 每 30 秒（DefaultServerListRefreshInterval）刷新一次
//
//      // ② IPing 健康检查
//      boolean isAlive = ping.isAlive(server)
//        → 默认 NoOpPing（不检查，返回 true）
//        → 生产改 PingUrl（发 HTTP HEAD 检查 /actuator/health）
//
//      // ③ ServerListFilter 过滤
//      List<Server> filtered = filter.filter(allServers)
//        → 默认 ZoneAvoidancePredicate（过滤掉 Zone 外的实例）
//        → 还能过滤 ConnectionLimit（连接数超标的）
//
//      // ④ IRule 选一个 ★ 核心！
//      Server chosen = rule.choose(key, filtered)
//
//   3. 返回 Server
//      → RestTemplate / Feign 用这个 Server 的 host:port 发请求
```

### 2.2 IRule 七种策略（★★★★★ 面试必问）

```java
// ★ IRule 是负载均衡策略接口
//   choose(key, serverList) → Server
//   Spring Cloud 默认用 ZoneAvoidanceRule（不是 RoundRobinRule！）
//
// ┌──────────────────────────────────────────────────────────────┐
// │  IRule 七种实现（★ 按推荐级别排序）                              │
// ├──────────────────────────────────────────────────────────────┤
// │                                                              │
// │  1. ZoneAvoidanceRule（★★★★★ 生产默认 + 推荐！）             │
// │     → 先过滤：排除 Zone 故障的实例 + 排除连接过多的实例        │
// │     → 再选：RoundRobinRule（轮询）                              │
// │     → 支持多 Zone 故障转移（本 Zone 全挂了 → 跨 Zone 选）       │
// │                                                              │
// │  2. BestAvailableRule（★★★★ 高并发推荐）                      │
// │     → 遍历所有 Server → 选 active request count 最小的        │
// │     → 哪个实例连接数最少就选哪个                                │
// │     → 自动避开慢实例                                          │
// │                                                              │
// │  3. WeightedResponseTimeRule（★★★★ 自适应权重）                │
// │     → 根据每个 Server 的平均 RT 动态计算权重                   │
// │     → RT 越小 → 权重越大 → 越容易被选中                        │
// │     → 每隔 30 秒重新计算权重                                   │
// │                                                              │
// │  4. AvailabilityFilteringRule（★★★ 过滤 + 轮询）               │
// │     → 先过滤：排除连接数超标的实例 + 熔断的实例                 │
// │     → 再 RoundRobinRule 轮询                                  │
// │     → 和 ZoneAvoidanceRule 类似但不考虑 Zone                  │
// │                                                              │
// │  5. RoundRobinRule（★★ 最简单轮询）                            │
// │     → 线性轮询 serverList（线程安全 AtomicInteger）             │
// │     → 没有任何过滤！生产不推荐（无健康检查 + 无熔断感知）        │
// │                                                              │
// │  6. RandomRule（★ 随机）                                      │
// │     → ThreadLocalRandom.nextInt(serverList.size())             │
// │     → 简单但不平均（某些实例可能被多次选中）                    │
// │                                                              │
// │  7. RetryRule（★★ 容错重试）                                  │
// │     → 内部包装另一个 IRule（默认 RoundRobinRule）              │
// │     → 选 Server → 失败 → 在超时时间内重试换另一个 Server        │
// │     → 避免偶发故障                                            │
// │                                                              │
// └──────────────────────────────────────────────────────────────┘
```

### 2.3 ZoneAvoidanceRule 为什么是默认？

```java
// ★ ZoneAvoidanceRule 核心：两层筛选 + 轮询
//
// 第一层：ZoneAvoidancePredicate.filter()
//   → 排除 Zone 故障的实例（ZoneStats 获取本 Zone 健康度）
//   → 排除 blackList（黑名单 Zone）
//   → 如果本 Zone 全挂了 → fallbackZone → 选其他 Zone
//
// 第二层：AvailabilityPredicate.filter()
//   → 排除 active request count > connectionLimit 的实例
//   → 排除熔断标记的实例
//
// 最后：RoundRobinRule.choose()
//   → 在筛选后的列表里轮询
//
// 为什么生产推荐？
//   1. Zone 级故障转移（本地机房全挂 → 自动跨机房）
//   2. 连接数保护（避免单个实例过载）
//   3. 熔断感知（自动避开 Sentinel/CircuitBreaker 熔断的实例）
```

### 2.4 IPing 健康检查

```java
// ★ IPing 是健康检查接口
//   默认实现：NoOpPing（什么都不做，永远返回 true）
//   生产必须改！
//
// 三种实现：
//
// 1. NoOpPing（默认）
//    → isAlive(server) → true
//    → 不做任何检查
//    → 坑！实例挂了 Ribbon 还是会选到
//
// 2. PingUrl（生产推荐）
//    → isAlive(server) → 发 HTTP HEAD 请求到 http://host:port/actuator/health
//    → 200 OK → alive；其他 → dead
//    → 需要目标服务暴露 /actuator/health
//
// 3. NIWSDiscoveryPing（配合 Eureka）
//    → isAlive(server) → 查 DiscoveryClient 该实例的状态
//    → UP → alive；DOWN → dead
//    → 如果用 Nacos → Nacos 的 Instance.healthStatus 也能查
```

### 2.5 RestTemplate + @LoadBalanced 怎么集成 Ribbon

```java
// ★ 用法
//   @Bean
//   @LoadBalanced  // ← 这个注解！
//   public RestTemplate restTemplate() { return new RestTemplate(); }
//
//   // 调用
//   restTemplate.getForObject("http://order-service/orders", String.class);
//
// ★ 原理
//   @LoadBalanced 标记的 RestTemplate → AutoConfiguration
//   → LoadBalancerAutoConfiguration 注入 LoadBalancerInterceptor
//   → RestTemplate.addInterceptors(LoadBalancerInterceptor)
//
// LoadBalancerInterceptor.intercept() 流程：
//   1. url = "http://order-service/orders"
//   2. URL host = "order-service"（服务名，不是真实 IP）
//   3. LoadBalancerClient.choose("order-service") → Server{host, port}
//   4. 把 URL 改写成真实地址 → "http://192.168.1.100:8080/orders"
//   5. restTemplate.execute(真实URL) → 发出 HTTP 请求
//
// ★ 关键：URL 里必须是服务名（不能是 IP）
//   http://order-service/orders ✓（负载均衡）
//   http://192.168.1.100:8080/orders ✗（直接调用，跳过 Ribbon）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Ribbon 四件套？| **LoadBalancer + ServerList + IRule + IPing**（LoadBalancer 协调者，ServerList 服务列表，IRule 选策略，IPing 健康检查）|
| 2 | 默认 IRule 是什么？| **ZoneAvoidanceRule**（不是 RoundRobinRule！）→ 先 Zone 故障过滤 + 连接数过滤 → 再 RoundRobinRule 轮询 |
| 3 | 七种 IRule 按推荐排序？| **ZoneAvoidanceRule（默认）> BestAvailableRule > WeightedResponseTimeRule > AvailabilityFilteringRule > RetryRule > RoundRobinRule > RandomRule** |
| 4 | RoundRobinRule 线程安全？| 是的！AtomicInteger nextIndex → incrementAndGet() → mod serverList.size() |
| 5 | IPing 默认实现？| **NoOpPing**（永远返回 true，不做检查）→ 生产必须改 PingUrl |
| 6 | ServerList 怎么来？| **DiscoveryEnabledNIWSServerList** → DiscoveryClient.getInstances(serviceId) → 底层 Nacos/Eureka；30 秒自动刷新 |
| 7 | RestTemplate @LoadBalanced 原理？| **LoadBalancerInterceptor** → 拦截请求 → LoadBalancerClient.choose(serviceId) → 把服务名 URL 改成真实 host:port URL → 执行 |
| 8 | OpenFeign 集成 Ribbon 原理？| **LoadBalancerFeignClient** → 包装 Feign Client → execute 时先 LoadBalancerClient.choose → 拿到 Server → 拼接 URL → Client.execute |
| 9 | Ribbon 和 Nacos 分工？| **Nacos 注册中心（服务发现 + 心跳续约）→ Ribbon 选 Server（负载均衡策略）** → Nacos 只负责告诉你有哪些实例，选哪个由 Ribbon 决定 |
| 10 | 同一个服务名多个 LoadBalancer？| **每个服务名一个 LoadBalancer 实例**（SpringClientFactory 缓存按 serviceId 隔离）→ 不会跨服务共享 |
| 11 | Ribbon vs Spring Cloud LoadBalancer？| LoadBalancer 是 Spring Cloud Commons 内置（Reactive 支持更好）→ Ribbon 逐步弃用（2021+ 官方推荐 LoadBalancer）；功能都差不多（同样的 IRule 模式）|
| 12 | 自定义 IRule？| 继承 IRule 接口 → 实现 choose(key, serverList) → @Bean 注册 → 每个服务可以配不同 IRule |
| 13 | WeightedResponseTimeRule 权重怎么计算？| 维护每个 Server 的平均 RT（EMA 指数加权移动平均）→ **权重 = (maxRT - avgRT) / (maxRT - minRT) * 100** → 权重越大越容易被选中 |
| 14 | ZoneAvoidanceRule 跨 Zone 故障转移？| 本地 Zone 所有实例被过滤掉 → 自动 fallback 到其他 Zone 的实例 → 支持多机房部署 |
| 15 | Ribbon 怎么和 Sentinel 配合？| Sentinel 熔断后 → SentinelLoadBalancerClientFilter 把熔断实例从 serverList 过滤掉 → Ribbon 选不到熔断的实例（AvailabilityPredicate 也会识别）|

---

## 四、核心类速查表

| 类 | 职责 | 关键方法 |
|----|------|---------|
| **LoadBalancerClient** | Spring Cloud 抽象 | choose(serviceId) → Server |
| **RibbonLoadBalancerClient** | Ribbon 默认实现 | choose → getLoadBalancer → chooseServer |
| **SpringClientFactory** | LoadBalancer 工厂 | getLoadBalancer(serviceId) → 按服务名隔离缓存 |
| **ZoneAwareLoadBalancer** | 默认 LoadBalancer | chooseServer → serverList + ping + filter + rule |
| **DynamicServerListLoadBalancer** | 动态刷新 | 每 30s serverList.getUpdatedListOfServers |
| **DiscoveryEnabledNIWSServerList** | DiscoveryClient 数据源 | getUpdatedListOfServers → Nacos/Eureka |
| **IRule** | ★ 负载均衡策略接口 | choose(key, serverList) |
| **ZoneAvoidanceRule** | 默认 IRule | Zone 过滤 + Availability 过滤 + RoundRobin |
| **BestAvailableRule** | 最少连接 | 选 activeRequests 最小的 |
| **WeightedResponseTimeRule** | RT 自适应 | EMA 平均 RT → 权重计算 |
| **RoundRobinRule** | 简单轮询 | AtomicInteger → mod serverList.size |
| **IPing** | 健康检查接口 | isAlive(server) |
| **NoOpPing** | 默认实现 | 永远返回 true |
| **PingUrl** | HTTP 检查 | HEAD /actuator/health → 200 OK |

---

**文档版本：v1.0 · 2026-10-21**  
**源码基线**：Spring Cloud Netflix Ribbon 2021.0.8 + ribbon-core 2.3.x  
**面试考点：15 题**  
**状态**：⭐ P0 组件（Ribbon）补漏完成
