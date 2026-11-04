# Spring Cloud Gateway 源码深度解析

> **源码基线**：Spring Cloud Gateway 3.1.5（Spring Boot 2.7.x 配套）  
> **核心仓库**：https://github.com/spring-cloud/spring-cloud-gateway  
> **底层依赖**：Spring WebFlux + Project Reactor + Reactor Netty  
> **面试权重**：⭐⭐⭐⭐⭐

---

## 一、Gateway 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  客户端请求（HTTP）                                            │
│  curl http://gateway:8080/api/order/create?name=张三         │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Reactor Netty Server（异步非阻塞 WebFlux Server）              │
│  Netty HttpServerHandler → 构造 ServerWebExchange             │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  DispatcherHandler（Spring WebFlux 核心分发器）                │
│  handle(exchange) → getHandler(exchange) → handler.handle    │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  ★ RoutePredicateHandlerMapping                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ 1. RouteLocator.getRoutes() → Flux<Route> 所有路由   │   │
│  │ 2. 遍历所有 Route                                    │   │
│  │ 3. Route.getPredicate().apply(exchange) → 匹配       │   │
│  │ 4. 匹配成功 → exchange.put(GATEWAY_ROUTE_ATTR, route)│   │
│  │ 5. 返回 FilteringWebHandler（包含 GatewayFilter 链） │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  ★ FilteringWebHandler.handle                                │
│  遍历 GlobalFilter + GatewayFilter → GatewayFilterChain.filter│
│  getOrder() 升序执行（洋葱模型）                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ RemoveResponseHeaderFilter  → 删响应头                │   │
│  │ → RequestSizeLimitFilter    → 请求大小限制           │   │
│  │ → XForwardedFilter          → 转发头                 │   │
│  │ → RewritePathFilter         → 重写路径               │   │
│  │ → ★ ReactiveLoadBalancerClientFilter → Ribbon 选实例│   │
│  │ → ★ NettyRoutingFilter      → 真正发 HTTP 请求       │   │
│  │ → NettyWriteResponseFilter  → 写响应                 │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  Reactor Netty Client                                        │
│  HttpClient.create().baseUrl("http://order-service")        │
│  .request().uri("/order/create")                             │
│  .send().response()                                          │
│  ← 返回 Mono<ClientResponse>（响应式）                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心组件详解

### 2.1 Route（路由定义）

```yaml
# yml 配置 → Route Definition
spring:
  cloud:
    gateway:
      routes:
      - id: order_service          # 路由 ID
        uri: lb://order-service     # ★ lb:// = Ribbon/LoadBalancer 负载均衡
        predicates:
        - Path=/api/order/**        # ★ 路径匹配
        - Header=Content-Type,application/json  # 请求头匹配
        filters:
        - StripPrefix=1             # 去掉 /api 前缀（/api/order → /order）
        - AddRequestHeader=X-Caller, gateway  # 加请求头
        - name: RequestRateLimiter  # 限流
          args:
            redis-rate-limiter.replenishRate: 10
            redis-rate-limiter.burstCapacity: 20

// ★ Route Java 类结构
public class Route {
    private final String id;                  // 路由 ID
    private final URI uri;                    // 目标 URI（lb://order-service）
    private final int order;                  // 优先级（越小越先匹配）
    private final Predicate<ServerWebExchange> predicate;  // ★ 匹配条件
    private final List<GatewayFilter> gatewayFilters;      // GatewayFilter 链
    private final Map<String, Object> metadata;            // 元数据
}
```

### 2.2 Predicate（路由匹配）

```java
// ★ Route.getPredicate().apply(exchange) → 返回 true/false
//   Spring Cloud Gateway 内置 RoutePredicateFactory：
//
//   Path=/api/order/**       → PathRoutePredicateFactory（AntPathMatcher）
//   Header=X-Request-Id,\d+  → HeaderRoutePredicateFactory
//   Query=foo,bar             → QueryRoutePredicateFactory
//   Method=GET                → MethodRoutePredicateFactory
//   After=2020-01-01T00:00:00 → AfterRoutePredicateFactory（时间）
//   Before=2030-12-31T23:59:59 → BeforeRoutePredicateFactory
//   Between=2020-01-01T00:00:00,2030-12-31T23:59:59
//   RemoteAddr=192.168.1.1/24 → RemoteAddrRoutePredicateFactory（IP 段）
//   Weight=group1, 80         → WeightRoutePredicateFactory（灰度权重）
//
// ★ 多个 Predicate → 组合 AND 关系（全部匹配才通过）
// ★ Predicate 匹配顺序：order 升序（越小越先匹配）
// ★ 多个 Route 匹配同一个请求 → 取 order 最小的那个
```

### 2.3 Filter 链（★ 洋葱模型）

```java
// ★ Filter 分两种：
//   GlobalFilter：全局 Filter（所有 Route 都执行）
//   GatewayFilter：路由级 Filter（只绑定到某个 Route）
//
// ★ 执行顺序：getOrder() 升序（越小越先执行）
//   GatewayFilter 先于 GlobalFilter？不是！统一按 order 排序
//
// ★ 常见内置 GlobalFilter（按 order）：
//   -1  RemoveResponseHeaderFilter（删 X-Forwarded-* 头）
//   0   XForwardedFilter（添加 X-Forwarded-* 头）
//   1   ServerHttpObservationFilter（Metrics）
//   100 ToResponseHeaderFilter
//   101 NettyWriteResponseFilter（写响应）
//   ⭐ ReactiveLoadBalancerClientFilter（order=101，Ribbon 选实例）
//   ⭐ NettyRoutingFilter（order=Integer.MAX_VALUE，发请求）
//
// ★ GatewayFilterChain.filter(exchange)：
//   遍历 filter 链 → 每个 filter.filter(exchange, chain)
//   调用 chain.filter(exchange) → 递归执行下一个 Filter
//   → 洋葱模型：前面执行，后面返回

// ★ NettyRoutingFilter（真正发 HTTP 请求的核心）：
public class NettyRoutingFilter implements GlobalFilter, Ordered {
    
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // ① 获取目标 URL
        URI requestUrl = exchange.getRequiredAttribute(GATEWAY_REQUEST_URL_ATTR);
        
        // ② 用 Reactor Netty Client 发请求
        HttpClient httpClient = HttpClient.create();
        
        return httpClient.request()
            .uri(requestUrl)
            .headers(h -> h.addAll(exchange.getRequest().getHeaders()))
            .method(method)
            .send((req, nettyRequestBody) -> nettyRequestBody.send(...))
            .responseSingle((resp, respBody) -> {
                // ③ 处理响应
                ServerHttpResponse response = exchange.getResponse();
                response.setStatusCode(resp.status());
                response.getHeaders().addAll(resp.headers().asHttpHeaders());
                return response.writeWith(respBody);
            });
    }
    
    @Override
    public int getOrder() {
        return Integer.MAX_VALUE;  // ★ 最后一个执行（先做所有 Filter，最后发请求）
    }
}

// ★ ReactiveLoadBalancerClientFilter（集成 Ribbon/LoadBalancer）：
//   在 NettyRoutingFilter 之前执行 → 把 lb://order-service 替换成 http://192.168.1.100:8080
//   LoadBalancerClient.choose("order-service") → 选一个实例 → 拼接真实 URL
```

### 2.4 为什么 Gateway 用 Reactor Netty 而不是传统 Servlet？

```java
// ★ 传统 Servlet 是线程模型（一个请求一个线程 → 高并发下线程爆炸）
//   → Tomcat 默认 200 个线程 → 高并发下大量请求排队
//
// ★ Reactor Netty 是事件驱动模型（少量线程处理大量连接）
//   → Netty EventLoop（CPU 核数 * 2 线程）
//   → 每个请求注册回调 → 结果回来后回调处理
//   → 理论上能处理 10w+ 并发连接
//
// ★ 代价：代码需要用 Mono / Flux 响应式 API（不能用阻塞 IO）
//   → 所有 Filter / Handler 都必须返回 Mono<Void>
//   → ThreadLocal 不可靠（异步切换线程后丢失）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Gateway 底层技术栈？| **Spring WebFlux + Project Reactor + Reactor Netty**（异步非阻塞响应式框架）|
| 2 | DispatcherHandler.getHandler 流程？| **RoutePredicateHandlerMapping** → 遍历 Route → predicate.apply 匹配 → 返回 FilteringWebHandler |
| 3 | Route 匹配顺序？| **order 升序**（越小越先匹配）→ 多个 Route 匹配同一个请求取 order 最小的 |
| 4 | Predicate 是什么？| **判断请求是否匹配 Route 的条件**（Path/Header/Query/Method/After/RemoteAddr/Weight）→ 多个 AND 关系 |
| 5 | GlobalFilter vs GatewayFilter？| GlobalFilter 全局（所有 Route 执行）→ GatewayFilter 路由级（只绑定到某个 Route）|
| 6 | Filter 执行顺序？| **getOrder() 升序**（越小越先执行）→ 洋葱模型（前面执行，后面返回）|
| 7 | NettyRoutingFilter 做什么？| **真正发 HTTP 请求** → Reactor Netty HttpClient → 返回 Mono<ClientResponse>；order=Integer.MAX_VALUE（最后执行）|
| 8 | ReactiveLoadBalancerClientFilter？| **Ribbon/LoadBalancer 集成** → 把 lb://order-service 替换成 http://ip:port |
| 9 | StickySession（粘性会话）？| 根据 Cookie / IP / Header 哈希 → 每次请求路由到同一个后端实例 |
| 10 | 限流实现？| **RequestRateLimiter + Redis** → 令牌桶算法 + 集群共享 Redis 计数 |
| 11 | 熔断实现？| **Spring Cloud CircuitBreaker + Sentinel / Resilience4j** → NettyRoutingFilter 前加熔断 Filter |
| 12 | Gateway 高并发为什么比 Nginx 慢？| Nginx C 实现 → 每秒 5w+ QPS；Gateway Java + Reactor Netty → 每秒 1w+ QPS（差 5 倍）；但 Gateway 有 Java 扩展能力（自定义 Filter）|
| 13 | CORS 跨域处理？| GlobalCorsProperties + CorsWebFilter → 配置 allowedOrigins/methods/headers |
| 14 | Gateway 和 Nginx 分工？| Nginx：静态资源 + SSL 卸载 + 四层/七层负载均衡（性能优先）→ Gateway：业务网关 + Java 扩展能力 + 限流/熔断/鉴权（功能优先）|
| 15 | 为什么 Gateway 不支持 Servlet？| **底层用 Netty**（不是 Tomcat）→ 用 Spring WebFlux（不是 Spring MVC）→ API 完全不同 |

---

## 四、核心类速查表

| 类 | 职责 | 关键方法 |
|----|------|---------|
| **DispatcherHandler** | Spring WebFlux 核心分发 | handle → getHandler → handler.handle |
| **RoutePredicateHandlerMapping** | ★ 路由匹配 | getHandlerInternal → 遍历 Route → predicate.apply |
| **RouteLocator** | Route 来源 | getRoutes → Flux<Route> |
| **Route** | 路由定义 | getPredicate() → predicate.apply(exchange) |
| **FilteringWebHandler** | Filter 链执行 | handle → GatewayFilterChain.filter |
| **GatewayFilterChain** | Filter 链容器 | filter → 递归执行下一个 Filter |
| **GatewayFilter / GlobalFilter** | Filter 接口 | filter(exchange, chain) → Mono<Void> |
| **NettyRoutingFilter** | ★ 真正发 HTTP | filter → Reactor Netty HttpClient.send |
| **ReactiveLoadBalancerClientFilter** | Ribbon 集成 | choose → LoadBalancerClient → 选实例 |
| **RoutePredicateFactory** | Predicate 工厂 | apply → Predicate<ServerWebExchange> |
| **GatewayFilterFactory** | GatewayFilter 工厂 | apply → GatewayFilter |

---

**文档版本：v1.0 · 2026-10-21**  
**源码基线**：Spring Cloud Gateway 3.1.5（Spring Boot 2.7.x）  
**面试考点：15 题**
