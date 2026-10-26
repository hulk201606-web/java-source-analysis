# Spring Cloud Gateway 源码深度解析

> **源码基线**：spring-cloud-gateway 3.1.x（Spring Cloud 2021.0.8 配套）  
> **核心仓库**：spring-cloud/spring-cloud-gateway  
> **大纲来源**：19_SpringCloud源码调研大纲.md P0-4  
> **面试权重**：⭐⭐⭐⭐⭐（国内高频）

---

## 一、Gateway 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  Gateway 核心四概念（大纲要求）                              │
│                                                               │
│  Route         → 路由规则（目标 URL + Predicate + Filter）   │
│  Predicate     → 匹配条件（HTTP Method/Header/Path 等）      │
│  Filter        → 请求/响应拦截器（在目标 URL 之前/之后执行）  │
│  HandlerMapping→ 把请求路由到对应 Route 的处理器              │
│                                                               │
│ ★ 底层技术栈：Reactor Netty（响应式非阻塞）                  │
│   → 所有请求都是 Mono/Flux（WebFlux 模型）                    │
│   → 比传统 Servlet 异步模型性能好（没有 Servlet 容器开销）    │
│                                                               │
│ ★ 启动流程：                                                  │
│   @EnableGateway → GatewayAutoConfiguration                  │
│     → 注册 RouteLocatorBuilder（YAML/API 配置 Route）        │
│     → 注册所有内置 GlobalFilter（有序！getOrder()）          │
│     → 启动 Reactor Netty HTTP Server（监听端口）             │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、请求处理全链路（★ 大纲锚点：DispatcherHandler + NettyRoutingFilter）

### 2.1 DispatcherHandler.handle → Route 匹配

```java
// ★ Gateway 请求处理完整链路（大纲锚点）
//
// HTTP 请求进来：Reactor Netty HTTP Server → DispatcherHandler.handle
//   → 遍历所有 HandlerMapping（只有 RoutePredicateHandlerMapping）
//   → RoutePredicateHandlerMapping.getHandler(exchange)
//     → 遍历 RouteLocator 拿到所有 Route
//     → 对每个 Route 执行 **Predicate.test(exchange)**
//     → 匹配成功 → 返回该 Route 对应的 GatewayHandler
//
// ★ Predicate 类型（大纲要求）：
//
//   After / Before / Between   → 时间窗口匹配
//   Header                     → 请求 Header 匹配
//   Query                      → Query 参数匹配
//   Path                       → URL 路径匹配 ★ 最常用
//   Method                     → HTTP 方法匹配
//   RemoteAddr                 → 客户端 IP 匹配
//   Weight                     → 权重路由（灰度）
//   Composite                  → 组合多个 Predicate
//
// ★ PathPatternParser（大纲要求）：
//   三种模式：
//   ① AntPath（默认）：/api/** → 通配符匹配
//   ② 正则：regexp:/api/(.*) → 正则匹配
//   ③ RESTful：/api/{id} → 路径参数匹配
//   → 匹配成功后，Path 参数存到 ServerWebExchange
```

### 2.2 GlobalFilter 链执行顺序（★ 大纲要求）

```java
// ★ GlobalFilter 链（所有 Route 都执行，大纲锚点）
//
// 执行顺序 = getOrder() 升序（数字越小越先执行）
//
// 内置 GlobalFilter 顺序（Spring Cloud Gateway 默认）：
//
//   -1  RemoveResponseHeaderFilter    → 删除不需要的响应头
//   -2  RequestSizeFilter             → 限制请求体大小
//   -3  XForwardedRequestFilter      → 添加 X-Forwarded-* Header
//   0   XForwardedFilter             → 转发相关 Header
//   1   NettyRoutingFilter            → ★ 核心！真正发 HTTP 请求（大纲锚点）
//   2   NettyWriteResponseFilter      → 把响应写回客户端
//   ...
//   99  ForwardPathFilter             → 处理 forward:/ 路径
//
// ★ GatewayFilter vs GlobalFilter 区别（大纲高频）：
//   GatewayFilter：路由级，**绑定到某个 Route** 上（在 application.yml 里配）
//   GlobalFilter：全局，**所有 Route 都执行**（自动注册）
//   → GlobalFilter 一般做通用逻辑（Header/压缩）
//   → GatewayFilter 一般做特定 Route 逻辑（鉴权/限流）
//
// ★ Filter 链执行：
//   GatewayFilterChain.filter(exchange) → 依次调用每个 Filter.filter()
//   → Filter 里可以 exchange.getResponse().setStatusCode() 短路返回
//   → Filter 必须调用 chain.filter(exchange) 进入下一个 Filter
```

### 2.3 NettyRoutingFilter（★★★★★ 核心发请求）

```java
// ★ NettyRoutingFilter.filter —— 大纲要求的核心考点
//   → Gateway 里**真正发 HTTP 请求到后端微服务**的地方
//   → 没有这个 Filter，Gateway 就是个空壳
//
// 源码链路（简化）：
//
// NettyRoutingFilter.filter(exchange, chain)
//
//   ① 拿到 Route 的目标 URL：
//     exchange.getAttribute(GATEWAY_REQUEST_URL_ATTR)
//     → Predicate 匹配后已设置好目标 URL（host:port/path）
//
//   ② 构造 Reactor Netty HttpClientRequest：
//     HttpClient.newRequest(requestUrl)
//       .method(httpMethod)
//       .headers(requestHeaders)
//       .body(requestBody)
//     → Reactor Netty（响应式非阻塞 HTTP Client）
//
//   ③ 执行请求：
//     httpClient.request(requestSpec).exchange()
//     → Reactor Netty 用 Netty TCP Client 发 HTTP 请求
//     → 返回 Mono<ClientResponse>
//     → Reactor Netty 内部：连接池 + keepalive + 异步回调
//
//   ④ 处理响应：
//     ClientResponse → 把响应体/响应头/状态码写回 ServerWebExchange
//     → 后续 NettyWriteResponseFilter 写回客户端
//
//   ⑤ 返回 chain.filter(exchange)（进入下一个 Filter）
//
// ★ Reactor Netty 为什么快？
//   底层：Netty TCP Client + EventLoop 模型
//   → 每个请求在 EventLoop 线程里执行（无锁）
//   → 连接池复用（keepalive）
//   → 零拷贝（DirectBuffer）
//   → 比 Apache HttpClient 快 3-5x
//
// ★ 和 OpenFeign 对比：
//   Gateway NettyRoutingFilter：**服务端反向代理**（Netty 异步，转发请求）
//   OpenFeign SynchronousMethodHandler：**客户端调用**（同步/异步 HTTP，主动发请求）
//   → 两者底层都用 HTTP Client，但 Gateway 是服务端代理，Feign 是客户端调用
```

---

## 三、面试考点（15 题，★ 大纲要求）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Gateway 底层是什么？| **Reactor Netty（响应式非阻塞）** → WebFlux 模型；没有 Servlet 容器开销；比 Spring MVC + Servlet 快 |
| 2 | Route 匹配流程？| DispatcherHandler → RoutePredicateHandlerMapping → **Predicate.test(exchange)** → PathPatternParser.match |
| 3 | Filter 链执行顺序？| **getOrder() 升序执行**；chain.filter(exchange) 进入下一个；可以短路返回 |
| 4 | GatewayFilter vs GlobalFilter？| **GatewayFilter 路由级**（绑定某个 Route）；**GlobalFilter 全局**（所有 Route 都执行）；GlobalFilter 一般做通用逻辑 |
| 5 | 真正发请求的 Filter？| **NettyRoutingFilter**（大纲锚点）→ 构造 Reactor Netty HttpClientRequest → exchange() → 发 HTTP 请求到后端 |
| 6 | 跨域（CORS）怎么处理？| **GlobalCorsProperties + CorsWebFilter**；配置 allowedOrigins/allowedMethods/allowedHeaders；Spring Cloud Gateway 自动配 |
| 7 | Gateway 和 Nginx 区别？| Gateway：**Filter 机制 + Java 扩展 + 响应式**；Nginx：C 性能好 + 配置简单；生产常组合（Nginx 入口 + Gateway 内部）|
| 8 | Gateway vs OpenFeign？| Gateway：**服务端反向代理**（转发请求）；Feign：**客户端调用**（主动发请求）；两者底层都用 HTTP Client |
| 9 | Gateway 灰度路由？| **Weight RoutePredicate**（权重路由）+ **Header RoutePredicate**（按 Header 路由）；实现灰度 |
| 10 | Gateway 限流？| **RequestRateLimiter**（内置 GatewayFilter）+ Redis 令牌桶；生产推荐 Sentinel 集成 |
| 11 | Gateway 学习路径？| application.yml 配 Route → 启动看 Route 匹配 → 源码看 NettyRoutingFilter → GlobalFilter 链 → Predicate 类型 |
| 12 | Gateway 异步模型？| Mono/Flux（响应式流）→ Backpressure（反压）→ 不会 OOM；Servlet 模型做不到 |
| 13 | Gateway 配置热更新？| RouteLocator 的 RouteDefinitionLocator → 配置变化自动刷新 Route；不需要重启 |
| 14 | Gateway Fallback？| `spring.cloud.gateway.fallback.response-status` + **DisableCircuitBreakerFilter**；或自定义 Fallback 工厂 |
| 15 | Gateway 和 LoadBalancer？| 内置 **LoadBalancerClientFilter** → 集成 Ribbon/Spring Cloud LoadBalancer → serviceId 替换成 host:port |

---

## 四、Seata AT 模式（P2，大纲核心考点）

### 4.1 AT 模式三阶段 undo_log（★★★★★ 大纲要求）

```java
// ★ Seata AT 模式一阶段就完成提交（面试最高频！）
//
// 核心设计：拦截业务 SQL → 自动生成 undo_log → 二阶段异步清理
//
// 完整流程（大纲要求）：
//
// ① @GlobalTransactional 开启全局事务
//    → GlobalBeginRequest → Seata TC（Transaction Coordinator）
//    → TC 生成 XID（全局事务 ID）
//    → 拦截器把 XID 放进 ThreadLocal（RootContext.bind(xid)）
//
// ② 执行业务 SQL（DataSourceProxy 代理数据源）
//    → Seata DataSourceProxy 包装原始 DataSource
//    → 生成 ConnectionProxy → 代理 PreparedStatement
//
//    三阶段 undo_log 生成（★★★★★ 大纲核心！）：
//
//    a. 前镜像查询（SELECT FOR UPDATE）
//       SELECT * FROM order WHERE id = 1 FOR UPDATE
//       → 拿到更新前的数据（beforeImage）
//       → 加行锁（MySQL FOR UPDATE）
//
//    b. 执行业务 SQL
//       UPDATE order SET status = 'paid' WHERE id = 1
//       → 正常执行，提交到 MySQL
//
//    c. 后镜像查询（SELECT FOR UPDATE）
//       SELECT * FROM order WHERE id = 1 FOR UPDATE
//       → 拿到更新后的数据（afterImage）
//
//    d. 写入 undo_log
//       INSERT INTO undo_log (xid, branch_id, before_image, after_image, sql)
//       VALUES (?, ?, ?, ?, ?)
//       → 前镜像 + 后镜像 + 业务 SQL 都存下来
//       → 用于二阶段回滚时自动恢复
//
// ③ 一阶段提交（★！）
//    → undo_log 和业务 SQL **在同一个本地事务里提交**
//    → 本地事务先提交（MySQL 事务）
//    → BranchRegisterRequest 注册到 TC
//    → TC 给 Branch 分配 branch_id
//
// ④ 全局提交/回滚（二阶段，异步）
//    - 提交：TC 异步通知所有 Branch → **删除 undo_log**（异步，不阻塞）
//    - 回滚：TC 通知所有 Branch → 读 undo_log → **根据前镜像恢复数据**
//      → UPDATE order SET status = 'init' WHERE id = 1（反向 SQL）
//      → 校验脏写（如果数据已被其他事务修改 → RollbackFailed）
//
// ★ 为什么一阶段就提交？
//   二阶段异步清理 undo_log → 提交延迟 = 本地事务提交延迟（毫秒级）
//   → 比传统 2PC 快一个数量级（2PC 要等所有参与者确认）
//
// ★ 全局锁解决脏写（大纲要求）：
//   Branch 提交时 → TC LockManager.addLock(lockKey, branchId)
//   → 同一行只能被一个全局事务写
//   → 其他事务写同一行 → 等待锁释放 / 抛出 LockConflictException
```

### 4.2 面试考点（P2-2 大纲要求）

| # | 考点 | 答案 |
|---|------|------|
| 1 | AT 模式一阶段就提交了？| **是的！** undo_log 和业务 SQL 在同一个本地事务提交 → 二阶段异步清理 undo_log |
| 2 | AT 模式怎么防止脏写？| **全局锁 + 本地 FOR UPDATE**；TC LockManager 管理全局锁；同一行只能被一个全局事务写 |
| 3 | undo_log 在哪张表？| **每业务表的 undo_log**；字段：xid + branch_id + before_image + after_image + sql |
| 4 | Seata 和 TCC/Saga 区别？| **AT 自动**（数据源代理 + SQL 拦截 + 自动 undo_log）；TCC/Saga **手动**实现两阶段；AT 对业务零侵入 |
| 5 | DataSourceProxy 做什么？| **代理原始 DataSource → 返回 ConnectionProxy**；ConnectionProxy 代理 PreparedStatement → 拦截 SQL → 生成 undo_log |

---

## 五、核心源码锚点清单（大纲要求）

| 组件 | 类 | 方法 | 核心功能 |
|------|----|------|---------|
| **Gateway** | DispatcherHandler | handle | 请求入口，路由到 HandlerMapping |
| | RoutePredicateHandlerMapping | getHandler | Predicate 匹配 Route |
| | NettyRoutingFilter | filter | 构造 Reactor Netty HTTP Client 发请求 ★ |
| | PathPatternParser | match | Path 匹配（Ant/正则/RESTful）|
| **Seata** | DataSourceProxy | getConnection | 创建 ConnectionProxy |
| | ConnectionProxy | execute | 拦截 SQL → 生成 undo_log |
| | ConnectionContext | appendUndoLog | undo_log 写入上下文 |
| | TransactionalTemplate | execute | @GlobalTransactional 入口 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Gateway 3.1.x + Seata 2.0.x  
**面试考点**：Gateway 15 题 + Seata 5 题  
**大纲来源**：19_SpringCloud源码调研大纲.md P0-4 + P2-1
