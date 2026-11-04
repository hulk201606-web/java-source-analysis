# Spring Cloud Sleuth 链路追踪源码深度解析

> **源码基线**：Spring Cloud Sleuth 3.1.10（Spring Boot 2.7.x 配套）  
> **核心仓库**：https://github.com/spring-cloud/spring-cloud-sleuth  
> **底层依赖**：io.zipkin.brave:brave（链路追踪核心库）  
> **面试权重**：⭐⭐⭐⭐

---

## 一、Sleuth 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心概念                                                      │
│  Trace（整条链路唯一 ID）→ Span（每一段调用唯一 ID）             │
│                                                               │
│  示例：A → B → C → D                                          │
│                                                               │
│  A → B 调用                                                     │
│  TraceId: aaaaaaaabbbbbbbbccccccccdddddddd                    │
│  SpanId: 1111111122222222（A 的 span）                         │
│  ParentSpanId: null                                           │
│  SpanId: 3333333344444444（B 的 span，parent=A 的 span）        │
│  ParentSpanId: 1111111122222222                                │
│                                                               │
│  拓扑：                                                         │
│     A (root, spanId=11111111)                                  │
│     ├─ B (spanId=33333333, parent=11111111)                   │
│     │  ├─ C (spanId=55555555, parent=33333333)                │
│     │  └─ D (spanId=77777777, parent=33333333)                │
│     └─ B 另一次调用                                            │
│        └─ D (spanId=99999999, parent=33333333)                │
└─────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────┐
│  Sleuth 核心拦截器（自动生效，不需要写代码）                     │
│                                                               │
│  HTTP 请求入口：                                               │
│    BraveServletFilter（Servlet Filter）                         │
│      → 生成 Root Span（TraceId + SpanId）                      │
│      → 存入 MDC（日志就能打 traceId！）                         │
│      → 设置请求属性（SleuthAttributesHolder）                   │
│                                                               │
│  下游 HTTP 调用（OpenFeign / RestTemplate）：                   │
│    ClientInterceptor（Feign）/ RestTemplate 拦截器              │
│      → 从 ThreadLocal 拿当前 Span                              │
│      → 生成 Child Span（parentSpanId = 当前 spanId）           │
│      → 在 HTTP Header 注入 X-B3-TraceId / X-B3-SpanId / ...   │
│      → 传给下游                                                 │
│                                                               │
│  JDBC 调用：                                                   │
│    BraveDataSourceProxy（数据源代理）                            │
│      → 执行 SQL 时生成 Span（记录 SQL + 执行时间）               │
│      → 上报给 Zipkin                                          │
│                                                               │
│  异步线程：                                                    │
│    LazyTraceExecutor / TraceAsyncConfigurerSupport              │
│      → Callable / Runnable 包装 → 把 Span 上下文传到子线程      │
│      → 解决跨线程 TraceId 丢失问题！                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 BraveServletFilter（HTTP 入口生成 Root Span）

```java
// ★ BraveServletFilter 是 Sleuth 的 HTTP 入口拦截器
//   Spring Boot 自动注册 Filter（优先级最高，第一个执行）
//
// doFilter 流程：
//   1. 从 HTTP Header 尝试解析 Trace 上下文
//      → X-B3-TraceId / X-B3-SpanId / X-B3-ParentSpanId
//      → 如果上游有 → 继续整条链路（Child Span）
//      → 如果没有 → 生成新 Trace（Root Span）
//
//   2. Tracer.newTrace() 或 Tracer.newChild()
//      Brave Tracer 生成 Span
//      Span 内部持有：traceId, spanId, parentSpanId, kind(SERVER)
//
//   3. Span.start() → 记录开始时间
//
//   4. Span.current().makeCurrent() → 存入 Brave 的 ThreadLocal
//
//   5. MDC.put("traceId", span.traceId())
//      MDC.put("spanId", span.spanId())
//      → Logback 配置 %d{traceId:-} → 日志自动打 traceId！
//
//   6. chain.doFilter() → 执行业务逻辑
//
//   7. Span.finish() → 记录结束时间 + 上报 Zipkin
//      MDC.clear() → 清理
```

### 2.2 Feign / RestTemplate 拦截（下游调用透传 Header）

```java
// ★ OpenFeign 自动集成 Sleuth
//   SleuthFeignAutoConfiguration → 注册 SpanClientInterceptor
//
// SpanClientInterceptor.invoke 流程：
//   1. Span span = tracer.currentSpan()
//      从 ThreadLocal 拿当前 Span（就是 HTTP 入口的那个）
//
//   2. Span newSpan = span.childOf(span).name("feign: " + path)
//      创建子 Span（parentSpanId = 当前 spanId）
//      newSpan.start()
//
//   3. HTTP Header 注入（★ 核心！跨服务透传）
//      requestTemplate.header("X-B3-TraceId", span.traceId())
//      requestTemplate.header("X-B3-SpanId", newSpan.spanId())
//      requestTemplate.header("X-B3-ParentSpanId", span.spanId())
//      requestTemplate.header("X-B3-Sampled", "1")
//      → 下游 BraveServletFilter 收到 → 解析 Header → 继续整条链路
//
//   4. 执行 Feign 请求 → 返回
//
//   5. newSpan.finish() → 上报 Zipkin
```

### 2.3 异步线程 Span 传递（★★★ 高频坑点）

```java
// ★ 问题：主线程有 traceId → @Async / new Thread() → 子线程没有 traceId
//   因为 ThreadLocal 不跨线程！
//
// ★ Sleuth 解决方案：包装 Executor / Callable / Runnable
//
// @Async 自动生效：
//   TraceAsyncConfigurerSupport.getAsyncExecutor()
//     → TraceExecutorWrapper 包装原始 Executor
//     → submit(Runnable task) → 包装成 TraceRunnable
//     → TraceRunnable.run() 内部：
//       → 拿到主线程的 Span
//       → 子线程 SpanInScope spanInScope = span.makeCurrent()
//       → 执行原始 task
//       → spanInScope.close() + span.finish()
//
// 手动跨线程：
//   主线程：Span span = tracer.currentSpan()
//   子线程：try (SpanInScope scope = span.makeCurrent()) {
//       // 这里有 traceId / spanId
//   }
//
// CompletableFuture：
//   TraceCompletableFuture.decorate(completableFuture)
//     → 把每个回调都包装在 Span 作用域里
```

### 2.4 MDC 日志 traceId（★ 面试高频）

```xml
<!-- Logback 配置：自动打印 traceId -->
<pattern>
    %d{HH:mm:ss.SSS} [%thread] %-5level 
    %logger{36} - [%d{traceId:-}, %d{spanId:-}] 
    %msg%n
</pattern>
<!-- 输出示例：10:30:15.123 [http-nio-8080-1] INFO  c.e.OrderService - [aaaa...bbbb, 1111...2222] 订单创建成功 -->
```

```
原理：
  1. BraveServletFilter.doFilter → MDC.put("traceId", span.traceId())
  2. 业务代码任意位置 log.info() → Logback 用上面的 pattern 输出 → 自动带 traceId
  3. BraveServletFilter.doFilter 结束 → MDC.clear()

异步线程问题：子线程 MDC 没有 traceId！
  → Sleuth 的 TraceRunnable 在子线程里也 MDC.put → 解决
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TraceId vs SpanId？| **TraceId 整条链路唯一（A→B→C→D 全链路共享）**；SpanId 每个节点唯一（每个服务实例有自己的 spanId）；ParentSpanId 指向父 Span |
| 2 | Sleuth 底层依赖？| **io.zipkin.brave:brave**（Brave 是 Zipkin 官方的链路追踪库）；Sleuth 是 Brave 的 Spring Boot 自动配置封装 |
| 3 | HTTP 入口怎么生成 Span？| **BraveServletFilter**（Servlet Filter，最高优先级）→ 从 Header 解析 trace 或 newTrace → Span.makeCurrent() → MDC.put → chain.doFilter → Span.finish() |
| 4 | 跨服务 TraceId 怎么传递？| **HTTP Header 注入**：X-B3-TraceId / X-B3-SpanId / X-B3-ParentSpanId / X-B3-Sampled → 下游 BraveServletFilter 解析 Header → 继续整条链路 |
| 5 | OpenFeign 自动集成？| 是的！SleuthFeignAutoConfiguration → 注册 **SpanClientInterceptor** → 在 HTTP Header 自动注入 Trace 上下文 |
| 6 | 异步线程 TraceId 丢失？| ThreadLocal 不跨线程！→ Sleuth 用 **TraceExecutorWrapper** 包装 Executor → 子线程 Span.makeCurrent() → MDC 也同步 |
| 7 | MDC 怎么打 traceId？| Logback pattern 加 **%d{traceId:-}** → BraveServletFilter 在主线程 MDC.put("traceId") → 日志自动带 |
| 8 | JDBC 也有 Span？| 是的！**BraveDataSourceProxy** 代理 DataSource → 每个 SQL 执行生成 Span（记录 SQL 文本 + 执行 RT）→ 上报 Zipkin |
| 9 | 上报到哪里？| 默认 **Zipkin Server**（spring.zipkin.base-url=http://localhost:9411）；也可以用 Kafka 异步上报（spring.zipkin.sender.type=kafka）|
| 10 | 采样策略？| 默认 **100% 采样**（sleuth.sampler.probability=1.0）→ 生产调低（0.1 = 10% 采样）→ 减少上报量 |
| 11 | Sleuth vs SkyWalking/Zipkin？| Sleuth 是 **埋点层**（拦截器 + Span 生成）；SkyWalking/Zipkin 是 **后端收集 + 展示**（UI + 存储）；Sleuth 默认上报 Zipkin，也可以上报 SkyWalking |
| 12 | Sleuth 能和 SkyWalking 配合吗？| 可以！Sleuth 埋点 → **Zipkin 格式** → SkyWalking 兼容 Zipkin 接收 → 都能在 SkyWalking UI 看到 |
| 13 | 业务代码手动创建 Span？| Tracer tracer = ...；Span span = tracer.newSpan().name("custom-op").start()；try (SpanInScope scope = span.makeCurrent()) { ... } finally { span.finish(); } |
| 14 | Sleuth 3.x 弃用了什么？| 弃用了 **spring-cloud-sleuth-stream**（用 Spring Cloud Stream 异步上报）→ 推荐用 Kafka sender |
| 15 | 网关 Gateway 也能打 trace？| 可以！**spring-cloud-starter-sleuth + spring-cloud-sleuth-zipkin** 加在 Gateway 里 → GatewayFilter 链也会生成 Span → 整条链路完整 |

---

**文档版本：v1.0 · 2026-10-21**  
**源码基线**：Spring Cloud Sleuth 3.1.10（Brave zipkin 核心）  
**面试考点：15 题**
