# OpenTelemetry Trace 上下文传播 + W3C TraceContext 源码级深度解析

> **源码基线**：OpenTelemetry Java API 1.40+ / Go 1.30+ / Specification v1.32  
> **核心位置**：opentelemetry-api + opentelemetry-context + opentelemetry-propagation  
> **面试权重**：⭐⭐⭐⭐⭐（可观测性最高频，SkyWalking/Zipkin/Jaeger 协议核心）

---

## 一、OpenTelemetry 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ OpenTelemetry = Trace + Metrics + Logs（可观测性三件套）  │
│                                                               │
│  核心架构（面试必背）：                                        │
│                                                               │
│  Instrumentation（埋点层）                                    │
│      ↓                                                        │
│  API（opentelemetry-api）                                     │
│      ↓                                                        │
│  SDK（opentelemetry-sdk）                                     │
│      ↓                                                        │
│  OTLP Exporter → Collector → Backend                          │
│                                                               │
│  ★ 上下文传播核心（面试核心！）：                               │
│   W3C TraceContext Propagator（标准 HTTP Header 传播）         │
│   B3 Propagator（Zipkin 兼容）                                │
│   Jaeger Propagator                                           │
│                                                               │
│  ★ Span 核心属性（面试高频）：                                │
│   TraceId（全局唯一，128-bit / 32 位 16 进制）                │
│   SpanId（64-bit / 16 位 16 进制）                             │
│   ParentSpanId（父子关系）                                    │
│   TraceFlags（采样标记）                                      │
│   TraceState（厂商自定义状态）                                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、W3C TraceContext 标准协议源码级

### 2.1 HTTP Header 格式

```
// ★ W3C TraceContext 标准（源码级格式，面试核心！）
//
// // 两个 HTTP Header：
// //
// // 1. traceparent（必选）：
// //    traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
// //    |   |                          |                         |  |
// //    |   TraceId（32位hex）         SpanId（16位hex）         |  TraceFlags
// //    Version(2位hex=00)                                       （2位hex）
// //    版本=00（当前唯一版本）
// //
// // 2. tracestate（可选）：
// //    tracestate: vendor1=value1,vendor2=value2
// //    // 逗号分隔的 key=value 对
// //    // 最多 32 个 vendor；总长度 ≤ 512B
// //    // 厂商自定义，用于跨服务传递额外追踪信息
// //
// // ★ TraceFlags（2 位 hex）：
// //   00 = 默认（未采样）
// //   01 = 采样记录（Sampled）
// //   // 第 0 位 = sampled flag（低位）
// //   // 其他位保留
// //
// // ★ TraceId（128-bit）：
// //   32 位 16 进制字符串
// //   全局唯一；一个 Trace 所有 Span 共享同一个 TraceId
// //
// // ★ SpanId（64-bit）：
// //   16 位 16 进制字符串
// //   每个 Span 唯一；父子 Span 的 SpanId 建立引用关系
//
```

### 2.2 传播流程源码级

```java
// ★ OpenTelemetry W3C TraceContext Propagation 源码级
//
// api/context/src/main/java/io/opentelemetry/context/
// api/trace/src/main/java/io/opentelemetry/api/trace/propagation/
// sdk/.../propagation/W3CTraceContextPropagator.java
//
// // Inject（注入：Client → Server 请求）：
// //
// // 1. Server 启动 Span：Span currentSpan = tracer.spanBuilder("serviceA")...startSpan()
// //
// // 2. Client 发 HTTP 请求前，Inject 到 Header：
// W3CTraceContextPropagator propagator = W3CTraceContextPropagator.getInstance();
// Context context = Context.current().with(currentSpan);
// propagator.inject(context, httpHeaders, (headers, key, value) -> headers.set(key, value));
//
// // 3. Propagator 写入两个 Header：
// //    traceparent: 00-{TraceId}-{SpanId}-{TraceFlags}
// //    tracestate:  {vendor1}=...,...
//
// // Extract（提取：Server 收到请求）：
// //
// // 1. Server 收到 HTTP 请求，从 Header 提取：
// Context extracted = propagator.extract(
//     Context.current(),
//     httpHeaders,
//     (headers, key) -> headers.getFirst(key)
// );
//
// // 2. 从 Context 获取 SpanContext：
// SpanContext parentContext = Span.fromContext(extracted).getSpanContext();
//
// // 3. Server 创建子 Span：
// Span childSpan = tracer.spanBuilder("serviceB")
//     .setParent(parentContext)  // ★ 设置父上下文！
//     .startSpan();
//
// // 4. Child Span 继承父 TraceId + TraceFlags（采样）
// //    新 SpanId 由 SDK 生成（64-bit 随机）
//
// // ★ Context 传播核心：
// //   OpenTelemetry Context 是 ThreadLocal 类似机制
// //   // Context.current() → 当前线程上下文
// //   // Context.with(value) → 返回带新值的新 Context（不可变）
// //   // // 不是全局变量，支持嵌套
// //
```

---

## 三、Span 父子关系 + 引用源码级

### 3.1 Parent-Child + FollowsFrom

```java
// ★ OpenTelemetry Span 引用类型（面试高频）
//
// api/trace/src/main/java/io/opentelemetry/api/trace/
//
// // 两种引用关系：
// //
// // 1. Parent-Child（默认）：
// //    Child Span 生命周期依赖 Parent
// //    // Parent 结束后 Child 才会被考虑
// //    // 同步调用场景（HTTP/gRPC/RPC）
// //
// // 2. FollowsFrom：
// //    Child Span 不依赖 Parent 生命周期
// //    // Parent 结束后 Child 可以继续
// //    // 异步场景（消息队列/Kafka/RabbitMQ）
// //
// // ★ 例子：
// //   OrderService → PaymentService（同步 HTTP）→ Parent-Child
// //   OrderService 发 Kafka 消息 → Consumer 消费 → FollowsFrom
// //
// // ★ SpanBuilder 设置 Parent：
// //
// // a. 显式指定（从 Extract 的 Context）：
// spanBuilder.setParent(parentSpanContext)
//
// // b. 隐式继承（从当前 Context）：
// spanBuilder.setParent(Context.current())  // 自动用 Context 中的 Span
//
// // c. 无 Parent（根 Span）：
// spanBuilder.setNoParent()  // 新 Trace 的根 Span
//
// // ★ SpanContext 必须满足：
// //   TraceId 非空（32 位 hex）
// //   SpanId 非空（16 位 hex）
// //   isValid() = true（合法的 TraceId + SpanId）
// //   // 非法 SpanContext 不能作为 Parent
//
```

### 3.2 Span 生命周期 + 属性

```java
// ★ Span 生命周期源码级（面试高频）
//
// // Span 状态机：
// //   UNSET → OK / ERROR
// //   setStatus(StatusCode.OK | ERROR | UNSET)
// //
// // Span 属性（Attributes）：
// //   span.setAttribute("http.method", "GET");
// //   span.setAttribute("http.url", "/api/orders");
// //   // Key-Value 形式；String / Number / Boolean / Array 类型
// //   // 官方定义了标准 Semantic Conventions
// //   // https://opentelemetry.io/docs/specs/semconv/
// //
// // Span Events（时序事件）：
// //   span.addEvent("cache-miss");
// //   span.addEvent("db-query", Attributes.of("db.system", "mysql"), timestamp);
// //   // 时间戳自动记录；可带属性
// //
// // Span Links（跨 Trace 引用）：
// //   span.addLink(otherSpanContext);
// //   // 一个 Span 可以 Link 多个其他 Trace 的 Span
// //   // 场景：批处理、数据流合并
// //
// // ★ Span 采样决策：
// //   TraceFlags.SAMPLED (0x01) → 记录并导出
// //   TraceFlags.DEFAULT (0x00) → 可能被丢弃
// //   // 采样率由 SDK Sampler 决定：
// //   // AlwaysOnSampler（全采）
// //   // AlwaysOffSampler（全丢）
// //   // TraceIdRatioBasedSampler（按 TraceId hash 比例采样）
// //   // ParentBasedSampler（继承 Parent 采样决策）
//
```

---

## 四、OpenTelemetry vs SkyWalking vs Zipkin 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：三种分布式追踪协议对比                           │
│  ┌──────────────┬──────────────────┬──────────┬──────────┐  │
│  │ 维度            │ OpenTelemetry     │ SkyWalking│ Zipkin    │  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 标准协议         │ ★ W3C TraceContext│ SW8+v3    │ B3        │  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 语言支持         │ ★ 多语言 SDK     │ Java/Go/...│ Java/Go  │  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 数据模型         │ Span + Link + Event│ Segment   │ Span      │  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 上下文传播       │ ★ traceparent +  │ sw8 Header│ X-B3-*    │  │
│  │                │ tracestate       │ + v3 gRPC │           │  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 社区            │ ★ CNCF 顶级项目  │ Apache    │ OpenZipkin│  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 后端            │ OTLP Collector  │ OAP Server│ Zipkin UI │  │
│  ├──────────────┼──────────────────┼──────────┼──────────┤  │
│  │ 面试高频         │ ★★★★★          │ ★★★★★    │ ★★★★      │  │
│  └──────────────┴──────────────────┴──────────┴──────────┘  │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   多语言微服务 + 标准协议 → **OpenTelemetry**（W3C 标准）     │
│   Java 微服务生态 → SkyWalking（侵入性 Instrumentation）     │
│   轻量 Zipkin 兼容 → Zipkin                                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ★ OpenTelemetry 三件套？| **Trace（分布式追踪）+ Metrics（指标）+ Logs（日志）** |
| 2 | ★ W3C TraceContext 两个 Header？| **traceparent + tracestate** |
| 3 | traceparent 格式？| `00-{TraceId 32hex}-{SpanId 16hex}-{TraceFlags 2hex}`；版本=00；TraceFlags 最低位=采样标志 |
| 4 | TraceId / SpanId 长度？| **TraceId=128-bit（32位hex）**；SpanId=64-bit（16位hex）|
| 5 | ★ Inject / Extract 是什么？| Inject：SpanContext → HTTP Header（Client 发请求前）；Extract：HTTP Header → SpanContext（Server 收请求后）|
| 6 | ★ Span 两种引用关系？| **Parent-Child（同步依赖生命周期）+ FollowsFrom（异步不依赖生命周期）** |
| 7 | Parent-Child vs FollowsFrom 场景？| Parent-Child：HTTP/gRPC 同步调用；FollowsFrom：Kafka/RabbitMQ 异步消息 |
| 8 | Span 生命周期状态？| UNSET → OK / ERROR；`span.setStatus(StatusCode.ERROR, "msg")` |
| 9 | Span Sampling 决策？| TraceFlags.SAMPLED(0x01) 记录；DEFAULT(0x00) 可能丢弃；SDK Sampler 决定 |
| 10 | ★ SDK 四种 Sampler？| AlwaysOn + AlwaysOff + TraceIdRatioBased（按比例采样）+ ParentBased（继承父 Span 决策）|
| 11 | OpenTelemetry Context 机制？| 不可变 ThreadLocal；`Context.current()` + `Context.with()` 支持嵌套 |
| 12 | span.addLink 是什么？| 跨 Trace 引用；一个 Span 可以 Link 多个其他 Trace 的 Span；批处理场景 |
| 13 | span.addEvent 是什么？| Span 内部时序事件；自动带时间戳；可带 Attributes |
| 14 | ★ OpenTelemetry vs SkyWalking vs Zipkin 对比？| 见第四节对比表（7 维度）|
| 15 | 面试最高频考点排序？| **1. traceparent 格式 2. Inject/Extract 流程 3. Parent-Child vs FollowsFrom 4. Sampler 5. 三协议对比** |
| 16 | 学习路径？| Span → W3C Header → Inject/Extract → Parent/Child → Sampler → 对比 |
| 17 | 面试常见追问？| Parent-Child Span 结束顺序？| **Child Span 必须在 Parent 之前结束**（Parent 依赖 Child）；否则违反语义 |
| 18 | 面试常见追问？| TraceId 必须唯一吗？| **全局唯一**（128-bit 随机）；碰撞概率可以忽略；所有 Span 共享同一个 TraceId |
| 19 | 面试陷阱？| 说 "Span 可以有多个 Parent" → ❌ **最多一个 Parent**；但可以有多个 Link |
| 20 | 面试陷阱？| 说 "traceparent 只有 TraceId 和 SpanId" → ❌ **还有 Version 和 TraceFlags**；格式是 4 个部分 |
| 21 | tracestate 最多多长？| **≤ 512B，最多 32 个 vendor key**；每个 key ≤ 15B；每个 value ≤ 241B |
| 22 | OTLP 是什么？| OpenTelemetry Protocol；gRPC / HTTP 两种传输；OTLP Collector 接收+处理+转发 |
| 23 | Semantic Conventions？| 官方标准属性命名（`http.method`, `db.system`）；避免厂商自定义混乱 |
| 24 | B3 vs W3C TraceContext？| B3（Zipkin）：X-B3-TraceId/SpanId/ParentSpanId/Sampled；W3C：traceparent 单一 Header；**W3C 更简洁、更标准** |
| 25 | Span Attributes 类型？| String / Boolean / Long / Double / Array（混合类型）|
| 26 | 典型生产配置？| **ParentBased(root=TraceIdRatioBased(rate=0.1)) + OTLP gRPC Exporter + W3C + B3 Propagator（兼容老系统）** |
| 27 | Span 采样 vs 后端采样？| Span 采样：客户端/SDK 层（减少导出数据量）；后端采样：Collector/Backend 层（有完整数据再决定）；**前端采样更省资源** |

---

**文档版本：v1.0 · 2026-11-02**  
**源码位置**：opentelemetry-api/context + trace + propagation/W3CTraceContextPropagator  
**面试考点：27 题**  
**差异化点**：traceparent 四部分格式、**Inject/Extract 标准传播流程**、Parent-Child vs FollowsFrom 两种引用、SDK 四种 Sampler、**OpenTelemetry vs SkyWalking vs Zipkin 7 维度对比**
