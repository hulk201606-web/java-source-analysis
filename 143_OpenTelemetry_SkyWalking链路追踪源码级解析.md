# 分布式追踪 OpenTelemetry + SkyWalking 链路上下文传播源码级深度解析

> **源码基线**：OpenTelemetry Java SDK 1.40.x + SkyWalking Java Agent 9.2.x  
> **核心仓库**：open-telemetry/opentelemetry-java；apache/skywalking-java  
> **面试权重**：⭐⭐⭐⭐⭐（可观测性三大支柱之一，微服务排障必备）

---

## 一、分布式追踪核心概念

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 分布式追踪 = 一个请求跨多个服务的完整调用链               │
│                                                               │
│  场景：用户下单                                                │
│  Client → Gateway → OrderService → InventoryService          │
│                        └→ AccountService → MySQL             │
│                                                               │
│  Trace（一条完整链路）：                                       │
│  TraceId: abc123（全局唯一，贯穿所有服务）                    │
│  ├── Span[Gateway]        SpanId=g1, ParentId=空（Root）     │
│  │   └── Span[Order]      SpanId=o1, ParentId=g1             │
│  │       ├── Span[Inventory] SpanId=i1, ParentId=o1          │
│  │       └── Span[Account]   SpanId=a1, ParentId=o1          │
│  │           └── Span[MySQL]   SpanId=m1, ParentId=a1        │
│
│  ★ 核心概念（面试必背！）：                                    │
│   Trace：一次完整请求的所有 Span 组成的树                    │
│   Span：一次方法/服务调用（有开始结束时间、标签、事件）      │
│   TraceId：整条链路唯一 ID                                   │
│   SpanId：当前 Span 唯一 ID                                  │
│   ParentSpanId：父 Span ID（构成调用树）                    │
│   Context Propagation：上下文如何跨进程传播（本文重点！）   │
│
│  ★ 可观测性三大支柱：                                         │
│   Logs（日志）+ Metrics（指标）+ Traces（链路）             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、OpenTelemetry 上下文传播源码级

### 2.1 W3C TraceContext 传播

```java
// ★ OpenTelemetry 跨进程传播（源码级，面试核心！）
//
// 标准：W3C Trace Context（traceparent HTTP Header）
//
// HTTP 请求头格式：
//   traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
//                ↑  ↑ TraceId（32 hex）           ↑ SpanId（16 hex） ↑ flags
//                版本
//
//   tracestate: vendor1=value1,vendor2=value2   （厂商扩展）
//
// // opentelemetry-api/.../context/propagation/TextMapPropagator.java
// public interface TextMapPropagator {
//     // ★ 注入：把 Context 写进载体（HTTP Header / Kafka Header）
//     <C> void inject(Context context, C carrier, TextMapSetter<C> setter);
//
//     // ★ 提取：从载体读出 Context（下游服务收到请求时）
//     <C> Context extract(Context context, C carrier, TextMapGetter<C> getter);
// }
//
// // W3CTraceContextPropagator.inject() 核心逻辑：
// public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
//     SpanContext spanContext = Span.fromContext(context).getSpanContext();
//     if (!spanContext.isValid()) return;
//
//     // 拼接 traceparent 字符串
//     String traceparent = "00-" +
//         spanContext.getTraceId() + "-" +      // TraceId
//         spanContext.getSpanId() + "-" +       // 当前 SpanId（下游的 ParentId）
//         flags;                                 // sampled 标志位
//
//     // ★ 写入 HTTP Header
//     setter.set(carrier, "traceparent", traceparent);
// }
//
// // extract()：下游解析 Header → 构建远程 SpanContext
// // → 下游新建的 Span ParentId = 上游 SpanId
//
```

### 2.2 Context 线程内传播

```java
// ★ Context 线程内传播（源码级）
//
// // OpenTelemetry Context 基于 com.google.errorprone / 自定义 Context
// // 本质：Immutable Map + ThreadLocal 存储
//
// public interface Context {
//     <V> Context with(Key<V> key, V value);  // 返回新 Context（不可变）
//     <V> V get(Key<V> key);
//
//     static Context current();              // 取当前线程 Context
//     Scope makeCurrent();                   // 绑定到当前线程
// }
//
// // ★ 典型用法（try-with-resources 自动恢复）
// Span span = tracer.spanBuilder("createOrder").startSpan();
// try (Scope scope = span.makeCurrent()) {
//     // 这个范围内 Context.current() 能拿到 span
//     inventoryService.deduct();   // 下游同线程自动继承
// } finally {
//     span.end();
// }
//
// ★ 面试高频：线程切换 Context 丢失怎么办？
//   Context 存在 ThreadLocal → 线程池切换会丢
//   解决：
//   ① 提交任务时手动包装（wrap Runnable）
//   ② 用 OpenTelemetry 的 Context.taskWrapping 包装 ExecutorService
//   ③ 响应式（Reactor）用 ContextPropagation 钩子
//
```

---

## 三、SkyWalking Agent 字节码增强

### 3.1 Java Agent + 插件机制

```
┌─────────────────────────────────────────────────────────────┐
│  ★ SkyWalking 无侵入探针（面试高频！）                       │
│                                                               │
│  核心：Java Agent + ByteBuddy 字节码增强                     │
│  → 业务代码零改动（不用加注解、不用改 SDK）                 │
│
│  启动方式：                                                    │
│  java -javaagent:/path/skywalking-agent.jar                  │
│       -Dskywalking.agent.service_name=order-service          │
│       -Dskywalking.collector.backend_service=oap:11800       │
│       -jar order-service.jar                                 │
│
│  // apm-sniffer/apm-agent-core/.../SkyWalkingAgent.java     │
│  public class SkyWalkingAgent {
//      public static void premain(String args, Instrumentation inst) {
//          // ① 加载所有插件定义（skywalking-agent/plugins/*.jar）
//          PluginFinder pluginFinder = loadPlugins();
//
//          // ② ByteBuddy 注册 ClassFileTransformer
//          new AgentBuilder.Default()
//              .type(pluginFinder.buildMatch())        // 匹配要增强的类
//              .transform(new Transformer(pluginFinder))
//              .installOn(inst);
//      }
//  }
//
│  插件定义示例（mysql-8.x-plugin）：                           │
│  // 匹配 Connection.prepareStatement 方法                   │
│  // 增强：方法前创建 ExitSpan，方法后结束 Span               │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 SkyWalking 跨进程传播（SW8 Header）

```java
// ★ SkyWalking ContextCarrier 跨进程传播（源码级）
//
// SkyWalking 用自定义 Header：sw8（不是 W3C，新版兼容 w3c）
//
// HTTP Header：
//   sw8: <采样>-<traceId>-<父spanId>-<父服务实例>-<父服务>-<入口端点>
//
// 字段都是 Base64 编码，例如：
//   sw8: 1-MGY0...-YzY4...-c2VydmljZQ==-b3JkZXItc2VydmljZQ==-L2FwaS9vcmRlcg==
//
// // apm-agent-core/.../context/ContextCarrier.java
// public class ContextCarrier {
//     private String traceId;
//     private String traceSegmentId;  // 父 Segment
//     private int spanId;             // 父 Span
//     private String parentService;
//     private String parentEndpoint;
//
//     // ★ 客户端：注入到请求头
//     public void inject() {
//         // 序列化成 sw8 头
//     }
// }
//
// // 客户端插件增强（如 HttpClient 插件）：
// //   ① ContextManager.createExitSpan("InventoryService", carrier)
// //   ② carrier 序列化写入 HTTP Header "sw8"
// //
// // 服务端插件增强（如 Tomcat 插件）：
// //   ① 从 HTTP Header "sw8" 提取 ContextCarrier
// //   ② ContextManager.createEntrySpan("POST /order", carrier)
// //   ③ 建立父子关系（跨进程的 Segment 连接起来）
//
```

### 3.3 TraceSegment + OAP 拓扑拼接

```
┌─────────────────────────────────────────────────────────────┐
│  ★ SkyWalking 数据上报与拓扑构建（面试高频）                 │
│                                                               │
│  Agent 端概念（和 OTel 略有不同）：                          │
│   TraceSegment：一个 JVM 进程内一次请求产生的 Span 链        │
│   跨进程通过 ContextCarrier 关联多个 Segment                 │
│   → OAP 服务器把多个 Segment 拼成完整 Trace                 │
│                                                               │
│  上报：                                                        │
│   Agent → gRPC → OAP Server（默认 11800 端口）              │
│   OAP 做：                                                    │
│   ① Segment 拼接成完整 Trace（按 traceId + refs）          │
│   ② 流式分析生成服务拓扑（Service/Instance/Endpoint 指标）  │
│   ③ 指标写存储（H2/ES/BanyanDB）                            │
│   UI（默认 8080）查询展示 Trace、拓扑图、响应时间           │
│
│  Span 三种类型：                                              │
│   EntrySpan：入口（Tomcat 收到 HTTP 请求）                  │
│   LocalSpan：本地方法调用                                    │
│   ExitSpan：出口（调 DB、Redis、下游 HTTP）                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、OpenTelemetry vs SkyWalking 全景对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ OTel vs SkyWalking 面试必背对比表                         │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │ 维度       │ OpenTelemetry │ SkyWalking    │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 定位       │ 厂商中立标准   │ APM 全家桶     │                │
│  │            │ + 采集 SDK     │（探针+存储+UI）│                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 埋点方式    │ SDK/API 注解  │ ★ Java Agent  │                │
│  │            │ + 自动探针     │ 字节码增强     │                │
│  │            │               │（零侵入）      │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 传播协议    │ W3C traceparent│ sw8（兼容 w3c）│               │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 后端       │ 不带后端       │ OAP + UI 自带  │                │
│  │            │（接 Jaeger/    │（BanyanDB/ES）│                │
│  │            │ Tempo/...）    │                │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ Metrics/Log│ ✅ 三支柱统一  │ 主要 Trace+    │                │
│  │            │               │ Metrics        │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 语言覆盖    │ 多语言最全     │ Java 最强      │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 采样       │ Head/Tail 采样│ 采样 + 采样率  │                │
│  └──────────┴──────────────┴──────────────┘                │
│
│  ★ 典型组合（2024-2025 趋势）：                              │
│   SkyWalking Agent → 转 OTLP 协议 → 统一可观测平台          │
│   或 OTel Collector 作为统一网关：                           │
│   App → OTel Collector → Tempo(Trace) / Prom(Metric) / Loki(Log)│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、采样机制

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 链路采样（面试高频！）                                     │
│                                                               │
│  全量采集成本太高（100% 请求都存 Trace）                     │
│  → 采样：只保留一部分链路                                    │
│                                                               │
│  ① Head Sampling（头部采样）：                                │
│     请求入口就决定采不采（看 traceparent flags）            │
│     → 全链路一致（都采或都不采），但可能漏掉异常链路         │
│     OTel：ParentBased(TraceIdRatioBased(0.1)) 采 10%        │
│                                                               │
│  ② Tail Sampling（尾部采样）：                                │
│     整条 Trace 完成后根据特征决定：                          │
│     → 错误的全采、慢请求全采、正常请求按比例采              │
│     → 需要 Collector 缓存完整 Trace（内存开销）             │
│     → OTel Collector Tail-Based Sampling Processor          │
│
│ ★ 面试高分点：生产推荐 Tail Sampling                         │
│   错误链路 100% 保留（排障价值最高）+ 正常链路采样           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Trace/Span 关系？| **一个 Trace 一棵树**；Span 是单次调用；TraceId 全局唯一；ParentSpanId 构成树 |
| 2 | 跨进程怎么传播？| **HTTP Header 注入/提取**：OTel 用 W3C traceparent；SkyWalking 用 sw8 |
| 3 | traceparent 格式？| `00-TraceId(32hex)-SpanId(16hex)-flags` |
| 4 | 上下文注入/提取接口？| TextMapPropagator.inject() / extract() |
| 5 | 线程内怎么传播？| Context 存 ThreadLocal + makeCurrent()；try-with-resources 自动恢复 |
| 6 | 线程池切换丢上下文？| 手动 wrap Runnable / Context.taskWrapping 包装 Executor |
| 7 | SkyWalking 零侵入原理？| **Java Agent premain + ByteBuddy 字节码增强**，-javaagent 启动 |
| 8 | SkyWalking sw8 头？| 采样-TraceId-SegmentId-SpanId-父服务实例-父服务-入口端点（Base64）|
| 9 | ContextCarrier？| SkyWalking 跨进程载体；客户端 createExitSpan 注入；服务端 createEntrySpan 提取 |
| 10 | Span 三类型？| **EntrySpan（入口）+ LocalSpan（本地）+ ExitSpan（出口）** |
| 11 | OAP 做什么？| 接收 gRPC Segment → 拼接完整 Trace → 生成拓扑/指标 → 写 ES/BanyanDB |
| 12 | OTel vs SkyWalking？| OTel 中立标准+三支柱+不带后端；SkyWalking APM 全家桶+Java Agent 零侵入 |
| 13 | 可观测性三支柱？| Logs + Metrics + Traces |
| 14 | Head vs Tail 采样？| Head 入口决定（一致但可能漏异常）；Tail 完成后按错误/慢采（排障好，需缓存）|
| 15 | 面试最高频考点排序？| **1. 上下文传播原理 2. traceparent/sw8 3. Agent 字节码增强 4. OTel vs SkyWalking 5. 采样** |
| 16 | 学习路径？| TextMapPropagator → Context ThreadLocal → SkyWalkingAgent premain → ContextCarrier → OAP |
| 17 | 面试常见追问？| 异步线程链路断了怎么办？→ 包装任务传递 Context；MQ 链路怎么串？→ Header 透传 traceId |
| 18 | Kafka 怎么传播？| 把 traceparent 放进 Kafka Record Headers，消费端提取 |
| 19 | OTel Collector 作用？| 统一接收/处理/导出（批量、采样、脱敏）；解耦应用和后端 |
| 20 | Baggage 是什么？| 和 TraceContext 一起传播的业务 KV（如 userId、租户 ID），全链路可用 |
| 21 | 面试高频场景？| 微服务调用链排障 + 慢请求定位 + 服务拓扑自动发现 + 错误链路聚合 |
| 22 | 面试陷阱？| 说 TraceId 靠 ThreadLocal 跨服务 → ❌ ThreadLocal 只在单 JVM；跨服务必须 Header 传播 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码**：opentelemetry-java context/propagation + skywalking-java SkyWalkingAgent/ContextCarrier  
**面试考点：22 题**  
**差异化点**：W3C traceparent 注入提取 + ThreadLocal 线程传播 + SkyWalking 字节码增强 + sw8 + Head/Tail 采样
