# Apache SkyWalking 源码深度解析（APM 链路追踪）

> **源码基线**：Apache SkyWalking 10.x（最新稳定）  
> **核心仓库**：https://github.com/apache/skywalking  
> **面试权重**：⭐⭐⭐⭐（微服务可观测性高频）

---

## 一、SkyWalking 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式系统可观测性平台（APM）                     │
│  三大支柱：Tracing（链路追踪）+ Metrics（指标）+ Logging（日志）│
│                                                               │
│  和 Zipkin / Jaeger / Pinpoint 对比：                         │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │ SkyWalking   │ Zipkin/Jaeger  │ Pinpoint    ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 探针语言      │ Java/Go/Python│ Java/Go/...   │ Java only   ││
│  │ 数据上报      │ gRPC（高效）  │ HTTP/gRPC     │ HTTP         ││
│  │ UI 能力       │ ★★★★★ 强    │ ★★★ 中等      │ ★★★★ 强    ││
│  │ Metrics       │ ★★★★★ 内置   │ ❌ 靠 Prometheus│ ★★★★ 内置   ││
│  │ Service Mesh  │ ★★★★★ 支持   │ ❌ 不支持      │ ❌ 不支持     ││
│  │ 社区          │ Apache Top   │ Netflix/Uber   │ 韩国团队     ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│  ★ 国内微服务首选 SkyWalking！                                │
│  → 阿里/腾讯/京东/美团/字节跳动内部都在用                     │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  架构组件                                                     │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │  Agent（探针）  │ OAP Server   │ UI（可视化）  │             │
│  │  嵌入业务应用  │ 数据处理/存储 │ 网页展示     │             │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         │ gRPC          │ gRPC          │ HTTP               │
│         ↓               ↓               ↓                    │
│  Tracer/ContextCarrier  AnalysisCore    Vue Dashboard        │
│  自动埋点 + 手动埋点    OAL 计算 + 拓扑  Trace 视图/火焰图     │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  和 Sleuth 对比（Spring Cloud 链路追踪）                     │
│                                                               │
│  Sleuth：Spring Cloud 全家桶内置                              │
│  → 基于 Brave（X-B3-TraceId/ SpanId 透传）                   │
│  → 自动埋点 Spring MVC / Feign / JDBC                        │
│  → 轻量但功能有限（只有 Tracing）                             │
│                                                               │
│  SkyWalking：独立 APM 平台                                    │
│  → Java Agent 字节码增强（不侵入业务代码）                     │
│  → 自动埋点 Java 生态所有框架（Spring/Dubbo/GRPC/MySQL...）    │
│  → Tracing + Metrics + Logging + Service Mesh                │
│  → 重但功能全                                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Java Agent 字节码增强（★★★★★ 核心）

```java
// ★ SkyWalking 用 Java Agent + ASM 做字节码增强！
//   不修改业务代码 → JVM 启动时加载 Agent → 自动增强
//
// -javaagent:skywalking-agent.jar
//   ↓
// SkyWalking Agent Premain-Class: org.apache.skywalking.apm.agent.SkyWalkingAgent
//   ↓
// Instrumentation.addTransformer(new ClassFileTransformer())
//   ↓
// 每个类加载时：ClassFileTransformer.transform()
//   → ASM 修改字节码 → 插入 Span 埋点
//   ↓
// 业务类的方法前后自动加上：
//   Span span = TracerManager.createEntrySpan(...);  // 方法开始
//   span.tag(...);                                  // 设置标签
//   span.error(...);                                // 异常标记
//   span.finish();                                  // 方法结束
//
// ★ 支持的自动埋点框架（核心）：
//   Spring MVC / WebFlux
//   Dubbo / gRPC / OkHttp
//   MySQL / PostgreSQL / Redis / RocketMQ / Kafka
//   ThreadPoolExecutor / ScheduledExecutorService
//
// ★ 自定义插件：
//   新增 META-INF/skywalking-plugin.def → 声明 ClassEnhancePluginDefine 实现
//   → 定义要增强的类 + 方法 → 实现 InstrumentationPoints
```

### 2.2 Tracer + Span 核心

```java
// ★ Tracer 是链路追踪的核心（基于 W3C TraceContext 标准）
//
// 三个核心概念：
//   ① Trace（全链路）：一个请求从开始到结束
//      → traceId（唯一标识，全局唯一）
//      → 由多个 Span 组成（树状结构）
//
//   ② Span（跨度）：一个方法/操作
//      → spanId（唯一标识，Trace 内唯一）
//      → parentSpanId（父 Span ID，构成树）
//      → operationName（操作名，如 GET /api/user）
//      → startTime / endTime / latency
//      → tags（标签，如 db.url、http.status_code）
//      → logs（日志，异常堆栈等）
//
//   ③ ContextCarrier（跨进程上下文传递）
//      → 类似 Sleuth 的 X-B3-TraceId / X-B3-SpanId
//      → SkyWalking 自己的 Header：sw8
//      → 或兼容 W3C traceparent / tracestate
//
// ★ Span 五种类型（面试高频）：
//   ① Entry Span（入口）：服务收到请求的第一个 Span（如 Controller 方法）
//   ② Exit Span（出口）：服务调用外部的 Span（如 Feign / Dubbo / MySQL）
//   ③ Local Span（本地）：内部方法调用
//   ④ MQ Span：消息队列发送/消费
//   ⑤ Virtual Span：虚拟调用（线程池异步任务）
//
// ★ Span 生命周期：
//   Entry Span → Local Span（可以嵌套多层）→ Exit Span
//     → 调用其他服务 → 对方服务又生成 Entry Span
//     → 跨进程通过 ContextCarrier 传递 traceId/spanId
```

### 2.3 ContextCarrier 跨进程传递

```java
// ★ ContextCarrier 是跨进程传递链路上下文的核心！
//
// 客户端（Service A）发请求 → 服务端（Service B）处理
//
// Service A：
//   Span exitSpan = TracerManager.createExitSpan("http://url", contextCarrier);
//   exitSpan.start();
//   httpRequest.setHeader("sw8", contextCarrier.serialize());  // ★ 注入到 HTTP Header
//   httpClient.execute();
//   exitSpan.finish();
//
// Service B：
//   String sw8 = httpRequest.getHeader("sw8");
//   ContextCarrier contextCarrier = ContextCarrier.deserialize(sw8);
//   Span entrySpan = TracerManager.createEntrySpan("GET /api/user", contextCarrier);
//   entrySpan.start();
//   // 业务处理
//   entrySpan.finish();
//
// ★ 为什么用 ContextCarrier？
//   跨 JVM 不能直接在 ThreadLocal 里放 Span（ThreadLocal 只在当前 JVM 有效）
//   → 必须通过网络 Header 传递 traceId/spanId
//   → 这就是 Sleuth / SkyWalking 都要注入 Header 的原因！
//
// ★ SkyWalking Header 格式（简化）：
//   sw8: 1-<traceId>-<parentSpanId>-<sampled>-<endpoint>
//   → W3C 标准：traceparent: 00-<traceId>-<spanId>-<flags>
```

### 2.4 OAL（Observability Analysis Language，★ 特色）

```
// ★ OAL 是 SkyWalking 独创的分析语言（类似 SQL，但面向可观测性数据）
//   OAP Server 用 OAL 做实时计算 → 生成 Metrics
//
// OAL 示例：
//
// ① 服务总请求数
//   service_calls = from Service | sum(Count.service_calls)
//
// ② 服务平均响应时间
//   service_resp_time = from Service | avg(Duration.service_resp_time)
//
// ③ Top 10 慢接口
//   top_slow_endpoints = from Endpoint | sort by Duration.endpoint_resp_time desc | top 10
//
// ④ 数据库调用统计
//   db_calls = from Database | sum(Count.db_calls)
//   db_resp_time = from Database | avg(Duration.db_resp_time)
//
// ★ OAL 强大之处：
//   ① 声明式（像 SQL 一样写，不用写 Java 代码）
//   ② 实时计算（基于流式处理引擎）
//   ③ 结果自动生成 Metrics → 可被 Prometheus 抓取
//
// ★ 和 PromQL 对比：
//   PromQL：查询式（查已存储的 Metrics）
//   OAL：流式计算式（实时处理 Trace → 生成 Metrics）
//   → OAL 功能更强，但也更复杂
```

### 2.5 OAP Server + Storage

```
// ★ OAP Server 是 SkyWalking 的处理大脑
//
// 核心组件：
//   AnalysisCore：流式分析引擎（接收 Agent 上报的 Trace/Span → OAL 计算）
//   Storage：存储后端（Elasticsearch / BanyanDB / H2 内存）
//   gRPC Server：接收 Agent 数据
//
// Storage 选择：
//   小规模（< 100 节点）：H2（内存，测试用）或 BanyanDB（轻量）
//   中规模（100-1000 节点）：Elasticsearch 7.x
//   大规模（> 1000 节点）：BanyanDB（Apache 自研，分布式时序数据库）
//
// ★ BanyanDB 是什么？
//   Apache SkyWalking 团队自研的分布式时序数据库
//   → 专门存储 Trace + Metrics + Logs
//   → 比 Elasticsearch 更适合 APM 场景
//   → 5.x 开始 SkyWalking 默认推荐 BanyanDB
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | SkyWalking 和 Sleuth 区别？| Sleuth：**Spring Cloud 内置，轻量，只有 Tracing**（Brave X-B3）；SkyWalking：**Java Agent 字节码增强 + Tracing + Metrics + Logging + Service Mesh** |
| 2 | SkyWalking 怎么埋点？| **Java Agent + ASM 字节码增强**（-javaagent:skywalking-agent.jar）→ JVM 启动时修改类字节码 → 不侵入业务代码 |
| 3 | Span 五种类型？| **Entry Span（入口）/ Exit Span（出口）/ Local Span（本地）/ MQ Span（消息队列）/ Virtual Span（线程池异步）** |
| 4 | ContextCarrier 作用？| **跨进程传递链路上下文**（traceId/parentSpanId）→ 注入 HTTP Header（sw8）或 Dubbo Attachment → 对方服务从 Header 提取并创建 Entry Span |
| 5 | SkyWalking Header 格式？| sw8: 1-\<traceId\>-\<parentSpanId\>-\<sampled\>-\<endpoint\>；兼容 W3C 标准：traceparent |
| 6 | OAL 是什么？| **SkyWalking 独创的可观测性分析语言**（类似 SQL）→ from Service \| sum(Count.service_calls) → 实时生成 Metrics |
| 7 | SkyWalking vs Zipkin/Jaeger？| SkyWalking：**全栈 APM（Tracing+Metrics+Logging）+ Java Agent + Service Mesh**；Zipkin/Jaeger：只有 Tracing，靠 Prometheus 做 Metrics |
| 8 | SkyWalking OAP Server？| **处理大脑** → AnalysisCore（OAL 计算）+ Storage（ES/BanyanDB）+ gRPC Server（接收 Agent 数据）|
| 9 | BanyanDB 是什么？| SkyWalking 团队自研的**分布式时序数据库** → 专门存储 Trace/Metrics/Logs → 比 ES 更适合 APM → 5.x 默认推荐 |
| 10 | SkyWalking Metrics 和 Prometheus？| OAL 实时生成 Metrics → OAP Server 暴露 /metrics → Prometheus 抓取 → Grafana 展示 |
| 11 | SkyWalking Service Mesh？| 支持！Istio + SkyWalking → 服务网格中的 Sidecar 自动上报 Span |
| 12 | SkyWalking 自动埋点框架？| Spring MVC / Dubbo / gRPC / OkHttp / MySQL / Redis / RocketMQ / Kafka / ThreadPoolExecutor |
| 13 | 自定义 SkyWalking 插件？| 实现 ClassEnhancePluginDefine → 定义要增强的类/方法 → 实现 InstrumentationPoints → 加到 META-INF/skywalking-plugin.def |
| 14 | SkyWalking UI 功能？| Trace 树状视图 / 火焰图 / 服务拓扑图 / 慢接口列表 / 错误率统计 / Metrics 曲线 |
| 15 | 异步任务链路追踪？| ThreadPoolExecutor 插件 → 线程池提交 Runnable/Callable → 自动把 Parent Span 上下文传递到子线程 |
| 16 | SkyWalking vs Pinpoint？| Pinpoint：韩国团队，Java only；SkyWalking：Apache 顶级项目，多语言 + Service Mesh + 国内社区活跃 |
| 17 | SkyWalking 采样？| 支持四种采样策略：**Const（固定）/ Random（随机）/ Rate（按比例）/ Threshold（按 QPS）**；采样影响精度但减少存储 |
| 18 | SkyWalking 学习路径？| ① 体验（Docker 一键部署 SkyWalking + Demo 应用）② 看 Trace 树 + 火焰图 ③ 理解 Java Agent + ASM 字节码 ④ OAL 自定义 Metrics |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache SkyWalking 10.x  
**面试考点：18 题**
