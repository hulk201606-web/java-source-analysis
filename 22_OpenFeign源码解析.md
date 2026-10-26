# OpenFeign 源码深度解析（声明式调用）

> **源码基线**：spring-cloud-openfeign 3.1.x（Spring Cloud 2021.0.8 配套）  
> **核心仓库**：spring-cloud/spring-cloud-openfeign  
> **大纲来源**：19_SpringCloud源码调研大纲.md P0-2  
> **面试权重**：⭐⭐⭐⭐⭐（国内高频）

---

## 一、OpenFeign 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  OpenFeign 三组件                                           │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ @EnableFeignClients                                   │    │
│  │   → 开启 Feign Client 扫描                            │    │
│  │   → Import + 注册 FeignClientFactoryBean 扫描器       │    │
│  └─────────────────────────────────────────────────────┘    │
│                           ↓                                   │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ @FeignClient(name="user-service")                    │    │
│  │   → 标注 Feign Client 接口                            │    │
│  │   → FeignClientFactoryBean.getObject 创建动态代理      │    │
│  └─────────────────────────────────────────────────────┘    │
│                           ↓                                   │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Feign.Builder                                         │    │
│  │   → 组装 Feign Client（Contract/Client/Decoder 等）   │    │
│  │   → ReflectiveFeign.newInstance 创建 JDK Proxy        │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
│ ★ 底层技术栈：JDK Dynamic Proxy + Contract 注解解析 + Client HTTP 执行│
│ ★ OpenFeign = Netflix Feign 核心 + Spring Boot 自动配置      │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路（完整调用栈 ★ 大纲锚点）

### 2.1 FeignClientFactoryBean.getObject（★ 大纲锚点）

```java
// ★ FeignClientFactoryBean.getObject
//   → FactoryBean 钩子：创建 Feign Client 代理对象
//
// 源码链路：
//   FeignClientFactoryBean.getObject()
//     → Feign.Builder builder = context.getBean(Feign.Builder.class)
//     → builder.logLevel(logLevel).decode()
//     → **ReflectiveFeign.newInstance(target) → 动态代理**
//     → 返回代理对象注册到 Spring 容器
//
// ★ FactoryBean 模式：
//   getObject() 返回代理对象（不是 FeignClientFactoryBean 本身）
//   → 注入 @FeignClient 接口时实际拿到的是 JDK Proxy
//   → Spring 启动时每个 @FeignClient 接口都会走一遍 getObject()
```

### 2.2 SynchronousMethodHandler.invoke（★★★★★ 核心执行入口）

```java
// ★ SynchronousMethodHandler.invoke —— 大纲要求的核心考点
//   → OpenFeign 方法调用的**完整执行链路**
//
// 用户代码：
//   UserClient.getUserById(123);
//     ↓
//   1. JDK Proxy.invoke → FeignInvocationHandler.dispatch()
//     ↓
//   2. ReflectiveFeign.FeignInvocationHandler.dispatch()
//     → 找到 Method → 查同步 MethodHandler → 调用 handler.invoke()
//     ↓
//   3. **SynchronousMethodHandler.invoke(Args)** ★ 核心！
//     ↓
//   4. 遍历 RequestTemplate.Factory：
//     for (RequestTemplate.Factory factory : factories) {
//         // Contract 解析注解 → 填充 MethodMetadata
//         // ParamExpander 展开参数值
//         // QueryTemplate / BodyTemplate 模板替换
//         template = factory.create(args, requestTemplate);
//     }
//     → 最终构造出完整的 RequestTemplate
//     ↓
//   5. Client.execute(request, options)
//     → 默认 Client.Default → HttpURLConnection（慢！生产必须替换）
//     → RibbonClient 负载均衡调用（spring-cloud-starter-loadbalancer）
//     → ApacheHttpClient5 / OkHttpClient 高性能替换
//     ↓
//   6. ResponseDecoder.decode(response, returnType)
//     → 反序列化响应成方法返回值
//     → 默认 JacksonDecoder
//     ↓
//   7. 返回给用户
//
// ★ 一句话总结 SynchronousMethodHandler：
//   **注解 → MethodMetadata → RequestTemplate → Client.execute → Decoder**
```

### 2.3 Contract 注解解析

```java
// ★ Contract.Default（spring-cloud-openfeign-core 自带）
//   → 解析 Spring MVC 注解（@GetMapping/@PostMapping/@RequestParam/@RequestBody）
//   → 生成 MethodMetadata
//
// MethodMetadata 核心字段：
//   public class MethodMetadata {
//       private String url;                // 请求路径（带 {param} 占位符）
//       private RequestTemplate.HttpMethod httpMethod; // GET/POST/PUT/DELETE
//       private Map<Integer, ParamExpander> indexToParamExpander; // 参数展开器
//       private Integer bodyIndex;         // Body 参数索引
//       private Class<?> returnType;       // 返回类型
//       private Collection<String> fallbackNames; // Fallback 类名
//   }
//
// ★ ParamExpander 参数展开：
//   @RequestParam("userId") Long userId
//   → ParamExpander 实现：把 userId 值填到 url 的 {userId} 占位符
//   → QueryTemplate 模板替换
//
// ★ BodyTemplate 模板填充：
//   @RequestBody User user
//   → BodyTemplate 实现：把 user 序列化成 JSON 放进 Request Body
//   → BodyTemplate 是 String.format 风格模板
```

### 2.4 Client 执行 + Ribbon 集成（★★★★★ 大纲要求）

```java
// ★ Client.execute 是真正发 HTTP 请求的地方
//
// Client 三种实现（面试高频）：
//
// ① Client.Default（默认，HttpURLConnection）
//   → 慢！每次都新建 TCP 连接 → 不适合生产
//   → 代码最少，性能最差
//
// ② LoadBalancerFeignClient（Spring Cloud 默认，大纲要求）
//   → 集成 Ribbon（或 Spring Cloud LoadBalancer）
//   → 执行流程：
//     a. LoadBalancerClient.choose(serviceId)
//     b. IRule.choose() → 选一个 Server（ZoneAvoidanceRule 默认）
//     c. 把 URL 里的 serviceId 替换成 Server 的 host:port
//     d. 执行真正的 HTTP 请求（ApacheHttpClient/OkHttp）
//   → 这就是 OpenFeign + Ribbon 负载均衡的核心！
//
// ③ ApacheHttpClient5 / OkHttpClient（生产推荐）
//   → 连接池 + keepalive + 异步
//   → 比 HttpURLConnection 快 5-10x
//   → 需要引入 starter 依赖
//
// ★ OpenFeign 与 Ribbon 分工：
//   OpenFeign：**注解解析 + 构造 RequestTemplate + 执行调用**
//   Ribbon：**负载均衡（IRule 选 Server）+ IPing 健康检查**
//   → Feign 内部 Client 层集成 Ribbon（不是用户手动调用）
```

### 2.5 RequestInterceptor（请求拦截器）

```java
// ★ RequestInterceptor 扩展点
//   → 在 RequestTemplate 构造完成后、Client.execute 之前执行
//   → 可以给请求加 Header / 改 URL / 加认证信息
//
// 常见用途：
// ① 加认证 Header（Bearer Token）
// ② 加 TraceId（Sleuth/Brave）
// ③ 加租户 ID（多租户系统）
// ④ GzipRequestInterceptor（请求压缩）
//
// 示例：
//   public class AuthRequestInterceptor implements RequestInterceptor {
//       @Override
//       public void apply(RequestTemplate template) {
//           template.header("Authorization", "Bearer " + token);
//       }
//   }
```

---

## 三、面试考点（15 题，★ 大纲要求）

| # | 考点 | 答案 |
|---|------|------|
| 1 | OpenFeign 为什么能用注解调用？| **JDK Dynamic Proxy** → `@FeignClient` 接口被 `FeignClientFactoryBean.getObject()` 包装成动态代理；调用方法时代理拦截，执行 HTTP 请求 |
| 2 | FeignClientFactoryBean.getObject 做什么？| FactoryBean 钩子 → 创建 Feign.Builder → `ReflectiveFeign.newInstance()` 创建 JDK Proxy → 注册到 Spring 容器 |
| 3 | SynchronousMethodHandler.invoke 核心？| **注解解析成 MethodMetadata → 构造 RequestTemplate → Client.execute → Decoder 解码响应**；OpenFeign 最核心的方法！|
| 4 | RequestTemplate 构造流程？| Contract.Default 解析注解 → ParamExpander 展开参数 → QueryTemplate/BodyTemplate 模板替换 → 最终 RequestTemplate |
| 5 | 默认 HTTP Client 是什么？| **HttpURLConnection（Client.Default）** → 慢！生产必须替换成 ApacheHttpClient5 或 OkHttp |
| 6 | OpenFeign 默认 Ribbon 负载均衡？| **是的，Feign 内部 LoadBalancerFeignClient 自动集成** → serviceId 替换成 host:port → 发真实请求 |
| 7 | OpenFeign 默认 IRule？| **ZoneAvoidanceRule**（Zone 级故障转移，比 RoundRobin 更健壮）|
| 8 | 请求压缩怎么做？| **GzipRequestInterceptor 实现 RequestInterceptor** → 替换 Client → 加 gzip 编码 → 服务端解压 |
| 9 | Fallback 降级？| `@FeignClient(fallback = UserClientFallback.class)` → 实现 UserClient 接口 → 方法返回默认值 → fallbackFactory 可接异常 |
| 10 | 超时配置？| `connectTimeout` + `readTimeout`（默认 60s）；Ribbon 也有单独超时配置；Feign 超时比 Ribbon 高才生效 |
| 11 | OpenFeign vs RestTemplate？| OpenFeign：**声明式注解 + 接口抽象**；RestTemplate：命令式 + 手动拼 URL；Feign 代码更干净，接口可 Mock |
| 12 | Spring MVC 注解兼容？| **Contract.Default 同时解析 Spring MVC（@GetMapping/@RequestParam）和原生 Feign（@RequestLine/@Param）注解** |
| 13 | 异步调用？| Spring Cloud 2020+ 支持 `CompletableFuture` 返回值（AsyncMethodHandler）；原生 Feign 也支持 |
| 14 | OpenFeign 学习路径？| @FeignClient 写接口 → 调用看同步请求 → 源码看 SynchronousMethodHandler → Ribbon 集成 → Fallback → RequestInterceptor |
| 15 | OpenFeign vs Dubbo？| OpenFeign：**HTTP + JSON + 声明式 + 跨语言**；Dubbo：TCP + Hessian2 + RPC + Java 生态；微服务跨语言选 Feign |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：spring-cloud-openfeign 3.1.x  
**面试考点：15 题**  
**大纲来源**：19_SpringCloud源码调研大纲.md P0-2
