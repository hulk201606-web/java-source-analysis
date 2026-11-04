# Apache Dubbo 源码深度解析（阿里系 RPC）

> **源码基线**：Apache Dubbo 3.3.x（最新稳定）  
> **核心仓库**：https://github.com/apache/dubbo  
> **面试权重**：⭐⭐⭐⭐⭐（阿里系高频）

---

## 一、Dubbo 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：高性能 Java RPC 框架                                │
│                                                               │
│  三大角色：                                                    │
│  Provider（服务提供者）→ Consumer（服务消费者）→ Registry（注册中心）│
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    Registry                              │ │
│  │               (Nacos / ZooKeeper)                       │ │
│  └──────────────────────┬──────────────────────────────────┘ │
│                         │ 注册 + 订阅                         │
│         ┌───────────────┴───────────────┐                    │
│         ↓                               ↓                    │
│  ┌──────────────┐              ┌──────────────┐             │
│  │   Provider    │              │   Consumer    │             │
│  │  暴露服务      │ ←—— RPC ——→  │  消费服务      │             │
│  └──────────────┘              └──────────────┘             │
│         ↓                               ↓                    │
│  ┌──────────────┐              ┌──────────────┐             │
│  │  Filter 链    │              │  Filter 链    │             │
│  │  + Netty 通信  │              │  + Netty 通信  │             │
│  └──────────────┘              └──────────────┘             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Provider 端启动流程                                          │
│                                                               │
│  @DubboService 注解 → ServiceBean.afterPropertiesSet          │
│    → ① buildInvokerChain：包装 Filter 链（invoke 过程拦截）  │
│    → ② Protocol.export：暴露服务                                │
│       → DubboProtocol.export → NettyServer 启动监听端口      │
│    → ③ Registry.register：注册服务到 Nacos / ZooKeeper       │
│    → ④ 启动就绪                                               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Consumer 端调用流程（★ 面试高频！）                           │
│                                                               │
│  @DubboReference 注解 → ReferenceBean.getObject               │
│    → ① JDK Proxy 生成消费端代理                                │
│    → ② Protocol.refer：引用服务                                │
│       → DubboProtocol.refer → 从 Registry 拉 Provider 列表   │
│    → ③ Cluster：集群容错（Failover / Failfast / Failsafe）     │
│    → ④ LoadBalance：负载均衡（Random / RoundRobin）            │
│    → ⑤ Filter 链：ConsumerContextFilter / ActiveLimitFilter    │
│    → ⑥ NettyClient：发 RPC 请求                                │
│    ↓                                                          │
│  Consumer 调用代理方法 → InvocationHandler.invoke             │
│    → ClusterInvoker.invoke → Filter 链 → DubboInvoker          │
│    → NettyClient.send → Provider 处理 → 返回结果               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Filter 链（★★★★★ 核心设计）

```java
// ★ Dubbo 所有功能都基于 Filter 链！
//   和 Sentinel Slot 链、Gateway Filter 链设计模式相同
//
// Provider 端 Filter 链（按执行顺序）：
//   EchoFilter → ClassLoaderFilter → GenericFilter → ContextFilter
//   → ExceptionFilter → TraceFilter → TimeoutFilter → ...
//   → 业务 Service.invoke
//
// Consumer 端 Filter 链：
//   ConsumerContextFilter → ActiveLimitFilter → FutureFilter
//   → MonitorFilter → ProtocolFilter → ...
//   → ClusterInvoker（负载均衡 + 容错）
//
// ★ Filter 接口定义：
//   Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException;
//   默认实现 AbstractFilter → invoke 里做处理 → 调用 invoker.invoke(invocation)
//   → 洋葱模型（前面拦截，后面返回）
//
// ★ ProtocolFilterWrapper 自动组装 Filter 链：
//   Protocol.export / Protocol.refer
//     → ProtocolFilterWrapper 包装
//     → 自动扫描 META-INF/dubbo/org.apache.dubbo.rpc.Filter
//     → ExtensionLoader 加载所有 Filter 实现
//     → 按 order 排序 → 链式包装 Invoker
//     → 最终返回一个 Invoker（链尾是真正的服务调用）
```

### 2.2 DubboInvoker → NettyClient 通信

```java
// ★ DubboInvoker 是 Consumer 发送 RPC 的核心
//
// invoke(Invocation invocation) 流程：
//   1. 从 Filter 链出来 → 到达 DubboInvoker.invoke
//   2. ExchangeClient currentClient = getClients(isInvokeAsync, total)
//      → 获取 NettyClient 实例（可能多连接）
//   3. request = currentClient.request(inv, timeout)
//      → Netty 异步发送 RPC 请求
//      → 返回 CompletableFuture / ResponseFuture
//   4. if (isInvokeAsync) return AsyncRpcResult（异步）
//      else return currentClient.request(...).get()（同步等待）
//
// ★ 底层 Netty Handler 链（和所有 Netty 框架一样）：
//
//   Consumer 端 NettyClient：
//     MultiMessageHandler → HeartbeatHandler → AllChannelHandler
//     → DecodeHandler → HeaderExchangeHandler → requestHandler
//
//   Provider 端 NettyServer：
//     同上 Handler 链 → HeaderExchangeHandler.received
//     → DubboProtocol.received → Filter 链 → Service.invoke
//
// ★ Dubbo 自定义协议（TCP 二进制帧）：
//   ┌──────┬──────┬────────────────────────────────────┐
//   │ Magic │ Flags │  Payload（序列化的 Invocation）      │
//   │ 2B    │ 1B    │                                     │
//   └──────┴──────┴────────────────────────────────────┘
//   → 比 HTTP 更轻量（无需 Header 解析 + Gzip 等）→ 性能更高
```

### 2.3 Cluster 集群容错（★★★★）

```java
// ★ Cluster 是服务调用的容错策略
//   Dubbo 内置 5 种（默认 Failover）：
//
// 1. FailoverCluster ★★★★★ 默认！自动重试
//    → 调用失败自动切下一个 Provider
//    → 默认重试 2 次（retries=2）
//    → 适合幂等操作（查询接口）
//
// 2. FailfastCluster 快速失败
//    → 调用失败立即抛异常，不重试
//    → 适合非幂等操作（支付接口）
//
// 3. FailsafeCluster 安全失败
//    → 调用失败返回空结果或降级
//    → 适合不重要的通知接口（短信/邮件）
//
// 4. FailbackCluster 失败自动恢复
//    → 调用失败异步重试
//    → 后台定时器补偿
//
// 5. ForkingCluster 并行调用
//    → 并行调用多个 Provider
//    → 第一个成功就返回
//    → 适合实时性要求高的场景（监控/报警）
//
// ★ Cluster 内部配合 LoadBalance：
//   FailoverCluster.invoke(invocation)
//     → 遍历重试次数：
//       LoadBalance.select(providers, invocation) → 选一个 Provider
//       → invoker.invoke(invocation) → 调用
//       → 失败 → 下一轮重试
//       → 成功 → 返回
```

### 2.4 LoadBalance 负载均衡

```java
// ★ Dubbo 内置 4 种 LoadBalance（默认 Random）：
//
// 1. RandomLoadBalance ★ 默认！随机
//    → ThreadLocalRandom.nextInt(providerCount)
//    → 简单高效，概率均匀
//
// 2. RoundRobinLoadBalance 轮询
//    → AtomicInteger + LongAdder（和 JDK Striped64 类似优化）
//    → 平滑轮询（避免集中打到某个 Provider）
//
// 3. LeastActiveLoadBalance 最少活跃调用
//    → 选 active 请求数最少的 Provider
//    → 和 Ribbon BestAvailableRule 类似
//
// 4. ConsistentHashLoadBalance 一致性哈希
//    → 相同 key（如 userId）固定路由到同一个 Provider
//    → Provider 增减时只影响相邻节点
```

### 2.5 注册中心（Nacos / ZooKeeper）

```java
// ★ Provider 端注册：
//   ServiceBean.afterPropertiesSet → Registry.register
//     → ZookeeperRegistry / NacosRegistry
//     → 创建节点 /dubbo/services/com.example.OrderService/providers/...
//
// ★ Consumer 端订阅：
//   ReferenceBean.getObject → Registry.subscribe
//     → 监听服务列表变更
//     → Provider 上下线实时推送到 Consumer
//     → Consumer 更新本地 invoker 列表
//
// ★ 和 Nacos 集成：
//   spring.cloud.nacos.discovery.server-addr=...
//   dubbo.registry.address=nacos://nacos-server:8848
//   → Provider 注册到 Nacos，Consumer 从 Nacos 订阅
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Dubbo 三大角色？| **Provider（服务提供者）/ Consumer（服务消费者）/ Registry（注册中心）** |
| 2 | Consumer 调用链路？| 代理方法 → InvocationHandler → **ClusterInvoker（Failover）→ LoadBalance（选 Provider）→ Filter 链 → DubboInvoker → NettyClient** |
| 3 | Filter 链设计？| **ProtocolFilterWrapper 自动组装** → 扫描 SPI 配置 → 洋葱模型 → 所有功能（限流/日志/鉴权）都基于 Filter |
| 4 | 默认容错策略？| **FailoverCluster（自动重试 2 次）** → 调用失败切下一个 Provider；非幂等操作用 Failfast |
| 5 | 默认负载均衡？| **RandomLoadBalance（随机）** → 简单高效；推荐 LeastActiveLoadBalance（最少活跃调用） |
| 6 | Dubbo 协议 vs HTTP？| Dubbo 用 **TCP 自定义二进制帧**（Magic+Flags+Payload）→ 比 HTTP 更轻量 → RPC 性能高；HTTP 用 HttpMessageCodec |
| 7 | Provider 端 Filter 链顺序？| EchoFilter → ClassLoaderFilter → GenericFilter → ContextFilter → ExceptionFilter → TraceFilter → TimeoutFilter → Service.invoke |
| 8 | Cluster 五种容错？| **Failover（自动重试，默认）/ Failfast（快速失败）/ Failsafe（安全降级）/ Failback（异步恢复）/ Forking（并行调用）** |
| 9 | 和 Spring Cloud OpenFeign 区别？| Dubbo：**RPC 二进制协议 + 自研容错**（性能高但和 HTTP 不兼容）；OpenFeign：**HTTP + RestTemplate**（兼容但性能低） |
| 10 | 调用超时？| dubbo.provider.timeout=3000（Provider 端）/ dubbo.consumer.timeout=5000（Consumer 端）→ Provider 端优先 |
| 11 | Provider 限流？| ActiveLimitFilter → **max.execute.count 限制并发数** → 超过限流（比 Sentinel 简单）|
| 12 | 熔断？| Sentinel 集成 → SentinelDubboProviderFilter / SentinelDubboConsumerFilter → Dubbo + Sentinel = 熔断限流 |
| 13 | 和 Nacos 注册中心集成？| 配置 dubbo.registry.address=nacos://nacos-server:8848 → Provider 注册 → Consumer 订阅 → 自动更新 |
| 14 | 泛化调用？| GenericFilter → **不需要接口定义就能调用** → 传方法名 + 参数 Map → 灵活但无类型安全 |
| 15 | Dubbo 3.x 新特性？| Tripe 协议（HTTP/2 支持流式）+ Triple 新集群 + Native Image 支持 |
| 16 | 序列化方式？| **Hessian2（默认）/ Dubbo 二进制 / JSON / Protobuf / Kryo** → Hessian2 兼容性最好 |
| 17 | 直连模式？| dubbo.reference.url=直连地址（跳过注册中心）→ 本地调试用 |
| 18 | Mock 测试？| @DubboReference(mock = "true") → 不连注册中心 → 用 Mock 实现 → 单元测试友好 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Dubbo 3.3.x  
**面试考点：18 题**
