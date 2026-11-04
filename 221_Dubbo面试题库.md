# 221 · Dubbo 高频面试题库（50 题 · 详解）

> **方向**：Dubbo 2.x/3.x 架构、SPI、服务暴露与引用、集群容错、通信协议与治理  
> **面试权重**：⭐⭐⭐⭐⭐（RPC 框架必问）  
> **配套源码文档**：[035_Dubbo源码解析.md](035_Dubbo源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问；不写未经验证的行号。

---

## 一、架构与调用链（Q1–Q6）

### Q1. Dubbo 的分层架构是怎样的？

**结论**：从上到下分为服务治理、服务发现、通信、运行时四层，核心抽象是 Proxy、Invoker、Protocol、Transporter。

**机制**：
- 服务治理：路由、容错、负载均衡、Filter、Mock。
- 服务发现：Registry、元数据上报、服务订阅。
- 通信：Protocol、Exchange、Codec、序列化。
- 运行时：URL、Invocation、RpcContext、线程模型。
- 3.x 增加应用级服务发现、Triple 协议与可观测能力。

**追问**：为什么用 URL 统一模型？把协议、地址、服务名、参数统一成可扩展字符串，便于注册中心与路由传递。

### Q2. 描述一次 Dubbo 调用的完整链路。

**结论**：Proxy → Invoker → Cluster → LoadBalance → Filter → Protocol → Exchange → Transport → Provider 反向回传。

**机制**：
- 消费端引用生成代理，代理调用 `Invoker.invoke`。
- Cluster 做容错，LoadBalance 选 Provider，Filter 做上下文与监控。
- Protocol 组装请求，Exchange 负责请求/响应配对，Transporter 用 Netty 发出去。
- Provider 端经过反序列化、Filter、Invoker 反射调用真实实现。
- 响应沿原链路回传，异步结果由 `AsyncRpcResult` 完成。

**追问**：Filter 在哪两层出现？消费端与提供端各有 Filter 链，顺序由 `@Activate` 的 order 决定。

### Q3. Dubbo 服务暴露的流程是什么？

**结论**：`ServiceConfig.export()` → `Protocol.export()` → `RegistryProtocol.export()` → 本地暴露 + 注册中心注册。

**机制**：
- ServiceConfig 解析 `<dubbo:service>` 配置，构建 URL 与 Invoker。
- `Protocol.export` 根据协议选择实现（默认 DubboProtocol）。
- `RegistryProtocol.export` 先 `doLocalExport` 启动本地 Server，再向注册中心注册 URL。
- 注册成功后写回 `exportedURL`，供消费者订阅。

**追问**：本地暴露与远程注册失败如何取舍？注册失败通常重试；本地 Server 已启动时要注意回滚或保留，避免端口泄漏。

### Q4. Dubbo 服务引用的流程是什么？

**结论**：`ReferenceConfig.create()` → `Protocol.refer()` → `RegistryProtocol.refer()` → 订阅并生成 Invoker。

**机制**：
- 解析配置得到直连 URL 或注册中心地址。
- 通过 Registry 订阅 Provider 变化，拿到可用 URL 列表。
- 为每个 URL 创建 Invoker，交给 Cluster 包装。
- 生成代理对象，缓存到 ReferenceConfig。

**追问**：引用是懒加载吗？默认 `init=false` 时首次调用才创建，`check=true` 会提前校验 Provider 可用性。

### Q5. Dubbo 的 URL 模型包含哪些信息？

**结论**：`protocol://username:password@host:port/path?parameters`，涵盖协议、地址、服务路径与扩展参数。

**机制**：
- protocol：dubbo、tri、rest 等。
- host/port：Provider 地址。
- path：服务接口全限定名或应用名。
- parameters：interface、version、group、methods、serialization、timeout 等。
- URL 是注册中心存储与路由的最小单位。

**追问**：为什么把方法名也放进参数？用于方法级服务发现与 Mock/路由匹配，避免一个服务只注册一个粗粒度 URL。

### Q6. Invoker 有哪几种类型？

**结论**：`AbstractProxyInvoker`（本地真实调用）、`AbstractClusterInvoker`（集群容错）、`AbstractDirectory` 相关 Invoker（注册中心目录）。

**机制**：
- `JdkProxyFactory` / `JavassistProxyFactory` 生成代理，最终调用 Invoker。
- `AbstractProxyInvoker` 把 `Invocation` 反射成方法调用。
- `RegistryDirectory` 把 URL 转成 Invoker，并随注册中心通知动态更新。
- ClusterInvoker 负责选择与重试，不直接执行业务。

**追问**：代理和 Invoker 的关系？代理负责方法签名与参数封装，Invoker 负责“怎么找到并调用 Provider”。

---

## 二、SPI 扩展机制（Q7–Q12）

### Q7. Dubbo SPI 与 JDK SPI 的区别是什么？

**结论**：JDK SPI 一次性加载所有实现、按接口名找文件；Dubbo SPI 支持按 key 懒加载、依赖注入、Wrapper 与 Adaptive。

**机制**：
- JDK SPI 文件在 `META-INF/services/接口全限定名`，内容是实现类名。
- Dubbo SPI 文件在 `META-INF/dubbo/接口全限定名`，内容是 `key=实现类`。
- 只有调用 `getExtension(key)` 时才实例化，支持缓存与单例。
- 支持 setter 注入其他扩展、Wrapper AOP、Adaptive 动态选择。

**追问**：Dubbo 3 还兼容 JDK SPI 吗？兼容，会同时读取 `META-INF/services`，但 Dubbo 扩展优先。

### Q8. ExtensionLoader 的加载流程是怎样的？

**结论**：读取扩展文件 → 解析 key 与类名 → 按需实例化 → 依赖注入 → Wrapper 包装 → 缓存。

**机制**：
- `getExtensionLoader(Class)` 为每个接口创建并缓存一个 ExtensionLoader。
- `loadDirectory` 扫描 `META-INF/dubbo`、`META-INF/services` 等目录。
- `getExtension(name)` 先查缓存，未命中则 `createExtension`。
- 创建时通过 `injectExtension` 注入依赖，再判断是否为 Wrapper 并包装。
- `@SPI` 标注接口，`value` 指定默认扩展名。

**追问**：扩展类无默认构造器会怎样？实例化失败并抛出异常，Wrapper 必须有“接口参数”的构造器。

### Q9. `@SPI` 与 `@Adaptive` 的作用是什么？

**结论**：`@SPI` 标记可扩展接口及默认实现；`@Adaptive` 标记需要运行时动态选择的方法或类。

**机制**：
- `@SPI("dubbo")` 表示默认扩展名为 dubbo。
- `@Adaptive({"protocol"})` 表示从 URL 参数 `protocol` 取值选择扩展。
- 方法级 `@Adaptive` 会生成代理逻辑；类级 `@Adaptive` 表示该类自身就是自适应实现。
- 常见的自适应接口：Protocol、Transporter、LoadBalance、Cluster、Serialization。

**追问**：如果没有传入 key 会怎样？回退到 `@SPI` 的默认值，再找不到则抛异常。

### Q10. Adaptive 扩展的代码是怎么生成和缓存的？

**结论**：ExtensionLoader 用字节码工具为接口生成自适应实现类，按 URL 参数选择实际扩展，并缓存生成结果。

**机制**：
- 扫描接口方法上的 `@Adaptive`，提取参数 key。
- 生成类实现接口方法：从 URL 取 key，拼接扩展名，调用 `getExtension`。
- 生成类通过 `AdaptiveClassCodeGenerator` 输出，编译后缓存。
- 调用时若 key 为空，使用默认扩展名。

**追问**：为什么不在运行时用反射选择？生成代码避免每次调用的反射开销，并便于统一异常处理。

### Q11. Wrapper 机制在 Dubbo SPI 中起什么作用？

**结论**：Wrapper 是对扩展的 AOP 包装，可实现监听、过滤、增强，而不侵入原实现。

**机制**：
- Wrapper 类持有接口实例，构造器只有一个接口参数。
- `createExtension` 时识别 Wrapper，按顺序包装真实扩展。
- 典型用途：ProtocolFilterWrapper、ProtocolListenerWrapper、QosProtocolWrapper。
- 包装链顺序影响行为，需结合扩展设计理解。

**追问**：Wrapper 与 JDK 动态代理区别？Wrapper 是静态类组合，性能更好；动态代理更灵活但有反射成本。

### Q12. Dubbo SPI 的缓存结构是怎样的？

**结论**：按接口缓存 ExtensionLoader，按扩展名缓存实例，按类型缓存 Adaptive 与 Wrapper 列表。

**机制**：
- `EXTENSION_LOADERS`：接口 → ExtensionLoader。
- `EXTENSION_INSTANCES`：Class → 实例。
- `cachedAdaptiveClass`：类级 Adaptive 实现。
- `cachedWrapperClasses`：Wrapper 集合。
- `cachedNames`：key → Class。

**追问**：并发安全怎么保证？关键 map 使用 ConcurrentHashMap，创建过程加锁，避免重复实例化。

---

## 三、注册中心与服务发现（Q13–Q17）

### Q13. Dubbo 注册中心承担什么职责？

**结论**：负责 Provider 注册、Consumer 订阅、变更通知与健康状态感知，是服务发现的核心。

**机制**：
- Provider 启动后把 URL 写入注册中心。
- Consumer 订阅服务，拿到可用 URL 列表并缓存。
- Provider 下线或地址变化时，注册中心通知 Consumer 更新目录。
- 注册中心不参与每次调用，调用是点对点直连。

**追问**：注册中心挂了还能调用吗？已订阅的 Consumer 有本地缓存，可继续调用；但新 Consumer 无法发现服务。

### Q14. Dubbo 使用 ZooKeeper 做注册中心的原理是什么？

**结论**：用临时节点表示 Provider，用子节点监听实现订阅与通知。

**机制**：
- URL 写到 `/dubbo/{服务名}/providers/{encodedURL}` 临时节点。
- Provider 与 ZK 保持会话，断线后节点自动删除，实现故障感知。
- Consumer 订阅父节点，监听子节点变化并刷新 Directory。
- 节点内容可携带 URL 元数据。

**追问**：ZK 临时节点有什么问题？会话超时与网络抖动可能误删节点，需要合理设置 sessionTimeout 与重连策略。

### Q15. Dubbo 使用 Nacos 做注册中心有什么不同？

**结论**：Nacos 提供命名服务与配置能力，注册模型更偏“服务名 + 实例列表”，适合云原生环境。

**机制**：
- 通过 NacosNamingService 注册实例与订阅服务。
- 支持临时实例、健康检查、权重与元数据。
- 与 ZK 相比，Nacos 更易运维，提供 HTTP/OpenAPI。
- Dubbo 3 的应用级服务发现更依赖 Nacos 的元数据模型。

**追问**：Nacos 挂掉后调用是否中断？已缓存的地址仍可用，但服务变更无法及时感知。

### Q16. 注册中心的订阅与通知机制如何工作？

**结论**：Consumer 订阅服务名，注册中心在 Provider 列表变化时回调 NotifyListener，Directory 据此重建 Invoker。

**机制**：
- `RegistryService.subscribe(url, listener)` 建立订阅。
- 首次订阅会立即回调一次，返回当前可用列表。
- 后续变化回调携带最新 URL 列表。
- `RegistryDirectory.notify` 对比新旧 URL，销毁失效 Invoker、创建新增 Invoker。

**追问**：通知丢失怎么办？注册中心通常有重试或定期刷新；Consumer 也可主动 lookup 兜底。

### Q17. Dubbo 3 的应用级服务发现解决了什么问题？

**结论**：把“接口级 URL 注册”改为“应用级元数据 + 服务名映射”，大幅减少注册中心数据量与推送压力。

**机制**：
- 2.x 每个接口/方法都可能注册 URL，规模大时 ZK 节点膨胀。
- 3.x 以应用为粒度注册实例，接口到应用的映射通过 MetadataReport 维护。
- Consumer 先查 ServiceNameMapping，再订阅应用实例。
- 支持多协议、多版本与 Kubernetes 场景。

**追问**：应用级发现的缺点？多一次映射查询，链路更复杂；需要元数据中心高可用。

---

## 四、集群容错与负载均衡（Q18–Q24）

### Q18. Dubbo 有哪些集群容错策略？

**结论**：Failover、Failfast、Failsafe、Forking、Available、Mergeable 等，通过 `cluster` 配置选择。

**机制**：
- Failover：失败重试其他 Provider，默认策略。
- Failfast：失败立即抛出，适合非幂等写。
- Failsafe：失败返回空结果，不抛异常。
- Forking：并行调用多个 Provider，取首个成功。
- Available：只调用可用的，失败直接报错。
- Mergeable：合并多个 Provider 的分组结果。

**追问**：写操作该选哪个？优先 Failfast 或业务幂等 + Failover，避免重复写。

### Q19. Failover 的重试流程是怎样的？

**结论**：按负载均衡选出 Invoker，调用失败后从可用列表中排除失败者，再选下一个重试，直到成功或次数耗尽。

**机制**：
- `retries` 默认 2，表示失败后最多再试 2 次。
- 维护 `invoked` 集合，避免重复调用同一个 Provider。
- 每次失败记录异常，最终抛出最后一次异常或聚合异常。
- 网络超时、业务异常、序列化异常都可能触发重试，需区分是否可重试。

**追问**：重试会不会放大故障？会，必须配合超时、熔断与幂等，避免雪崩。

### Q20. Forking 策略适合什么场景？

**结论**：适合“多 Provider 结果可竞争、只需最快成功”的读场景，用并行换延迟。

**机制**：
- 同时向多个 Provider 发起调用，任一成功即返回。
- 其余请求继续执行或取消，取决于实现与线程池。
- 会消耗更多线程与连接，不适合高并发写。
- `forks` 参数控制并行数量。

**追问**：Forking 与 Failover 的本质区别？Failover 是串行重试，Forking 是并行竞争。

### Q21. Dubbo 的负载均衡算法有哪些？

**结论**：Random、RoundRobin、LeastActive、ConsistentHash、ShortestResponse、P2C 等。

**机制**：
- Random：按权重随机，简单但波动大。
- RoundRobin：按权重轮询，需维护计数。
- LeastActive：选活跃调用数最少的 Provider，适合长尾。
- ConsistentHash：按参数哈希到虚拟节点，保证同一 key 稳定路由。
- ShortestResponse：按历史响应时间选最快。
- P2C：随机选两个再比较，兼顾性能与效果。

**追问**：默认是哪个？2.x 默认 Random；3.x 常用 Random 或 P2C，需结合版本配置确认。

### Q22. 一致性哈希在 Dubbo 中如何实现？

**结论**：对 Provider 地址生成多个虚拟节点放到哈希环上，请求参数哈希后顺时针找第一个节点。

**机制**：
- 默认 160 个虚拟节点/Provider，减少数据倾斜。
- 支持 `hash.nodes` 与 `hash.arguments` 配置。
- 同一参数值稳定落到同一 Provider，适合缓存亲和。
- Provider 增减只影响相邻区间，迁移量小。

**追问**：参数怎么选？通常选用户 ID、租户 ID 等业务主键；不要用随机数，否则失去亲和性。

### Q23. LeastActive 为什么能改善长尾延迟？

**结论**：它优先选择当前活跃调用最少的 Provider，避免请求继续堆到已经变慢的节点。

**机制**：
- 每次调用前活跃数 +1，完成后 -1。
- 负载均衡时比较活跃数，最小者优先。
- 相同活跃数时再用权重随机。
- 能间接感知 Provider 的处理能力与故障。

**追问**：活跃数能完全代表负载吗？不能，慢调用与网络问题仍需超时、熔断配合。

### Q24. Dubbo 的路由规则（Router）有哪些？

**结论**：支持标签路由、条件路由、脚本路由，用于灰度、隔离与多机房调度。

**机制**：
- Tag 路由：Provider 打标签，Consumer 按标签选择。
- 条件路由：按方法、参数、IP 等条件匹配。
- 脚本路由：用脚本表达式决定目标。
- 路由在 ClusterInvoker 选 Provider 前生效。

**追问**：路由与负载均衡的顺序？先路由缩小候选集，再做负载均衡。

---

## 五、通信、协议与线程模型（Q25–Q30）

### Q25. Dubbo 默认通信协议与底层框架是什么？

**结论**：默认 `dubbo://` 协议，底层基于 Netty（早期可选 Mina），采用长连接与自定义二进制协议。

**机制**：
- `DubboProtocol` 负责 Server/Client 的创建与请求响应处理。
- `ExchangeHandler` 处理请求、响应、心跳与事件。
- 传输层由 `Transporter` 扩展点选择 Netty/Mina。
- 默认端口 20880，长连接复用。

**追问**：为什么用长连接？RPC 调用频繁，短连接建立成本高；长连接配合心跳保证可用性。

### Q26. Dubbo 协议的报文结构是怎样的？

**结论**：16 字节协议头 + 可变长消息体，头部包含魔数、请求/响应标志、状态、消息 ID、协议版本、序列化 ID 等。

**机制**：
- 魔数 `0xdabb` 用于快速识别 Dubbo 报文。
- 标志位区分请求/响应、单向调用、事件、心跳。
- 消息 ID 用于请求响应配对。
- 序列化 ID 决定使用 Hessian2、Fastjson2、Kryo、Protobuf 等。
- 消息体通常是 `Invocation` 或 `Result` 对象。

**追问**：为什么需要消息 ID？长连接上多个请求并发，响应必须按 ID 找回对应的 Future。

### Q27. Dubbo 的序列化方式如何选择？

**结论**：默认 Hessian2，兼容性好；性能敏感可用 Kryo/FST/Protobuf，跨语言用 Protobuf/JSON。

**机制**：
- 序列化扩展通过 SPI 加载，`serialization` 参数指定。
- 需要保证 Consumer 与 Provider 的序列化 ID 与版本一致。
- 序列化对象应避免循环引用、超大对象与不兼容字段变更。
- Protobuf 需要 IDL 与代码生成，跨语言友好。

**追问**：Hessian2 的坑？对某些复杂类型与字段变更敏感，跨版本升级要回归测试。

### Q28. Dubbo 的线程模型（Dispatcher）有哪些？

**结论**：all、direct、message、execution 四种，决定 IO 线程与业务线程如何分配。

**机制**：
- all：所有消息都交给业务线程池。
- direct：不经过线程池，直接在 IO 线程执行。
- message：请求交给业务线程池，响应/心跳等由 IO 线程处理。
- execution：只把请求交给线程池，其他在 IO 线程。
- 线程池类型：fixed、cached、limited，需防止队列打满。

**追问**：默认是哪种？2.x 默认 all（业务线程池），可配置；高吞吐与低延迟要结合业务阻塞情况选择。

### Q29. Dubbo 的异步调用怎么实现？

**结论**：`async=true` 返回 `CompletableFuture`/`Future`，通过 `AsyncRpcResult` 与回调完成结果，不阻塞调用线程。

**机制**：
- 消费端发起后立即拿到 Future。
- 响应到达时由 Exchange 找到对应 Future 并 `complete`。
- 支持 `onreturn`、`onthrow` 回调。
- 3.x 推荐 `CompletableFuture` 编排。

**追问**：异步调用还需要业务线程池吗？需要，但用于编排与回调处理，不要阻塞 IO 线程。

### Q30. Dubbo 3 的 Triple 协议是什么？

**结论**：基于 HTTP/2 与 Protobuf 的兼容 gRPC 的新协议，兼顾跨语言、网关穿透与流式通信。

**机制**：
- 协议名 `tri://`，支持 unary 与 stream。
- 与 gRPC 客户端/服务端可互操作。
- 复用 HTTP/2 多路复用、头部压缩与 TLS。
- 适合云原生、Service Mesh 与多语言服务。

**追问**：Triple 会完全替代 dubbo 协议吗？不会，存量系统与性能场景仍大量使用 dubbo 协议。

---

## 六、服务治理与实战（Q31–Q35）

### Q31. Dubbo 的 Filter 链有哪些内置实现？

**结论**：ContextFilter、ConsumerContextFilter、ExceptionFilter、TimeoutFilter、MonitorFilter、AccessLogFilter 等。

**机制**：
- Filter 分消费端与提供端，`@Activate` 控制启用与顺序。
- ContextFilter 传递 RpcContext 与隐式参数。
- ExceptionFilter 统一异常转换，避免把内部异常直接暴露。
- MonitorFilter 统计调用量、耗时、失败率。
- 自定义 Filter 通过 SPI 注册。

**追问**：Filter 能修改参数吗？可以，但会破坏幂等与可观测性，需谨慎并记录。

### Q32. 什么是 Dubbo 泛化调用？

**结论**：不依赖接口 Jar，用 `GenericService.$invoke(方法名, 参数类型, 参数值)` 发起调用。

**机制**：
- 适合网关、测试平台、跨语言桥接。
- 参数与返回值使用 Map/POJO 描述。
- 需要 Provider 暴露服务，消费端无需接口类。
- 性能略低于强类型代理，且类型安全较弱。

**追问**：泛化调用怎么做参数校验？在网关层做 schema 校验，避免把错误参数直接打到 Provider。

### Q33. RpcContext 有什么作用？隐式参数如何传递？

**结论**：RpcContext 保存一次调用的上下文与 attachment，隐式参数会随请求透传到 Provider。

**机制**：
- `RpcContext.getContext().setAttachment(key, value)`。
- Provider 端 `getAttachment` 读取。
- 常用于 traceId、租户、灰度标签。
- 调用完成后要清理，避免线程复用导致串数据。

**追问**：异步调用中上下文会丢吗？会，需要显式传递或在回调线程恢复上下文。

### Q34. Dubbo 的服务降级与 Mock 怎么做？

**结论**：通过 Mock 机制在 Consumer 端返回兜底数据，或用路由/容错策略跳过故障 Provider。

**机制**：
- `MockInvokersSelector` 按 `mock` 参数选择 Mock Invoker。
- 可配置强制 Mock 或失败后 Mock。
- 降级应记录指标与告警，避免掩盖真实故障。
- 写操作慎用 Mock 返回成功，防止数据不一致。

**追问**：Mock 与 Failsafe 区别？Mock 返回业务兜底值；Failsafe 是失败时返回空结果，语义更弱。

### Q35. Dubbo 生产常见问题与排查思路有哪些？

**结论**：超时、线程池打满、序列化不兼容、注册中心抖动、连接泄漏是高频问题，按“链路 + 指标 + 日志”排查。

**机制**：
- 超时：区分连接超时、读超时、业务超时，检查 Provider 耗时与网络。
- 线程池：看队列长度、拒绝策略、慢调用，避免 all 模式阻塞。
- 序列化：对齐版本与 serialization ID，检查字段兼容。
- 注册中心：看会话、节点、订阅回调与本地缓存。
- 连接：检查长连接数、心跳、重连与句柄泄漏。

**追问**：升级 2.x 到 3.x 要注意什么？应用级服务发现、Triple 兼容、序列化与配置中心迁移，建议灰度。

---

## 七、进阶补充（Q36–Q40）

### Q36. Dubbo 3 应用级服务发现具体解决了什么？

**结论**：把接口级 URL 注册改为“应用实例注册 + 接口到应用映射”，降低注册中心数据量与推送压力。

**机制**：
- 应用启动时注册实例，而不是每个接口/方法一个 URL。
- `ServiceNameMapping` 维护接口到应用的映射。
- `MetadataReport` 保存接口元数据。
- 消费端先查映射，再订阅应用实例。
- 两种发现粒度对比见 [232 Q20](232_分布式与微服务面试题库.md)。

**追问**：应用级发现的代价？多一次映射查询与元数据中心依赖，链路更复杂。

### Q37. Triple 协议与 gRPC 如何互通？

**结论**：Triple 基于 HTTP/2 + Protobuf，兼容 gRPC 的 unary/stream 语义，可与 gRPC 客户端/服务端互调。

**机制**：
- 使用标准 HTTP/2 与 HPACK，便于网关穿透。
- Protobuf IDL 与代码生成可复用。
- 支持 unary、server/client/bidi stream。
- 需要版本与 IDL 兼容，注意异常/状态码映射。

**追问**：Triple 能完全替代 dubbo 协议吗？不能，存量二进制协议与性能场景仍大量使用。

### Q38. Dubbo 异步调用的上下文如何传递？

**结论**：异步调用返回 CompletableFuture，但 ThreadLocal/RpcContext 不会自动跨线程传播，需要显式传递。

**机制**：
- 发起线程设置 attachment/上下文。
- 回调可能在其他线程执行，需恢复上下文。
- 可用上下文包装器或 MDC 传递 traceId。
- 避免在 IO 线程做阻塞回调。

**追问**：异步调用超时怎么处理？超时取消 Future，并确保资源释放与幂等。

### Q39. Dubbo 线程池打满如何排查？

**结论**：看线程池活跃数、队列、拒绝数、慢调用与下游依赖，区分 IO 等待与业务阻塞。

**机制**：
- `Dispatcher` 模式决定哪些任务进业务池。
- 慢 SQL/远程调用占满线程。
- 队列无界会掩盖问题并导致延迟。
- 通过指标、线程栈与链路追踪定位。

**追问**：扩容线程池能解决吗？若下游已瓶颈，扩容只会放大压力，应配合限流降级。

### Q40. Dubbo 的服务治理能力有哪些？

**结论**：标签路由、条件路由、灰度、限流、熔断、降级、Mock 与调用链追踪，围绕服务与实例做治理。

**机制**：
- Tag/条件路由控制流量去向。
- 灰度按实例/标签放量。
- Sentinel/Resilience4j 做限流熔断。
- Mock 与降级保护消费者。
- 治理规则应可动态下发与回滚。

**追问**：治理规则放哪里？配置中心/注册中心动态下发，避免重启生效。

---

## 八、进阶补充（Q41–Q45）

### Q41. Dubbo 的 QoS 与运维命令有什么用？

**结论**：QoS 提供在线查看服务、调用、线程池与配置的能力，便于排障与动态治理。

**机制**：
- Telnet/HTTP 命令查询服务与端口。
- 查看消费者/提供者连接与调用统计。
- 动态调整部分配置。
- 生产需限制访问与鉴权。

**追问**：QoS 能改业务配置吗？部分运行参数可以，但需谨慎并审计。

### Q42. Dubbo 的序列化安全要注意什么？

**结论**：反序列化可能被恶意构造利用，需限制可反序列化类型、使用安全序列化协议并做鉴权。

**机制**：
- Hessian2 等协议存在反序列化风险。
- 配置白名单/类检查。
- 优先 Protobuf 等结构化协议。
- 网络入口鉴权与限流。

**追问**：内网调用就安全吗？不，内网也可能被横向移动，仍需最小权限与类型白名单；通用防护见 [225 Q31](225_JavaSecurity与Crypto面试题库.md)。

### Q43. 泛化调用适合什么场景？性能如何？

**结论**：不依赖接口 Jar，通过 GenericService 调用，适合网关/测试平台，但类型安全与性能弱于强类型代理。

**机制**：
- 参数用 Map/POJO 描述。
- 无需引入服务接口。
- 反射与动态类型增加开销。
- 需要网关层做 schema 校验。

**追问**：生产网关该用泛化调用吗？可用，但要限制方法白名单、参数大小与调用频率。

### Q44. Triple 的流式调用怎么用？

**结论**：Triple 支持 unary、server stream、client stream、bidi stream，适合大结果集与实时推送。

**机制**：
- 基于 HTTP/2 流与 Protobuf。
- 客户端可订阅 StreamObserver。
- 需要背压与取消处理。
- 与 gRPC 语义兼容。

**追问**：流式调用要注意什么？连接生命周期、背压、错误传播与资源释放。

### Q45. Dubbo 在 Mesh/云原生环境如何部署？

**结论**：应用级服务发现 + Triple + Sidecar/Service Mesh，服务注册到 K8s/Nacos，治理下沉到基础设施。

**机制**：
- 应用实例注册与元数据上报。
- Triple 便于网关与 Mesh 穿透。
- Sidecar 负责流量治理。
- 与 K8s Service/Ingress 集成。

**追问**：Dubbo 会被 Mesh 替代吗？不会，Dubbo 仍是 RPC 框架，Mesh 是通信治理层；服务网格基础见 [232 Q41](232_分布式与微服务面试题库.md)。

---

## 九、进阶补充（Q46–Q50）

### Q46. Dubbo 服务如何压测与容量规划？

**结论**：按接口 QPS、RT、线程池与连接数压测，评估 Provider 容量与超时阈值，预留冗余。

**机制**：
- 单机/集群压测找拐点。
- 关注线程池队列、RT P99 与错误率。
- 连接数与序列化开销。
- 按峰值与增长规划扩容。

**追问**：压测结果能直接上线吗？需结合真实流量、依赖与网络差异；线程池见 [Q39](221_Dubbo面试题库.md)。

### Q47. 注册中心故障如何演练与降级？

**结论**：模拟注册中心不可用，验证本地缓存、重连与直连兜底，确保调用不中断。

**机制**：
- 已订阅地址本地缓存。
- 客户端重连与重试。
- 直连 URL 兜底。
- 监控注册与订阅异常。

**追问**：注册中心长时间故障怎么办？依赖缓存并人工介入，恢复后重新订阅。

### Q48. Dubbo 需要监控哪些指标？

**结论**：QPS、RT、错误率、线程池活跃/队列、连接数、注册订阅、序列化耗时与限流拒绝。

**机制**：
- 消费端/提供端分别统计。
- 线程池打满与拒绝。
- 长连接与心跳状态。
- 按接口维度定位。

**追问**：RT 高但 CPU 不高？可能是下游依赖、锁等待或线程池排队。

### Q49. Dubbo 灰度发布与回滚怎么做？

**结论**：按标签/实例/流量比例灰度，观察 RT 与错误率，异常时切回旧版本。

**机制**：
- 标签路由与权重。
- 新旧 Provider 并行。
- 版本与序列化兼容。
- 快速回滚与流量切换。

**追问**：灰度期间新老版本能互调吗？需保证接口与序列化兼容。

### Q50. Dubbo 升级 2.x 到 3.x 要注意什么？

**结论**：应用级服务发现、Triple 兼容、配置与序列化迁移、注册中心元数据变化，建议灰度。

**机制**：
- 服务发现粒度变化。
- Triple 与旧协议并行。
- 配置与治理规则迁移。
- 依赖版本对齐。

**追问**：能一次性全量升级吗？不建议，先灰度关键服务并保留回滚；版本差异见 [Q35](221_Dubbo面试题库.md)。

---

## 十、速记清单

| 主题 | 必背一句话 |
|---|---|
| 调用链 | Proxy → Invoker → Cluster → LoadBalance → Filter → Protocol → Transport |
| SPI | key 懒加载 + 依赖注入 + Wrapper + Adaptive |
| 暴露 | ServiceConfig → Protocol → RegistryProtocol → 注册 URL |
| 引用 | ReferenceConfig → Protocol.refer → 订阅 → Invoker |
| 容错 | Failover 串行重试，Forking 并行竞争 |
| 负载 | Random / RoundRobin / LeastActive / ConsistentHash / P2C |
| 协议 | 16 字节头 + 消息体，消息 ID 配对 |
| 3.x | 应用级服务发现 + Triple/HTTP2 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：50 题 · 详解**
