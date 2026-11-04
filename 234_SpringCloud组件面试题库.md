# 234 · Spring Cloud 组件高频面试题库（45 题 · 详解）

> **方向**：Nacos、OpenFeign、Sentinel、Gateway、Seata、Resilience4j、链路追踪与配置中心  
> **面试权重**：⭐⭐⭐⭐⭐（微服务组件必问）  
> **配套源码文档**：[022_Nacos源码解析.md](022_Nacos源码解析.md)、[024_OpenFeign源码解析.md](024_OpenFeign源码解析.md)、[025_Sentinel源码解析.md](025_Sentinel源码解析.md)、[026_Gateway源码解析.md](026_Gateway源码解析.md)、[023_SeataAT模式源码解析.md](023_SeataAT模式源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问。

---

## 一、Nacos（Q1–Q8）

### Q1. Nacos 的核心功能有哪些？

**结论**：服务注册与发现、动态配置管理、服务健康检查、命名空间隔离，是微服务注册中心与配置中心。

**机制**：
- Naming：服务实例注册、订阅、健康检查。
- Config：配置发布、监听、灰度与回滚。
- 临时实例与持久实例。
- 命名空间/分组隔离环境与租户。

**追问**：Nacos 能替代 Eureka 吗？可以，功能更全，且支持 AP/CP 切换。

### Q2. Nacos 的服务注册与发现流程是怎样的？

**结论**：Provider 注册实例并心跳，Consumer 订阅服务，Nacos 推送变更，客户端本地缓存。

**机制**：
- 实例注册包含 IP、端口、权重、元数据。
- 临时实例靠心跳维持，超时剔除。
- 订阅后本地缓存实例列表。
- 变更通过推送/长轮询通知。

**追问**：Nacos 挂了服务还能调用吗？客户端有缓存可继续，但新实例发现受阻。

### Q3. Nacos 的 AP 与 CP 模式有什么区别？

**结论**：AP 用 Distro 协议，最终一致、优先可用；CP 用 JRaft/Raft，强一致但写更重。

**机制**：
- AP：临时实例、异步复制、适合服务发现。
- CP：持久实例、Raft 多数派提交。
- 可在配置中指定模式。
- 不同模式的数据一致性语义不同。

**追问**：配置中心该用哪种？关键配置用 CP，普通服务发现用 AP。

### Q4. Nacos 配置中心的长轮询是怎么实现的？

**结论**：客户端携带配置的 MD5/版本发起长轮询，服务端无变更时挂起请求，变更后立即返回。

**机制**：
- 客户端比较本地与服务端版本。
- 无变更则 hold 请求一段时间。
- 变更时唤醒并返回新配置。
- 减少无效轮询，接近实时。

**追问**：长轮询会占连接吗？会，但 Nacos 用异步机制控制资源。

### Q5. Nacos 配置灰度与回滚怎么做？

**结论**：按 beta 发布、IP/标签灰度、版本历史回滚，逐步放量验证。

**机制**：
- 配置有版本与历史记录。
- 灰度指定实例/标签。
- 观察日志与指标后全量。
- 出问题回滚到历史版本。

**追问**：灰度配置如何避免不一致？客户端监听 + 版本号，按实例维度控制。

### Q6. Nacos 临时实例与持久实例有什么区别？

**结论**：临时实例靠心跳存活，断开即删除；持久实例保存在服务端，需手动删除。

**机制**：
- 临时实例适合动态扩容的服务。
- 持久实例适合需要保留的元数据。
- 健康检查策略不同。
- AP/CP 模式对实例类型有约束。

**追问**：临时实例心跳超时会怎样？被标记不健康并剔除，恢复后重新注册。

### Q7. Nacos 的命名空间与分组有什么作用？

**结论**：命名空间隔离租户/环境，分组进一步细分服务集合，避免跨环境串用。

**机制**：
- namespace 隔离配置与服务。
- group 用于服务分组与权限。
- 客户端按 namespace/group 查找。
- 支持多环境部署。

**追问**：隔离能替代权限控制吗？不能，还需鉴权与网络隔离。

### Q8. Nacos 集群如何保证高可用？

**结论**：多节点部署 + 虚拟 IP/负载均衡 + Raft/Distro 一致性 + 客户端多地址配置。

**机制**：
- 节点间数据同步。
- 客户端配置多个地址。
- 故障节点自动摘除。
- 配置与实例数据持久化。

**追问**：Nacos 集群脑裂怎么办？CP 模式靠 Raft 多数派，AP 模式需业务容忍短暂不一致。

---

## 二、OpenFeign（Q9–Q14）

### Q9. OpenFeign 的工作原理是什么？

**结论**：基于接口 + 注解生成动态代理，把方法调用转换为 HTTP 请求，经负载均衡与拦截器发送。

**机制**：
- `@FeignClient` 标记接口。
- SpringMvcContract 解析注解。
- ReflectiveFeign 生成代理。
- Encoder/Decoder 处理请求响应。
- 集成 LoadBalancer 选择实例。

**追问**：Feign 与 RestTemplate 区别？Feign 声明式、自动编解码与负载均衡，代码更简洁。

### Q10. Feign 的动态代理与契约解析是怎么做的？

**结论**：`ReflectiveFeign` 创建 JDK 代理，`Contract` 把方法元数据解析为 `MethodMetadata`。

**机制**：
- SpringMvcContract 解析 @RequestMapping 等。
- 生成 RequestTemplate。
- InvocationHandler 组装请求。
- 返回类型由 Decoder 反序列化。

**追问**：Feign 支持继承接口吗？支持，但注解继承规则需注意。

### Q11. Feign 的超时与重试如何配置？

**结论**：连接超时、读超时通过 Request.Options 配置；重试由 Retryer 控制，默认不重试。

**机制**：
- `connectTimeout`、`readTimeout`。
- Retryer.Default 可配置次数与间隔。
- 重试需幂等，避免重复写。
- 熔断/降级配合保护下游。

**追问**：Feign 默认重试吗？默认 `Retryer.NEVER_RETRY`，需要显式配置。

### Q12. Feign 的拦截器有什么用？

**结论**：在请求发送前统一加 Header、鉴权、traceId，在响应后做统一处理。

**机制**：
- 实现 `RequestInterceptor`。
- `apply(RequestTemplate)` 修改请求。
- 可传递用户上下文与签名。
- 注意线程上下文与异步传递。

**追问**：拦截器能修改 Body 吗？可以操作 RequestTemplate，但复杂修改建议用 Encoder。

### Q13. Feign 如何与负载均衡配合？

**结论**：通过 `LoadBalancerFeignClient` 获取服务实例，按负载均衡策略选择后发起调用。

**机制**：
- 服务名对应 Spring Cloud LoadBalancer。
- 优先同区域/健康实例。
- 支持重试切换实例。
- 与注册中心缓存配合。

**追问**：Feign 自带 Ribbon 吗？旧版本集成 Ribbon，新版本用 Spring Cloud LoadBalancer。

### Q14. Feign 常见问题有哪些？

**结论**：超时、序列化不匹配、Header 丢失、服务名错误、重试导致重复、熔断误判。

**机制**：
- 检查 @FeignClient name 与注册中心服务名。
- 对齐 JSON 字段与日期格式。
- 确认拦截器与上下文传递。
- 配置合理超时与重试。
- 观察熔断指标。

**追问**：Feign 调用 404 是什么原因？路径不匹配或服务端未暴露该接口。

---

## 三、Sentinel（Q15–Q21）

### Q15. Sentinel 的核心概念有哪些？

**结论**：资源、规则、SlotChain、统计、限流、熔断、系统保护，围绕资源做流量治理。

**机制**：
- 资源用资源名标识。
- 规则包括流控、熔断、热点、系统规则。
- SlotChain 责任链处理请求。
- 统计窗口支撑决策。

**追问**：Sentinel 与 Hystrix 区别？Sentinel 规则更丰富、控制台完善、性能开销低。

### Q16. Sentinel 的滑动窗口是如何实现的？

**结论**：用 LeapArray 分桶统计，按时间窗口滚动，近似统计 QPS 与 RT。

**机制**：
- 时间轴切分为多个桶。
- 当前桶累加计数。
- 窗口滑动时复用旧桶。
- 降低锁竞争，保证性能。

**追问**：为什么不用精确计数？高并发下精确统计成本高，近似足够决策。

### Q17. Sentinel 的 FlowSlot 如何限流？

**结论**：根据流控规则判断是否放行，支持直接拒绝、匀速排队、预热/冷启动。

**机制**：
- QPS/并发数阈值。
- 匀速排队控制请求间隔。
- 预热避免冷启动被打满。
- 拒绝后返回限流异常。

**追问**：限流阈值怎么定？基于压测与容量，结合下游承受能力。

### Q18. Sentinel 的熔断降级原理是什么？

**结论**：统计慢调用比例/异常比例/异常数，达到阈值打开熔断，半开探测恢复。

**机制**：
- 慢调用 RT 阈值。
- 异常比例与异常数。
- 熔断时间窗口。
- 半开状态放少量请求探测。

**追问**：熔断后请求怎么处理？走 fallback 或快速失败，避免拖垮上游。

### Q19. Sentinel 的热点参数限流是什么？

**结论**：按参数值维度统计与限流，防止某个热点商品/用户打满系统。

**机制**：
- 指定参数索引与阈值。
- 对热点值单独限流。
- 可配置参数例外。
- 适合秒杀、详情页热点。

**追问**：热点参数统计成本高吗？有本地缓存与抽样，按需开启。

### Q20. Sentinel 集群流控如何工作？

**结论**：由 Token Server 统一分配令牌，客户端向服务端申请，保证集群总阈值。

**机制**：
- 单机限流无法控制总量。
- Token Server 维护全局令牌。
- 客户端批量申请降低通信。
- 需要高可用与超时兜底。

**追问**：Token Server 挂了怎么办？降级为单机限流或放行，按业务策略。

### Q21. Sentinel 规则如何持久化？

**结论**：控制台规则默认存内存，重启丢失；生产应持久化到 Nacos/ZK/DB 并监听变更。

**机制**：
- 数据源扩展。
- 启动拉取规则。
- 变更推送刷新。
- 保证多实例一致。

**追问**：规则推送失败会怎样？本地缓存继续生效，需监控与重试。

---

## 四、Spring Cloud Gateway（Q22–Q27）

### Q22. Gateway 的核心架构是什么？

**结论**：基于 WebFlux/Reactor Netty 的响应式网关，由 RouteDefinition、Predicate、Filter 组成。

**机制**：
- RoutePredicateHandlerMapping 匹配路由。
- GatewayFilterChain 执行过滤器。
- GlobalFilter 全局处理。
- 非阻塞 IO 支持高并发。

**追问**：Gateway 与 Zuul 区别？Gateway 基于 WebFlux，性能与扩展性更好。

### Q23. Gateway 的路由断言有哪些？

**结论**：Path、Method、Header、Query、Cookie、Host、时间、权重等断言组合。

**机制**：
- 断言决定请求是否匹配路由。
- 多个断言按 AND 组合。
- 支持动态配置。
- 匹配后进入过滤器链。

**追问**：如何做灰度路由？用 Weight 断言或自定义断言按比例分流。

### Q24. Gateway 的过滤器链如何执行？

**结论**：GlobalFilter 与 GatewayFilter 按 Order 排序，形成前后置处理链。

**机制**：
- pre 逻辑在 chain.filter 前。
- post 逻辑在返回后。
- 可短路返回响应。
- 顺序影响鉴权、限流、日志。

**追问**：过滤器能修改请求体吗？需要缓存/装饰器，注意内存与流式读取限制。

### Q25. Gateway 如何实现限流与熔断？

**结论**：用 RequestRateLimiter 过滤器 + Redis 令牌桶限流，熔断用 Resilience4j/Hystrix 过滤器。

**机制**：
- KeyResolver 定义限流维度（IP/用户/API）。
- Redis Lua 原子扣令牌。
- 熔断按失败率/慢调用。
- 超限返回 429。

**追问**：网关限流与服务限流如何配合？网关挡总量，服务做细粒度与业务限流。

### Q26. Gateway 如何实现动态路由？

**结论**：路由存配置中心/数据库，监听变更后刷新 RouteDefinitionLocator，无需重启。

**机制**：
- RouteDefinitionRepository 提供路由。
- 配置中心推送变更。
- 刷新路由缓存。
- 支持灰度与回滚。

**追问**：动态刷新会中断请求吗？已有请求继续，新请求按新路由。

### Q27. Gateway 与 Nginx 有什么区别？

**结论**：Nginx 偏高性能反向代理与静态资源，Gateway 偏 Java 生态的鉴权、限流、动态路由与服务治理。

**机制**：
- Nginx 用 C 模块，性能高。
- Gateway 用 Java/Reactor，易扩展。
- Gateway 与服务发现集成好。
- 生产常 Nginx + Gateway 分层。

**追问**：Gateway 性能足够吗？通过水平扩展与合理过滤链，多数业务足够。

---

## 五、Seata（Q28–Q34）

> **主题归属**：本节聚焦 Seata 组件实现；2PC/TCC/Saga 的通用模型见 [232 Q5–Q8](232_分布式与微服务面试题库.md)，一致性业务选型见 [233 Q23](233_高并发数据一致性面试题库.md)。

### Q28. Seata 的三个核心角色是什么？

**结论**：TC 事务协调者、TM 事务管理器、RM 资源管理器。

**机制**：
- TC 维护全局事务与分支状态。
- TM 发起/提交/回滚全局事务。
- RM 管理本地资源与 undo。
- 通过事务 ID 关联分支。

**追问**：TM 与 RM 一定在不同服务吗？TM 通常在发起方，RM 在每个参与者。

### Q29. Seata AT 模式的一阶段与二阶段做什么？

**结论**：一阶段执行业务 SQL 并记录 undo，二阶段全局提交删 undo 或回滚反向补偿。

**机制**：
- 代理数据源解析 SQL。
- 记录 before/after image。
- 注册分支事务。
- 二阶段按 TC 指令提交/回滚。

**追问**：AT 的隔离性如何？默认读未提交，可用全局锁或 select for update 提升。

### Q30. Seata TCC 模式的三个动作是什么？

**结论**：Try 预留、Confirm 确认、Cancel 取消，由业务实现资源控制。

**机制**：
- Try 检查并冻结资源。
- Confirm 使用资源。
- Cancel 释放资源。
- 需处理空回滚、悬挂、幂等。

**追问**：TCC 与 AT 区别？TCC 业务侵入大但控制强，AT 对业务透明但隔离弱。

### Q31. Seata Saga 模式是什么？

**结论**：长事务拆为本地事务链，失败按反向补偿，适合跨服务长流程。

**机制**：
- 状态机编排正向与补偿。
- 每个步骤本地事务。
- 失败重试与人工干预。
- 适合订单履约、开户等流程。

**追问**：Saga 与 TCC 区别？Saga 补偿已执行动作，TCC 预留资源后确认。

### Q32. Seata 的全局锁是什么？

**结论**：AT 模式在修改数据时申请全局锁，防止其他全局事务并发修改同一行。

**机制**：
- 锁记录在 TC/锁表。
- 按表 + 主键申请。
- 二阶段释放。
- 提升写隔离但降低并发。

**追问**：全局锁能完全防脏读吗？读仍需配合隔离级别或 for update。

### Q33. Seata 的 undo log 表结构是什么？

**结论**：记录分支 ID、SQL 类型、前后镜像、表名与主键，用于二阶段回滚。

**机制**：
- `undo_log` 与业务表同库。
- before/after image 支持反向 SQL。
- 回滚按主键定位。
- 提交后删除。

**追问**：undo log 会膨胀吗？二阶段清理，异常时需人工/补偿。

### Q34. Seata 的高可用如何部署？

**结论**：TC 集群 + 注册中心 + 事务分组，RM 随业务服务部署，保证协调与存储高可用。

**机制**：
- TC 多节点注册。
- 事务分组映射 TC 集群。
- 存储使用 DB/Redis。
- 失败重试与补偿。

**追问**：TC 挂了事务怎么办？客户端重试其他 TC，超时后回滚/补偿。

---

## 六、Resilience4j、追踪与配置（Q35–Q40）

### Q35. Resilience4j 的熔断器如何工作？

**结论**：基于失败率/慢调用阈值，状态机 CLOSED → OPEN → HALF_OPEN，半开探测恢复。

**机制**：
- 滑动窗口统计失败率。
- OPEN 时快速失败。
- HALF_OPEN 放少量请求。
- 可配置最小调用数与等待时间。

**追问**：与 Sentinel 熔断区别？Resilience4j 是库级、轻量；Sentinel 有控制台与集群规则。

### Q36. 舱壁（Bulkhead）、重试与超时如何配合？

**结论**：舱壁限制并发，重试处理瞬时失败，超时防止无限等待，需避免重试放大故障。

**机制**：
- Bulkhead 信号量隔离资源。
- Retry 指数退避 + 抖动。
- TimeLimiter 控制单次调用时长。
- 重试只用于幂等操作。

**追问**：重试次数越多越好吗？不是，会放大下游压力，需配合熔断。

### Q37. Spring Cloud 链路追踪如何传播 traceId？

**结论**：通过 Sleuth/Micrometer Tracing 自动注入 TraceId/SpanId，并在 HTTP/消息头中传播。

**机制**：
- W3C TraceContext 或 B3 头。
- 日志 MDC 输出 traceId。
- Feign/RestTemplate 自动透传。
- 异步/线程池需手动传递上下文。

**追问**：异步线程会丢 traceId 吗？会，需要用 TaskDecorator 或上下文包装。

### Q38. Spring Cloud Config 与 Bus 的作用是什么？

**结论**：Config 提供集中配置，Bus 用消息广播配置变更，触发刷新。

**机制**：
- Config Server 从 Git/DB 读取配置。
- 客户端通过 Bus 接收变更事件。
- `@RefreshScope` 刷新 Bean。
- 支持加密与高可用。

**追问**：Config 与 Nacos 配置中心区别？Nacos 自带推送与灰度，Config 更偏 Git 版本管理。

### Q39. Ribbon 与 Spring Cloud LoadBalancer 有什么区别？

**结论**：Ribbon 已进入维护模式，LoadBalancer 是官方替代，支持 Reactor 与客户端负载均衡。

**机制**：
- Ribbon 用 ILoadBalancer + IRule。
- LoadBalancer 用 ServiceInstanceListSupplier。
- 支持缓存、健康检查、同区域优先。
- 与 Feign/Gateway 集成。

**追问**：Ribbon 还能用吗？存量可用，新项目建议 LoadBalancer。

### Q40. Spring Cloud 项目常见问题有哪些？

**结论**：版本兼容、超时与重试、注册中心抖动、配置不生效、链路断裂、熔断误判是高频问题。

**机制**：
- 对齐 Spring Boot/Cloud 版本。
- 统一超时与重试策略。
- 注册中心多地址与缓存。
- 配置变更需监听与刷新。
- 全链路 traceId 与指标。
- 熔断阈值基于压测。

**追问**：升级 Spring Cloud 要注意什么？版本 train、契约变化、组件替换（如 Ribbon→LoadBalancer）。

---

## 七、进阶补充（Q41–Q45）

### Q41. Spring Cloud 版本如何选型与兼容？

**结论**：按 Spring Boot 版本选择对应的 Spring Cloud release train，组件与依赖必须整体对齐。

**机制**：
- Spring Cloud 版本与 Boot 版本有官方兼容矩阵。
- 不同 train 会替换组件（如 Ribbon→LoadBalancer）。
- 升级需同步 BOM、配置与客户端。
- 先升级 Boot 再升级 Cloud，逐项回归。

**追问**：能混用不同 Cloud 版本吗？不建议，BOM 与自动配置容易冲突。

### Q42. Spring Cloud Gateway Server MVC 与 WebFlux 版有什么区别？

**结论**：WebFlux 版基于 Reactor Netty，适合高并发网关；MVC 版基于 Servlet 栈，适合已有 MVC 体系与阻塞式过滤器。

**机制**：
- WebFlux 版非阻塞、吞吐高。
- MVC 版复用 Servlet 生态与阻塞模型。
- 过滤器/断言 API 有差异。
- 选型看团队栈与性能要求。

**追问**：MVC 版能替代 WebFlux 版吗？功能相近，但高并发下 WebFlux 更合适。

### Q43. 从 Nacos 2.x 迁移到 3.x 要注意什么？

**结论**：关注 gRPC/协议变化、鉴权、配置模型与客户端兼容，先灰度再全量。

**机制**：
- 3.x 强化 gRPC 与集群一致性；基础模型见 [Q1–Q8](234_SpringCloud组件面试题库.md)。
- 客户端 SDK 与版本需匹配。
- 配置迁移与命名空间兼容。
- 监控注册与配置推送。

**追问**：迁移期间双注册可以吗？可灰度双写，但需避免路由冲突。

### Q44. Spring Cloud Config 的 import 与刷新机制是什么？

**结论**：`spring.config.import` 从 Config Server 拉取配置，结合 Bus/RefreshScope 动态刷新。

**机制**：
- Config Data 在启动时导入外部配置；Config/Bus 基础见 [Q38](234_SpringCloud组件面试题库.md)。
- Bus 广播变更事件。
- `@RefreshScope` 重建 Bean。
- 失败时回退本地配置。

**追问**：刷新会中断请求吗？已有请求继续，新请求使用新配置，注意 Bean 状态重建。

### Q45. 微服务可观测性如何统一？

**结论**：指标、日志、追踪三件套统一上下文，traceId 贯穿网关、服务、消息与数据库。

**机制**：
- Micrometer 采集指标；TraceId 传播基础见 [Q37](234_SpringCloud组件面试题库.md)。
- 日志 MDC 带 traceId/spanId。
- W3C TraceContext 跨服务传播。
- 消息/异步线程手动传递上下文。

**追问**：异步/消息会断链吗？会，需在消息头与线程池包装中传递上下文。

---

## 八、速记清单

| 组件 | 必背一句话 |
|---|---|
| Nacos | 注册发现 + 配置中心，AP/CP 可选 |
| OpenFeign | 接口代理转 HTTP，Contract + 拦截器 |
| Sentinel | SlotChain + 滑动窗口 + 限流熔断 |
| Gateway | 路由断言 + 过滤器链 + 响应式 |
| Seata | TC/TM/RM，AT/TCC/Saga |
| Resilience4j | CLOSED/OPEN/HALF_OPEN 状态机 |
| 追踪 | TraceId 透传，异步需手动传递 |
| 配置 | Config/Bus 或 Nacos 推送刷新 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：45 题 · 详解**
