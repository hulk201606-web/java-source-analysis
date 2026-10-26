# Spring Cloud 全家桶源码调研大纲

> **调研目标**：Spring Cloud 高频面试组件的源码级解析  
> **Spring Boot 基线**：2.7.18（Java 8 兼容）  
> **Spring Cloud 基线**：2021.0.8（Java 8 兼容、Spring Boot 2.7.x 配套）  
> **源码访问方式**：Maven 本地仓 / GitHub spring-cloud 仓库  
> **产出形式**：md 文档（源码锚点 + 面试考点 + 核心流程图）

---

## 一、调研优先级总览

| 优先级 | 组件 | 面试权重 | 核心源码仓库 | 核心内容 |
|--------|------|---------|-------------|---------|
| **P0** | Nacos（注册中心 + 配置中心）| ⭐⭐⭐⭐⭐ | nacos-group/nacos | naming 服务发现 + config 配置中心 + raft 一致性协议 |
| **P0** | OpenFeign 声明式调用 | ⭐⭐⭐⭐⭐ | spring-cloud/spring-cloud-openfeign | 动态代理 + RequestTemplate 构造 + Ribbon 负载均衡调用 |
| **P0** | Sentinel 熔断限流 | ⭐⭐⭐⭐⭐ | alibaba/Sentinel | Slot 链 + NodeSelector + Rule 判断 + 降级策略 |
| **P0** | Gateway 网关 | ⭐⭐⭐⭐⭐ | spring-cloud/spring-cloud-gateway | Route 定义 + Predicate 匹配 + Filter 链 + Netty Server |
| **P0** | Ribbon 客户端负载均衡 | ⭐⭐⭐⭐ | Netflix/ribbon + spring-cloud-netflix | IRule 选择 + IPing 健康检查 + Server 列表更新 |
| **P1** | Config 配置中心 | ⭐⭐⭐⭐ | spring-cloud/spring-cloud-config | Git/SVN/本地资源加载 + @RefreshScope + Bus 刷新 |
| **P1** | Bus 消息总线 | ⭐⭐⭐⭐ | spring-cloud/spring-cloud-bus | RabbitMQ/Kafka 广播 + EnvironmentChangeEvent |
| **P1** | Sleuth 链路追踪 | ⭐⭐⭐⭐ | spring-cloud/spring-cloud-sleuth | Span/TraceId 生成 + Brave 包装 + JDBC/HTTP 拦截器 |
| **P2** | Seata 分布式事务 | ⭐⭐⭐⭐⭐（面试高）| seata/seata | AT 模式 undo_log + Global Lock + 两阶段提交 |
| **P2** | Resilience4j 熔断（替代 Hystrix）| ⭐⭐⭐⭐ | resilience4j | CircuitBreaker 状态机 + Bulkhead + RateLimiter |

---

## 二、P0 组件详细调研大纲

### 🎯 P0-1 Nacos（注册中心 + 配置中心）

> **源码仓库**：`https://github.com/nacos-group/nacos`  
> **核心模块**：nacos-common / nacos-core / nacos-naming / nacos-config / nacos-api  
> **面试频率**：⭐⭐⭐⭐⭐（国内互联网公司必问）

#### 调研章节结构

```
1. Nacos 整体架构
   1.1 四大模块职责（API / Naming / Config / Core）
   1.2 部署模式（单机 / 集群 / 伪集群）
   1.3 通信协议（HTTP + gRPC 混合，Nacos 2.0+）

2. Naming 服务发现核心
   2.1 Instance 注册流程（InstanceController.registerInstance）
       → 接收 HTTP POST → ServiceManager.register → 持久化 + 内存更新
   2.2 Instance 心跳机制（ClientBeatProcessor / ClientBeatCheckTask）
       → 5 秒发送一次心跳 → 15 秒没心跳标记 unhealthy → 30 秒剔除
   2.3 Instance 查询流程（InstanceController.listInstances）
       → ServiceManager.selectInstances → Distro 协议数据同步
   2.4 Consistency 一致性协议
       → CP 模式：Raft（服务端 Paxos-like）
       → AP 模式：Distro（客户端 Push + 服务端 Pull 混合）
       → ★ CP vs AP 对比 + 选哪个？
   2.5 Distro 协议完整源码（核心面试考点）
       → 节点 hash 分配 + 客户端 Push + 服务端 Pull + 数据校验
   2.6 Raft 协议（CP 模式）
       → Leader 选举 + 日志复制 + 成员变更

3. Config 配置中心核心
   3.1 配置发布流程（ConfigController.publishConfig）
       → MySQL 持久化 + 内存更新 + 通知服务端 + 通知客户端
   3.2 配置获取流程（ConfigController.getConfig）
       → 内存查询 → 文件查询 → MySQL 查询（三级缓存）
   3.3 配置变更通知
       → 长轮询（HTTP）+ gRPC 推送（Nacos 2.0+）
       → 客户端 Listener 机制（NotifyListener.receiveConfigInfo）
   3.4 Nacos 2.0 gRPC 通信升级
       → RequestProcessor 分发 + BiRequestStream 双向流

4. 面试考点（20 题）
   4.1 Nacos CP/AP 怎么切换？config 用 CP，naming 默认 AP
   4.2 Distro 协议和 Raft 区别？Distro 是 AP + 无中心化 + 最终一致性
   4.3 Nacos 心跳机制？客户端 5s/15s/30s 三阶段
   4.4 配置变更通知原理？长轮询（HTTP 空响应保持）+ gRPC 推送
   4.5 Nacos 数据三级缓存？内存 → 文件 → MySQL
   4.6 Nacos 集群节点 hash 分配？node.key = hash(IP:PORT) → 数据分片
```

#### 核心源码锚点清单

| 类 | 方法 | 关键行号（需实际验证）| 核心功能 |
|----|------|---------------------|---------|
| InstanceController | registerInstance | 约 L110-180 | 处理服务注册 HTTP 请求 |
| ServiceManager | registerInstance | 约 L180-230 | 注册 + 持久化 + 内存更新 |
| ServiceManager | selectInstances | 约 L320-380 | 查询实例列表 |
| ClientBeatCheckTask | run | 约 L50-90 | 心跳检查定时任务 |
| DataStore | put / get | 约 L80-120 | AP 模式数据存储 |
| DistroDataProcessor | process | 约 L100-150 | Distro 协议处理 |
| ConfigController | publishConfig | 约 L150-200 | 配置发布 |
| ConfigController | getConfig | 约 L100-140 | 配置获取 |
| ClientWorker | checkConfigInfo | 约 L120-180 | 客户端长轮询 |

---

### 🎯 P0-2 OpenFeign 声明式调用

> **源码仓库**：`https://github.com/spring-cloud/spring-cloud-openfeign`  
> **核心模块**：spring-cloud-openfeign-core / feign-core  
> **面试频率**：⭐⭐⭐⭐⭐

#### 调研章节结构

```
1. OpenFeign 整体架构
   1.1 三组件：@EnableFeignClients / @FeignClient / Feign.Builder
   1.2 动态代理底层（JDK Proxy / CGLIB）
   1.3 执行流程全链路：方法调用 → 代理 → RequestTemplate → Client → HTTP 响应

2. 核心源码链路（完整调用栈）
   2.1 FeignClientFactoryBean.getObject
       → 创建 JDK Proxy / CGLIB → 注册到 Spring 容器
   2.2 SynchronousMethodHandler.invoke
       → ★ 动态代理方法执行入口
       → ReflectiveFeign.FeignInvocationHandler.dispatch
       → 遍历 RequestTemplate.Factory
       → 构造 RequestTemplate
       → Client.execute（默认 FeignClient → 底层用 Ribbon / HttpClient5）
   2.3 RequestTemplate 构造
       → Contract 解析注解 → MethodMetadata
       → ParamExpander 参数展开
       → QueryTemplate / BodyTemplate 模板填充
   2.4 Client 执行
       → 默认 Client.Default → HttpURLConnection
       → RibbonClient → LoadBalancerClient.choose + RestTemplate
       → ApacheHttpClient5 / OkHttpClient 替换

3. 注解处理链路
   3.1 @FeignClient → FeignClientFactoryBean（Spring FactoryBean）
   3.2 @GetMapping/@PostMapping → Contract.Default（Spring MVC Contract）
   3.3 @RequestParam → ParamExpander 展开
   3.4 @RequestBody → BodyTemplate + HttpMessageConverter
   3.5 @Response → decode + HttpMessageConverter

4. 与 Ribbon 集成（⭐⭐⭐⭐⭐）
   4.1 RibbonClient：choose(loadBalancer, url)
   4.2 LoadBalancerClient：choose → IRule → Server
   4.3 负载均衡时机：Feign 内部 Client 层统一做

5. 面试考点（15 题）
   5.1 OpenFeign 为什么能用注解调用？动态代理（JDK Proxy）
   5.2 FeignClientFactoryBean.getObject 做什么？创建代理 + 注册 BeanDefinition
   5.3 SynchronousMethodHandler.invoke 核心？构造 RequestTemplate → Client.execute
   5.4 RequestTemplate 构造流程？Contract 解析注解 → MethodMetadata → ParamExpander
   5.5 默认 HTTP Client 是什么？HttpURLConnection（慢！生产必须替换）
   5.6 OpenFeign 默认 Ribbon 负载均衡？是的，Feign 内部 Client 层集成
   5.7 请求压缩怎么做？RequestInterceptor 实现 GzipRequestInterceptor
```

#### 核心源码锚点清单

| 类 | 方法 | 核心功能 |
|----|------|---------|
| FeignClientFactoryBean | getObject | 创建代理 Bean（FactoryBean 钩子）|
| ReflectiveFeign | FeignInvocationHandler.dispatch | 代理方法执行入口 |
| SynchronousMethodHandler | invoke | ★ 核心执行：构造 Template → Client.execute |
| Contract.Default | parseAndValidateMetadata | 解析方法注解 → MethodMetadata |
| RequestTemplate.Factory | create | 根据 MethodMetadata 构造 Template |
| Client.Default | execute | 默认 HttpURLConnection 调用 |
| LoadBalancerFeignClient | execute | Ribbon Client 负载均衡调用 |

---

### 🎯 P0-3 Sentinel 熔断限流

> **源码仓库**：`https://github.com/alibaba/Sentinel`  
> **核心模块**：sentinel-core / sentinel-web-servlet / sentinel-dubbo-adapter / sentinel-cluster  
> **面试频率**：⭐⭐⭐⭐⭐（阿里系必问）

#### 调研章节结构

```
1. Sentinel 整体架构
   1.1 核心概念：Resource / Rule / Context / Entry / Exit
   1.2 四大核心功能：流量控制 / 熔断降级 / 系统自适应 / 热点参数
   1.3 核心处理器链（责任链模式）

2. 核心处理链路（SphU.entry 完整执行栈）
   2.1 Env.sph.entry → CtSph.entryWithType
   2.2 构建 Resource + 创建 Context
   2.3 Slot 链（责任链模式，★ 核心考点！）
       → NodeSelectorSlot（为不同 Context 创建不同 Node）
       → ClusterBuilderSlot（创建 ClusterNode，统计跨 Context 集群数据）
       → StatisticSlot（计数器：pass / block / exception / success）
       → FlowSlot（流量控制：QPS / 线程数）
       → DegradeSlot（熔断降级：异常比例 / RT / 慢调用数）
       → SystemSlot（系统自适应）
       → ParamFlowSlot（热点参数限流）
   2.4 Slot 返回 BlockException → 限流/降级触发
   2.5 Entry.exit → Node 的 pass 计数器 -1

3. 限流规则 FlowRule 源码（⭐⭐⭐⭐⭐）
   3.1 FlowRule 字段：resource / grade(QPS/线程) / count / strategy / limitApp / clusterMode
   3.2 FlowChecker.checkFlow → 根据 grade 判断
       → QPS：StatisticNode.pass / passWindow（滑动窗口）
       → 线程：StatisticNode.curThreadNum（原子计数器）
   3.3 四种 strategy：DIRECT / RELATE / CHAIN / CLUSTER
   3.4 滑动窗口计数器实现
       → 60 个 1 秒窗口（LeapArray<MetricBucket>）
       → 当前窗口 pass + 前 1 秒窗口 pass ≈ QPS

4. 熔断规则 DegradeRule 源码（⭐⭐⭐⭐⭐）
   4.1 DegradeRule 字段：resource / grade / count / timeWindow / statIntervalMin
   4.2 三种 grade：慢调用比例 / 异常比例 / 异常数
   4.3 熔断状态机（核心！面试必问）
       → CLOSED（正常）→ 触发规则 → HALF_OPEN（放行一个请求）→ 成功 → CLOSED / 失败 → OPEN
       → OPEN → timeWindow 后 → HALF_OPEN
   4.4 CircuitBreaker 实现：halfOpenPass / failCount / retryTimestamp

5. Node 层级结构
   5.1 Node（接口）→ StatisticNode / ClusterNode / EntranceNode
   5.2 Node 计数器：pass / block / success / exception（滑动窗口）
   5.3 ClusterNode：统计同一个 Resource 跨 Context 的集群数据

6. 面试考点（20 题）
   6.1 Sentinel 核心处理器链？Slot 责任链（NodeSelector / ClusterBuilder / Statistic / Flow / Degrade / System / ParamFlow）
   6.2 Sentinel 限流 vs 熔断？限流是预防性（流量高就挡）；熔断是故障后（异常/慢调用多了断开）
   6.3 熔断三种规则？慢调用比例（grade=0）/ 异常比例（grade=1）/ 异常数（grade=2）
   6.4 熔断状态机？CLOSED → OPEN → HALF_OPEN → CLOSED（timeWindow 后触发 HALF_OPEN）
   6.5 Sentinel 滑窗计数器？LeapArray<MetricBucket>（60 个 1 秒桶）
   6.6 热点参数限流怎么实现？ParamFlowSlot → 按方法参数值做限流
   6.7 Sentinel 规则持久化？数据源 SPI：FileDataSource / NacosDataSource / ApolloDataSource
```

#### 核心源码锚点清单

| 类 | 方法 | 核心功能 |
|----|------|---------|
| CtSph | entryWithType | ★ 入口：创建 Resource + 构建 Slot 链 |
| ProcessorSlotChain | entry | Slot 链执行入口 |
| StatisticSlot | entry | 计数器 pass +1 |
| FlowSlot | entry | 限流规则检查 |
| DegradeSlot | entry | 熔断规则检查 |
| LeapArray | currentWindow | 获取当前时间窗口 |
| MetricBucket | addPass | pass 计数器 +1 |
| CircuitBreaker | tryPass / revertToClosed | 熔断状态机转换 |
| FlowChecker | checkFlow | 限流判断 |
| DegradeRuleManager | checkDegrade | 熔断判断 |

---

### 🎯 P0-4 Gateway 网关

> **源码仓库**：`https://github.com/spring-cloud/spring-cloud-gateway`  
> **核心模块**：spring-cloud-gateway-core / spring-cloud-gateway-server  
> **面试频率**：⭐⭐⭐⭐⭐

#### 调研章节结构

```
1. Gateway 整体架构
   1.1 核心四概念：Route / Predicate / Filter / HandlerMapping
   1.2 Netty Server 底层：Reactor Netty（响应式非阻塞）
   1.3 启动流程：@EnableGateway / AutoConfiguration / RouteLocatorBuilder

2. 请求处理全链路（⭐⭐⭐⭐⭐）
   2.1 DispatcherHandler.handle → RoutePredicateHandlerMapping
   2.2 Route 匹配（Predicate.test）
       → After / Before / Between / Header / Query / Path / Method / RemoteAddr / Weight / Composite
       → ★ PathPatternParser AntPath / 正则 / RESTful 三种模式
   2.3 全局 Filter 链执行（有序！）
       → RemoveResponseHeaderFilter → RequestSizeFilter → XForwardedFilter → NettyRoutingFilter → NettyWriteResponseFilter
       → ★ GatewayFilter vs GlobalFilter 区别
   2.4 NettyRoutingFilter（★ 核心！真正发 HTTP 请求）
       → 构造 Reactor Netty HttpClientRequest
       → HttpClient.execute → 后端微服务
       → 响应写回 ServerWebExchange

3. GatewayFilter / GlobalFilter
   3.1 GatewayFilter：路由级，绑定到某个 Route 上
   3.2 GlobalFilter：全局，所有 Route 都执行
   3.3 执行顺序：getOrder() 升序执行
   3.4 常用内置：AddRequestHeader / AddResponseHeader / StripPrefix / RewritePath / RequestRateLimiter

4. 面试考点（15 题）
   4.1 Gateway 底层是什么？Reactor Netty（响应式非阻塞 WebClient）
   4.2 Route 匹配流程？Predicate.test + PathPatternParser.match
   4.3 Filter 链执行顺序？getOrder() 升序执行 + GatewayFilterChain.filter 链
   4.4 GatewayFilter vs GlobalFilter？GatewayFilter 路由级（绑定到 Route）；GlobalFilter 全局（所有 Route）
   4.5 真正发请求的 Filter？NettyRoutingFilter（核心！构造 Reactor Netty HttpClientRequest）
   4.6 跨域怎么处理？GlobalCorsProperties + CorsWebFilter
   4.7 Gateway 和 Nginx 区别？Gateway 有 Filter 机制 + Java 扩展能力（写 Java 代码）；Nginx C 性能好
```

---

### 🎯 P0-5 Ribbon 客户端负载均衡

> **源码仓库**：Netflix/ribbon + spring-cloud-netflix  
> **核心模块**：ribbon-core / ribbon-loadbalancer  
> **面试频率**：⭐⭐⭐⭐

#### 调研章节结构

```
1. Ribbon 整体架构
   1.1 核心概念：LoadBalancer / ServerList / IRule / IPing
   1.2 三种使用方式：RestTemplate / Feign Client / 直接调用 LoadBalancerClient

2. 核心执行链路
   2.1 LoadBalancerClient.choose(serviceId)
       → SpringClientFactory.getLoadBalancer(serviceId)
       → ILoadBalancer.chooseServer(key)
   2.2 ZoneAwareLoadBalancer.chooseServer
       → ServerList 拉取服务列表（静态 / DiscoveryClient）
       → IPing 健康检查（PingUrl / NoOpPing）
       → IRule 选一个 Server
   2.3 IRule 选哪个？（★ 核心考点）
       → RoundRobinRule（轮询，默认）
       → RandomRule（随机）
       → BestAvailableRule（选连接数最少的）
       → AvailabilityFilteringRule（先过滤掉连接多/熔断的 → 轮询）
       → WeightedResponseTimeRule（权重根据 RT 动态计算）
       → RetryRule（失败重试换 Server）
       → ZoneAvoidanceRule（Zone 级故障转移，生产推荐！）

3. 面试考点（10 题）
   3.1 Ribbon 两种 ServerList？静态（配置文件）/ DiscoveryClient（Nacos/Eureka 动态）
   3.2 IRule 默认用哪个？ZoneAvoidanceRule（Zone 级故障转移更健壮）
   3.3 IPing 健康检查默认？NoOpPing（不检查）→ 生产改 PingUrl
   3.4 Ribbon 和 Nacos 分工？Nacos 注册中心（服务发现）→ Ribbon 选 Server（负载均衡）
   3.5 Ribbon vs LoadBalancer（Spring Cloud 2021+）？LoadBalancer 是 Ribbon 替代（Spring Cloud Commons 内置）
```

---

## 三、P1 组件调研大纲（简述）

### P1-1 Spring Cloud Config 配置中心

```
核心链路：
  ConfigServer（Git/SVN/本地资源/Native）
    → EnvironmentRepository.findOne（拉取配置）
    → ConfigController（HTTP 暴露）
    → ConfigClient（@PropertySource + Environment）
    → @RefreshScope（刷新 scope 重建 Bean）

面试考点：
  - Config 客户端怎么监听配置变更？RefreshEndpoint + /actuator/refresh
  - @RefreshScope 原理？RefreshScope 是 Scope 实现 → 刷新时清空 scope → getBean 重新创建
  - Config 多文件组合？application-{name}.properties + bootstrap.properties
```

### P1-2 Spring Cloud Bus 消息总线

```
核心链路：
  Config 变更 → RefreshEndpoint → BusRefreshEvent → RabbitMQ/Kafka 广播
    → 所有监听节点收到事件 → EnvironmentChangeEvent → @RefreshScope 刷新

面试考点：
  - Bus 怎么触发刷新？curl /actuator/bus-refresh
  - Bus 默认 RabbitMQ/Kafka？Spring Cloud 2021+ 两者都支持，依赖哪个用哪个
  - 和 Nacos 配置中心的配置变更通知有什么区别？Bus 是 Spring Cloud 内部机制；Nacos 是自身的
```

### P1-3 Sleuth 链路追踪

```
核心链路：
  SleuthFilter / BraveServletFilter（HTTP 请求拦截器）
    → Span 生成（traceId + spanId）
    → MDC 注入（日志 %d{traceId:-}）
    → 下游 Feign/RestTemplate 调用时透传 Header
    → JDBC 拦截器注入 SQL traceId
    → Logback/SLF4J 日志输出 traceId

面试考点：
  - traceId 和 spanId 区别？traceId 整条链路唯一；spanId 当前节点唯一（父子关系）
  - 怎么在日志里打 traceId？Logback MDC + %d{traceId:-}
  - Sleuth 和 SkyWalking/Zipkin 区别？Sleuth 是链路追踪埋点（Brave 包装）；SkyWalking/Zipkin 是后端收集展示
```

---

## 四、P2 组件调研大纲（简述）

### P2-1 Seata 分布式事务 AT 模式

```
核心链路（AT 模式，★ 面试高）：
  1. 开启全局事务：@GlobalTransactional → GlobalBeginRequest → TC
  2. 执行业务 SQL
     → Seata DataSourceProxy（数据源代理）
     → 执行 SELECT ... FOR UPDATE（前镜像）
     → 执行业务 SQL（UPDATE/INSERT/DELETE）
     → 执行 SELECT ... FOR UPDATE（后镜像）
     → 写入 undo_log（前镜像 + 后镜像 + 业务 SQL）
  3. 提交/回滚：
     → 提交：undo_log 删除 + 全局事务标记 committed
     → 回滚：读 undo_log → 根据前镜像恢复数据 + 业务 SQL 反向写回

面试考点：
  - AT 模式一阶段就提交了？是的，二阶段异步清理 undo_log
  - AT 模式怎么防止脏写？全局锁 + 本地 FOR UPDATE
  - undo_log 在哪张表？每业务表的 undo_log（global_id + branch_id + before + after）
  - Seata 和 TCC/Saga 区别？AT 自动（数据源代理 + SQL 拦截）；TCC/Saga 手动实现两阶段
```

### P2-2 Resilience4j 熔断（替代 Hystrix）

```
核心模块：
  CircuitBreaker（状态机：CLOSED→OPEN→HALF_OPEN）
  Bulkhead（隔离：信号量/线程池）
  RateLimiter（限流：令牌桶）
  Retry（重试）
  Fallback（降级）

面试考点：
  - CircuitBreaker 三种状态？CLOSED（正常）/ OPEN（熔断）/ HALF_OPEN（放行一个探测请求）
  - 和 Sentinel 区别？Sentinel 更重（完整限流框架 + 控制台）；Resilience4j 轻量（只做熔断/隔离/重试）
  - 线程隔离 vs 信号量隔离？线程隔离：每个熔断用独立线程池；信号量隔离：总线程数限制
```

---

## 五、源码访问策略

### 5.1 Maven 本地仓（推荐）

```bash
# Spring Cloud OpenFeign
$ mvn dependency:get -Dartifact=org.springframework.cloud:spring-cloud-openfeign-core:3.1.8
# → 本地仓 C:\Users\xxx\.m2\repository\org\springframework\cloud\spring-cloud-openfeign-core\3.1.8\

# Nacos（本地仓没有 → GitHub 下载 zip）
# → https://github.com/nacos-group/nacos/releases/tag/2.2.3
# → 解压 nacos-server-2.2.3.jar 或源码

# Sentinel
# → https://github.com/alibaba/Sentinel/releases/tag/1.8.6
```

### 5.2 IDEA Decompile（快速验证）

```
IDEA 直接 Ctrl+Click 类名 → 反编译查看源码（即使没有 jar 的源码 jar）
→ 大多数 Spring Cloud 组件都可以这样看反编译代码
→ 对于 P0 调研足够用（不需要完整带注释源码）
```

### 5.3 源码仓库 Clone（深度调研）

```bash
git clone https://github.com/nacos-group/nacos.git
git clone https://github.com/alibaba/Sentinel.git
git clone https://github.com/spring-cloud/spring-cloud-openfeign.git
git clone https://github.com/spring-cloud/spring-cloud-gateway.git
git clone https://github.com/seata/seata.git
```

---

## 六、调研产出计划

| 阶段 | 组件 | 产出 md 文档 | 预计时间 |
|------|------|-------------|---------|
| Phase 1 | Nacos | 19_SpringCloud_Nacos源码解析.md | 3 天 |
| Phase 2 | OpenFeign | 20_SpringCloud_OpenFeign源码解析.md | 2 天 |
| Phase 3 | Sentinel | 21_SpringCloud_Sentinel源码解析.md | 3 天 |
| Phase 4 | Gateway | 22_SpringCloud_Gateway源码解析.md | 3 天 |
| Phase 5 | Ribbon | 23_SpringCloud_Ribbon源码解析.md | 1 天 |
| Phase 6 | Config + Bus + Sleuth | 24_SpringCloud_ConfigBusSleuth.md | 2 天 |
| Phase 7 | Seata AT 模式 | 25_Seata_AT模式源码解析.md | 3 天 |
| Phase 8 | Resilience4j | 26_Resilience4j熔断源码解析.md | 1 天 |
| **合计** | 10 个组件 | **8 份 md 文档** | **18 天** |

---

**调研大纲完成！** 按优先级从 P0→P1→P2 依次执行。P0 五个组件（Nacos/OpenFeign/Sentinel/Gateway/Ribbon）是国内互联网面试最核心的。
