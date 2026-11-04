# Spring Cloud Config 配置中心源码深度解析

> **源码基线**：Spring Cloud Config Server 3.1.8（Spring Boot 2.7.x 配套）  
> **核心仓库**：https://github.com/spring-cloud/spring-cloud-config  
> **面试权重**：⭐⭐⭐⭐

---

## 一、Config 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  Config Server（配置服务端）                                   │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  EnvironmentRepository（★ 核心 SPI，拉配置策略可插拔）   │  │
│  │    ├─ GitRepository（默认！clone Git → 读配置文件）      │  │
│  │    ├─ SvnRepository（SVN）                              │  │
│  │    ├─ NativeRepository（本地文件系统）                    │  │
│  │    └─ VaultRepository / JDBCRepository / RedisRepository│  │
│  │                                                         │  │
│  │  ConfigController（HTTP 暴露）                            │  │
│  │    /{application}/{profile}[/{label}]                    │  │
│  │    /{application}-{profile}.yml / .properties             │  │
│  │    /actuator/env（手动刷新）                              │  │
│  └───────────────────────────────────────────────────────┘  │
│                              ↑ HTTP 拉取                      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Config Client（配置客户端，每个微服务实例）                    │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  bootstrap.yml（★ 关键！必须 bootstrap，不能 application）│  │
│  │    spring.cloud.config.uri= http://config-server:8888   │  │
│  │    spring.cloud.config.name= order-service              │  │
│  │                                                         │  │
│  │  ConfigDataEnvironmentPostProcessor（启动时拉配置）        │  │
│  │    → 在 ApplicationContext.refresh 之前执行              │  │
│  │    → RestTemplate.getForObject(configUrl, Environment)  │  │
│  │    → EnvironmentPropertySourceLocator 包装成 PropertySource│
│  │    → 注入到 Spring Environment 的 propertySources 列表最前面│
│  │                                                         │  │
│  │  @RefreshScope（★ 配置变更时刷新 Bean）                    │  │
│  │    → RefreshScope is Spring Cloud Commons 内置            │  │
│  │    → Scope.getBean → 从 RefreshScope 的缓存中取          │  │
│  │    → 刷新时 RefreshScope.clear → 下次 getBean 重新创建    │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Server 端：GitRepository 怎么拉配置

```java
// ★ GitRepository 是 EnvironmentRepository 的默认实现
//   Config Server 启动时 → Spring Boot AutoConfiguration
//   → EnvironmentRepositoryConfiguration
//   → 如果 spring.cloud.config.server.git.uri 配置了 → 创建 GitRepository
//
// GitRepository.findOne(application, profile, label) 流程：
//
// 1. clone 或 fetch Git 仓库
//    Git.cloneOrFetch(uri, baseDir, label)
//    → 首次 clone 到本地临时目录
//    → 后续 fetch + reset --hard（保证和远端一致）
//
// 2. 遍历 profile 目录
//    找 application-{profile}.yml / .properties / .yaml
//    找 application.yml / .properties（全局配置）
//    找 {application}-{profile}.yml（应用级 profile 配置）
//    合并优先级：应用级 profile > 全局 profile > 应用级默认 > 全局默认
//
// 3. 构造 Environment 对象返回
//    Environment.add(PropertySource)
//    ConfigController 把 Environment 序列化成 JSON 返回给 Client
```

### 2.2 Client 端：ConfigDataEnvironmentPostProcessor（★ 启动时拉配置）

```java
// ★ ConfigDataEnvironmentPostProcessor 是 Spring Boot 启动扩展点
//   实现 EnvironmentPostProcessor 接口
//   在 ApplicationContext 构造完成但还没 refresh 之前执行
//
// 执行时机（Spring Boot 启动阶段）：
//   ① 读取 bootstrap.yml → 创建 Environment（空）
//   ② ConfigDataEnvironmentPostProcessor.postProcessEnvironment
//      → 扫描所有 spring.cloud.config.uri
//      → RestTemplate GET http://config-server/{name}/{profile}/{label}
//      → 拿到 Environment JSON → 转换成 PropertySource
//      → addFirst 到 Environment.propertySources（优先级最高！）
//   ③ ApplicationContext.refresh
//      → @Value / @ConfigurationProperties 绑定的就是 Config Server 返回的值
//
// 为什么必须 bootstrap.yml 不能 application.yml？
//   因为 bootstrap 是最早加载的 Phase（BOOTSTRAP 阶段）
//   ConfigDataEnvironmentPostProcessor 就是在这个阶段执行
//   application.yml 还没加载 → 所以 config server 地址只能写 bootstrap.yml
```

### 2.3 @RefreshScope 原理（★★★★★ 面试必问）

```java
// ★ @RefreshScope 注解作用在 @Bean 上
//   Spring Cloud Commons 的 Scope 实现
//
// 原理：RefreshScope 是一个 Scope
//   Scope 是 Spring BeanFactory 的缓存容器
//   默认有 singleton（单例）/ prototype（原型）/ session / request 等
//   RefreshScope 就是 Spring Cloud 新增的 Scope
//
// 执行流程：
//   1. 第一次 getBean("myRefreshBean")
//      → RefreshScope.get(name)
//      → 如果缓存里没有 → ObjectFactory.getObject() 创建 Bean
//      → 存入 RefreshScope 的 ConcurrentHashMap<String, Object> cache
//      → 返回 Bean
//
//   2. 配置变更触发 EnvironmentChangeEvent
//      → EnvironmentChangeEvent 监听（Spring Cloud Bus 或 Actuator /refresh）
//      → RefreshScope 收到事件
//      → RefreshScope.clear() → 清空所有 cache
//
//   3. 再次 getBean("myRefreshBean")
//      → RefreshScope.get(name)
//      → cache 里没有了 → 重新 ObjectFactory.getObject() 创建 Bean
//      → 此时用的是最新 Environment → 配置更新生效！
//
// ★ 注意：@RefreshScope 只影响 Scope 内的 Bean
//   如果配置了 @Value("${timeout}") 的普通 singleton Bean → 不会自动刷新
//   必须加 @RefreshScope！
//
// ★ EnvironmentChangeEvent 触发路径：
//   ① Actuator /actuator/refresh → RefreshScopeRefreshedEvent
//   ② Spring Cloud Bus /actuator/bus-refresh → 广播 RefreshRemoteApplicationEvent → 各节点本地刷新
```

---

## 三、面试考点（12 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Config Server 支持哪些后端？| **Git（默认）/ SVN / Native（本地文件）/ Vault / JDBC / Redis / JDBC**（EnvironmentRepository SPI 可插拔）|
| 2 | bootstrap.yml 和 application.yml 区别？| **bootstrap.yml 在 ApplicationContext 构造之前加载**（BOOTSTRAP 阶段）→ Config Server 地址必须写这里；application.yml 在之后加载 → 可以引用 Config Server 的值 |
| 3 | 客户端怎么拉配置？| ConfigDataEnvironmentPostProcessor → **RestTemplate GET ConfigController** → 拿到 Environment → addFirst 到 propertySources |
| 4 | @RefreshScope 原理？| **RefreshScope 是一个 Scope** → Bean 创建后存在 Scope 的 cache 里 → 配置变更时 clear cache → 下次 getBean 重新创建（用新 Environment）|
| 5 | 配置变更怎么触发刷新？| 两种方式：① **Actuator /actuator/refresh**（单节点）② **Spring Cloud Bus /actuator/bus-refresh**（集群广播）→ 都触发 EnvironmentChangeEvent → RefreshScope.clear |
| 6 | Config Server 怎么保护？| spring-security + 配置 spring.security.user.name/password → Client 配置 spring.cloud.config.username/password |
| 7 | Git 分支怎么指定？| spring.cloud.config.label= main / master / feature-v2（Config Server GitRepository 会 checkout 到这个 label）|
| 8 | 多 profile 怎么组合？| application-{name}-{profile}.yml → profile 会覆盖默认 profile（同名 key 后加载覆盖先加载）|
| 9 | 配置优先级？| **bootstrap.yml > Config Server > application.yml** → 谁先加载谁优先级高（addFirst 插在最前面）|
| 10 | Config vs Nacos 配置中心？| Config：Git 驱动 + EnvironmentRepository SPI + RefreshScope（Spring Cloud 原生）；Nacos：动态推 + 控制台 + 监听（阿里巴巴）|
| 11 | 配置加密？| JCE（jasypt-spring-boot）或 Vault 后端 → Config Server 解密后返回明文给 Client |
| 12 | 配置文件本地缓存？| spring.cloud.config.allowOverride=true + overrideNone=false → Client 可以用本地文件覆盖远端 |

---

**文档版本：v1.0 · 2026-10-21**  
**源码基线**：Spring Cloud Config 3.1.8  
**面试考点：12 题**
