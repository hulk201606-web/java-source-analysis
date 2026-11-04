# 229 · Spring 体系高频面试题库（55 题 · 详解）

> **方向**：IoC/DI、Bean 生命周期、循环依赖、AOP、事务、MVC、Spring Boot 自动配置  
> **面试权重**：⭐⭐⭐⭐⭐（Java 后端必问）  
> **配套源码文档**：[016_Spring核心源码解析.md](016_Spring核心源码解析.md)、[017_SpringAOP源码解析.md](017_SpringAOP源码解析.md)、[018_Spring事务管理源码解析.md](018_Spring事务管理源码解析.md)、[019_Spring扩展点源码解析.md](019_Spring扩展点源码解析.md)、[020_SpringMVC源码解析.md](020_SpringMVC源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问。

---

## 一、IoC 与 Bean 生命周期（Q1–Q14）

### Q1. Spring 的核心是什么？

**结论**：IoC 容器与 AOP，前者管理对象与依赖，后者把横切逻辑从业务代码中解耦。

**机制**：
- IoC 通过容器创建、装配、管理 Bean。
- DI 是 IoC 的实现方式，注入依赖。
- AOP 通过代理织入切面。
- Spring Boot 在其上提供自动配置与 starters。

**追问**：Spring 一定需要 XML 吗？不需要，注解与 Java Config 已主流。

### Q2. 如何理解 IoC 与 DI？

**结论**：IoC 是控制权反转，对象由容器创建；DI 是容器把依赖注入对象。

**机制**：
- 传统代码自己 `new` 依赖。
- IoC 后由容器决定生命周期与依赖。
- DI 方式：构造器、setter、字段。
- 好处是解耦、可测试、可替换实现。

**追问**：IoC 等于 DI 吗？不完全，DI 是 IoC 的一种实现手段。

### Q3. BeanFactory 与 ApplicationContext 有什么区别？

**结论**：BeanFactory 是最底层容器，ApplicationContext 在其上增加事件、国际化、资源加载、自动注册后处理器等能力。

**机制**：
- BeanFactory 懒加载，getBean 时才创建。
- ApplicationContext 启动时预实例化单例。
- ApplicationContext 支持事件发布、Environment、AOP 集成。
- 实际项目几乎都用 ApplicationContext。

**追问**：ApplicationContext 一定是 BeanFactory 吗？是，它继承 BeanFactory 接口。

### Q4. Bean 的完整生命周期是怎样的？

**结论**：实例化 → 属性填充 → Aware 回调 → BeanPostProcessor 前置 → 初始化 → 后置 → 使用 → 销毁。

**机制**：
- 构造器/工厂方法实例化。
- `populateBean` 注入依赖。
- Aware 接口注入容器资源。
- `@PostConstruct`、`InitializingBean`、init-method。
- AOP 代理通常在 BeanPostProcessor 后置阶段创建。
- 销毁回调 `@PreDestroy`、DisposableBean、destroy-method。

**追问**：AOP 代理在哪个阶段生成？`postProcessAfterInitialization`，所以初始化方法在目标对象上执行。

### Q5. Spring 如何解决单例 Bean 的循环依赖？

**结论**：通过三级缓存提前暴露早期引用，允许 A 注入尚未完成初始化的 B。

**机制**：
- 一级 `singletonObjects`：完整单例。
- 二级 `earlySingletonObjects`：早期引用。
- 三级 `singletonFactories`：ObjectFactory，可生成代理。
- A 实例化后把工厂放入三级缓存，B 依赖 A 时拿到早期引用。
- 若需要 AOP，工厂返回代理对象。

**追问**：为什么需要三级而不是两级？为了支持 AOP 代理的延迟创建，避免提前生成错误代理。

### Q6. 构造器循环依赖能解决吗？

**结论**：不能，构造器注入必须在实例化时拿到依赖，此时对象尚未创建，无法提前暴露。

**机制**：
- 三级缓存只能处理属性注入阶段的循环。
- 构造器循环会直接报 BeanCurrentlyInCreationException。
- 解决：改 setter/字段注入，或重构依赖。
- `@Lazy` 可延迟注入代理。

**追问**：protoType 循环依赖能解决吗？不能，原型 Bean 不缓存，循环依赖直接失败。

### Q7. @Autowired 的注入原理是什么？

**结论**：由 `AutowiredAnnotationBeanPostProcessor` 在属性填充阶段解析，按类型找 Bean，必要时按名称/Qualifier 消歧。

**机制**：
- 扫描字段、setter、构造器上的注解。
- 按类型匹配候选 Bean。
- 多个候选时用 `@Primary`、`@Qualifier`、字段名。
- 通过反射注入。

**追问**：@Autowired 能用在构造器上吗？可以，推荐构造器注入，便于不可变与测试。

### Q8. @Resource 与 @Autowired 有什么区别？

**结论**：@Autowired 是 Spring 注解，默认按类型；@Resource 是 JSR-250，默认按名称。

**机制**：
- @Autowired 可配 `required=false`。
- @Resource 按 name 匹配，找不到再按类型。
- @Resource 不支持 `@Primary` 的同等语义。
- 两者都可由后处理器处理。

**追问**：推荐哪个？Spring 项目通常用 @Autowired/构造器注入，减少对具体容器 API 的耦合。

### Q9. Spring Bean 有哪些作用域？

**结论**：singleton、prototype、request、session、application、websocket。

**机制**：
- singleton：容器内单例，默认。
- prototype：每次 getBean 新建。
- request/session：Web 请求/会话级。
- application：ServletContext 级。
- 自定义作用域可实现 Scope 接口。

**追问**：prototype 的销毁由谁管理？Spring 不管理，调用方负责清理。

### Q10. FactoryBean 与 BeanFactory 有什么区别？

**结论**：BeanFactory 是容器；FactoryBean 是“生产 Bean 的工厂 Bean”，用于创建复杂对象。

**机制**：
- `FactoryBean.getObject()` 返回实际对象。
- `&beanName` 获取 FactoryBean 本身。
- 常用于整合 MyBatis Mapper、代理对象。
- 与普通 Bean 的创建流程不同。

**追问**：FactoryBean 会被 AOP 代理吗？可以，但要注意代理作用于 getObject 返回对象还是工厂本身。

### Q11. BeanPostProcessor 的作用是什么？

**结论**：在 Bean 初始化前后做增强，是 AOP、@Autowired、事件监听等的扩展基础。

**机制**：
- `postProcessBeforeInitialization` 在初始化前。
- `postProcessAfterInitialization` 在初始化后。
- 可返回代理对象替换原 Bean。
- 自身也是 Bean，需注意初始化顺序。

**追问**：BeanPostProcessor 能修改 BeanDefinition 吗？不能，那是 BeanFactoryPostProcessor 的职责。

### Q12. BeanFactoryPostProcessor 的作用是什么？

**结论**：在 Bean 实例化前修改 BeanDefinition 或注册新定义，如占位符解析、配置类解析。

**机制**：
- `postProcessBeanFactory` 遍历 BeanDefinition。
- `ConfigurationClassPostProcessor` 解析 @Configuration/@ComponentScan。
- `PropertySourcesPlaceholderConfigurer` 解析占位符。
- 执行时机早于 BeanPostProcessor。

**追问**：它和 BeanPostProcessor 的顺序？BFPP 先于 Bean 实例化，BPP 在实例化后。

### Q13. Spring 的初始化回调顺序是怎样的？

**结论**：构造器 → @Autowired → Aware → @PostConstruct → InitializingBean → init-method → BeanPostProcessor 后置。

**机制**：
- 属性注入完成后执行 Aware。
- `@PostConstruct` 由 CommonAnnotationBeanPostProcessor 调用。
- `afterPropertiesSet` 随后。
- 自定义 init-method 最后。
- 之后才是 AOP 代理后置处理。

**追问**：销毁顺序呢？@PreDestroy → DisposableBean → destroy-method，与初始化大致相反。

### Q14. @Conditional 的作用是什么？

**结论**：按条件决定是否注册 Bean/配置类，是 Spring Boot 自动配置的核心。

**机制**：
- 实现 `Condition.matches` 判断。
- Boot 提供 @ConditionalOnClass/@ConditionalOnMissingBean 等。
- 条件在解析配置类时评估。
- 支持组合条件与自定义条件。

**追问**：条件评估失败会怎样？跳过该 Bean/配置类，不影响其他配置。

---

## 二、AOP（Q15–Q22）

### Q15. AOP 的核心概念有哪些？

**结论**：切面、切点、连接点、通知、织入、目标对象、代理。

**机制**：
- 切面是横切逻辑模块。
- 切点定义哪些方法被拦截。
- 连接点是可被拦截的执行点。
- 通知是拦截后执行的动作。
- 织入把切面应用到目标对象。

**追问**：Spring AOP 的织入时机？运行时通过代理织入，不是编译期。

### Q16. Spring AOP 的 JDK 动态代理与 CGLIB 有什么区别？

**结论**：JDK 代理基于接口，CGLIB 基于子类；有接口默认 JDK，无接口用 CGLIB。

**机制**：
- JDK 代理实现 InvocationHandler，生成接口代理。
- CGLIB 生成子类，覆盖方法。
- CGLIB 不能代理 final 类/方法。
- Boot 2.x 起默认使用 CGLIB（proxyTargetClass=true）。

**追问**：为什么 Boot 默认 CGLIB？避免接口缺失导致的代理失败，统一行为。

### Q17. Spring AOP 的拦截器链是如何执行的？

**结论**：`ReflectiveMethodInvocation.proceed()` 递归调用拦截器，形成“洋葱模型”。

**机制**：
- 拦截器按顺序封装为 MethodInterceptor。
- proceed 调用下一个拦截器，最后调用目标方法。
- 前置逻辑在递归前，后置逻辑在递归后。
- 异常可被环绕通知捕获。

**追问**：多个切面的顺序如何决定？由 `@Order`/Ordered 控制，顺序影响嵌套关系。

### Q18. 切点表达式怎么写？

**结论**：常用 `execution`、`@annotation`、`within`、`args`、`this/target` 等指示符组合。

**机制**：
- `execution(* com.xxx.service.*.*(..))` 按方法签名匹配。
- `@annotation(Transactional)` 按注解匹配。
- `within` 按类型匹配。
- 可用 `&&`、`||`、`!` 组合。

**追问**：切点越多性能越差吗？匹配在启动/首次调用时解析，运行时有一定开销，应避免过宽切点。

### Q19. Spring AOP 有哪些通知类型？

**结论**：Before、AfterReturning、AfterThrowing、After、Around 五种。

**机制**：
- Before 在方法前。
- AfterReturning 正常返回后。
- AfterThrowing 异常时。
- After 是 finally 语义。
- Around 可控制是否执行目标方法。

**追问**：Around 与 Before/After 的关系？Around 最灵活，可覆盖其他通知能做的事。

### Q20. 为什么同类内部调用会导致 AOP 失效？

**结论**：自调用绕过代理，直接调用目标对象方法，切面不会执行。

**机制**：
- 外部调用经过代理，代理再调用目标。
- 目标对象内部 `this.method()` 不经过代理。
- 事务、缓存、异步注解都会失效。
- 解决：注入自身代理、AopContext、拆分到另一个 Bean。

**追问**：`AopContext.currentProxy()` 需要什么配置？`exposeProxy=true`。

### Q21. @Transactional 为什么会失效？

**结论**：常见于自调用、private/final 方法、异常被吞、异常类型不匹配、事务管理器不匹配。

**机制**：
- 代理无法拦截 private/final/static 方法。
- 默认只回滚 RuntimeException/Error。
- 捕获异常未抛出则不会回滚。
- 多数据源需指定事务管理器。
- 类未被 Spring 管理也不会生效。

**追问**：checked 异常如何回滚？`@Transactional(rollbackFor = Exception.class)`。

### Q22. Spring AOP 与 AspectJ 有什么区别？

**结论**：Spring AOP 是运行时代理，只支持方法级；AspectJ 是编译/加载期织入，能力更强。

**机制**：
- Spring AOP 依赖代理，不能拦截字段/构造器。
- AspectJ 可织入字段访问、构造器、静态方法。
- AspectJ 性能更好但配置复杂。
- Spring 可集成 AspectJ 注解与部分切点。

**追问**：为什么大多数项目用 Spring AOP？够用、无额外编译步骤、与容器集成好。

---

## 三、事务管理（Q23–Q30）

### Q23. Spring 事务的实现原理是什么？

**结论**：通过 AOP 代理拦截事务方法，`TransactionInterceptor` 负责开启、提交、回滚与资源绑定。

**机制**：
- `@EnableTransactionManagement` 注册事务基础设施。
- 拦截器获取事务管理器。
- `getTransaction` 开启事务并绑定连接。
- 方法成功提交，异常回滚。
- 连接通过 ThreadLocal 绑定到当前线程。

**追问**：事务代理和 AOP 代理是同一个吗？通常由同一个代理链处理，事务是其中一个拦截器。

### Q24. Spring 的 7 种事务传播行为是什么？

**结论**：REQUIRED、REQUIRES_NEW、NESTED、SUPPORTS、NOT_SUPPORTED、MANDATORY、NEVER。

**机制**：
- REQUIRED：有事务加入，无则新建，默认。
- REQUIRES_NEW：挂起当前，新建独立事务。
- NESTED：嵌套事务，用 savepoint。
- SUPPORTS：有则加入，无则非事务执行。
- NOT_SUPPORTED：挂起事务，非事务执行。
- MANDATORY：必须有事务，否则报错。
- NEVER：必须无事务，否则报错。

**追问**：REQUIRES_NEW 与 NESTED 区别？前者独立提交/回滚，后者随外层回滚。

### Q25. Spring 事务的隔离级别有哪些？

**结论**：DEFAULT、READ_UNCOMMITTED、READ_COMMITTED、REPEATABLE_READ、SERIALIZABLE。

**机制**：
- DEFAULT 使用数据库默认。
- RC 防脏读。
- RR 防不可重复读，MySQL 默认。
- Serializable 防幻读但性能最差。
- Spring 只传递配置，具体由数据库实现。

**追问**：MySQL RR 能完全防幻读吗？InnoDB 通过 MVCC + 间隙锁在多数场景下防幻读。

### Q26. 什么是 rollback-only？为什么会出现 UnexpectedRollbackException？

**结论**：内层事务标记回滚但外层捕获异常继续提交时，外层被标记 rollback-only，最终抛 UnexpectedRollbackException。

**机制**：
- 内层 REQUIRED 加入外层事务。
- 内层异常触发 `setRollbackOnly`。
- 外层捕获异常后仍尝试 commit。
- 事务管理器发现 rollback-only，拒绝提交。

**追问**：如何避免？内层用 REQUIRES_NEW，或让异常继续抛出。

### Q27. 事务同步（TransactionSynchronization）有什么用？

**结论**：在事务提交/回滚前后执行回调，用于缓存清理、事件发布、消息发送等。

**机制**：
- `TransactionSynchronizationManager` 注册同步器。
- `beforeCommit`、`afterCommit`、`afterCompletion`。
- 资源绑定在 ThreadLocal。
- 适合“事务提交后再做”的动作。

**追问**：afterCommit 抛异常会回滚吗？不会，事务已提交，需自行补偿/记录。

### Q28. 嵌套事务（NESTED）是如何实现的？

**结论**：使用数据库 savepoint，内层回滚只回到保存点，外层仍可继续。

**机制**：
- 外层事务创建 savepoint。
- 内层失败回滚到 savepoint。
- 外层可选择继续或整体回滚。
- 依赖 JDBC 与数据库支持。

**追问**：NESTED 与 REQUIRES_NEW 的资源关系？NESTED 共用外层连接，REQUIRES_NEW 使用新连接。

### Q29. 大事务有什么问题？如何优化？

**结论**：长时间占用连接与锁、undo 膨胀、主从延迟、回滚成本高，应拆分与缩短。

**机制**：
- 避免在事务内做远程调用/文件 IO。
- 拆分批量操作为小批次。
- 减少锁范围与事务粒度。
- 非核心逻辑放到事务外或异步。

**追问**：事务中能发消息吗？本地消息表/事务同步更可靠，直接发消息可能先于提交。

### Q30. Spring 如何处理分布式事务？

**结论**：Spring 本身只提供本地事务抽象，分布式事务需 Seata、消息最终一致、TCC/Saga 等方案。

**机制**：
- 本地事务用 `PlatformTransactionManager`。
- 分布式可用 Seata AT/TCC/Saga。
- 也可用本地消息表 + 重试 + 对账。
- 强一致与最终一致按业务选择。

**追问**：Seata AT 的原理？代理数据源，记录 undo log，二阶段提交/回滚。

---

## 四、MVC 与 Spring Boot（Q31–Q40）

### Q31. DispatcherServlet 的请求处理流程是什么？

**结论**：获取 Handler → 找 HandlerAdapter → 执行拦截器前置 → 调用处理器 → 处理返回值 → 渲染/写响应 → 后置与完成回调。

**机制**：
- `getHandler` 遍历 HandlerMapping。
- `getHandlerAdapter` 按处理器类型选择。
- `applyPreHandle` 执行拦截器。
- `ha.handle` 执行业务方法。
- `processDispatchResult` 处理异常与视图。

**追问**：前后端分离项目还走视图解析吗？`@ResponseBody` 直接写 JSON，不走 ViewResolver。

### Q32. HandlerMapping 与 HandlerAdapter 的作用是什么？

**结论**：Mapping 负责 URL 到 Handler 的映射，Adapter 负责调用不同类型的 Handler。

**机制**：
- RequestMappingHandlerMapping 解析注解映射。
- 匹配路径、方法、参数、Header。
- RequestMappingHandlerAdapter 支持 HandlerMethod。
- 其他 Adapter 支持 Controller 接口、函数式端点。

**追问**：为什么需要 Adapter？让 DispatcherServlet 与具体处理器实现解耦。

### Q33. Spring MVC 的参数解析是怎么做的？

**结论**：由 `HandlerMethodArgumentResolver` 链按参数类型与注解解析，如 @RequestParam、@RequestBody、@PathVariable。

**机制**：
- 每个解析器实现 `supportsParameter` 与 `resolveArgument`。
- 按注册顺序匹配。
- `@RequestBody` 用 HttpMessageConverter 反序列化。
- 复杂对象可做数据绑定。

**追问**：自定义参数解析器怎么加？实现接口并注册到 `WebMvcConfigurer`。

### Q34. 返回值是如何处理的？

**结论**：由 `HandlerMethodReturnValueHandler` 处理，@ResponseBody 走消息转换器写响应，否则走视图解析。

**机制**：
- `RequestResponseBodyMethodProcessor` 处理 @ResponseBody。
- 根据 Accept 选择 Converter。
- 返回 ModelAndView 时交给 ViewResolver。
- 返回 void 时可手动写响应。

**追问**：JSON 序列化用哪个转换器？Jackson 的 MappingJackson2HttpMessageConverter。

### Q35. Spring MVC 拦截器与 Filter 有什么区别？

**结论**：Filter 属于 Servlet 容器，拦截所有请求；Interceptor 属于 Spring MVC，能访问 Handler 与 Model。

**机制**：
- Filter 在 DispatcherServlet 之前。
- Interceptor 的 preHandle/postHandle/afterCompletion 围绕 Handler。
- Interceptor 可拿到 HandlerMethod。
- Filter 更适合编码、跨域、安全过滤。

**追问**：拦截器能拦截静态资源吗？取决于映射配置，默认静态资源可能不经过 Handler。

### Q36. @ControllerAdvice 与 @ExceptionHandler 如何工作？

**结论**：@ControllerAdvice 定义全局通知，@ExceptionHandler 统一处理异常并返回响应。

**机制**：
- 异常解析器扫描 ControllerAdvice。
- 按异常类型匹配处理方法。
- 可返回 ResponseEntity 或 @ResponseBody。
- 可结合 @RestControllerAdvice 简化。

**追问**：异常处理顺序？越具体的异常类型优先，未被处理则交给容器默认错误页。

### Q37. Spring Boot 自动配置的原理是什么？

**结论**：`@EnableAutoConfiguration` 导入自动配置类，结合条件注解按 classpath 与配置决定生效。

**机制**：
- `AutoConfigurationImportSelector` 读取 `META-INF/spring/...AutoConfiguration.imports`（旧版 spring.factories）。
- 条件注解过滤。
- `@ConditionalOnMissingBean` 允许用户覆盖。
- 配置类注册 Bean。

**追问**：如何排除自动配置？`@SpringBootApplication(exclude=...)` 或配置 `spring.autoconfigure.exclude`。

### Q38. 如何自定义一个 Starter？

**结论**：提供自动配置类 + 条件注解 + 配置属性 + `AutoConfiguration.imports` 注册。

**机制**：
- 编写 `@AutoConfiguration` 类。
- 用 `@ConditionalOnClass/@ConditionalOnProperty` 控制。
- 绑定 `@ConfigurationProperties`。
- 在 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 声明。

**追问**：Starter 与自动配置的区别？Starter 是依赖聚合，自动配置是生效逻辑。

### Q39. Spring Boot 配置优先级是怎样的？

**结论**：命令行参数 > 环境变量 > 配置文件 > 默认值，profile 与外部配置可覆盖内部。

**机制**：
- `application.properties/yml`。
- `application-{profile}` 覆盖默认。
- 外部配置、命令行、环境变量优先级更高。
- `@ConfigurationProperties` 绑定。
- 可用 `spring.config.location` 指定位置。

**追问**：同名配置谁生效？优先级高者覆盖低者，列表类型可能合并而非覆盖。

### Q40. Spring Boot 启动流程与事件有哪些？

**结论**：`SpringApplication.run` 创建环境、打印 Banner、创建上下文、准备上下文、刷新、启动后回调。

**机制**：
- 加载 ApplicationListener/Initializer。
- 准备 Environment。
- 创建并 refresh ApplicationContext。
- 调用 Runner。
- 事件：ApplicationStarting、EnvironmentPrepared、ContextPrepared、ContextLoaded、Started、Ready。

**追问**：CommandLineRunner 与 ApplicationRunner 区别？前者拿字符串数组，后者拿 ApplicationArguments。

---

## 五、进阶补充（Q41–Q45）

### Q41. Spring Security 的认证授权链路是怎样的？

**结论**：FilterChain 拦截请求 → AuthenticationManager 认证 → SecurityContext 保存身份 → AuthorizationManager 做授权。

**机制**：
- `SecurityContextPersistenceFilter` 恢复上下文。
- 用户名密码/Token 过滤器触发认证。
- `ProviderManager` 调用 `UserDetailsService` 与密码编码器。
- 授权过滤器按角色/权限判断。
- 认证结果放入 SecurityContext。

**追问**：JWT 无状态认证放哪一层？放在自定义 Filter，解析后设置 SecurityContext，并做令牌校验。

### Q42. Spring Boot 3 的 AOT 与 Native Image 支持是什么？

**结论**：Spring AOT 在构建期生成反射/资源提示，配合 GraalVM Native Image 生成快速启动的原生可执行文件。

**机制**：
- AOT 处理 Bean 定义、代理与反射元数据。
- Native Image 需要 runtime hints。
- 启动快、内存低，但动态能力受限。
- 需验证框架与库的兼容性。

**追问**：所有 Spring 应用都适合 Native Image 吗？大量动态代理/脚本/反射的框架需额外配置，成本较高；通用 AOT/CDS/Graal 对比见 [227 Q41](227_Java9+新特性面试题库.md)。

### Q43. Spring 的参数校验是如何生效的？

**结论**：基于 Bean Validation（JSR-380），`@Valid`/`@Validated` 触发校验，失败抛异常由全局异常处理转换。

**机制**：
- Controller 参数加 `@Valid` 触发方法参数校验。
- 类级别 `@Validated` 支持方法校验。
- `MethodValidationPostProcessor` 处理服务层校验。
- 自定义约束与分组校验。

**追问**：为什么校验没生效？缺少 `@Valid`、代理未生效、分组不匹配或异常被吞。

### Q44. Spring WebFlux 的线程模型是什么？

**结论**：基于 Reactor + Netty 事件循环，少量线程处理大量请求，阻塞操作会拖垮事件循环。

**机制**：
- 请求在 EventLoop 线程处理。
- Mono/Flux 表达异步流。
- 背压通过 request(n) 控制。
- 阻塞 IO 应切换到弹性线程池。

**追问**：WebFlux 一定比 MVC 快吗？高并发 IO 场景优势明显，简单同步业务未必。

### Q45. Spring 6/7 的演进方向有哪些？

**结论**：可观测性、ProblemDetail、HTTP Interface、AOT/Native、虚拟线程支持与 Jakarta 基线。

**机制**：
- Micrometer Observation 统一指标与追踪。
- RFC7807 ProblemDetail 标准化错误响应。
- `@HttpExchange` 声明式 HTTP 客户端。
- 对虚拟线程的适配与配置。
- 版本升级需关注 Jakarta 命名空间与依赖兼容。

**追问**：升级 Spring 6/7 要注意什么？Jakarta EE 迁移、弃用 API、第三方库兼容与测试覆盖。

---

## 六、进阶补充（Q46–Q50）

### Q46. Spring Cache 抽象如何工作？

**结论**：通过 AOP 拦截注解方法，按 CacheManager 读写缓存，支持 TTL、条件与同步。

**机制**：
- `@Cacheable` 命中直接返回，未命中执行方法并写入。
- `@CachePut` 更新缓存。
- `@CacheEvict` 删除缓存。
- 需要配置 CacheManager 与 KeyGenerator。

**追问**：缓存注解会失效吗？自调用、代理未生效、key 冲突都会导致失效。

### Q47. @Async 与 @Scheduled 的实现原理是什么？

**结论**：@Async 通过代理把方法提交到 TaskExecutor；@Scheduled 由调度器按 cron/固定频率触发。

**机制**：
- `@EnableAsync` 注册异步代理。
- 默认使用 SimpleAsyncTaskExecutor，生产应自定义线程池。
- `@EnableScheduling` 注册 ScheduledAnnotationBeanPostProcessor。
- 多实例定时任务需分布式锁防重复。

**追问**：@Async 会丢异常吗？会，需配置 AsyncUncaughtExceptionHandler 或返回 Future。

### Q48. Spring TestContext 框架有什么用？

**结论**：为测试提供容器上下文缓存、依赖注入、事务回滚与 MockMvc 等能力。

**机制**：
- `@SpringBootTest` 启动上下文。
- 上下文按配置缓存，减少重复启动。
- `@MockBean`/`@SpyBean` 替换 Bean。
- `@Transactional` 测试自动回滚。

**追问**：测试很慢怎么办？缩小上下文、使用切片测试、避免频繁 @MockBean 导致缓存失效。

### Q49. Spring Security OAuth2 Resource Server 如何校验 JWT？

**结论**：通过 Bearer Token 过滤器解析 JWT，使用公钥/Issuer 校验签名与声明，再转换为 Authentication。

**机制**：
- 配置 `JwtDecoder` 与 issuer-uri。
- 校验签名、过期、iss/aud。
- 权限映射为 GrantedAuthority。
- 可结合方法级授权。

**追问**：JWT 过期怎么处理？客户端刷新令牌，服务端拒绝过期 Token；基础见 [225 Q36–Q37](225_JavaSecurity与Crypto面试题库.md)。

### Q50. @ConfigurationProperties 如何绑定与校验？

**结论**：按前缀绑定配置到对象，支持嵌套、宽松绑定与 JSR-380 校验。

**机制**：
- `@ConfigurationProperties(prefix="app")`。
- 构造器/Setter 绑定。
- `@Validated` + 约束注解校验。
- 需注册或使用 `@EnableConfigurationProperties`。

**追问**：绑定失败会怎样？启动失败并提示字段错误，需检查类型与配置键。

---

## 七、进阶补充（Q51–Q55）

### Q51. Spring Batch 适合什么场景？

**结论**：面向批量任务的分片、事务、重试、跳过与监控框架，适合大数据量离线处理。

**机制**：
- Job/Step 定义批处理流程。
- Chunk 分片与事务边界。
- 失败重试/跳过策略。
- 元数据表记录执行状态。

**追问**：和定时任务区别？Batch 强调数据分片、重启与可观测，不只是一次调度。

### Q52. Spring Integration 是什么？

**结论**：基于消息通道的企业集成框架，用 Adapter/Gateway 连接文件、JMS、HTTP、数据库等。

**机制**：
- Message Channel 解耦生产/消费。
- Inbound/Outbound Adapter。
- 路由、过滤、转换。
- 适合系统间数据流转。

**追问**：和 MQ 区别？Integration 是集成编排层，可对接多种 MQ/协议；MQ 只是其中一种通道。

### Q53. Spring Data JPA 的工作原理是什么？

**结论**：通过 Repository 接口动态生成实现，把方法名/注解解析为查询，底层用 JPA EntityManager。

**机制**：
- Repository 代理生成 CRUD。
- 方法名派生查询。
- `@Query` 自定义 SQL/JPQL。
- 分页/排序与 Specification。

**追问**：JPA 性能问题在哪？懒加载、N+1、脏检查与大批量操作，需按场景优化。

### Q54. Spring WebSocket 如何工作？

**结论**：用 WebSocketHandler 处理连接与消息，配置 HandshakeInterceptor 与消息代理，支持广播。

**机制**：
- `@EnableWebSocket` 注册 Handler。
- Text/BinaryMessage 处理。
- STOMP 可用消息代理。
- 会话管理与心跳。

**追问**：集群下如何广播？用 Redis/MQ 做会话与消息共享；Netty WebSocket 见 [220 Q46](220_Netty面试题库.md)。

### Q55. Spring 事件机制与事务事件如何配合？

**结论**：ApplicationEventPublisher 发布事件，监听器默认同步；事务事件可在提交后执行。

**机制**：
- `@EventListener` 注册监听。
- `@TransactionalEventListener` 绑定阶段。
- 默认同步，异步需 @Async。
- 事务回滚时不执行 AFTER_COMMIT。

**追问**：监听器异常会影响主流程吗？同步监听会，需异步/隔离异常；事件机制见 [019](019_Spring扩展点源码解析.md)。

---

## 八、速记清单

| 主题 | 必背一句话 |
|---|---|
| IoC | 容器创建与注入，控制权反转 |
| 生命周期 | 实例化→填充→Aware→BPP→初始化→使用→销毁 |
| 循环依赖 | 三级缓存提前暴露，构造器循环无解 |
| AOP | 代理 + 拦截器链，自调用失效 |
| 事务 | 传播行为 + AOP 拦截 + ThreadLocal 绑定连接 |
| MVC | DispatcherServlet 11 步，Mapping/Adapter/Resolver |
| Boot | 自动配置 = 条件注解 + imports 注册 |
| 配置 | 命令行 > 环境变量 > 配置文件 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：55 题 · 详解**
