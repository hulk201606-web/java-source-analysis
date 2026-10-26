# 16 · Spring Boot 自动配置 + BeanPostProcessor + 事件机制 + 生命周期回调链

> **源码基线**：Spring Boot 2.7.18 + Spring Framework 5.3.35（Java 8 兼容版）  
> **面试权重**：⭐⭐⭐⭐⭐（Spring 框架高级知识点，大厂常问）

---

## 第一部分：Spring Boot 自动配置原理

### 1.1 自动配置核心链路

```java
@SpringBootApplication   // ← 包含 @EnableAutoConfiguration
public class MyApp {
    public static void main(String[] args) {
        SpringApplication.run(MyApp.class, args);
    }
}

// @EnableAutoConfiguration 做了什么？
//
// @EnableAutoConfiguration 内部：
//   @Import(AutoConfigurationImportSelector.class)  ← ★ 关键！
//
// AutoConfigurationImportSelector 是 DeferredImportSelector 的实现
// → 会在 ConfigurationClassPostProcessor 处理 @Configuration 类时被触发
// → selectImports() 方法被调用 → 扫描所有 META-INF/spring.factories 文件
// → 找出 key=org.springframework.boot.autoconfigure.EnableAutoConfiguration 的配置类列表
// → 过滤掉 exclude 指定的 → 用 @Conditional 评估条件 → 最终确定要加载哪些自动配置类
```

### 1.2 AutoConfigurationImportSelector.selectImports 源码

**源码锚点**：Spring Boot 2.7.18 `AutoConfigurationImportSelector.java` L97-125

```java
public String[] selectImports(AnnotationMetadata annotationMetadata) {
    // ① 没有 AutoConfigurationMetadata → 直接跳过
    if (!isEnabled(annotationMetadata)) {
        return NO_IMPORTS;
    }
    
    // ② 加载自动配置元数据（META-INF/spring-configuration-metadata.json）
    AutoConfigurationMetadata autoConfigurationMetadata = AutoConfigurationMetadataLoader.loadMetadata(this.beanClassLoader);
    
    // ★ ③ 核心！获取自动配置入口
    AutoConfigurationEntry autoConfigurationEntry = getAutoConfigurationEntry(autoConfigurationMetadata, annotationMetadata);
    
    // ④ 返回要导入的配置类名数组
    return StringUtils.toStringArray(autoConfigurationEntry.getConfigurations());
}

// ★★ 这个方法是自动配置的核心！
protected AutoConfigurationEntry getAutoConfigurationEntry(AutoConfigurationMetadata autoConfigurationMetadata,
        AnnotationMetadata annotationMetadata) {
    
    // ① 用 SpringFactoriesLoader 从 classpath 所有 jar 里找自动配置类
    //    扫描 META-INF/spring.factories → key=EnableAutoConfiguration → value=配置类列表
    List<String> configurations = getCandidateConfigurations(annotationMetadata, attributes);   // L125
    
    // ② 去重 + 排序
    configurations = removeDuplicates(configurations);
    
    // ③ 排除被 @SpringBootApplication(exclude=...) 或 @EnableAutoConfiguration(exclude=...) 指定的
    Set<String> exclusions = getExclusions(annotationMetadata, attributes);
    configurations.removeAll(exclusions);
    
    // ★ ④ 用 AutoConfigurationImportFilter 进一步过滤
    //    Spring Boot 内置了 OnClassCondition / OnBeanCondition / OnWebApplicationCondition
    //    检查条件是否满足（比如有没有某个类在 classpath 上）
    configurations = filter(configurations, autoConfigurationMetadata);   // ★ 条件过滤
    
    // ⑤ 触发 AutoConfigurationImportListener（扩展点）
    fireAutoConfigurationImportEvents(configurations, exclusions);
    
    return new AutoConfigurationEntry(configurations, exclusions);
}
```

### 1.3 getCandidateConfigurations · SpringFactoriesLoader

**源码锚点**：AutoConfigurationImportSelector.java L181-189

```java
protected List<String> getCandidateConfigurations(AnnotationMetadata metadata, AnnotationAttributes attributes) {
    // ★ SpringFactoriesLoader.loadFactoryNames 扫描所有 META-INF/spring.factories 文件
    //    每个 jar 包里可以有 META-INF/spring.factories
    //    文件内容：key=接口全限定名, value=逗号分隔的实现类列表
    //    自动配置的 key=org.springframework.boot.autoconfigure.EnableAutoConfiguration
    List<String> configurations = SpringFactoriesLoader.loadFactoryNames(
            getSpringFactoriesLoaderFactoryClass(),   // → EnableAutoConfiguration.class
            getBeanClassLoader());
    Assert.notEmpty(configurations, "...");
    return configurations;
}

// Spring Boot 2.7.18 的 spring-boot-autoconfigure.jar 里：
// META-INF/spring.factories 文件内容节选：
//
// org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
//   org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration,\
//   org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,\
//   org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,\
//   org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration,\
//   ...（几百个自动配置类）
```

### 1.4 @Conditional 族 · 条件评估

> **条件评估是自动配置的灵魂！** 几百个自动配置类不会全部加载 → 根据条件筛选。

```java
// @Conditional 注解族（核心 6 个）：
//
// ① @ConditionalOnClass
//    classpath 上有指定类才生效
//    → 实现类 OnClassCondition
//    → 例：@ConditionalOnClass({ DataSource.class, JdbcTemplate.class })
//
// ② @ConditionalOnMissingBean
//    容器里没有指定 Bean 才生效（允许用户覆盖！）
//    → 实现类 OnBeanCondition
//    → 例：@ConditionalOnMissingBean(DataSource.class)
//
// ③ @ConditionalOnBean
//    容器里有指定 Bean 才生效
//    → 实现类 OnBeanCondition
//
// ④ @ConditionalOnProperty
//    配置文件里有指定属性且匹配才生效
//    → 例：@ConditionalOnProperty(prefix="spring.datasource", name="url")
//
// ⑤ @ConditionalOnWebApplication
//    当前是 Web 应用才生效（SERVLET / REACTIVE / ANY）
//    → 实现类 OnWebApplicationCondition
//
// ⑥ @ConditionalOnMissingClass
//    classpath 上没有指定类才生效（和 OnClass 相反）
//
// 条件评估流程：
//   ConditionEvaluator.shouldSkip()
//     → 找出类上所有 @Conditional 注解
//     → 拿到每个 Condition 的实现类（OnClassCondition / OnBeanCondition 等）
//     → 逐个评估条件 → 如果有一个不匹配 → 跳过这个自动配置类
```

### 1.5 @ConditionalOnClass 评估原理

```java
// OnClassCondition.getMatchOutcome() 简化逻辑：
//
// ① 找到 @ConditionalOnClass 的 value 数组（类名列表）
// ② 遍历每个类名 → ClassUtils.isPresent(className, classLoader)
//    → 尝试 Class.forName(className) → 成功返回 true，ClassNotFoundException 返回 false
// ③ 所有类都 present → match → 返回 Match
// ④ 有一个类 missing → 不匹配 → 返回 NoMatch
//
// 实战示例（DataSourceAutoConfiguration）：
// @ConditionalOnClass({ DataSource.class, EmbeddedDatabaseType.class })
// → 只有当 classpath 上有 DataSource 和 EmbeddedDatabaseType 时才加载
// → 如果用户没有引入 spring-jdbc 依赖 → 不会配置 DataSource！
```

### 1.6 自定义 Spring Boot Starter

```java
// 写一个自定义 starter 只需要 3 步：
//
// Step 1：写自动配置类
@Configuration
@ConditionalOnClass(MyService.class)                    // 有 MyService 类才生效
@ConditionalOnMissingBean(MyService.class)               // 容器里没有才注册（允许用户覆盖）
@EnableConfigurationProperties(MyProperties.class)       // 绑定配置文件
public class MyAutoConfiguration {

    @Bean
    public MyService myService(MyProperties props) {
        return new MyService(props.getUrl(), props.getTimeout());
    }
}

@ConfigurationProperties(prefix = "my.service")   // application.yml: my.service.url=xxx
public class MyProperties {
    private String url;
    private int timeout = 3000;
    // getter / setter
}

// Step 2：在 META-INF/spring.factories 里注册
// org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
//   com.example.mystarter.MyAutoConfiguration

// Step 3：（可选）META-INF/spring-configuration-metadata.json
// → 给 IDE 提供配置提示

// Spring Boot 2.7+ 推荐用 AutoConfiguration.imports 文件替代 spring.factories
// META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
// 文件内容：每行一个自动配置类全限定名
```

---

## 第二部分：Spring BeanPostProcessor 体系

### 2.1 BeanPostProcessor 核心接口

**源码锚点**：Spring Framework 5.3.35 `BeanPostProcessor.java`

```java
public interface BeanPostProcessor {

    // ★ 前置：Bean 初始化前调用（populateBean 之后，initializeBean 之前）
    // 返回值会替换原 Bean（可以返回代理对象！）
    default Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    // ★ 后置：Bean 初始化后调用（initializeBean 之后）
    // 返回值会替换原 Bean（AOP 代理就是在这里创建的！）
    default Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}

// 调用时机（Bean 创建完整生命周期中的位置）：
//
// doCreateBean:
//   ├─ createBeanInstance()    ← 实例化（反射 / CGLIB）
//   ├─ populateBean()          ← 属性注入（@Autowired / @Value）
//   │    └─ applyPropertyValues()
//   │
//   ├─ initializeBean()        ← 初始化
//   │    │
//   │    ├─ invokeAwareMethods()                              ← Aware 回调
//   │    │    → BeanNameAware / BeanClassLoaderAware / BeanFactoryAware
//   │    │
//   │    ├─ ★ applyBeanPostProcessorsBeforeInitialization()   ← postProcessBeforeInitialization
//   │    │    → @PostConstruct 在这里执行！（CommonAnnotationBeanPostProcessor）
//   │    │
//   │    ├─ invokeInitMethods()
//   │    │    → InitializingBean.afterPropertiesSet()
//   │    │    → init-method 属性指定的方法
//   │    │
//   │    └─ ★ applyBeanPostProcessorsAfterInitialization()    ← postProcessAfterInitialization
//   │         → AOP 代理在这里创建！（AnnotationAwareAspectJAutoProxyCreator）
//   │
//   └─ 注册到 singletonObjects（一级缓存）
```

### 2.2 Spring 内置 BeanPostProcessor 全清单

| BeanPostProcessor | 实现类 | 核心作用 | 注册时机 |
|-------------------|--------|---------|---------|
| **AutowiredAnnotationBeanPostProcessor** | Spring 内置 | 处理 `@Autowired` / `@Value` 注入 | 默认注册 |
| **CommonAnnotationBeanPostProcessor** | Spring 内置 | 处理 `@Resource` / `@PostConstruct` / `@PreDestroy` | 默认注册（需 javax.annotation） |
| **AnnotationAwareAspectJAutoProxyCreator** | Spring AOP | 创建 AOP 代理（postProcessAfterInitialization）| 开启 AOP 时注册 |
| **TransactionInterceptor** | Spring 事务 | `@Transactional` 代理的 MethodInterceptor | 开启事务时注册 |
| **ApplicationContextAwareProcessor** | Spring 内置 | 处理 `ApplicationContextAware` / `EnvironmentAware` / `ResourceLoaderAware` | 默认注册 |
| **PostProcessorRegistrationDelegate** 特殊 | | BeanFactoryPostProcessor 实现，注册所有 BeanPostProcessor | 容器启动时注册 |

### 2.3 BeanPostProcessor 执行顺序

```java
// AbstractApplicationContext.registerBeanPostProcessors() 源码：
//
// ★ 关键：BeanPostProcessor 分三批注册 + 执行！
//
// 第一批：实现 PriorityOrdered 接口的 BeanPostProcessor
//   → 排序注册到 BeanFactory
//
// 第二批：实现 Ordered 接口的（但不是 PriorityOrdered）
//   → 排序注册
//
// 第三批：没有 Ordered 接口的
//   → 按 BeanDefinition 注册顺序
//
// ★ 同一个 Bean 创建时：
//   postProcessBeforeInitialization → 按上述顺序依次执行
//   postProcessAfterInitialization  → 按上述顺序依次执行
//
// ★ AOP 代理创建时机：
//   AnnotationAwareAspectJAutoProxyCreator 是 Ordered.HIGHEST_PRECEDENCE
//   → 通常是第一个 postProcessAfterInitialization 执行
//   → 它返回代理对象 → 后面的 BeanPostProcessor 接收到的是代理对象
```

### 2.4 自定义 BeanPostProcessor

```java
// 示例：给所有 Bean 打日志
@Component
public class LoggingBeanPostProcessor implements BeanPostProcessor, Ordered {

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        System.out.println("[BeforeInit] " + beanName + " → " + bean.getClass().getSimpleName());
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        System.out.println("[AfterInit] " + beanName);
        return bean;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;  // 最后执行
    }
}

// 输出：
// [BeforeInit] dataSource → HikariDataSource
// [BeforeInit] myService → MyService
// [AfterInit] myService
// [AfterInit] dataSource
```

---

## 第三部分：Spring 事件机制

### 3.1 事件机制核心架构

```java
// 事件机制三要素：
//
// ① ApplicationEvent（事件）：继承 ApplicationEvent 的类
//    新版本用 ApplicationEvent<T> 泛型（Spring 4.2+）
//
// ② ApplicationListener（监听器）：实现 ApplicationListener<E extends ApplicationEvent>
//    重写 onApplicationEvent(E event) 方法
//    或用 @EventListener 注解标记方法（Spring 4.2+）
//
// ③ ApplicationEventMulticaster（事件广播器）：
//    SimpleApplicationEventMulticaster（默认实现）
//    → 管理 ApplicationListener 列表
//    → 广播事件给所有匹配的监听器
//    → 支持异步执行（设置 taskExecutor）

// 核心流程：
//
// 发布事件：applicationEventMulticaster.multicastEvent(event)
//   │
//   ├─ ① 找出所有 ApplicationListener
//   │
//   ├─ ② 遍历监听器 → 判断是否支持这个事件类型
//   │    (ClassUtils.isAssignableValue(listenerType, event))
//   │
//   ├─ ③ 如果有 taskExecutor（异步）
//   │    executor.execute(() -> listener.onApplicationEvent(event))
//   │    ← 非阻塞，调用线程立即返回
//   │
//   └─ ④ 如果没有 taskExecutor（同步，默认）
//        listener.onApplicationEvent(event)
//        ← 阻塞，监听器执行完才返回
```

### 3.2 SimpleApplicationEventMulticaster.multicastEvent 源码

**源码锚点**：SimpleApplicationEventMulticaster.java L137-155

```java
public void multicastEvent(final ApplicationEvent event, @Nullable ResolvableType eventType) {
    ResolvableType type = (eventType != null ? eventType : resolveDefaultEventType(event));
    
    // ★ 异步还是同步？看有没有 taskExecutor
    Executor executor = getTaskExecutor();
    for (ApplicationListener<?> listener : getApplicationListeners(event, type)) {
        if (executor != null) {
            // ★ 异步：提交到线程池执行
            executor.execute(() -> invokeListener(listener, event));
        }
        else {
            // ★ 同步（默认）：当前线程直接执行
            invokeListener(listener, event);
        }
    }
}

// invokeListener：异常隔离
protected void invokeListener(ApplicationListener<?> listener, ApplicationEvent event) {
    ErrorHandler errorHandler = getErrorHandler();
    if (errorHandler != null) {
        try {
            doInvokeListener(listener, event);
        } catch (Throwable err) {
            errorHandler.handleError(err);   // ★ 监听器异常不会影响其他监听器！
        }
    }
    else {
        doInvokeListener(listener, event);   // 没有 errorHandler → 异常直接抛出
    }
}
```

### 3.3 异步事件 + @Async

```java
// ① 配置 SimpleApplicationEventMulticaster 异步
@Configuration
public class EventConfig {
    @Bean
    public ApplicationEventMulticaster applicationEventMulticaster() {
        SimpleApplicationEventMulticaster multicaster = new SimpleApplicationEventMulticaster();
        multicaster.setTaskExecutor(new ThreadPoolTaskExecutor());  // ★ 设置异步线程池
        multicaster.setErrorHandler(t -> log.error("事件处理异常", t));
        return multicaster;
    }
}

// ② 监听器用 @Async 注解（Spring 4.2+，更灵活）
@Component
public class NotificationListener {

    // ★ @Async 让这个监听器在异步线程执行（和 multicaster 级别异步二选一）
    @Async
    @EventListener
    public void onUserRegistered(UserRegisteredEvent event) {
        // 发送欢迎邮件 → 慢操作，异步执行
        emailService.sendWelcomeEmail(event.getUserId());
    }

    // ★ 还能监听条件（只有 id > 1000 才处理）
    @EventListener(condition = "#event.userId > 1000")
    public void onVipUserRegistered(UserRegisteredEvent event) {
        vipService.handleNewVip(event.getUserId());
    }
}

// ③ 开启 @Async 支持
@EnableAsync  // ← 加在启动类或配置类上
```

### 3.4 Spring 内置事件

| 事件 | 触发时机 | 典型用途 |
|------|---------|---------|
| ApplicationContextEvent | | |
| → ContextRefreshedEvent | AbstractApplicationContext.finishRefresh() | 启动完成后初始化资源 |
| → ContextClosedEvent | AbstractApplicationContext.close() | 关闭时释放资源 |
| ApplicationEnvironmentPreparedEvent | SpringApplication.prepareEnvironment() | Environment 准备好时修改配置 |
| ApplicationStartedEvent | SpringApplication.callRunners() 后 | ApplicationRunner / CommandLineRunner 执行完 |
| ApplicationReadyEvent | SpringApplication.run() 完成 | 应用完全就绪（对外提供服务）|
| ApplicationFailedEvent | SpringApplication.run() 异常 | 启动失败时告警 |

### 3.5 事件 vs 直接调用

| 维度 | 事件机制 | 直接调用 |
|------|---------|---------|
| 耦合度 | ✅ 低（发布者不知道订阅者）| ❌ 高（直接依赖）|
| 性能 | 同步快；异步慢（线程切换）| 最快 |
| 可靠性 | 监听器异常隔离 | try-catch 控制 |
| 顺序 | 不保证顺序 | 顺序可控 |
| 适用场景 | 通知类（邮件、短信、日志）| 必须立即执行的核心业务 |

---

## 第四部分：Spring Bean 生命周期完整回调链

> **面试最高频！** Bean 从创建到销毁经过的完整回调序列。

### 4.1 完整生命周期图

```
BeanDefinition 加载
  │
  ├─ ① BeanFactoryPostProcessor.postProcessBeanFactory()
  │    → 修改 BeanDefinition（加属性、改 scope 等）
  │    → ★ ConfigurationClassPostProcessor（@Configuration 类解析在这里）
  │
  ├─ ② BeanFactoryPostProcessor.postProcessBeanDefinitionRegistry()
  │    → 注册额外的 BeanDefinition
  │
  ├─ ③ finishBeanFactoryInitialization → 开始实例化单例
  │
  ├─ ④ BeanPostProcessor.postProcessBeforeInstantiation()   ← 很少用（几乎没人实现）
  │
  ├─ ⑤ ★ 实例化（createBeanInstance）
  │    → Supplier / FactoryMethod / 构造函数反射
  │
  ├─ ⑥ ★ 提前暴露 singletonFactories（循环依赖三级缓存）
  │    → addSingletonFactory(beanName, ObjectFactory)
  │
  ├─ ⑦ ★ 属性注入（populateBean）
  │    → InstantiationAwareBeanPostProcessor.postProcessProperties（@Autowired 在这里）
  │    → applyPropertyValues
  │
  ├─ ⑧ ★ 初始化（initializeBean）
  │    │
  │    ├─ ⑧-a invokeAwareMethods()
  │    │    → BeanNameAware.setBeanName()
  │    │    → BeanClassLoaderAware.setBeanClassLoader()
  │    │    → BeanFactoryAware.setBeanFactory()
  │    │
  │    ├─ ⑧-b ★ applyBeanPostProcessorsBeforeInitialization()
  │    │    → @PostConstruct 在这里执行！
  │    │    → CommonAnnotationBeanPostProcessor.postProcessBeforeInitialization()
  │    │
  │    ├─ ⑧-c invokeInitMethods()
  │    │    → InitializingBean.afterPropertiesSet()
  │    │    → init-method 属性指定的自定义方法
  │    │
  │    └─ ⑧-d ★ applyBeanPostProcessorsAfterInitialization()
  │         → ★★★ AOP 代理在这里创建！
  │         → AnnotationAwareAspectJAutoProxyCreator.postProcessAfterInitialization()
  │
  ├─ ⑨ 注册到 singletonObjects（一级缓存）
  │
  ├─ ⑩ ★ 销毁阶段（context.close() / JVM shutdown hook）
  │     │
  │     ├─ ⑩-a @PreDestroy 在这里执行！
  │     │    → CommonAnnotationBeanPostProcessor 调用
  │     │
  │     ├─ ⑩-b DisposableBean.destroy()
  │     │
  │     └─ ⑩-c destroy-method 属性指定的自定义方法
  │
  └─ 结束（Bean 被 GC）
```

### 4.2 生命周期回调速查表

| 阶段 | 回调 | 类型 | 注解/接口 | 源码入口 |
|------|------|------|----------|---------|
| **BeanDefinition 处理** | | | | |
| 1 | postProcessBeanDefinitionRegistry | BeanFactoryPostProcessor | BeanFactoryPostProcessor 接口 | AbstractApplicationContext.invokeBeanFactoryPostProcessors() |
| 2 | postProcessBeanFactory | BeanFactoryPostProcessor | BeanFactoryPostProcessor 接口 | 同上 |
| **实例化前** | | | | |
| 3 | postProcessBeforeInstantiation | BeanPostProcessor | BeanPostProcessor 接口 | AbstractAutowireCapableBeanFactory.createBean() |
| **实例化** | | | | |
| 4 | createBeanInstance | Spring 内部 | - | doCreateBean() |
| **属性注入** | | | | |
| 5 | @Autowired / @Value | 注解 | @Autowired / @Value | populateBean() → AutowiredAnnotationBeanPostProcessor |
| 6 | @Resource | 注解 | @Resource | populateBean() → CommonAnnotationBeanPostProcessor |
| 7 | postProcessProperties | BeanPostProcessor | InstantiationAwareBeanPostProcessor | populateBean() |
| **初始化** | | | | |
| 8 | BeanNameAware | Aware | BeanNameAware 接口 | invokeAwareMethods() |
| 9 | BeanFactoryAware | Aware | BeanFactoryAware 接口 | invokeAwareMethods() |
| 10 | ApplicationContextAware | Aware | ApplicationContextAware 接口 | ApplicationContextAwareProcessor |
| 11 | @PostConstruct | **初始化前** | @PostConstruct 注解 | applyBeanPostProcessorsBeforeInitialization() |
| 12 | InitializingBean | **初始化中** | InitializingBean 接口 | invokeInitMethods() |
| 13 | init-method | **初始化中** | XML / @Bean(initMethod="") | invokeInitMethods() |
| 14 | postProcessAfterInitialization | BeanPostProcessor | BeanPostProcessor 接口 | ★★ AOP 代理在这里创建！ |
| **销毁** | | | | |
| 15 | @PreDestroy | **销毁前** | @PreDestroy 注解 | DefaultSingletonBeanRegistry.destroySingleton() |
| 16 | DisposableBean | 销毁中 | DisposableBean 接口 | 同上 |
| 17 | destroy-method | 销毁中 | XML / @Bean(destroyMethod="") | 同上 |

### 4.3 面试高频问答

```java
// ★ 1. @PostConstruct 和 InitializingBean.afterPropertiesSet() 和 init-method 执行顺序？
//
//   @PostConstruct（⑧-b 初始化前）
//     → InitializingBean.afterPropertiesSet()（⑧-c 初始化中）
//     → init-method（⑧-c 初始化中）
//
//   销毁顺序类似：
//   @PreDestroy（⑩-a 销毁前）
//     → DisposableBean.destroy()（⑩-b 销毁中）
//     → destroy-method（⑩-c 销毁中）

// ★ 2. @PostConstruct 和 @PreDestroy 是怎么被调用的？
//
//   CommonAnnotationBeanPostProcessor 是 BeanPostProcessor 实现
//   → postProcessBeforeInitialization() 里检查 @PostConstruct
//   → postProcessBeforeDestruction() 里检查 @PreDestroy
//   → 反射调用标注的方法

// ★ 3. Aware 回调为什么不是 BeanPostProcessor？
//
//   invokeAwareMethods() 是 initializeBean() 内部单独调用的
//   → 在 applyBeanPostProcessorsBeforeInitialization 之前
//   → 所以 @PostConstruct 里能拿到注入的 BeanFactory / ApplicationContext
//   → 但 Aware 在 @Autowired 之后还是之前？
//     → Aware 在 initializeBean 最开始调用
//     → @Autowired 在 populateBean（initializeBean 之前）
//     → 所以 Aware 和 @Autowired 都在 @PostConstruct 之前

// ★ 4. AOP 代理创建在哪个阶段？
//
//   postProcessAfterInitialization()（⑧-d 初始化后）
//   → AnnotationAwareAspectJAutoProxyCreator 返回代理对象
//   → 注册到 singletonObjects 的是代理对象（不是原始对象）
```

---

## 第五部分：面试考点（Spring 扩展点 20 题）

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **自动配置** | | |
| 1 | Spring Boot 自动配置核心？| **@EnableAutoConfiguration → AutoConfigurationImportSelector → SpringFactoriesLoader 扫描 META-INF/spring.factories → @Conditional 评估条件** |
| 2 | spring.factories key？| **org.springframework.boot.autoconfigure.EnableAutoConfiguration** |
| 3 | 6 种 @Conditional 族？| @ConditionalOnClass（有类才加载）/ OnMissingBean（无 Bean 才加载）/ OnBean（有 Bean 才加载）/ OnProperty / OnWebApplication / OnMissingClass |
| 4 | @ConditionalOnMissingBean 作用？| **容器里没有这个 Bean 才注册 → 允许用户覆盖自动配置！** |
| 5 | 自定义 Starter 3 步？| ① 写 @Configuration + @Conditional 自动配置类 ② META-INF/spring.factories 或 AutoConfiguration.imports 注册 ③ 可选 spring-configuration-metadata.json 给 IDE 提示 |
| **BeanPostProcessor** | | |
| 6 | BeanPostProcessor 两种方法？| **postProcessBeforeInitialization（初始化前）/ postProcessAfterInitialization（初始化后）** → 返回值替换原 Bean |
| 7 | BeanPostProcessor 执行顺序？| **PriorityOrdered → Ordered → 无顺序**，同一个 Bean 的 before/after 按这个顺序依次执行 |
| 8 | AOP 代理在哪个 BeanPostProcessor 里创建？| **AnnotationAwareAspectJAutoProxyCreator.postProcessAfterInitialization()**（Spring AOP 核心）|
| 9 | @Autowired 怎么实现的？| **AutowiredAnnotationBeanPostProcessor.postProcessProperties()**（InstantiationAwareBeanPostProcessor 实现）→ 在 populateBean 阶段 |
| **事件机制** | | |
| 10 | Spring 事件三要素？| **ApplicationEvent + ApplicationListener + ApplicationEventMulticaster** |
| 11 | SimpleApplicationEventMulticaster 默认同步还是异步？| **默认同步**（taskExecutor=null）→ 当前线程直接执行监听器；设置 taskExecutor 后异步 |
| 12 | 事件监听器异常会影响其他监听器吗？| **默认不会！** invokeListener 有 ErrorHandler 包裹（SimpleApplicationEventMulticaster 源码） |
| 13 | @EventListener 条件监听？| **@EventListener(condition = "#event.field > 100")** → SpEL 表达式条件 |
| **生命周期** | | |
| 14 | @PostConstruct 在生命周期哪个位置？| **initializeBean 里 applyBeanPostProcessorsBeforeInitialization()**（Aware 回调之后，InitializingBean 之前）|
| 15 | Bean 初始化四步？| **invokeAwareMethods → @PostConstruct → InitializingBean.afterPropertiesSet → init-method → postProcessAfterInitialization（AOP 代理创建）** |
| 16 | Bean 销毁三步？| **@PreDestroy → DisposableBean.destroy → destroy-method** |
| 17 | Aware 回调 vs @Autowired 注入顺序？| **@Autowired 在 populateBean（initializeBean 之前）**；**Aware 在 invokeAwareMethods（initializeBean 最开始）**；→ @Autowired 先完成，然后 Aware |
| 18 | AOP 代理注册到 singletonObjects 里的是代理还是原始对象？| **代理对象！** postProcessAfterInitialization 返回代理 → 注册的是代理 → 后续所有 getBean 拿到的都是代理 |
| 19 | @Bean(initMethod="xxx") vs @PostConstruct vs InitializingBean 顺序？| **@PostConstruct（初始化前）→ InitializingBean.afterPropertiesSet（初始化中）→ @Bean initMethod（初始化中）** |
| 20 | 生命周期里循环依赖在哪一步解决？| **populateBean 之前！** 提前暴露 singletonFactories（第三级缓存）→ 被循环依赖引用时 ObjectFactory.getObject() 返回早期引用 |

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线**：Spring Boot 2.7.18 + Spring Framework 5.3.35（Java 8 兼容版）  
**覆盖**：Spring Boot 自动配置完整流程（@EnableAutoConfiguration → AutoConfigurationImportSelector → SpringFactoriesLoader → @Conditional 族评估）+ BeanPostProcessor 体系（核心接口 + 内置实现清单 + 执行顺序 + AOP/@Autowired 实现）+ Spring 事件机制（三要素 + SimpleApplicationEventMulticaster 同步/异步 + @EventListener + @Async）+ Bean 生命周期完整回调链（BeanFactoryPostProcessor → 实例化 → 提前暴露 → 属性注入 → Aware → @PostConstruct → InitializingBean → init-method → AOP 代理创建 → @PreDestroy → DisposableBean → destroy-method）+ 20 个面试考点  
**源码锚点行号全部来自 Spring Boot 2.7.18 / Spring Framework 5.3.35 源码副本**
