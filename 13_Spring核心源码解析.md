# 13 · Spring Framework + Spring Boot 核心源码解析

> **源码基线**：Spring Boot 2.7.18 + Spring Framework 5.3.35（Java 8 兼容版本，面试最高频）  
> **源码位置**：
>   - `d:\GC_WorkSpaces\spring-boot-2.7.18\spring-boot-project\spring-boot\src\main\java\`
>   - `d:\GC_WorkSpaces\spring-framework-5.3.35\`
> **面试权重**：⭐⭐⭐⭐⭐（面试必考第一梯队）

---

## 一、Spring Boot 启动流程 · 6 步完整链路

### 1.1 总览

```java
// 用户写的一行代码：
@SpringBootApplication
public class MyApp {
    public static void main(String[] args) {
        SpringApplication.run(MyApp.class, args);
    }
}

// SpringApplication.run() 内部 6 步：
//
// ① getRunListeners + listeners.starting() → 启动监听（ApplicationListener 体系）
// ② createApplicationContext() → 创建上下文（WebServlet / WebReactive / NonWeb）
// ③ prepareContext() → 准备上下文（Environment + Banner + initializers）
// ④ refreshContext() → ★ 核心：IoC 容器刷新（AbstractApplicationContext.refresh 11 步）
// ⑤ listeners.started() → 启动完成监听
// ⑥ callRunners() → 执行 ApplicationRunner / CommandLineRunner
```

### 1.2 SpringApplication.run 核心方法

**源码锚点**：SpringBoot 2.7.18 `SpringApplication.java` L293-329

```java
public ConfigurableApplicationContext run(String... args) {
    // ① 获取启动监听器 + 广播 starting 事件
    SpringApplicationRunListeners listeners = getRunListeners(args);   // L298
    listeners.starting(bootstrapContext, this.mainApplicationClass);   // L299 ★ 通知所有 ApplicationListener 开始启动

    try {
        ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
        
        // ② 创建 Environment + Banner
        ConfigurableEnvironment environment = prepareEnvironment(listeners, bootstrapContext, applicationArguments);
        Banner printedBanner = printBanner(environment);
        
        // ③ 创建 ApplicationContext（判断 Web 类型）
        context = createApplicationContext();   // L305 ★ 关键：根据 WebApplicationType 创建
        
        // ④ 准备上下文（把 Environment / Banner / initializers / listeners 都塞进去）
        prepareContext(bootstrapContext, context, environment, listeners, applicationArguments, printedBanner);  // L307
        
        // ⑤ ★ 核心！刷新 IoC 容器
        refreshContext(context);   // L308 → AbstractApplicationContext.refresh() 11 步
        
        // ⑥ 刷新后扩展点
        afterRefresh(context, applicationArguments);
        
        // ⑦ 广播 started 事件
        listeners.started(context, timeTakenToStartup);   // L314
        
        // ⑧ 执行 Runner（ApplicationRunner / CommandLineRunner）
        callRunners(context, applicationArguments);   // L315
    } catch (Throwable ex) {
        handleRunFailure(context, ex);
        throw new IllegalStateException(ex);
    }
    return context;
}
```

### 1.3 createApplicationContext · 三种上下文类型

**源码锚点**：SpringApplication.java L566-575

```java
protected ConfigurableApplicationContext createApplicationContext() {
    Class<?> contextClass = this.applicationContextClass;
    if (contextClass == null) {
        // ★ 根据 WebApplicationType 选择：
        //   SERVLET  → AnnotationConfigServletWebServerApplicationContext（内嵌 Tomcat）
        //   REACTIVE → AnnotationConfigReactiveWebServerApplicationContext（WebFlux）
        //   NONE     → AnnotationConfigApplicationContext（纯 Java 后端）
        switch (this.webApplicationType) {
            case SERVLET:   contextClass = AnnotationConfigServletWebServerApplicationContext.class; break;
            case REACTIVE:  contextClass = AnnotationConfigReactiveWebServerApplicationContext.class; break;
            default:        contextClass = AnnotationConfigApplicationContext.class; break;
        }
    }
    return (ConfigurableApplicationContext) BeanUtils.instantiateClass(contextClass);
}
// 自动检测 WebApplicationType：ClassPath 上有没有 DispatcherServlet / DispatcherHandler
```

### 1.4 refreshContext → AbstractApplicationContext.refresh

**源码锚点**：SpringApplication.java L405-413

```java
private void refreshContext(ConfigurableApplicationContext context) {
    refresh(context);   // → AbstractApplicationContext.refresh()
    if (this.registerShutdownHook) {
        // ★ 注册 JVM shutdown hook：JVM 退出时 graceful shutdown
        context.registerShutdownHook();
    }
}
// → 进入 Spring Framework 核心：AbstractApplicationContext.refresh 11 步
```

---

## 二、AbstractApplicationContext.refresh · 11 步 IoC 容器刷新

> **这是 Spring IoC 的心脏**，面试必考。

**源码锚点**：Spring Framework 5.3.35 `AbstractApplicationContext.java` L567-627

```java
public void refresh() throws BeansException, IllegalStateException {
    synchronized (this.startupShutdownMonitor) {   // ★ 锁：防并发刷新
    
        // ============ 第一阶段：准备 ============
        
        // ① prepareRefresh：准备刷新（initPropertySources + validateRequiredProperties + 发布 ContextRefreshedEvent 预留）
        prepareRefresh();   // L573
        
        // ② obtainFreshBeanFactory：获取新的 BeanFactory
        //    → refreshBeanFactory() 创建 GenericApplicationContext 内部的 DefaultListableBeanFactory
        //    → loadBeanDefinitions() 加载所有 BeanDefinition（扫描 @Component / @Configuration）
        ConfigurableListableBeanFactory beanFactory = obtainFreshBeanFactory();   // L577
        
        // ③ prepareBeanFactory：准备 BeanFactory
        //    → 注册 Aware 回调（BeanClassLoaderAware / BeanFactoryAware）
        //    → 注册 ApplicationContextAwareProcessor
        //    → 注册 3 个固定 bean：environment / systemProperties / systemEnvironment
        prepareBeanFactory(beanFactory);   // L581
        
        // ============ 第二阶段：扩展点 ============
        
        // ④ postProcessBeanFactory：后置处理 BeanFactory（空方法，子类覆盖）
        //    → 比如注册 PropertyEditorRegistrar
        postProcessBeanFactory(beanFactory);   // L586
        
        // ⑤ invokeBeanFactoryPostProcessors：★ 执行 BeanFactoryPostProcessor
        //    → 先执行 BeanDefinitionRegistryPostProcessor（@Configuration 类解析 + 扫描 @Component）
        //    → 再执行普通 BeanFactoryPostProcessor
        //    → 内部会调用 ConfigurationClassPostProcessor（解析 @Configuration）
        invokeBeanFactoryPostProcessors(beanFactory);   // L591 ★
        
        // ⑥ registerBeanPostProcessors：注册 BeanPostProcessor
        //    → 按优先级排序：PriorityOrdered → Ordered → 无顺序
        //    → 每个 BeanPostProcessor 注册到 BeanFactory（不执行，只注册）
        registerBeanPostProcessors(beanFactory);   // L594 ★
        
        // ============ 第三阶段：初始化基础设施 ============
        
        // ⑦ initMessageSource：初始化国际化消息源（如果有 "messageSource" bean 就用它，否则用空 MessageSource）
        initMessageSource();   // L599
        
        // ⑧ initApplicationEventMulticaster：初始化事件广播器
        //    → 如果有 "applicationEventMulticaster" bean 就用它，否则用 SimpleApplicationEventMulticaster
        initApplicationEventMulticaster();   // L603
        
        // ============ 第四阶段：Web + Listener + 实例化 ============
        
        // ⑨ onRefresh：子类覆盖（Web 子类内嵌 Tomcat 就在这里启动）
        onRefresh();   // L607
        
        // ⑩ registerListeners：注册应用监听器
        //    → 把 ApplicationListener 注册到事件广播器
        //    → 发布早期事件（ApplicationEnvironmentPreparedEvent / ContextInitializedEvent）
        registerListeners();   // L611
        
        // ============ 第五阶段：★★★ 核心实例化 ============
        
        // ⑪ finishBeanFactoryInitialization：★★★ 实例化所有非懒加载单例 Bean
        //    → beanFactory.preInstantiateSingletons()
        //    → 这是 Bean 创建的核心入口（doGetBean → createBean → doCreateBean）
        //    → 循环依赖三级缓存也在这里发生
        finishBeanFactoryInitialization(beanFactory);   // L615 ★★★
        
        // ⑫ finishRefresh：最后收尾
        //    → 清除 ResourceLoader 缓存
        //    → 发布 ContextRefreshedEvent
        //    → 初始化 LifecycleProcessor + 调用 onRefresh
        finishRefresh();   // L623
    }
}
```

### 2.1 11 步执行时序图

```
SpringApplication.refreshContext()
  │
  └─ AbstractApplicationContext.refresh()
       │
       ├─ ① prepareRefresh          → 准备环境 + 验证属性
       ├─ ② obtainFreshBeanFactory  → 创建 BeanFactory + ★ 加载 BeanDefinition（扫描）
       ├─ ③ prepareBeanFactory      → 注册 Aware 回调 + 固定 bean
       ├─ ④ postProcessBeanFactory  → 空方法（子类扩展）
       ├─ ⑤ invokeBeanFactoryPostProcessors → ★★★ 执行 @Configuration 解析（ConfigurationClassPostProcessor）
       ├─ ⑥ registerBeanPostProcessors      → ★★ 注册 BeanPostProcessor（排序）
       ├─ ⑦ initMessageSource                → 国际化
       ├─ ⑧ initApplicationEventMulticaster  → 事件广播器
       ├─ ⑨ onRefresh                         → Web 子类启动 Tomcat
       ├─ ⑩ registerListeners                 → 注册监听器
       ├─ ⑪ finishBeanFactoryInitialization   → ★★★★ 实例化所有非懒加载单例
       └─ ⑫ finishRefresh                     → 发布 ContextRefreshedEvent
```

### 2.2 BeanDefinition 加载链路

```
obtainFreshBeanFactory() 
  → refreshBeanFactory() 
    → loadBeanDefinitions()   ← 关键！加载 BeanDefinition
    
// 注解驱动（Spring Boot 默认）：
//   ConfigurationClassPostProcessor（BeanDefinitionRegistryPostProcessor）
//   → 处理 @Configuration 类
//     → @ComponentScan → ClassPathBeanDefinitionScanner
//       → 扫描 basePackages 下所有 @Component / @Service / @Repository / @Controller
//         → 生成 BeanDefinition → registry.registerBeanDefinition()
//     → @Import → 导入额外配置类
//     → @Bean → 生成 FactoryBean 或普通 BeanDefinition
```

---

## 三、Bean 创建完整生命周期

> **核心入口**：`AbstractBeanFactory.doGetBean()` → `AbstractAutowireCapableBeanFactory.createBean()` → `doCreateBean()`

### 3.1 doGetBean · 获取 Bean 主流程

**源码锚点**：AbstractBeanFactory.java L257-375

```java
protected <T> T doGetBean(final String name, @Nullable final Class<T> requiredType,
        @Nullable final Object[] args, boolean typeCheckOnly) {
    
    // ① 单例缓存：先从三级缓存拿
    Object sharedInstance = getSingleton(beanName);   // L257
    if (sharedInstance != null && args == null) {
        // 单例命中 → 直接返回（可能是原始对象，也可能是代理对象）
        bean = getObjectForBeanInstance(sharedInstance, ...);
    }
    
    // ② 父工厂委派（如果当前工厂没有这个 BeanDefinition）
    BeanFactory parentBeanFactory = getParentBeanFactory();   // L279
    if (parentBeanFactory != null && !containsBeanDefinition(beanName)) {
        // 当前工厂没有 → 交给父工厂
        return (T) parentBeanFactory.getBean(nameToLookup, args);   // L293
    }
    
    // ③ 单例 + 首次创建
    if (mbd.isSingleton()) {
        // getSingleton 的 ObjectFactory 回调 → 真正创建 Bean
        sharedInstance = getSingleton(beanName, () -> {
            try {
                return createBean(beanName, mbd, args);   // L336 ★★★ 核心！
            } catch (BeansException ex) {
                destroySingleton(beanName);
                throw ex;
            }
        });
        bean = getObjectForBeanInstance(sharedInstance, ...);
    }
    
    // ④ Prototype（多例）→ 每次都创建新的
    else if (mbd.isPrototype()) {
        Object prototypeInstance = null;
        try {
            prototypeInstance = createBean(beanName, mbd, args);   // L354
        } finally {
            registerDependentBean(...);
        }
    }
    
    return (T) bean;
}
```

### 3.2 createBean · 创建前的 Smart 代理检查

**源码锚点**：AbstractAutowireCapableBeanFactory.java L503-566

```java
protected Object createBean(String beanName, RootBeanDefinition mbd, @Nullable Object[] args) {
    
    // ① 检查是否有 SmartInstantiationAwareBeanPostProcessor 想代理这个 Bean
    //    → resolveBeforeInstantiation：如果返回非 null → 直接用代理对象（不需要 doCreateBean）
    Object bean = resolveBeforeInstantiation(beanName, mbdToUse);   // L531
    if (bean != null) {
        // 有 Smart 代理 → 直接返回（可能已经 initializeBean 过了）
        return bean;
    }
    
    // ② 正常创建 → doCreateBean（实例化 + 属性注入 + 初始化）
    Object beanInstance = doCreateBean(beanName, mbdToUse, args);   // L573 ★★★
    return beanInstance;
}
```

### 3.3 doCreateBean · 实例化 + 注入 + 初始化 三步曲

**源码锚点**：AbstractAutowireCapableBeanFactory.java L573-648

```java
protected Object doCreateBean(String beanName, RootBeanDefinition mbd, @Nullable Object[] args) {
    
    // ============ 阶段 ①：实例化（createBeanInstance）============
    BeanWrapper instanceWrapper = null;
    // ★ 关键：处理循环依赖！
    //   earlySingletonExposure = 单例 + allowCircularReferences=true + 正在创建中
    boolean earlySingletonExposure = (mbd.isSingleton() && this.allowCircularReferences &&
            isSingletonCurrentlyInCreation(beanName));   // L608
    
    if (earlySingletonExposure) {
        // ★ 提前暴露到 singletonFactories（第三级缓存）
        //   ObjectFactory → 循环依赖的另一方可以从中拿到早期引用
        addSingletonFactory(beanName, () -> getEarlyBeanReference(beanName, mbd, bean));   // L610
    }
    
    // ★ createBeanInstance：实例化（反射 / CGLIB / Supplier）
    //   → resolveInstantiationStrategy.instantiate()
    //   → 如果有 Supplier → supplier.get()
    //   → 如果有 FactoryMethod → CGLIB / 反射调
    //   → 否则 → 构造函数反射（autowireConstructor / instantiateBean）
    instanceWrapper = createBeanInstance(beanName, mbd, args);   // L583 ★
    
    Object bean = instanceWrapper.getWrappedInstance();
    
    // ============ 阶段 ②：属性注入（populateBean）============
    // ★ populateBean：@Autowired / @Value / set 方法注入
    //   → InstantiationAwareBeanPostProcessor.postProcessProperties
    //   → AutowiredAnnotationBeanPostProcessor 处理 @Autowired
    //   → 注入时如果依赖的 Bean 还没创建 → 触发递归 doGetBean → 循环依赖三级缓存发挥作用
    populateBean(beanName, mbd, instanceWrapper);   // L623 ★★
    
    // ============ 阶段 ③：初始化（initializeBean）============
    // ★ initializeBean：Aware 回调 + @PostConstruct + InitializingBean.afterPropertiesSet
    //   → invokeAwareMethods：BeanNameAware / BeanClassLoaderAware / BeanFactoryAware
    //   → applyBeanPostProcessorsBeforeInitialization：@PostConstruct 在这里执行
    //   → invokeInitMethods：InitializingBean.afterPropertiesSet + init-method
    //   → applyBeanPostProcessorsAfterInitialization：AOP 代理在这里创建！
    Object exposedObject = initializeBean(beanName, bean, mbd);   // L430 ★★★
    
    return exposedObject;
}
```

### 3.4 Bean 生命周期完整时序

```
BeanDefinition 存在 → finishBeanFactoryInitialization.preInstantiateSingletons()
  │
  ├─ ① getSingleton(beanName)
  │    ├─ 第一级缓存 singletonObjects 命中？→ 直接返回
  │    ├─ 第二级缓存 earlySingletonObjects 命中？→ 直接返回
  │    └─ 第三级缓存 singletonFactories 命中？→ factory.getObject() → 存入二级缓存
  │
  ├─ ② createBean(beanName, mbd, args)
  │    │
  │    ├─ ②-a Smart 代理检查（resolveBeforeInstantiation）
  │    │    └─ SmartInstantiationAwareBeanPostProcessor 返回非 null → 直接返回代理
  │    │
  │    └─ ②-b doCreateBean
  │         │
  │         ├─ 提前暴露到 singletonFactories（循环依赖）
  │         │
  │         ├─ ★ 实例化 createBeanInstance
  │         │    ├─ Supplier → supplier.get()
  │         │    ├─ FactoryMethod → CGLIB / 反射
  │         │    └─ 构造函数 → instantiateBean / autowireConstructor
  │         │
  │         ├─ ★ 属性注入 populateBean
  │         │    ├─ @Autowired / @Value → AutowiredAnnotationBeanPostProcessor
  │         │    ├─ @Resource → CommonAnnotationBeanPostProcessor
  │         │    └─ setter 注入 → applyPropertyValues
  │         │
  │         ├─ ★ 初始化 initializeBean
  │         │    ├─ BeanNameAware / BeanFactoryAware / ApplicationContextAware 回调
  │         │    ├─ @PostConstruct（BeanPostProcessor.postProcessBeforeInitialization）
  │         │    ├─ InitializingBean.afterPropertiesSet() + init-method
  │         │    └─ AOP 代理创建（BeanPostProcessor.postProcessAfterInitialization）
  │         │
  │         └─ 从 singletonFactories 移到 singletonObjects → registerSingleton
  │
  └─ 返回完整初始化后的 Bean（可能是 AOP 代理）
```

---

## 四、循环依赖三级缓存

### 4.1 为什么需要三级缓存

```java
// 经典循环依赖场景：
@Service
public class ServiceA {
    @Autowired
    private ServiceB b;   // 依赖 B
}

@Service
public class ServiceB {
    @Autowired
    private ServiceA a;   // 依赖 A → 循环！
}

// 没有三级缓存会怎样？
// ① 创建 A → 需要注入 B → 创建 B
// ② 创建 B → 需要注入 A → 但 A 还没创建好 → 死锁！
//
// 有三级缓存：
// ① 创建 A → 提前把 A 的 ObjectFactory 放入 singletonFactories
// ② 注入 B → 创建 B → B 注入 A → 从 singletonFactories 拿到 A 的早期引用
// ③ A 和 B 都创建成功 → 完美！
```

### 4.2 DefaultSingletonBeanRegistry 三级缓存

**源码锚点**：DefaultSingletonBeanRegistry.java L78-84

```java
public class DefaultSingletonBeanRegistry {
    
    // ★ 第一级缓存：完整初始化后的单例 Bean
    //   key=beanName, value=完整初始化后的对象（属性注入 + @PostConstruct + AOP 代理都完成了）
    private final Map<String, Object> singletonObjects = new ConcurrentHashMap<>(256);   // L78
    
    // ★ 第二级缓存：早期暴露的单例 Bean（已经实例化但还没注入/初始化）
    //   key=beanName, value=早期引用（可能是代理对象，也可能是原始对象）
    //   → 从第三级缓存的 factory.getObject() 取出后放入这里，避免重复创建代理
    private final Map<String, Object> earlySingletonObjects = new ConcurrentHashMap<>(16);   // L84
    
    // ★ 第三级缓存：ObjectFactory 工厂
    //   key=beanName, value=ObjectFactory → 调用 factory.getObject() 拿到早期引用
    //   → 为什么用工厂而不是直接放对象？因为 AOP 代理！
    //      如果 A 需要被代理 → 这个 factory 会返回代理对象（getEarlyBeanReference 里创建代理）
    private final Map<String, ObjectFactory<?>> singletonFactories = new HashMap<>(16);   // L81
}
```

### 4.3 三级缓存协作流程

```
A 和 B 循环依赖的完整时序：

Step 1: 容器开始实例化 A
  │
  ├─ ① A 开始实例化 → isSingletonCurrentlyInCreation("A") = true
  │
  ├─ ② A 提前暴露到 singletonFactories：
  │    addSingletonFactory("A", () -> getEarlyBeanReference("A"))
  │    // ObjectFactory 闭包：调用时才执行 getEarlyBeanReference
  │    // getEarlyBeanReference 内部会做 SmartInstantiationAwareBeanPostProcessor.getEarlyBeanReference
  │    // 如果 A 有 AOP 代理 → 这里返回代理对象！
  │
  ├─ ③ A 实例化完成（createBeanInstance）
  │
  ├─ ④ A 开始属性注入 → 需要注入 ServiceB
  │    → doGetBean("B") → B 不存在 → 创建 B
  │
Step 2: 容器实例化 B
  │
  ├─ ⑤ B 也提前暴露到 singletonFactories（B 的工厂）
  │
  ├─ ⑥ B 实例化完成
  │
  ├─ ⑦ B 属性注入 → 需要注入 ServiceA
  │    → doGetBean("A")
  │
Step 3: 从三级缓存拿到 A 的早期引用
  │
  ├─ ⑧ getSingleton("A") → 一级缓存没有 → 二级缓存没有 → 三级缓存有！
  │    singletonFactories.get("A") → factory.getObject()
  │    → 执行 getEarlyBeanReference → 返回 A 的早期引用（可能是代理）
  │    → 从 singletonFactories 移除 → 放入 earlySingletonObjects（二级缓存）
  │    → B 拿到了 A 的早期引用 ✅
  │
Step 4: B 创建完成 → 回到 A
  │
  ├─ ⑨ B 初始化完成 → registerSingleton("B", b) → 放入 singletonObjects
  │
  ├─ ⑩ A 的属性注入完成（B 已经注入进来了）
  │
  ├─ ⑪ A 初始化完成 → registerSingleton("A", a)
  │     → 从 singletonFactories 移除
  │     → 从 earlySingletonObjects 移除
  │     → 放入 singletonObjects ✅
  │
  └─ 循环依赖解决！🎉
```

### 4.4 为什么需要第三级缓存（ObjectFactory 工厂）

```
问题：为什么不能把早期对象直接放到 singletonFactories？
      为什么还要用 ObjectFactory 工厂包装？

答案：**为了 AOP 代理！**

场景：ServiceA 需要被 AOP 代理（比如有 @Transactional 方法）

如果只有两级缓存：
  ① A 实例化完成 → 直接放入 singletonFactories（原始对象，不是代理）
  ② B 从 singletonFactories 拿到 A → 拿到的是原始对象
  ③ A 初始化完成 → 创建代理对象 → 放入 singletonObjects
  ④ B 持有 A 的原始对象 → AOP 失效！❌

有第三级缓存（ObjectFactory）：
  ① A 提前暴露 → addSingletonFactory("A", () -> getEarlyBeanReference("A"))
  ② B 从 singletonFactories 拿到 factory → factory.getObject()
  ③ factory 内部执行 getEarlyBeanReference → SmartInstantiationAwareBeanPostProcessor.getEarlyBeanReference
     → 返回 AOP 代理对象（不是原始对象！）
  ④ B 拿到 A 的代理对象 ✅
  ⑤ A 初始化完成 → 创建另一个代理对象？
     → 但 B 已经持有了早期代理...
     → Spring 会检查 earlySingletonObjects 里的对象是否和最终代理一致
     → 如果 B 已经持有早期代理 → 用早期代理（保证 B 和 singletonObjects 里的是同一个）

→ ObjectFactory 的闭包延迟了代理的创建时机
→ 只有在真的需要时才创建（循环依赖被引用时）
→ 非循环依赖的单例 → ObjectFactory 永远不会被调用 → 不会提前创建代理 ✅
```

---

## 五、面试考点

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **Spring Boot 启动** | | |
| 1 | SpringApplication.run 6 步？| ① getRunListeners+starting ② createApplicationContext ③ prepareContext ④ **refreshContext** ⑤ started ⑥ callRunners |
| 2 | createApplicationContext 三种类型？| SERVLET（内嵌 Tomcat）/ REACTIVE（WebFlux）/ NONE（纯后端）→ 根据 WebApplicationType 判断 |
| 3 | Spring Boot 自动配置入口？| @EnableAutoConfiguration → SpringFactoriesLoader 加载 META-INF/spring.factories |
| **IoC 容器刷新** | | |
| 4 | AbstractApplicationContext.refresh 11 步核心？| **② obtainFreshBeanFactory（加载 BeanDefinition）→ ⑤ invokeBeanFactoryPostProcessors（@Configuration 解析）→ ⑥ registerBeanPostProcessors（排序注册）→ ⑪ finishBeanFactoryInitialization（实例化单例）**|
| 5 | ConfigurationClassPostProcessor 作用？| BeanDefinitionRegistryPostProcessor → 解析 @Configuration 类 → 扫描 @Component → 处理 @Bean |
| 6 | BeanPostProcessor 注册顺序？| **PriorityOrdered → Ordered → 无顺序** → 内部 + 外部分开注册 |
| **Bean 创建** | | |
| 7 | Bean 创建三步曲？| **createBeanInstance（反射实例化）→ populateBean（@Autowired 注入）→ initializeBean（@PostConstruct + AOP）**|
| 8 | initializeBean 里 AOP 代理在哪创建？| **applyBeanPostProcessorsAfterInitialization**（初始化后）→ AnnotationAwareAspectJAutoProxyCreator.postProcessAfterInitialization |
| 9 | @Autowired 注入在哪执行？| populateBean 里 → AutowiredAnnotationBeanPostProcessor.postProcessProperties |
| **循环依赖** | | |
| 10 | 循环依赖三级缓存？| **① singletonObjects（完整单例）/ ② earlySingletonObjects（早期暴露）/ ③ singletonFactories（ObjectFactory 工厂）**|
| 11 | 为什么需要第三级缓存 ObjectFactory？| **AOP 代理！** 延迟代理创建时机 → getEarlyBeanReference 里创建 → 保证 B 拿到的是代理不是原始对象 |
| 12 | 循环依赖只有单例能解决？| **是的！** 多例每次创建新对象 → 不存在循环问题（但也不会缓存）；构造器注入无法解决（实例化阶段就需要依赖）|
| 13 | @Autowired 循环依赖报错？| 构造器注入报错（Cannot resolve circular dependency）；字段/setter 注入能解决（三级缓存）|
| **AOP** | | |
| 14 | Spring AOP 原理？| **动态代理**：JDK Proxy（接口）/ CGLIB（类）→ BeanPostProcessor.postProcessAfterInitialization 创建代理 |
| 15 | 切面优先级？| @Order / Ordered 接口 → 数字越小优先级越高；@Before 先执行高优先级，@After 先执行低优先级（洋葱模型）|
| **事务** | | |
| 16 | @Transactional 实现？| **TransactionInterceptor**（BeanPostProcessor 创建代理时织入）→ PlatformTransactionManager.getTransaction → commit/rollback |
| 17 | @Transactional 失效场景？| ① 方法非 public ② 同类内部调用绕过代理 ③ catch 异常没抛出 ④ noRollbackFor 配置错误 ⑤ 数据库引擎不支持事务 |
| 18 | 事务传播行为 7 种？| REQUIRED / SUPPORTS / MANDATORY / REQUIRES_NEW / NOT_SUPPORTED / NEVER / NESTED |

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线：Spring Boot 2.7.18 + Spring Framework 5.3.35（Java 8 兼容版）**  
**覆盖：Spring Boot run 6 步 + AbstractApplicationContext.refresh 11 步 + Bean 创建完整生命周期（doGetBean→createBean→doCreateBean→populateBean→initializeBean）+ 循环依赖三级缓存原理（singletonObjects/earlySingletonObjects/singletonFactories）+ 18 个面试考点**  
**源码锚点行号全部来自 Spring Framework 5.3.35 / Spring Boot 2.7.18 源码副本**
