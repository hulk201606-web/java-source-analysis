# 14 · Spring AOP 核心源码深度解析

> **源码基线**：Spring Framework 5.3.35（Java 8 兼容版，面试最高频）  
> **源码位置**：`d:\GC_WorkSpaces\spring-framework-5.3.35\spring-aop\src\main\java\`  
> **面试权重**：⭐⭐⭐⭐⭐（AOP 是 Spring 最核心的两大功能之一）

---

## 一、AOP 总体架构

### 1.1 AOP 核心概念

```java
// AOP 关键术语（面试必考）：
//
// ① Aspect（切面）：@Aspect 标注的类 → 横切关注点的模块化封装
//
// ② JoinPoint（连接点）：程序执行中的某个点（方法调用 / 字段访问）
//    → Spring AOP 只支持方法调用类型的 JoinPoint
//
// ③ Pointcut（切入点）：定义在哪些 JoinPoint 上应用 Advice
//    → execution(* com.example.service.*.*(..))
//
// ④ Advice（通知/增强）：在 JoinPoint 上执行的动作
//    → @Before / @AfterReturning / @AfterThrowing / @After / @Around
//
// ⑤ Advisor（切面 = Pointcut + Advice）：Spring 内部用的统一抽象
//    → 一个 Advisor = 一个 Pointcut + 一个 Advice
//    → @Aspect 里的每个 @Before/@After 最终都会被解析成一个 Advisor
//
// ⑥ Target（目标对象）：被代理的原始 Bean
//
// ⑦ Proxy（代理对象）：AOP 创建的对象 → 包装 Target
//
// ⑧ Weaving（织入）：把 Advice 应用到 Target 的过程
//    → 编译时织入（AspectJ 编译期）
//    → 类加载时织入（AspectJ LTW，Load-Time Weaving）
//    → 运行时织入（Spring AOP 默认，BeanPostProcessor 创建代理）
```

### 1.2 AOP 核心类关系图

```
AbstractAutoProxyCreator（BeanPostProcessor）
  │
  ├─ AnnotationAwareAspectJAutoProxyCreator（@Aspect 注解驱动）
  │    └─ findCandidateAdvisors() → BeanFactoryAspectJAdvisorsBuilder.buildAspectJAdvisors()
  │         → 扫描所有 @Aspect 类 → 生成 Advisor 列表
  │
  └─ postProcessAfterInitialization()
       └─ wrapIfNecessary()
            ├─ getAdvicesAndAdvisorsForBean() → 找到所有匹配的 Advisor
            └─ createProxy() → 创建代理对象
                 ├─ JdkDynamicAopProxy（接口 → JDK Proxy）
                 │    └─ invoke() → ReflectiveMethodInvocation.proceed()
                 └─ CglibAopProxy（类 → CGLIB 子类）
                      └─ DynamicAdvisedInterceptor.intercept() → ReflectiveMethodInvocation.proceed()

ReflectiveMethodInvocation（MethodInvocation 实现）
  │
  └─ proceed() → 递归调用拦截器链 → 最后 invokeJoinpoint() → 反射调用 target
```

---

## 二、AOP 代理创建全流程

### 2.1 AbstractAutoProxyCreator.postProcessAfterInitialization

**源码锚点**：AbstractAutoProxyCreator.java L289-293

```java
// ★ 这是 BeanPostProcessor 的钩子方法！
// Spring 在 Bean 初始化完成后（initializeBean 第 ③ 阶段）调用
public Object postProcessAfterInitialization(@Nullable Object bean, String beanName) {
    if (bean != null) {
        Object cacheKey = getCacheKey(bean.getClass(), beanName);
        // 如果已经缓存了 → 直接返回（避免重复代理）
        if (this.earlyProxyReferences.remove(cacheKey) != bean) {
            return wrapIfNecessary(bean, beanName, cacheKey);   // ★ 核心！决定是否需要代理
        }
    }
    return bean;
}
```

### 2.2 wrapIfNecessary · 是否需要代理

**源码锚点**：AbstractAutoProxyCreator.java L329-358

```java
protected Object wrapIfNecessary(Object bean, String beanName, Object cacheKey) {
    // ① targetSourcedBeans 里已经有的 → 直接返回（避免重复代理）
    if (StringUtils.hasLength(beanName) && this.targetSourcedBeans.contains(beanName)) {
        return bean;
    }
    // ② 已经标记为不需要代理 → 直接返回（缓存机制）
    if (Boolean.FALSE.equals(this.advisedBeans.get(cacheKey))) {
        return bean;
    }
    // ③ ★ 基础设施类 / 应该跳过的 → 标记为 FALSE 并返回
    //    基础设施类：Advice / Pointcut / Advisor / AopInfrastructureBean
    //    → 这些类本身就是 AOP 的基础设施，不能被代理！
    if (isInfrastructureClass(bean.getClass()) || shouldSkip(bean.getClass(), beanName)) {
        this.advisedBeans.put(cacheKey, Boolean.FALSE);
        return bean;
    }

    // ④ ★ 核心！查找是否有匹配的 Advisor
    //    getAdvicesAndAdvisorsForBean() 会：
    //    → 遍历所有 Advisor（包括 @Aspect 里解析出来的）
    //    → 用 Pointcut 匹配当前 bean 的所有方法
    //    → 返回匹配的 Advisor 数组
    Object[] specificInterceptors = getAdvicesAndAdvisorsForBean(bean.getClass(), beanName, null);
    if (specificInterceptors != DO_NOT_PROXY) {
        // ⑤ ★ 有匹配的 Advisor → 创建代理对象！
        this.advisedBeans.put(cacheKey, Boolean.TRUE);
        Object proxy = createProxy(
                bean.getClass(), beanName, specificInterceptors, 
                new SingletonTargetSource(bean));   // targetSource 持有原始对象
        this.proxyTypes.put(cacheKey, proxy.getClass());
        return proxy;
    }
    
    // ⑥ 没有匹配的 Advisor → 标记为 FALSE，返回原始对象
    this.advisedBeans.put(cacheKey, Boolean.FALSE);
    return bean;
}
```

### 2.3 AnnotationAwareAspectJAutoProxyCreator.findCandidateAdvisors

**源码锚点**：AnnotationAwareAspectJAutoProxyCreator.java L90-95

```java
// ★ 这是找 Advisor 的关键步骤！
// Spring AOP 要应用的 Advisor 来自两个来源：
protected List<Advisor> findCandidateAdvisors() {
    // ① 先找 Spring 容器里显式注册的 Advisor Bean
    //    比如 XML 配置的 <aop:config> 里的 Advisor
    List<Advisor> advisors = super.findCandidateAdvisors();   // 父类 AbstractAdvisorAutoProxyCreator
    
    // ② ★ 再扫描所有 @Aspect 注解的类 → 生成 Advisor
    //    → BeanFactoryAspectJAdvisorsBuilder.buildAspectJAdvisors()
    //    → 内部用 AspectJAnnotationUtils 解析 @Aspect 里的 @Before/@After/@Around
    //    → 每个注解方法生成一个 Advisor（Pointcut + Advice）
    if (this.aspectJAdvisorsBuilder != null) {
        advisors.addAll(this.aspectJAdvisorsBuilder.buildAspectJAdvisors());
    }
    return advisors;
}
```

### 2.4 createProxy · 选择 JDK Proxy 还是 CGLIB

**源码锚点**：AbstractAutoProxyCreator.java L443-498

```java
protected Object createProxy(Class<?> beanClass, @Nullable String beanName,
        @Nullable Object[] specificInterceptors, TargetSource targetSource) {
    
    // ① 创建 AdvisedSupport（AOP 配置上下文）
    AdvisedSupport advised = new AdvisedSupport();
    advised.setTargetSource(targetSource);   // targetSource 持有原始 Bean
    advised.setInterceptorNames(interceptorNames);
    advised.setTargetClass(beanClass);
    
    // ② ★ 关键：选择 JDK Proxy 还是 CGLIB？
    //    规则：
    //    - 如果 beanClass 有接口 → 默认用 JDK Proxy（基于接口代理）
    //    - 如果 beanClass 没有接口 → 默认用 CGLIB（子类代理）
    //    - 如果 @EnableAspectJAutoProxy(proxyTargetClass=true) → 强制用 CGLIB
    //    - 如果 beanClass 有接口但 proxyTargetClass=true → 也用 CGLIB
    if (shouldProxyTargetClass(beanClass, beanName)) {
        // proxyTargetClass=true → 强制 CGLIB
        advised.setProxyTargetClass(true);
    }
    
    // ③ 创建 AopProxy
    AopProxyFactory aopProxyFactory = this.aopProxyFactory;   // DefaultAopProxyFactory
    AopProxy aopProxy = aopProxyFactory.createAopProxy(advised);
    
    // ④ ★ 最后！调用 getProxy() 创建真正的代理对象
    return aopProxy.getProxy(getProxyClassLoader());
}
```

### 2.5 DefaultAopProxyFactory.createAopProxy · 最终选择

```java
// DefaultAopProxyFactory（Spring 默认的 AopProxyFactory）
public AopProxy createAopProxy(AdvisedSupport config) {
    // ★ 决策三步骤：
    //
    // ① 如果 proxyTargetClass=true
    //    → 强制用 CGLIB（即使有接口）
    if (config.isProxyTargetClass()) {
        // 需要检查 CGLIB 是否在 classpath 上
        return new CglibAopProxy(config);
    }
    
    // ② 如果 bean 有接口
    //    → 用 JDK Proxy（不需要 CGLIB 依赖）
    if (hasNoUserDeclaredInterfaces(config.getTargetClass())) {
        return new JdkDynamicAopProxy(config);
    }
    
    // ③ 没有接口
    //    → 用 CGLIB
    return new CglibAopProxy(config);
}

// 决策逻辑总结：
//
// 有接口 + proxyTargetClass=false → JDK Proxy ✅
// 有接口 + proxyTargetClass=true  → CGLIB ✅
// 无接口                          → CGLIB ✅（唯一选择）
```

---

## 三、JDK Proxy 代理执行流程

### 3.1 JdkDynamicAopProxy.invoke

**源码锚点**：JdkDynamicAopProxy.java L185-240

```java
// ★ 这是 JDK 代理对象的 invoke 方法！
// 每调用一次代理方法 → 进入这里
public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object oldProxy = null;
    boolean setProxyContext = false;
    TargetSource targetSource = this.advised.targetSource;

    try {
        // ① 特殊方法处理：equals / hashCode / getDecoratedClass / Advised 接口
        //    这些方法不走 AOP 拦截器链
        
        // ② ★ 暴露当前代理到 ThreadLocal
        //    如果 exposeProxy=true（@EnableAspectJAutoProxy(exposeProxy=true)）
        //    → AopContext.setCurrentProxy(proxy)
        //    → 解决同类内部调用绕过代理问题（AopContext.currentProxy().method()）
        if (this.advised.exposeProxy) {
            oldProxy = AopContext.setCurrentProxy(proxy);
            setProxyContext = true;
        }

        // ③ ★ 从 TargetSource 获取原始对象
        //    targetSource.getTarget() → 拿到被代理的原始 Bean
        target = targetSource.getTarget();
        Class<?> targetClass = (target != null ? target.getClass() : null);

        // ④ ★ 获取拦截器链
        //    this.advised.getInterceptorsAndDynamicInterceptionAdvice(method, targetClass)
        //    → 根据当前调用的 method + targetClass
        //    → 找到所有匹配的 Advisor → 转成 MethodInterceptor 数组
        //    → 最终生成 ReflectiveMethodInvocation 需要的 interceptors 列表
        MethodInterceptor[] interceptors = this.advised.getInterceptors(method, targetClass);

        // ⑤ ★ 创建 ReflectiveMethodInvocation（调用链）
        MethodInvocation invocation =
                new ReflectiveMethodInvocation(proxy, target, method, args, targetClass, chain);

        // ⑥ ★ 执行调用链！
        Object retVal = invocation.proceed();   // ← 进入拦截器链递归

        // ⑦ 处理返回值 / 异常
        ...
        return retVal;
    }
    finally {
        // ⑧ 恢复暴露的代理
        if (setProxyContext) {
            AopContext.setCurrentProxy(oldProxy);
        }
    }
}
```

### 3.2 CglibAopProxy · 子类代理

```java
// CGLIB 代理 vs JDK Proxy：
//
// JDK Proxy：基于接口 → 代理类实现和 target 相同的接口
//   Target implements UserService { ... }
//   Proxy implements UserService { invoke() → invocation.proceed() }
//
// CGLIB：基于继承 → 代理类是 target 的子类
//   Target { ... }
//   Proxy extends Target { 
//     // 覆盖所有非 final 方法
//     public void doSomething() { 
//       new ReflectiveMethodInvocation(...).proceed(); 
//     }
//   }
//
// CGLIB 限制：
//   ❌ 不能代理 final 类（无法继承）
//   ❌ 不能代理 final 方法（无法覆盖）
//   ❌ 不能代理 static 方法（静态方法不参与继承）
//   ❌ 不能代理 private 方法（无法覆盖）
//
// JDK Proxy 限制：
//   ❌ 必须有接口
//   ❌ 只能代理接口上声明的方法（类的额外 public 方法无法代理）
```

---

## 四、ReflectiveMethodInvocation.proceed · 拦截器链递归执行

### 4.1 proceed 核心逻辑

**源码锚点**：ReflectiveMethodInvocation.java L160-188

```java
// ★ 这是 AOP 最核心的方法！拦截器链的递归执行逻辑
// 像洋葱一样：@Before（外层）→ ... → @Around → target → @After（从内往外）
public Object proceed() throws Throwable {
    // ① 如果已经是最后一个拦截器 → 直接调 target 方法
    if (this.currentInterceptorIndex == this.interceptorsAndDynamicMethodMatchers.size() - 1) {
        return invokeJoinpoint();   // ← 最终调用 target 方法（反射）
    }

    // ② ★ 取当前拦截器（currentInterceptorIndex 从 -1 开始，每次 ++）
    Object interceptorOrInterceptionAdvice =
            this.interceptorsAndDynamicMethodMatchers.get(++this.currentInterceptorIndex);
    
    // ③ 动态匹配检查（DynamicMethodMatcher 做运行时匹配）
    if (interceptorOrInterceptionAdvice instanceof InterceptorAndDynamicMethodMatcher) {
        InterceptorAndDynamicMethodMatcher dm =
                (InterceptorAndDynamicMethodMatcher) interceptorOrInterceptionAdvice;
        Class<?> targetClass = (this.targetClass != null ? this.targetClass : this.method.getDeclaringClass());
        if (dm.methodMatcher.matches(this.method, targetClass, this.arguments)) {
            return dm.interceptor.invoke(this);   // ★ 拦截器调用 this → 拦截器内部再调 invocation.proceed()
        }
        else {
            // 动态匹配失败 → 跳过这个拦截器，继续下一个
            return proceed();
        }
    }
    else {
        // ④ ★ 普通 MethodInterceptor（静态匹配已经通过了）
        //    → interceptor.invoke(this) 就是执行当前拦截器
        //    → 拦截器内部如果需要继续调用 → 再调 invocation.proceed()
        return ((MethodInterceptor) interceptorOrInterceptionAdvice).invoke(this);
    }
}

// invokeJoinpoint：最终调用 target 方法
protected Object invokeJoinpoint() throws Throwable {
    return AopUtils.invokeJoinpointUsingReflection(this.target, this.method, this.arguments);
}
```

### 4.2 拦截器链执行时序（洋葱模型）

```
@Aspect 里定义：
  @Before("execution(* Service.method())") public void before() { ... }       ← 第 1 个拦截器
  @After("execution(* Service.method())")  public void after() { ... }        ← 第 2 个拦截器
  @AfterReturning("execution(* Service.method())") public void returning() {} ← 第 3 个拦截器
  @AfterThrowing("execution(* Service.method())") public void throwing() {}    ← 第 4 个拦截器

// 执行顺序：洋葱模型
//
// 代理对象.method()
//   │
//   ├─ 1. @Before 执行  ← 最外层（currentInterceptorIndex=0）
//   │    │
//   │    ├─ 2. @Around proceed()（如果有 Around → 这里是 proceed() 调用点）
//   │    │    │
//   │    │    ├─ 3. target.method() 执行（invokeJoinpoint）
//   │    │    │
//   │    │    ├─ 4. target 返回正常 → proceed() 返回（回到 Around 之后）
//   │    │    │    └─ 如果 Around 里还有 @AfterReturning 逻辑 → 执行
//   │    │    │
//   │    │    └─ 如果 target 抛异常 → proceed() 抛异常（回到 Around）
//   │    │         └─ @AfterThrowing 执行
//   │    │
//   │    └─ Around 完成（return 或 throw）
//   │
//   ├─ 5. @AfterReturning（如果正常返回）
//   │
//   ├─ 6. @After（无论正常还是异常都执行，finally 语义）
//   │
//   └─ 返回结果给调用者

// 洋葱模型关键点：
//   proceed() 是递归的！
//   每个拦截器都拿到 ReflectiveMethodInvocation 对象
//   拦截器执行完自己的逻辑后，要调 invocation.proceed() → 传递给下一个拦截器
//   最后一个拦截器执行完 → invokeJoinpoint() → 调用 target 方法
//   然后递归回来 → 每个拦截器可以拿到返回值/异常做后处理
```

### 4.3 拦截器链的生成

```java
// 拦截器链是怎么生成的？
//
// 在 wrapIfNecessary 找到匹配的 Advisor 后：
//
// ① AdvisedSupport.getInterceptorsAndDynamicInterceptionAdvice(method, targetClass)
//    → 遍历所有 Advisor
//    → AdvisorAdapterRegistry 把每个 Advisor 转成 MethodInterceptor
//    → 如果 Pointcut 是 StaticMethodMatcher → 直接加入拦截器链
//    → 如果 Pointcut 是 DynamicMethodMatcher → 包装成 InterceptorAndDynamicMethodMatcher
//
// ② 拦截器链排序：@Order / Ordered 接口 / @Aspect 里的优先级
//    → 数字越小优先级越高
//    → @Before 先执行高优先级（洋葱外层）
//    → @After 先执行低优先级（洋葱内层）
//
// ③ 最终生成：List<Object> interceptorsAndDynamicMethodMatchers
//    → 这个列表传给 ReflectiveMethodInvocation 构造函数
//    → proceed() 从 index=-1 开始递归执行
```

---

## 五、@Aspect 注解解析

### 5.1 AnnotationAwareAspectJAutoProxyCreator

**源码锚点**：AnnotationAwareAspectJAutoProxyCreator.java L90-95

```java
// ★ Spring 扫描 @Aspect 类的入口
// 实现 BeanPostProcessor + BeanFactoryPostProcessor
public class AnnotationAwareAspectJAutoProxyCreator extends AspectJAwareAdvisorAutoProxyCreator {

    @Override
    protected List<Advisor> findCandidateAdvisors() {
        // ① 先找显式注册的 Advisor Bean
        List<Advisor> advisors = super.findCandidateAdvisors();
        
        // ② ★ 扫描 @Aspect 注解的类
        //    BeanFactoryAspectJAdvisorsBuilder 内部：
        //    → beanFactory.getBeanNamesForType(Object.class) → 遍历所有 Bean
        //    → 检查每个 Bean 是否有 @Aspect 注解
        //    → 如果有 → 用 AspectJAdvisorFactory 解析
        //    → 每个 @Before/@After/@Around/@AfterReturning/@AfterThrowing 方法
        //      生成一个 InstantiationModelAwarePointcutAdvisorImpl
        if (this.aspectJAdvisorsBuilder != null) {
            advisors.addAll(this.aspectJAdvisorsBuilder.buildAspectJAdvisors());
        }
        return advisors;
    }
}
```

### 5.2 AspectJ 注解类型

```java
// AspectJ 5 种通知类型：
//
// ① @Before：前置通知 → 在 target 方法执行前执行
//    → 对应 MethodBeforeAdviceInterceptor
//    → 没有返回值，不能阻止 target 执行（除非抛异常）
//
// ② @AfterReturning：后置返回通知 → target 正常返回后执行
//    → 对应 AfterReturningAdviceInterceptor
//    → 可以拿到 target 的返回值
//
// ③ @AfterThrowing：后置异常通知 → target 抛异常后执行
//    → 对应 ThrowAdviceInterceptor
//    → 可以拿到异常对象
//
// ④ @After：后置通知 → finally 语义，无论正常还是异常都执行
//    → 对应 AspectJAfterAdvice（用 try-finally 包装）
//    → ★ 和 Java finally 一样，正常和异常都会执行
//
// ⑤ @Around：环绕通知 → 最强大，可以完全控制 target 执行
//    → 对应 AspectJAroundAdvice
//    → 参数必须是 ProceedingJoinPoint
//    → 可以决定是否执行 target、是否修改参数、是否修改返回值
//
// ★ 优先级：
//    同一 @Aspect 里多个通知 → @Around 最外层 → target → @Before/@AfterReturning/@AfterThrowing/@After
//    多个 @Aspect 之间 → 用 @Order / Ordered 接口控制
```

### 5.3 Pointcut 切点表达式

```java
// execution 表达式（最常用）：
// execution(修饰符? 返回值 包路径.类.方法(参数) 异常?)
//
// 示例：
//   execution(* com.example.service.*.*(..))
//   含义：com.example.service 包下所有类的所有 public 方法
//
//   execution(public * com.example..*Service.*(..))
//   含义：com.example 包下任何层级、名字以 Service 结尾的类、所有 public 方法
//
//   execution(* save*(..))
//   含义：所有名字以 save 开头的方法（不限定类）
//
// 通配符：
//   *  → 匹配任意字符（单个包 / 单个类 / 单个方法名的一部分）
//   .. → 匹配任意子包 / 任意参数（在 execution 里是参数匹配，在 within 里是包匹配）
//   +  → 匹配类及其所有子类
//
// 其他切点指示符：
//   within(com.example.service.*)  → 限定类范围（不关心方法）
//   target(com.example.Service)     → 限定被代理对象类型
//   args(java.lang.String)          → 限定方法参数类型
//   @annotation(com.example.Log)    → 限定方法有某个注解（比如自定义 @Log）
//   @within(com.example.Service)    → 限定类有某个注解
//
// 组合：
//   execution(...) && @Transactional  → 方法匹配 && 有 @Transactional 注解
//   execution(...) || execution(...)  → 或
//   !execution(...)                  → 非
```

---

## 六、Spring AOP 经典面试题深入

### 6.1 JDK Proxy vs CGLIB 区别

| 维度 | **JDK Proxy** | **CGLIB** |
|------|--------------|-----------|
| 原理 | 基于接口 | 基于继承（生成子类）|
| 要求 | 必须有接口 | 类不能是 final |
| final 方法 | 可以代理（接口方法）| ❌ 不能代理（无法覆盖）|
| final 类 | 可以代理 | ❌ 不能代理（无法继承）|
| private 方法 | ❌ 不能代理 | ❌ 不能代理（无法覆盖）|
| static 方法 | ❌ 不能代理 | ❌ 不能代理 |
| 依赖 | JDK 内置 | 需要 CGLIB 库 |
| 性能 | 反射调用（稍慢）| FastClass 索引调用（稍快）|
| 默认选择 | 有接口时用 | 无接口时用 / proxyTargetClass=true 时强制用 |

### 6.2 同类内部调用绕过代理问题

```java
// 问题代码：
@Service
public class ServiceA {
    
    @Transactional(rollbackFor = Exception.class)
    public void doTransaction() {
        // ...
    }
    
    public void doSomething() {
        // ❌ 问题！this.doTransaction() 绕过了代理！
        // this 指向的是原始对象，不是代理对象
        // → @Transactional 失效！不会开启事务！
        this.doTransaction();
    }
}

// 为什么会这样？
//   Spring 创建的是代理对象 proxy
//   doSomething() 在代理对象上被调用 → 进入 JdkDynamicAopProxy.invoke()
//   invoke() 内部拿到原始对象 target → ReflectiveMethodInvocation
//   proceed() → 执行 doSomething()（这是 target 上的方法）
//   doSomething() 里 this.doTransaction() → this = target（原始对象）
//   → 直接调 target 的 doTransaction()，完全绕过代理！
//   → @Transactional 不生效！

// 解决方案 1：AopContext.currentProxy()
@Service
public class ServiceA {
    @Transactional
    public void doTransaction() { ... }
    
    public void doSomething() {
        // ✅ 用 AopContext.currentProxy() 拿到当前代理对象
        ((ServiceA) AopContext.currentProxy()).doTransaction();
    }
}
// 需要 @EnableAspectJAutoProxy(exposeProxy=true) 开启暴露

// 解决方案 2：自己注入自己
@Service
public class ServiceA {
    @Autowired
    private ServiceA self;  // 注入自己 → self 是代理对象！
    
    @Transactional
    public void doTransaction() { ... }
    
    public void doSomething() {
        // ✅ self 是代理对象 → @Transactional 生效
        self.doTransaction();
    }
}

// 解决方案 3：ApplicationContext.getBean()
public void doSomething() {
    // ✅ 从容器拿 → 拿到的是代理对象
    applicationContext.getBean(ServiceA.class).doTransaction();
}
```

### 6.3 @Transactional 失效场景

```java
// ① 方法非 public
@Transactional
void doSomething() { ... }  // ❌ 非 public → AOP 不会代理这个方法
// 原因：Spring AOP 是基于代理对象的，private/protected 方法无法被 JDK Proxy/CGLIB 拦截

// ② 同类内部调用（绕过代理）
// 见 6.2 详细讲解

// ③ catch 异常没抛出
@Transactional
public void doSomething() {
    try {
        // db operation
    } catch (Exception e) {
        // ❌ 吃掉了异常 → 事务不会回滚！
        log.error(e);
    }
}
// 原因：TransactionInterceptor 只在检测到异常时才 rollback
//       catch 后没抛出 → TransactionInterceptor 以为正常返回 → commit！

// ④ noRollbackFor 配置错误
@Transactional(noRollbackFor = RuntimeException.class)
public void doSomething() {
    throw new RuntimeException();  // ❌ 抛了 RuntimeException，但 noRollbackFor 指定了不回滚
}

// ⑤ 数据库引擎不支持事务
// MyISAM 引擎不支持事务 → @Transactional 配置了也没用

// ⑥ 多数据源场景下没指定 TransactionManager
// Spring 默认用 JtaTransactionManager 或 DataSourceTransactionManager
// 如果有多个数据源，需要用 @Transactional(transactionManager = "xxxTransactionManager")
```

### 6.4 AOP 执行顺序（优先级）

```java
// 多个 Aspect 的执行顺序：
//
// 规则：
//   ① @Order / Ordered 接口 → 数字越小优先级越高
//   ② @Aspect 里的 @Before 按优先级从高到低执行（洋葱外层 → 内层）
//   ③ @Aspect 里的 @After 按优先级从低到高执行（洋葱内层 → 外层）
//   ④ 同一个 Aspect 里的多个 @Before/@After → 按方法名字母序（不保证！不要依赖顺序）
//
// 示例：
@Aspect @Order(1)
public class LogAspect {
    @Before("...") public void before() { System.out.println("Log before"); }
    @After("...") public void after() { System.out.println("Log after"); }
}

@Aspect @Order(2)
public class TxAspect {
    @Before("...") public void before() { System.out.println("Tx before"); }
    @After("...") public void after() { System.out.println("Tx after"); }
}

// 执行顺序：
//   Log before（Order=1，优先级高 → 洋葱外层）
//   Tx before（Order=2，优先级低 → 洋葱内层）
//   target.method()
//   Tx after（Order=2 先结束 → 洋葱内层先弹出来）
//   Log after（Order=1 后结束 → 洋葱外层后弹出来）

// 洋葱模型图：
//
//   ┌─────────────┐
//   │ Log before  │ ← Order=1（最高优先级）
//   │ ┌───────────┐ │
//   │ │ Tx before │ │ ← Order=2
//   │ │ ┌─────────┐ │ │
//   │ │ │  target │ │ │ ← 真正的业务方法
//   │ │ └─────────┘ │ │
//   │ │ │Tx after │ │ │ ← Order=2 先弹出来
//   │ └───────────┘ │
//   │  Log after    │ ← Order=1 后弹出来
//   └─────────────┘
```

### 6.5 Spring AOP vs AspectJ

| 维度 | **Spring AOP** | **AspectJ** |
|------|---------------|-------------|
| 原理 | 运行时动态代理（JDK Proxy / CGLIB） | **编译时 / 类加载时织入** |
| 能力 | 只能拦截方法调用 | 可以拦截字段赋值 / 构造函数 / 方法调用 / 静态初始化块 |
| final 方法 | ❌ 不能代理 CGLIB final 方法 | ✅ 可以（编译时修改字节码）|
| 性能 | 运行时代理有开销 | 编译时织入后和手写 Advice 一样快 |
| 依赖 | spring-aop / spring-aspects | aspectjweaver / aspectjrt |
| 配置 | @EnableAspectJAutoProxy | -javaagent:aspectjweaver.jar（LTW）或编译时加 ajc |
| 适用 | Spring 容器内的 Bean | 任何 Java 类（包括非 Spring）|

---

## 七、面试考点（AOP 15 题）

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **AOP 原理** | | |
| 1 | Spring AOP 实现原理？| **运行时动态代理**：AbstractAutoProxyCreator（BeanPostProcessor）→ postProcessAfterInitialization → wrapIfNecessary → JdkDynamicAopProxy / CglibAopProxy → ReflectiveMethodInvocation.proceed() |
| 2 | 代理创建入口？| AbstractAutoProxyCreator.postProcessAfterInitialization L289 → wrapIfNecessary L329 |
| 3 | wrapIfNecessary 判断逻辑？| 基础设施类不能代理（Advice/Advisor/Pointcut/AopInfrastructureBean）→ getAdvicesAndAdvisorsForBean 找匹配的 Advisor → DO_NOT_PROXY 则不创建代理 |
| 4 | JDK Proxy vs CGLIB 选择规则？| **有接口 + proxyTargetClass=false → JDK**；**有接口 + proxyTargetClass=true → CGLIB**；**无接口 → CGLIB**（唯一选择）|
| **拦截器链** | | |
| 5 | ReflectiveMethodInvocation.proceed() 核心逻辑？| **currentInterceptorIndex 从 -1 开始** → 每次 ++ → 取拦截器 → 调 interceptor.invoke(this) → 拦截器内部再调 proceed() → 最后 invokeJoinpoint() 反射调用 target |
| 6 | 拦截器链排序？| **@Order / Ordered 接口 → 数字越小优先级越高**；@Before 按优先级从高到低，@After 按优先级从低到高（洋葱模型）|
| **@Aspect** | | |
| 7 | @Aspect 怎么被扫描到的？| **AnnotationAwareAspectJAutoProxyCreator** → findCandidateAdvisors() → BeanFactoryAspectJAdvisorsBuilder.buildAspectJAdvisors() → 扫描所有 @Aspect Bean → AspectJAdvisorFactory 解析成 Advisor |
| 8 | @Aspect 里的 5 种通知？| **@Before / @AfterReturning / @AfterThrowing / @After（finally 语义）/ @Around（最强大，控制 target 执行）** |
| 9 | @After 和 @AfterReturning 区别？| @After 是 finally 语义（**无论正常还是异常都执行**）；@AfterReturning 只在正常返回后执行 |
| **失效场景** | | |
| 10 | @Transactional 失效 5 场景？| ① 方法非 public ② 同类内部调用绕过代理 ③ catch 异常没抛出 ④ noRollbackFor 配置错误 ⑤ 数据库引擎不支持事务 |
| 11 | 同类内部调用怎么解决？| **① AopContext.currentProxy()（需 exposeProxy=true）② 自己注入自己 ③ ApplicationContext.getBean()** |
| 12 | CGLIB 限制？| ❌ final 类 / ❌ final 方法 / ❌ private 方法 / ❌ static 方法 |
| **高级** | | |
| 13 | exposeProxy 作用？| **JdkDynamicAopProxy.invoke 中 setCurrentProxy 到 ThreadLocal** → 解决同类内部调用问题 → AopContext.currentProxy() 可拿到当前代理对象 |
| 14 | AspectJ 比 Spring AOP 强在哪？| **编译时/类加载时织入** → 可以拦截字段赋值/构造函数/静态初始化块 → 支持 final 方法代理 → 性能更好 |
| 15 | Advisor 和 Advice 区别？| **Advice 是增强逻辑**；**Advisor = Pointcut + Advice**（Spring 内部统一抽象，@Aspect 里的每个注解方法都会被解析成 Advisor）|

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线**：Spring Framework 5.3.35（Java 8 兼容版）  
**覆盖**：Spring AOP 代理创建全流程（AbstractAutoProxyCreator.postProcessAfterInitialization → wrapIfNecessary → getAdvicesAndAdvisorsForBean → createProxy → JdkDynamicAopProxy/CglibAopProxy）+ ReflectiveMethodInvocation.proceed() 拦截器链递归执行（洋葱模型）+ @Aspect 注解扫描（AnnotationAwareAspectJAutoProxyCreator → BeanFactoryAspectJAdvisorsBuilder）+ 5 种通知类型 + Pointcut 切点表达式 + JDK Proxy vs CGLIB 对比 + 同类内部调用解决方案 + @Transactional 失效 5 场景 + 15 个面试考点  
**源码锚点行号全部来自 Spring Framework 5.3.35 源码副本**
