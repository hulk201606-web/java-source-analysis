# JDK 1.8 核心源码深度解析 · 总览索引

> 基于 `d:\GC_WorkSpaces\jdk-source-analysis\src\`（JDK 1.8.0_451 完整源码副本）逐文件精读  
> 所有知识点均标注 **源码锚点行号**，可直接点击跳转

---

## 📚 文档体系导航

| # | 板块 | 核心源码类 | 面试权重 | 子文档 |
|---|------|-----------|---------|--------|
| 1 | 集合框架 | HashMap / ConcurrentHashMap / ArrayList / LinkedList | ⭐⭐⭐⭐⭐ | [01_集合框架源码解析.md](01_集合框架源码解析.md) |
| 2 | 并发基础 | AQS / ReentrantLock / Thread / Object(wait/notify) / synchronized / volatile / CAS / ThreadPoolExecutor / ThreadLocal | ⭐⭐⭐⭐⭐ | [02_并发基础源码解析.md](02_并发基础源码解析.md) |
| 3 | IO / NIO | FilterInputStream / BufferedInputStream / DataInputStream / PushbackInputStream / ObjectInputStream / Buffer | ⭐⭐⭐⭐⭐ | [03_IO_NIO体系源码解析.md](03_IO_NIO体系源码解析.md) |
| 4 | JVM / GC | Object(mark word) / GarbageCollectorMXBean | ⭐⭐⭐⭐ | [04_JVM与GC源码解析.md](04_JVM与GC源码解析.md) |
| 5 | 反射与异步 | String / Proxy / InvocationHandler / CompletableFuture（异步链触发机制源码解析）| ⭐⭐⭐⭐ | [05_反射与异步编程.md](05_反射与异步编程.md) |
| 6 | AQS 共享工具类 | CountDownLatch / Semaphore（公平+非公平）/ CyclicBarrier / LinkedBlockingQueue（双锁）/ ArrayBlockingQueue（环形）| ⭐⭐⭐⭐⭐ | [06_AQS共享工具类源码解析.md](06_AQS共享工具类源码解析.md) |
| 7 | ForkJoinPool | WorkQueue 双端队列 / work-stealing / fork-join 帮忙偷 / commonPool / CompletableFuture 默认底层 | ⭐⭐⭐⭐⭐ | [07_ForkJoinPool分治框架源码解析.md](07_ForkJoinPool分治框架源码解析.md) |
| 8 | ConcurrentSkipListMap | Node 三身份 / Index 多层跳表 / findPredecessor CAS 清理 / doPut 三检查 + 随机层级 / doRemove 两步删除 + helpDelete 多线程协作 / tryReduceLevel 防抖 | ⭐⭐⭐⭐⭐ | [08_ConcurrentSkipListMap跳表源码解析.md](08_ConcurrentSkipListMap跳表源码解析.md) |
| 9 | Stream API | Pipeline 双向链表 + Sink 反向链式包装 + Spliterator 可分裂迭代 + evaluate 完整流程 + 并行流 ForkJoinPool 分治 + StatefulOp 强制串行 + 短路机制 + Collectors 框架 | ⭐⭐⭐⭐⭐ | [09_Stream_API源码解析.md](09_Stream_API源码解析.md) |
| 10 | 网络IO与类加载 | URLConnection 三阶段（setupRequest→connect→getInputStream）+ HttpURLConnection 重定向 + Selector SPI 工厂 + Channel.register + select self-pipe wakeup + ClassLoader 双亲委派 + loadClass + defineClass 安全检查三步曲 + 并行加载锁 | ⭐⭐⭐⭐⭐ | [10_网络IO与类加载源码解析.md](10_网络IO与类加载源码解析.md) |
| 11 | invokedynamic 与 Lambda | invokedynamic 字节码结构 + BootstrapMethod + Lambda 编译原理（不生成额外 .class 文件）+ MethodHandle 完整变换 API（bindTo/asType/asSpreader/asCollector/dropArguments/insertArguments/guardWithTest/catchException）+ LambdaMetafactory.metafactory 源码 + 框架应用（Spring 5/MyBatis 3.5/Dubbo/Netty）+ CallSite 三种类型 + Java 9 String 拼接优化 | ⭐⭐⭐⭐⭐ | [11_invokedynamic与Lambda源码解析.md](11_invokedynamic与Lambda源码解析.md) |
| 12 | Unsafe / VarHandle / SwitchPoint / Lookup / JPMS | Unsafe 六大类 API + CAS cmpxchg + park/unpark permit 令牌 + VarHandle 类型安全替代 + SwitchPoint 失效传播（MutableCallSite K_true→K_false 永久失效）+ MethodHandles.Lookup 四种访问模式（PUBLIC/PACKAGE/PROTECTED/PRIVATE）+ IMPL_LOOKUP 绕过权限 + unreflect 反射转 MH + JPMS 模块系统（module-info / exports vs opens / PlatformClassLoader / 强封装反射限制）| ⭐⭐⭐⭐⭐ | [12_UnsafeVarHandleSwitchPointLookupJPMS源码解析.md](12_UnsafeVarHandleSwitchPointLookupJPMS源码解析.md) |
| 13 | Spring Framework + Spring Boot | Spring Boot run 6 步（getRunListeners→createApplicationContext→prepareContext→refreshContext→started→callRunners）+ AbstractApplicationContext.refresh 11 步（BeanDefinition加载→BeanFactoryPostProcessor→BeanPostProcessor→实例化单例）+ Bean 创建完整生命周期（doGetBean→createBean→doCreateBean→populateBean→initializeBean）+ 循环依赖三级缓存（singletonObjects/singletonFactories/earlySingletonObjects + ObjectFactory 延迟代理创建）+ @Autowired/@Transactional/AOP 原理 | ⭐⭐⭐⭐⭐ | [13_Spring核心源码解析.md](13_Spring核心源码解析.md) |
| 14 | Spring AOP 深度解析 | AbstractAutoProxyCreator 代理创建全流程（postProcessAfterInitialization→wrapIfNecessary→createProxy→JdkDynamicAopProxy/CglibAopProxy）+ ReflectiveMethodInvocation.proceed() 拦截器链递归（洋葱模型）+ AnnotationAwareAspectJAutoProxyCreator 扫描 @Aspect + 5 种通知类型 + Pointcut 切点表达式 + JDK Proxy vs CGLIB 对比 + 同类内部调用绕过代理解决方案（3 种）+ @Transactional 失效 5 场景 + AspectJ vs Spring AOP 对比 + 15 个面试考点 | ⭐⭐⭐⭐⭐ | [14_SpringAOP源码解析.md](14_SpringAOP源码解析.md) |
| 15 | Spring 事务管理深度解析 | TransactionInterceptor.invokeWithinTransaction()（TransactionAspectSupport L336）+ AbstractPlatformTransactionManager.getTransaction() 7 种传播行为源码级处理（REQUIRED 加入 / REQUIRES_NEW 挂起 / NESTED savepoint / SUPPORTS / MANDATORY / NEVER / NOT_SUPPORTED）+ handleExistingTransaction 完整逻辑（L408-476）+ REQUIRED 内层异常 setRollbackOnly() 标记外层回滚传播机制 + TransactionSynchronizationManager ThreadLocal 绑定 Connection + DataSourceTransactionManager.doBegin → setAutoCommit(false) + commit/rollback 完整源码流程 + @Transactional 失效 6 场景 + 15 个面试考点 | ⭐⭐⭐⭐⭐ | [15_Spring事务管理源码解析.md](15_Spring事务管理源码解析.md) |
| 16 | Spring 扩展点全集（自动配置 / BeanPostProcessor / 事件 / 生命周期）| Spring Boot 自动配置（@EnableAutoConfiguration → AutoConfigurationImportSelector → SpringFactoriesLoader 扫描 META-INF/spring.factories → @Conditional 6 种评估条件）+ 自定义 Starter 3 步规范 + BeanPostProcessor 体系（核心接口 + 内置实现清单 + 执行顺序 + AOP/@Autowired 实现原理）+ Spring 事件机制（三要素 + SimpleApplicationEventMulticaster 同步/异步 + @EventListener + @Async）+ Bean 生命周期完整回调链（BeanFactoryPostProcessor → 实例化 → 提前暴露 → @Autowired → Aware → @PostConstruct → InitializingBean → init-method → AOP 代理创建 → @PreDestroy → DisposableBean → destroy-method）+ 20 个面试考点 | ⭐⭐⭐⭐⭐ | [16_Spring扩展点源码解析.md](16_Spring扩展点源码解析.md) |
| 17 | Spring MVC 请求处理链路 | DispatcherServlet.doDispatch 11 步完整源码（checkMultipart → getHandler → getHandlerAdapter → applyPreHandle → ha.handle → applyPostHandle → processDispatchResult）+ HandlerMapping（RequestMappingHandlerMapping 注册时机 + RequestMappingInfo 匹配规则）+ HandlerAdapter（RequestMappingHandlerAdapter supports HandlerMethod）+ HandlerMethodArgumentResolver 参数解析（@RequestParam/@RequestBody/@PathVariable 注册顺序）+ HandlerMethodReturnValueHandler 返回值处理（@ResponseBody → Jackson 序列化）+ HandlerInterceptor 洋葱模型（preHandle 正序 → postHandle/afterCompletion 逆序）+ ViewResolver + @ControllerAdvice/@ExceptionHandler 全局异常处理 + 20 个面试考点 | ⭐⭐⭐⭐⭐ | [17_SpringMVC源码解析.md](17_SpringMVC源码解析.md) |

---

## 🏗️ 知识体系全景图

```
JDK 1.8 核心源码
├── java.lang
│   ├── Object        ─── mark word / wait-notify monitor / clone / finalize
│   ├── String        ─── 不可变性 / Horner 哈希 / intern 常量池
│   ├── Thread        ─── 6 状态机 / start vs run / interrupt 协作式中断 / join
│   └── Class         ─── 反射入口（forName / getDeclaredField / getMethod）
│
├── java.util
│   ├── HashMap           ─── 2^n 容量 / hash 扰动 / resize 拆分 / 红黑树阈值 8
│   ├── ConcurrentHashMap ─── CAS + synchronized 桶锁 / sizeCtl 三重语义 / ForwardingNode
│   ├── ArrayList         ─── 1.5× 扩容 / System.arraycopy / fail-fast
│   ├── LinkedList        ─── 双向链表 / Deque 双端队列
│   └── Collections       ─── unmodifiableList / synchronizedList（装饰器模式）
│
├── java.util.concurrent
│   ├── locks
│   │   ├── AbstractQueuedSynchronizer ─── state + CLH 队列 + CAS（灵魂！）
│   │   ├── ReentrantLock             ├── 非公平锁 NonfairSync（插队 CAS）
│   │   │                             └── 公平锁 FairSync（hasQueuedPredecessors）
│   │   └── ConditionObject          ─── Condition 队列 ↔ Sync 队列转换 / await 六步 / signal 五步
│   ├── atomic
│   │   └── AtomicInteger ─── volatile value + Unsafe.cas 自旋
│   └── CompletableFuture ─── 状态机 / thenApply / thenCompose / handle / exceptionally / allOf / anyOf
│
├── java.io
│   ├── FilterInputStream  ─── 装饰器基类（protected volatile InputStream in + 三参数依赖）
│   ├── BufferedInputStream ─── byte[] buf + fill() 四种场景 + mark/reset 实现
│   ├── DataInputStream    ─── readInt 大端序 / readUTF 静态方法 + 缓冲复用
│   ├── PushbackInputStream ─── unread 回退（buf[--pos] = b）
│   ├── BufferedReader     ─── skipLF 状态机处理 \r\n / readLine / 独立实现缓冲
│   ├── PrintStream        ─── System.out / autoFlush / 吞 IOException + trouble 标志
│   └── ObjectInputStream  ─── 装饰器变体（内部持 PushbackInputStream）
│
├── java.nio
│   └── Buffer ─── 4 索引不变量（mark ≤ pos ≤ limit ≤ cap）/ flip / clear / allocate vs allocateDirect
│
└── java.lang.reflect
    ├── Proxy ─── JDK 动态代理（ProxyGenerator 生成字节码 → InvocationHandler.invoke）
    └── InvocationHandler ─── 拦截器接口（invoke(proxy, method, args)）
```

---

## 🎯 面试高频考点速查表

### 集合框架

| 问题 | 答案 | 源码锚点 |
|------|------|---------|
| HashMap 为什么线程不安全？ | JDK7 头插法并发扩容链表成环；JDK8 虽尾插安全但数据可能丢失 | HashMap.java |
| HashMap 为什么转红黑树阈值 8？ | 泊松分布 P(k=8, λ=0.75) ≈ 0.00000006 | TREEIFY_THRESHOLD |
| HashMap 扩容怎么拆分链表？ | `(hash & oldCap)==0` 留原位，否则偏移 oldCap | resize() |
| ConcurrentHashMap sizeCtl 三重语义？ | -1 初始化；-(N+1) 扩容中；正数阈值 | CHM 内部 |
| CHM 扩容期间怎么 put？ | 看到 ForwardingNode(MOVED) 就 helpTransfer 帮忙迁移 | CHM 内部 |

### 并发基础

| 问题 | 答案 | 源码锚点 |
|------|------|---------|
| synchronized 锁升级完整路径？ | 无锁→偏向锁→轻量级锁→重量级锁（HotSpot 实现） | mark word + monitor |
| volatile 保证什么？ | 可见性 + 禁止指令重排；❌ 不保证原子性 | JMM |
| CAS 三大问题？ | ABA / 自旋浪费 / 单变量限制 | AtomicStampedReference |
| AQS 核心设计？ | `volatile int state` + CLH 双向队列 + CAS | AQS 三大字段 |
| Node.waitStatus 5 种值？ | CANCELLED=1 / SIGNAL=-1 / CONDITION=-2 / PROPAGATE=-3 / 0 | AQS.Node |
| shouldParkAfterFailedAcquire 三次握手？ | 0→CAS=SIGNAL→return false；再一轮 ws=-1→park | AQS |
| 公平锁 vs 非公平锁？ | 非公平 lock() 先来插队 CAS；公平 tryAcquire 先 hasQueuedPredecessors | ReentrantLock.Sync |
| Condition await 六步？ | 入队列→fullyRelease→park→转移到 Sync 队列→acquireQueued→返回 | ConditionObject.await |
| Condition signal 五步？ | 检查持锁→取 first→CAS CONDITION→0→enq→unpark | doSignal + transferForSignal |
| Thread 6 状态机？ | NEW/RUNNABLE/BLOCKED/WAITING/TIMED_WAITING/TERMINATED | Thread.State |
| wait/notify 必须 synchronized？ | 防止丢失唤醒竞态 + wait 要释放锁 | Object.java |

### IO / NIO

| 问题 | 答案 | 源码锚点 |
|------|------|---------|
| FilterInputStream.read(b) 为什么不调 in.read(b)？ | 依赖 read(b,0,len) 的多态分派到子类增强 | L106-108 |
| BufferedInputStream.fill 四种场景？ | A:无 mark 丢；B:markpos>0 前移；C:×2 扩容；D:marklimit 到 mark 失效 | fill() |
| 装饰器 vs 继承 vs 代理？ | 装饰器加功能（继承同类基类）；代理拦截（继承 Proxy/生成子类） | 设计哲学 |
| Buffer 4 索引不变量？ | mark ≤ position ≤ limit ≤ capacity | Buffer.java |
| flip() 做什么？ | 写→读：limit=position, pos=0, mark=-1 | Buffer.flip |

### JVM / GC

| 问题 | 答案 | 源码锚点 |
|------|------|---------|
| JVM 堆分代？ | Young（Eden+S0+S1 ≈1/3）+ Old（≈2/3）+ Metaspace | HotSpot 模型 |
| CMS vs G1 区别？ | CMS 标记-清除（碎片）；G1 Region 化（可预测停顿）| GC 算法 |
| GC Roots 有哪些？ | 局部变量 / 静态变量 / 常量池 / JNI 引用 / monitor / 活跃线程 | HotSpot |

---

## 📂 工作区结构

```
d:\GC_WorkSpaces\jdk-source-analysis\
├── JDK核心源码深度解析_总览索引.md   ← 本文件
├── 01_集合框架源码解析.md
├── 02_并发基础源码解析.md            ← 包含 AQS + Condition await/signal 完整机制
├── 03_IO_NIO体系源码解析.md
├── 04_JVM与GC源码解析.md
├── 05_反射与异步编程.md
└── src\                               ← JDK 1.8.0_451 源码副本
    ├── java\lang\
    ├── java\util\
    ├── java\util\concurrent\locks\
    ├── java\io\
    └── java\nio\
```

---

**第一阶段产出物 · 2026-10-19**
