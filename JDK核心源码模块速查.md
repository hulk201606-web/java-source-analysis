# JDK 1.8.0_451 核心源码 · 模块级速查

> 定位：JDK 标准库是"写业务代码时天天调用、面试时天天被问"的基础设施。本文档**不做逐文件逐方法精注释**（那会让你面对 200+ 文件昏头），而是按"你应该先理解什么、面试会考什么、调用链怎么走"的优先级做模块架构速查。
>
> 策略：✅ 高频类已逐文件精注释 → ✅ 本速查覆盖剩余包宏观脉络 + 调用链图解 + 面试考点

---

## 📋 第一部分：已精注释核心类清单（10 个）

| 包 | 类 | 核心知识点 | 面试权重 |
|----|-----|----------|----------|
| java.lang | Object | JVM 对象布局（mark word+klass+字段）、wait/notify monitor 机制、clone 浅拷贝 | ⭐⭐⭐⭐⭐ |
| java.lang | String | 不可变性 4 大理由、hash Horner 算法（31 乘数）、intern 常量池、substring JDK7 内存泄漏修复 | ⭐⭐⭐⭐⭐ |
| java.lang | Thread | 6 状态机、start() vs run() 本质区别、sleep vs wait vs yield、interrupt 协作式中断 | ⭐⭐⭐⭐⭐ |
| java.util | HashMap | 2^n 容量基石、hash 扰动 `h ^ h>>>16`、resize 拆分 `hash & oldCap`、红黑树阈值泊松分布 | ⭐⭐⭐⭐⭐ |
| java.util | ArrayList | 1.5× 扩容、System.arraycopy、fail-fast 迭代器、Vector 对比 | ⭐⭐⭐⭐ |
| java.util | LinkedList | 双向链表、first/last 指针不变量、Deque 双端队列 | ⭐⭐⭐ |
| java.util.concurrent | ConcurrentHashMap | CAS+sync 桶锁、sizeCtl 多重语义、ForwardingNode、Transfer 协同扩容 | ⭐⭐⭐⭐⭐ |
| java.io | InputStream | 抽象模板方法、FilterInputStream 装饰器基类、mark/reset 机制 | ⭐⭐⭐ |
| java.io | BufferedReader | 装饰器字符缓冲、readLine 行尾处理、mark/reset 字符级实现 | ⭐⭐⭐ |
| java.io | InputStreamReader | 桥接模式 byte→char、StreamDecoder 内部结构 | ⭐⭐⭐ |

**文件位置**：`d:\GC_WorkSpaces\jdk-source-analysis\src\` 下对应子包

---

## 🌐 第二部分：剩余包模块级速查（宏观架构）

### 一、java.net（86 文件）· 网络编程

#### 核心类族谱

```
java.net
├─ InetAddress           ← IP 地址抽象（Inet4Address / Inet6Address）
│   └─ getByName("www.baidu.com") → DNS 解析 → InetAddress
│
├─ Socket（客户端 TCP） / ServerSocket（服务端 TCP）
│   └─ 内部持有 Socket → SocketImpl（抽象） → 平台实现
│       ├─ DualStackPlainSocketImpl    → Windows/Linux 通用实现
│       ├─ TwoStacksPlainSocketImpl    → Solaris 双栈实现
│       └─ SocksSocketImpl             → SOCKS 代理实现
│
├─ DatagramSocket（UDP） / DatagramPacket
│   └─ 无连接，每次 send/receive 独立报文
│
├─ URL / URI（统一资源定位）
│   ├─ URLConnection    → openConnection() 抽象基类
│   │   ├─ HttpURLConnection   → HTTP 协议
│   │   └─ JarURLConnection    → JAR 协议
│   └─ URLClassLoader   → 从 URL 加载类（热部署、远程加载）
│
└─ CookieManager / ProxySelector / Authenticator ← HTTP 辅助
```

#### 面试考点

| 考点 | 核心答案 |
|------|---------|
| TCP 三次握手在哪层实现 | JVM `Socket` 调用 OS `connect()` 系统调用，内核 TCP 协议栈完成三次握手 |
| Socket 和 ServerSocket 底层 | 都是 native 方法，HotSpot 调用 `libnet.so`（Linux）/ `net.dll`（Windows）|
| URL vs URI | URI 是抽象（http://example.com/path?query），URL 是具体可定位的 URI 子集 |
| DatagramSocket 为什么丢包 | UDP 无连接、不保证送达、OS 缓冲区满直接丢弃（应用层自行重传）|

---

### 二、java.nio（81 文件）· 新 IO（非阻塞）

#### Buffer 继承体系（核心抽象）

```
Buffer（位置管理：position / limit / capacity / mark）
  ├─ ByteBuffer     ← 最常用，JVM/OS 直接操作字节
  │   ├─ HeapByteBuffer      → 堆内存（GC 管理）
  │   ├─ DirectByteBuffer    → 堆外内存（native 分配，不受 GC 压力）
  │   └─ MappedByteBuffer    → 内存映射文件（mmap，文件直接映射到内存）
  ├─ CharBuffer / ShortBuffer / IntBuffer / LongBuffer / FloatBuffer / DoubleBuffer
  │   └─ 可由 ByteBuffer 视图创建（asIntBuffer() 等）
  └─ StringCharBuffer        → 轻量 CharBuffer 包装 String
```

#### 四个关键状态

```
缓冲区状态转换：
  write 模式 ── flip() ──▶ read 模式
  ┌─────────────────────────────────────────┐
  │ capacity = 总容量                       │
  │ position = 当前读写位置                 │
  │ limit    = 读写边界                     │
  ├─────────────────────────────────────────┤
  │ clear()  → position=0, limit=capacity   │ ← 准备写入
  │ flip()   → limit=position, position=0   │ ← 准备读取
  │ rewind() → position=0（limit 不变）     │ ← 重读一遍
  │ compact()→ 未读数据前移，为后续写入腾空间 │ ← 读/写混合场景
  └─────────────────────────────────────────┘
```

#### 面试考点

| 考点 | 核心答案 |
|------|---------|
| DirectByteBuffer vs HeapByteBuffer | Direct 用堆外内存（不受 GC 压力，但分配释放慢）；Heap 用堆内存（GC 管理，但每次 IO 可能拷贝）|
| MappedByteBuffer 是什么 | OS mmap() 内存映射文件，文件操作直接在内存中完成，无需 read/write 系统调用 |
| NIO 非阻塞怎么实现 | Selector + Channel，单线程管理多路连接（epoll on Linux / kqueue on macOS / select on Windows）|
| Buffer 的状态翻转 | flip() = limit=position, position=0；clear() = position=0, limit=capacity |

---

### 三、java.math（8 文件）· 高精度数值

```
java.math
├─ BigInteger          ← 任意精度整数（支持加/减/乘/除/模/幂/素性测试）
│   ├─ 内部用 int[] 数组存储大整数（每格存一个 32-bit chunk）
│   ├─ 乘法：朴素 O(n²) / Karatsuba O(n^1.585) / Toom-Cook O(n^1.465)
│   ├─ Miller-Rabin 概率素性测试
│   └─ BitSieve（埃拉托色尼筛法预生成小素数表）
│
├─ BigDecimal           ← 任意精度定点数（用于金融计算）
│   ├─ 内部 = BigInteger unscaledValue + int scale（小数点位置）
│   ├─ 除法必须指定 RoundingMode（否则可能无限循环小数）
│   └─ MathContext（精度 + 舍入模式）
│
├─ RoundingMode         ← 8 种舍入策略（HALF_UP/HALF_DOWN/HALF_EVEN/CEILING/FLOOR 等）
├─ MathContext          ← 上下文（precision + roundingMode）
└─ MutableBigInteger    ← 内部可变实现（性能优化，不对外暴露）
```

#### 面试考点

| 考点 | 核心答案 |
|------|---------|
| 为什么金融计算要用 BigDecimal | float/double 是 IEEE 754 二进制浮点，0.1+0.2≠0.3；BigDecimal 十进制定点精确 |
| BigDecimal 除法为什么要指定 RoundingMode | 1/3=0.3333... 无限循环，必须告诉它怎么舍入 |
| HALF_UP vs HALF_EVEN | UP 四舍五入；EVEN 银行家舍入（向最近偶数舍入，减少累计偏差，金融首选）|

---

### 四、java.util.concurrent（59 文件）· 并发工具包

#### 体系分层

```
                ┌──────────────────────────────┐
                │     Executor 框架层          │
                │  Executor / ExecutorService  │
                │  ThreadPoolExecutor           │
                │  ScheduledThreadPoolExecutor │
                │  ForkJoinPool (JDK7+ 并行)   │
                └─────────────┬────────────────┘
                              │ 提交任务
                ┌─────────────▼────────────────┐
                │      并发集合层              │
                │  ConcurrentHashMap  ✅已注释  │
                │  ConcurrentSkipListMap/Set    │
                │  CopyOnWriteArrayList/Set    │
                │  ConcurrentLinkedQueue/Deque │
                │  ArrayBlockingQueue (有界)    │
                │  LinkedBlockingQueue (无界)   │
                │  PriorityBlockingQueue       │
                │  DelayQueue / SynchronousQueue│
                └─────────────┬────────────────┘
                              │ 线程间协作
                ┌─────────────▼────────────────┐
                │      同步工具层              │
                │  CountDownLatch (倒计时门闩)  │
                │  CyclicBarrier (循环屏障)     │
                │  Semaphore (信号量)           │
                │  Phaser (阶段同步, JDK7+)    │
                │  Exchanger (数据交换)         │
                └─────────────┬────────────────┘
                              │ 线程本地
                ┌─────────────▼────────────────┐
                │      Future / CompletableFuture│
                │  FutureTask / CompletionStage │
                └──────────────────────────────┘
```

#### 线程池（ThreadPoolExecutor）核心参数

```java
public ThreadPoolExecutor(
    int corePoolSize,       // 核心线程数（始终保持）
    int maximumPoolSize,    // 最大线程数
    long keepAliveTime,     // 非核心线程空闲存活时间
    TimeUnit unit,
    BlockingQueue<Runnable> workQueue,   // 任务队列
    ThreadFactory threadFactory,          // 线程创建方式
    RejectedExecutionHandler handler      // 拒绝策略
)
```

#### 拒绝策略 4 选 1

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| AbortPolicy（默认） | 直接抛 RejectedExecutionException | 需要感知拒绝并处理 |
| CallerRunsPolicy | 调用者线程自己执行 | 不丢任务但阻塞提交者 |
| DiscardPolicy | 静默丢弃 | 不重要的日志/监控任务 |
| DiscardOldestPolicy | 丢弃队列最老任务再重试 | 追求新任务优先 |

#### 同步工具对比

| 工具 | 作用 | 典型场景 |
|------|------|---------|
| CountDownLatch | N 个线程都完成后继续 | 并行计算等所有子任务 |
| CyclicBarrier | N 个线程互相等待后一起出发 | 多人游戏同时开始、流水线阶段同步 |
| Semaphore | 控制并发数 | 数据库连接池限流 |
| Phaser | 可动态增减参与者的屏障 | 动态并行任务 |
| Exchanger | 两个线程交换数据 | 生产者-消费者直接交换缓冲区 |

---

### 五、java.lang.reflect（32 文件）· 反射

```
反射核心对象：
  Class          ← 类元数据（getDeclaredField/Method/Constructor）
  Field          ← 字段反射（set/get 支持 private 突破）
  Method         ← 方法反射（invoke(obj, args...) 动态调用）
  Constructor    ← 构造器反射（newInstance() 创建实例）
  Parameter      ← 方法参数元数据（JDK8+ 需 -parameters 编译参数）

动态代理（Proxy）：
  InvocationHandler.invoke(proxy, method, args) → 拦截所有方法调用
  实现原理：Proxy.newProxyInstance() 动态生成代理类字节码
           → ProxyGenerator.generateProxyClass()（sun.misc 内部）
           → 字节码加载进 JVM → 返回代理实例

访问控制突破：
  AccessibleObject.setAccessible(true) → 绕过 private 检查
  需 RuntimePermission("accessDeclaredMembers")
```

#### 面试考点

| 考点 | 核心答案 |
|------|---------|
| 反射为什么慢 | setAccessible(true) 仍有安全检查开销；invoke() 需参数装箱拆箱；无法被 JIT 内联 |
| Proxy 动态代理怎么实现 | 运行时生成代理类 .class 字节码（ASM/Javassist 等库也做这件事），实现目标接口，所有方法转发给 InvocationHandler |
| getDeclaredMethod vs getMethod | getDeclaredMethod 只拿本类声明的；getMethod 拿 public（含继承的）|
| 反射能突破 private 吗 | 能（setAccessible(true)），但 JDK 17+ 强封装默认禁止，需 `--add-opens` 或 `--add-exports` |

---

## 🔗 第三部分：高频调用链图解

### 1. HashMap.put() 全链路

```
map.put("key", value)
 │
 ├─ hash(key)                       ← h ^ (h >>> 16)
 ├─ (n - 1) & hash                  ← 计算桶索引（n 是容量）
 │
 ├─ 桶为空 → newNode() CAS 写入     ← 单线程 putVal 直接创建
 ├─ 桶有值
 │   ├─ 头节点 hash/key 匹配 → 替换
 │   ├─ 链表 → 遍历，尾插；达到 TREEIFY_THRESHOLD 且 n≥MIN_TREEIFY_CAPACITY → 转红黑树
 │   └─ 红黑树 → putTreeVal()
 │
 └─ 插入后 size++
     └─ size > threshold → resize()
         ├─ 新容量 × 2
         └─ 遍历所有桶，(hash & oldCap) 判断新位置
```

### 2. 线程池 execute() 任务调度

```
pool.execute(runnable)
 │
 ├─ runningWorkerCount < corePoolSize
 │   └─ addWorker() → 创建核心线程，立即执行任务
 ├─ 否则 → workQueue.offer() 入队等待
 │   ├─ 入队成功 → 检查运行状态
 │   └─ 入队失败（队列满）
 │       └─ runningWorkerCount < maximumPoolSize
 │           └─ addWorker() → 创建非核心线程
 │           └─ 达到 maximumPoolSize → handler.rejectedExecution() 拒绝策略
 │
 └─ Worker 线程循环：getTask() → runTask() → afterExecute()
```

### 3. IO 链从文件读一行

```
Files.newBufferedReader(path, "UTF-8")
 │
 ├─ new FileInputStream(path)              ← native open() 系统调用打开文件
 │   └─ FileChannel.open()                 ← NIO 封装（JDK11+）
 │
 ├─ new InputStreamReader(fis, "UTF-8")   ← StreamDecoder 桥接
 │   ├─ fis.read() → 读 byte[]
 │   └─ CharsetDecoder.decode() → 转为 char[]
 │
 └─ new BufferedReader(reader, 8192)      ← 8KB 字符缓冲
     └─ readLine()
         ├─ cb 缓冲区检查（是否需要从底层 reader.fill()）
         ├─ 逐字符扫描 '\n' 或 '\r'
         └─ 找到换行 → new String(cb, start, len) 返回

性能：8KB 缓冲合并多次小 read → 大幅减少系统调用次数
```

---

## 🎯 第四部分：面试考点速查（完整版）

| 考点 | 涉及包/类 | 关键答案 |
|------|-----------|---------|
| HashMap 扩容机制 | java.util.HashMap | 2× 扩容，链表拆 lo/hi 两段，(hash & oldCap) 判断新位置 |
| HashMap 为什么转红黑树 | java.util.HashMap | 泊松分布 P(k=8)≈0，恶意哈希攻击保底 O(log n) |
| HashMap 为什么线程不安全 | java.util.HashMap | JDK7 头插法并发扩容链表成环死循环；JDK8 虽尾插安全但数据仍可能丢失 |
| ConcurrentHashMap 并发控制 | java.util.concurrent | JDK8 CAS+sync 桶锁，sizeCtl 多语义，ForwardingNode 协同扩容 |
| ArrayList vs LinkedList | java.util | 数组 O(1) 随机访问 + O(n) 插入删除；双向链表 O(1) 头尾插入 + O(n) 随机访问 |
| String 不可变 4 理由 | java.lang.String | 常量池复用、HashMap key 安全、常量折叠、线程安全 |
| start() vs run() | java.lang.Thread | start() 调 OS 创建新线程；run() 是普通方法调用 |
| wait vs sleep vs yield | java.lang | wait 释放锁；sleep 不释放；yield 让出时间片 |
| volatile 做什么 | java.util.concurrent | 可见性（CPU 缓存失效协议）+ 禁止指令重排；不保证原子性 |
| synchronized 锁升级 | java.lang.Object | 无锁 → 偏向锁 → 轻量级锁 → 重量级锁（HotSpot 自适应）|
| ThreadLocal 内存泄漏 | java.lang.Thread | ThreadLocalMap key 是弱引用 value 是强引用；线程池复用线程导致 value 无法 GC |
| 线程池核心参数 | java.util.concurrent | 核心线程数、最大线程数、空闲存活时间、任务队列、拒绝策略 |
| 拒绝策略 4 种 | java.util.concurrent | Abort(抛异常)/CallerRuns(自己跑)/Discard(丢)/DiscardOldest(丢最老)|
| CountDownLatch vs CyclicBarrier | java.util.concurrent | Latch 一次性计数；Barrier 循环可复用 |
| volatile CAS ABA 问题 | java.util.concurrent.atomic | 需 AtomicStampedReference 加时间戳解决 |
| DirectByteBuffer vs HeapByteBuffer | java.nio | Direct 堆外内存（零拷贝但分配慢）；Heap 堆内存（GC 管理但拷贝）|
| Buffer flip() 翻转 | java.nio | limit=position, position=0（从写模式切读模式）|
| 反射为什么慢 | java.lang.reflect | 安全检查、参数装箱、无法 JIT 内联；方法句柄 MethodHandles 更快 |
| Proxy 动态代理原理 | java.lang.reflect | 运行时生成代理类字节码，实现接口，转发 InvocationHandler |
| BigDecimal 用 HALF_EVEN | java.math | 银行家舍入，向最近偶数舍入，减少累计偏差 |
| TCP 三次握手在哪实现 | java.net | OS 内核 TCP 协议栈完成三次握手，JVM 只是调 connect() 系统调用 |
| equals 5 契约 | java.lang.Object | 自反/对称/传递/一致/与 null 比返回 false |
| hashCode 契约 | java.lang.Object | equals 相等 → hashcode 必须相等；反之不成立 |
| finalize 为什么废弃 | java.lang.Object | 时机不确定、性能差、异常被吞；用 try-with-resources |
| Java 选 31 做 hash 乘数 | java.lang.String | 31*i ≈ (i<<5)-i 位运算快；奇素数哈希分布好 |
