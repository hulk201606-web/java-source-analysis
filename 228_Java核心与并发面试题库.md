# 228 · Java 核心与并发高频面试题库（45 题 · 详解）

> **方向**：集合、并发基础、AQS/线程池、JMM、JVM/GC  
> **面试权重**：⭐⭐⭐⭐⭐（Java 后端必问）  
> **配套源码文档**：[004_集合框架源码解析.md](004_集合框架源码解析.md)、[005_并发基础源码解析.md](005_并发基础源码解析.md)、[009_AQS共享工具类源码解析.md](009_AQS共享工具类源码解析.md)、[007_JVM与GC源码解析.md](007_JVM与GC源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问。

---

## 一、集合框架（Q1–Q12）

### Q1. HashMap 1.8 的底层结构是什么？

**结论**：数组 + 链表 + 红黑树，链表长度达到 8 且容量 ≥ 64 时转红黑树。

**机制**：
- `Node[] table` 作为桶数组，容量始终是 2 的幂。
- 每个桶先挂链表，冲突多时树化。
- 红黑树节点是 `TreeNode`，同时维护双向链表便于拆分。
- 负载因子默认 0.75，超过阈值扩容。

**追问**：为什么用红黑树而不是 AVL？红黑树旋转/染色更少，综合插入删除性能更好。

### Q2. HashMap 的 put 流程是怎样的？

**结论**：计算 hash → 定位桶 → 空桶直接放 → 冲突则链表/树插入 → 判断树化 → 检查扩容。

**机制**：
- `hash(key)` 扰动后与 `(n-1)` 做与运算定位。
- 桶为空则 CAS/直接赋值（非并发场景）。
- 链表尾插，树节点按 hash/Comparable 插入。
- 插入后 `size++`，超过阈值触发 resize。

**追问**：1.8 为什么改尾插？头插在扩容时可能形成环，尾插避免该问题并便于树化。

### Q3. HashMap 扩容时元素如何迁移？

**结论**：容量翻倍，利用 `hash & oldCap` 判断元素留在原索引还是移动到“原索引 + oldCap”。

**机制**：
- 2 的幂扩容后，元素新索引只多一个高位。
- 低位链保持原位置，高位链迁到新位置。
- 红黑树按同样规则拆分，必要时退化为链表。
- 避免重新计算完整 hash。

**追问**：为什么容量必须是 2 的幂？便于用位运算取模，并让扩容拆分规则简单高效。

### Q4. HashMap 的 hash 扰动为什么要异或高 16 位？

**结论**：让高位参与索引计算，减少低位相同导致的冲突。

**机制**：
- 索引只取 hash 的低位（`n-1` 掩码）。
- `h ^ (h >>> 16)` 把高 16 位混入低位。
- 计算成本低，冲突分布更均匀。
- 1.8 简化了 1.7 的多次扰动。

**追问**：扰动能完全避免冲突吗？不能，只能改善分布，冲突仍由链表/红黑树处理。

### Q5. 为什么树化阈值是 8、退化阈值是 6？

**结论**：基于泊松分布，链表长度达到 8 的概率极低；6 与 8 之间留缓冲避免频繁转换。

**机制**：
- 正常情况下 hash 均匀，长链表很少出现。
- 树化还要满足容量 ≥ 64，否则优先扩容。
- 退化到 6 避免在 7/8 边界反复树化。
- 树节点占用空间更大，不适合短链表。

**追问**：容量不足 64 时链表很长怎么办？优先扩容而不是树化，扩容能打散冲突。

### Q6. HashMap 线程安全吗？并发 put 会有什么问题？

**结论**：不安全，可能出现数据覆盖、size 不准，1.7 还可能扩容成环；并发场景用 ConcurrentHashMap。

**机制**：
- 多线程同时写同一桶会互相覆盖。
- `size++` 非原子，统计失真。
- 扩容时多线程迁移可能破坏链表。
- 1.8 修复了成环，但仍非线程安全。

**追问**：用 `Collections.synchronizedMap` 可以吗？可以但全局锁，性能差；CHM 更合适。

### Q7. ConcurrentHashMap 1.8 的结构与并发控制是什么？

**结论**：Node 数组 + 链表/红黑树，空桶用 CAS 插入，非空桶用 synchronized 锁桶头。

**机制**：
- 锁粒度是单个桶，不是整个表。
- 空桶插入通过 CAS 竞争。
- 链表/树操作锁住首节点。
- 扩容支持多线程协助迁移（ForwardingNode）。

**追问**：为什么不用分段锁？1.8 锁粒度更细，并发度更高，内存占用更小。

### Q8. ConcurrentHashMap 的 sizeCtl 有哪些含义？

**结论**：控制表初始化、扩容与当前扩容线程数，是 CHM 并发协调的核心字段。

**机制**：
- 0：未初始化。
- -1：正在初始化。
- 负数：正在扩容，绝对值表示参与线程数。
- 正数：下一次扩容阈值。
- 通过 CAS 修改，协调多线程。

**追问**：多线程如何协助扩容？看到 ForwardingNode 的线程可加入迁移，按 stride 领取桶区间。

### Q9. ArrayList 的扩容机制是怎样的？

**结论**：默认容量 10（首次 add 时分配），扩容为原来的 1.5 倍，用 `Arrays.copyOf` 复制。

**机制**：
- `grow` 计算新容量 `old + (old >> 1)`。
- 复制旧数组到新数组。
- 扩容是 O(n)，频繁扩容影响性能。
- 可预设容量避免多次复制。

**追问**：为什么是 1.5 倍而不是 2 倍？折中空间浪费与扩容次数，减少内存突增。

### Q10. fail-fast 是什么？如何实现？

**结论**：迭代过程中检测到结构修改就抛 `ConcurrentModificationException`，通过 modCount 校验。

**机制**：
- 集合维护 `modCount`，结构修改时递增。
- 迭代器保存期望值 `expectedModCount`。
- 每次 `next` 比较，不一致则抛出。
- 这是尽力检测，不保证并发下一定触发。

**追问**：如何安全删除？用迭代器 `remove()`，或改用并发集合/收集后统一删除。

### Q11. LinkedHashMap 如何实现 LRU？

**结论**：`accessOrder=true` 时按访问顺序维护双向链表，重写 `removeEldestEntry` 淘汰最旧节点。

**机制**：
- 节点继承 HashMap Node 并增加 before/after。
- `get` 触发 `afterNodeAccess` 把节点移到尾部。
- `put` 触发 `afterNodeInsertion` 判断淘汰。
- 淘汰 head 节点是 O(1)。

**追问**：LinkedHashMap 线程安全吗？不安全，生产用 Caffeine/Guava Cache。

### Q12. TreeMap 与跳表有什么区别？为什么并发场景用跳表？

**结论**：TreeMap 是红黑树、单线程；ConcurrentSkipListMap 用跳表 + CAS，支持无锁并发。

**机制**：
- 红黑树插入删除需要旋转/染色，并发实现复杂。
- 跳表多层索引，查找/插入 O(log n) 期望。
- 跳表节点可 CAS 插入，易于无锁实现。
- 并发场景 CHM 已足够，跳表适合有序并发访问。

**追问**：跳表为什么用随机层级？用概率平衡代替严格平衡，降低实现复杂度。

---

## 二、并发基础（Q13–Q24）

### Q13. Java 线程有哪些状态？

**结论**：NEW、RUNNABLE、BLOCKED、WAITING、TIMED_WAITING、TERMINATED 六种。

**机制**：
- NEW：创建未 start。
- RUNNABLE：可运行/运行中（含 IO 等待）。
- BLOCKED：等待 monitor 锁。
- WAITING：无限等待 notify/join/park。
- TIMED_WAITING：带超时等待。
- TERMINATED：执行结束。

**追问**：RUNNABLE 包含 IO 阻塞吗？包含，JVM 不区分可运行与等待 CPU。

### Q14. synchronized 的底层实现是什么？

**结论**：通过对象 monitor 实现，字节码 monitorenter/monitorexit，锁状态从轻量级膨胀到重量级。

**机制**：
- 同步块编译为 monitorenter/monitorexit。
- 同步方法用 ACC_SYNCHRONIZED 标记。
- 锁信息在对象头 mark word。
- 竞争激烈时膨胀为 ObjectMonitor，进入等待队列。

**追问**：synchronized 能保证可见性吗？能，解锁前的写会刷新到主内存，加锁会失效本地缓存。

### Q15. volatile 的语义是什么？

**结论**：保证可见性与有序性，不保证原子性。

**机制**：
- 写 volatile 会插入 StoreLoad 屏障，刷新到主内存。
- 读 volatile 会从主内存重新读取。
- 禁止指令重排到屏障前后。
- `i++` 这类复合操作仍非原子。

**追问**：volatile 能替代锁吗？只能用于状态标记/单次读写，复合操作仍需 CAS 或锁。

### Q16. CAS 是什么？ABA 问题怎么解决？

**结论**：比较并交换的原子指令；ABA 用版本号/时间戳解决。

**机制**：
- CAS 比较内存值与期望值，相等才写入新值。
- 底层是 cmpxchg 指令，配合锁前缀保证原子。
- ABA：值从 A→B→A，CAS 误以为没变。
- 解决：AtomicStampedReference 加版本号。

**追问**：CAS 有什么缺点？自旋消耗 CPU、只能保证单个变量、ABA 风险。

### Q17. AQS 的核心原理是什么？

**结论**：一个 volatile state + CLH 双向等待队列 + CAS，子类定义 state 的获取/释放语义。

**机制**：
- `state` 表示锁/信号量/计数器状态。
- 获取失败入队，前驱唤醒后继。
- 独占模式与共享模式两套逻辑。
- `LockSupport.park/unpark` 阻塞与唤醒。

**追问**：AQS 为什么用双向队列？便于取消节点与从尾部查找有效前驱。

### Q18. ReentrantLock 公平锁与非公平锁有什么区别？

**结论**：公平锁按队列顺序获取，非公平锁允许插队；非公平吞吐更高但有饥饿风险。

**机制**：
- 公平锁 `hasQueuedPredecessors` 检查前面是否有等待者。
- 非公平锁直接 CAS 抢锁，失败再入队。
- 两者都支持可重入，state 递增。
- 默认非公平。

**追问**：为什么非公平更快？减少线程唤醒与上下文切换，插队能提高吞吐。

### Q19. Condition 的实现原理是什么？

**结论**：基于 AQS 的条件队列，await 释放锁并挂起，signal 把节点移回同步队列。

**机制**：
- 每个 Condition 维护一个等待队列。
- `await` 释放锁、入条件队列、park。
- `signal` 把节点从条件队列转到同步队列。
- 唤醒后需重新竞争锁。

**追问**：Condition 与 Object.wait/notify 区别？Condition 可多队列、可超时、更灵活。

### Q20. ThreadLocal 的原理与内存泄漏原因是什么？

**结论**：每个 Thread 持有 ThreadLocalMap，key 是弱引用的 ThreadLocal，value 强引用；key 被回收后 value 可能泄漏。

**机制**：
- `set` 以 ThreadLocal 为 key 存入当前线程 map。
- Entry key 是 WeakReference。
- 线程池复用线程时 map 不清理，value 长期存活。
- 使用后必须 `remove()`。

**追问**：为什么 key 用弱引用？让 ThreadLocal 对象可被回收，但 value 仍需手动清理。

### Q21. 线程池的执行流程是什么？

**结论**：核心线程未满则建核心线程 → 入队列 → 队列满则建非核心线程 → 达到最大线程则执行拒绝策略。

**机制**：
- `corePoolSize` 控制常驻线程。
- `workQueue` 缓冲任务。
- `maximumPoolSize` 控制峰值线程。
- `RejectedExecutionHandler` 处理过载。

**追问**：使用无界队列会怎样？最大线程数失效，任务无限堆积，可能 OOM。

### Q22. 线程池的拒绝策略有哪些？

**结论**：AbortPolicy、CallerRunsPolicy、DiscardPolicy、DiscardOldestPolicy，也可自定义。

**机制**：
- Abort：抛 RejectedExecutionException，默认。
- CallerRuns：由提交线程执行，形成反压。
- Discard：静默丢弃。
- DiscardOldest：丢最旧任务再尝试提交。

**追问**：生产推荐哪个？通常自定义并记录指标；CallerRuns 可用于降速，但会阻塞提交线程。

### Q23. 线程池大小如何设置？

**结论**：CPU 密集型约 N+1，IO 密集型可大于 N，但最终以压测与监控为准。

**机制**：
- CPU 型线程过多会增加上下文切换。
- IO 型线程大部分时间等待，可提高并发。
- 可用 `N * (1 + 等待时间/计算时间)` 估算。
- 结合队列、下游容量与超时。

**追问**：线程越多吞吐越高吗？不是，超过瓶颈会增加切换与内存压力。

### Q24. CompletableFuture 的常用编排方式有哪些？

**结论**：thenApply/thenCompose/thenCombine、allOf/anyOf、exceptionally/handle，支持异步链与异常处理。

**机制**：
- `thenApply` 转换结果。
- `thenCompose` 扁平化嵌套 Future。
- `allOf` 等全部完成，`anyOf` 任一完成。
- 默认 commonPool，阻塞任务应指定 Executor。

**追问**：CompletableFuture 能取消吗？`cancel` 只在未完成时有效，底层任务不一定被中断。

---

## 三、并发工具与内存模型（Q25–Q32）

### Q25. CountDownLatch 与 CyclicBarrier 的区别是什么？

**结论**：CountDownLatch 一次性倒计数，CyclicBarrier 可循环等待一组线程到达。

**机制**：
- CountDownLatch 基于 AQS 共享模式，count 到 0 释放。
- CyclicBarrier 用 ReentrantLock + Condition，支持 reset 复用。
- Latch 主线程等待子任务完成。
- Barrier 多线程互相等待后继续。

**追问**：CountDownLatch 能重置吗？不能，需要新建；CyclicBarrier 可以 reset。

### Q26. Semaphore 如何控制并发度？

**结论**：用 AQS state 表示许可数，acquire 减一，release 加一，支持公平/非公平。

**机制**：
- `acquire` 获取许可，无许可则排队。
- `release` 归还许可并唤醒等待者。
- 可用于限流、连接池、资源池。
- 公平模式按队列顺序分配。

**追问**：Semaphore 与线程池限流的区别？Semaphore 控制并发进入数，不管理线程生命周期。

### Q27. 常见的阻塞队列有哪些？

**结论**：ArrayBlockingQueue、LinkedBlockingQueue、PriorityBlockingQueue、DelayQueue、SynchronousQueue、LinkedTransferQueue。

**机制**：
- Array 有界数组 + 单锁/双条件。
- Linked 可选有界，双锁提高吞吐。
- Priority 无界优先堆。
- Delay 按到期时间出队。
- Synchronous 不存储，直接交接。
- TransferQueue 支持生产者等待消费者。

**追问**：线程池为什么常用 LinkedBlockingQueue？吞吐高，但默认无界需谨慎。

### Q28. ForkJoinPool 的工作原理是什么？

**结论**：工作窃取，每个线程有双端队列，自己取头部，空闲线程从其他队列尾部偷任务。

**机制**：
- fork 把任务放入当前线程队列。
- join 等待结果，必要时帮助执行。
- 空闲线程扫描其他队列偷任务。
- 适合可递归分治的计算型任务。

**追问**：ForkJoin 适合 IO 吗？不适合，阻塞会占用工作线程；IO 用普通线程池或虚拟线程。

### Q29. ConcurrentSkipListMap 为什么适合并发有序场景？

**结论**：跳表 + CAS 实现无锁读和细粒度写，支持高并发有序访问。

**机制**：
- 多层索引加速查找。
- 插入用 CAS 链接节点。
- 删除分步并帮助清理。
- 比红黑树更易实现无锁并发。

**追问**：什么时候用它？需要有序、并发、范围查询，且不希望全局锁。

### Q30. JMM 与 happens-before 是什么？

**结论**：JMM 定义多线程读写共享变量的可见性与有序性规则，happens-before 描述跨线程可见关系。

**机制**：
- 程序顺序规则、监视器锁规则、volatile 规则。
- 线程 start/join 规则。
- 传递性。
- 满足 happens-before 不代表执行顺序固定，只保证可见性。

**追问**：happens-before 能保证原子性吗？不能，只保证可见性与部分有序。

### Q31. 指令重排与内存屏障有什么关系？

**结论**：编译器和 CPU 可重排指令提升性能，内存屏障限制重排并保证可见性。

**机制**：
- 编译器重排、CPU 乱序执行、缓存可见性。
- LoadLoad/StoreStore/LoadStore/StoreLoad 屏障。
- volatile 写插入 StoreLoad 屏障。
- final 字段有特殊初始化保证。

**追问**：单例为什么要双重检查 + volatile？防止指令重排导致拿到未初始化对象。

### Q32. 伪共享是什么？如何避免？

**结论**：多个线程修改同一缓存行中的不同变量，导致缓存行反复失效；用填充/对齐隔离。

**机制**：
- CPU 缓存行通常 64 字节。
- 同一行被多核频繁写会互相失效。
- 解决：字段填充、`@Contended`、线程本地变量。
- Disruptor、LongAdder 都有相关优化。

**追问**：伪共享常见于哪里？高并发计数器、环形队列、状态标志。

---

## 四、JVM 与 GC（Q33–Q40）

> **主题归属**：本节是 Java 层面试视角；HotSpot C++ 实现见 [224 Q15–Q24](224_JVM底层C++面试题库.md)，JIT 优化与去优化见 [226](226_JIT编译器面试题库.md)。

### Q33. JVM 运行时内存区域有哪些？

**结论**：堆、方法区/Metaspace、虚拟机栈、本地方法栈、程序计数器；堆与 Metaspace 线程共享，其余线程私有。

**机制**：
- 堆存对象实例。
- Metaspace 存类元数据。
- 栈存栈帧、局部变量、操作数栈。
- PC 记录当前字节码位置。
- 直接内存不属于 JVM 规范区域。

**追问**：字符串常量池在哪？JDK 7 起移到堆中。

### Q34. 对象创建与分配的过程是什么？

**结论**：类加载检查 → 分配内存 → 初始化零值 → 设置对象头 → 执行构造方法。

**机制**：
- 检查类是否已加载。
- TLAB/指针碰撞或空闲列表分配。
- 内存清零，设置 mark word 与 Klass 指针。
- 执行 `<init>` 初始化字段。

**追问**：分配一定在 Eden 吗？多数在 Eden/TLAB，大对象可能直接老年代。

### Q35. GC Roots 有哪些？

**结论**：线程栈局部变量、静态字段、常量、JNI 引用、活动线程、同步锁对象等。

**机制**：
- 从 Roots 出发做可达性分析。
- 不可达对象才可回收。
- 引用类型影响回收时机（强/软/弱/虚）。
- 循环引用不影响可达性判断。

**追问**：为什么不用引用计数？循环引用无法回收，维护成本高。

### Q36. 常见 GC 算法有哪些？

**结论**：标记-清除、复制、标记-整理、分代收集。

**机制**：
- 标记-清除产生碎片。
- 复制适合存活率低的新生代。
- 标记-整理适合老年代，避免碎片。
- 分代按对象生命周期采用不同算法。

**追问**：为什么新生代用复制？存活对象少，复制成本低。

### Q37. G1 的核心设计与回收流程是什么？

**结论**：Region 化堆，按回收收益选择 Region，Young GC + Mixed GC + 并发标记。

**机制**：
- Region 大小 1–32MB。
- 并发标记用 SATB 保快照。
- 按停顿目标选择回收 Region。
- Humongous 存大对象。

**追问**：G1 一定比 CMS 好吗？多数场景更好，但小堆或特定负载需实测。

### Q38. CMS 的流程与问题是什么？

**结论**：初始标记、并发标记、重新标记、并发清除；问题是碎片与并发模式失败。

**机制**：
- 初始/重新标记 STW。
- 并发标记与清除占 CPU。
- 不压缩产生碎片。
- Full GC 退化严重。

**追问**：CMS 为什么被移除？维护成本高、碎片不可控，G1/ZGC 替代。

### Q39. 类加载过程与双亲委派是什么？

**结论**：加载、验证、准备、解析、初始化；双亲委派让父加载器优先加载，保证类唯一与安全。

**机制**：
- 加载生成 Class 对象。
- 验证格式与语义。
- 准备分配静态变量并设零值。
- 解析符号引用。
- 初始化执行 `<clinit>`。
- 委派失败才自己加载。

**追问**：如何打破双亲委派？重写 loadClass，如 Tomcat、SPI、OSGi。

### Q40. 线上 OOM/GC 问题如何排查？

**结论**：先看 GC 日志与堆/线程快照，再定位对象、泄漏根与线程池，最后调参与修代码。

**机制**：
- `jstat`/GC 日志看频率、耗时、老年代增长。
- `jmap`/`jcmd` 导出堆，MAT 分析 dominator tree。
- `jstack` 看线程阻塞与死锁。
- NMT 看堆外内存。
- 参数与代码优化后回归压测。

**追问**：OOM 一定是堆吗？不一定，还有 Metaspace、直接内存、线程栈、句柄泄漏。

---

## 五、进阶补充（Q41–Q45）

### Q41. String 为什么不可变？intern 有什么用？

**结论**：String 内部字符数组不可修改，保证线程安全、可缓存哈希与常量池复用；intern 把字符串放入常量池。

**机制**：
- 类与字段 final，避免子类与反射修改（正常语义下）。
- 不可变使 hash 可缓存，适合作为 HashMap key。
- 字符串常量池复用相同字面量。
- `intern()` 返回常量池中的规范实例。

**追问**：new String("a") 会创建几个对象？可能创建堆对象，字面量 "a" 还可能进入常量池；细节见 [008 Q1](008_反射与异步编程.md)。

### Q42. equals 与 hashCode 的契约是什么？

**结论**：equals 相等的对象 hashCode 必须相等；hashCode 相等不要求 equals 相等。

**机制**：
- 重写 equals 必须重写 hashCode。
- 用作 HashMap/HashSet key 时两者都参与定位。
- 可变字段作为 hashCode 会导致查找失败。
- 推荐使用 Objects.equals/hash。

**追问**：只重写 equals 会怎样？相同对象可能落到不同桶，HashSet 去重失效。

### Q43. 泛型擦除与桥方法是什么？

**结论**：泛型在编译后被擦除为原始类型，桥方法用于保持多态与类型转换正确。

**机制**：
- 运行时无法获取泛型参数类型。
- 不能 `new T[]`、不能 `instanceof List<String>`。
- 子类重写泛型方法时编译器生成桥方法。
- 反射可通过签名获取泛型信息。

**追问**：擦除会带来什么问题？堆污染、运行时类型转换异常、重载冲突。

### Q44. 反射的性能开销在哪里？如何优化？

**结论**：反射有查找、访问检查、装箱与调用分派开销；缓存元数据、setAccessible、MethodHandle 可降低。

**机制**：
- `getMethod/getDeclaredMethod` 每次查找成本高。
- 访问检查与参数装箱。
- 缓存 Method/Field，避免重复查找。
- 热点路径可用 MethodHandle 或生成访问器。

**追问**：setAccessible 一定安全吗？会绕过访问控制，受模块强封装限制，需谨慎。

### Q45. BIO、NIO 与 AIO 有什么区别？

**结论**：BIO 阻塞式一连接一线程；NIO 多路复用非阻塞；AIO 异步回调，由操作系统完成通知。

**机制**：
- BIO 简单但线程成本高。
- NIO 用 Selector 管理多 Channel。
- AIO 用 Future/CompletionHandler。
- Netty 基于 NIO（可选 epoll）封装。

**追问**：NIO 一定比 BIO 快吗？高并发连接下优势明显，低并发简单场景 BIO 足够；细节见 [006](006_IO_NIO体系源码解析.md)。

---

## 六、速记清单

| 主题 | 必背一句话 |
|---|---|
| HashMap | 数组+链表+红黑树，2 的幂扩容，高位拆分 |
| CHM | CAS 空桶 + synchronized 桶头 |
| AQS | state + CLH 队列 + CAS |
| 线程池 | 核心→队列→最大→拒绝 |
| ThreadLocal | 弱引用 key，value 需 remove |
| JMM | happens-before 保证可见性，不保证原子性 |
| GC | 分代复制/标记整理，G1 Region 化 |
| 排查 | GC 日志 + 堆快照 + 线程栈 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：45 题 · 详解**
