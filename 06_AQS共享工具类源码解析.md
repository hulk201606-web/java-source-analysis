# 06 · AQS 共享模式 + 并发工具类源码解析

> 源码位置：`src/java/util/concurrent/`  
> 面试权重：⭐⭐⭐⭐⭐（6 大并发工具类 + 生产者-消费者双锁对比）  
> AQS 知识前置：请先阅读本系列 `02_并发基础源码解析.md` 中 AQS 章节

---

## 一、CountDownLatch · 倒计时闩锁

> 源码锚点：`CountDownLatch.java` L161-186  
> 核心：AQS 共享模式的极简实现

### 1.1 一句话设计

**主线程 await() 等，N 个子线程 countDown() 扣，扣到 0 主线程放行。一次性使用。**

### 1.2 Sync · 20 行极简共享实现

**源码锚点**：L161-186

```java
private static final class Sync extends AbstractQueuedSynchronizer {
    Sync(int count) { setState(count); }   // state = 需 countDown 的次数

    // 共享获取：state == 0 就放行，否则返回 -1 阻塞
    protected int tryAcquireShared(int acquires) {
        return (getState() == 0) ? 1 : -1;
    }

    // 共享释放：CAS 自旋 state--，减到 0 返回 true（触发唤醒）
    protected boolean tryReleaseShared(int releases) {
        for (;;) {
            int c = getState();
            if (c == 0) return false;
            int nextc = c - 1;
            if (compareAndSetState(c, nextc))
                return nextc == 0;   // ★ 减到 0 才返回 true
        }
    }
}
```

### 1.3 核心方法

| 方法 | 源码锚点 | 行为 |
|------|---------|------|
| `CountDownLatch(int count)` | L198 | 构造 Sync，state=count |
| `await()` | L230 | `acquireSharedInterruptibly(1)` → state≠0 就 park |
| `countDown()` | L255 | `releaseShared(1)` → state--，减到 0 唤醒所有等待线程 |

### 1.4 与 CyclicBarrier 对比

| 维度 | CountDownLatch | CyclicBarrier |
|------|---------------|---------------|
| 方向 | 子线程扣，主线程等 | **所有线程互相等，齐步走** |
| 计数器 | 递减到 0 | 递增到 parties |
| 可重置 | ❌ 一次性 | ✅ `reset()` / 自动 `nextGeneration()` |
| 实现 | **AQS 共享** | **ReentrantLock + Condition** |
| 触发回调 | ❌ | ✅ `barrierCommand.run()` |

---

## 二、Semaphore · 信号量（许可证）

> 源码锚点：`Semaphore.java` L166-254  
> 核心：AQS 共享模式 + 公平/非公平两种实现

### 2.1 一句话设计

**N 张许可证，线程 acquire() 扣一张，release() 还一张；没许可证就阻塞排队。**

### 2.2 Sync · 基类

**源码锚点**：L166-216

```java
abstract static class Sync extends AbstractQueuedSynchronizer {
    Sync(int permits) { setState(permits); }   // state = 许可证总数

    final int nonfairTryAcquireShared(int acquires) {
        for (;;) {
            int available = getState();
            int remaining = available - acquires;
            if (remaining < 0 ||                      // 不够 → 阻塞
                compareAndSetState(available, remaining))  // 够 → CAS 扣减
                return remaining;
        }
    }

    protected final boolean tryReleaseShared(int releases) {
        for (;;) {
            int current = getState();
            int next = current + releases;
            if (next < current) throw new Error("Maximum permit count exceeded");
            if (compareAndSetState(current, next)) return true;
        }
    }
}
```

### 2.3 公平 vs 非公平

**源码锚点**：L221-254

```java
// 非公平（默认）：直接 CAS，不管队列有没有人
static final class NonfairSync extends Sync {
    protected int tryAcquireShared(int acquires) {
        return nonfairTryAcquireShared(acquires);
    }
}

// 公平：先 hasQueuedPredecessors() 检查队列
static final class FairSync extends Sync {
    protected int tryAcquireShared(int acquires) {
        for (;;) {
            if (hasQueuedPredecessors()) return -1;   // 有前驱 → 不能插队
            int available = getState();
            int remaining = available - acquires;
            if (remaining < 0 || compareAndSetState(available, remaining))
                return remaining;
        }
    }
}
```

**公平 vs 非公平时序图**：
```
非公平（默认）：
  A ──acquire──▶ state 5→4 ✅ CAS 成功
  B ──排队────▶ Sync Queue
  C ──acquire──▶ state 4→3 ✅ 插队成功！（B 还在排队）→ 高吞吐，可能饿死

公平：
  A ──acquire──▶ state 5→4 ✅ 队列空
  B ──排队────▶ Sync Queue
  C ──acquire──▶ hasQueuedPredecessors()=true → 去排队！→ 严格 FIFO
```

---

## 三、CyclicBarrier · 可循环屏障

> 源码锚点：`CyclicBarrier.java` L151-277  
> 核心：**ReentrantLock + Condition**（不是 AQS！）+ Generation 代际机制

### 3.1 一句话设计

**N 个线程互相等，齐步走；到齐后执行 barrierCommand，然后自动重置。**

### 3.2 核心字段

**源码锚点**：L151-171

```java
private static class Generation {
    boolean broken = false;    // ★ 代际标记：broken 就跳过
}

private final ReentrantLock lock = new ReentrantLock();
private final Condition trip = lock.newCondition();   // ★ 所有线程等在这里
private final int parties;                              // 需要等多少个线程
private final Runnable barrierCommand;                // 全部到达后执行
private Generation generation = new Generation();      // 当前代际
private int count;                                      // 当前代际已到达数量（从 parties 递减）
```

### 3.3 nextGeneration / breakBarrier

**源码锚点**：L177-193

```java
// 全部到达 → 切换到下一代
private void nextGeneration() {
    trip.signalAll();              // ★ 唤醒所有等待线程
    count = parties;               // 重置计数器
    generation = new Generation();  // ★ 新代际（旧 Generation 继续被等待线程引用到退出）
}

// 异常/中断 → 打破屏障
private void breakBarrier() {
    generation.broken = true;   // ★ 标记 broken = true
    count = parties;
    trip.signalAll();             // 唤醒所有等待线程 → 它们看到 broken 后抛异常
}
```

### 3.4 dowait · 核心等待逻辑

**源码锚点**：L198-260

```java
private int dowait(boolean timed, long nanos)
    throws InterruptedException, BrokenBarrierException, TimeoutException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
        final Generation g = generation;

        if (g.broken) throw new BrokenBarrierException();
        if (Thread.interrupted()) { breakBarrier(); throw new InterruptedException(); }

        int index = --count;
        if (index == 0) {  // ★ 最后一个到达
            boolean ranAction = false;
            try {
                final Runnable command = barrierCommand;
                if (command != null) command.run();   // ★ 执行回调（在最后一个线程上）
                ranAction = true;
                nextGeneration();   // ★ 唤醒所有 + 切换代际
                return 0;
            } finally {
                if (!ranAction) breakBarrier();
            }
        }

        // 非最后一个 → 阻塞等 trip.signalAll()
        for (;;) {
            try {
                if (!timed) trip.await();
                else if (nanos > 0L) nanos = trip.awaitNanos(nanos);
            } catch (InterruptedException ie) {
                if (g == generation && !g.broken) { breakBarrier(); throw ie; }
            }

            if (g.broken) throw new BrokenBarrierException();
            if (g != generation) return index;   // ★ 代际变了 → 被唤醒了，退出
            // 超时处理 ...
        }
    } finally {
        lock.unlock();
    }
}
```

### 3.5 Generation 代际机制详解

```
为什么需要 Generation？
  → 多个线程可能同时在 await()，它们引用的是旧 Generation 对象
  → 最后一个线程到达 → nextGeneration() 创建新 Generation
  → 被唤醒的老线程检查 g != generation → 发现代际变了 → 正常退出
  → reset() 也可以用 generation 跳过旧等待线程

屏障被打破（一个线程 interrupt）：
  → breakBarrier() → generation.broken = true
  → 所有等待线程被唤醒后检查 g.broken → 抛 BrokenBarrierException
  → 新到达的线程看到 g.broken 也直接抛异常
  → 必须调 reset() 才能恢复
```

### 3.6 流程图

```
线程 A          线程 B          线程 C          线程 D（最后）
  │               │               │               │
  ├─ await()      ├─ await()      ├─ await()      ├─ await()
  ├─ count--=3    ├─ count--=2    ├─ count--=1    ├─ count--=0 ✅
  │               │               │               ├─ barrierCommand.run()
  │               │               │               ├─ nextGeneration()
  │               │               │               │  ├─ trip.signalAll()
  │               │               │               │  ├─ count = parties
  │               │               │               │  └─ generation = new Gen
  ├─ trip.await ──┤trip.await ───┤trip.await ───┤               │
  │               │               │               │
  │  signalAll 唤醒（检查 g != generation → 是）
  ├─ return       ├─ return       ├─ return       ├─ return 0
  │               │               │               │
  └─ 新一代开始 ───┴───────────────┴───────────────┘
```

---

## 四、LinkedBlockingQueue · 双锁生产者-消费者

> 源码锚点：`LinkedBlockingQueue.java` L122-454  
> 核心：**两把 ReentrantLock + 两个 Condition**

### 4.1 关键设计

```java
// ★★★ 两把锁（takeLock 只管出队，putLock 只管入队）
private final ReentrantLock takeLock = new ReentrantLock();
private final Condition notEmpty = takeLock.newCondition();

private final ReentrantLock putLock = new ReentrantLock();
private final Condition notFull = putLock.newCondition();

private final AtomicInteger count = new AtomicInteger(0);   // 队列大小
```

**为什么双锁？**
```
单锁：put 和 take 互斥 → 生产者和消费者不能并发
双锁：put 只和 put 互斥，take 只和 take 互斥
     生产者和消费者可以同时操作队列 → 吞吐量翻倍！
```

### 4.2 put() 生产者

**源码锚点**：L331-361

```java
public void put(E e) throws InterruptedException {
    if (e == null) throw new NullPointerException();
    int c = -1;
    Node<E> node = new Node<E>(e);
    final ReentrantLock putLock = this.putLock;
    final AtomicInteger count = this.count;
    putLock.lockInterruptibly();
    try {
        while (count.get() == capacity) {   // 队列满？
            notFull.await();                // ★ 阻塞在 notFull
        }
        enqueue(node);                      // 入队（last.next = node）
        c = count.getAndIncrement();
        if (c + 1 < capacity)               // 还有空间
            notFull.signal();               // 唤醒其他 put 线程
    } finally {
        putLock.unlock();
    }
    if (c == 0)                       // ★ 入队前 count=0
        signalNotEmpty();             // → 跨锁唤醒 take 线程
}
```

### 4.3 take() 消费者

**源码锚点**：L434-454

```java
public E take() throws InterruptedException {
    E x; int c = -1;
    final AtomicInteger count = this.count;
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lockInterruptibly();
    try {
        while (count.get() == 0) {   // 队列空？
            notEmpty.await();         // ★ 阻塞在 notEmpty
        }
        x = dequeue();                // 出队（head.next → 新 head）
        c = count.getAndDecrement();
        if (c > 1)
            notEmpty.signal();        // 唤醒其他 take 线程
    } finally {
        takeLock.unlock();
    }
    if (c == capacity)                // ★ 出队前 count=capacity
        signalNotFull();              // → 跨锁唤醒 put 线程
    return x;
}
```

### 4.4 跨锁唤醒

**源码锚点**：L170-183

```java
// put 完 → 发现之前是空 → 唤醒 take 线程
private void signalNotEmpty() {
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lock();                // ★ put 线程跨锁拿 takeLock
    try { notEmpty.signal(); }      // 唤醒等在 notEmpty 上的 take 线程
    finally { takeLock.unlock(); }
}

// take 完 → 发现之前是满 → 唤醒 put 线程
private void signalNotFull() {
    final ReentrantLock putLock = this.putLock;
    putLock.lock();                 // ★ take 线程跨锁拿 putLock
    try { notFull.signal(); }
    finally { putLock.unlock(); }
}
```

**为什么要跨锁？**
```
put 线程只持有 putLock → 无法操作 notEmpty（绑定 takeLock）
take 线程只持有 takeLock → 无法操作 notFull（绑定 putLock）
→ 必须跨锁拿对方的锁才能 signal 对方的 Condition
```

### 4.5 dequeue · help GC 设计

**源码锚点**：L209-219

```java
private E dequeue() {
    Node<E> h = head;
    Node<E> first = h.next;
    h.next = h;         // ★ help GC！被删节点自己指向自己 → 不可达 → 方便 GC
    head = first;
    E x = first.item;
    first.item = null;  // ★ help GC
    return x;
}
```

---

## 五、ArrayBlockingQueue · 单锁环形队列

> 源码锚点：`ArrayBlockingQueue.java` L88-186  
> 核心：**ReentrantLock + 双 Condition + 环形数组**

### 5.1 核心字段

**源码锚点**：L94-117

```java
final Object[] items;             // ★ 预分配数组，零 GC
int takeIndex;                    // 下一个取位置
int putIndex;                     // 下一个放位置
int count;                        // 队列大小

// ★ 单锁！和 LinkedBlockingQueue 双锁不同
final ReentrantLock lock;
private final Condition notEmpty;
private final Condition notFull;
```

### 5.2 enqueue / dequeue · 环形数组

**源码锚点**：L157-186

```java
private void enqueue(E x) {
    final Object[] items = this.items;
    items[putIndex] = x;
    if (++putIndex == items.length) putIndex = 0;   // ★ 环形回绕
    count++;
    notEmpty.signal();                                // 直接 signal，不用跨锁！
}

private E dequeue() {
    final Object[] items = this.items;
    E x = (E) items[takeIndex];
    items[takeIndex] = null;
    if (++takeIndex == items.length) takeIndex = 0;   // ★ 环形回绕
    count--;
    notFull.signal();                                  // 直接 signal
    return x;
}
```

### 5.3 put / take

**源码锚点**：L347-408

```java
public void put(E e) throws InterruptedException {
    checkNotNull(e);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
        while (count == items.length)   // 队列满？
            notFull.await();
        enqueue(e);                     // 入队 + notEmpty.signal()
    } finally {
        lock.unlock();
    }
}

public E take() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
        while (count == 0)             // 队列空？
            notEmpty.await();
        }
        return dequeue();               // 出队 + notFull.signal()
    } finally {
        lock.unlock();
    }
}
```

### 5.4 环形数组图解

```
items[0] items[1] items[2] items[3] items[4] items[5] items[6] items[7]
  X         X         X         X         X         null      null      null
                                          ↑ takeIndex(0)             ↑ putIndex(5)
                                        下次取这里                下次放这里
count = 5

put 一个元素：items[5]=new → putIndex=6 → count=6 → notEmpty.signal()
take 一个元素：items[0]=null → takeIndex=1 → count=4 → notFull.signal()
takeIndex 或 putIndex 到 items.length 就回绕到 0
```

---

## 六、阻塞队列三大对比

| 维度 | **LinkedBlockingQueue** | **ArrayBlockingQueue** | **SynchronousQueue** |
|------|------------------------|------------------------|---------------------|
| 底层结构 | 链表 | **固定数组** | 无（直接交付）|
| 锁数量 | **双锁（putLock + takeLock）** | **单锁** | CAS |
| 默认容量 | Integer.MAX_VALUE | 必须指定 | 0 |
| GC 压力 | 每次 put new Node | **预分配，零 GC** | 零 GC |
| 吞吐量 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| 队列满时 signal | **跨锁 signalNotEmpty/signalNotFull** | **直接 signal（单锁内）** | 没有队列，直接交付 |
| 适用场景 | 高并发生产者-消费者 | 固定容量（如线程池）| 直接交付（无缓冲）|

---

## 七、扩展考点补全

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **CountDownLatch** | | |
| 1 | await 不可被 countDown 唤醒？| 可以！state 到 0 时 tryReleaseShared 返回 true → doReleaseShared 唤醒所有等待线程 |
| 2 | 可重用吗？| ❌ 一次性。state 到 0 后无法重新置回正数 |
| **Semaphore** | | |
| 3 | 和锁的区别 | 锁 state=1（互斥），Semaphore state=N（共享 N 个）；默认非公平 |
| 4 | release 可以比 acquire 多吗？| 可以！state 会增加，溢出检查 `next < current`（L191）|
| 5 | 默认公平？| ❌ 默认非公平，构造函数 `new Semaphore(n, true)` 才是公平 |
| **CyclicBarrier** | | |
| 6 | 是 AQS 实现吗？| ❌ **ReentrantLock + Condition**（L156-158），没用 AQS |
| 7 | Generation 是什么？| 代际标记。每个新代际创建新 Generation 对象；等待线程检查 generation 是否变了来判断是否被唤醒 |
| 8 | 最后一个线程要做什么？| 除了 count--，还要执行 barrierCommand + nextGeneration（唤醒所有 + 重置）|
| 9 | broken 状态怎么恢复？| 必须调 `reset()` 手动恢复，或调 `await()` 到齐后自动 nextGeneration |
| **LinkedBlockingQueue** | | |
| 10 | 为什么用双锁？| put 和 take 完全解耦 → 生产者和消费者可以并发操作队列 → 吞吐量翻倍 |
| 11 | count 为什么 AtomicInteger？| 两把锁都不保护 count（putLock 只管入队，takeLock 只管出队）→ 必须用原子类 |
| 12 | fullyLock 为什么先 put 后 take？| 和 signalNotEmpty/signalNotFull 一致，避免死锁！signalNotEmpty 先拿 takeLock（生产者→消费者），signalNotFull 先拿 putLock（消费者→生产者）|
| 13 | dequeue 为什么 h.next = h？| help GC！被删节点自指 → 不可达 → GC 方便回收旧 head |
| **ArrayBlockingQueue** | | |
| 14 | 单锁怎么 signal？| enqueue/dequeue 内部直接 signal notEmpty/notFull（都在持有 lock 的范围内）|
| 15 | 环形数组怎么回绕？| `if (++putIndex == items.length) putIndex = 0;` （L162-163）|
| 16 | 预分配数组 vs 链表 | 数组：零 GC、缓存友好；链表：动态扩容、无界 |
| **综合** | | |
| 17 | 线程池默认用什么队列？| ThreadPoolExecutor 不传队列时用 LinkedBlockingQueue（几乎无界）|
| 18 | 生产级应该用什么队列？| ArrayBlockingQueue（有界），否则 LinkedBlockingQueue + 自定义拒绝策略 |

---

## 八、PriorityBlockingQueue · 无界优先级队列

> 源码锚点：`PriorityBlockingQueue.java`  
> 核心：**单 ReentrantLock + 堆数组 + 无界扩容**

### 8.1 核心字段

**源码锚点**：L151-172

```java
private transient Object[] queue;             // 堆数组
private transient int size;                   // 大小
private final Comparator<? super E> comparator;
private final ReentrantLock lock;             // 单锁
private final Condition notEmpty;              // 只需要一个 Condition（无界不会满！）
private transient volatile int allocationSpinLock;   // ★ 扩容用的 CAS 自旋锁
```

### 8.2 为什么 put 永不阻塞？

```java
public void put(E e) {
    offer(e);     // ★ 无界扩容，队列永不满 → 永不阻塞
}
```

### 8.3 tryGrow · 无界扩容精髓

**源码锚点**：L288-317

```java
private void tryGrow(Object[] array, int oldCap) {
    lock.unlock();   // ★ 释放主锁！（扩容耗时，不能长时间占锁）
    Object[] newArray = null;
    if (allocationSpinLock == 0 &&
        UNSAFE.compareAndSwapInt(this, allocationSpinLockOffset, 0, 1)) {
        try {
            int newCap = oldCap + ((oldCap < 64) ? (oldCap + 2) : (oldCap >> 1));
            if (newCap - MAX_ARRAY_SIZE > 0) {
                int minCap = oldCap + 1;
                if (minCap < 0 || minCap > MAX_ARRAY_SIZE) throw new OutOfMemoryError();
                newCap = MAX_ARRAY_SIZE;
            }
            if (newCap > oldCap && queue == array)
                newArray = new Object[newCap];   // ★ 在锁外 new（另一线程可能也在 grow）
        } finally {
            allocationSpinLock = 0;
        }
    }
    if (newArray == null) Thread.yield();   // 另一线程在 grow → 让步
    lock.lock();                            // 重新拿锁
    if (newArray != null && queue == array) {   // double-check
        queue = newArray;
        System.arraycopy(array, 0, newArray, 0, oldCap);
    }
}
```

**扩容设计亮点**：
```
1. 释放主锁 → 不在锁内 new Object[]（减少锁持有时间）
2. allocationSpinLock（CAS 自旋锁）保证只有一个线程在分配新数组
3. 扩容策略：小队列 ×2（oldCap < 64 时 +oldCap+2），大队列 ×1.5
4. double-check（queue == array）→ 拿锁后检查是否已被其他线程扩容过
5. 另一线程在 grow → Thread.yield() 让步 → 之后再进入
```

### 8.4 offer · 入队 + siftUp 堆上滤

**源码锚点**：L477-498

```java
public boolean offer(E e) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    int n, cap; Object[] array;
    while ((n = size) >= (cap = (array = queue).length))
        tryGrow(array, cap);   // 队列满 → 扩容（永不阻塞）
    try {
        Comparator<? super E> cmp = comparator;
        if (cmp == null)
            siftUpComparable(n, e, array);     // ★ 堆上滤（小顶堆）
        else
            siftUpUsingComparator(n, e, array, cmp);
        size = n + 1;
        notEmpty.signal();                     // 唤醒 take 线程
    } finally {
        lock.unlock();
    }
    return true;
}
```

### 8.5 dequeue · 出队 + siftDown 堆下滤

**源码锚点**：L322-339

```java
private E dequeue() {
    int n = size - 1;
    if (n < 0) return null;
    else {
        Object[] array = queue;
        E result = (E) array[0];       // ★ 堆顶（最小元素）
        E x = (E) array[n];
        array[n] = null;               // help GC
        Comparator<? super E> cmp = comparator;
        if (cmp == null)
            siftDownComparable(0, x, array, n);   // ★ 堆下滤
        else
            siftDownUsingComparator(0, x, array, n, cmp);
        size = n;
        return result;
    }
}
```

### 8.6 heapify · 构造时建堆

**源码锚点**：L437-448

```java
private void heapify() {
    Object[] array = queue;
    int n = size;
    for (int i = (n >>> 1) - 1; i >= 0; i--) {   // 从最后一个非叶子节点开始
        siftDownComparable(i, (E) array[i], array, n);
    }
}
```

### 8.7 堆操作图解

```
大顶堆（compareTo 反序）/ 小顶堆（默认）：
  [  0  ]    ← 堆顶（最小）
  [1] [2]    ← children(i) = 2i+1, 2i+2
 [3][4][5][6]

siftUp(k, x)：x 从 k 往上冒泡
  while k > 0:
    parent = (k-1) >>> 1
    if x.compareTo(array[parent]) >= 0 → break（父 ≤ x，堆性质满足）
    array[k] = array[parent];  // 父下沉
    k = parent
  array[k] = x

siftDown(k, x)：x 从 k 往下沉
  while (left = 2k+1) < n:
    j = left; if right = left+1 < n && array[right] < array[left] → j = right
    if x.compareTo(array[j]) <= 0 → break
    array[k] = array[j]; k = j
  array[k] = x
```

---

## 九、DelayQueue · 延时队列 + Leader-Follower

> 源码锚点：`DelayQueue.java`  
> 核心：**内部 PriorityQueue + Leader-Follower 模式**

### 9.1 核心字段

**源码锚点**：L74-99

```java
private final PriorityQueue<E> q = new PriorityQueue<E>();   // ★ 内部 PriorityQueue
private Thread leader = null;        // ★ Leader-Follower 模式关键字段
private final ReentrantLock lock;
private final Condition available = lock.newCondition();
```

### 9.2 Leader-Follower 模式图解

```
为什么需要 Leader？
  队列头的元素 delay=5s → 多个线程同时等待 → 每个都要 awaitNanos(5s)
  → 5s 后全部被唤醒 → 竞争 lock → 只有一个拿到 → 其他继续等

Leader 优化：
  只有一个线程当 Leader → awaitNanos(delay)
  其他线程 Follower → available.await()（无限等，等 Leader 唤醒）
  Leader 拿到元素后 → 唤醒一个 Follower → 它可能成为新 Leader

时序：
  T1: q.peek().delay=5s → leader=null → leader=T1 → awaitNanos(5s)
  T2: q.peek().delay=5s → leader=T1 → available.await()
  T3: q.peek().delay=5s → leader=T1 → available.await()
  ... 5s 后 ...
  T1: poll() → return ✅ → signal() 唤醒一个 Follower
  T2: 被唤醒 → 变成 Leader → 等下一个元素
```

### 9.3 take · Leader-Follower 完整实现

**源码锚点**：L204-236

```java
public E take() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
        for (;;) {
            E first = q.peek();
            if (first == null)
                available.await();           // 队空 → 无限等
            else {
                long delay = first.getDelay(NANOSECONDS);
                if (delay <= 0)
                    return q.poll();          // ★ 到期了！直接 poll
                first = null;                 // don't retain ref while waiting（GC 友好）
                if (leader != null)
                    available.await();       // ★ 已有 Leader → Follower 无限等
                else {
                    Thread thisThread = Thread.currentThread();
                    leader = thisThread;     // ★ 我当 Leader！
                    try {
                        available.awaitNanos(delay);   // 只等 delay 时间
                    } finally {
                        if (leader == thisThread)      // 我还是 Leader？
                            leader = null;             // 让出 Leader 位置
                    }
                }
            }
        }
    } finally {
        if (leader == null && q.peek() != null)
            available.signal();               // ★ Leader 退出后唤醒 Follower
        lock.unlock();
    }
}
```

### 9.4 offer · 入队 + leader 失效

**源码锚点**：L136-149

```java
public boolean offer(E e) {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
        q.offer(e);                        // 委托 PriorityQueue.offer → siftUp
        if (q.peek() == e) {               // ★ 新元素是堆顶？（更早到期）
            leader = null;                 // ★ 让旧 Leader 失效，重新竞争
            available.signal();            // 唤醒等待线程重新选 Leader
        }
        return true;
    } finally {
        lock.unlock();
    }
}
```

**leader = null 为什么重要？**
```
场景：Leader 正在 awaitNanos(10s) → 新 offer 一个 delay=1s 的元素
  q.peek() == e → leader = null → available.signal()
  Leader 被唤醒 → 发现 leader != thisThread → 变成 Follower → 重新 for(;;)
  新的线程（或原来的 Leader）成为新 Leader → awaitNanos(1s)

  如果不重置 leader=null：
  旧 Leader 继续等 10s → 新元素 1s 后没人取 → 延迟！
```

### 9.5 DelayQueue 是无界的

```java
public void put(E e) { offer(e); }               // 永不阻塞
public boolean offer(E e, long timeout, TimeUnit unit) {
    return offer(e);                               // timeout 参数忽略！
}
// 底层 PriorityQueue 是无界的 → DelayQueue 也无界
```

---

## 十、PriorityBlockingQueue vs DelayQueue vs LinkedBlockingQueue

| 维度 | **PriorityBlockingQueue** | **DelayQueue** | **LinkedBlockingQueue** |
|------|--------------------------|----------------|------------------------|
| 底层数据结构 | **堆数组** | **内部 PriorityQueue** | 链表 |
| 排序 | Comparable/Comparator | Delayed.getDelay() | FIFO 插入顺序 |
| 锁 | **单 ReentrantLock** + notEmpty | **单 ReentrantLock** + available | **双锁** (putLock/takeLock) |
| 有界？| **无界**（tryGrow 动态扩容）| **无界**（委托 PQ）| 默认几乎无界 |
| put 阻塞？| ❌ 永不 | ❌ 永不 | 可能满阻塞 |
| take 等待策略 | 等堆顶到期 | **Leader-Follower 模式** | 等队列非空 |
| 适用场景 | 优先级任务 | 定时/延时任务 | 生产者-消费者 |

---

**文档版本：v1.2 · 2026-10-21**  
**源码基线：JDK 1.8.0_451**  
**v1.2 更新：补 P0 缺口 §十一 LinkedTransferQueue · TransferQueue 核心**

---

## 十一、LinkedTransferQueue · 直接交付 + 队列缓冲合体

> **源码锚点**：`src/java/util/concurrent/LinkedTransferQueue.java` L72-530  
> **面试权重**：⭐⭐⭐⭐⭐

### 11.1 为什么需要 LinkedTransferQueue？

```
// ★ SynchronousQueue vs LinkedBlockingQueue：
//
// SynchronousQueue：直接交付模式 → 没有缓冲 → put 必须等 take 同时在线
// LinkedBlockingQueue：队列缓冲模式 → put 先入队 → take 之后来的时候直接取
//
// ★ LinkedTransferQueue = 两者合体！
//   hasWaitingConsumer() 为 true → transfer 直接交付（SynchronousQueue 模式）
//   hasWaitingConsumer() 为 false → transfer 入队（LinkedBlockingQueue 模式）
//
// ★ 应用场景：
//   Tomcat 线程池任务队列 → 核心线程都在忙 + 有消费者在等 → 直接交付；否则入队
//   Netty / RocketMQ 核心队列
//   Guava / Reactor 等框架内部
```

### 11.2 核心字段

```java
// LinkedTransferQueue 核心字段（L72-80）：
public class LinkedTransferQueue<E> extends AbstractQueue<E>
    implements TransferQueue<E>, java.io.Serializable {
    
    // ★ 内部节点（类似 ConcurrentSkipListMap.Node 的虚拟节点思想）
    static final class Node {
        final boolean isData;   // ★ true=生产者 put 的值；false=消费者 take 的请求
        Object item;            // 值（isData=true 时是实际元素；false 时是 CAS 占位符）
        volatile Node next;     // 链表后继
        volatile Thread waiter; // 如果还没匹配 → 挂在此线程上（LockSupport.park）
        // ...
    }
    
    // ★ 头尾指针（CAS 原子引用）
    transient volatile Node head;
    transient volatile Node tail;
    
    // ★ 关键！是否有等待的消费者节点在链表头
    // head.isData == false → 有消费者在等 → transfer 可以直接匹配
    // head.isData == true 或 head == null → 没有消费者 → transfer 入队
    public boolean hasWaitingConsumer() { return getWaitingConsumerCount() > 0; }
}
```

### 11.3 transfer vs put 核心逻辑

```java
// ★ transfer(E e)：直接交付 + 队列缓冲合体
public void transfer(E e) throws InterruptedException {
    if (tryTransfer(e))     // ① 如果有等待的消费者 → 直接交付 → 立即返回
        return;
    // ② 没有消费者在等 → 调用 transferSloop → 入队 + 挂起 wait
    transferSloop(new Node(e, true), SYNC, null);
}

// ★ tryTransfer(E e)：非阻塞直接交付
// 和 transfer 的区别：如果没有消费者 → 直接返回 false（不入队）
public boolean tryTransfer(E e) {
    Node h = head;
    // 链表头是消费者节点（isData=false）→ 直接匹配
    if (h != null && !h.isData && transferMatch(h, e, null)) {
        return true;  // ★ 直接交付成功！
    }
    // 链表头不是消费者（是生产者或空）→ 入队等待
    return false;
}

// transferMatch(h, e, null)：CAS 匹配一个消费者节点
// Node 内部 item 初始是 CAS 占位符（match）
// 生产者 CAS item match → e → 成功匹配 → 唤醒消费者线程
```

### 11.4 transfer 流程全图

```
transfer(E e)
  │
  ├─ head 存在 + head.isData == false
  │  → transferMatch(head, e)  ← CAS 匹配！
  │  → 设置 head.item = e（从 CAS 占位符变成实际值）
  │  → LockSupport.unpark(head.waiter)  ← 唤醒等待的消费者
  │  → 立即返回 ✅
  │
  └─ head == null 或 head.isData == true
     → transferSloop 入队
     → 创建 Node(isData=true, item=e)
     → CAS tail → 新 Node
     → 当前线程 LockSupport.park() 挂起
     → 被消费者 unpark 唤醒 → 返回
```

### 11.5 take 流程

```java
// take()：和 LinkedBlockingQueue.take() 类似，但匹配更灵活
public E take() throws InterruptedException {
    Node h = head;
    // 有生产者节点在队头 → 直接取（入队后被直接 match 了？或者还没被 transfer）
    if (h != null && h.isData && h.item != null) {
        // ★ 已经有生产者在等 → 直接 CAS 匹配
        E e = transferMatch(h, null, isData=true);
        return e;
    }
    // 队头不是生产者 → 创建消费者 Node（isData=false）入队等待
    // 生产者来 transfer 时 → head.isData=false → 直接 match
    Node node = new Node(null, false);
    // CAS head → 新消费者节点
    // LockSupport.park() 挂起
    // 被 transfer 的生产者 unpark 唤醒 → 返回 match 到的值
}
```

### 11.6 面试考点（LinkedTransferQueue 5 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | LinkedTransferQueue vs LinkedBlockingQueue 区别？| **多了直接交付模式**：hasWaitingConsumer() 为 true → transfer 不走队列直接匹配（类似 SynchronousQueue）|
| 2 | Node.isData 什么意思？| **true=生产者（值）；false=消费者（请求）** → 头节点是消费者 transfer 才能直接匹配 |
| 3 | transfer 和 put 区别？| put 永远入队；transfer 如果有等待的消费者 **直接交付不入队** |
| 4 | tryTransfer 什么意思？| 非阻塞直接交付 → 没有消费者立即返回 false（不入队）|
| 5 | 应用场景？| Tomcat 线程池任务队列（核心线程忙时直接交付 / 否则入队）|
