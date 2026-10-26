# 07 · ForkJoinPool Work-Stealing 分治框架源码解析

> 源码位置：`src/java/util/concurrent/ForkJoinPool.java` + `ForkJoinTask.java`  
> 面试权重：⭐⭐⭐⭐⭐（Java 并发调度最高级框架，CompletableFuture 默认底层）  
> 前置：建议先理解线程池 + AQS 共享模式

---

## 一、Work-Stealing 核心思想

### 1.1 一句话设计

**每个 worker 有自己的双端任务队列（WorkQueue），自己从 top 端取（LIFO），偷别人的从 base 端取（FIFO），负载不均时自动窃取。**

### 1.2 为什么需要 Work-Stealing？

```
传统线程池：
  所有任务在一个公共队列
  → 所有 worker 竞争同一把锁 → 高开销
  → 任务执行时间不均 → 某些 worker 闲死，某些累死

ForkJoinPool：
  每个 worker 有自己的 WorkQueue
  → 本地任务无锁（自己操作自己的 top）
  → 空闲 worker 偷忙 worker 的尾部任务（base）
  → 负载自动均衡 → 吞吐量翻几倍
```

### 1.3 关键术语

| 术语 | 含义 |
|------|------|
| **fork** | 分裂：大任务拆成小任务，push 到当前 worker 的 WorkQueue |
| **join** | 合并：等待 fork 出来的子任务完成；**关键！join 时当前 worker 帮忙偷别人的任务** |
| **WorkQueue** | 每个 worker 独立的双端队列 |
| **base** | 队列尾部（窃取端）—— 其他 worker 从这里偷 |
| **top** | 队列头部（本地端）—— 拥有者从这里 LIFO 取 |
| **scan** | 遍历所有 WorkQueue 尝试窃取 |
| **helpStealer / helpComplete** | join 时帮忙执行别人的任务加速完成 |

---

## 二、整体架构

### 2.1 ForkJoinPool 结构图

```
ForkJoinPool (parallelism=N)
  │
  ├── WorkQueue[0]  ← worker-0 拥有
  │     └── [base................top]
  │          base 端（别人偷）    top 端（自己取）
  │
  ├── WorkQueue[1]  ← worker-1 拥有
  │     └── [base................top]
  │
  ├── WorkQueue[2]  ← worker-2 拥有
  │     └── [base................top]
  │
  └── ...

外部 submit → 分配到某个 WorkQueue（hashing）
内部 fork  → push 到当前 worker 的 WorkQueue.top

本地 LIFO：pop() 从 top 端取 → 无锁（只有 owner 操作）
窃取 FIFO：scan() 从 base 端偷 → CAS 竞争（可能有多个 stealers）
```

### 2.2 双端队列图解

```
WorkQueue.array[]
  [  0  ][  1  ][  2  ][  3  ][  4  ][  5  ][  6  ][  7  ]
                                          ↑ base=2    ↑ top=5
                                          窃取端      本地端
  
  base=2 → 下次窃取从 index 2 开始
  top=5  → 下次 push 到 index 5；pop 从 index 4（top-1）开始
  
  拥有者 push：top→top+1 → 无锁写入 index top
  拥有者 pop ：top-1→top → 无锁读取 index top-1
  窃取者 steal：CAS base→base+1 → 竞争！多线程可能同时偷同一个
  
  base 是 volatile（可见性），top 不是（只有 owner 操作）
```

---

## 三、WorkQueue · 核心字段

> 源码锚点：`ForkJoinPool.java` L789-825

```java
static final class WorkQueue {
    static final int INITIAL_QUEUE_CAPACITY = 1 << 13;   // 8192 初始
    static final int MAXIMUM_QUEUE_CAPACITY = 1 << 26;   // 64M 上限

    // 实例字段：
    volatile int scanState;     // 版本号，<0: inactive; odd: scanning
    int stackPred;              // pool ctl 栈前驱
    int nsteals;                // 偷了多少次（调试用）
    int hint;                   // 随机化 hint
    int config;                 // pool index + mode（LIFO/FIFO）
    volatile int qlock;         // 1: locked, < 0: terminate, 0: free
    volatile int base;          // ★ 窃取端（下一个 poll 位置，volatile）
    int top;                    // ★ 本地端（下一个 push 位置，非 volatile）
    ForkJoinTask<?>[] array;    // 任务数组（初始 null，懒分配）
    final ForkJoinPool pool;
    final ForkJoinWorkerThread owner;   // ★ 拥有者线程（可能 null = shared queue）
    volatile Thread parker;     // owner 的 Thread（park/unpark 用）
    volatile ForkJoinTask<?> currentJoin;   // 正在 join 的任务
    volatile ForkJoinTask<?> currentSteal;  // 正在偷的任务
}
```

---

## 四、核心操作

### 4.1 push · 往自己队列放

> 源码锚点：ForkJoinPool.java L870-884

```java
final void push(ForkJoinTask<?> task) {
    ForkJoinTask<?>[] a; ForkJoinPool p;
    int b = base, s = top, n;
    if ((a = array) != null) {
        int m = a.length - 1;
        // ★ 无锁！只有 owner 操作 top
        U.putOrderedObject(a, ((m & s) << ASHIFT) + ABASE, task);  // 写入 array[top]
        U.putOrderedInt(this, QTOP, s + 1);                         // top++
        if ((n = s - b) <= 1) {                                     // 队列可能空了
            if ((p = pool) != null)
                p.signalWork(p.workQueues, this);                  // 唤醒其他 worker
        }
        else if (n >= m)
            growArray();                                            // 队列满了 → ×2 扩容
    }
}
```

### 4.2 pop · 从自己队列取（LIFO）

> 源码锚点：ForkJoinPool.java L918-924

```java
final ForkJoinTask<?> pop() {
    ForkJoinTask<?>[] a; ForkJoinTask<?> t; int m;
    if ((a = array) != null && (m = a.length - 1) >= 0) {
        for (int s; (s = top - 1) - base >= 0;) {   // top-1 倒序找
            long j = ((m & s) << ASHIFT) + ABASE;
            if ((t = (ForkJoinTask<?>)U.getObject(a, j)) == null)
                break;
            if (U.compareAndSwapObject(a, j, t, null)) {   // CAS 拿到任务
                top = s;                                    // top--
                return t;
            }
        }
    }
    return null;   // 队列为空或竞争失败
}
```

### 4.3 scan · 窃取别人的任务

> 源码锚点：ForkJoinPool.java L1726-1781

```java
private ForkJoinTask<?> scan(WorkQueue w, int r) {
    WorkQueue[] ws; int m;
    if ((ws = workQueues) != null && (m = ws.length - 1) > 0 && w != null) {
        int ss = w.scanState;
        for (int origin = r & m, k = origin, oldSum = 0, checkSum = 0;;) {
            WorkQueue q; ForkJoinTask<?>[] a; ForkJoinTask<?> t;
            int b, n; long c;
            if ((q = ws[k]) != null) {
                if ((n = (b = q.base) - q.top) < 0 &&    // 队列非空（base - top < 0）
                    (a = q.array) != null) {
                    // ★ 从 base 端取（FIFO）
                    long i = (((a.length - 1) & b) << ASHIFT) + ABASE;
                    if ((t = ((ForkJoinTask<?>)
                              U.getObjectVolatile(a, i))) != null &&
                        q.base == b) {
                        if (ss >= 0) {
                            // CAS 窃取！
                            if (U.compareAndSwapObject(a, i, t, null)) {
                                q.base = b + 1;
                                if (n < -1)
                                    signalWork(ws, q);  // 还有 → 唤醒自己
                                return t;                // ★ 偷到了！
                            }
                        }
                    }
                    // 没偷到 → 换随机位置继续
                    r ^= r << 1; r ^= r >>> 3; r ^= r << 10;
                    origin = k = r & m;
                    oldSum = checkSum = 0;
                    continue;
                }
                checkSum += b;
            }
            if ((k = (k + 1) & m) == origin) {    // 遍历完一圈
                if (oldSum == (oldSum = checkSum)) {
                    // 连续两圈 checkSum 相同 → 队列稳定 → 自己没事干
                    break;   // 退出 → awaitWork 等待
                }
                checkSum = 0;
            }
        }
    }
    return null;   // 所有队列都空
}
```

**scan 核心要点**：
```
1. 从随机位置开始遍历 WorkQueue[]（origin = r & m）
2. 每个 queue 从 base 端尝试 CAS 窃取（FIFO 公平）
3. 用 checkSum（累加各 queue 的 base）判断"所有队列都稳定空了"
4. 连续两圈 checkSum 相同 → 所有队列确定空了 → return null → 去 park
5. xorshift 随机数生成（r ^= r << 13 ...），避免每次从同一位置开始
```

### 4.4 runWorker · 主循环

> 源码锚点：ForkJoinPool.java L1697-1708

```java
final void runWorker(WorkQueue w) {
    w.growArray();              // 懒分配（第一次用才分配 8192）
    int seed = w.hint;
    int r = (seed == 0) ? 1 : seed;
    for (ForkJoinTask<?> t;;) {
        if ((t = scan(w, r)) != null)   // ★ 先 scan（偷别人的）
            w.runTask(t);               // 执行任务
        else if (!awaitWork(w, r))      // scan 失败 → park 等待
            break;                      // pool 关闭 → 退出
        r ^= r << 13; r ^= r >>> 17; r ^= r << 5;  // xorshift
    }
}
```

**注意**：`runWorker` 只从 **scan** 拿任务（偷别人的），不从 **pop**（自己队列）拿！那自己的队列谁取？→ **join 时通过 helpStealer / awaitJoin 帮忙从自己的队列取**。这是 ForkJoinPool 最精妙的设计之一。

---

## 五、ForkJoinTask · fork / join

> 源码锚点：`ForkJoinTask.java` L378-405 + ForkJoinPool.java L2056

### 5.1 fork · 分裂任务

```java
// ForkJoinTask.fork()（ForkJoinPool 内部类可见）
public final ForkJoinTask<?> fork() {
    Thread t; ForkJoinPool p; WorkQueue w;
    if ((t = Thread.currentThread()) instanceof ForkJoinWorkerThread) {
        // ★ worker 线程 → push 到自己的 WorkQueue
        (w = (p = ((ForkJoinWorkerThread)t).pool).workQueues[...]).push(this);
        p.signalWork(p.workQueues, w);
    }
    else if ((p = ForkJoinPool.commonPool()).tryAddSubmission(this)) {
        // 非 worker 线程 → 提交到 commonPool
    }
    return this;
}
```

### 5.2 join · 等待 + 帮忙偷

> 源码锚点：ForkJoinTask.java L384-392

```java
private int doJoin() {
    int s; Thread t; ForkJoinWorkerThread wt; ForkJoinPool.WorkQueue w;
    return (s = status) < 0 ? s :    // 已完成 → 直接返回
        ((t = Thread.currentThread()) instanceof ForkJoinWorkerThread) ?
        // worker 线程：
        (w = (wt = (ForkJoinWorkerThread)t).workQueue).
        tryUnpush(this) && (s = doExec()) < 0 ? s :   // ★ 先尝试从自己队列取出来自己执行！
        wt.pool.awaitJoin(w, this, 0L) :              // ★ 不行 → awaitJoin（帮忙偷任务加速）
        externalAwaitDone();   // 非 worker 线程：外部等待
}
```

**awaitJoin 核心思想**（简化）：
```
awaitJoin(w, this, deadline) {
    while (status != DONE) {
        // 帮忙偷别人的任务 → 加速整个 pool 的完成 → 间接加速 this 的完成
        if ((task = scan(w, r)) != null) {
            w.runTask(task);    // 偷到任务 → 执行
            // 执行完 → 可能发现 this 已经被加速完成 → 返回
        } else if (this 已经在某个队列里) {
            // 从别的 queue 里把 this 偷出来自己执行
            helpStealer(this);
        }
        // ... 超时 / CAS 自旋 ...
    }
    return status;
}
```

### 5.3 join 的"帮忙偷"机制图解

```
Worker A（执行大任务，fork 出子任务 T1/T2/T3）
  │
  ├─ fork(T1) → push 到 A 的 queue
  ├─ fork(T2) → push 到 A 的 queue
  ├─ fork(T3) → push 到 A 的 queue
  │
  ├─ join(T1)
  │   ├─ tryUnpush(T1) → 把 T1 从自己的 queue 里取出来 → 自己执行 T1 ✅
  │   ├─ 执行完 T1 → T1.status = DONE → join 返回
  │   │
  ├─ join(T2)
  │   ├─ 发现 T2 已经被 Worker B 偷走了
  │   ├─ awaitJoin → scan 偷别人的任务 → 帮忙执行其他任务
  │   │   → 间接加速 T2 的完成（T2 可能在 B 手里，B 空闲时就执行了）
  │   │
  │   └─ 或者 helpStealer → 从 B 的 queue 里把 T2 偷回来自己执行 ✅
  │
  └─ join(T3) → 同上
```

**为什么 join 要帮忙偷？**
```
传统等待：join() → 当前线程阻塞 → 浪费 CPU → 整个 pool 吞吐量下降
ForkJoinPool：join() → 当前线程不阻塞 → 帮忙执行其他任务 → 加速整个 pool
             → 间接加速自己等待的任务完成
→ 这是 "work-stealing + work-sharing" 的精华
```

---

## 六、完整执行流程示例

### 6.1 RecursiveAction（无返回值）

```java
public class ParallelPrint extends RecursiveAction {
    private final int[] arr;
    private final int lo, hi;
    private static final int THRESHOLD = 100;

    protected void compute() {
        if (hi - lo <= THRESHOLD) {
            // 叶子节点：直接做
            for (int i = lo; i < hi; i++) System.out.println(arr[i]);
        } else {
            // 分裂
            int mid = (lo + hi) >>> 1;
            ParallelPrint left = new ParallelPrint(arr, lo, mid);
            ParallelPrint right = new ParallelPrint(arr, mid, hi);
            left.fork();          // ★ fork → push 到当前 worker 的 queue
            right.compute();      // ★ 当前线程直接执行右半（优化！少一次 fork）
            left.join();          // ★ 等待 + 帮忙偷
        }
    }
}

// 使用
ForkJoinPool pool = new ForkJoinPool();
pool.invoke(new ParallelPrint(arr, 0, arr.length));
pool.shutdown();
```

### 6.2 RecursiveTask（有返回值）

```java
public class ParallelSum extends RecursiveTask<Long> {
    private final long[] arr;
    private final int lo, hi;
    private static final int THRESHOLD = 1000;

    protected Long compute() {
        if (hi - lo <= THRESHOLD) {
            long sum = 0;
            for (int i = lo; i < hi; i++) sum += arr[i];
            return sum;
        } else {
            int mid = (lo + hi) >>> 1;
            ParallelSum left = new ParallelSum(arr, lo, mid);
            ParallelSum right = new ParallelSum(arr, mid, hi);
            left.fork();
            long rightResult = right.compute();   // 右半直接执行
            long leftResult = left.join();         // 左半等待 + 帮忙偷
            return leftResult + rightResult;
        }
    }
}
```

### 6.3 执行时序图

```
ForkJoinPool(4)
  Worker A        Worker B        Worker C        Worker D
    │                │                │                │
    ├─ fork(T1)      ├─ scan 偷 T1    ├─ scan 偷 T3    ├─ scan 偷 T5
    │  push A.queue  │  base→base+1   │  base→base+1   │  base→base+1
    ├─ fork(T2)      ├─ 执行 T1      ├─ 执行 T3      ├─ 执行 T5
    │  push A.queue  │                │                │
    ├─ fork(T3)      ├─ scan 偷 T6    ├─ scan 偷 T2    ├─ scan 偷 T7
    │  push A.queue  │  (从 C)       │  (从 A)       │
    ├─ fork(T4)      ├─ 执行 T6      ├─ 执行 T2      ├─ 执行 T7
    │  push A.queue  │                │                │
    ├─ fork(T5)      │                │                │
    ├─ fork(T6)      │                │                │
    ├─ fork(T7)      │                │                │
    │  A.queue:                                              │
    │  [base=4...............................top=7]         │
    │                                                        │
    ├─ join(根任务)   ├─ scan 偷 T4    ├─ scan 偷 ?     ├─ scan 偷 ?
    │  awaitJoin      │  (从 A)       │  A 空了       │  A 空了
    │  帮忙 scan      │                │                │
    ├─ 偷到 T4 ───────┘ 执行 T4      ├─ pool idle    ├─ pool idle
    │  执行 T4                       │ park          │ park
    ├─ 根任务完成 ✅                  │                │                │
```

---

## 七、Common Pool · CompletableFuture 默认底层

> 源码锚点：ForkJoinPool.java L2599

```java
// CompletableFuture.supplyAsync(fn) 默认用这个 commonPool
public static ForkJoinPool commonPool() {
    // 单例，首次调用时创建
    return common;
}

// 创建 common pool：
// parallelism = Runtime.getRuntime().availableProcessors() - 1
// 如果可用核数 ≤ 2 → parallelism = 1
// 不叫 commonPool 的线程执行 CompletableFuture 任务时，会被分配到 commonPool 的某个 WorkQueue
// commonPool 不 shutdown（整个 JVM 生命周期都在）
```

**CompletableFuture 默认线程池**：
```java
// 这些都用 ForkJoinPool.commonPool()
CompletableFuture.supplyAsync(supplier);         // 默认 commonPool
CompletableFuture.runAsync(runnable);            // 默认 commonPool
CompletableFuture.completedFuture(val);
// thenApply / thenAccept / thenRun / handle / whenComplete 等
// → 如果前一个 stage 在 commonPool 完成，后续 stage 也用 commonPool
```

### 7.1 asyncMode

```java
new ForkJoinPool(
    parallelism,
    defaultForkJoinWorkerThreadFactory,
    null,                       // UncaughtExceptionHandler
    true                        // ★ asyncMode
);

// asyncMode = false（默认）：LIFO 队列，适合分治算法
// asyncMode = true：FIFO 队列，适合事件处理（CompletableFuture 场景）
// asyncMode 通过 config 字段的最低位标记
```

---

## 八、扩展考点补全

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **WorkQueue** | | |
| 1 | base 和 top 哪个 volatile？| **base volatile**（窃取端被多线程竞争），**top 非 volatile**（只有 owner 操作）|
| 2 | 队列初始容量？| `INITIAL_QUEUE_CAPACITY = 1 << 13 = 8192`（L800）|
| 3 | push 要加锁吗？| **不要**！只有 owner 操作 top → 无锁写入 |
| 4 | pop 为什么有 CAS？| 可能有 stealers 同时在从 base 端偷 → base 和 top 交叉时需要 CAS |
| 5 | scan 怎么判断"所有队列都空了"？| 连续两圈 scan 的 **checkSum 相同**（checkSum = 所有 queue.base 之和）→ 队列稳定 |
| 6 | 数组怎么扩容？| `growArray()` → old.length << 1，从旧队列 poll 所有任务 → CAS 搬到新数组 |
| **fork / join** | | |
| 7 | fork 和 compute 有什么区别？| fork 是异步（push 到队列），compute 是同步（当前线程直接执行）。**右半任务直接 compute 是经典优化** |
| 8 | join 真的会阻塞吗？| 不一定！**join 先 tryUnpush 把任务从自己队列取出来执行**，如果任务已被别人执行就帮忙偷 |
| 9 | join 帮忙偷的好处？| 传统 join → 当前线程阻塞 → 浪费 CPU；ForkJoinPool join → 当前线程帮忙执行 → 加速整个 pool |
| 10 | 为什么 RecursiveAction/Task 要继承它们？| 因为 ForkJoinPool 只认识 ForkJoinTask 子类，RecursiveAction（无返回）/ RecursiveTask（有返回）是抽象实现 |
| **ForkJoinPool** | | |
| 11 | parallelism 默认多少？| `availableProcessors()` → 传 parallelism=N 创建 N 个 worker |
| 12 | commonPool 和普通 ForkJoinPool 区别？| commonPool 是 JVM 全局单例（默认 workers=N-1），不 shutdown；普通 pool 可以 shutdown |
| 13 | CompletableFuture 默认用哪个？| **ForkJoinPool.commonPool()**（asyncMode=LIFO）|
| 14 | 什么时候用 new ForkJoinPool？| 需要隔离时（如一个计算失败不影响其他）或需要自定义 parallelism/asyncMode 时 |
| 15 | 和传统线程池的性能对比？| 分治场景 ForkJoinPool 快 2-5x（work-stealing + 无锁本地队列）；IO 密集传统线程池更好 |
| **深入理解** | | |
| 16 | runWorker 主循环只 scan 不 pop？| 是的！runWorker 只偷别人的，自己队列的任务由 **join 时的 awaitJoin/helpStealer** 处理 |
| 17 | push 后为什么 signalWork？| 告诉其他 worker 可能有任务了 → 唤醒 park 的 worker 去 scan |
| 18 | CAS 失败时怎么处理？| 自旋重试（scan 里 CAS 失败 → xorshift 换随机位置 → 继续）|
| 19 | 队列满了怎么办？| `push` 时 `n >= m` → `growArray()` 扩容 ×2（最多到 64M）|
| 20 | 窃取方向为什么 base→top（FIFO）？| FIFO 窃取保证公平性 + 窃取较老的任务（它们可能有更多子任务要 join）→ 间接提升整体效率 |

---

## 十、scan checkSum · 深度揭秘（最难的算法细节）

> 源码锚点：ForkJoinPool.java L1726-1781

### 10.1 问题背景

```
Worker scan 的目标：判断所有 WorkQueue 是否"稳定为空"
  → 如果稳定空 → worker 可以 park 等待（节省 CPU）
  → 如果不稳定空 → 可能被 steal 成功 → 继续 scan

最直观的做法：遍历 N 个 queue，每个都 peek 一下，看是否空
  → O(N) 每个 queue 都要做一次 volatile 读 base
  → 而且可能有新任务加入 → 空的判断不可靠

ForkJoinPool 的 checkSum 技巧：
  → 只累加每个 queue 的 base（volatile 只读）
  → 用数学方法判断稳定性
```

### 10.2 scan 完整源码 + checkSum 逻辑标注

**源码锚点**：L1726-1781

```java
private ForkJoinTask<?> scan(WorkQueue w, int r) {
    WorkQueue[] ws; int m;
    if ((ws = workQueues) != null && (m = ws.length - 1) > 0 && w != null) {
        int ss = w.scanState;                     // scan 状态（INACTIVE/SCANNING）
        for (int origin = r & m, k = origin, oldSum = 0, checkSum = 0;;) {
            WorkQueue q; ForkJoinTask<?>[] a; ForkJoinTask<?> t;
            int b, n; long c;
            if ((q = ws[k]) != null) {
                if ((n = (b = q.base) - q.top) < 0 &&    // ★ 队列非空判定
                    (a = q.array) != null) {
                    // ============ 窃取尝试 ============
                    long i = (((a.length - 1) & b) << ASHIFT) + ABASE;
                    if ((t = ((ForkJoinTask<?>)
                              U.getObjectVolatile(a, i))) != null &&
                        q.base == b) {
                        if (ss >= 0) {
                            if (U.compareAndSwapObject(a, i, t, null)) {
                                q.base = b + 1;
                                if (n < -1) signalWork(ws, q);
                                return t;   // ★ 偷到了！
                            }
                        }
                        // INACTIVE 时尝试激活自己
                        else if (oldSum == 0 && w.scanState < 0)
                            tryRelease(c = ctl, ws[m & (int)c], AC_UNIT);
                    }
                    // ============ 窃取失败 → 换位置继续 ============
                    if (ss < 0) ss = w.scanState;   // 刷新 scanState
                    // ★ xorshift 随机数生成
                    r ^= r << 1; r ^= r >>> 3; r ^= r << 10;
                    origin = k = r & m;             // 跳到随机位置重扫
                    oldSum = checkSum = 0;          // 重置 sum
                    continue;
                }
                // ★ checkSum 累加：队列空时才加 base
                // base 变化说明队列有变化 → checkSum 跟着变
                checkSum += b;
            }
            // ============ 一圈扫完 ============
            if ((k = (k + 1) & m) == origin) {
                if ((ss >= 0 || (ss == (ss = w.scanState))) &&
                    oldSum == (oldSum = checkSum)) {  // ★★★ checkSum 稳定了！
                    // 连续两圈 checkSum 相同 → 所有队列稳定空了
                    if (ss < 0 || w.qlock < 0) break;  // 已经 inactive → 直接退出
                    // 尝试 inactive（告诉 pool 我 idle 了）
                    int ns = ss | INACTIVE;
                    long nc = ((SP_MASK & ns) |
                               (UC_MASK & ((c = ctl) - AC_UNIT)));
                    w.stackPred = (int)c;
                    U.putInt(w, QSCANSTATE, ns);
                    if (U.compareAndSwapLong(this, CTL, c, nc))
                        ss = ns;
                    else
                        w.scanState = ss;   // CAS 失败 → 回滚
                }
                checkSum = 0;
            }
        }
    }
    return null;   // 所有队列都稳定空
}
```

### 10.3 checkSum 判断稳定性图解

```
N=3 个 worker，每个有一个 WorkQueue
初始：q0=[b=2], q1=[b=5], q2=[b=3] （base 是 volatile 读）

第 1 圈 scan（origin=0）：
  k=0: q0 非空 → 尝试偷 → 成功！→ return task
  → 或失败 → r 和 k 跳到随机位置重扫

假设连续扫描全是"队列空"情况：
  圈 1: q0.b=2, q1.b=5, q2.b=3 → checkSum=10
  圈 2: q0.b=2, q1.b=5, q2.b=3 → checkSum=10
  → oldSum == checkSum（都是 10）→ 稳定！→ 尝试 inactive → park

为什么连续两圈？
  一圈时可能刚好有队列 base 变化 → 不稳定
  两圈都相同 → 几乎可以确定所有队列稳定了

为什么只用 base？
  base 是 volatile（窃取端会更新）→ 只读 base 就够了
  top 不 volatile → 本地变化不影响 checkSum
  base 稳定说明没有 steal 发生（没人从 base 端取）
  → 没有 steal + n=(base-top)≥0 → 队列稳定空
```

### 10.4 scanState 的状态机

```
scanState 字段设计：
  scanState >= 0 → ACTIVE（正在扫描/执行任务）
  scanState 奇数 → SCANNING（正在 scan）
  scanState < 0 → INACTIVE（park 中）
  INACTIVE bit = 0x80000000（最高位）

scan 时：
  ss = w.scanState（初始 >= 0）
  CAS 窃取成功 → return task → 任务完成后 runWorker 会继续 scan
  全队列稳定空 → ss | INACTIVE → CAS 更新 scanState → 尝试 inactive
  inactive 成功 → awaitWork（park）等新任务

被唤醒后：
  signalWork 会把 scanState 的 INACTIVE 清掉（tryRelease AC_UNIT）
  → worker 重新 runWorker → scan → 执行任务
```

### 10.5 scan 的 CAS 竞争

```
多个 worker 同时 scan 同一个 queue 的 base 端：
  Thread A: CAS base=2 → base=3 ✅ 成功，拿到任务
  Thread B: 读 base=2 → CAS 失败 → 重新读 base=3 → 队列可能空了
  Thread C: 读 base=2 → CAS 失败 → 换 queue 继续 scan

为什么不用锁？
  → 锁开销大
  → CAS 失败只是多读一次 volatile + 重试 → 比锁便宜
  → 大多数时候 base 端竞争不大（任务主要从 top 端本地取）
```

### 10.6 xorshift 随机数

```java
r ^= r << 13; r ^= r >>> 17; r ^= r << 5;   // xorshift
// 每次 scan 到窃取失败或队列非空时，用 xorshift 生成新的扫描起点
// 避免所有 worker 从同一位置开始扫描 → 减少竞争

r ^= r << 1; r ^= r >>> 3; r ^= r << 10;   // 另一组 xorshift（窃取失败时用）
```

---

## 十一、CompletableFuture.thenCompose · 异步链触发机制

> **完整异步链触发机制源码解析见 [05_反射与异步编程.md 第五章](file:///d:/GC_WorkSpaces/jdk-source-analysis/05_反射与异步编程.md)（result/stack 架构、tryFire 三模式、UniCompose+UniRelay 桥接、CoCompletion 双输入共享、allOf/anyOf 树形构建）**

> 源码位置：`src/java/util/concurrent/CompletableFuture.java`  
> 核心：Completion 栈 + postComplete NESTED 模式 + UniRelay 桥接

### 11.1 核心架构：Completion 栈 + 三种 tryFire 模式

**源码锚点**：L419-422

```java
// Completion.tryFire 的三种模式（有符号很重要）：
static final int SYNC   =  0;   // 同步触发（创建时立即尝试执行）
static final int ASYNC  =  1;   // 异步触发（ForkJoinTask.run 时调用）
static final int NESTED = -1;   // 嵌套触发（postComplete 递归调用）

// 每个 CompletableFuture 内部维护一个 Completion 栈：
volatile Completion stack;      // 栈顶（CAS 原子更新）

// Completion 是内部类，代表一个"依赖"（thenApply/thenAccept/thenCompose 等）
// 每个 Completion 有 source（前驱 CF）+ dep（当前 CF）+ fn（执行逻辑）
```

### 11.2 thenApply 的 Completion 链式触发

```java
// 源码：L624-634
private <V> CompletableFuture<V> uniApplyStage(Executor e, Function f) {
    CompletableFuture<V> d = new CompletableFuture<V>();   // ① 创建依赖 CF
    if (e != null || !d.uniApply(this, f, null)) {         // ② 尝试立即执行
        UniApply<T,V> c = new UniApply<T,V>(e, d, this, f);  // ③ 创建 UniCompletion
        push(c);                                            // ④ push 到 this.stack
        c.tryFire(SYNC);                                    // ⑤ 同步尝试一次（可能此时 this 已完成）
    }
    return d;
}

// 触发链：
// 1. this（前驱 CF）complete → this.result 被设置
// 2. this.postComplete() → 从 stack pop 所有 Completion
// 3. 每个 Completion.tryFire(NESTED) → 执行 fn → dep.completeValue(result)
// 4. dep 又触发自己的 postComplete → 继续链 ...
```

### 11.3 postComplete · NESTED 链式执行

**源码锚点**：L470-491

```java
final void postComplete() {
    CompletableFuture<?> f = this; Completion h;
    while ((h = f.stack) != null ||
           (f != this && (h = (f = this).stack) != null)) {
        CompletableFuture<?> d; Completion t;
        if (f.casStack(h, t = h.next)) {   // ★ CAS 把栈顶弹出
            if (t != null) {
                if (f != this) {
                    pushStack(h);      // 非 NESTED 模式 → 递归时 push 回去
                    continue;
                }
                h.next = null;         // 链尾 → detach help GC
            }
            f = (d = h.tryFire(NESTED)) == null ? this : d;  // ★ NESTED 模式执行
        }
    }
}
```

### 11.4 thenCompose · UniCompose + UniRelay 桥接

**源码锚点**：L930-979

```java
// thenCompose 返回的依赖 CF 是 Compose 的 dep
// UniCompose 是 source=this, dep=返回值 CF, fn=用户传入的 Function
// UniCompose.tryFire 调用 dep.uniCompose(this, fn)

final boolean uniCompose(
    CompletableFuture<S> a,     // source CF（已完成）
    Function<? super S, ? extends CompletionStage<T>> f,  // 用户的 compose 函数
    UniCompose<S,T> c) {
    Object r; Throwable x;
    if (a == null || (r = a.result) == null || f == null)
        return false;
    tryComplete: if (result == null) {   // dep 还没完成
        if (r instanceof AltResult) {
            if ((x = ((AltResult)r).ex) != null) {
                completeThrowable(x, r);   // source 异常 → dep 也异常
                break tryComplete;
            }
            r = null;
        }
        try {
            if (c != null && !c.claim()) return false;
            @SuppressWarnings("unchecked") S s = (S) r;
            CompletableFuture<T> g = f.apply(s).toCompletableFuture();  // ★ 用户函数返回内层 CF
            if (g.result == null || !uniRelay(g)) {   // 内层 CF 已完成？
                // ★ 核心：内层 CF 还没完成 → 桥接！
                UniRelay<T> copy = new UniRelay<T>(this, g);  // 创建 Relay
                g.push(copy);                                  // push 到内层 CF 的 stack
                copy.tryFire(SYNC);                            // 立即尝试（内层可能刚好完成）
                if (result == null) return false;              // dep 还没被 complete → 等待内层
            }
        } catch (Throwable ex) {
            completeThrowable(ex);
        }
    }
    return true;
}
```

### 11.5 UniRelay · thenCompose 的关键桥接

**源码锚点**：L907-927

```java
// UniRelay 是 UniCompletion<T,T>，把内层 CF 的结果 relay 到外层 CF
static final class UniRelay<T> extends UniCompletion<T,T> {
    UniRelay(CompletableFuture<T> dep, CompletableFuture<T> src) {
        super(null, dep, src);   // executor=null（同步执行）
    }
    final CompletableFuture<T> tryFire(int mode) {
        CompletableFuture<T> d; CompletableFuture<T> a;
        if ((d = dep) == null || !d.uniRelay(a = src))
            return null;
        src = null; dep = null;
        return d.postFire(a, mode);
    }
}

// uniRelay：把 src 的 result 直接 copy 到 this
final boolean uniRelay(CompletableFuture<T> a) {
    Object r;
    if (a == null || (r = a.result) == null)
        return false;
    if (result == null) completeRelay(r);   // this.completeRelay(a.result)
    return true;
}
```

### 11.6 thenCompose 完整链式触发图解

```
场景：
  CF<String> f1 = ...;
  CF<Integer> f2 = f1.thenCompose(s -> f1Async(s));
  // f1Async 返回 CF<Integer>

执行时序：
  1. f1.thenCompose 创建：
     - f2 = new CompletableFuture<Integer>()
     - UniCompose(f1, f2, fn) push 到 f1.stack
  
  2. f1 complete("hello") → f1.postComplete()
     → pop UniCompose → tryFire(NESTED)
     → f2.uniCompose(f1, fn, compose)
     → g = f1Async("hello")    ← 用户函数执行，返回内层 CF
     → g.result == null → UniRelay(f2, g) push 到 g.stack
     → copy.tryFire(SYNC) → g 还没完成 → return false
     → f2 还没完成 → uniCompose return false → tryFire return null
     → postComplete 继续 pop f1.stack ...
  
  3. g complete(42) → g.postComplete()
     → pop UniRelay → tryFire(NESTED)
     → f2.uniRelay(g) → f2.completeRelay(42)
     → f2 完成！→ f2.postComplete()
     → 继续触发 f2 的依赖...
```

### 11.7 UniApply vs UniCompose vs UniRelay 对比

| Completion | source | dep | fn | tryFire 行为 |
|-----------|--------|-----|----|-------------|
| **UniApply** | 前驱 CF | 当前 CF | `T→V` | `fn(src.result)` → `dep.completeValue(result)` |
| **UniAccept** | 前驱 CF | 当前 CF | `T→void` | `fn(src.result)` → `dep.completeValue(null)` |
| **UniRun** | 前驱 CF | 当前 CF | `()→void` | `fn.run()` → `dep.completeValue(null)` |
| **UniCompose** | 前驱 CF | 当前 CF | `T→CF<V>` | `fn(src.result)` → 得到内层 CF → 用 UniRelay 桥接 |
| **UniRelay** | 内层 CF | 外层 CF（dep）| 无 | 把 `src.result` 直接 relay 到 `dep.completeRelay(result)` |

### 11.8 claim · 单次触发竞争

```java
// UniCompletion.claim() 保证 tryFire 只被一个线程执行一次
final boolean claim() {
    Executor e = executor;
    if (compareAndSetForkJoinTaskTag((short)0, (short)1)) {  // CAS 0→1
        if (e == null) return true;   // 同步模式：直接执行
        executor = null;
        e.execute(this);              // 异步模式：丢到 executor
    }
    return false;   // CAS 失败 → 已被其他线程 claim 了
}
// ForkJoinTaskTag 是 ForkJoinTask 继承的字段（short 类型）
// UniCompletion 没有 ForkJoinTask 基类 → 用外部方式实现 claim
// （源码中 UniCompletion 的 claim 用的是不同的 CAS 机制）
```

---

## 十二、三大并发框架对比

| 维度 | **ThreadPoolExecutor** | **ForkJoinPool** | **Parallel Stream** |
|------|----------------------|-----------------|--------------------|
| 核心模型 | 公共队列 + N worker | 每个 worker 独立 WorkQueue + work-stealing | ForkJoinPool 上的分治 |
| 队列结构 | 一把锁的公共队列 | **N 个无锁双端队列** | ForkJoinPool 的 WorkQueue |
| 任务获取 | 竞争锁取任务 | **本地无锁 pop + 跨队列 CAS steal** | ForkJoinPool 内部 |
| 负载均衡 | 锁竞争 + 固定策略 | **自动窃取（忙的 worker 任务被偷）** | ForkJoinPool 自动 |
| join 语义 | 不支持 | **worker 帮忙偷任务加速** | 由 ForkJoinPool 支持 |
| 适用场景 | IO 密集 / 异构任务 | CPU 密集 / 分治算法 | 集合并行操作 |
| 性能（分治场景）| 基准 | **2-5x** | 依赖 ForkJoinPool |
| 性能（IO 场景）| **最优** | 较差（不适合 IO）| 不适用 |
| 复杂度 | ⭐⭐ 可配置队列+拒绝策略 | ⭐⭐⭐⭐⭐ 无锁 work-stealing | ⭐ 简单 API |

---

**文档版本：v1.1 · 2026-10-19**  
**源码基线：JDK 1.8.0_451**  
**v1.1 更新：scan checkSum 深入解析（稳定性判断 + xorshift 随机 + scanState 状态机）+ CompletableFuture thenCompose 异步链（UniCompose + UniRelay 桥接 + postComplete NESTED 模式）**
