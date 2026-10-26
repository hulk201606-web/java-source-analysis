# 09 · Stream API 源码深度解析

> 源码位置：`src/java/util/stream/`（36 个文件）  
> 面试权重：⭐⭐⭐⭐⭐（惰性求值 + 反向 Sink 链 + Spliterator + 并行分治）  
> 前置：建议先理解 ForkJoinPool（并行流底层）+ 函数式接口

---

## 一、核心架构总览

### 1.1 一句话设计

**Stream 是一个惰性求值的 Pipeline：source → N 个中间操作 → terminal，terminal 触发时才把所有中间操作包装成一个链式 Sink，用 Spliterator 遍历 source 数据。**

### 1.2 核心五件套

```
① BaseStream<T,S>        → Stream / IntStream / LongStream / DoubleStream
     原始特化避免装箱！（IntStream 用 int 不是 Integer）

② AbstractPipeline       → 双向链表：sourceStage ← prevStage ← ... ← this(nextStage)
     每个中间操作返回新的 Pipeline 节点，串成一条链

③ Sink<T>                → 操作链的"桶"接口：begin → accept(×N) → end
     filter/map 实现 opWrapSink 返回 Sink.ChainedReference

④ Spliterator<T>         → source 的数据迭代器：tryAdvance / trySplit / estimateSize
     trySplit 支持并行流分治

⑤ TerminalOp<E_IN,R>     → 终端操作：evaluateSequential + evaluateParallel
     forEach / reduce / collect / max / count 等
```

### 1.3 Pipeline 构建 vs 求值

```java
// ① 构建阶段：什么都不做，只串 Pipeline 链
Stream<String> s = strings.stream()          // sourceStage (ReferencePipeline.Head)
    .filter(s -> s.startsWith("A"))          // prevStage 指向 sourceStage
    .mapToInt(String::length)                // prevStage 指向 filterStage
    .max();                                  // prevStage 指向 mapToIntStage（不是 terminalOp！）

// ② 求值阶段：terminalOp 触发 → 反向 wrapSink 链 + spliterator 遍历
int maxLen = s.get();   // ← 这才是 terminalOp
```

---

## 二、Pipeline 链式结构

### 2.1 AbstractPipeline 双向链表

**源码锚点**：`AbstractPipeline.java` L72-213

```java
abstract class AbstractPipeline<E_IN, E_OUT, S extends BaseStream<E_OUT, S>>
        extends PipelineHelper<E_OUT> implements BaseStream<E_OUT, S> {

    private final AbstractPipeline<?, E_IN, ?> previousStage;   // ★ 前驱
    private AbstractPipeline nextStage;                          // ★ 后继

    AbstractPipeline(AbstractPipeline<?, E_IN, ?> previousStage, int opFlags) {
        if (previousStage.linkedOrConsumed) throw new IllegalStateException(MSG_LINKED);
        previousStage.linkedOrConsumed = true;
        previousStage.nextStage = this;     // ★ 串成双向链表
        this.previousStage = previousStage;
        this.sourceStage = previousStage.sourceStage;   // 都指向 sourceStage
        this.depth = previousStage.depth + 1;            // 深度 +1
        ...
    }
}
```

### 2.2 Pipeline 链表图解

```java
strings.stream()                          // Head (sourceStage, depth=0)
    .filter(...)                          // StatelessOp, prev=Head, next=null
    .mapToInt(...)                        // StatelessOp, prev=filterStage, next=null
    .max()                                // ← 这里还没串！max() 返回 TerminalOp，不返回 Stream

// 链表结构（前向）：
Head(sourceStage) → filterStage → mapToIntStage → null

// 链表结构（反向）：
null ← Head ← filterStage ← mapToIntStage ← terminal Op 触发时反向遍历
```

### 2.3 SourceStage · Head 节点

```java
// ReferencePipeline.Head：sourceStage 构造时创建
// sourceSpliterator / sourceSupplier 保存 source 的 Spliterator
// sourceStage.sourceAnyStateful = false（初始）
// sourceStage.depth = 0
```

### 2.4 StatefulOp vs StatelessOp

| 类型 | 代表 | 含义 | 并行影响 |
|------|------|------|---------|
| **StatelessOp** | filter / map / peek / mapToInt | 无状态，每个元素独立处理 | 可以和后续操作合并（融合）|
| **StatefulOp** | sorted / distinct / limit / skip | 有状态（要存数据）| **强制切断并行流**（每个 stateful 操作是一个并行段边界）|

```java
// opIsStateful 判断：
StreamOpFlag.IS_SORTED / IS_DISTINCT / IS_SHORT_CIRCUIT 等
→ parallel + stateful = 分段并行（stateful 之前的操作并行，之后串行）
```

---

## 三、Sink · 桶接口核心

**源码锚点**：`Sink.java` + `ReferencePipeline.java` filter/map

### 3.1 Sink 接口生命周期

```java
// 每个 Sink 代表 Pipeline 中的一个操作
// 生命周期：begin → accept × N → end
public interface Sink<T> {
    default void begin(long size) { }            // 通知即将开始，size 是预估元素数（-1=未知）
    void accept(T t);                            // 接收一个元素（核心！）
    default void end() { }                       // 通知结束
    default boolean cancellationRequested() { return false; }   // 短路信号
}

// 原始特化版本（避免装箱）：
Sink.OfInt / Sink.OfLong / Sink.OfDouble
// 每个都有 accept(int) / accept(long) / accept(double)
// default accept(Object) → 拆箱后调用 primitive 版本
```

### 3.2 ChainedReference · 链式包装器

**源码锚点**：`ReferencePipeline.java` filter 实现 L165-178

```java
// filter 的 opWrapSink 返回 ChainedReference
// → 把 filter 的逻辑"嵌进" Sink 链中
Sink<P_OUT> opWrapSink(int flags, Sink<P_OUT> sink) {
    return new Sink.ChainedReference<P_OUT, P_OUT>(sink) {
        @Override
        public void begin(long size) {
            downstream.begin(-1);   // ★ filter 后大小未知 → -1
        }
        @Override
        public void accept(P_OUT u) {
            if (predicate.test(u))  // ★ 有条件地传递给 downstream
                downstream.accept(u);
        }
    };
}

// map 的 opWrapSink
Sink<P_OUT> opWrapSink(int flags, Sink<R> sink) {
    return new Sink.ChainedReference<P_OUT, R>(sink) {
        @Override
        public void accept(P_OUT u) {
            downstream.accept(mapper.apply(u));   // ★ 先 map 再传给 downstream
        }
    };
}
```

### 3.3 ChainedReference 结构

```java
// ChainedReference 持有 downstream（下一个 Sink）
static abstract class ChainedReference<T, E_OUT> implements Sink<T> {
    protected final Sink<? super E_OUT> downstream;
    public ChainedReference(Sink<? super E_OUT> downstream) {
        this.downstream = downstream;
    }
    // default begin/end/cancellationRequested 都转发给 downstream
}
```

---

## 四、Spliterator · 可分裂迭代器

**源码锚点**：`Spliterator.java` L296-700

```java
public interface Spliterator<T> {
    // 关键三方法：
    boolean tryAdvance(Consumer<? super T> action);  // 串行：有元素则消费 → true；无 → false
    Spliterator<T> trySplit();                        // ★ 并行：分裂出一半的 Spliterator（返回 null 表示不可再分）
    long estimateSize();                               // 预估剩余元素数（返回 exactSizeIfKnown 更准）
    int characteristics();                            // 特性位：ORDERED / DISTINCT / SORTED / SIZED / NONNULL / IMMUTABLE / CONCURRENT / SUBSIZED
}
```

### 4.1 Spliterator 特性位

```
ORDERED：按顺序（List.forEachOrdered）
DISTINCT：无重复（Set）
SORTED：有序（TreeSet）
SIZED：大小已知（ArrayList）
SUBSIZED：trySplit 分裂的两半也有 SIZED（数组，不是链表）
CONCURRENT：可并发修改
IMMUTABLE：不可变

trySplit 能分裂的前提：有 SIZED 特性
→ ArrayList / 数组 Spliterator：可以分裂（SUBSIZED）
→ LinkedList：trySplit 返回 null（链表不能 O(1) 分裂）
```

### 4.2 常见 Spliterator 实现

| 实现 | trySplit | 说明 |
|------|---------|------|
| ArrayList.ListSpliterator | ✅ 可 | SUBSIZED：二分分裂 |
| 数组 Arrays.spliterator | ✅ 可 | SUBSIZED |
| HashMap.KeySpliterator | ✅ 可 | 桶数组二分 |
| LinkedList | ❌ null | 链表不能 O(1) 分裂 |
| StreamSupport.spliterator | 看源 | 包装已有 Spliterator |

---

## 五、evaluate · 求值流程核心

### 5.1 AbstractPipeline.evaluate 完整流程

**源码锚点**：`AbstractPipeline.java` + `PipelineHelper`

```java
// TerminalOp 触发：s.get() → reduceOps.makeTerminal() → evaluateParallel() / evaluateSequential()

// PipelineHelper 的 evaluateSequential（并行场景也可能回退）
final <P_IN> R evaluateSequential(PipelineHelper<E_OUT> helper, Spliterator<P_IN> spliterator) {
    // ① 反向 wrapSink：从 terminal 到 source
    Sink<P_OUT> sink = ...;   // terminalOp 提供的 terminal Sink
    sink = helper.wrapSink(sink);   // ★ 反向包一层 intermediate Op
    // ② 正向遍历：spliterator.forEachRemaining → sink
    // ③ sink.begin → spliterator.forEachRemaining(sink::accept) → sink.end
}

// wrapSink 的反向构造链（关键！）：
// 从 terminalStage 开始，反向遍历 prevStage
// 每个 intermediate 调 opWrapSink 把 sink 再包一层
// 最终得到：source → filter → map → terminal Sink
// 这是反向构造 → 正向执行
```

### 5.2 wrapSink 反向构造图解

```java
// 用户代码：
strings.stream()          // Head (sourceStage)
    .filter(...)          // filterStage (OpFlag=NOT_SIZED)
    .mapToInt(...)        // mapToIntStage (OpFlag=NOT_SORTED|NOT_DISTINCT)
    .max();               // terminalOp

// evaluateSequential 内部：
// ① terminalOp.makeSink() → terminalSink（ReduceOps.ReduceSink）
// ② helper.wrapSink(terminalSink) 从 terminalStage 反向遍历：

//   mapToIntStage.opWrapSink(flags, terminalSink)
//     → ChainedInt(terminalSink) { accept(int) → terminalSink.accept(mapper.applyAsInt(u)) }
//     返回：[mapToInt Sink → terminal Sink]

//   filterStage.opWrapSink(flags, 上面返回的 Sink)
//     → ChainedReference(上面返回的 Sink) { accept(u) → predicate.test(u) ? downstream.accept(u) : nothing }
//     返回：[filter Sink → mapToInt Sink → terminal Sink]

//   sourceStage（Head）没有 opWrapSink（是 source 不是 intermediate）
//     → 返回：[filter Sink → mapToInt Sink → terminal Sink]

// ③ 正向执行：
//   sourceStage.spliterator.forEachRemaining(filter Sink::accept)
//     → filter.accept → predicate.test → mapToInt.accept → mapper → terminal.accept
//   sink.begin() → forEachRemaining → sink.end()
```

### 5.3 完整求值时序

```
                    filterStage          mapToIntStage         terminal
                      │                    │                  │
  Spliterator.begin ──┤                    │                  │
                      │ sink.begin         │                  │
  [s1] ──────────────► filter.accept ─────► filter.test ────► false → drop
                      │                    │
  [s2] ──────────────► filter.accept ─────► filter.test ────► true
                      │                    │                  │
                      │                    │  mapToInt.accept │
                      │                    │  mapper.applyAsInt│
                      │                    │ ───────────────► terminal.accept(123)
                      │                    │                  │
  ...                  ...                  ...                ...
                      │                    │                  │
  Spliterator.end ────┤                    │                  │
                      │ sink.end           │                  │
```

---

## 六、并行流 · evaluateParallel

### 6.1 并行 vs 串行

```java
strings.stream().filter(...).map(...).max();          // 串行：ForkJoinPool 单线程
strings.parallelStream().filter(...).map(...).max();  // 并行：ForkJoinPool.commonPool() 多线程

// parallelStream 内部：
sourceStage.sourceAnyStateful = false → sourceStage.parallel = true
// evaluateParallel 时会走 AbstractTask 分治框架
```

### 6.2 AbstractTask 分治框架

**源码锚点**：`AbstractTask.java`

```java
// 并行流底层 = ForkJoinPool + Spliterator.trySplit
// AbstractTask 是 ForkJoinTask 子类
// 每个 Task 负责一段 Spliterator 数据

// 分治策略：
// initialTask → 调 Spliterator.trySplit() 把 source 分成两半
//   前半 → new AbstractTask(前半) 提交给 ForkJoinPool
//   后半 → 当前 Task 继续处理（或再分裂）
// 递归直到 trySplit 返回 null 或 split 后的 size 太小（阈值！）
// 最后各 Task 用 terminalOp 的 evaluateSequential 处理自己那一段
// 结果用 CombineOp（二元函数）合并

// 阈值：
// ForkJoinPool.commonPool().parallelism × 8 = 每段最小元素数
// ArrayList 1000 元素 × 4 核 = 每段 8/1000 大约分 125 段
```

### 6.3 并行分治图解

```
parallelStream().map().max()
  │
  ├─ initialTask → trySplit() → split1[0..499] + split2[500..999]
  │   │
  │   ├─ split1 再 split → s1a[0..249] + s1b[250..499]
  │   │   ├─ s1a: evaluateSequential(terminalOp) → result1
  │   │   ├─ s1b: evaluateSequential(terminalOp) → result2
  │   │   └─ combine(result1, result2) → resultAB
  │   │
  │   ├─ split2 再 split → s2a[500..749] + s2b[750..999]
  │   │   ├─ s2a: evaluateSequential → result3
  │   │   ├─ s2b: evaluateSequential → result4
  │   │   └─ combine(result3, result4) → resultCD
  │   │
  │   └─ combine(resultAB, resultCD) → max ✅
```

### 6.4 StatefulOp 强制串行

```java
// sorted / distinct 等 StatefulOp 会强制切断并行
// AbstractPipeline.sourceSpliterator() 中有处理：
if (isParallel() && sourceStage.sourceAnyStateful) {
    for (AbstractPipeline p = sourceStage; p != terminal; p = p.nextStage) {
        if (p.opIsStateful()) {
            depth = 0;
            // ★ 用 p.opEvaluateParallelLazy(u, spliterator) 把前一段并行跑完
            // 然后 StatefulOp 之后的代码走串行（sourceStage.sourceAnyStateful = true）
            spliterator = p.opEvaluateParallelLazy(u, spliterator);
        }
    }
}

// 原因：sorted 需要全局数据才能排序 → 必须先合并所有分片
// → sorted 之前的操作可以并行，sorted 之后必须串行
```

---

## 七、惰性求值 & 短路

### 7.1 为什么是惰性的？

```
用户调用 stream() / filter() / map() / limit() 时：
  → 只是创建新的 AbstractPipeline 节点，串到链表上
  → 不遍历数据，不执行任何函数，不分配任何临时对象
  → 为什么？因为不知道终端操作是什么！

如果中间操作做了数据遍历：
  → 无法知道要不要短路（findFirst / anyMatch 可以提前停）
  → 无法并行（并行需要 Spliterator）
  → 无法优化融合（filter+map 可以融合成一次遍历）

→ 全部延迟到终端操作！这就是"惰性"的含义
```

### 7.2 短路操作

| 终端操作 | 短路 | 说明 |
|---------|------|------|
| `findFirst` / `findAny` | ✅ | 找到就停，`cancellationRequested() → true` |
| `anyMatch` | ✅ | 有一个 true 就停 |
| `allMatch` | ✅ | 有一个 false 就停 |
| `noneMatch` | ✅ | 有一个 true 就停 |
| `forEach` | ❌ | 必须遍历全部 |
| `reduce` / `collect` / `max` / `min` | ❌ | 必须遍历全部 |

### 7.3 短路实现原理

```java
// 中间操作 filter 的 accept：
public void accept(P_OUT u) {
    if (predicate.test(u))
        downstream.accept(u);
}

// 终端操作 findFirst 的 accept：
public void accept(P_OUT u) {
    if (value == null) value = u;    // 存第一个
    // 不设置 cancellationRequested → 继续遍历？
    // 实际：terminalOp.makeSink() 返回的 Sink 有自己的 cancellationRequested
}

// 上游 Sink.begin 会检查 downstream.cancellationRequested()
// Sink.ChainedReference.default accept：
public void accept(P_OUT u) {
    if (!downstream.cancellationRequested())
        downstream.accept(u);
}

// → 终端 Sink 设置 value 后，cancellationRequested() → true
// → 所有中间 Sink 下一次 accept 时看到 downstream 取消 → 停止转发
// → Spliterator.forEachRemaining 的实现是：
//   while (spliterator.tryAdvance(sink)) {
//     if (cancellationRequested()) break;
//   }
```

---

## 八、Collectors · 收集器框架

### 8.1 Collector 接口

```java
public interface Collector<T, A, R> {
    Supplier<A> supplier();                    // 创建累加容器
    BiConsumer<A, T> accumulator();            // 消费一个 T，累加到 A
    Function<A, R> finisher();                 // 把 A 转成 R（可选：IDENTITY_FINISH）
    BinaryOperator<A> combiner();              // 并行时合并两个 A
    Set<Characteristics> characteristics();    // 特性：IDENTITY_FINISH / CONCURRENT / UNORDERED
}
```

### 8.2 常用 Collectors

```java
Collectors.toList()              // → ArrayList
Collectors.toSet()               // → HashSet
Collectors.toMap(keyFn, valFn)   // → HashMap
Collectors.groupingBy(classifier)// → Map<K, List<T>>
Collectors.partitioningBy(pred)  // → Map<Boolean, List<T>>
Collectors.summingInt/maxBy/minBy
Collectors.joining(sep, prefix, suffix)
Collectors.toCollection(Supplier)
Collectors.collectingAndThen(downstream, finisher)
Collectors.mapping(f, downstream)
Collectors.flatMapping(f, downstream)
Collectors.filtering(pred, downstream)
Collectors.teeing(downstream1, downstream2, merger)
```

### 8.3 groupingBy + 并行陷阱

```java
// 串行：groupingBy 用一个 HashMap → 多线程安全问题？串行所以没问题
// 并行：groupingBy 会用多个 HashMap（每个分片一个）→ 最后 combiner 合并
//       combiner 是 HashMap.putAll → 多线程合并 HashMap 是安全的吗？
//       → HashMap 不是线程安全的！但合并是 ForkJoinTask 的单线程做的 → 没问题

// ⚠️ 陷阱：parallelStream + toList/toSet 是安全的（每个分片独立合并）
// ⚠️ 陷阱：parallelStream + 自定义 Collector 必须确保 combiner 线程安全！

// ✅ 官方推荐：parallelStream + groupingByConcurrent（用 ConcurrentHashMap）
// groupingByConcurrent 的特性包含 CONCURRENT → 不需要多 HashMap 合并
// 每个分片直接 put 到同一个 ConcurrentHashMap
```

---

## 九、面试考点补全

| # | 考点 | 答案 |
|---|------|------|
| **惰性求值** | | |
| 1 | Stream 为什么惰性？| 构建时只串 Pipeline 链表，不遍历数据。延迟到 terminalOp 才：①反向 wrapSink 链 ②spliterator 遍历。原因：需要支持短路/并行/融合 |
| 2 | Stream.build 阶段做了什么？| 只创建 AbstractPipeline 节点 → 串成双向链表（prevStage / nextStage）→ 不分配临时对象、不执行函数 |
| **Sink 机制** | | |
| 3 | Sink 的生命周期？| begin(size) → accept × N → end() + cancellationRequested 短路信号 |
| 4 | filter 的 opWrapSink 做什么？| 返回 ChainedReference(downstream)，accept 里 predicate.test(u) 才传递给 downstream |
| 5 | Sink 链是正还是反向构造？| **反向！** 从 terminalStage 反向遍历 prevStage 调 opWrapSink，正向执行（source → terminal）|
| 6 | begin(size) 的 size 是怎么来的？| source Spliterator.estimateSize()。filter 后 → -1（未知）；map 后 → 不变 |
| **Spliterator** | | |
| 7 | Spliterator.trySplit 返回 null 会怎样？| 并行分治停止（当前 Task 成为叶子）|
| 8 | LinkedList 能并行流吗？| 理论可以但效率极低——LinkedList Spliterator 的 trySplit 返回 null（链表不能 O(1) 分裂）→ 退化串行 |
| 9 | trySplit 能分裂的前提？| **SUBSIZED 特性**（trySplit 分裂的两半也有 SIZED）|
| **并行流** | | |
| 10 | 并行流底层用什么线程池？| **ForkJoinPool.commonPool()**（parallelism = CPU 核数 -1）|
| 11 | 并行流分治阈值多少？| ForkJoinPool.parallelism × 8 = 每段最小元素数 |
| 12 | StatefulOp 会强制串行吗？| **是的！** sorted / distinct 等会切断并行段——之前的并行跑完后，之后的代码串行 |
| 13 | 并行流一定更快吗？| **不一定！** 数据量小、并行开销大；LinkedList 不能分裂 → 退化串行；共享变量 → 锁竞争 |
| 14 | parallel() 后再 sequential() 会怎样？| 最后一个模式生效——sequential() 覆盖 parallel() |
| **短路** | | |
| 15 | findFirst 怎么短路？| 终端 Sink.accept 存第一个值后，cancellationRequested()→true；中间 Sink 检查 downstream 取消 → 停止转发 |
| 16 | anyMatch 怎么短路？| 找到 true → 之后全部 accept 都被 cancellationRequested 拦截 |
| **Collectors** | | |
| 17 | groupingBy + 并行流安全吗？| 安全（每个分片独立 HashMap → ForkJoinTask 单线程合并）。但推荐 groupingByConcurrent（ConcurrentHashMap 直接 put）|
| 18 | collect 的 Combiner 什么时候用？| **并行流**：每个分片各自 accumulator，最后 combiner 合并 |
| 19 | Collectors.CONCURRENT 特性含义？| 多个线程可以安全地共享同一个累加容器 → groupingByConcurrent 的特性 |
| **其他** | | |
| 20 | Stream 只能用一次？| 是的！terminalOp 求值后 sourceStage.linkedOrConsumed = true → 再次 terminalOp 抛 IllegalStateException |
| 21 | map vs flatMap？| map: T→R（一对一）；flatMap: T→Stream\<R\>（一对多扁平化）|
| 22 | limit / skip 的开销？| limit 让 Spliterator 遍历提前终止（tryAdvance 返回 true 直到达到 count）；skip 让 Spliterator 跳过前 N 个元素 |
| 23 | peek 为什么叫"调试用"？| peek 是 StatelessOp，不改变数据，只做副作用；但并行流中副作用的顺序不确定 → 生产代码慎用 |

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线：JDK 1.8.0_451**  
**覆盖：Pipeline 双向链表 + Sink 反向链式包装 + Spliterator 可分裂迭代 + evaluate 完整流程 + 并行流分治 + StatefulOp 强制串行 + 短路机制 + Collectors 框架 + 23 个面试考点**
