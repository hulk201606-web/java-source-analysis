# 04 · JVM 与 GC 源码解析

> 源码位置：`src/java/lang/`（Object.mark word）+ HotSpot JVM 层  
> 面试权重：⭐⭐⭐⭐

---

## 一、运行时数据区全景

```
JVM 进程内存布局（HotSpot 64-bit）：

┌──────────────────────────────────────────────────────────────┐
│ Native Memory（堆外，不受 GC 管辖）                           │
│  ┌──────────┬──────────┬───────────────┬──────────────────┐ │
│  │JVM Args  │ClassLoader│ Thread Stacks  │ Direct Memory    │ │
│  │/常量池    │ / MetaSpace│ (每线程一份)  │ (NIO DirectBuffer)│ │
│  └──────────┴──────────┴───────────────┴──────────────────┘ │
├──────────────────────────────────────────────────────────────┤
│ Heap（堆，GC 管理）                                          │
│  ┌──────────────────┐  ┌──────────────┐  ┌───────────────┐ │
│  │ Young Gen ≈1/3    │  │ Old Gen ≈2/3 │  │ Metaspace      │ │
│  │ Eden(80%)+S0+S1  │  │ 大对象/长寿   │  │ 类元数据       │ │
│  └──────────────────┘  └──────────────┘  └───────────────┘ │
└──────────────────────────────────────────────────────────────┘

参数示例：-Xmx4g -Xms2g -XX:MetaspaceSize=256m -XX:MaxDirectMemorySize=512m
```

---

## 二、对象内存布局

**源码锚点**：`Object.java` L43-45（mark word 提及）

### 2.1 HotSpot 对象内存布局（64 位）

```
┌───────────────────────────────────┐
│  mark word (64 bits)              │  ← 锁状态存在这里！
├───────────────────────────────────┤
│  klass 指针 (32 bits，指针压缩后)   │  ← 指向 Class 对象
├───────────────────────────────────┤
│  实例字段 (int/double/引用...)      │
├───────────────────────────────────┤
│  对齐填充 (凑 8 字节边界)          │
└───────────────────────────────────┘

mark word 的 64 bits 分配（核心！）：
┌──────────────────────────────────────────────────────────────┐
│ 状态        │ bit 布局                                        │
├──────────────────────────────────────────────────────────────┤
│ 🟢 无锁     │ hashcode(31) │ GC年龄(4) │ 001                 │
│ 🔵 偏向锁   │ 线程ID(54)   │ 时间戳(2) │ GC年龄(4) │ 101      │
│ 🟡 轻量级锁 │ LockRecord 指针(62) ─────────────────── │ 00     │
│ 🔴 重量级锁 │ ObjectMonitor 指针(62) ─────────────────│ 10     │
└──────────────────────────────────────────────────────────────┘

锁标记：bit0; 类型：bit1
  001=无锁, 101=偏向, 00=轻量, 10=重量
```

### 2.2 对象分配流程

```java
User u = new User();

HotSpot 内部路径：
  ├─ 大对象（> -XX:PretenureSizeThreshold，默认 0）
  │   → 直接进 Old Gen（避免 Young Gen 反复搬）
  │
  ├─ Eden 有空间？← TLAB（Thread Local Allocation Buffer）
  │   → 在当前线程的 TLAB 里分配（bump-the-pointer 指针碰撞，O(1)）
  │     TLAB 满了 → 从 Eden 再分一块 TLAB
  │     Eden 没空间 → 触发 Young GC
  │
  └─ 老年代空间？
      → 空闲列表（Free List）分配
```

**TLAB 是什么？**
```
Thread Local Allocation Buffer：
  每个线程在 Eden 里独占一小块（默认 1/64，-XX:TLABSize）
  → 避免多线程同时在 Eden 里分配的 CAS 竞争
  → 指针碰撞（bump-the-pointer）：移动一个指针就完成分配
  → 线程安全且极快！
```

### 2.3 对象晋升（Young → Old）

| 条件 | 方式 | JVM 参数 |
|------|------|---------|
| 年龄达到阈值 | Young GC 后存活，年龄+1，≥MaxTenuringThreshold 晋升 | `-XX:MaxTenuringThreshold=15` |
| Survivor 不足 | Survivor 放不下所有存活对象，多出来的直接晋升 | 自动 |
| 动态年龄计算 | Survivor 相同年龄对象总大小 > Survivor 一半 | `-XX:TargetSurvivorRatio=50` |

---

## 三、GC 算法

### 3.1 四种基础 GC 算法

| 算法 | 原理 | 优点 | 缺点 | 适用代 |
|------|------|------|------|--------|
| **标记-清除** | 1. GC Roots 标记存活 2. 清除未标记 | 回收循环引用 | 碎片化 | 老年代（CMS）|
| **复制** | 1. 把存活复制到另一半 2. 清空原块 | 无碎片 + 指针碰撞分配 | 浪费 50% | 新生代 |
| **标记-整理** | 1. 标记存活 2. 向一端整理 3. 清空边界外 | 无碎片 | 整理开销大 | 老年代 |
| **分代收集** | 新生代用复制；老年代用标记-清除/整理 | 组合最优 | - | HotSpot 默认 |

### 3.2 HotSpot 五大 GC 组合

| GC 名称 | 新生代 | 老年代 | 特点 |
|--------|--------|--------|------|
| **Serial / Serial Old** | 复制（STW 单线程）| 标记-整理（STW 单线程）| 最简单，-client 默认 |
| **Parallel Scavenge / Parallel Old** | 复制（STW 多线程）| 标记-整理（STW 多线程）| -server 默认，吞吐量优先 |
| **ParNew / CMS** | 复制（STW 多线程）| **标记-清除（大部分并发）**| 低延迟 |
| **G1** | Region 化（混合）| Region 化（混合）| JDK9 默认，可预测停顿 |
| **ZGC / Shenandoah** | Region 化（并发）| Region 化（并发）| 亚毫秒停顿 |

### 3.3 CMS · 经典低延迟 GC

```
CMS 阶段：
  1. Initial Mark（STW，极短）
     → 标记 GC Roots 直接引用的对象
  2. Concurrent Mark（并发）
     → 从 1 的结果向下遍历整个对象图
  3. Remark（STW，短）
     → 修 2 中被业务线程修改的引用（写屏障 dirty card）
  4. Concurrent Sweep（并发）
     → 清除未标记对象（产生碎片！）

CMS 缺点：
  产生碎片 → 需要 Full GC 整理
  必须预留空间（老年代 90% 以上就可能触发 GC）
  浮动垃圾（CMS 期间新产生的垃圾只能等下一次）
```

### 3.4 G1 · Region 化革命

```
G1 核心：把堆分成 2048 个 Region（每个 1-32MB，可调）
  每个 Region 可动态扮演 Eden/Survivor/Old/Humongous

G1 优势：
  可预测停顿：-XX:MaxGCPauseMillis=N，G1 调整每次收集的 Region 数
  增量整理：每次只回收少量 Region（复制算法搬到空 Region）
  大对象友好：Humongous Region 存 > Region/2 的对象
  自动调优：后台统计每个 Region 回收价值，优先高价值的

G1 阶段：
  Young GC → Eden Region 存活复制到 Survivor/Old Region
  Mixed GC → Young GC 后顺便回收部分 Old Region（价值高的）
```

### 3.5 GC Roots · "谁是存活的根？"

```
HotSpot 中 GC Roots：
  1. 虚拟机栈中的局部变量引用（每个线程每个栈帧）
  2. 方法区中的静态变量引用（static 字段）
  3. 方法区中的常量引用（字符串常量池、类常量）
  4. 本地方法栈中的 JNI 引用
  5. 被 synchronized 持有的对象（monitor）
  6. 活跃线程（Thread 实例本身）
```

### 3.6 Stop-The-World（STW）

```
GC 需要 STW 是因为：遍历对象引用时业务线程不能同时改引用

解决方案：
  1. 经典 STW → 所有线程跑到 safepoint 后挂起
  2. CMS 只在 Initial Mark 和 Remark STW → 大部分并发
  3. G1/ZGC 用 着色指针 + 读屏障 → 几乎无 STW

safepoint（安全点）：
  HotSpot 在方法返回、循环尾部、方法调用等位置插入 safepoint
  GC 请求后，所有线程跑到最近的 safepoint 挂起
```

---

## 四、synchronized 锁升级（HotSpot 实现）

### 4.1 完整升级路径

```
无锁(001) ──▶ 偏向锁(101) ──▶ 轻量级锁(00) ──▶ 重量级锁(10)

  │                │               │
  │ 同一线程反复    │ 两个线程竞争    │ 真正并发 CAS 失败
  │ 用同一把锁      │ 偏向撤销      │
  ▼                ▼               ▼

  CAS 把 mark word  CAS 撤销偏向    CAS 抢占 LockRecord
  写成线程 ID       进入轻量级锁   竞争失败 → CAS 改重量级
                                  → ObjectMonitor 放堆上
                                  → 竞争线程 park 阻塞（BLOCKED）
```

### 4.2 ObjectMonitor 与 AQS 对比

| 维度 | HotSpot ObjectMonitor | AQS（ReentrantLock）|
|------|----------------------|---------------------|
| 状态字段 | `_recursions` + `_owner` | `volatile int state` |
| 队列 | CXQ + EntryList + WaitSet（3 个！）| Sync Queue + Condition Queue（2 套）|
| 锁类型 | JVM 内置 | JDK 层实现 |
| 公平性 | 非公平（CXQ 抢占）| 可配置 |
| Condition | 只能 wait/notify（1 个 WaitSet）| 多个 ConditionObject |

### 4.3 JVM 调优参数

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `-XX:+UseBiasedLocking` | true | 启用偏向锁（JDK8 默认）|
| `-XX:PreBlockSpin` | 10 | 重量级锁自旋次数 |
| `-XX:+UseG1GC` | false | 使用 G1（JDK9 默认 true）|
| `-XX:MaxGCPauseMillis` | 200 | G1 目标停顿时间 |
| `-Xmn` | 无 | 设置新生代大小 |
| `-XX:+PrintGCDetails` | false | 打印详细 GC 日志 |

---

## 五、面试高频考点汇总

| # | 考点 | 答案 |
|---|------|------|
| 1 | JVM 堆分代结构 | Young（Eden+S0+S1 ≈1/3）+ Old（≈2/3）+ Metaspace |
| 2 | 为什么分代？ | 弱分代假说：90%+ 对象朝生夕灭。新生代用高频复制，老年代用低频整理 |
| 3 | 对象何时晋升老年代？ | 年龄≥MaxTenuringThreshold(15) / Survivor 放不下 / 大对象直接进 |
| 4 | CMS 为什么有碎片？ | 标记-清除不整理；需要 Full GC。G1 用 Region 化解决 |
| 5 | GC Roots 有哪些？ | 局部变量 / 静态变量 / 常量池 / JNI 引用 / monitor / 活跃线程 |
| 6 | STW 是什么？ | GC 暂停所有业务线程（safepoint）。CMS 只在 Initial Mark 和 Remark STW |
| 7 | TLAB 是什么？ | 每个线程在 Eden 独占一小块 → 指针碰撞 O(1) 分配 → 避免 CAS 竞争 |
| 8 | mark word 四种状态？ | 001=无锁 / 101=偏向 / 00=轻量 / 10=重量 |
| 9 | 锁升级完整路径？ | 无锁→偏向锁（同一线程反复用）→轻量级锁（交替用）→重量级锁（真正并发）|
| 10 | ObjectMonitor 与 AQS 队列对比？ | Monitor：CXQ+EntryList+WaitSet（3 个）；AQS：Sync Queue + Condition Queue（2 套）|

---

**文档版本：v1.0 · 2026-10-19**  
**源码基线：JDK 1.8.0_451**
