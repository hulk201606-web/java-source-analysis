# JDK 1.8.0_451 核心源码注释总览

> 本项目是 Oracle JDK 1.8.0_451 安装目录 `src` 源码的工作区副本，在保留官方英文 Javadoc 的基础上，补充中文逻辑梳理注释。
> 注释策略：**不重复翻译官方 Javadoc**，增量价值集中在 **JVM 底层实现细节、设计意图、算法图解、面试考点** 四个维度。

---

## 📁 已注释核心文件清单

| 包路径 | 类名 | 注释密度 | 核心覆盖点 |
|--------|------|----------|------------|
| java.lang | Object | ⭐⭐⭐⭐⭐ | JVM 对象布局 + mark word + 两大契约 + wait/notify monitor 机制 + clone 浅拷贝 + finalize 废弃原因 |
| java.lang | String | ⭐⭐⭐⭐⭐ | 不可变性设计意图 + hash Horner 法则 + 31 乘数选择 + intern 池 + substring JDK7 内存泄漏修复 |
| java.lang | Thread | ⭐⭐⭐⭐ | 线程 6 状态机图 + start() vs run() 本质区别 + sleep vs wait vs yield 对比 + interrupt 协作式中断 + daemon 守护线程 |
| java.util | HashMap | ⭐⭐⭐⭐⭐ | 2 的幂容量设计基石 + TREEIFY_THRESHOLD=8 泊松分布依据 + hash 扰动函数 (h ^ h>>16) + resize 扩容拆分逻辑 (hash & oldCap) |
| java.util | ArrayList | ⭐⭐⭐ | 1.5 倍扩容 + System.arraycopy 性能 + fail-fast 迭代器 + Vector vs ArrayList |
| java.util | LinkedList | ⭐⭐⭐ | 双向链表结构 + first/last 指针不变量 + Deque 接口 + 时间复杂度对比 |
| java.util.concurrent | ConcurrentHashMap | ⭐⭐⭐⭐ | JDK8 CAS+synchronized 桶锁 + sizeCtl 多重语义 + ForwardingNode 扩容占位符 + Transfer 多线程协同 |
| java.io | InputStream | ⭐⭐⭐⭐ | 抽象模板方法设计 + 字节流体系族谱树形图 + 装饰器模式核心 + mark/reset 机制 |
| java.io | BufferedReader | ⭐⭐⭐ | 装饰器字符缓冲 + 内部字段语义（cb/nChars/nextChar） + readLine 行尾处理 + mark/reset 字符级实现 |
| java.io | InputStreamReader | ⭐⭐⭐ | 桥接模式 byte→char + StreamDecoder 内部结构 + Charset 默认值 + JDK11 FileReader 变化 |

---

## 🏗️ Java 集合框架数据结构对比表

| 特性 | HashMap | ArrayList | LinkedList | ConcurrentHashMap | TreeMap |
|------|---------|-----------|------------|-------------------|---------|
| 底层结构 | 2^n 桶数组 + 链表/红黑树 | Object[] 数组 | 双向链表 | 2^n 桶数组 + CAS+synchronized | 红黑树 |
| 默认容量 | 16 | 10 | -（无） | 16 | - |
| 扩容因子 | 2× | 1.5× | - | 2× | - |
| 遍历顺序 | ❌ 无序 | ✅ 插入序 | ✅ 插入序 | ❌ 无序 | ✅ key 排序 |
| null key | ✅ 允许 | N/A | N/A | ❌ 不允许 | ❌ 不允许 |
| 线程安全 | ❌ | ❌ | ❌ | ✅ CAS+sync | ❌ |
| get 复杂度 | O(1)/O(log n) | O(1) | O(n) | O(1)/O(log n) | O(log n) |
| put 复杂度 | O(1)/O(log n) | 均摊 O(1) | O(1) | O(1)/O(log n) | O(log n) |
| 红黑树阈值 | 链表≥8 且表≥64 | - | - | 链表≥8 且表≥64 | 天然红黑树 |

### 哈希碰撞处理策略对比

```
JDK 1.7 HashMap: 链表头插法（并发扩容可能成环死循环）
JDK 1.8 HashMap: 链表尾插法 + 链表→红黑树（阈值 8 / 64）
ConcurrentHashMap: 链表→红黑树（JDK8 CAS+synchronized）
                 ForwardingNode 占位 + Transfer 协同扩容
```

---

## 🧵 Java 线程状态机

```
                    ┌──────────────┐
                    │    NEW       │
                    └──────┬───────┘
                           │ start()
                           ▼
                    ┌──────────────┐
              ┌────▶│   RUNNABLE   │◀──────────────────┐
              │     └──────┬───────┘                   │
              │            │                           │
              │  抢锁失败   │ wait()/带超时sleep          │ 被 notify/sleep到期/中断
              │            ▼                           │
              │     ┌──────────────┐  ┌────────────────┴────────────┐
              │     │   BLOCKED    │  │         WAITING/TIMED_WAITING │
              │     │ (等锁)       │  │  (等通知/结果)              │
              │     └──────────────┘  └─────────────────────────────┘
              │                                    │
              │                                    │ 执行完毕/异常
              │                                    ▼
              │                           ┌──────────────┐
              └───────────────────────────│  TERMINATED  │
                                          └──────────────┘
```

**关键状态辨析**：
| 状态 | 触发条件 | 含义 |
|------|---------|------|
| BLOCKED | synchronized 竞争锁失败 | 锁在别的线程手里，当前线程只能等 |
| WAITING | Object.wait() 无超时 | 无限等 notify/notifyAll |
| TIMED_WAITING | Thread.sleep() / wait(timeout) | 带超时的等待 |
| RUNNABLE | 获得 CPU 时间片 | 线程在运行或就绪队列里等 CPU |

---

## 🔑 Object.wait() / notify() 执行流程

```
线程 A（消费者）                线程 B（生产者）
   │                             │
   ├─ synchronized(obj)          │
   │  └─ 获得 monitor            │
   ├─ obj.wait()  ────────────┐  │
   │  ├─ 释放 monitor          │  │
   │  └─ 进入 WaitSet          │  ├─ synchronized(obj)
   │                             │  └─ 获得 monitor
   │                             ├─ 生产数据
   │                             ├─ obj.notify()
   │                             │  └─ WaitSet → EntryList
   │                             ├─ release monitor
   │                             │
   ├─ 重新竞争 monitor ◀────────┘
   ├─ 获得锁，从 wait() 返回
   └─ 消费数据
```

**伪唤醒（Spurious Wakeup）**：必须用 `while` 循环重新检查条件，不能用 `if`
```java
synchronized (obj) {
    while (!condition)    // ✅ 正确：循环检查
        obj.wait();
    // while (!condition) // ❌ 错误：伪唤醒后立即跳过检查
}
```

---

## 📦 String 不可变性设计

### 为什么 String 要做成 final + 不可变？

1. **常量池复用**：同一字符串字面量共用一个对象，节省内存
   ```java
   String s1 = "hello";
   String s2 = "hello";
   // s1 == s2 为 true（常量池复用）
   // s1.equals(s2) 也为 true
   ```

2. **HashMap key 安全**：hashCode 不会在 put 之后改变
3. **JVM 常量折叠优化**：编译期 `"a" + "b"` 变成 `"ab"`
4. **天然线程安全**：无需任何同步即可多线程共享

### intern() 机制
```
new String("abc").intern()  →  检查常量池
   ├─ 已有 "abc" → 返回池中引用
   └─ 无 "abc"   → 放入池后返回新引用
```

---

## ⚡ HashMap 扩容拆分逻辑图解

**为什么 2 的幂容量 + (hash & oldCap) 判断 = O(1) 定位？**

```
扩容前: oldCap = 16 (10000₂)
        oldCap - 1 = 01111₂

节点 A: hash = 01011011
  旧索引 = 01011011 & 01111 = 0101 = 5
  hash & oldCap = 01011011 & 10000 = 00000 = 0  →  留在原位置 5

节点 B: hash = 01101011
  旧索引 = 01101011 & 01111 = 0101 = 5
  hash & oldCap = 01101011 & 10000 = 10000 ≠ 0  →  偏移 oldCap = 5 + 16 = 21

扩容后: newCap = 32 (100000₂)
        newCap - 1 = 011111₂

节点 A: hash & 011111 = 0101 = 5 ✅ 确实留在 5
节点 B: hash & 011111 = 10101 = 21 ✅ 确实在 21
```

**结论**：高位新增的那个 bit（即 `hash & oldCap` 的结果）决定了新位置。无需重新 hash！

---

## 📝 JVM 对象内存布局（HotSpot）

```
┌────────────────────────────────────┐
│  mark word (8 bytes)               │
│  ├─ hashcode                       │  ← hashCode() 计算后缓存这里
│  ├─ 锁状态 (无锁/偏向/轻量/重量)   │  ← synchronized 锁的状态
│  ├─ GC 分代年龄                     │
│  └─ 线程 ID / 锁记录指针            │
├────────────────────────────────────┤
│  klass 指针 (8 bytes on 64-bit)    │  ← 指向 Class 对象
├────────────────────────────────────┤
│  实例字段...                        │
│  (int a; double d; Object ref;)    │
├────────────────────────────────────┤
│  对象对齐填充 (到 8 字节边界)       │
└────────────────────────────────────┘
```

---

## 📖 Java IO 流体系（新增）

### 字节流族谱（InputStream / OutputStream）

```
InputStream（抽象基类，唯一必须实现 read()）
  ├─ FileInputStream        → 文件字节读取
  ├─ ByteArrayInputStream   → 内存 byte[] 数据源
  ├─ PipedInputStream        → 管道（线程间字节通信）
  └─ FilterInputStream（装饰器基类，持有 protected InputStream in）
      ├─ BufferedInputStream  → 8KB 字节缓冲 + mark/reset 支持
      ├─ DataInputStream      → 基本类型读取（readInt/readDouble...）
      ├─ ObjectInputStream    → Java 对象反序列化
      └─ PushbackInputStream  → 回退 1 字节（unread）

OutputStream 结构完全对称（抽象 write(byte) + FilterOutputStream 装饰器链）
```

### 字符流族谱（Reader / Writer）

```
Reader（抽象基类，唯一必须实现 read(char[],off,len)）
  ├─ FileReader              → JDK11-: 继承 InputStreamReader
  │                            JDK11+: 直接 FileChannel + StreamDecoder
  ├─ StringReader            → String 作为字符数据源
  └─ BufferedReader（装饰器基类）
      └─ 可叠加：mark/reset + 行缓冲

InputStreamReader（桥接模式！）
  └─ 内部持有 StreamDecoder（sun.nio.cs 内部类）
     ├─ 依赖底层 InputStream（byte 来源）
     ├─ 依赖 CharsetDecoder（NIO 字符解码器）
     └─ read() 全委托给 sd.read() 做 byte→char 转换
```

### 典型 IO 装饰器链路（从文件读文本）

```
BufferedReader           ← 提供 readLine() + 8KB 字符缓冲
  └─ InputStreamReader  ← 负责 byte → char 编码转换（StreamDecoder）
      └─ FileInputStream ← 真正读磁盘文件（native 系统调用）
```

**为什么不直接用 FileReader.readLine()？** JDK 11 前 FileReader 没有 readLine，必须叠 BufferedReader；JDK 11+ 推荐 `Files.newBufferedReader(path)` 底层也是这条链。

### 关键设计模式

| 模式 | 体现类 | 说明 |
|------|--------|------|
| 模板方法 | InputStream | read(byte[]) 循环调用抽象 read()；子类只需实现一个方法 |
| 装饰器 | FilterInputStream / BufferedReader | 持有 in 字段委托，叠加功能（缓冲/解码/对象反序列化） |
| 桥接 | InputStreamReader | 字节流 → 字符流，通过 Charset 做编码转换 |

### mark/reset 机制支持矩阵

| 流类型 | 默认是否支持 | 说明 |
|--------|------------|------|
| FileInputStream | ❌ | 不支持 mark/reset |
| BufferedInputStream | ✅ | 用额外缓冲数组缓存回看字节 |
| PushbackInputStream | ✅ | 天然支持（内部保存一个 pushback 字节） |
| BufferedReader | ✅ | 完整实现，支持字符级回看 |
| InputStreamReader | ❌ | 不支持 mark/reset |

---

## 🎯 高频面试考点速查表

| 考点 | 涉及类 | 关键答案 |
|------|--------|---------|
| HashMap 扩容机制 | HashMap | 2× 扩容，链表拆成 lo/hi 两段，保持顺序 |
| HashMap 为什么转红黑树 | HashMap | 泊松分布 P(k=8)≈0，恶意哈希攻击时保底 O(log n) |
| String 为什么不可变 | String | 常量池复用、HashMap key 安全、常量折叠、线程安全 |
| start() vs run() | Thread | start() 调 OS 创建新线程；run() 是普通方法调用 |
| wait vs sleep | Object, Thread | wait 释放锁；sleep 不释放锁；wait 需在 synchronized 块内 |
| ConcurrentHashMap 并发控制 | ConcurrentHashMap | JDK8 用 CAS 写空桶 + synchronized 锁桶头，读无锁 |
| ArrayList 扩容 | ArrayList | 1.5× 扩容，System.arraycopy native 拷贝 |
| == vs equals | Object | == 比引用；equals 比内容（可重写） |
| hashCode 契约 | Object | equals 相等 → hashcode 必须相等；反之不成立 |
| finalize 为什么废弃 | Object | 时机不确定、性能差、异常被吞，用 try-with-resources |
| Java 为什么选 31 做 hash 乘数 | String | 31*i ≈ (i<<5)-i 位运算快；奇素数哈希分布好 |

---

## 📂 工作区路径

```
d:\GC_WorkSpaces\jdk-source-analysis\
├── JDK核心源码注释总览.md          ← 本文档
├── src\
│   ├── java\lang\                 ← Object/String/Thread 已注释
│   ├── java\util\                 ← HashMap/ArrayList/LinkedList/ConcurrentHashMap 已注释
│   ├── java\io\                   ← InputStream/BufferedReader/InputStreamReader 已注释
│   ├── java\net\                  ← 待进一步梳理
│   └── java\nio\                  ← 待进一步梳理
└── build\                          ← javac 输出目录（编码需用 UTF-8）
```

---

## ⚠️ 编译注意

单独编译 JDK 源码会因为 sun.misc.Unsafe 等内部 API 引用报错，这是 JDK 源码的固有特性，**不是注释引入的语法问题**。注释是纯文本，Java 编译器完全忽略注释内容，不影响编译结果。

如需验证编码一致性，使用：`javac -encoding UTF-8 -d build src/java/lang/Object.java`
