# 03 · IO/NIO 体系源码深度解析

> 源码位置：`src/java/io/` + `src/java/nio/`  
> 面试权重：⭐⭐⭐⭐⭐（装饰器模式 + Buffer 4 索引 + 三大变体设计）  
> 本文深挖细节，跳过继承族谱等基础知识

---

## 一、装饰器模式核心 · FilterInputStream

> 源码锚点：`FilterInputStream.java` L46-80

### 1.1 一句话设计

**把一个 InputStream 包成另一个 InputStream，透明传递调用，子类可以在传递前后增强功能。**

### 1.2 核心字段与构造

```java
public class FilterInputStream extends InputStream {
    protected volatile InputStream in;    // ★ 唯一字段：持有的底层流

    protected FilterInputStream(InputStream in) {
        this.in = in;
    }
}
```

### 1.3 装饰器为什么叫"过滤器"？

FilterInputStream 本身就是"透传"：
```java
public int read() throws IOException {
    return in.read();   // 什么都不做，直接转调底层
}
```

**增强发生在子类覆盖**：
```
InputStream (抽象基类)
    │
    ├─ FileInputStream         （原始数据源）
    │
    └─ FilterInputStream       （装饰器基类）
        │
        ├─ BufferedInputStream  → 加缓冲
        ├─ DataInputStream      → 加基本类型读写
        └─ PushbackInputStream → 加 pushback 能力
```

### 1.4 read 三参数设计 · 多态分派陷阱

```java
// FilterInputStream.read(byte[]) 正确的委托链
public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);   // ★ 调自己的 read(b, off, len)
}

// ★ 为什么不直接 in.read(b)？
// 因为 read(b, 0, len) 会被子类覆盖 → 多态分派到子类增强逻辑
// 如果直接 in.read(b)，BufferedInputStream 的缓冲增强就被绕过了！

// 反面教材：如果有人在 FilterInputStream 写：
//   public int read(byte b[]) { return in.read(b); }
// 那 BufferedInputStream.read(b) 就绕过了缓冲，直接读底层了
```

---

## 二、BufferedInputStream · fill 四种场景（用户追问 4 次的核心）

> 源码锚点：`BufferedInputStream.java` L213-250  
> 核心：fill() 是 BufferedInputStream 的心脏，所有 read 都靠它

### 2.1 缓冲字段

```java
protected volatile byte buf[];     // ★ volatile！支持 close() 时 CAS 替换
protected int count;               // buffer 中有效字节数
protected int pos;                 // 当前读取位置
protected int markpos = -1;        // mark 位置（-1 = 无 mark）
protected int marklimit;           // mark 的存活上限
```

### 2.2 fill() 完整方法体

**源码锚点**：L213-249

```java
private void fill() throws IOException {
    byte[] buffer = getBufIfOpen();

    // ========= 场景 A：没有 mark → 直接丢 =========
    if (markpos < 0)
        pos = 0;               /* no mark: throw away the buffer */

    // ========= 场景 B：有 mark + buffer 已满 =========
    else if (pos >= buffer.length)

        // B-1: mark 在 buffer 中间 → 前移 mark 之后的数据
        if (markpos > 0) {
            int sz = pos - markpos;
            System.arraycopy(buffer, markpos, buffer, 0, sz);  // ★ 数组拷贝
            pos = sz;
            markpos = 0;
        }

        // B-2: mark 就在 buffer 头 + buffer 超过 marklimit → mark 失效
        else if (buffer.length >= marklimit) {
            markpos = -1;   /* buffer got too big, invalidate mark */
            pos = 0;        /* drop buffer contents */
        }

        // B-3: mark 在 buffer 头 + buffer 没超 marklimit → 扩容 ×2
        else if (buffer.length >= MAX_BUFFER_SIZE) {
            throw new OutOfMemoryError("Required array size too large");
        }
        else {            /* grow buffer */
            int nsz = (pos <= MAX_BUFFER_SIZE - pos) ? pos * 2 : MAX_BUFFER_SIZE;
            if (nsz > marklimit) nsz = marklimit;
            byte nbuf[] = new byte[nsz];
            System.arraycopy(buffer, 0, nbuf, 0, pos);
            if (!bufUpdater.compareAndSet(this, buffer, nbuf)) {  // ★ CAS 替换
                throw new IOException("Stream closed");           // close() 同时进行
            }
            buffer = nbuf;
        }

    // ========= 场景 C：buffer 没满 + 有 mark + 位置合理 → 继续读 =========
    count = pos;
    int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
    if (n > 0)
        count = n + pos;
}
```

### 2.3 fill 四场景图解

```
场景 A：无 mark
  buffer [==========================] pos=count=8192
                                         ↓ 丢！
  buffer [                          ] pos=0
                                         ↓ 填
  buffer [===========新数据==========] pos=xxx

场景 B-1：mark 在中间，前移
  buffer [markpos=200|====已读数据====|pos=8192]
                        ↓ System.arraycopy
  buffer [markpos=0|====已读数据====|          ] pos=7992

场景 B-2：mark 在头 + 超 marklimit，mark 失效
  buffer [markpos=0|=================================] 8192 > marklimit=4096
                        ↓ mark 丢
  buffer [                          ] pos=0

场景 B-3：mark 在头 + 没超 marklimit，×2 扩容
  buffer [markpos=0|=================================] 4096 < marklimit=8192
                        ↓ CAS 替换 buf
  buffer [markpos=0|=================================|=================] 8192
```

### 2.4 buf 的 volatile + CAS 设计

```java
// BufferedInputStream 中 buf 是 volatile！
// 为什么？close() 可能在另一个线程执行：

public void close() throws IOException {
    byte[] buffer;
    while ((buffer = buf) != null) {
        if (bufUpdater.compareAndSet(this, buffer, null)) {  // CAS buf→null
            InputStream input = in;
            in = null;
            if (input != null) input.close();
            return;
        }
        // CAS 失败 → fill() 正在扩容 → 重试
    }
}

// fill() 中替换 buf 时：
if (!bufUpdater.compareAndSet(this, buffer, nbuf)) {
    // close() 抢先把 buf 置 null
    throw new IOException("Stream closed");
}
```

---

## 三、DataInputStream · readUTF 缓冲复用（零 GC 优化）

> 源码锚点：`DataInputStream.java` L58-59 + L588-603

### 3.1 静态缓冲字段

```java
// DataInputStream 实例级别持有可复用缓冲
private byte bytearr[] = new byte[80];   // 默认 80 字节
private char chararr[] = new char[80];   // 默认 80 字符
```

### 3.2 readUTF 缓冲复用逻辑

**源码锚点**：L588-603

```java
public final static String readUTF(DataInput in) throws IOException {
    int utflen = in.readUnsignedShort();   // 先读 2 字节长度
    byte[] bytearr = null;
    char[] chararr = null;

    // ★★★ 零 GC 优化！
    if (in instanceof DataInputStream) {
        DataInputStream dis = (DataInputStream) in;
        if (dis.bytearr.length < utflen) {           // 不够用？
            dis.bytearr = new byte[utflen * 2];      // 扩容 ×2
            dis.chararr = new char[utflen * 2];
        }
        chararr = dis.chararr;   // 复用 DataInputStream 的缓冲
        bytearr = dis.bytearr;   // 不 new！直接用已有数组
    } else {
        bytearr = new byte[utflen];   // 不是 DataInputStream → 只能 new
        chararr = new char[utflen];
    }

    in.readFully(bytearr, 0, utflen);   // 把 modified UTF-8 读进 bytearr
    // ... UTF-8 解码到 chararr ...
    return new String(chararr, 0, chararr_count);
}
```

### 3.3 为什么叫"零 GC"？

```
// 场景：循环读 1000 个 UTF-8 字符串
DataInputStream dis = new DataInputStream(new FileInputStream("data.bin"));

// 不做缓冲复用时：每次 readUTF 都 new 两个数组
for (int i = 0; i < 1000; i++) dis.readUTF();
// GC 压力：1000 × 2 = 2000 个临时数组 → Young GC 频繁

// 做了缓冲复用时：bytearr/chararr 是 dis 的字段
// 第一次用 80 字节，遇到更长的才扩容一次
// 后续所有 readUTF 都复用这两个数组 → 几乎零临时对象
// GC 压力：接近 0
```

### 3.4 modified UTF-8 vs 标准 UTF-8

```
JDK 为什么自己搞一个 modified UTF-8？
  → 标准 UTF-8 把 null 字符编码为 0x00（1 字节）
  → Java 序列化用 null 标记对象结束，不能和 0x00 冲突
  → modified UTF-8 把 0x00 编码为 0xC0 0x80（2 字节）
  → 正常 ASCII 范围内（0x01-0x7F）保持单字节不变
```

---

## 四、ObjectInputStream · 变体设计（不继承 FilterInputStream）

> 源码锚点：`ObjectInputStream.java` L218-219

### 4.1 为什么不继承 FilterInputStream？

```java
// 常规做法：
//   class FilterInputStream extends InputStream { protected InputStream in; }
//   → 子类自动持有底层流

// ObjectInputStream 反而是：
public class ObjectInputStream
    extends InputStream implements ObjectInput, ObjectStreamConstants  {
    // ★ 直接继承 InputStream，不继承 FilterInputStream
}

// 原因：
// 1. ObjectInputStream 需要协议 peek（看接下来是什么类型）
//    用 FilterInputStream 的 in 无法"偷看"
// 2. ObjectInputStream 内部自己维护 PushbackInputStream
//    → 可以 pushback 数据再重新读
// 3. 它的"底层流"不只是 InputStream，还要支持 Serializable 协议
//    → 需要额外的 ObjectStreamClass 元数据
```

### 4.2 变体继承关系图

```
InputStream
    │
    ├─ ByteArrayInputStream       （原始数据源）
    ├─ FileInputStream            （原始数据源）
    ├─ FilterInputStream          （装饰器基类）
    │   ├─ BufferedInputStream
    │   ├─ DataInputStream
    │   ├─ PushbackInputStream
    │   └─ ...
    │
    └─ ★ ObjectInputStream        （变体！直接继承 InputStream）
        内部持有 PushbackInputStream
        实现 ObjectInput 协议
```

---

## 五、NIO Buffer · 四索引核心

> 源码锚点：`Buffer.java` L185-188 + L309-383

### 5.1 四个核心字段

**源码锚点**：L185-188

```java
public abstract class Buffer {
    private int mark = -1;       // 标记（reset() 恢复这里）
    private int position = 0;   // 当前读写位置
    private int limit;          // 上限（不能读写超过这里）
    private int capacity;       // 总容量（创建后不变）
}
```

### 5.2 四索引关系图

```
 capacity    mark  position      limit
    │          │       │            │
    ├──────────┼───────┼────────────┤
    0         20      35           60         100
    
  容量=100（不变）
  mark=20（reset 恢复到这里）
  position=35（下一个要读写的位置）
  limit=60（有效数据到这里为止）
  
  可用数据量 = limit - position = 25
```

### 5.3 六大核心方法

**源码锚点**：mark() L309 / reset() L325 / clear() L350 / flip() L378 / rewind() L386

```java
// 场景 A：写入完成 → 切换到读取
//   position=写入终点, limit=capacity
flip();
// → limit = position  （把 limit 设为写入终点）
// → position = 0      （从头开始读）
// → mark = -1         （丢弃 mark）

// 场景 B：读完一半 → 从头再读
//   position=35, limit=60
rewind();
// → position = 0      （回到开头）
// → mark = -1
// → limit 不变！

// 场景 C：读完了 → 清空再写
//   position=60, limit=60
clear();
// → position = 0      （从头写）
// → limit = capacity  （上限恢复最大）
// → mark = -1

// 场景 D：读到某处想回到之前的标记
//   position=35, mark=20
reset();
// → position = mark = 20
// → mark/limit/capacity 不变

// 场景 E：只想记住当前位置
mark();
// → mark = position

// 场景 F：读到一半有未读数据 → 压缩
//   position=35, limit=60
compact();  // ByteBuffer 方法（Buffer 没有）
// → [position..limit) 向前搬到 [0..limit-position)
// → position = limit - position
// → limit = capacity
// → mark = -1
```

### 5.4 完整读写循环示例

```java
// NIO 读文件典型流程
ByteBuffer buf = ByteBuffer.allocate(1024);   // capacity=1024, position=0, limit=1024

while (channel.read(buf) != -1) {
    // ① 写入阶段：channel.read → buf
    buf.flip();    // ★ flip！切换到读取模式
    // flip 后：position=0, limit=实际读入字节数

    // ② 读取阶段：buf → 业务处理
    while (buf.hasRemaining()) {
        byte b = buf.get();    // position 递增
        // 处理 b ...
    }

    // ③ 准备下一轮
    buf.clear();   // ★ clear！准备下一轮写入
    // clear 后：position=0, limit=1024, mark=-1
}
```

### 5.5 direct vs heap Buffer

```java
ByteBuffer.allocate(1024);       // heap：在 JVM 堆上，GC 管
ByteBuffer.allocateDirect(1024);  // direct：在 OS 堆外内存，不受 GC 管

// direct 优势：
// 1. 和 Channel 交互时不需要复制到 native buffer
// 2. 大 buffer 不受堆内存限制
// 3. 不受 GC 影响（不会被移动，适合长生命周期）

// direct 劣势：
// 1. 创建/释放开销大（需要 OS 级内存分配）
// 2. 小 buffer 场景反而更慢
// 3. 不受 GC 管 → 忘记释放 = 内存泄漏（靠 Cleaner 机制）

// 选择原则：
//   频繁小 buffer → heap
//   大 buffer / 长生命周期 / Channel 直连 → direct
```

---

## 六、三大 IO 设计模式总结

### 6.1 完整对比表

| 设计模式 | 代表类 | 核心机制 | 适用场景 |
|---------|--------|---------|---------|
| **装饰器** | FilterInputStream 家族 | `protected volatile InputStream in` 持有底层流，透传 + 覆盖增强 | 层层叠加增强能力（缓冲→类型→回推）|
| **变体继承** | ObjectInputStream | 直接继承 InputStream 而非 FilterInputStream，内部自己维护协议层 | 需要 peek/pushback + 额外元数据，无法用简单装饰器 |
| **静态工厂+缓冲复用** | DataInputStream.readUTF | 静态方法 + `instanceof` 检查，复用实例级 bytearr/chararr | 高频调用场景，避免 GC 压力 |

### 6.2 装饰器模式（已讲）

```
FilterInputStream 家族 → 层层包装增强
BufferedInputStream（缓冲）→ DataInputStream（类型）→ PushbackInputStream（回推）
```

### 6.3 变体设计（ObjectInputStream）

```
不继承 FilterInputStream → 直接继承 InputStream → 自己维护协议层
类似例子：Scanner（不继承 Reader，内部持 Reader）
```

### 6.3 静态工厂 + 缓冲复用（DataInputStream.readUTF）

```
静态方法 readUTF(DataInput) → 检查 instanceof → 复用实例缓冲
零 GC 优化的经典范例
```

---

## 七、扩展考点补全

| # | 考点 | 答案 |
|---|------|------|
| **装饰器** | | |
| 1 | FilterInputStream.read(byte[]) 为什么调 read(b,0,len)？| 多态分派到子类（BufferedInputStream 等）的增强逻辑，否则缓冲被绕过 |
| 2 | BufferedInputStream.buf 为什么 volatile？| close() 可能在另一线程 CAS 替换 buf → 可见性 |
| **fill 四场景** | | |
| 3 | markpos < 0 是什么意思？| 没有 mark，fill 直接 pos=0 丢旧数据 |
| 4 | mark 在中间时 fill 怎么做？| System.arraycopy 前移 mark 之后的数据，避免丢失 mark 位置 |
| 5 | buffer.length >= marklimit 时？| mark 失效（markpos=-1），缓冲全部丢弃 |
| 6 | 扩容是 ×2 还是 +1？| ×2（pos*2），最多扩到 marklimit |
| **readUTF** | | |
| 7 | 缓冲复用于什么实例？| 如果 `in instanceof DataInputStream` → 复用 dis 的 bytearr/chararr |
| 8 | modified UTF-8 为什么要改？| 把 null(0x00) 改成 0xC0 0x80，避免和序列化标记冲突 |
| 9 | readUTF 是静态方法？| 是！`public final static String readUTF(DataInput in)`，所以缓冲复用才成立 |
| **ObjectInputStream** | | |
| 10 | 为什么不继承 FilterInputStream？| 需要协议 peek + 内部 PushbackInputStream + ObjectStreamClass 元数据 |
| **NIO Buffer** | | |
| 11 | 四个索引创建时是多少？| capacity=N, mark=-1, position=0, limit=N |
| 12 | flip 后 position/limit/mark 分别是？| position=0, limit=写入终点, mark=-1 |
| 13 | clear 真正清空数据了吗？| 没有！只是重置索引，旧数据还在数组里（下次写入会覆盖）|
| 14 | compact 做了什么？| 把 [position..limit) 的数据搬到 [0..)，position 指向新写入位置 |
| 15 | direct buffer 什么时候用？| 大 buffer / 长生命周期 / Channel 直连（减少 GC + 复制）|
| **Logger 日志** | | |
| 16 | Logger 的父子引用方向？| **parent 强引用 + kids 弱引用**（避免子 Logger 不让父 GC）|
| 17 | Logger 的 level 继承机制？| levelObject==null 时向上遍历 parent 找第一个非 null 的 levelObject → root 默认 INFO |
| 18 | Handler 链用什么集合？| **CopyOnWriteArrayList**（COW，迭代无锁，并发安全）|
| 19 | 日志为什么会被多次 publish？| parent 向上传播机制 → 每个有同一 Handler 的父 Logger 都会 publish → setUseParentHandlers(false) 停止 |
| 20 | Logger.getChildLogger 懒创建？| LogManager.demandLogger(name) 从 Hashtable 取，不存在才 new → 建立 parent-child 关系 |
| **ResourceBundle** | | |
| 21 | getBundle 候选 locale 回退链？| zh_CN → zh → ROOT（逐级 fallback），全部找不到才抛 MissingResourceException |
| 22 | ResourceBundle 缓存用什么？| **ConcurrentHashMap<CacheKey, BundleReference>**，CacheKey=(baseName, locale, classLoader)，BundleReference 是 SoftReference（内存不足可 GC）|
| 23 | ResourceBundle Control 自定义什么？| getFormats（加载格式）+ getCandidateLocales（候选列表）+ toBundleName（命名）+ needsReload（过期检查）|
| **Timer** | | |
| 24 | Timer 的任务怎么调度？| **单线程串行** + TaskQueue 小顶堆（nextExecutionTime 最小的在堆顶）|
| 25 | TimerTask period 正负号含义？| period=0 一次性；**period>0 schedule 固定延迟（完成后等 period）；period<0 scheduleAtFixedRate 固定速率（按原计划时间）**|
| 26 | Timer 一个任务抛异常会怎样？| **整个 Timer 线程崩溃**，剩余任务都不执行！建议用 ScheduledExecutorService |
| **ServiceLoader SPI** | | |
| 27 | SPI 配置文件放哪？| **META-INF/services/接口全限定名**（文件名=接口名，每行=实现类名）|
| 28 | ServiceLoader 默认用什么 ClassLoader？| **当前线程的 TCCL**（Thread.currentThread().getContextClassLoader()）← Web 容器中能找到 WEB-INF/lib 的 provider |
| 29 | LazyIterator.hasNext 会实例化吗？| ❌ **不会！** 只加载类名，next() 才 Class.forName + newInstance（懒加载）|
| 30 | ClassLoader.getResources 为什么能找到多个 jar 的同名配置？| URLClassLoader 遍历所有 URL（每个 jar）→ 每个 jar 都查一遍 → 返回所有匹配的 URL |
| 31 | ServiceLoader 同名 provider 会实例化两次吗？| ❌ LinkedHashMap<String,S> providers 缓存，key=类全名 → 同名只实例化一次 |
| 32 | SPI 和 Spring 依赖注入区别？| SPI **接口+配置文件自动发现**（JDK 内置），DI 容器管理更灵活（但依赖外部框架）|

---

## 八、java.util.logging Logger · 日志框架源码解析

> 源码位置：`src/java/util/logging/Logger.java` + `Handler.java` + `Level.java`  
> 面试权重：⭐⭐⭐（Handler 链 + parent 继承机制 + Level 过滤）

### 6.1 核心架构

```
Logger 是树形结构（父子通过 parent 引用）：
  root Logger ("")  ← 根节点
  ├── Logger("com")
  │   ├── Logger("com.google")
  │   │   └── Logger("com.google.android")
  │   └── Logger("com.facebook")
  └── Logger("org")

日志发布规则（log(LogRecord)）：
  ① 检查 level 是否 >= Logger.level（不满足 → 跳过）
  ② 检查 Filter（如果有且返回 false → 跳过）
  ③ 发布到 Logger 自己的 Handler 链（CopyOnWriteArrayList）
  ④ 如果 useParentHandlers=true → 向上遍历 parent 树
     对每个 parent 发布到它的 Handler（直到 parent 是 root 或 useParentHandlers=false）
```

### 6.2 核心字段

**源码锚点**：Logger.java L256-279

```java
private volatile LogManager manager;           // LogManager 管理所有 Logger
private String name;                           // Logger 名字（树形路径）
private final CopyOnWriteArrayList<Handler> handlers  // ★ Handler 链（COW！并发安全）
    = new CopyOnWriteArrayList<>();
private volatile boolean useParentHandlers = true;   // ★ 是否传递给父 Logger
private volatile Filter filter;                 // 可选过滤器
private static final Object treeLock = new Object(); // 树结构锁
private volatile Logger parent;                 // ★ 父 Logger（强引用）
private ArrayList<LogManager.LoggerWeakRef> kids;  // ★ 子 Logger（弱引用！）
private volatile Level levelObject;             // Logger 自身的 level（null = 继承）
private volatile int levelValue;                // 实际生效的 level（向上查找继承）
```

### 6.3 Handler 链 + parent 向上传播

**源码锚点**：L719-751

```java
public void log(LogRecord record) {
    if (!isLoggable(record.getLevel())) return;    // ① level 检查
    Filter theFilter = filter;
    if (theFilter != null && !theFilter.isLoggable(record)) return;  // ② Filter 检查

    // ③ + ④：Handler 链 + parent 向上传播
    Logger logger = this;
    while (logger != null) {
        final Handler[] loggerHandlers = logger.getHandlers();
        for (Handler handler : loggerHandlers) {
            handler.publish(record);    // ★ 发布到当前 Logger 的所有 Handler
        }
        if (!logger.getUseParentHandlers()) break;  // ★ 停止传播
        logger = logger.getParent();                 // ★ 向上找父 Logger
    }
}
```

**传播图解**：
```
Logger("com.google.android").info("test")
  │
  ├─ isLoggable(INFO) → INFO >= levelValue → ✅
  ├─ Filter.isLoggable → ✅
  │
  ├─ Logger("com.google.android").handlers:
  │   ├─ ConsoleHandler.publish(record)      ← 自己的 Handler
  │   └─ FileHandler.publish(record)         ← 自己的 Handler
  │
  ├─ useParentHandlers = true → 向上：
  │
  ├─ Logger("com.google").handlers:
  │   └─ ConsoleHandler.publish(record)      ← 父的 Handler 也收到！
  │
  ├─ useParentHandlers = true → 向上：
  │
  ├─ Logger("com").handlers:
  │   └─ (没有 Handler)
  │
  ├─ useParentHandlers = true → 向上：
  │
  ├─ root Logger("").handlers:
  │   └─ ConsoleHandler.publish(record)      ← 根的 Handler 也收到！
  │
  └─ root.parent = null → 结束

⚠️ 注意：同一个 record 被 ConsoleHandler 发布多次（每个 Logger 都有 ConsoleHandler 时）
→ 可以设置 logger.setUseParentHandlers(false) 停止传播
```

### 6.4 Level 继承机制

**源码锚点**：levelValue 计算逻辑

```java
// Logger 的 level 是 volatile int（不是 Level 对象）
// levelObject == null → 向上继承 parent.levelValue
// 直到 root Logger（root.level 默认 INFO）

private int effectiveLevel() {
    for (Logger l = this; l != null; l = l.parent) {
        Level lv = l.levelObject;
        if (lv != null) return lv.intValue();   // ★ 找到第一个非 null 的 levelObject
    }
    return Level.INFO.intValue();   // root 默认 INFO
}

// isLoggable 用 levelValue 做 >= 比较
public boolean isLoggable(Level level) {
    if (level == null) throw new NullPointerException();
    return level.intValue() >= levelValue;   // ★ levelValue 向上继承后的结果
}
```

### 6.5 getLogger · LogManager 懒创建

**源码锚点**：L491-548

```java
public static Logger getLogger(String name) {
    LogManager manager = LogManager.getLogManager();
    Logger logger = manager.demandLogger(name);
    return logger;
}

// LogManager.getLogger：
public synchronized Logger getLogger(String name) {
    Logger target = loggers.get(name);   // 从 Hashtable 取（懒创建）
    if (target != null) return target;
    target = new Logger(name, null);     // 创建新 Logger
    loggers.put(name, target);           // 存入 Hashtable
    // 建立父子关系（向上找最近的父 Logger）
    target.setParent(findParent(name));
    return target;
}
```

### 6.6 Handler 接口 + 三种内置 Handler

| Handler | 输出目标 | 说明 |
|---------|---------|------|
| **ConsoleHandler** | System.err | 默认绑定到 root Logger，Level=INFO |
| **FileHandler** | 文件 | 支持滚动/限制大小，Formatter 默认为 XMLFormatter |
| **SocketHandler** | TCP 远程 Socket | 发到远程 Logging Server |
| **MemoryHandler** | 内存缓冲 | 循环缓存 N 条记录，满了推到另一个 Handler |

```java
// Handler 接口：
public abstract class Handler {
    public abstract void publish(LogRecord record);   // 核心方法
    public abstract void flush();                      // 刷新
    public abstract void close();                      // 关闭
    public void setFormatter(Formatter newFormatter) { ... }
    public abstract void setLevel(Level newLevel);
}
```

### 6.7 Filter / Formatter

```java
// Filter：比 level 更细粒度的过滤
public interface Filter {
    boolean isLoggable(LogRecord record);
    // 可以自定义：根据类名/方法名/线程名决定要不要记
}

// Formatter：日志格式化输出
public abstract class Formatter {
    public abstract String format(LogRecord record);
    // SimpleFormatter：人类可读格式（默认）
    // XMLFormatter：XML 格式（FileHandler 默认）
}
```

### 6.8 Logger vs Log4j vs SLF4J

| 维度 | **java.util.logging** | **Log4j 1.x** | **SLF4J** |
|------|----------------------|---------------|-----------|
| Handler/Appender | Handler | Appender | Logger（API 门面）|
| Level | 7 级（SEVERE/INFO/FINE...）| 5 级 | SLF4J 定义 |
| 配置方式 | logging.properties | log4j.xml/properties | 绑定实现 |
| 依赖 | JDK 内置 | 外部依赖 | API + 实现分离 |
| 生产级 | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐（推荐）|

---

**文档版本：v1.2 · 2026-10-20**  
**源码基线：JDK 1.8.0_451**  
**v1.2 更新：新增第九章 ResourceBundle + 第十章 Timer + 第十一章 ServiceLoader SPI 机制 + 15 个新面试考点（总 35 题）**

---

## 九、ResourceBundle · 国际化资源加载器

> 源码位置：`src/java/util/ResourceBundle.java`  
> 面试权重：⭐⭐⭐（getBundle 缓存 + 候选 locale 回退链 + Control 策略 + 父子链）

### 9.1 核心设计

```java
// ResourceBundle 是抽象基类，核心是 getBundle(baseName, locale, classLoader)
// 支持两种格式：
//   ① .properties 文件：java.util.PropertyResourceBundle
//   ② .class 类文件：自定义 ResourceBundle 子类（通过 Control.getFormats 返回）

// 命名规则（Control.toBundleName）：
//   baseName = "messages", locale = Locale.CHINA → "messages_zh_CN"
//   baseName = "messages", locale = Locale.ROOT  → "messages"
```

### 9.2 getBundleImpl · 候选 locale 回退链

**源码锚点**：L1323-1403

```java
private static ResourceBundle getBundleImpl(String baseName, Locale locale,
                                            ClassLoader loader, Control control) {
    CacheKey cacheKey = new CacheKey(baseName, locale, loader);

    // ① 先查缓存
    BundleReference bundleRef = cacheList.get(cacheKey);
    if (bundleRef != null) {
        bundle = bundleRef.get();
        if (isValidBundle(bundle) && hasValidParentChain(bundle))
            return bundle;   // 缓存命中！全部有效 → 直接返回
    }

    // ② 缓存未命中 → 候选 locale 回退链
    List<String> formats = control.getFormats(baseName);  // 默认 "java.properties"

    ResourceBundle baseBundle = null;
    for (Locale targetLocale = locale;
         targetLocale != null;
         targetLocale = control.getFallbackLocale(baseName, targetLocale)) {
        // ★★★ 关键：Control.getCandidateLocales 返回候选列表
        // 例：locale=zh_CN → ["zh_CN", "zh", ""] 逐级 fallback
        List<Locale> candidateLocales = control.getCandidateLocales(baseName, targetLocale);
        bundle = findBundle(cacheKey, candidateLocales, formats, 0, control, baseBundle);
        // 找到有效 bundle → break
    }
    return bundle;
}
```

**候选 locale 回退链图解**：
```
getBundle("messages", Locale.CHINA)
  │
  ├─ targetLocale = zh_CN
  │   ├─ getCandidateLocales("messages", zh_CN) → [zh_CN, zh, ROOT]
  │   ├─ findBundle([zh_CN, zh, ROOT]) → 找到 messages_zh_CN.properties？
  │   │   有 → return messages_zh_CN ✅
  │   │   没有 messages_zh_CN → 找 messages_zh → 找 messages → return messages
  │   │
  │   └─ 三个都没有 → baseBundle 暂存
  │
  ├─ targetLocale = control.getFallbackLocale("messages", zh_CN)
  │   默认 Control.getFallbackLocale 返回 Locale.ROOT → 循环终止
  │
  └─ bundle == null → return baseBundle（如果有的话）→ 否则抛 MissingResourceException
```

### 9.3 缓存 + Control 策略

```java
// ResourceBundle 内部用 static final ConcurrentHashMap<CacheKey, BundleReference> cacheList
// 缓存 key = (baseName, locale, classLoader)
// BundleReference 是 SoftReference → 内存不足时可被 GC 回收

// Control 是加载策略接口：
public static class Control {
    public List<String> getFormats(String baseName) {
        // 默认返回 ["java.properties"] → 只加载 .properties
        // 可以返回 ["java.class", "java.properties"] → 先找 .class，再找 .properties
    }
    public List<Locale> getCandidateLocales(String baseName, Locale locale) {
        // 默认：zh_CN → [zh_CN, zh, ROOT]
        // ROOT = 全局默认（messages.properties）
    }
    public Locale getFallbackLocale(String baseName, Locale locale) {
        // 默认返回 Locale.ROOT → 循环终止
    }
    public String toBundleName(String baseName, Locale locale) {
        // messages + zh_CN → "messages_zh_CN"
    }
}
```

### 9.4 父子链 + getObject 查找顺序

```java
// setParent 建立父子链（getBundleImpl 中调）：
// messages_zh_CN.parent = messages_zh
// messages_zh.parent = messages

// ResourceBundle.getObject(key) 查找顺序：
//   ① 当前 bundle 中找 key
//   ② 当前 bundle.parent 中找 key
//   ③ parent.parent 中找 key
//   ④ ... 直到 ROOT bundle
//   ⑤ 都没找到 → MissingResourceException

// 好处：国际化只需要覆盖变化的部分，不需要每个 locale 都维护完整的 key-value
```

### 9.5 Logger 如何用到 ResourceBundle

```java
// Logger.getLogger(name, "com.example.Messages")
// → LoggerBundle 用 ServiceLoader.loadInstalled(ResourceBundleControlProvider.class)
// → JDK 8 起用 ResourceBundle.Control 加载自定义格式
// → Logger.logrb() 可以指定 resource bundle name，自动从 bundle 取消息模板
```

---

## 十、Timer · 定时任务线程

> 源码位置：`src/java/util/Timer.java` + `TimerTask.java`  
> 面试权重：⭐⭐⭐（mainLoop 单线程串行执行 + TaskQueue 小顶堆 + period 正负号语义）

### 10.1 核心设计

```java
// Timer 单线程串行执行所有 TimerTask！
// → 一个任务阻塞 → 后面所有任务都延后
// → 一个任务抛异常 → 整个 Timer 线程崩溃，剩余任务都不执行

// TimerTask 是抽象类，用户实现 run()
// state 四值：VIRGIN=0 → SCHEDULED=1 → EXECUTED=2 / CANCELLED=3
// period 正负号决定调度方式！
```

### 10.2 TimerTask 核心字段

**源码锚点**：TimerTask.java L40-81

```java
public abstract class TimerTask implements Runnable {
    final Object lock = new Object();
    int state = VIRGIN;

    static final int VIRGIN = 0;    // 初始状态（还没调度）
    static final int SCHEDULED = 1;  // 已入队等待执行
    static final int EXECUTED  = 2;  // 已执行（非重复任务）
    static final int CANCELLED = 3;  // 已取消（用户调 cancel() 或执行异常）

    long nextExecutionTime;   // ★ 下次执行时间（毫秒时间戳）
    long period = 0;          // ★ 调度周期：
                              //   period == 0 → 一次性，执行后移除
                              //   period > 0 → schedule（固定延迟：上一次完成后等 period 毫秒）
                              //   period < 0 → scheduleAtFixedRate（固定速率：按原始计划时间执行）
}
```

**period 正负号图解**：
```
场景：任务执行耗时 30ms，period=100ms
  schedule (period>0)：
    T=0 执行 → T=30 完成 → T=130 执行 → T=160 完成 → T=260 执行 ...
    （每次完成后等 period 毫秒，执行时间影响后续调度）
  scheduleAtFixedRate (period<0)：
    T=0 执行 → T=30 完成 → T=100 执行（即使还在执行也要按原计划时间）
    （-period = 100，按 T=0, T=100, T=200, T=300... 时间点触发）
    如果执行耗时太长 → 任务会连续追赶执行（catch up）
```

### 10.3 TaskQueue · 小顶堆

**源码锚点**：Timer.java L569-649

```java
class TaskQueue {
    private TimerTask[] queue = new TimerTask[128];  // 下标 1 开始（0 空着）
    private int size = 0;

    // 核心操作：
    void add(TimerTask task) {
        if (size + 1 == queue.length)
            queue = Arrays.copyOf(queue, 2*queue.length);
        queue[++size] = task;
        fixUp(size);   // ★ 堆上滤（新元素往 bubble）
    }

    TimerTask getMin() { return queue[1]; }   // ★ O(1) 取堆顶

    void removeMin() {
        queue[1] = queue[size];
        queue[size--] = null;
        fixDown(1);   // ★ 堆下滤（堆底元素往沉）
    }

    void rescheduleMin(long newTime) {
        queue[1].nextExecutionTime = newTime;
        fixDown(1);   // 堆顶改了执行时间 → 往下调整
    }
}
```

### 10.4 mainLoop · 单线程调度核心

**源码锚点**：L518-559

```java
private void mainLoop() {
    while (true) {
        try {
            TimerTask task;
            boolean taskFired;
            synchronized(queue) {
                // ① 队空 → 等 notify
                while (queue.isEmpty() && newTasksMayBeScheduled)
                    queue.wait();
                if (queue.isEmpty())
                    break;  // queue.isEmpty() && !newTasksMayBeScheduled → 整个 Timer 要销毁

                // ② 取堆顶（最早要执行的任务）
                task = queue.getMin();

                synchronized(task.lock) {
                    // ③ 已取消 → 跳过
                    if (task.state == TimerTask.CANCELLED) {
                        queue.removeMin();
                        continue;
                    }

                    // ④ 判断执行时间到没到
                    currentTime = System.currentTimeMillis();
                    executionTime = task.nextExecutionTime;
                    taskFired = (executionTime <= currentTime);

                    if (taskFired) {
                        if (task.period == 0) {
                            // 一次性 → removeMin + state=EXECUTED
                            queue.removeMin();
                            task.state = TimerTask.EXECUTED;
                        } else {
                            // 重复 → rescheduleMin
                            queue.rescheduleMin(
                              task.period < 0
                                ? currentTime   - task.period    // scheduleAtFixedRate 按原时间
                                : executionTime + task.period);  // schedule 按执行时间
                        }
                    }
                }

                // ⑤ 没到时间 → wait(executionTime - currentTime)
                if (!taskFired)
                    queue.wait(executionTime - currentTime);
            }

            // ⑥ 执行任务（出 queue 锁！避免 run() 阻塞队列操作）
            if (taskFired) task.run();
        } catch(InterruptedException e) { }
    }
}
```

### 10.5 Timer vs ScheduledExecutorService

| 维度 | **Timer** | **ScheduledExecutorService** |
|------|-----------|------------------------------|
| 线程数 | **1 个**（单线程串行）| **可配置 N 个** |
| 一个任务异常 | **整个 Timer 崩溃**（thread 退出）| 只取消当前任务，其他继续 |
| 锁粒度 | 整个 queue 锁 | 每个任务独立 |
| 重复任务 | period 正负号区分 | schedule/scheduleAtFixedRate 方法分离 |
| 精度 | 毫秒 | 纳秒 |
| 推荐 | ❌ 不推荐 | ✅ 生产级 |

---

## 十一、ServiceLoader · SPI 机制源码

> 源码位置：`src/java/util/ServiceLoader.java`  
> 面试权重：⭐⭐⭐⭐（SPI 自动发现 + ClassLoader 资源加载 + TCCL 上下文类加载器 + LazyIterator 懒加载）

### 11.1 SPI 三要素

```
Service Provider Interface：
  ① 服务接口（Service）：public interface CodecSet { ... }
  ② 服务实现（Provider）：public class GzipCodec implements CodecSet { ... }
  ③ 配置文件：META-INF/services/com.example.CodecSet（文件名=接口全限定名）
     文件内容：com.example.GzipCodec（每行一个实现类全限定名）

ServiceLoader.load(CodecSet.class)
  → 从 META-INF/services/com.example.CodecSet 加载实现列表
  → Class.forName + newInstance 实例化
  → 返回 Iterator<CodecSet>（LazyIterator 懒加载）
```

### 11.2 核心字段 + 构造函数

**源码锚点**：L185-227

```java
public final class ServiceLoader<S> implements Iterable<S> {
    private static final String PREFIX = "META-INF/services/";

    private final Class<S> service;             // 服务接口 Class 对象
    private final ClassLoader loader;           // ★ 类加载器（决定从哪加载配置文件）
    private final AccessControlContext acc;     // SecurityManager 安全上下文
    private LinkedHashMap<String,S> providers;  // ★ 缓存已实例化的 provider（类名→实例）
    private LazyIterator lookupIterator;        // 懒加载迭代器

    private ServiceLoader(Class<S> svc, ClassLoader cl) {
        service = Objects.requireNonNull(svc);
        loader = (cl == null) ? ClassLoader.getSystemClassLoader() : cl;
        acc = (System.getSecurityManager() != null) ? AccessController.getContext() : null;
        reload();   // ★ 初始化：清空缓存 + 创建 LazyIterator
    }
}
```

### 11.3 load · 三种加载方式

**源码锚点**：L507-567

```java
// 方式 1：指定 ClassLoader
public static <S> ServiceLoader<S> load(Class<S> service, ClassLoader loader) {
    return new ServiceLoader<>(service, loader);
}

// 方式 2：当前线程的 TCCL（Thread Context ClassLoader）← 最常用
public static <S> ServiceLoader<S> load(Class<S> service) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    return ServiceLoader.load(service, cl);
}

// 方式 3：系统级 ClassLoader（只加载 bootstrap + extension 的 provider）
public static <S> ServiceLoader<S> loadInstalled(Class<S> service) {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    // ... 包装后 load
}
```

**三种方式的区别**：
```
load(service)：用当前线程的 TCCL
  → Web 容器（Tomcat/Spring）中 TCCL 是 WebAppClassLoader
  → 所以能找到 WEB-INF/lib/*.jar 里的 META-INF/services/
  → ✅ 最常用！

load(service, systemClassLoader)：只找系统类加载器
  → 找不到 WEB-INF 里的 provider
  → ❌ Web 容器中会找不到实现

loadInstalled：用 installed extensions（Java 9 之前的 extension 目录）
  → 找到 jre/lib/ext/*.jar 里的 provider
  → JDK 内置工具用（Logger 的 ResourceBundleControlProvider 就是用 loadInstalled）
```

### 11.4 LazyIterator · 懒加载迭代核心

**源码锚点**：L323-411

```java
private class LazyIterator implements Iterator<S> {
    Class<S> service;
    ClassLoader loader;
    Enumeration<URL> configs = null;   // ★ ClassLoader.getResources 返回的配置文件 URL 列表
    Iterator<String> pending = null;   // 当前配置文件解析出的实现类名列表
    String nextName = null;             // 下一个要实例化的类名

    // hasNextService：找下一个实现类名（不实例化）
    private boolean hasNextService() {
        if (nextName != null) return true;
        if (configs == null) {
            // ★ 关键：loader.getResources("META-INF/services/" + service.getName())
            // 从所有可见的 jar 中找同名配置文件
            String fullName = PREFIX + service.getName();
            configs = loader.getResources(fullName);
        }
        while ((pending == null) || !pending.hasNext()) {
            if (!configs.hasMoreElements()) return false;
            pending = parse(service, configs.nextElement());  // 解析一个配置文件
        }
        nextName = pending.next();
        return true;
    }

    // nextService：实例化一个实现类
    private S nextService() {
        if (!hasNextService()) throw new NoSuchElementException();
        String cn = nextName;
        nextName = null;
        Class<?> c = Class.forName(cn, false, loader);   // ★ Class.forName 懒加载类
        if (!service.isAssignableFrom(c)) fail(service, "not a subtype");
        S p = service.cast(c.newInstance());             // ★ newInstance() 实例化
        providers.put(cn, p);                            // ★ 缓存到 LinkedHashMap
        return p;
    }
}
```

**完整加载流程图解**：
```
ServiceLoader.load(CodecSet.class)  ← 用户调用
  │
  ├─ new ServiceLoader(CodecSet.class, TCCL)
  │   └─ reload() → providers.clear() + LazyIterator 创建
  │
  ├─ serviceLoader.iterator().hasNext()  ← 第一次迭代
  │   └─ LazyIterator.hasNext() → hasNextService()
  │       ├─ loader.getResources("META-INF/services/com.example.CodecSet")
  │       │   → 遍历所有 jar 的 URLClassLoader
  │       │   → 找每个 jar 中的 META-INF/services/com.example.CodecSet
  │       │   → 返回 Enumeration<URL>{url1, url2, url3}
  │       ├─ parse(url1) → 读出类名列表 ["GzipCodec", "ZipCodec"]
  │       ├─ pending 遍历 → nextName = "GzipCodec"
  │       └─ return true
  │
  ├─ serviceLoader.iterator().next()  ← 实例化
  │   └─ LazyIterator.next() → nextService()
  │       ├─ Class.forName("com.example.GzipCodec", false, loader)
  │       ├─ service.isAssignableFrom(c) → ✅
  │       ├─ c.newInstance() → new GzipCodec()  ← 调用无参构造函数！
  │       ├─ providers.put("com.example.GzipCodec", gzipCodec)  ← 缓存
  │       └─ return gzipCodec
  │
  ├─ 第二次 hasNext()
  │   └─ nextName 还有 "ZipCodec" → return true
  │
  ├─ 第二次 next()
  │   └─ Class.forName("com.example.ZipCodec") → new ZipCodec() → 缓存
  │
  ├─ 第三次 hasNext()
  │   └─ pending 空 → configs.hasMoreElements() → false → return false
  │
  └─ 迭代结束 ✅
```

### 11.5 ClassLoader.getResources · 多 jar 同名配置

```
为什么能找到多个 jar 里的同名配置文件？

ClassLoader.getResources("META-INF/services/com.example.CodecSet")
  → URLClassLoader 遍历它的 URL[] 数组（每个 jar 一个 URL）
  → 对每个 URL：在 jar 里找 META-INF/services/com.example.CodecSet
  → 找到就加到 Enumeration 中
  → 返回所有找到的 URL

多个 jar 都有这个文件？全部返回！
→ ServiceLoader 遍历每个 URL，分别 parse → 汇总所有实现类名

⚠️ 类名重复怎么办？
  ServiceLoader 用 LinkedHashMap<String,S> providers 缓存
  → key = 实现类全限定名
  → 同名类只实例化一次（第一个出现的生效）
```

### 11.6 parse · 配置文件解析

```java
// 配置文件格式：
// - 每行一个实现类全限定名
// - # 开头的是注释
// - 空行忽略
// - 不允许空格或 tab（严格 Java 标识符语法）

// ServiceLoader.parse 方法：
//   BufferedReader.readLine() 逐行读
//   ln.indexOf('#') >= 0 → ln.substring(0, ci) 截断注释
//   ln.trim() → 空行 continue
//   检查无空格/tab + Java 标识符语法 → 加入 names list
//   下一个 URL 继续 parse

// 典型配置文件 META-INF/services/com.example.CodecSet：
# Gzip codec
com.example.GzipCodec
# Zip codec
com.example.ZipCodec
```

### 11.7 reload · 重新加载

```java
public void reload() {
    providers.clear();                              // ★ 清空所有缓存的 provider
    lookupIterator = new LazyIterator(service, loader);  // 重建懒迭代器
}

// 用途：
// 1. 运行时动态安装了新 jar → reload() 才能发现新 provider
// 2. TCCL 变了 → reload() 用新 TCCL 重新查找
// 3. JSP 热部署 → 旧 classloader 的 provider 失效 → reload
```

### 11.8 SPI vs 反射 vs 依赖注入

| 维度 | **ServiceLoader SPI** | **反射 Class.forName** | **Spring 依赖注入** |
|------|----------------------|----------------------|-------------------|
| 发现机制 | META-INF/services 自动 | 硬编码类名 | 配置文件 / 注解自动扫描 |
| 耦合度 | 松耦合（接口 + 实现分离）| 紧耦合（代码写死类名）| 中耦合（容器托管）|
| 依赖 | JDK 内置 | JDK 内置 | 外部框架 |
| 支持多实现 | ✅ 自动发现全部 | ❌ 只能一个 | ✅ 容器管理 |
| 典型用途 | JDBC Driver / SLF4J Binding / Dubbo Extension | 框架内部 | 企业级 DI |

---

## 十二、SocketChannel · NIO 非阻塞通道

> 源码位置：`src/java/nio/channels/SocketChannel.java`  
> 面试权重：⭐⭐⭐⭐（connect 非阻塞 + finishConnect 二次确认 + Selector 配合）

### 12.1 Channel 层次

```java
Channel（接口）→ AbstractInterruptibleChannel（抽象基类）→ AbstractSelectableChannel（支持 Selector）
  ├─ SocketChannel（TCP 客户端）
  ├─ ServerSocketChannel（TCP 服务端）
  └─ DatagramChannel（UDP）

// Channel 必须 configureBlocking(false) 才能注册到 Selector！
// 否则抛 IllegalBlockingModeException
```

### 12.2 connect · 非阻塞连接两阶段

**源码锚点**：L389-440

```java
// connect(remote) 非阻塞模式的关键：
// ① 返回 false → 连接还在进行 → 必须调 finishConnect() 完成
// ② 返回 true → 连接已建立

// 典型非阻塞连接流程：
SocketChannel sc = SocketChannel.open();
sc.configureBlocking(false);
sc.connect(new InetSocketAddress("example.com", 80));

// 连接还在进行中...
while (!sc.finishConnect()) {
    // 可以做其他事，或等 OP_CONNECT 就绪
    selector.select();
}
// 连接成功！
```

**非阻塞连接两阶段图解**：
```
connect()（立即返回）
  │
  ├─ blocking=true → 底层 socket.connect() 阻塞直到建立 → 返回 true
  │
  └─ blocking=false → 底层 socket.connect() 立即返回 false
                      TCP 三次握手继续后台进行（OS 内核）
                      返回 false 给上层
                      → 注册到 Selector 监听 OP_CONNECT

finishConnect()（二次确认）
  │
  ├─ TCP 握手还没完成 → 返回 false（继续等）
  ├─ TCP 握手成功 → 返回 true（连接建立）
  └─ TCP 握手失败 → 抛 IOException（Connection refused / timeout / etc）

⚠️ 陷阱：NonBlockingConnect 超时不是 JDK 层面的！
  connect() 无 timeout 参数
  需要自己实现：记录 connectStartTime → finishConnect 时检查是否超时
```

### 12.3 read / write · ByteBuffer 驱动

```java
// read 返回 int：
//   正数 = 实际读了多少字节（可能 < ByteBuffer 容量）
//   0 = 非阻塞模式下暂时没数据
//   -1 = 对端关闭连接（EOF）
//   IOException = 网络错误

int n = sc.read(buf);
if (n == -1) {
    sc.close();    // 对端关闭
} else {
    buf.flip();
    // 处理数据...
    buf.clear();
}

// write 返回 int：
//   正数 = 实际写了多少字节（可能 < ByteBuffer 剩余）
//   0 = 非阻塞模式下内核发送缓冲满了（等 OP_WRITE 就绪）
//   IOException = 网络错误

while (buf.hasRemaining()) {
    int n = sc.write(buf);
    if (n == 0) break;   // 内核缓冲满了 → 等下次 OP_WRITE
}
```

### 12.4 NIO 完整 Echo Client 模板

```java
// ★ 这就是完整的 SocketChannel 使用流程
public static void main(String[] args) throws IOException {
    SocketChannel sc = SocketChannel.open();
    sc.configureBlocking(false);

    // 阶段 ① 发起连接
    if (!sc.connect(new InetSocketAddress("localhost", 8080))) {
        // 非阻塞：连接还在进行 → 等 OP_CONNECT
        Selector selector = Selector.open();
        sc.register(selector, SelectionKey.OP_CONNECT);
        selector.select();
        selector.selectedKeys().clear();
        sc.finishConnect();   // ★ 必须调！完成连接
    }

    // 阶段 ② 写数据
    ByteBuffer writeBuf = ByteBuffer.wrap("hello".getBytes());
    while (writeBuf.hasRemaining()) {
        int n = sc.write(writeBuf);
        if (n == 0) {
            // 内核缓冲满 → 等 OP_WRITE
            Selector s2 = Selector.open();
            sc.register(s2, SelectionKey.OP_WRITE);
            s2.select();
            s2.selectedKeys().clear();
        }
    }

    // 阶段 ③ 读数据
    ByteBuffer readBuf = ByteBuffer.allocate(1024);
    int n = sc.read(readBuf);
    if (n == -1) {
        System.out.println("对端关闭");
    } else {
        readBuf.flip();
        System.out.println(new String(readBuf.array(), readBuf.position(), readBuf.limit()));
    }

    sc.close();
}
```

---

## 十三、java.lang.ref Reference 体系 · 弱引用机制

### 13.1 四种引用对比

| 类型 | 类名 | GC 行为 | 典型用途 |
|------|------|--------|---------|
| **强引用** | 无（普通对象引用）| **永不回收**（只要还有强引用链可达）| 普通业务对象 |
| **软引用** | SoftReference | **内存不足时回收** | 内存敏感型缓存（比 WeakReference 更"耐回收"）|
| **弱引用** | WeakReference | **GC 立即回收**（只要没强引用链可达）| WeakHashMap key / ThreadLocalMap Entry |
| **虚引用** | PhantomReference | **GC 后入队**（get() 永远返回 null）| Finalizer / Cleaner / DirectByteBuffer 堆外内存清理 |

### 13.2 Reference 核心结构

**源码锚点**：Reference.java L42-125

```java
public abstract class Reference<T> {
    private T referent;           // ★ 被引用的对象（T）
    volatile ReferenceQueue<? super T> queue;  // ★ 入队目标（null = 不入队）
    Reference<T> next;            // ReferenceQueue 链表用
    static Reference<Object> pending = null;    // ★ GC 发现的待处理链表

    // GC 标记-清除阶段：
    //   扫描每个 Reference.referent 的可达性
    //   → 不可达但 referent != null（强引用也不可达）
    //   → 判断 Reference 类型：
    //      SoftReference：是否还内存不足？→ 是才 clear
    //      WeakReference：立即 clear
    //      PhantomReference：不 clear，但入队
    //      FinalReference(Finalizer)：入队 Finalizer 链表
    //   → Reference.discovered = pending（头插法）
    //   → pending = this（新的待处理链表头）
}
```

### 13.3 ReferenceHandler · JVM 内部线程

**源码锚点**：Reference.java L129-232

```java
// ReferenceHandler 是 JVM 启动时创建的最高优先级守护线程
// 核心循环：while (true) { tryHandlePending(true); }
// tryHandlePending 做什么？
static boolean tryHandlePending(boolean waitForNotify) {
    synchronized (lock) {
        if (pending != null) {
            // 从 pending 链表取一个
            r = pending;
            pending = r.discovered;
            r.discovered = null;
        } else {
            // 没待处理的 → 等 lock.wait()（GC 会 notify）
            if (waitForNotify) lock.wait();
            return waitForNotify;
        }
    }

    // Fast path：Cleaner（DirectByteBuffer 堆外内存清理）
    if (c != null) {
        c.clean();   // ★ 直接调用 Cleaner.clean() 释放堆外内存
        return true;
    }

    // 入队 ReferenceQueue
    ReferenceQueue<? super Object> q = r.queue;
    if (q != ReferenceQueue.NULL) q.enqueue(r);
    return true;
}
```

**GC → ReferenceHandler 时序**：
```
GC 标记阶段：
  扫描 Reference 类型字段
  → SoftReference.softRefersTo() / WeakReference.enqueued() / PhantomReference
  → 发现不可达的 referent
  → clear referent（软/弱）
  → this.discovered = pending
  → pending = this （头插法，所有被 GC 的 Reference 串成链）
  → 最后 notify ReferenceHandler 的 lock

ReferenceHandler 线程：
  wait() 被 notify → tryHandlePending(true)
  → 从 pending 链表取 Reference
  → Cleaner 类型 → c.clean() 直接释放
  → 其他类型 → r.queue.enqueue(r) 入队用户的 ReferenceQueue
```

### 13.4 ReferenceQueue · poll / remove

```java
// 用户代码：
ReferenceQueue<Object> queue = new ReferenceQueue<>();
WeakReference<Object> ref = new WeakReference<>(obj, queue);
// obj 被 GC 回收 → ref 自动入队 queue
// → 从 queue 取出 ref → 知道 obj 被回收了

Reference<?> r = queue.poll();   // 非阻塞取
if (r == ref) {
    // obj 被回收了
}

// queue.remove(timeout) → 阻塞等待直到有 Reference 入队
```

### 13.5 应用场景

| 场景 | 用什么引用 | 为什么 |
|------|-----------|--------|
| WeakHashMap | **WeakReference** | key 被 GC 回收时自动入队 → expungeStaleEntries 清理 stale Entry |
| ThreadLocalMap | **WeakReference** | key 是 ThreadLocal → key 被 GC 但 value 还在 → Entry 弱引用 key，强引用 value |
| DirectByteBuffer | **PhantomReference + Cleaner** | 堆外内存没有 GC → Cleaner.clean() 调用 Unsafe.freeMemory |
| Finalizer | **FinalReference** | 对象覆写 finalize() → 入 Finalizer 队列 → FinalizerThread 执行 finalize() |
| 内存敏感缓存 | **SoftReference** | 内存不足时才回收 → 比 WeakReference 更耐回收 |

---

## 十四、扩展考点补全

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **SocketChannel** | | |
| 1 | SocketChannel connect 非阻塞？| **connect() 返回 false + finishConnect() 二次确认** → TCP 三次握手后台进行 |
| 2 | SocketChannel read/write 返回 0？| **内核发送/接收缓冲满** → 等 Selector 通知 OP_READ / OP_WRITE 就绪 |
| 3 | SocketChannel read 返回 -1？| **对端关闭连接（EOF）** → 应该 close channel |
| **Reference 体系** | | |
| 4 | 四种引用 GC 行为？| **Strong（永不回收）/ Soft（内存不足才回收）/ Weak（GC 立即回收）/ Phantom（GC 后入队，get()=null）**|
| 5 | ReferenceHandler 谁启动的？| **JVM 启动时自动创建**，Thread.MAX_PRIORITY + Daemon，核心循环 tryHandlePending |
| 6 | pending 链表是什么？| GC 发现的待处理 Reference 链表（头插法）→ ReferenceHandler 逐个处理 |
| 7 | Cleaner（DirectByteBuffer）怎么清理？| PhantomReference 子类 → GC 后 ReferenceHandler 检测 `r instanceof Cleaner` → **c.clean() 直接调用 Unsafe.freeMemory()** |
| 8 | WeakHashMap key 为什么用 WeakReference？| key 被 GC 回收 → 自动入队 → expungeStaleEntries 清理 stale Entry → map 自动收缩 |
| 9 | ThreadLocalMap Entry 为什么有弱引用 key？| ThreadLocal 可能被 GC（如果没有强引用）→ 但 Entry 还有 value（强引用）→ 弱引用 key 让 GC 能回收 ThreadLocal |
| 10 | Finalizer 和 FinalReference？| 覆写 finalize() 的对象 → 入 Finalizer 队列 → FinalizerThread 串行执行 finalize() → **强烈不推荐使用**（Finalizer 线程低优先级）|

---

**文档版本：v1.3 · 2026-10-20**  
**源码基线：JDK 1.8.0_451**  
**v1.3 更新：新增第十二章 SocketChannel NIO 非阻塞 + 第十三章 java.lang.ref Reference 体系 + 11 个新面试考点（总 46 题）**

---

## 十五、扩展考点补全

---

## 十六、Serializable + Externalizable + ObjectOutputStream · Java 序列化协议

> **源码锚点**：`src/java/io/Serializable.java` + `Externalizable.java` + `ObjectOutputStream.java`  
> **面试权重**：⭐⭐⭐⭐⭐

### 16.1 序列化核心接口

```java
// ★ Serializable：标记接口（没有方法！只用来标记哪些类可以序列化）
public interface Serializable {
    // long serialVersionUID 是标准字段（不是接口方法）
    // 编译器建议每个 Serializable 类声明 serialVersionUID
    // 反序列化时比对 → 版本不一致 → InvalidClassException
}

// ★ Externalizable：Serializable 子接口（有两个方法，强制自定义序列化）
public interface Externalizable extends Serializable {
    // 自定义序列化：只写你想写的字段
    void writeExternal(ObjectOutput out) throws IOException;
    // 自定义反序列化：只从 out 读字段（注意：反序列化时会调用无参构造函数！）
    void readExternal(ObjectInput in) throws IOException, ClassNotFoundException;
}

// ★ Externalizable 注意点：
//   ① 必须有 public 无参构造函数（反序列化时调用）
//   ② 可以写 transient 字段（Serializable 序列化不到，但 Externalizable 你可以自己写）
//   ③ 性能更好（不反射，直接按你指定的字段写）
```

### 16.2 ObjectOutputStream.writeObject 完整流程

```java
// ★ ObjectOutputStream.writeObject 简化版：
public final void writeObject(Object obj) throws IOException {
    // ① 空值 + 顶层对象 → 触发 writeObjectOverride 钩子
    if (obj == null || enableResolve || streamClassDescriptor(obj).isProxy()) {
        // ...
    }
    // ★ ② 完整序列化流程（writeSerialData）：
    //
    writeObject0(obj, false);   // L1485-1500
}

private void writeObject0(Object obj, boolean unshared) {
    // ① 写 ObjectStreamConstants（协议头 magic = 0xACED，version = 5）
    //
    // ② 写 ClassDescriptor → ObjectStreamClass
    //    包括：类名、serialVersionUID、字段列表、父类、writeObject 方法签名
    //
    // ③ 每个字段：
    //    case SC_STREAM: // 字符串
    //      writeUTF(String s);
    //    case SC_LONG / SC_INT / SC_BOOLEAN ...:
    //      writeLong / writeInt / writeBoolean
    //    case SC_OBJECT: // 引用类型
    //      writeObject0(fieldValue);  // 递归调用（可能触发循环引用检测）
    //
    // ④ 特殊方法处理：
    //    if (hasWriteObject) {
    //      // 调用用户类的 writeObject(ObjectOutputStream) 方法
    //      // 里面可以 this.writeUTF / this.writeInt 自定义字段
    //    }
    //    if (hasWriteReplace) {
    //      // ★ 调用 writeReplace() → 替换成另一个对象再序列化
    //      obj = obj.writeReplace();
    //    }
}

// ★ 对象级替换（两种机制互补）：
//
// writeReplace()：序列化前替换（写到流里的是替换后的对象）
// readResolve()：反序列化后替换（从流里读出来的对象，换成 resolve 返回的对象）
//
// 典型应用：Enum 单例序列化、JDK StringCache、JDK 1.8 HashMap writeReplace
```

### 16.3 自定义序列化钩子方法

```java
// ★ 每个 Serializable 类可以定义 3 个私有方法：
//
// ① writeObject / readObject（最常用！）
//    → 调用方在 writeObject0 里检测到有这个方法 → invokeMethod 反射调用
//    → 里面可以：this.defaultWriteObject()（写默认字段）+ 自定义 writeInt/writeUTF
//
// ② writeReplace / readResolve（对象级替换）
//    → writeReplace 在 writeObject 之前调用 → 返回的对象才被序列化
//    → readResolve 在 readObject 之后调用 → 返回的对象才是 getObject() 的结果
//    → 典型应用：
//      JDK HashMap.writeReplace → 返回 HashMap.Entry[] 数组（避免循环引用）
//      JDK Enum.readResolve → 返回 Enum.valueOf(name)（反序列化后正确的单例）
//
// ③ readObjectNoData（父类反序列化钩子）
//    → 如果父类 Serializable + 子类新增字段 → 子类序列化后用老版本 JDK 反序列化
//      找不到子类字段 → 子类的 readObjectNoData 被调用（不是 readObject）
```

### 16.4 serialVersionUID · 版本控制

```java
// ★ 为什么建议每个 Serializable 类加 serialVersionUID？
//
// 如果没有显式声明 → JVM 根据类签名、字段列表等自动生成一个 hash
// 类一改字段（新增/删除/改类型）→ 自动生成的 hash 就变了
// → 反序列化时 InvalidClassException
//
// 显式声明 → 版本号自己控制 → 类改了字段但版本号不变 → 反序列化尽量兼容
//
// 兼容性规则：
//   新增字段 → 反序列化出来默认值（null/0/false）
//   删除字段 → 反序列化时跳过（旧版本序列化的字段新版本不读）
//   改字段类型 → InvalidClassException！
```

### 16.5 面试考点（序列化 6 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Serializable 和 Externalizable 区别？| Serializable 标记接口无方法；Externalizable 有 writeExternal/readExternal 强制自定义；Externalizable 必须 public 无参构造 |
| 2 | ObjectOutputStream.writeObject 流程？| ① 写协议头 magic=0xACED ② 写 ClassDescriptor（类名/字段列表）③ 递归写字段 ④ 调用 writeObject/writeReplace 钩子 |
| 3 | writeReplace 和 readResolve？| writeReplace 序列化前替换对象；readResolve 反序列化后替换对象（Enum 单例用 readResolve）|
| 4 | serialVersionUID 作用？| 显式控制版本号 → 类改了字段但版本号不变 → 反序列化尽量兼容（新增字段默认值）|
| 5 | transient 序列化？| **不会被序列化**（writeObject0 跳过 SC_NULL 字段）；但 Externalizable 可以手动写 transient 字段 |
| 6 | Java 序列化为什么慢？| 反射写每个字段 + 写完整元数据（ClassDescriptor 全字段）+ 循环引用检测 → 比 JSON/Protobuf 慢 5-10 倍 |

---

---

## 十七、Pattern + Matcher · Java 正则表达式引擎

> **源码锚点**：`src/java/util/regex/Pattern.java` + `Matcher.java`  
> **面试权重**：⭐⭐⭐

### 17.1 核心设计

```java
// ★ Java 正则引擎：非 NFA/DFA，用"回溯"算法（贪心 + 回溯）
//   Pattern.compile 把正则字符串编译成内部字节码（AbstractCharPredicate 数组）
//   Matcher 用这些字节码在目标字符串上执行匹配

public final class Pattern implements Serializable {
    
    public static Pattern compile(String regex) {
        return compile(regex, 0);
    }
    
    // flags 常量（位运算）：
    //   CASE_INSENSITIVE = 0x02  → 忽略大小写
    //   MULTILINE        = 0x08  → ^ 和 $ 匹配每行
    //   DOTALL           = 0x20  → . 也匹配 \n
    //   UNICODE_CASE     = 0x40  → Unicode 大小写
    
    public Matcher matcher(CharSequence input) {
        // ★ 对同一个 Pattern，每个 Matcher 独立状态（start/end/groups）
        //   Pattern 可以被多个 Matcher 共享（线程安全）
        return new Matcher(this, input, 0);
    }
    
    // ★ matches / find / group 常用 API：
    //   Pattern.matches("a*b", "aaab") → 静态方法（每次 compile → 低效）
    //   Pattern p = Pattern.compile("a*b"); p.matcher("aaab").matches() → 推荐！
}

// Matcher 核心 API：
public final class Matcher {
    
    public boolean matches()     // 整个字符串匹配
    public boolean find()        // 查找下一个匹配位置（可多次调用）
    public boolean find(int start)  // 从 start 位置开始查找
    public String group()        // 返回当前匹配的完整字符串
    public String group(int n)   // 返回第 n 个捕获组
    public int groupCount()      // 返回捕获组数量
    public int start()           // 匹配开始位置
    public int end()             // 匹配结束位置（不包含）
    
    public Matcher appendReplacement(StringBuffer sb, String replacement) {
        // ★ 替换第一个匹配（支持 $1, $2 捕获组引用）
    }
    public StringBuffer appendTail(StringBuffer sb) {
        // ★ 把最后一次匹配后的剩余字符串 append 到 sb
    }
}

// ★ 实战：提取 URL 参数
String url = "http://example.com?name=张三&age=25";
Pattern p = Pattern.compile("([?&])(\\w+)=([^&]+)");
Matcher m = p.matcher(url);
while (m.find()) {
    String key = m.group(2);    // name / age
    String value = m.group(3);  // 张三 / 25
}
```

### 17.2 面试考点（Pattern/Matcher 4 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Pattern.compile 为什么不每次都调？| **compile 编译昂贵**（生成内部字节码数组）→ 推荐复用 Pattern 实例 |
| 2 | Matcher 是线程安全的吗？| ❌ **不安全！** 每个 Matcher 内部有状态（start/end/groups）→ 每次 matcher() 返回新实例 |
| 3 | matches 和 find 区别？| matches 整个字符串匹配；**find 查找下一个匹配位置**（可多次调用循环遍历）|
| 4 | 捕获组怎么用？| group() 完整匹配；group(1)/group(2) 分别是第 1/2 个括号捕获的内容 |

---

**文档版本：v1.5 · 2026-10-21**  
**源码基线：JDK 1.8.0_451**  
**v1.5 更新：补 P1 缺口 §十七 Pattern + Matcher 正则引擎 + 4 个面试考点（总 24 题）**
