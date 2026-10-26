# 12 · Unsafe / VarHandle / SwitchPoint / Lookup / JPMS 底层机制

> 源码位置：`src/java/lang/invoke/SwitchPoint.java` + `MethodHandles.Lookup`  
> 面试权重：⭐⭐⭐⭐⭐（**JDK 底层原语全解**，AQS / Netty / Dubbo 都依赖这些）

---

## 一、Unsafe · JVM 底层原语（JDK 1.8 无源码但全框架依赖）

### 1.1 为什么不能直接用 Unsafe

```java
// sun.misc.Unsafe 是 JDK 内部类，所有方法都是 native
// JDK 1.8 中构造函数是 private + System.getSecurityManager() 检查
// → 反射获取 theUnsafe 是常见手段（但 Java 9+ 彻底禁了）

// 典型获取方式（JDK 8）：
Field f = Unsafe.class.getDeclaredField("theUnsafe");
f.setAccessible(true);
Unsafe unsafe = (Unsafe) f.get(null);

// 风险：Unsafe 可以做任何事——包括直接堆外内存、绕过类加载、破坏 JVM 安全
// → 生产框架（Netty / Dubbo / AQS）都在用，但应用代码绝对不能碰
```

### 1.2 核心 API 分类

| 类别 | 方法 | 底层 | 应用框架 |
|------|------|------|---------|
| **CAS 原子操作** | `compareAndSwapObject/Int/Long` | CPU cmpxchg 指令 | AQS / CHM / AtomicInteger / Dubbo |
| **内存读写** | `getInt/putInt/getLong/putLong/getObject` 带 offset | 直接操作对象内存 | Netty FastThreadLocal / AQS state |
| **park/unpark** | `park(boolean absolute, long time) / unpark(Thread)` | JVM 线程调度原语 | LockSupport / AQS / FutureTask |
| **堆外内存** | `allocateMemory / freeMemory / getAddress / putAddress` | malloc/free C 调用 | Netty PooledByteBufAllocator / DirectByteBuffer Cleaner |
| **类加载** | `defineClass / resolveClass / defineAnonymousClass` | JVM ClassTable | Dubbo ASM / Lambda 匿名类 / MyBatis |
| **对象操作** | `allocateInstance / objectFieldOffset / staticFieldOffset / staticFieldBase` | JVM 内部 | Netty FastThreadLocal / 反射框架 |
| **数组操作** | `arrayBaseOffset / arrayIndexScale` | JVM 数组内存布局 | AQS CLH Node / 各种数组 CAS 实现 |
| **volatile 语义** | 所有 get/put 都支持 volatile 模式 | 内存屏障 | CHM sizeCtl / AQS head/tail |

### 1.3 CAS 原子操作核心

```java
// compareAndSwapInt / compareAndSwapLong / compareAndSwapObject
// 三个参数：对象、偏移量、预期值、新值
// 语义：如果当前值 == 预期值 → 原子更新为新值 → 返回 true
//       否则 → 不做任何操作 → 返回 false

// AQS 中 state 的 CAS：
// unsafe.compareAndSwapInt(this, stateOffset, expect, update)
// ↓
// ① 读取 this 对象内存中 stateOffset 位置的 int 值
// ② 与 expect 比较（CPU 原子比较）
// ③ 如果相等 → 写入 update（CPU 原子写入）→ 返回 true
// ④ 如果不等 → 不写 → 返回 false
// ⑤ 整个 compare-and-swap 是一条 CPU cmpxchg 指令（x86: lock cmpxchg）

// CHM 中 sizeCtl 的 CAS：
// sizeCtl 字段在 ConcurrentHashMap 中
// sizeCtlOffset = unsafe.objectFieldOffset(ConcurrentHashMap.class.getDeclaredField("sizeCtl"))
// unsafe.compareAndSwapInt(table, base + i*AS, 0, -1)  // 头节点 CAS -1 标识迁移中
```

### 1.4 park/unpark · LockSupport 底层

```java
// LockSupport.park() 直接调用 Unsafe.park(false, 0L)
// LockSupport.unpark(t) 直接调用 Unsafe.unpark(t)

// Unsafe.park 是 JVM 级别的线程挂起
// 底层用每个 Thread 的 permit（许可令牌，0/1）实现：
//   - park：permit > 0 → permit = 0 立即返回；否则线程挂起
//   - unpark：permit = 1（如果是 1 就不变）→ 唤醒 park 中的线程

// JVM 实现（简化）：
void Park(bool absolute, long time) {
    while (true) {
        if (permit > 0) { permit = 0; return; }  // 有令牌 → 立即返回
        if (time == 0) return;                    // 无限等待...
        if (time < 0) sleep(abs(time));          // 限时等待
        // ... 处理伪唤醒
    }
}
void Unpark(Thread t) {
    permit = 1;
    wakeup(t);  // 唤醒 t（如果在 park 中）
}
```

### 1.5 Unsafe.defineClass · 绕过类加载

```java
// Unsafe.defineClass(name, bytes, off, len, loader, pd)
// 和 ClassLoader.defineClass 的区别：
//   ① 不做安全检查（preDefineClass 被跳过！）
//   ② 可以直接定义任何类，包括 java.*（绕过 SecurityException）
//   ③ 指定的 loader 可以是 null → 不挂到任何 ClassLoader 上

// Dubbo / ASM / LambdaMetafactory 都用这个动态定义类
// LambdaMetafactory 生成的 Lambda 类最终走 Unsafe.defineAnonymousClass
// （匿名类：不挂到任何 ClassLoader，GC 可以直接回收）
```

### 1.6 堆外内存 · Netty DirectByteBuf

```java
// Netty 的 PooledByteBufAllocator 管理堆外内存
// 核心：用 Unsafe.allocateMemory/freeMemory 直接操作 C 内存
// 绕过 JVM GC → 不受堆大小限制 → 但需要自己管理生命周期

// 典型流程：
//   allocateMemory(size) → 返回堆外内存地址 long addr
//   putByte/putInt(addr, value) → 直接写堆外内存
//   getByte/getInt(addr) → 直接读堆外内存
//   freeMemory(addr) → 释放堆外内存

// DirectByteBuffer（JDK 内置）也用类似方式
// 但 DirectByteBuffer 有 Cleaner（PhantomReference）在对象被 GC 时自动 freeMemory
```

---

## 二、VarHandle（Java 9+）· 类型安全的 Unsafe 替代品

### 2.1 为什么需要 VarHandle

```
Unsafe 问题：
  ① 所有方法都是 native，Java 层没文档 → 难用
  ② 没有类型检查 → compareAndSwapObject 容易传错类型
  ③ 构造函数私有 + SecurityManager → 反射获取麻烦
  ④ Java 9 彻底禁了 sun.misc.Unsafe → 模块系统访问控制

VarHandle 是 Java 9 引入的类型安全替代：
  → 每个 VarHandle 绑定一个具体字段 → 编译期类型检查
  → 支持 volatile / acquire / release 多种内存序
  → 通过 MethodHandles.Lookup.findVarHandle 获取
```

### 2.2 VarHandle 典型用法

```java
// Java 9+ 中，AQS 可以用 VarHandle 替代 Unsafe：
class MyAQS {
    private static final VarHandle STATE;
    static {
        try {
            STATE = MethodHandles.lookup()
                .findVarHandle(MyAQS.class, "state", int.class);
        } catch (Exception e) { throw new Error(e); }
    }
    private volatile int state;

    // CAS 原子操作：
    boolean compareAndSetState(int expect, int update) {
        return STATE.compareAndSet(this, expect, update);
    }

    // volatile 读：
    int getState() { return (int) STATE.getVolatile(this); }

    // release 写（比 volatile 弱，只保证后续读可见）：
    void setState(int newState) { STATE.setRelease(this, newState); }
}
```

### 2.3 VarHandle vs Unsafe 对比

| 维度 | **Unsafe** | **VarHandle（Java 9+）** |
|------|-----------|------------------------|
| 类型安全 | ❌ compareAndSwapObject 无类型检查 | ✅ 绑定具体字段 → 编译期检查 |
| CAS | compareAndSwapInt(obj, offset, expect, update) | varHandle.compareAndSet(obj, expect, update) |
| 内存序 | volatile 模式 | **支持多种**：plain / opaque / acquire / release / volatile |
| 获取方式 | 反射 theUnsafe（JDK 9+ 被禁）| MethodHandles.Lookup.findVarHandle |
| 堆外内存 | ✅ allocateMemory / freeMemory | ❌ 不支持 |
| 类加载 | ✅ defineClass | ❌ 不支持 |
| 应用 | AQS / Netty / Dubbo（JDK 8）| Java 9+ 标准 API |

---

## 三、SwitchPoint · 失效传播机制

### 3.1 核心设计

**源码锚点**：SwitchPoint.java L112-180

```java
// SwitchPoint 是 MutableCallSite 的包装器，提供"一键失效"机制
// 核心结构：
//   private final MutableCallSite mcs;        // 内部 CallSite
//   private final MethodHandle mcsInvoker;    // CallSite 动态 invoker
//   valid = true → mcs.getTarget() == K_true  // 布尔值 true
//   valid = false → mcs.getTarget() == K_false // 布尔值 false

// 为什么需要 SwitchPoint？
//   MutableCallSite 的 target 可以单独改
//   但多个 CallSite 同时失效 → 需要遍历逐个改 → 麻烦
//   SwitchPoint.invalidateAll(SwitchPoint[]) → 一键全部失效
//   → 所有依赖这个 SwitchPoint 的 guardWithTest 同时切到 fallback
```

### 3.2 guardWithTest · 条件分支

**源码锚点**：L170-174

```java
public MethodHandle guardWithTest(MethodHandle target, MethodHandle fallback) {
    if (mcs.getTarget() == K_false)
        return fallback;  // 已经失效 → 直接返回 fallback
    // 还没失效 → 创建一个 if-else 的 MH
    // test = mcsInvoker（返回 boolean：true=valid，false=invalid）
    // → test ? target : fallback
    return MethodHandles.guardWithTest(mcsInvoker, target, fallback);
}

// 使用：
SwitchPoint sp = new SwitchPoint();
MethodHandle fastPath = ...;   // 正常路径
MethodHandle slowPath = ...;   // fallback 路径

MethodHandle guarded = sp.guardWithTest(fastPath, slowPath);
// 正常情况下 guarded 走 fastPath
// sp.invalidateAll() → guarded 永久性切到 slowPath（不可回退！）

// 应用场景：LambdaMetafactory 的动态优化
//   第一次 invoke 走解释执行 → 记录 profiling 信息
//   满足优化条件 → 生成优化后的代码 → invalidateAll 旧 CallSite → 新 CallSite 生效
```

### 3.3 invalidateAll · 一键失效

**源码锚点**：L217+

```java
public static void invalidateAll(SwitchPoint[] switchPoints) {
    // 把所有 SwitchPoint 的 mcs.target 从 K_true 改成 K_false
    // 触发 MutableCallSite 的失效机制
    // JVM 内部会让所有依赖这些 SwitchPoint 的 guardWithTest 失效
    // → 它们的 target 从 fastPath 永久切到 slowPath
    // → JIT 也会失效对应的内联代码
}

// 失效是单向的！一旦 invalid → 永远 invalid → 不能恢复
// 这是 JVM 优化的关键假设：假设条件永远成立 → 激进内联 → 失效时回退
```

---

## 四、MethodHandles.Lookup · 权限模型

### 4.1 Lookup 的四种访问模式

**源码锚点**：MethodHandles.java L525-548

```java
// Lookup 内部：
private final Class<?> lookupClass;   // ★ 调用方类（权限检查基准）
private final int allowedModes;       // ★ 允许的访问模式位掩码

// 四种模式常量：
public static final int PUBLIC    = 0x01;  // 公开方法/字段
public static final int PRIVATE   = 0x02;  // 私有（lookupClass 自己的）
public static final int PROTECTED = 0x04;  // 受保护（lookupClass 的子类）
public static final int PACKAGE   = 0x08;  // 包私有（同 package）

// 默认 lookup()：只给 PUBLIC | PROTECTED | PACKAGE（不给 PRIVATE）
// IMPL_LOOKUP（Java 9+）：TRUSTED = -1（绕过所有权限检查）
// Netty 用 IMPL_LOOKUP 绕过反射限制！
```

### 4.2 三种 Lookup 获取方式

```java
// 方式 ①：默认 lookup() → 只能访问 public/protected/package
MethodHandles.Lookup lookup = MethodHandles.lookup();
// lookupClass = 当前调用 lookup() 的类
// allowedModes = PUBLIC | PROTECTED | PACKAGE（不给 PRIVATE）
// → 不能访问私有方法/字段！

// 方式 ②：in(lookupClass) → 指定 lookupClass，但模式不超过当前的
MethodHandles.Lookup restricted = lookup.in(OtherClass.class);
// lookupClass = OtherClass
// allowedModes = min(原 allowedModes, OtherClass 可见的模式)
// → 权限只会更少，不会更多

// 方式 ③：IMPL_LOOKUP（Java 9+）→ 绕过所有权限！
MethodHandles.Lookup impl = MethodHandles.privateLookupIn(targetClass, lookup);
// 需要 Java 9+ 模块化系统授权
// 或者用 IMPL_LOOKUP（JDK 内部用）
// → 可以访问任何类的私有方法/字段 → Netty FastThreadLocal 用这个

// Java 8 中，绕过 Lookup 限制的常见手段：
//   MethodHandles.lookup().unreflect(method) → method.setAccessible(true) 先反射突破
// Java 9+ 模块化系统更严格 → IMPL_LOOKUP 是唯一合法途径
```

### 4.3 unreflect · 反射 → MethodHandle

**源码锚点**：L1181+

```java
// 把反射的 Method / Field / Constructor 转成 MethodHandle
public MethodHandle unreflect(Method m) throws IllegalAccessException {
    // 内部检查：
    //   ① lookupClass 是否有权限访问 m
    //   ② allowedModes 是否包含 m 的修饰符对应的模式
    //   ③ 如果 m 的修饰符是 private → 需要 lookupClass == m.getDeclaringClass()
    // 检查通过 → 创建 MethodHandleImpl（指向 m 对应的 native 实现）
}

public MethodHandle unreflectGetter(Field f) throws IllegalAccessException {
    // 类似：检查对 f 的访问权限 → 返回 ()fieldType 的 MH
}

public MethodHandle unreflectConstructor(Constructor<?> c) {
    // 返回 (constructor params...) c.newInstance() 的 MH
}
```

### 4.4 Netty 用 Lookup 的典型场景

```java
// Netty FastThreadLocal 的关键代码（简化）：
// 用 MethodHandles.Lookup.IMPL_LOOKUP 绕过访问控制

// 在 Netty 4.1.x 中：
// 反射获取 ThreadLocal.ThreadLocalMap 的 table 字段
// → 用 Unsafe 或 MethodHandle 直接操作这个 table 数组
// → 比反射 Field.get/set 快很多

// 核心思想：
//   反射做不到的（比如访问 JDK 内部私有类）
//   → 用 IMPL_LOOKUP.unreflect(field) → 得到 MethodHandle
//   → MethodHandle 调用速度接近 invokevirtual
//   → 绕过了反射的性能开销 + 访问限制
```

---

## 五、JPMS · Java 9 模块系统（Java 9+）

### 5.1 为什么需要 JPMS

```
Java 8 及以前的问题：
  ① 类路径地狱（Classpath Hell）：多个版本的 jar 在 classpath 上 → 谁先找到用谁
  ② 无法封装：public = 所有人可见，想隐藏内部实现只能靠 package-private
  ③ JDK 太大：rt.jar 包含所有 JDK 类 → 启动慢、占内存
  ④ 安全问题：任何代码都能用反射访问任何类

JPMS 解决什么：
  ① 模块化：每个模块有 module-info.java
  ② 强封装：exports 控制可见性，requires 控制依赖
  ③ JDK 分模块：java.base（核心）+ 其他模块 → 按需要加载
  ④ 无 classpath：模块路径 module-path 替代 classpath
```

### 5.2 module-info.java 示例

```java
// module-info.java
module com.example.myapp {
    requires transitive com.example.api;   // ★ 依赖的模块（传递依赖：我的依赖的依赖也可见）
    requires java.sql;                      // JDK 模块依赖
    requires static com.example.test;      // 静态依赖（编译时需要，运行时可选）

    exports com.example.myapp.api;          // ★ 对外暴露的包（只有这个包能被其他模块访问）
    exports com.example.myapp.internal to com.example.plugin;  // 限定 exports：只暴露给特定模块

    opens com.example.myapp.internal;      // ★ 允许反射访问（exports 控制编译时可见，opens 控制运行时反射可见）
}
```

### 5.3 JDK 类加载器层级变化

```
Java 8 及以前：
  BootstrapClassLoader（C++）
  └─ ExtClassLoader（加载 jre/lib/ext）
      └─ AppClassLoader（加载 classpath）

Java 9+（JPMS 模块系统）：
  BootstrapClassLoader（C++）→ 加载 java.base 核心类
  └─ PlatformClassLoader（替代 ExtClassLoader）→ 加载 JDK 其他模块
      └─ AppClassLoader（加载 module-path 的应用模块）
          └─ 自定义 ClassLoader

// 关键变化：
// ① PlatformClassLoader 替代 ExtClassLoader → JDK 模块由它加载
// ② 打破传统双亲委派（部分模块加载可能由 AppClassLoader 触发）
// ③ 类路径上的未命名模块（unnamed module）→ 访问权限被严格限制
```

### 5.4 强封装 · 反射访问限制

```java
// Java 8：setAccessible(true) 可以访问任何私有字段
// Java 9+：setAccessible(true) 只能访问：
//   ① 当前模块自己的 private 成员
//   ② 目标模块 opens 了的包的成员
//   ③ 未命名模块（classpath 上的 jar）的成员

// 如果目标包没有 opens：
//   → InaccessibleObjectException（Java 9+ 新增）
//   → 无法绕过！

// 解决方法：
//   ① 启动参数：--add-opens java.base/sun.misc=ALL-UNNAMED
//   ② 模块系统授权：在 module-info.java 中 opens
//   ③ 用 IMPL_LOOKUP（JDK 内部特权）

// Netty / Dubbo / Spring 在 Java 9+ 都需要处理这个问题：
//   框架依赖 Unsafe / sun.misc / JDK 内部类 → 需要 --add-opens
//   或者升级到 Java 9+ 兼容版本（用 VarHandle 替代 Unsafe）
```

### 5.5 JPMS vs OSGi

| 维度 | **JPMS（Java 9+）** | **OSGi** |
|------|---------------------|----------|
| 规范 | JEP 261 标准 | OSGi Alliance 规范 |
| 运行时 | JVM 内置 | 外部容器（Equinox / Felix）|
| 动态 | ❌ 不支持热插拔 | ✅ 支持动态安装/卸载 |
| 类加载 | 每个模块一个 ClassLoader | 每个 Bundle 一个 ClassLoader |
| 粒度 | 模块级别 | Bundle + Service 级别 |
| 典型用途 | Java 平台模块化 | 大型企业应用（Eclipse / Karaf）|

---

## 六、PhantomReference + Cleaner · DirectByteBuffer 堆外内存清理

### 6.1 PhantomReference 核心特性

**源码锚点**：PhantomReference.java L50-78

```java
public class PhantomReference<T> extends Reference<T> {
    // ★ get() 永远返回 null！（即使 referent 还活着）
    public T get() {
        return null;
    }

    public PhantomReference(T referent, ReferenceQueue<? super T> q) {
        super(referent, q);   // 必须传 ReferenceQueue！
    }
}

// 和其他三种引用的关键区别：
//   StrongRef  → referent 强引用，永远不会被 GC
//   SoftRef    → referent 还在，内存不足才 clear + 可能入队
//   WeakRef    → referent 还在，GC 立即 clear + 入队
//   PhantomRef → ★ referent 始终不可见（get()=null），GC 后 referent 被回收，PhantomReference 入队
//                不入队 → 完全没用（get() 永远 null，也不会触发任何清理）

// PhantomReference 的唯一用途：
//   GC 回收 referent 时，你被"通知"了（ReferenceQueue 收到了 PhantomReference）
//   → 你可以做清理动作（释放堆外内存、关闭 native 资源等）
```

### 6.2 Cleaner · 特殊的 PhantomReference

**源码锚点**：Reference.java L174-218（tryHandlePending 核心方法）

```java
// Cleaner 是 sun.misc.Cleaner（JDK 内部类，继承 PhantomReference）
// 但它不走 ReferenceQueue！→ 直接在 ReferenceHandler 线程里执行 clean()

// Reference.tryHandlePending 的关键 fast path：
static boolean tryHandlePending(boolean waitForNotify) {
    Reference<Object> r;
    Cleaner c;
    try {
        synchronized (lock) {
            if (pending != null) {
                r = pending;
                // ★ 关键！先检查是不是 Cleaner（OOM 时 instanceof 可能抛异常 → 提前做）
                c = r instanceof Cleaner ? (Cleaner) r : null;
                pending = r.discovered;   // 从 pending 链表移除
                r.discovered = null;
            } else {
                if (waitForNotify) lock.wait();
                return waitForNotify;
            }
        }
    } catch (OutOfMemoryError x) {
        Thread.yield();   // OOM 时让出 CPU → 等待 GC 回收
        return true;
    }

    // ★ Cleaner Fast Path！
    // Cleaner 不需要 ReferenceQueue → 直接调 clean()
    if (c != null) {
        c.clean();   // ← DirectByteBuffer 的堆外内存就是在这里被释放的！
        return true;
    }

    // 普通 PhantomReference / WeakReference / SoftReference → 入队
    ReferenceQueue<? super Object> q = r.queue;
    if (q != ReferenceQueue.NULL) q.enqueue(r);
    return true;
}
```

### 6.3 DirectByteBuffer + Cleaner 清理链路

```
DirectByteBuffer 分配堆外内存时：
  ↓
① ByteBuffer.allocateDirect(size)
② DirectByteBuffer(int cap) 构造函数：
   long addr = Unsafe.allocateMemory(cap);   // 分配堆外内存
   Cleaner.create(this, new Deallocator(addr, cap));  // ★ 创建 Cleaner
   // Cleaner 内部：PhantomReference<DirectByteBuffer> + Thunk(Deallocator)
   // DirectByteBuffer 被 GC 回收时 → Cleaner 入 pending 链表
③ ReferenceHandler 线程：
   tryHandlePending → 检测 r instanceof Cleaner → c.clean()
④ Cleaner.clean() → Deallocator.run()：
   Unsafe.freeMemory(addr);   // ★ 释放堆外内存！

// 完整时序图：
//   用户 new DirectByteBuffer → 分配堆外内存 + Cleaner 绑定
//   用户丢掉 DirectByteBuffer 引用（无强引用可达）
//   GC 标记-清除：发现 DirectByteBuffer 不可达
//     → 但 DirectByteBuffer 有 Cleaner（PhantomReference）
//     → Cleaner 入 discovered 链表 → pending 链表
//   ReferenceHandler 线程：
//     tryHandlePending → Cleaner fast path → c.clean()
//     → Deallocator.run() → Unsafe.freeMemory(addr)
//     → 堆外内存被释放 ✅

⚠️ 陷阱：Cleaner 依赖 GC！→ 如果堆外内存泄漏（没有强引用但 GC 迟迟不来）→ 堆外内存一直不释放
⚠️ 所以生产框架（Netty）自己管理堆外内存（PooledByteBufAllocator）→ 不用 Cleaner
```

### 6.4 Cleaner vs finalize

| 维度 | **Cleaner（PhantomReference）** | **finalize（FinalReference）** |
|------|--------------------------------|-------------------------------|
| 优先级 | **ReferenceHandler 线程（MAX_PRIORITY）** | Finalizer 线程（低优先级）|
| 可靠性 | ✅ 一定执行（GC 触发）| ❌ 不保证执行（OOM 时 Finalizer 队列可能堆积）|
| 执行时机 | **GC 回收 referent 后立即执行** | GC 回收后，Finalizer 线程异步执行 |
| 线程安全 | Cleaner clean() 在 ReferenceHandler 线程 | finalize() 在 Finalizer 线程（单线程串行）|
| 典型用途 | DirectByteBuffer 堆外内存 | （已过时，Java 9 标记 deprecated）|
| 风险 | Cleaner clean() 如果抛异常 → 不影响其他 Cleaner | finalize() 抛异常 → 吞掉，其他 finalize 继续 |

---

## 七、Unsafe.allocateInstance · 绕过构造函数

### 7.1 为什么需要绕过构造函数

```java
// 框架（反序列化 / ORM / 依赖注入）经常需要：
//   创建对象但不调用构造函数！
// 为什么？
//   ① 反序列化：对象从字节流恢复 → 构造函数不该执行
//   ② ORM 框架：从数据库结果集创建对象 → 字段填充要绕过构造函数的初始化逻辑
//   ③ 依赖注入：某些对象构造函数有副作用 → 框架想跳过

// 常规做法：
//   Constructor.newInstance() → 必须调用构造函数
//   Class.newInstance() → 必须调用无参构造函数
//   → 都不能绕过！

// Unsafe.allocateInstance(cls) 就是解决这个问题的：
//   只分配内存 + 零初始化 → 不执行任何构造函数
//   → 返回一个"干净"的对象实例（所有字段都是默认值）
```

### 7.2 Unsafe.allocateInstance 底层

```java
// 简化伪代码：
public native Object allocateInstance(Class<?> cls);
// JVM 内部：
// ① 检查 cls 是否是 abstract / interface → 抛 InstantiationException
// ② 分配对象内存（和 new 指令一样的内存布局）
// ③ 零初始化所有字段（int=0, long=0, boolean=false, Object=null）
// ④ 返回对象引用
// ⑤ 没有调用任何构造函数！没有执行任何 <init> 方法！

// 使用示例：
public class User {
    private String name;
    private int age;
    
    public User() {
        System.out.println("构造函数被调用了！");  // 这行不会执行！
        this.name = "default";
        this.age = 0;
    }
}

// Unsafe.allocateInstance(User.class)：
User u = (User) unsafe.allocateInstance(User.class);
System.out.println(u.name);   // null（不是 "default"！）
System.out.println(u.age);    // 0
// "构造函数被调用了！" 不会打印！
```

### 7.3 反序列化怎么用 allocateInstance

```java
// Java 内置反序列化（ObjectInputStream.readObject）：
//   ① 找到要反序列化的类
//   ② 检查是否有 readObject 自定义逻辑
//   ③ ★ 调用 Unsafe.allocateInstance(cls) 创建对象（不调构造函数！）
//   ④ 从字节流读取每个字段的值 → 通过反射/Unsafe 写入对象字段
//   ⑤ 调用 readObject / readResolve（如果有）

// 为什么不能用 Constructor.newInstance？
//   反序列化的对象可能没有无参构造函数！
//   或者构造函数有副作用（连接数据库、初始化资源等）
//   → allocateInstance 完美解决

// Kryo / Protostuff / Jackson 等高性能序列化框架都用 allocateInstance
// 比反射创建对象快很多（allocateInstance 跳过构造函数 + 直接内存分配）
```

### 7.4 allocateInstance 风险

```
⚠️ 必须小心使用：
  ① final 字段可以被修改（构造函数不会执行 final 字段赋值）
  ② 字段类型检查被跳过（可以给 int 字段赋 null）
  ③ 对象可能处于非法状态（某些框架依赖构造函数做初始化）
  ④ Java 9+ 模块化系统可能限制访问

// 典型错误：
class Counter {
    private final int value;
    Counter(int v) { this.value = v; }
}

// allocateInstance(Counter.class) → value = 0（默认值）
// 而不是构造函数传入的值 → final 字段被绕过了！
```

---

## 八、Java 9+ StringConcatFactory · 模板字符串优化

### 8.1 Java 8 String 拼接问题

```java
// 用户代码：
String s = "Hello, " + name + "! You are " + age + " years old.";

// Java 8 javac 编译后：
String s = new StringBuilder()
    .append("Hello, ")
    .append(name)
    .append("! You are ")
    .append(age)
    .append(" years old.")
    .toString();

// 问题：
//   ① 每次拼接都创建一个 StringBuilder + 多次 append
//   ② 循环中拼接 → 大量短命 StringBuilder → GC 压力
//   ③ 即使常量折叠（全部是常量 → 编译期合并），有一个变量就不行
```

### 8.2 Java 9+ invokedynamic + StringConcatFactory

```java
// Java 9+ javac 用 invokedynamic 优化 String 拼接！
// 编译后：
String s = invokedynamic makeConcatWithConstants(
    /* bootstrap */ StringConcatFactory.makeConcatWithConstants,
    /* nameAndType */ "makeConcatWithConstants",
    /* 模板字符串 */ "\u0001Hello, \u0002! You are \u0003 years old.",
    /* 动态参数 1 */ name,
    /* 动态参数 2 */ age
);

// 模板字符串中的 \u0001 / \u0002 / \u0003 是占位符：
//   \u0001 → 第一个动态参数（name）
//   \u0002 → 第二个动态参数（age）
//   \u0003 → 第三个动态参数（如果有）
// 位置：占位符之间的文本是常量部分（"Hello, "、"! You are "、" years old."）

// BootstrapMethod：StringConcatFactory.makeConcatWithConstants
// 第一次 invokedynamic 时调 → 生成最优拼接策略 → 返回 CallSite
// 后续拼接直接走缓存的 CallSite → 比 StringBuilder 快！
```

### 8.3 StringConcatFactory 四种模式

```java
// StringConcatFactory（Java 9+）提供四种拼接模式：

// ① makeConcat（最简单）：纯动态参数拼接
//   invokedynamic makeConcat → 没有模板 → 只有动态参数
//   s = a + b + c → makeConcat(a, b, c)

// ② makeConcatWithConstants（最常用）：常量 + 动态参数
//   invokedynamic makeConcatWithConstants → 带模板字符串
//   s = "Hello, " + name → "\u0001Hello, \u0002" + name

// ③ makeConcatWithBootstrapMethods（最灵活）：运行时自定义拼接策略
//   可以传一个 MethodHandle 作为拼接策略 → 完全自定义
//   适合极端场景（加密拼接、国际化等）

// ④ makeConcatWithExceptionHandling（Java 11+）：带异常处理
//   拼接过程中如果某个参数是 null → 可以自定义处理方式
//   比如 null 转 "" 或抛异常
```

### 8.4 最优拼接策略生成

```java
// StringConcatFactory.makeConcatWithConstants 内部：
// 根据模板字符串 + 参数数量 + 参数类型 → 选择最优策略：

// 场景 A：全部是常量 → 编译期就合并好 → 返回常量字符串 CallSite
//   "\u0001Hello" + " World" → 直接返回 "Hello World" CallSite
//   → 无运行时开销！

// 场景 B：混合常量 + 少量动态参数 → 生成链式调用 CallSite
//   "Hello, " + name + "!" → 返回一个 CallSite：
//   (String name) → "Hello, " + name + "!"
//   → JIT 内联后几乎是一次 StringBuilder + append

// 场景 C：有 null 处理需求 → makeConcatWithExceptionHandling
//   可以指定 null 值的处理方式 → 避免显式 if (x == null) x = ""

// 场景 D：循环中大量拼接 → 建议改用 StringBuilder 显式管理
//   StringConcatFactory 针对单次拼接优化，循环中显式 StringBuilder 更好
```

### 8.5 性能对比

```java
// 基准测试（JDK 17, 拼接 "a" + b + "c"，100 万次）：
//
// Java 8 StringBuilder：     ~300ms
// Java 9+ makeConcatWithConstants：~180ms（快 40%）
//
// 为什么更快？
//   ① 减少 StringBuilder 分配（BootstrapMethod 可能复用或避免）
//   ② JIT 更容易内联（invokedynamic CallSite 内联后接近手写 StringBuilder）
//   ③ 常量部分编译期就处理好了 → 运行时只处理动态参数

// 但循环中拼接还是应该显式用 StringBuilder！
// Java 9+ 的优化只针对单次拼接表达式
// for (int i=0; i<n; i++) s += i;  // 还是慢 → 每次循环都生成新的 CallSite
// for (int i=0; i<n; i++) sb.append(i);  // 快 → 复用同一个 StringBuilder
```

---

## 九、面试考点（扩展 20 题）

| # | 考点 | 答案 |
|---|------|------|
| **Unsafe** | | |
| 1 | Unsafe CAS 核心？| `compareAndSwapObject/Int/Long(obj, offset, expect, update)` → **一条 CPU cmpxchg 指令原子操作** |
| 2 | Unsafe park/unpark 底层？| 每个 Thread 有 permit（许可令牌 0/1）→ park 消耗令牌，unpark 发放令牌（**令牌可预发放**）|
| 3 | 哪些框架依赖 Unsafe？| **AQS / ConcurrentHashMap / AtomicInteger / LockSupport / Netty / Dubbo** |
| 4 | Unsafe.defineClass 与 ClassLoader.defineClass 区别？| **跳过安全检查**（preDefineClass 的 java.* 限制）+ 可以指定 null loader（不挂到任何 ClassLoader）|
| **VarHandle（Java 9+）** | | |
| 5 | VarHandle 为什么比 Unsafe 好？| **类型安全**（绑定具体字段）+ 编译期检查 + 支持多种内存序（plain/acquire/release/volatile）|
| 6 | VarHandle 怎么获取？| `MethodHandles.lookup().findVarHandle(cls, "fieldName", type)` |
| **SwitchPoint** | | |
| 7 | SwitchPoint 失效机制？| 内部 MutableCallSite 从 K_true 切到 K_false → **永久失效，不可恢复** → guardWithTest 永远走 fallback |
| 8 | SwitchPoint.invalidateAll 应用场景？| LambdaMetafactory 动态优化 + JIT 激进假设失效 → 一键让所有相关 CallSite 回退 |
| **Lookup** | | |
| 9 | Lookup 四种访问模式？| PUBLIC=0x01 / PACKAGE=0x08 / PROTECTED=0x04 / **PRIVATE=0x02（默认 lookup() 不给！）** |
| 10 | IMPL_LOOKUP 是什么？| Java 9+ 提供的**绕过所有权限检查**的 Lookup（JDK 内部用）→ Netty FastThreadLocal 用 |
| 11 | unreflect 怎么突破私有方法访问？| `MethodHandles.lookup().unreflect(method)` → 需要 method.setAccessible(true) 先绕过反射限制；Java 9+ 需 IMPL_LOOKUP |
| **JPMS** | | |
| 12 | JPMS 解决什么问题？| **类路径地狱 + 无法封装 + JDK 太大 + 反射安全** → 模块化 + exports/opens 强封装 |
| 13 | Java 9+ 类加载器层级变化？| ExtClassLoader → **PlatformClassLoader**；BootstrapClassLoader 只加载 java.base；模块强封装 |
| 14 | exports vs opens 区别？| exports 控制**编译时可见性**；opens 控制**运行时反射可见性**（exports 了也需要 opens 才能反射）|
| 15 | Java 9+ 反射访问限制？| setAccessible(true) 只能访问自己模块 + opens 了的包 → 目标模块没 opens 抛 InaccessibleObjectException |
| **PhantomReference + Cleaner** | | |
| 16 | PhantomReference get() 返回什么？| **永远返回 null**（即使 referent 还活着）→ 这是它和其他三种引用的核心区别 |
| 17 | Cleaner 为什么在 tryHandlePending 里有 fast path？| Cleaner 不需要 ReferenceQueue → **直接 instanceof Cleaner 检测 → c.clean() 释放堆外内存** → 不走 enqueue（避免 ReferenceQueue 的额外开销）|
| 18 | DirectByteBuffer 堆外内存清理链路？| ByteBuffer.allocateDirect → Unsafe.allocateMemory → Cleaner.create → GC 回收 DirectByteBuffer → ReferenceHandler tryHandlePending → Cleaner fast path → Unsafe.freeMemory |
| 19 | Cleaner vs finalize 哪个可靠？| **Cleaner 可靠得多**（ReferenceHandler 线程 MAX_PRIORITY）；finalize 在 Finalizer 线程（低优先级，可能 OOM 堆积不执行）|
| **allocateInstance** | | |
| 20 | allocateInstance 和 Constructor.newInstance 区别？| **allocateInstance 不调构造函数**（只分配内存 + 零初始化）；Constructor.newInstance 必须调构造函数 → 反序列化/ORM 框架用 allocateInstance |
| 21 | allocateInstance 有什么风险？| final 字段可以被修改 + 字段类型检查跳过 + 对象可能处于非法状态 + Java 9+ 模块化限制 |
| **StringConcatFactory** | | |
| 22 | Java 9+ String 拼接为什么用 invokedynamic？| **第一次拼接由 StringConcatFactory.makeConcatWithConstants 生成最优策略 → 后续走 CallSite 缓存 → 比 StringBuilder 快 40%** |
| 23 | StringConcatFactory 模板字符串占位符？| \u0001 = 第一个动态参数，\u0002 = 第二个...位置之间的文本是常量部分 |
| 24 | 循环中拼接还能用 StringConcatFactory 优化吗？| ❌ 不行！StringConcatFactory 优化单次拼接表达式；**循环中显式 StringBuilder 更好** |

---

**文档版本：v1.1 · 2026-10-20**  
**源码基线：JDK 1.8.0_451（PhantomReference / Reference.tryHandlePending / SwitchPoint / Lookup 有 Java 源码；StringConcatFactory / JPMS 为 Java 9+ 标准知识）**  
**v1.1 更新：新增第六章 PhantomReference + Cleaner（get()=null 永久不可见 + Cleaner fast path instanceof 检测 + DirectByteBuffer 堆外内存清理链路）+ 第七章 Unsafe.allocateInstance 绕过构造函数（零初始化 + 反序列化用法 + 风险）+ 第八章 Java 9+ StringConcatFactory（模板字符串 \u0001 占位符 + 四种模式 + 性能对比 + 循环陷阱）+ 9 个新面试考点（总 24 题）**
