# 11 · JVM invokedynamic 与 Lambda 底层原理

> 源码位置：`src/java/lang/invoke/` + `src/java/lang/String.java` + JVM 规范  
> 面试权重：⭐⭐⭐⭐⭐（Lambda 表达式核心 + MethodHandle + BootstrapMethods +  invokedynamic 字节码）

---

## 一、invokedynamic 是什么

### 1.1 为什么需要 invokedynamic

```
Java 7 以前，字节码的调用指令是"静态绑定"的：
  invokevirtual    → 调用虚方法（多态分发，靠 vtable）
  invokeinterface  → 调用接口方法
  invokespecial    → 调用私有/构造/父类方法
  invokestatic     → 调用静态方法

每个指令在编译时就确定了目标方法的符号引用 → Class 文件中的 CONSTANT_Methodref
→ JVM 在 resolve 阶段就能确定要调用哪个方法

问题：Java 8+ Lambda、Groovy、Scala、Kotlin 等动态语言，
  编译时并不知道要调用哪个方法（Lambda 函数式接口的实现是动态的）
  → 需要一个"延迟绑定"的调用指令 → invokedynamic
```

### 1.2 invokedynamic 字节码结构

```
invokedynamic #0, 0      // 调用常量池索引 #0 的 BootstrapMethod

CONSTANT_InvokeDynamic:
  bootstrap_method_attr_index = 123   // 指向 BootstrapMethods 属性
  name_and_type_index = 456            // 指向调用名和类型描述符

BootstrapMethods 属性（Class 文件的 BootstrapMethods 属性）：
  bootstrap_method_ref = MethodRef("java/lang/invoke/LambdaMetafactory.metafactory")
  bootstrap_arguments = [
    MethodHandle(implMethod: "LambdaTest.lambda$foo:()V"),
    String("()V"),           // 目标函数式接口的方法类型
    MethodHandle(target: "LambdaTest.foo:()V"),
    // ... 其他参数
  ]
```

### 1.3 执行流程

```
invokedynamic 执行时：
  ① 第一次遇到 → 调用 BootstrapMethod（LambdaMetafactory.metafactory）
  ② BootstrapMethod 返回一个 CallSite 对象
  ③ CallSite.target 是 MethodHandle（指向最终要调用的方法）
  ④ JVM 把 CallSite 缓存起来 → 后续直接走 target.invoke()
  ⑤ 如果 CallSite 是 MutableCallSite → target 可以变（动态改变调用目标）

→ 核心思想：**第一次调用时由 BootstrapMethod 决定调用目标**
→ 后续调用直接走缓存，性能几乎和 invokevirtual 一样快
```

---

## 二、Lambda 表达式编译原理

### 2.1 Lambda 编译后是什么

```java
// 用户写的：
Runnable r = () -> System.out.println("hello");

// javac 编译后生成：
public class LambdaTest {
    // ① 主类里生成一个私有静态方法（lambda$前缀）
    private static void lambda$foo$0() {
        System.out.println("hello");
    }

    public static void main(String[] args) {
        // ② invokedynamic 调用 LambdaMetafactory.metafactory
        // 第一次调用返回 CallSite → target = MethodHandle(lambda$foo$0)
        // 后续直接 invoke lambda$foo$0
        Runnable r = invokedynamic #0;
        r.run();
    }
}

// ★ javac 不会生成匿名内部类！
// Lambda 是 invokedynamic 实现的，不是匿名内部类！
// （匿名内部类会生成额外的 .class 文件；Lambda 不会生成额外 class）
```

### 2.2 匿名内部类 vs Lambda

```java
// 匿名内部类：
Runnable r1 = new Runnable() {
    public void run() { System.out.println("hello"); }
};
// 编译后：生成 LambdaTest$1.class → 每个匿名内部类都有独立的字节码文件

// Lambda：
Runnable r2 = () -> System.out.println("hello");
// 编译后：不生成额外 .class 文件
// ① 在主类生成 lambda$run$0() 私有静态方法
// ② invokedynamic 调用 BootstrapMethod 绑定
// ③ 运行时生成 Lambda 对象（通过 ASM 动态生成）
```

### 2.3 BootstrapMethod · LambdaMetafactory

**源码锚点**：`java.lang.invoke.LambdaMetafactory`

```java
// BootstrapMethod 是 JVM 第一次执行 invokedynamic 时调的
// 默认是 LambdaMetafactory.metafactory（但可以自定义）
public static CallSite metafactory(
    MethodHandles.Lookup caller,
    String invokedName,
    MethodType invokedType,
    MethodType samMethodType,      // 目标函数式接口的方法类型（擦除后）
    MethodHandle implMethod,       // Lambda 对应的 MethodHandle
    MethodType instantiatedMethodType
) throws LambdaConversionException {
    // ★ 用 ASM 动态生成一个实现了函数式接口的类
    // 这个类不是 .class 文件，是运行时字节码
    // 然后返回 MutableCallSite（初始 target = implMethod）
    ...
}

// 动态生成的类大致如下：
final class $$Lambda$LambdaTest$001 implements Runnable {
    public void run() {
        LambdaTest.lambda$foo$0();   // ★ 调用主类的 lambda$ 方法
    }
    // 实例化 CallSite：
    // MutableCallSite site = new MutableCallSite(implMethod);
    // return site;
}
```

### 2.4 Lambda 捕获 vs 非捕获

```java
// 非捕获 Lambda：不引用外部变量 → 静态方法 → 无状态
Runnable r = () -> System.out.println("hello");
// → invokedynamic 直接绑到 lambda$run$0()

// 捕获 Lambda：引用外部变量 → 实例方法 → 有状态
String msg = "hello";
Runnable r = () -> System.out.println(msg);
// → msg 被 final 捕获（或 effectively final）
// → 生成的 lambda$run$1(String msg) 私有静态方法带参数
// → invokedynamic 绑到带参数的 MethodHandle
// → 动态生成的类有 final String msg 字段保存捕获值
```

---

## 三、MethodHandle · 类型安全的方法句柄

### 3.1 MethodHandle vs Reflection

| 维度 | **MethodHandle** | **反射 Method** |
|------|-----------------|----------------|
| 类型安全 | ✅ 编译期类型检查 | ❌ 运行时才发现参数错误 |
| 性能 | ⭐⭐⭐⭐⭐（JIT 友好）| ⭐⭐（慢很多，invoke 要装箱）|
| 访问控制 | ✅ Lookup 检查 | ✅ setAccessible 可绕过 |
| 适用 | 动态语言 / Lambda / 动态代理 | 框架内部（Spring/MyBatis）|

```java
// MethodHandle 使用示例：
MethodHandles.Lookup lookup = MethodHandles.lookup();

// ① 反射式获取
MethodHandle mh = lookup.findVirtual(
    String.class, "length", MethodType.methodType(int.class));

// ② 调用（三种方式等价）
mh.invoke("hello");                    // 动态参数类型
mh.invokeExact("hello");               // 精确参数类型（最快）
mh.bindTo("hello").invoke();            // 提前绑定 receiver

// ③ vs 反射
Method m = String.class.getMethod("length");
int len = (int) m.invoke("hello");      // 要强转 + 装箱
```

### 3.2 MethodType · 方法类型

```java
// MethodType 是 (参数类型列表) → 返回类型 的不可变对象
MethodType mt1 = MethodType.methodType(void.class);              // ()V
MethodType mt2 = MethodType.methodType(String.class, int.class); // (I)Ljava/lang/String;
MethodType mt3 = MethodType.methodType(void.class, int.class, String.class); // (ILjava/lang/String;)V

// invokedynamic 的 name_and_type 就是 MethodType
// BootstrapMethod 用这个类型做参数匹配
```

### 3.3 CallSite · 调用点

```java
// CallSite 是 invokedynamic 的运行时绑定点
// 三种类型：
ConstantCallSite    → target 永远不变（只读）
MutableCallSite     → target 可以随时变（JVM 会失效内联）
VolatileCallSite    → target volatile 可见（多线程可见性）

// LambdaMetafactory 返回的是 MutableCallSite
// 为什么用 MutableCallSite？
//   → 第一次返回后，JVM 可以用 ASM 重新生成更高效的 target（方法内联）
//   → MutableCallSite 的 setTarget 可以更新
//   → 更新后 JVM 需要重新内联 → 不会导致旧代码执行错误
```

---

## 四、String 拼接 · invokedynamic 优化

### 4.1 Java 9 前：StringBuilder 反编译

```java
// 代码：
String s = "a" + b + c;

// Java 8 及以前 javac 编译后：
String s = new StringBuilder().append("a").append(b).append(c).toString();
// 每个 + 都生成 StringBuilder + append 调用
// 循环中 String 拼接 → 大量 StringBuilder 创建 → GC 压力
```

### 4.2 Java 9+：invokedynamic + StringConcatFactory

```java
// Java 9+ 用 invokedynamic 优化 String 拼接：
// javac 编译后：
String s = invokedynamic makeConcatWithConstants(
    makeConcatWithConstants, 
    "a\u0001\u0002",   // ★ 模板字符串 + 分隔符
    b,                  // 动态参数
    c);

// BootstrapMethod 是 StringConcatFactory.makeConcatWithConstants：
// → 根据模板字符串动态生成最优拼接策略
//   只有常量 → 编译期就拼接好
//   有动态参数 → 创建最优的拼接实现（避免中间 StringBuilder）
// → 首次调用生成拼接策略，后续直接走缓存

// 模板字符串 "a\u0001\u0002" 含义：
//   \u0001 = 参数占位符（替换为 b 的值）
//   \u0002 = 参数占位符（替换为 c 的值）
//   最终拼接："a" + b + c
```

---

## 五、MethodHandle 完整变换 API

### 5.1 变换方法总览

MethodHandle 的强大之处在于 **10+ 种变换方法**——可以组合、套娃、链式生成新的 MethodHandle，而不需要反射：

```java
// MethodHandle 变换分为 5 类：
// ① 参数调整：bindTo / insertArguments / dropArguments / spread / collect
// ② 类型适配：asType / asVarargsCollector / asFixedArity
// ③ 逻辑控制：guardWithTest / catchException
// ④ 前后处理：beforeInvocation / afterInvocation / aroundInvoke
// ⑤ 组合链：compose / andThen（在 MethodHandles 中）
```

### 5.2 bindTo · 提前绑定 receiver

**源码锚点**：MethodHandle.java L1275-1278

```java
// 把实例方法的 receiver 提前绑定 → 变成静态方法
MethodHandle mh = lookup.findVirtual(String.class, "length", MethodType.methodType(int.class));
// mh.type() = (String)int

MethodHandle bound = mh.bindTo("hello");
// bound.type() = ()int  ← receiver 被吃掉了！
bound.invoke();   // → 5（"hello".length()）

// 应用场景：Lambda 捕获外部变量
// String msg = "hello";
// Runnable r = () -> System.out.println(msg);
// 编译后：LambdaMetafactory 用 bindTo 把 msg 绑定到 lambda$ 方法
```

### 5.3 asType · 自动类型适配

**源码锚点**：L750

```java
// 把一个 MethodHandle 的参数类型/返回类型自动适配成另一个 MethodType
// 底层做了隐式转换（装箱/拆箱/基本类型提升）

MethodHandle mh = lookup.findStatic(Math.class, "abs", MethodType.methodType(int.class, int.class));
// mh.type() = (int)int

// 适配成接收 long → 返回 double
MethodHandle adapted = mh.asType(MethodType.methodType(double.class, long.class));
// adapted 自动做 long→int 截断 + int→double 提升
adapted.invokeExact(123456789L);  // → 123456789.0

// 核心规则：
//   - 参数类型：可以自动向上转型（int→long）或装箱拆箱
//   - 返回类型：可以自动向上转型或装箱拆箱
//   - 不安全的转换（long→int 可能溢出）→ ClassCastException
```

### 5.4 asSpreader / asCollector · 数组 ↔ 可变参数

```java
// asSpreader：把数组参数展开成 N 个单独参数
MethodHandle mh = ...;  // type = (int, int, int)int
MethodHandle spreader = mh.asSpreader(int[].class, 3);
// spreader.type() = (int[])int → 数组展开成 3 个 int 参数
spreader.invoke(new int[]{1, 2, 3});  // = mh.invoke(1, 2, 3)

// asCollector：把 N 个参数收集成一个数组参数
MethodHandle mh2 = ...;  // type = (int[])int
MethodHandle collector = mh2.asCollector(int[].class, 3);
// collector.type() = (int, int, int)int → 3 个 int 收集成数组
collector.invoke(1, 2, 3);  // = mh2.invoke(new int[]{1, 2, 3})
```

### 5.5 asVarargsCollector / asFixedArity · 可变参数支持

```java
// asVarargsCollector：让一个固定参数的 MH 支持可变参数调用
MethodHandle mh = ...;  // type = (int, int)int
MethodHandle varargs = mh.asVarargsCollector(int[].class);
varargs.invokeWithArguments(1, 2, 3, 4);  // 最后两个参数打包成数组 → mh.invoke(1, new int[]{2,3,4})

// asFixedArity：反过来，去掉可变参数支持
MethodHandle fixed = varargs.asFixedArity();
fixed.invokeWithArguments(1, 2);  // 必须精确 2 个参数
```

### 5.6 dropArguments / insertArguments · 参数增删

```java
// dropArguments：删除指定位置的参数（调用时忽略传入的值）
MethodHandle mh = lookup.findStatic(String.class, "valueOf", MethodType.methodType(String.class, int.class));
// mh.type() = (int)String

// 在 index=0 位置插入一个 String 参数（调用时 String 参数被忽略）
MethodHandle dropped = mh.dropArguments(MethodType.methodType(String.class), 0);
// dropped.type() = (String, int)String → 多了一个没用的 String 参数
dropped.invoke("ignore me", 42);  // → "42"

// insertArguments：在指定位置插入固定值（调用时不需要传这个参数）
MethodHandle inserted = mh.insertArguments(0, 42);
// inserted.type() = ()String → 42 被固定了
inserted.invoke();  // → "42"
```

### 5.7 guardWithTest · 条件分支

```java
// guardWithTest：类似 if-else
// 先调用 test MH → boolean → true 走 target → false 走 fallback
MethodHandle test = lookup.findStatic(...);   // ()boolean
MethodHandle target = ...;                     // ()int
MethodHandle fallback = ...;                   // ()int

MethodHandle guarded = test.guardWithTest(target, fallback);
// guarded.type() = ()int
guarded.invoke();  // test() ? target() : fallback()

// SwitchPoint.guardWithTest 也有类似方法，但 SwitchPoint 是带失效机制的
```

### 5.8 catchException · try-catch

```java
// catchException：类似 try-catch
// target 抛指定异常 → 调 fallback 处理
MethodHandle target = ...;     // (String)int
MethodHandle fallback = ...;   // (String, Exception)int

MethodHandle safe = target.catchException(String.class, Exception.class, fallback);
// safe.type() = (String)int
safe.invoke("hello");
// → try { target.invoke("hello"); } catch (Exception e) { fallback.invoke("hello", e); }
```

### 5.9 LambdaMetafactory.metafactory · BootstrapMethod 源码

**源码锚点**：LambdaMetafactory.java L291-305

```java
// JVM 第一次执行 invokedynamic 时调这个方法
public static CallSite metafactory(
    MethodHandles.Lookup caller,    // 调用者的 Lookup（权限检查）
    String invokedName,             // 函数式接口的方法名（如 "run"）
    MethodType invokedType,         // CallSite 的方法类型
    MethodType samMethodType,       // 目标 SAM 方法类型（擦除后）
    MethodHandle implMethod,        // Lambda 对应的实现方法 MH
    MethodType instantiatedMethodType  // 实例化后的方法类型（泛型具体化）
) throws LambdaConversionException {
    // 核心：创建 InnerClassLambdaMetafactory（或 JDK 内部的 LambdaMetafactory）
    // → 用 ASM 动态生成实现了函数式接口的类（不是 .class 文件！）
    // → 返回 MutableCallSite
    AbstractValidatingLambdaMetafactory mf = new InnerClassLambdaMetafactory(...);
    mf.validateMetafactoryArgs();
    return mf.buildCallSite();
}

// InnerClassLambdaMetafactory.buildCallSite() 内部：
// ① generateLambdaClass()：用 ASM 生成字节码 → 动态类
// ② createConstructorHandle()：获取构造函数 MH
// ③ 把捕获的参数 bindTo 构造函数 → 预实例化 Lambda 对象
// ④ 返回 MutableCallSite(target = boundConstructor)
```

---

## 六、invokedynamic 在框架中的应用

### 6.1 应用全景图

```
invokedynamic 三大应用场景：
  ① Lambda 表达式（JDK 8 语言特性）→ LambdaMetafactory
  ② 动态优化（JIT 友好）→ CallSite 缓存
  ③ 框架层面（Spring/Netty/Dubbo）→ 比反射快的动态调用

Spring / Netty / Dubbo / MyBatis 等框架都在利用 invokedynamic + MethodHandle：
  - Spring 5+ 用 MethodHandle 替代反射调用 Bean 方法
  - Netty FastThreadLocal / MpscArrayQueue 用 Unsafe + MethodHandle
  - Dubbo 的动态代理有 ASM / Javassist / JDK Proxy 三种 → invokedynamic 是第四种（实验性）
  - MyBatis 3.5+ 用 MethodHandle 替代反射调用 Mapper 方法
```

### 6.2 Spring 5 MethodHandle 替代反射

```java
// Spring 4.x：BeanUtils.invokeMethod() 用反射
public static Object invokeMethod(Method method, Object target, Object... args) {
    method.setAccessible(true);
    return method.invoke(target, args);  // 反射：慢！
}

// Spring 5.x：ReflectiveMethodInvocation 内部用 MethodHandle
// 第一次调用时：MethodHandles.Lookup.unreflect(method) → 缓存 MethodHandle
// 后续调用：methodHandle.invoke(target, args) → 比反射快 3-5 倍

// Spring 的 KeyEventDispatcher / ApplicationListener 也用类似优化
// 核心：反射只做第一次（获取 Method → 转 MethodHandle），后续全走 MH
```

### 6.3 MyBatis 3.5 MethodHandle Mapper 调用

```java
// MyBatis 的 Mapper 接口调用
// 旧版：MapperProxy 用反射调 Method
public Object invoke(Object proxy, Method method, Object[] args) {
    return method.invoke(executor, args);  // 反射
}

// MyBatis 3.5+：MapperProxy 内部用 MethodHandle 缓存
// 每个 Mapper 方法第一次调用时：
//   MethodHandle mh = MethodHandles.lookup().unreflect(method);
//   methodHandleCache.put(method, mh);
// 后续：mh.invoke(executor, args) → 更快

// 对比基准测试（JDK 17）：
//   反射：~2500 ns/call
//   MethodHandle invoke：~600 ns/call
//   MethodHandle invokeExact：~200 ns/call
//   invokevirtual（理想情况）：~100 ns/call
```

### 6.4 Dubbo 动态代理三种实现

```java
// Dubbo 的 ProxyFactory 有三种实现：
// ① JdkProxyFactory：java.lang.reflect.Proxy（传统反射）
// ② JavassistProxyFactory：Javassist 字节码生成（类似 ASM）
// ③ SpringProxyFactory：Spring AOP 代理

// 第四种（实验性）：invokedynamic + LambdaMetafactory
// → 不生成代理类！直接用 Lambda 绑定目标方法
// → 性能接近 invokevirtual
// → 但需要 Dubbo 内部做 invokedynamic 的 BootstrapMethod 实现

// 为什么 invokedynamic 代理更快？
//   JVM 对 invokedynamic 的 CallSite 做了内联优化
//   → 第一次 BootstrapMethod 返回 MethodHandle
//   → JIT 内联 → 后续调用直接跳到目标方法（几乎零开销）
//   → 反射有 Method.invoke 的装箱/拆箱/参数检查开销
```

### 6.5 Netty FastThreadLocal · Unsafe + MethodHandle

```java
// Netty 的 FastThreadLocal 比 JDK ThreadLocal 快 5-10 倍
// 核心：每个线程有自己的 FastThreadLocal 变量数组（通过 Unsafe 直接访问）
// 用 MethodHandles + VarHandle（Java 9）替代反射：

// Netty 内部：
// MethodHandles.Lookup.IMPL_LOOKUP.unreflectGetter/Setter → 直接操作字段
// 比 Field.get/set 反射快 3-5 倍
// 在 Netty 这种高频调用框架中，这个差异会被放大

// Netty 还利用 MethodHandle 做：
//   - ChannelHandler 的 invoke 调用（避免反射）
//   - EventExecutor 的 task 提交
```

### 6.6 CGLIB / Javassist vs invokedynamic

| 维度 | **CGLIB / Javassist** | **JDK Proxy（反射）** | **invokedynamic + Lambda** |
|------|----------------------|---------------------|---------------------------|
| 生成方式 | ASM 动态生成 .class | Proxy.newProxyInstance | BootstrapMethod 动态绑定 |
| 额外文件 | 有（内存字节码）| 无 | **无（不生成类）**|
| 性能 | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐（接近 invokevirtual）|
| 限制 | final 类不能代理 | 只能代理接口 | **无限制**（任意方法）|
| JDK 内置 | ❌ 第三方库 | ✅ | ✅ |
| 典型用途 | Spring AOP / Dubbo | JDK 动态代理 | Lambda / 框架内部优化 |

---

## 七、面试考点

| # | 考点 | 答案 |
|---|------|------|
| **MethodHandle API** | | |
| 1 | bindTo 做什么？| **提前绑定 receiver → 实例方法变静态方法**；Lambda 捕获变量就是用 bindTo |
| 2 | asType 自动类型适配？| 自动装箱/拆箱/基本类型提升；不安全转换（long→int）抛 ClassCastException |
| 3 | asSpreader / asCollector？| **asSpreader 数组→N 个参数**；**asCollector N 个参数→数组** |
| 4 | dropArguments / insertArguments？| drop 删除指定位置参数（忽略传入值）；insert 在指定位置插入固定值（调用时不用传）|
| 5 | guardWithTest / catchException？| guardWithTest = if-else 分支；catchException = try-catch 异常处理 |
| **LambdaMetafactory** | | |
| 6 | metafactory 做什么？| BootstrapMethod 第一次 invokedynamic 时调 → **用 ASM 动态生成函数式接口实现类 + 返回 MutableCallSite** |
| 7 | metafactory 参数 samMethodType vs instantiatedMethodType？| samMethodType = 擦除后的 SAM 方法类型；instantiatedMethodType = 泛型具体化后的类型（约束调用参数）|
| **框架应用** | | |
| 8 | Spring 5 怎么用 MethodHandle？| **ReflectiveMethodInvocation 内部首次 MethodHandles.Lookup.unreflect → 后续 invoke 替代反射**（快 3-5 倍）|
| 9 | MyBatis 3.5 Mapper 调用优化？| MapperProxy 缓存 MethodHandle → 首次 unreflect，后续 invoke 替代反射 |
| 10 | Dubbo 动态代理第四种？| **invokedynamic + LambdaMetafactory**（实验性）→ 不生成代理类，性能接近 invokevirtual |
| 11 | invokedynamic 代理 vs CGLIB？| invokedynamic **不生成类 + 无 final 限制 + JIT 内联优化**；CGLIB 要生成子类，不能代理 final 类 |
| 12 | Netty FastThreadLocal 优化？| Unsafe + MethodHandles.Lookup.IMPL_LOOKUP 替代反射 → 比 Field.get/set 快 3-5 倍 |
| **invokedynamic 基础** | | |
| 13 | invokedynamic 和 invokevirtual 区别？| invokevirtual 编译时确定 Methodref → vtable 多态分发；**invokedynamic 第一次调用才由 BootstrapMethod 决定目标** |
| 14 | invokedynamic 执行流程？| 第一次调 BootstrapMethod → 返回 CallSite → 缓存 target → 后续直接走 invoke |
| 15 | Lambda 和匿名内部类区别？| Lambda **不生成额外 .class 文件** → 主类生成 lambda$ 私有方法 + invokedynamic 绑定；匿名内部类生成独立 .class |
| 16 | Java 9 String 拼接优化？| **invokedynamic + StringConcatFactory** → 根据模板字符串动态生成最优拼接策略（JDK 1.8 没有这个工厂）|

---

**文档版本：v1.1 · 2026-10-20**  
**源码基线：JDK 1.8.0_451**  
**v1.1 更新：新增第五章 MethodHandle 完整变换 API（bindTo/asType/asSpreader/asCollector/dropArguments/insertArguments/guardWithTest/catchException）+ 第六章 invokedynamic 在框架中的应用（Spring 5/MyBatis 3.5/Dubbo/Netty）+ 5 个新面试考点（总 16 题）**
