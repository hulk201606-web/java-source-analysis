# 15 · Spring 事务管理核心源码深度解析

> **源码基线**：Spring Framework 5.3.35（Java 8 兼容版，面试最高频）  
> **源码位置**：`d:\GC_WorkSpaces\spring-framework-5.3.35\spring-tx\src\main\java\`  
> **面试权重**：⭐⭐⭐⭐⭐（Spring 两大核心之一）

---

## 一、Spring 事务管理总体架构

### 1.1 核心类关系

```
TransactionInterceptor（AOP MethodInterceptor）
  │
  └─ TransactionAspectSupport.invokeWithinTransaction()   ← 核心方法 L336
       │
       ├─ ① createTransactionIfNecessary() → 开启事务（如果需要）
       │    └─ PlatformTransactionManager.getTransaction() → AbstractPlatformTransactionManager.getTransaction()
       │         ├─ doGetTransaction() → 拿当前事务对象
       │         ├─ isExistingTransaction() → 有没有现有事务
       │         ├─ handleExistingTransaction() → 现有事务时按传播行为处理
       │         └─ startTransaction() → doBegin() → 开始新事务
       │
       ├─ ② proceedWithInvocation() → 执行 target 方法
       │
       ├─ ③ completeTransactionAfterThrowing() → 异常时 rollback
       │
       ├─ ④ commitTransactionAfterReturning() → 正常时 commit
       │    └─ PlatformTransactionManager.commit() → AbstractPlatformTransactionManager.commit()
       │         └─ doCommit() → JDBC Connection.commit()
       │
       └─ ⑤ cleanupTransactionInfo() → ThreadLocal 清理

ThreadLocal 机制：
  TransactionSynchronizationManager：
    ├─ resources（Map）：数据源 → Connection/Session 绑定
    ├─ currentTransactionName
    ├─ currentTransactionReadOnly
    ├─ actualTransactionActive
    └─ synchronizations（ThreadLocal<List>）
```

### 1.2 事务执行完整时序

```
代理对象.method()
  │
  ├─ TransactionInterceptor.invoke()  ← AOP 拦截器
  │    │
  │    └─ invokeWithinTransaction()   L336 ★★★ 事务核心
  │         │
  │         ├─ ① 查找 @Transactional 注解属性
  │         │    TransactionAttributeSource.getTransactionAttribute(method, targetClass)
  │         │    → 找到传播行为 / 隔离级别 / rollbackFor / timeout 等
  │         │
  │         ├─ ② createTransactionIfNecessary() → 开启事务
  │         │    │
  │         │    └─ PlatformTransactionManager.getTransaction(def)
  │         │         │
  │         │         ├─ doGetTransaction() → 获取 TransactionStatus
  │         │         │    → DataSourceTransactionManager：从 ThreadLocal 拿 ConnectionHolder
  │         │         │    → 如果没有 ConnectionHolder → 创建空的
  │         │         │
  │         │         ├─ isExistingTransaction(transaction)
  │         │         │    → 判断 ThreadLocal 里有没有已经开启的事务
  │         │         │    → 有 → handleExistingTransaction()
  │         │         │    → 没有 → 继续按传播行为处理
  │         │         │
  │         │         ├─ handleExistingTransaction(def, transaction)
  │         │         │    └─ 现有事务时，根据 PROPAGATION_* 决定怎么处理（7 种）
  │         │         │
  │         │         └─ startTransaction() → 真正开启事务
  │         │              → doBegin()
  │         │               → DataSourceTransactionManager.doBegin():
  │         │                  → Connection con = dataSource.getConnection();
  │         │                  → con.setAutoCommit(false);  // ★ 关闭自动提交 → 开始事务
  │         │                  → TransactionSynchronizationManager.bindResource(ds, conHolder);
  │         │
  │         ├─ ③ try { invocation.proceedWithInvocation(); } catch { ④⑤ }
  │         │    → 执行 target.method()（真正的业务代码）
  │         │
  │         ├─ ④ catch Throwable ex { completeTransactionAfterThrowing(txInfo, ex); throw ex; }
  │         │    │
  │         │    └─ rollback()  条件：
  │         │         ① ex instanceof RuntimeException || Error（默认）
  │         │         ② ex 在 rollbackFor 指定范围内
  │         │         ③ status.isGlobalRollbackOnly() 被标记为全局回滚
  │         │    └─ PlatformTransactionManager.rollback(status) → doRollback()
  │         │         → Connection.rollback()  // ★ 回滚数据库
  │         │
  │         ├─ ⑤ commitTransactionAfterReturning(txInfo)
  │         │    └─ commit() 条件：正常返回 或 ex 在 noRollbackFor 范围内
  │         │    └─ PlatformTransactionManager.commit(status) → doCommit()
  │         │         → Connection.commit()    // ★ 提交数据库
  │         │
  │         └─ ⑥ finally { cleanupTransactionInfo(txInfo); }  ← ThreadLocal 清理
```

---

## 二、TransactionInterceptor.invokeWithinTransaction 核心源码

**源码锚点**：TransactionAspectSupport.java L336-408（TransactionInterceptor 继承它）

```java
protected Object invokeWithinTransaction(Method method, @Nullable Class<?> targetClass,
        final InvocationCallback invocation) throws Throwable {

    // ① 查找 @Transactional 注解的属性
    TransactionAttributeSource tas = getTransactionAttributeSource();
    final TransactionAttribute txAttr = (tas != null ? tas.getTransactionAttribute(method, targetClass) : null);
    // → txAttr 包含：propagationBehavior / isolationLevel / timeout / rollbackFor / noRollbackFor

    PlatformTransactionManager ptm = asPlatformTransactionManager(tm);
    final String joinpointIdentification = methodIdentification(method, targetClass, txAttr);

    // ② 常规事务流程（绝大多数情况走这里）
    if (txAttr == null || !(ptm instanceof CallbackPreferringPlatformTransactionManager)) {
        
        // ★ ②-a 开启事务（如果需要）
        TransactionInfo txInfo = createTransactionIfNecessary(ptm, txAttr, joinpointIdentification);

        Object retVal;
        try {
            // ★ ②-b 执行业务方法
            retVal = invocation.proceedWithInvocation();
        }
        catch (Throwable ex) {
            // ★ ②-c 异常处理 → 决定是 rollback 还是 commit（看 rollbackFor）
            completeTransactionAfterThrowing(txInfo, ex);
            throw ex;
        }
        finally {
            // ★ ②-d 清理 ThreadLocal（TransactionInfo + resources）
            cleanupTransactionInfo(txInfo);
        }

        // ★ ②-e 正常返回 → commit
        commitTransactionAfterReturning(txInfo);
        return retVal;
    }
    ...
}
```

---

## 三、PlatformTransactionManager.getTransaction · 7 种传播行为

> **传播行为决定了"当前方法遇到已存在的事务时该怎么办"——面试必考。**

**源码锚点**：AbstractPlatformTransactionManager.java L341-476

### 3.1 getTransaction 主流程

```java
public final TransactionStatus getTransaction(@Nullable TransactionDefinition definition) {
    TransactionDefinition def = (definition != null ? definition : TransactionDefinition.withDefaults());

    // ① 拿当前事务对象
    Object transaction = doGetTransaction();
    
    // ② 判断当前有没有已存在的事务
    if (isExistingTransaction(transaction)) {
        // ★ 有！→ handleExistingTransaction() 按传播行为处理
        return handleExistingTransaction(def, transaction, debugEnabled);
    }
    
    // ③ 没有已存在事务 → 继续按传播行为处理
    if (def.getPropagationBehavior() == PROPAGATION_MANDATORY) {
        // MANDATORY：必须有现有事务，没有 → 报错！
        throw new IllegalTransactionStateException("No existing transaction found...");
    }
    else if (PROPAGATION_REQUIRED || PROPAGATION_REQUIRES_NEW || PROPAGATION_NESTED) {
        // REQUIRED / REQUIRES_NEW / NESTED → 都需要开启新事务
        suspend(null);  // 挂起（如果有）
        return startTransaction(def, transaction, ...);  // doBegin() 真正开启
    }
    else {
        // SUPPORTS / NOT_SUPPORTED / NEVER → 空事务（不开启，只是同步）
        return prepareTransactionStatus(def, null, true, ...);
    }
}
```

### 3.2 handleExistingTransaction · 现有事务时的处理（7 种传播行为）

**源码锚点**：AbstractPlatformTransactionManager.java L408-476

```java
private TransactionStatus handleExistingTransaction(
        TransactionDefinition definition, Object transaction, boolean debugEnabled) {

    // ┌─────────────────────────────────────────────────────────────┐
    // │ PROPAGATION_NEVER = 4                                        │
    // │ 已有事务？→ 直接报错！不能在已有事务里执行 NEVER 方法          │
    // └─────────────────────────────────────────────────────────────┘
    if (definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_NEVER) {
        throw new IllegalTransactionStateException(
                "Existing transaction found for transaction marked with propagation 'never'");
    }

    // ┌─────────────────────────────────────────────────────────────┐
    // │ PROPAGATION_NOT_SUPPORTED = 3                                │
    // │ 已有事务？→ ★ 挂起现有事务 → 方法在无事务状态执行             │
    // │ 执行完 → 恢复原有事务                                        │
    // └─────────────────────────────────────────────────────────────┘
    if (definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_NOT_SUPPORTED) {
        Object suspendedResources = suspend(transaction);  // ★ 挂起
        return prepareTransactionStatus(definition, null, false, ..., suspendedResources);
        // 注意：返回的 TransactionStatus 没有事务对象！isNewTransaction=false
    }

    // ┌─────────────────────────────────────────────────────────────┐
    // │ PROPAGATION_REQUIRES_NEW = 2                                 │
    // │ 已有事务？→ ★ 挂起现有事务 → 开启全新独立的新事务             │
    // │ 新事务 commit/rollback 不影响原有事务                        │
    // │ 方法结束 → 恢复原有事务                                      │
    // └─────────────────────────────────────────────────────────────┘
    if (definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRES_NEW) {
        SuspendedResourcesHolder suspendedResources = suspend(transaction);  // ★ 挂起旧事务
        try {
            return startTransaction(definition, transaction, ..., suspendedResources);  // ★ 开启新事务
        } catch (...) {
            resumeAfterBeginException(transaction, suspendedResources, beginEx);
            throw beginEx;
        }
    }

    // ┌─────────────────────────────────────────────────────────────┐
    // │ PROPAGATION_NESTED = 5                                       │
    // │ 已有事务？→ ★ 创建保存点（savepoint）作为嵌套事务             │
    // │ 内层 rollback → 只回滚到 savepoint                            │
    // │ 外层 rollback → 整个事务（包括嵌套）都回滚                    │
    // └─────────────────────────────────────────────────────────────┘
    if (definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_NESTED) {
        if (!isNestedTransactionAllowed()) { throw new NestedTransactionNotSupportedException(); }
        
        if (useSavepointForNestedTransaction()) {
            DefaultTransactionStatus status = prepareTransactionStatus(definition, transaction, false, ...);
            status.createAndHoldSavepoint();   // ★ 创建 JDBC savepoint
            return status;
        } else {
            return startTransaction(definition, transaction, ..., null);
        }
    }

    // ┌─────────────────────────────────────────────────────────────┐
    // │ PROPAGATION_REQUIRED = 0（默认）                             │
    // │ 已有事务？→ ★ 加入现有事务（不挂起也不开新的）                │
    // │ 所有加入的方法共享同一个事务 → 一个全 commit，一个全 rollback  │
    // └─────────────────────────────────────────────────────────────┘
    // PROPAGATION_SUPPORTS = 1：有就加入，没有就不用事务（和 REQUIRED 处理方式一样）
    // PROPAGATION_MANDATORY = 6：有就加入，没有就报错（这里是"有"的情况，继续）
    
    // ★ 默认行为：加入现有事务
    return prepareTransactionStatus(definition, transaction, false, ...);
}
```

### 3.3 7 种传播行为速查表

| 传播行为 | 常量值 | 有现有事务时 | 无现有事务时 | 典型场景 |
|---------|--------|------------|------------|---------|
| **REQUIRED**（默认）| 0 | ✅ **加入现有事务** | ✅ **开启新事务** | 绝大多数业务方法 |
| SUPPORTS | 1 | ✅ **加入现有事务** | ⚠️ **不开启事务** | 查询方法（有事务就用，没有也无所谓）|
| MANDATORY | 6 | ✅ **加入现有事务** | ❌ **报错！必须有事务** | 必须在事务里执行的方法（如数据库迁移）|
| **REQUIRES_NEW** | 2 | 🔄 **挂起旧事务 → 开启新事务** | ✅ **开启新事务** | 日志记录（主事务回滚不影响日志）|
| NOT_SUPPORTED | 3 | 🔄 **挂起旧事务 → 无事务执行** | ⚠️ **无事务执行** | 非数据库操作（如发送消息）|
| NEVER | 4 | ❌ **报错！不能在事务里** | ⚠️ **无事务执行** | 必须不在事务里的方法 |
| **NESTED** | 5 | 📍 **创建 savepoint** | ✅ **开启新事务** | 子流程可独立回滚（淘宝订单拆单）|

### 3.4 传播行为图解

```
外层方法（有事务 T1）调用内层方法：
│
├─ 内层 REQUIRED → ✅ 加入 T1（外层回滚 → 内层也回滚）
├─ 内层 REQUIRES_NEW → 🔄 挂起 T1 → 开 T2 → 执行 → commit/rollback T2 → 恢复 T1
│                      T2 独立！T1 回滚不影响 T2
├─ 内层 NESTED → 📍 T1 里创建 savepoint SP
│                 内层回滚 → 只回滚到 SP
│                 外层回滚 → T1 整体回滚（包括内层）
├─ 内层 SUPPORTS → ✅ 加入 T1（有则加）
├─ 内层 NOT_SUPPORTED → 🔄 挂起 T1 → 无事务执行 → 恢复 T1
├─ 内层 MANDATORY → ✅ 加入 T1（必须有）
└─ 内层 NEVER → ❌ 报错（不能在事务里）

没有外层事务时：
├─ REQUIRED → ✅ 开新事务
├─ SUPPORTS → ⚠️ 不开启（非事务执行）
├─ MANDATORY → ❌ 报错（必须有）
├─ REQUIRES_NEW → ✅ 开新事务
├─ NOT_SUPPORTED → ⚠️ 不开启
├─ NEVER → ⚠️ 不开启
└─ NESTED → ✅ 开新事务（savepoint 没用，savepoint 需要现有事务）
```

---

## 四、TransactionSynchronizationManager · ThreadLocal 事务上下文

### 4.1 核心字段

```java
// TransactionSynchronizationManager（Spring 事务 ThreadLocal 管理核心）

// ★ resources：DataSource → ConnectionHolder 绑定
//   同一个线程多次 getTransaction → 从 ThreadLocal 里拿同一个 Connection
//   → 保证同一个事务里所有 SQL 用的是同一个 Connection！
private static final ThreadLocal<Map<Object, Object>> resources =
        new NamedThreadLocal<>("Transactional resources");

// ★ synchronizations：事务同步回调列表
//   比如 AOP / Spring Batch / 自定义同步器
private static final ThreadLocal<List<TransactionSynchronization>> synchronizations =
        new NamedThreadLocal<>("Transaction synchronizations");

private static final ThreadLocal<String> currentTransactionName = ...;
private static final ThreadLocal<Boolean> currentTransactionReadOnly = ...;
private static final ThreadLocal<Boolean> actualTransactionActive = ...;
```

### 4.2 DataSourceTransactionManager.doBegin · JDBC 事务开启

```java
// DataSourceTransactionManager.doBegin() 简化版：
protected void doBegin(Object transaction, TransactionDefinition definition) {
    
    // ① 从 DataSource 获取 Connection
    Connection con = dataSource.getConnection();
    
    // ② ★ 关闭自动提交 → 这就是事务开始！
    //    没有 setAutoCommit(false) → 所有 SQL 自动提交 → 没有事务
    con.setAutoCommit(false);
    
    // ③ 隔离级别（如果指定了）
    if (definition.getIsolationLevel() != ISOLATION_DEFAULT) {
        con.setTransactionIsolation(definition.getIsolationLevel());
    }
    
    // ④ 是否只读
    if (definition.isReadOnly()) {
        try { con.setReadOnly(true); } catch (...) {}
    }
    
    // ⑤ ★ 绑定 Connection 到 ThreadLocal
    //    TransactionSynchronizationManager.bindResource(dataSource, new ConnectionHolder(con));
    //    之后同一个线程再调 dataSource.getConnection() → 直接从 ThreadLocal 拿
    //    → 保证整个事务用的是同一个 Connection！
}

// DataSource.getConnection() 在事务内做了什么？
//   → TransactionSynchronizationManager.getResource(dataSource)
//   → 如果 ThreadLocal 里有 ConnectionHolder → 直接返回 Connection（不新建！）
//   → 这就是 Spring 事务"一个事务一个 Connection"的核心！
```

---

## 五、commit / rollback 流程

### 5.1 commit 流程

```java
// AbstractPlatformTransactionManager.commit(status)
public final void commit(TransactionStatus status) {
    if (status.isRollbackOnly()) {
        // ★ 如果被标记为 rollback-only → 走 rollback！
        //    场景：REQUIRED 加入的内层方法抛异常 → 外层被标记 rollback-only
        //    → 外层即使正常返回也要 rollback
        rollback(status);
        return;
    }
    
    // ★ 1. 触发 beforeCommit 同步回调
    triggerBeforeCommit(status);
    
    // ★ 2. 如果是新事务 → 真正 commit
    if (status.isNewTransaction()) {
        if (!status.hasSavepoint()) {
            // 没有 savepoint → 常规 commit
            doCommit(status);   // DataSourceTransactionManager.doCommit → Connection.commit()
        } else {
            // 有 savepoint → NESTED 内层 → 释放 savepoint（不真正 commit，外层 commit）
            status.releaseHeldSavepoint();
        }
    }
    
    // ★ 3. 触发 afterCommit / afterCompletion 同步回调
    triggerAfterCommit(status);
    triggerAfterCompletion(status, COMMIT);
    
    // ★ 4. cleanupTransactionInfo → 清理 ThreadLocal
    cleanupAfterCompletion(status);
}
```

### 5.2 rollback 流程

```java
// AbstractPlatformTransactionManager.rollback(status)
public final void rollback(TransactionStatus status) {
    
    // ★ 1. 如果是 NESTED + 有 savepoint → 只回滚到 savepoint
    if (status.hasSavepoint()) {
        status.rollbackToSavepoint();   // ★ 只回滚嵌套部分
        return;
    }
    
    // ★ 2. 如果不是新事务 → 标记 rollback-only（不能真正 rollback，因为是加入的）
    if (!status.isNewTransaction()) {
        // REQUIRED 加入的内层 → 不能真正 rollback
        // 只能标记外层为 rollback-only → 让外层 commit 时走 rollback
        status.setRollbackOnly();  // ★ 标记！外层 commit 时检查 isRollbackOnly → rollback
        return;
    }
    
    // ★ 3. 新事务 + 没有 savepoint → 真正 rollback
    doRollback(status);   // DataSourceTransactionManager.doRollback → Connection.rollback()
    
    // ★ 4. 触发 afterCompletion 回调 + 清理 ThreadLocal
    triggerAfterCompletion(status, ROLLBACK);
    cleanupAfterCompletion(status);
}
```

### 5.3 REQUIRED 加入事务的 rollback 传播（面试高频）

```java
// REQUIRED 传播行为下的 rollback 传播机制：
//
// 外层 Outer.method()（REQUIRED）
//   └─ 内层 Inner.method()（REQUIRED）→ 加入外层事务
//
// 场景 1：内层抛 RuntimeException
//   → inner.rollback() → 不是新事务 → 只能 setRollbackOnly()
//   → outer.commit() → 检查 status.isRollbackOnly() == true → rollback！
//   → 整个事务回滚 ✅（符合预期）
//
// 场景 2：内层 catch 了异常
//   → inner.commit() → 不是新事务 → 不做 commit（外层控制）
//   → outer.commit() → 正常 commit ✅
//   → 内层异常被吃掉了 → 外层不知道 → 数据不一致！❌
//   → 这是 REQUIRED 的坑：异常必须往外抛！
//
// 场景 3：内层抛 checked 异常（没有配置 rollbackFor）
//   → Spring 默认只对 RuntimeException/Error rollback
//   → 内层抛 checked 异常 → 不在 rollbackFor 里 → completeTransactionAfterThrowing 标记为 commit
//   → inner.commit() → 不是新事务 → 不做 commit
//   → outer.commit() → 正常 commit ❌
//   → 数据库脏数据！→ 必须配置 rollbackFor = Exception.class
```

---

## 六、@Transactional 失效场景（深度分析）

### 6.1 失效 5 场景 + 第 6 场景（新发现）

```java
// ① 方法非 public
@Transactional
void doSomething() { ... }
// ❌ 原因：Spring AOP 代理是基于方法拦截的 → private/protected/package-private 方法无法被代理
//          JDK Proxy 只能代理接口方法（接口方法都是 public）
//          CGLIB 代理类的方法 → private 方法无法覆盖
// 修复：改成 public

// ② 同类内部调用（绕过代理）
// 详细见 14_AOP 文档 → 3 种解决方案

// ③ catch 异常没抛出 + 没有手动标记 rollback
@Transactional
public void doSomething() {
    try {
        mapper.insert(...);
    } catch (Exception e) {
        log.error(e);
        // ❌ 没有 throw → 事务不会回滚！
        // TransactionInterceptor 只在 throw 时才 rollback
        // 或者手动：TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
    }
}

// ④ 没有 rollbackFor 配置（checked 异常）
@Transactional  // 默认 rollbackFor = RuntimeException.class
public void doSomething() throws Exception {
    throw new Exception();  // ❌ checked 异常 → 不回滚！
}
// 修复：@Transactional(rollbackFor = Exception.class)

// ⑤ 数据库引擎不支持事务
// MyISAM → 不支持事务 → @Transactional 配置了也没用
// 必须用 InnoDB

// ★ ⑥ Async / 同类调用 / 锁
@Transactional
@Async  // ❌ 新线程执行 → 脱离 Spring 事务上下文！
public void doSomething() { ... }
// 或：
@Transactional  // ❌ synchronized 锁在 AOP 之外 → 可能死锁
public synchronized void doSomething() { ... }
```

---

## 七、隔离级别

```java
// Spring 支持 5 种标准隔离级别 + 1 种默认：
//
// ISOLATION_DEFAULT = -1（默认）→ 用数据库默认隔离级别
// ISOLATION_READ_UNCOMMITTED = 1 → 读未提交（脏读 + 不可重复读 + 幻读）
// ISOLATION_READ_COMMITTED = 2   → 读已提交（Oracle 默认，MySQL 不是）
// ISOLATION_REPEATABLE_READ = 4  → 可重复读（MySQL InnoDB 默认）
// ISOLATION_SERIALIZABLE = 8    → 串行化（最高，性能最差）
//
// MySQL InnoDB 默认 REPEATABLE_READ（防幻读用 MVVC + 间隙锁）
// Oracle 默认 READ_COMMITTED
//
// Spring 隔离级别设置位置：
//   @Transactional(isolation = Isolation.READ_COMMITTED)
//   → AbstractPlatformTransactionManager.getTransaction() → doBegin() → con.setTransactionIsolation(level)
//
// ⚠️ 陷阱：不是所有数据库都支持所有隔离级别
//   MySQL 不支持 READ_UNCOMMITTED（虽然 JDBC 常量有）
//   → setTransactionIsolation 可能警告但不生效
```

---

## 八、面试考点（事务 15 题）

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **事务原理** | | |
| 1 | Spring 事务核心机制？| **AOP 代理 + ThreadLocal + DataSource.getConnection + setAutoCommit(false)** |
| 2 | TransactionInterceptor 核心方法？| invokeWithinTransaction() TransactionAspectSupport.java L336 |
| 3 | 事务开启入口？| PlatformTransactionManager.getTransaction(def) → AbstractPlatformTransactionManager.java L341 |
| 4 | DataSourceTransactionManager.doBegin 核心？| ① dataSource.getConnection() ② con.setAutoCommit(false) ③ **TransactionSynchronizationManager.bindResource(ds, conHolder)** 绑定到 ThreadLocal |
| 5 | 为什么一个事务用同一个 Connection？| **TransactionSynchronizationManager.resources（ThreadLocal）** 存 DataSource → ConnectionHolder 映射 → 同一个线程 getConnection() 直接从 ThreadLocal 拿 |
| **7 种传播行为** | | |
| 6 | REQUIRED 含义？| **默认！有事务就加入，没有就开启新的** → AbstractPlatformTransactionManager L365 |
| 7 | REQUIRES_NEW 含义？| **挂起旧事务 → 开启新独立事务** → L427 |
| 8 | NESTED 含义？| **创建 savepoint（JDBC 保存点）** → 内层回滚只回到 savepoint → L451 status.createAndHoldSavepoint() |
| 9 | REQUIRED 加入内层 rollback 怎么传播？| **内层 setRollbackOnly()（标记外层）→ 外层 commit 时检查 isRollbackOnly() → rollback** → 内层不能真正 rollback，因为事务是外层控制的 |
| 10 | MANDATORY / NEVER / NOT_SUPPORTED 区别？| MANDATORY：必须有现有事务否则报错；NEVER：不能有事务否则报错；NOT_SUPPORTED：有事务就挂起 → 无事务执行 |
| **commit / rollback** | | |
| 11 | commit 核心流程？| triggerBeforeCommit → isNewTransaction? doCommit() : releaseSavepoint → triggerAfterCommit → cleanupAfterCompletion |
| 12 | rollback 核心流程？| hasSavepoint? rollbackToSavepoint() : isNewTransaction? doRollback() : **setRollbackOnly()** |
| 13 | setRollbackOnly() 什么时候用？| **REQUIRED 加入的内层异常** → 不能真正 rollback（事务是外层的）→ 只能标记外层为 rollback-only → 外层 commit 时回滚 |
| **失效场景** | | |
| 14 | @Transactional 失效 5 场景？| ① 方法非 public ② 同类内部调用绕过代理 ③ catch 异常没抛出 ④ 没有 rollbackFor（checked 异常不回滚）⑤ 数据库引擎不支持 |
| 15 | checked 异常怎么让事务回滚？| **@Transactional(rollbackFor = Exception.class)** → 默认只对 RuntimeException/Error rollback |

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线**：Spring Framework 5.3.35（Java 8 兼容版）  
**覆盖**：Spring 事务架构 + TransactionInterceptor.invokeWithinTransaction() 核心 + AbstractPlatformTransactionManager.getTransaction() 7 种传播行为源码级 + REQUIRED 加入事务的 rollback 传播机制 + TransactionSynchronizationManager ThreadLocal 绑定 + DataSourceTransactionManager.doBegin → setAutoCommit(false) + commit/rollback 完整流程 + @Transactional 失效 6 场景 + 隔离级别 + 15 个面试考点  
**源码锚点行号全部来自 Spring Framework 5.3.35 源码副本**
