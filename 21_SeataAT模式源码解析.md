# Seata 2.0.0 AT 模式源码深度解析（分布式事务）

> **源码基线**：Seata 2.0.0（d:\GC_WorkSpaces\incubator-seata-2.0.0\ 真实源码）  
> **面试权重**：⭐⭐⭐⭐⭐（分布式事务必问）  
> **核心模块**：TM（Transaction Manager）/ RM（Resource Manager）/ TC（Transaction Coordinator）

---

## 一、Seata 整体架构

### 1.1 三大角色

```
┌──────────────┐          ┌──────────────────────┐          ┌──────────────┐
│  TM 事务管理器 │          │  TC 事务协调器          │          │  RM 资源管理器 │
│ (发起方)      │ ──────► │ (协调中心)              │ ◄─────── │ (参与者)      │
│               │          │                        │          │              │
│ @GlobalTransactional     │  GlobalSession          │          │ DataSourceProxy │
│ → GlobalBegin            │  BranchSession          │          │ ConnectionProxy │
│ → GlobalCommit/Rollback  │  LockManager (全局锁)   │          │ undo_log 写入   │
└──────────────┘          └──────────────────────┘          └──────────────┘
    Client 端（微服务 A）       Seata Server（单独部署）        Client 端（微服务 B/C）
    transactionalTemplate       端口 8091                       DataSourceProxy
```

### 1.2 四种事务模式

| 模式 | 实现方式 | 侵入性 | 性能 | 适用场景 |
|------|---------|--------|------|---------|
| **AT（自动事务）** | 数据源代理 + SQL 拦截 + undo_log + 全局锁 | 低（只加注解）| 中 | MySQL/Oracle 支持事务的业务（**默认模式**）|
| **TCC（Try-Confirm-Cancel）** | 三阶段业务接口（手工实现）| 高（业务侵入）| 高 | 跨库/不支持本地事务 |
| **XA（两阶段提交）** | 数据库层面 XA 协议 | 中（依赖 XA 驱动）| 低（锁粒度大）| 金融/强一致性要求高 |
| **Saga（长事务）** | 状态机 + 补偿事务 | 中（状态机定义）| 高 | 长流程/异步链路 |

---

## 二、AT 模式完整执行流程

### 2.1 时序图（从 @GlobalTransactional 到 undo_log 写入）

```
@GlobalTransactional
public void createOrder() {
    orderService.create();      // 微服务 A 发起方
    storageService.deduct();    // 微服务 B 参与者
    accountService.debit();     // 微服务 C 参与者
}

// ① TM 发起全局事务
TransactionalTemplate.execute (L52-120)
    → handle propagation（REQUIRED/REQUIRES_NEW 等）
    → DefaultGlobalTransaction.begin
        → TC.GlobalBegin → 返回 XID（全局事务 ID）
        → RootContext.bind(xid)  // 绑定到当前线程 ThreadLocal

// ② 业务代码执行 → DataSourceProxy.getConnection()
DataSourceProxy (rm-datasource)
    → 包装原始 DataSource.getConnection()
    → 返回 ConnectionProxy（继承 AbstractConnectionProxy）
    → 内部 connectionContext.bind(xid)

// ③ 执行 UPDATE/INSERT/DELETE → ConnectionProxy 自动拦截
ConnectionProxy.execute
    → SqlExecutorTemplate（模板方法模式）
        → 1. SELECT ... FOR UPDATE（前镜像）
        → 2. 执行业务 SQL
        → 3. SELECT ... FOR UPDATE（后镜像）
        → 4. 写入 undo_log（beforeImage + afterImage + SQL）
        → 5. 注册 Branch 到 TC（BranchRegister）
           → TC 创建 BranchSession（保存 lockKey + branchStatus）

// ④ 业务正常返回 → TM GlobalCommit
TransactionalTemplate.execute → commit phase
    → DefaultGlobalTransaction.commit
        → TC.GlobalCommit
            → 遍历所有 BranchSession
                → 异步通知 RM GlobalReleaseLock（释放全局锁）
                → 异步通知 RM BranchCommit（删除 undo_log）
            → GlobalSession.status = GlobalStatus.Committed

// ⑤ 业务抛异常 → TM GlobalRollback
TransactionalTemplate.execute → catch → rollback phase
    → DefaultGlobalTransaction.rollback
        → TC.GlobalRollback
            → 遍历所有 BranchSession
                → 异步通知 RM BranchRollback
                    → 查询 undo_log → 根据 beforeImage 还原数据
                    → 执行业务 SQL 反向写回
            → GlobalSession.status = GlobalStatus.RolledBack
```

---

## 三、TM 事务管理器核心

### 3.1 TransactionalTemplate.execute（⭐⭐⭐⭐⭐）

> **源码锚点**：`tm/src/main/java/io/seata/tm/api/TransactionalTemplate.java` L52-120  
> **核心方法**：Spring AOP 切面调用的事务模板入口

```java
public Object execute(TransactionalExecutor business) throws Throwable {
    // ① 获取事务上下文
    TransactionInfo txInfo = business.getTransactionInfo();
    GlobalTransaction tx = GlobalTransactionContext.getCurrent(); // null or existing

    // ② ★ 事务传播处理（和 Spring @Transactional 传播语义一致）
    Propagation propagation = txInfo.getPropagation();
    switch (propagation) {  // L65-111
        case REQUIRED:      // 默认：有就加入，没有就开新的
            tx = GlobalTransactionContext.getCurrentOrCreate();
            break;
        case REQUIRES_NEW:  // 挂起旧事务 → 创建新事务
            if (existingTransaction(tx)) suspendedResourcesHolder = tx.suspend(false);
            tx = GlobalTransactionContext.createNew();
            break;
        case NOT_SUPPORTED: // 挂起旧事务 → 非事务执行
            if (existingTransaction(tx)) suspendedResourcesHolder = tx.suspend(false);
            return business.execute();
        case SUPPORTS:      // 有事务就加入，没有就非事务执行
            if (notExistingTransaction(tx)) return business.execute();
            break;
        case MANDATORY:     // 必须有事务，否则抛异常
            if (notExistingTransaction(tx)) throw new TransactionException("No existing transaction...");
            break;
        case NEVER:         // 不能有事务，否则抛异常
            if (existingTransaction(tx)) throw new TransactionException("Existing transaction...");
            break;
    }

    // ③ ★ 开启全局事务
    tx.begin(timeout, name);  // → TC GlobalBegin RPC

    try {
        // ④ 执行业务逻辑
        Object result = business.execute();
        // ⑤ 提交全局事务
        tx.commit();  // → TC GlobalCommit RPC
        return result;
    } catch (Throwable ex) {
        // ⑥ 回滚全局事务
        tx.rollback();  // → TC GlobalRollback RPC
        throw ex;
    } finally {
        // ⑦ 清理上下文
        //   RootContext.unbind();
    }
}
```

### 3.2 DefaultGlobalTransaction（⭐⭐⭐⭐⭐）

> **源码锚点**：`tm/src/main/java/io/seata/tm/api/DefaultGlobalTransaction.java` L87-100  
> **三个核心方法**：begin / commit / rollback

```java
// ★ DefaultGlobalTransaction 核心字段（L47-51）
public class DefaultGlobalTransaction implements GlobalTransaction {
    private TransactionManager transactionManager;   // 底层事务管理器（TMClient）
    private String xid;                               // 全局事务 ID
    private GlobalStatus status;                      // 全局事务状态
    private GlobalTransactionRole role;               // Launcher / Participant
}

// ★ begin 方法（L87-100）
public void begin(int timeout, String name) throws TransactionException {
    this.createTime = System.currentTimeMillis();
    if (role != GlobalTransactionRole.Launcher) {
        // Participant 加入已有事务 → 不发 GlobalBegin（XID 已存在）
    } else {
        // Launcher 发起新事务 → 发 GlobalBegin RPC 给 TC
        xid = transactionManager.begin(null, null, name, timeout);
        RootContext.bind(xid);  // ★ ThreadLocal 绑定 XID
    }
}

// ★ commit → transactionManager.commit(xid) → TC GlobalCommit RPC
// ★ rollback → transactionManager.rollback(xid) → TC GlobalRollback RPC
```

### 3.3 XID 全局事务 ID

```java
// ★ XID 格式：ip:port + transactionId
//   例子：192.168.1.100:8091:1234567890
//
// XID 传递链路：
//   TM GlobalBegin → TC 分配 → 返回给 TM → TM RootContext.bind(xid)
//   → FeignInterceptor 把 XID 放到 HTTP Header（RootContext.getXid()）
//   → 下游微服务 SeataInterceptor 拦截 → RootContext.bind(xid)
//   → 业务执行期间 RootContext.getXid() 一直能拿到（ThreadLocal）
//
// RootContext 核心：ThreadLocal<String> xid
```

---

## 四、RM 资源管理器核心

### 4.1 DataSourceProxy（AT 模式入口）⭐⭐⭐⭐⭐

> **源码锚点**：`rm-datasource/src/main/java/io/seata/rm/datasource/DataSourceProxy.java` L42-115  
> **继承关系**：`AbstractDataSourceProxy extends DataSourceProxy implements Resource`  
> **核心作用**：包装原始 DataSource → 拦截 getConnection() → 返回 ConnectionProxy

```java
// ★ DataSourceProxy 核心字段（L46-60）
public class DataSourceProxy extends AbstractDataSourceProxy implements Resource {
    private String resourceGroupId = "DEFAULT";   // 默认资源组
    private String jdbcUrl;                        // JDBC URL（从原始 DataSource 提取）
    private String resourceId;                    // ★ 资源 ID = dbType + jdbcUrl（TC 用它识别数据源）
    private String dbType;                        // mysql / oracle / postgresql
    private String userName;
    private String kernelVersion;                 // 数据库内核版本（影响 undo_log 解析）
}

// ★ DataSourceProxy 构造流程
//   1. 从原始 DataSource 获取 Connection → 读取元数据
//   2. 提取 jdbcUrl / dbType / userName / kernelVersion
//   3. 生成 resourceId = dbType + ":" + jdbcUrl
//   4. 注册到 DefaultResourceManager → RM 启动时自动注册 Resource
//   5. DefaultResourceManager.registerResource(resource)
//      → TC ResourceManager.registerResource(resourceId) RPC
//      → TC 保存 Resource 列表

// ★ getConnection 返回 ConnectionProxy（继承 AbstractConnectionProxy）
//   ConnectionProxy = 原始 Connection + context（SQLUndoLog 列表 + lockKey）
```

### 4.2 ConnectionProxy（undo_log 写入核心）⭐⭐⭐⭐⭐

> **源码锚点**：`rm-datasource/src/main/java/io/seata/rm/datasource/ConnectionProxy.java`  
> **核心方法**：bind(xid) / checkLock(lockKeys) / appendUndoLog(sqlUndoLog) / appendLockKey(lockKey)

```java
// ★ ConnectionProxy 字段（L50-58）
public class ConnectionProxy extends AbstractConnectionProxy {
    private final ConnectionContext context = new ConnectionContext();  // ★ 事务上下文
    private final LockRetryPolicy lockRetryPolicy = new LockRetryPolicy(this);
}

// ★ ConnectionContext 字段（核心！保存 undo_log + lockKey）
public class ConnectionContext {
    private String xid;                    // 全局事务 ID
    private String branchId;               // 分支事务 ID（注册后由 TC 分配）
    private boolean globalLockRequire;     // 是否需要全局锁
    private List<SQLUndoLog> undoItems;    // ★ undo_log 列表（beforeImage + afterImage）
    private Set<String> lockKeys;         // ★ 需要加锁的 key（格式：tableName:pkValue）
}

// ★ bind(xid) 方法（L84-86）
//   从 TM/Feign 拦截器拿到 XID 后，ConnectionProxy 绑定 XID
public void bind(String xid) {
    context.bind(xid);
}

// ★ checkLock(lockKeys) 方法（L112-126）
//   执行业务 SQL 前检查全局锁（SELECT FOR UPDATE 之后）
public void checkLock(String lockKeys) throws SQLException {
    boolean lockable = DefaultResourceManager.get().lockQuery(
        BranchType.AT, getDataSourceProxy().getResourceId(),
        context.getXid(), lockKeys);
    if (!lockable) throw new LockConflictException(...);
}

// ★ appendUndoLog(sqlUndoLog) 方法（L171-173）
//   业务 SQL 执行后，写入 undo_log 到 ConnectionContext
public void appendUndoLog(SQLUndoLog sqlUndoLog) {
    context.appendUndoItem(sqlUndoLog);
}

// ★ appendLockKey(lockKey) 方法（L181+）
//   记录需要加全局锁的 key（注册 Branch 时传给 TC）
```

### 4.3 undo_log 三阶段写入流程（⭐⭐⭐⭐⭐）

```java
// ★ Seata AT 模式 SQL 执行拦截核心（伪代码）
//   入口：SqlExecutorTemplate.execute(connectionProxy, sql, params)
//   内部模板方法：beforeImage → executeSQL → afterImage → undo_log写入 → BranchRegister

public <T> T execute(ConnectionProxy connectionProxy, String sql, Object[] params) {
    // ① 前镜像：SELECT ... FOR UPDATE（在本地事务内，加行锁）
    //    目的：获取 UPDATE 之前的原始数据快照
    //    用 TableMetaCache 拿到主键列名 → 构造 WHERE 条件
    TableMeta tableMeta = TableMetaCacheFactory.getTableMetaCache(dataSourceProxy).getTableMeta(tableName);
    String whereSql = buildWhereForUpdate(sql, tableMeta, params);
    
    connectionProxy.setAutoCommit(false);  // ★ 开启本地事务
    connectionProxy.getTargetConnection().begin();
    
    ResultSet before = connectionProxy.getTargetConnection().createStatement().executeQuery(whereSql);
    List<Row> beforeImage = parseResultSet(before);
    
    // ② 执行业务 SQL（在同一个本地事务内）
    PreparedStatement ps = connectionProxy.getTargetConnection().prepareStatement(sql);
    setParams(ps, params);
    ps.executeUpdate();
    
    // ③ 后镜像：SELECT ... FOR UPDATE（UPDATE 之后再查一次）
    ResultSet after = connectionProxy.getTargetConnection().createStatement().executeQuery(whereSql);
    List<Row> afterImage = parseResultSet(after);
    
    // ④ 写入 undo_log（SQLUndoLog 对象序列化后插入 undo_log 表）
    SQLUndoLog undoLog = new SQLUndoLog();
    undoLog.setSqlType(SQLType.UPDATE);
    undoLog.setBeforeImage(beforeImage);
    undoLog.setAfterImage(afterImage);
    undoLog.setSql(sql);
    undoLog.setTableMeta(tableMeta);
    connectionProxy.appendUndoLog(undoLog);  // ★ 保存到 ConnectionContext
    
    // ⑤ 注册 Branch 到 TC
    DefaultResourceManager.get().branchRegister(BranchType.AT,
        connectionProxy.getDataSourceProxy().getResourceId(),
        null,  // clientId
        connectionProxy.getContext().getXid(),
        String.join(",", connectionProxy.getContext().getLockKeys()),  // ★ lockKey
        connectionProxy.getContext().getUndoLog().length,              // undo_log 字节数组
        connectionProxy.getConnectionProxy().getContext().getBranchId());
    
    // ⑥ 本地事务提交（undo_log 和业务数据在同一个本地事务内 → 原子性！）
    connectionProxy.getTargetConnection().commit();
    
    return result;
}
```

---

## 五、TC 事务协调器核心

### 5.1 BranchSession 关键字段（⭐⭐⭐⭐⭐）

> **源码锚点**：`server/src/main/java/io/seata/server/session/BranchSession.java` L55-88

```java
// ★ BranchSession 结构（TC 保存每个分支事务的会话信息）
public class BranchSession implements Lockable, Comparable<BranchSession>, SessionStorable {
    private String xid;                  // 全局事务 ID
    private long transactionId;          // 全局事务 ID（数字形式）
    private long branchId;               // ★ 分支事务 ID（全局唯一，TC 分配）
    private String resourceGroupId;      // 资源组
    private String resourceId;           // ★ 资源 ID = dbType + jdbcUrl
    private String lockKey;              // ★ 加锁的行键（tableName:pkValue）
    private BranchType branchType;       // AT / TCC / XA / SAGA
    private BranchStatus status;         // ★ 分支事务状态（Unknown/Registered/PhaseOne_Done/Committed/Rollbacked）
    private LockStatus lockStatus;       // Locked / Released
    private Map<BucketLockMap, Set<String>> lockHolder;  // ★ 持有的锁（AT 模式才初始化）
}

// TC 维护结构：
//   GlobalSession（一个全局事务）→ Map<Long, BranchSession> branchSessions
//   LockManager（全局锁管理器）→ Map<String, LockHolderMap>（tableName + pkValue → BranchSession）
```

### 5.2 全局锁机制（⭐⭐⭐⭐⭐）

```java
// ★ Seata AT 模式全局锁：解决"脏写"问题
//
// 脏写问题：
//   事务 A UPDATE order SET status='paid' WHERE id=100;  → 还没提交
//   事务 B UPDATE order SET status='refunded' WHERE id=100; → 覆盖了 A 的修改
//   → A 提交 / B 提交 → 最后谁提交谁生效 → 数据不一致
//
// Seata 全局锁解决方案：
//   1. 业务 SQL 执行前 → ConnectionProxy.checkLock(lockKey) → TC 检查全局锁
//      → LockManager.addLock(lockKey, branchSession)
//      → 如果 lockKey 已被其他 xid 持有 → 返回 false → 抛 LockConflictException
//
//   2. BranchCommit / BranchRollback 时 → TC LockManager.releaseLock(lockKey)
//      → 让其他等待的分支事务可以继续
//
// ★ lockKey 格式：tableName:pkValue
//   例子：order:100, order:101, order_item:200
//
// ★ AT 模式锁是"软锁"（存在 TC 内存中，不是数据库行锁）
//   → 数据库层锁：Branch 执行期间用本地事务 + SELECT FOR UPDATE 加行锁
//   → TC 全局锁：防止不同全局事务同时修改同一行数据
```

### 5.3 BranchCommit / BranchRollback

```java
// ★ BranchCommit（TC → RM 异步通知）
//   RM 收到 BranchCommit 消息 → UndoLogManager.deleteUndoLog(xid, branchId)
//   → 从 undo_log 表删除记录（异步，不阻塞全局提交）
//
// ★ BranchRollback（TC → RM 通知回滚）
//   RM 收到 BranchRollback 消息：
//     → 查 undo_log 表 WHERE xid=? AND branch_id=?
//     → 反序列化 undo_log 字节数组 → 拿到 beforeImage + afterImage + SQL 类型
//     → 根据 beforeImage 构造反向 SQL：
//        UPDATE order SET status=beforeImage.status WHERE id=beforeImage.id AND status=afterImage.status
//        （★ WHERE 条件用 afterImage 做乐观锁 → 如果数据又被改了 → Rollback 失败 → 人工介入）
//     → 执行反向 SQL → 还原数据
//     → 删除 undo_log
//
// ★ Rollback 乐观锁保护：
//   WHERE id=100 AND status='paid'（afterImage）
//   → 如果另一个事务把 status 改成了 'refunded' → 影响行数 0
//   → RollbackFailed（数据被非法修改，Seata 无法自动回滚）
```

---

## 六、面试考点（20 题）

### 6.1 AT 模式核心（12 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | AT 模式 undo_log 三阶段？| **SELECT FOR UPDATE（前镜像）→ 执行业务 SQL → SELECT FOR UPDATE（后镜像）** → 写入 undo_log |
| 2 | undo_log 内容？| beforeImage（前镜像）+ afterImage（后镜像）+ SQL 类型 + 表元数据 |
| 3 | undo_log 表结构？| xid + branch_id + undo_log（BLOB）+ log_type |
| 4 | DataSourceProxy 核心作用？| **包装原始 DataSource → 拦截 getConnection() → 返回 ConnectionProxy** |
| 5 | ConnectionProxy 的 context 存什么？| **xid + branchId + undoItems 列表 + lockKeys 集合** |
| 6 | 全局锁怎么解决脏写？| **TC LockManager.addLock(lockKey, branchId)** → 同一行只允许一个全局事务加锁 |
| 7 | lockKey 格式？| **tableName:pkValue**（如 order:100, order:101）|
| 8 | BranchCommit 做什么？| TC 异步通知 RM → **UndoLogManager.deleteUndoLog(xid, branchId)**（异步删除，不阻塞提交）|
| 9 | BranchRollback 怎么做？| RM 查 undo_log → **根据 beforeImage 构造反向 SQL（UPDATE ... WHERE pk AND afterImage）** → 乐观锁保护 |
| 10 | Rollback 乐观锁失败怎么办？| **影响行数 0 → RollbackFailed**（数据被其他事务改了 → 人工介入 / 重试）|
| 11 | XID 传递方式？| **FeignInterceptor 拦截 → 放到 HTTP Header → 下游 SeataInterceptor 拦截** → RootContext.bind(xid) |
| 12 | AT 模式优缺点？| ✅ 自动（数据源代理 + 注解）+ 无业务侵入；❌ **不支持 DDL + 不支持多主键复杂表 + rollback 乐观锁失败无法自动回滚** |

### 6.2 TM/TC/RM 架构（8 题）

| # | 考点 | 答案 |
|---|------|------|
| 13 | 三大角色？| TM（发起方）→ TC（协调中心）→ RM（参与者）|
| 14 | TransactionalTemplate.execute 流程？| **Propagation 处理 → Begin → business.execute → Commit/Rollback → Finally 清理** |
| 15 | 事务传播和 Spring 区别？| Seata 自己实现一套（REQUIRED/REQUIRES_NEW/NOT_SUPPORTED/SUPPORTS/NEVER/MANDATORY），语义和 Spring 一致 |
| 16 | DefaultGlobalTransaction.begin 核心？| **TransactionManager.begin RPC 给 TC → 返回 xid → RootContext.bind(xid) ThreadLocal 绑定** |
| 17 | BranchSession 关键字段？| **xid / branchId / resourceId / lockKey / status / lockHolder（全局锁）** |
| 18 | TC GlobalCommit 流程？| **遍历所有 BranchSession → 异步通知 RM BranchCommit（删除 undo_log + 释放锁）** → 异步不阻塞 |
| 19 | TC GlobalRollback 流程？| **遍历所有 BranchSession（后进先出）→ 通知 RM BranchRollback（undo_log 反向 SQL 还原）** |
| 20 | 四种事务模式适用场景？| AT（默认/业务透明）→ TCC（手工实现/不支持本地事务）→ XA（金融/强一致）→ Saga（长流程/异步链路）|

---

## 七、核心源码锚点速查表

| 模块 | 类 | 方法 | 精确行号 | 核心功能 |
|------|------|------|---------|---------|
| **TM** | TransactionalTemplate | **execute** | L52-120 | ★ 事务模板入口：Propagation→Begin→Commit/Rollback |
| **TM** | DefaultGlobalTransaction | **begin** | L87-100 | ★ RootContext.bind(xid) ThreadLocal 绑定 |
| **TM** | GlobalTransactionContext | getCurrentOrCreate | — | 获取/创建当前全局事务 |
| **RM** | DataSourceProxy | 构造 + getConnection | L74-115 | ★ 包装原始 DS → 返回 ConnectionProxy |
| **RM** | ConnectionProxy | **bind** | L84-86 | 绑定 XID |
| **RM** | ConnectionProxy | **checkLock** | L112-126 | ★ TC LockQuery 检查全局锁 |
| **RM** | ConnectionProxy | **appendUndoLog** | L171-173 | ★ undo_log 写入 ConnectionContext |
| **RM** | ConnectionContext | undoItems + lockKeys | 字段 | 事务上下文核心 |
| **TC** | BranchSession | 核心字段 | L55-78 | ★ xid/branchId/resourceId/lockKey/status/lockHolder |
| **TC** | BranchSession | 构造（AT 模式）| L85-88 | AT 模式 lockHolder=new ConcurrentHashMap<>(8) |

---

## 八、源码访问与调试

### 8.1 本地源码路径

```
d:\GC_WorkSpaces\incubator-seata-2.0.0\
├── tm\src\main\java\io\seata\tm\           ← TM 事务管理器
│   └── api\TransactionalTemplate.java       ← ★ 事务模板入口
│   └── api\DefaultGlobalTransaction.java    ← ★ begin/commit/rollback
├── rm-datasource\src\main\java\io\seata\rm\datasource\  ← RM AT 模式核心
│   ├── DataSourceProxy.java                 ← ★ 数据源代理
│   └── ConnectionProxy.java                 ← ★ 连接代理 + undo_log
├── server\src\main\java\io\seata\server\   ← TC 事务协调器
│   └── session\BranchSession.java           ← ★ 分支会话
└── sqlparser\seata-sqlparser-core\          ← SQL 解析器（Druid/ANTLR）
```

### 8.2 本地启动调试

```bash
# 1. 启动 TC（Seata Server，单机模式）
cd d:\GC_WorkSpaces\incubator-seata-2.0.0
mvn -pl server -am package -DskipTests
cd server/target/seata-server-2.0.0/bin
./seata-server.sh -p 8091 -m file   # 端口 8091，file 模式存储

# 2. IDEA 远程调试
#   TC 端口 8091
#   Server 启动参数 -Dtransaction.service.group=my_tx_group

# 3. Spring Boot 项目引入
#   pom.xml: seata-spring-boot-starter
#   yml: seata.tx-service-group=my_tx_group
#   DataSourceProxy 和 @GlobalTransactional 自动注入
```

### 8.3 断点建议

```
# TM 侧：
TransactionalTemplate.execute           L52  ← 事务模板入口
DefaultGlobalTransaction.begin          L98  ← 开启全局事务
DefaultGlobalTransaction.commit         —    ← 提交全局事务

# RM 侧：
ConnectionProxy.checkLock               L112 ← 全局锁检查
ConnectionProxy.appendUndoLog           L171 ← undo_log 写入

# TC 侧：
BranchSession（构造时观察字段值）         L85  ← AT 模式 lockHolder 初始化
```

---

**文档版本：v1.0 · 2026-10-21**  
**源码基线：Seata 2.0.0（基于 d:\GC_WorkSpaces\incubator-seata-2.0.0\ 真实源码）**  
**面试考点：20 题，源码锚点：10+ 处精确行号**
