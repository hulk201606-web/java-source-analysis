# MyBatis-Plus 源码深度解析

> **源码基线**：MyBatis-Plus 3.5.17（最新稳定）  
> **核心仓库**：https://github.com/baomidou/mybatis-plus  
> **面试权重**：⭐⭐⭐⭐⭐（国内持久层高频）

---

## 一、MyBatis-Plus 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  定位：MyBatis 的增强工具包（只增强不改变）                     │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  核心增强：                                                │ │
│  │  ① 通用 CRUD（BaseMapper）→ 单表操作零 SQL               │ │
│  │  ② 条件构造器（Wrapper）→ QueryWrapper / LambdaQueryWrapper│ │
│  │  ③ 分页插件（PaginationInnerInterceptor）                 │ │
│  │  ④ 主键策略（ASSIGN_ID / AUTO / UUID）                    │ │
│  │  ⑤ 自动填充（MetaObjectHandler）                          │ │
│  │  ⑥ 逻辑删除（@TableLogic）                                 │ │
│  │  ⑦ 乐观锁（OptimisticLockerInnerInterceptor）              │ │
│  │  ⑧ SQL 注入拦截（MybatisPlusInterceptor + InnerInterceptor）│ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  和 MyBatis 关系：                                             │
│    MyBatisPlusConfiguration extends MybatisConfiguration      │
│    → 自动注册 MybatisPlusInterceptor（MyBatis 原生拦截器）    │
│    → 重写 MappedStatement 解析逻辑（支持 BaseMapper 通用方法） │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 MybatisPlusInterceptor（★ SQL 拦截核心）

```java
// ★ MyBatis-Plus 所有增强都基于拦截器
//   继承 MyBatis Interceptor 接口
//   内部维护 List<InnerInterceptor>（内部插件链）
//
// MybatisPlusInterceptor.intercept(Invocation invocation) 流程：
//   1. 从 invocation 拿 Object target
//      → Executor（query/update）→ StatementHandler（prepare）
//      → ParameterHandler（setParameters）→ ResultSetHandler（handleResultSets）
//
//   2. 遍历 innerInterceptorList
//      for (InnerInterceptor innerInterceptor : innerInterceptorList) {
//          innerInterceptor.intercept(executor, ms, parameter, rowBounds, resultHandler, boundSql);
//      }
//
// ★ 内置 InnerInterceptor 实现（核心插件）：
//   ① PaginationInnerInterceptor ★ 分页（LIMIT 改写 SQL）
//   ② OptimisticLockerInnerInterceptor 乐观锁
//   ③ LogicDeleteInnerInterceptor 逻辑删除
//   ④ AutoFillInnerInterceptor 自动填充
//   ⑤ TenantLineInnerInterceptor 多租户
//   ⑥ DataPermissionInterceptor 数据权限
//
// ★ 执行顺序：按 List 添加顺序（先加先执行）
```

### 2.2 PaginationInnerInterceptor 分页核心（★★★★★）

```java
// ★ 分页插件是面试最高频！
//
// 原理：拦截 MyBatis Executor.query 方法
//
// 完整流程：
//
//   Executor.query(ms, parameter, rowBounds, resultHandler)
//     ↓
//   PaginationInnerInterceptor.intercept()
//     ↓
//   1. 判断 parameter 是否有 Page 参数
//      → parameter instanceof IPage → 是分页请求
//      → 不是 → 直接放行（不拦截）
//
//   2. 改写 SQL
//      BoundSql boundSql = ms.getBoundSql(parameter);
//      String originalSql = boundSql.getSql();
//      String countSql = "SELECT COUNT(0) FROM (" + originalSql + ") temp";
//      String pageSql = originalSql + " LIMIT offset, size";
//
//      → 构造新的 BoundSql（countSql）执行 → 得到 total
//      → 构造新的 MappedStatement → 用 pageSql 执行 → 得到 records
//
//   3. 组装分页结果
//      Page<T> result = new Page<>(current, size, total);
//      result.setRecords(records);
//      → 返回给调用方
//
// ★ 为什么能实现自动分页？
//   MyBatis Executor.query 被拦截 → 替换 MappedStatement.sql
//   → 底层 JDBC 真正执行的是改写后的 LIMIT SQL
//   → 对业务代码完全透明（调用 list(page, wrapper) 就行）
//
// ★ 为什么要单独发 COUNT？
//   LIMIT 只能拿当前页数据 → 不知道 total 总数
//   → 必须额外 SELECT COUNT(0) 查总数 → 总共两次 SQL
```

### 2.3 BaseMapper 通用 CRUD（★★★★）

```java
// ★ BaseMapper<T> 接口 → 所有表自动有 CRUD 方法
//   不需要写 XML！
//
//   int insert(T entity)                              → INSERT
//   int deleteById(Serializable id)                   → DELETE WHERE id = ?
//   int updateById(T entity)                          → UPDATE SET ... WHERE id = ?
//   T selectById(Serializable id)                     → SELECT * WHERE id = ?
//   List<T> selectBatchIds(Collection<? extends Serializable> idList) → WHERE id IN (...)
//   T selectOne(Wrapper<T> queryWrapper)               → SELECT ... WHERE wrapper 条件 LIMIT 1
//   List<T> selectList(Wrapper<T> queryWrapper)       → SELECT ... WHERE wrapper 条件
//   List<T> selectPage(IPage<T> page, Wrapper<T> queryWrapper) → 分页查询
//
// ★ BaseMapper 方法怎么映射到 SQL？
//   1. MybatisPlusConfiguration 扫描 BaseMapper 子类
//   2. 动态生成 MappedStatement（每个方法一个）
//   3. SQL 模板自动拼接：
//      → insert(entity) → 遍历 entity 非空字段 → INSERT INTO table(col1,col2) VALUES(?,?)
//      → deleteById(id) → DELETE FROM table WHERE pk = #{id}
//      → selectPage(page, wrapper) → SQL + LIMIT + COUNT
//
// ★ 为什么能自动生成？
//   启动时解析实体类注解（@TableName / @TableField / @TableId）
//   → TableInfo 元数据缓存 → 运行时 SQL 模板拼接
```

### 2.4 Wrapper 条件构造器

```java
// ★ QueryWrapper / LambdaQueryWrapper 条件拼接
//
// QueryWrapper（字符串列名）：
//   QueryWrapper<User> wrapper = new QueryWrapper<>();
//   wrapper.eq("name", "张三")          → WHERE name = '张三'
//         .ge("age", 18)               → AND age >= 18
//         .like("email", "@qq.com")    → AND email LIKE '%@qq.com%'
//         .orderByDesc("create_time")  → ORDER BY create_time DESC
//         .last("LIMIT 10");           → 原样拼到 SQL 最后（危险！SQL 注入）
//
// LambdaQueryWrapper（方法引用，★ 推荐！编译期检查）：
//   LambdaQueryWrapper<User> wrapper = new LambdaQueryWrapper<>();
//   wrapper.eq(User::getName, "张三")      → 编译期检查，列名不会写错！
//         .ge(User::getAge, 18)
//         .like(User::getEmail, "@qq.com")
//
// ★ Wrapper 本质：
//   内部维护 Map<String, Object>（column → value）
//   遍历 map 拼接 WHERE 子句 → 传给 MyBatis ParameterHandler
//   → 底层还是 PreparedStatement（参数化查询，防止 SQL 注入）
//
// ★ 为什么 LambdaQueryWrapper 更安全？
//   User::getName 是方法引用 → 编译期检查
//   如果 User 类改了字段 → IDE 立刻标红提醒
//   QueryWrapper("name") 是字符串 → 改字段后运行时才报错
```

### 2.5 @TableName / @TableId / @TableField（实体元数据）

```java
// ★ 实体注解 → 启动时 TableInfo 元数据缓存
//
// @TableName("t_user")           → 指定表名（默认是类名驼峰转下划线）
// public class User {
//     @TableId(type = IdType.ASSIGN_ID)  // ★ 雪花算法生成 ID（默认）
//     private Long id;
//
//     @TableField("user_name")     // 指定列名（默认驼峰转下划线）
//     private String name;
//
//     @TableField(fill = FieldFill.INSERT)  // 自动填充
//     private LocalDateTime createTime;
//
//     @TableLogic                  // 逻辑删除标记
//     private Integer deleted;      // 0=正常，1=已删除
// }
//
// ★ IdType 主键策略：
//   AUTO            → 数据库自增
//   ASSIGN_ID ★默认 → 雪花算法（Long 型，分布式唯一）
//   ASSIGN_UUID     → UUID（String 型）
//   INPUT           → 手动输入
//
// ★ @TableLogic 逻辑删除原理：
//   deleteById → 实际执行 UPDATE SET deleted=1 WHERE id=?
//   selectXxx → 自动追加 AND deleted=0
//   updateById → 自动追加 AND deleted=0
//
// ★ MetaObjectHandler 自动填充：
//   public class MyMetaObjectHandler implements MetaObjectHandler {
//       @Override
//       public void insertFill(MetaObject metaObject) {
//           this.strictInsertFill(metaObject, "createTime", LocalDateTime.class, LocalDateTime.now());
//           this.strictInsertFill(metaObject, "updateTime", LocalDateTime.class, LocalDateTime.now());
//       }
//   }
//   → 所有 insert/update 自动填充时间字段
```

### 2.6 TableInfo 启动扫描流程

```java
// ★ 启动时自动扫描实体类 → 生成 TableInfo 元数据
//
// SqlSessionFactoryBean → buildSqlSessionFactory()
//   → MybatisPlusConfiguration 初始化
//   → TableInfoHelper.initTableInfo(...)
//     → 扫描 Mapper 包 → 找到 BaseMapper 子类
//     → 拿到泛型 T 的 Class → 解析注解
//     → @TableName → tableName
//     → @TableId + IdType → 主键信息
//     → @TableField → 字段信息（fieldList）
//     → 缓存到 TableInfoHelper.tableInfoMap（全局 Map）
//
// ★ 运行时每次 CRUD 操作：
//   → TableInfoHelper.getTableInfo(entityClass)
//   → 拿到 tableName + fieldList + primaryKey
//   → 动态拼接 SQL 模板
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | MP 分页插件原理？| **拦截 Executor.query → 改写 SQL 加 LIMIT → 额外发 COUNT 查总数 → 组装 Page<T> 返回** |
| 2 | 分页为什么发两次 SQL？| LIMIT SQL 只能拿当前页数据 → 不知道 total 总数 → 必须单独 SELECT COUNT(0) |
| 3 | BaseMapper 通用 CRUD 怎么实现？| 启动时解析 TableInfo（实体注解元数据）→ **动态生成 MappedStatement + SQL 模板拼接** |
| 4 | LambdaQueryWrapper 为什么安全？| **方法引用 User::getName → 编译期检查** → 字段改名 IDE 立刻报错；QueryWrapper("name") 运行时才报错 |
| 5 | @TableLogic 逻辑删除原理？| deleteById → 实际 **UPDATE SET deleted=1**；select/update → **自动追加 AND deleted=0** |
| 6 | IdType ASSIGN_ID 是什么？| **雪花算法（Snowflake）** → Long 型，分布式唯一 ID（Twitter 开源）；MP 默认主键策略 |
| 7 | 乐观锁怎么实现？| OptimisticLockerInnerInterceptor → **update 时自动追加 version 条件 + version = version + 1** |
| 8 | 自动填充？| MetaObjectHandler → insertFill / updateFill → **执行 SQL 前自动 set createTime/updateTime** |
| 9 | 多租户插件？| TenantLineInnerInterceptor → 所有 SQL **自动追加 tenant_id = 当前租户** |
| 10 | last("LIMIT 10") 危险吗？| ⚠️ 危险！**原样拼接到 SQL 末尾 → 可 SQL 注入**；只建议写 LIMIT 子句，不要拼用户输入 |
| 11 | MP 和 JPA 区别？| MP：**SQL 友好（自动生成但可自定义）+ MyBatis 生态**；JPA：**面向对象（Hibernate 自动生成 SQL 不透明）** |
| 12 | MP 和 MyBatis 关系？| MP 是 **MyBatis 增强工具包**（只增强不改变）→ 底层完全兼容 MyBatis，XML 和 BaseMapper 可以混用 |
| 13 | TableInfo 什么时候初始化？| **SqlSessionFactory 创建时** → 扫描实体注解 → 缓存到全局 Map → 运行时直接查表元数据 |
| 14 | PaginationInnerInterceptor 怎么注册？| @Bean 注入 MybatisPlusInterceptor → addInnerInterceptor(new PaginationInnerInterceptor()) |
| 15 | 自定义 SQL 怎么写分页？| 方法参数加 IPage<T> + Wrapper<T> → 自动分页（不需要手动写 LIMIT）；自定义 XML 也能分页 |
| 16 | MP 能做 SQL 审计吗？| 可以！自定义 InnerInterceptor → 在 intercept 里打印 BoundSql.getSql() + parameter → 实现审计日志 |
| 17 | MP 批量操作？| saveBatch(list) → 循环 insert（不是真正 JDBC batch）→ 可以用 @Transactional 保证原子性 |
| 18 | 字段填充 @TableField(fill=INSERT) 和自动填充区别？| @TableField(fill=) 声明填充时机；MetaObjectHandler 提供填充值 → 两者配合：注解声明 + Handler 实现 |

---

## 四、核心类速查表

| 类 | 职责 | 关键方法 |
|----|------|---------|
| **MybatisPlusInterceptor** | ★ 总拦截器 | intercept → innerInterceptorList 遍历 |
| **PaginationInnerInterceptor** | 分页插件 | intercept → 改写 SQL → count + page 两次执行 |
| **OptimisticLockerInnerInterceptor** | 乐观锁 | intercept → 追加 version 条件 |
| **TenantLineInnerInterceptor** | 多租户 | intercept → 追加 tenant_id 条件 |
| **LogicDeleteInnerInterceptor** | 逻辑删除 | intercept → delete 改 update / select 加条件 |
| **BaseMapper<T>** | ★ 通用 CRUD 接口 | insert / selectById / selectPage / selectList |
| **TableInfoHelper** | 表元数据缓存 | initTableInfo / getTableInfo |
| **TableInfo** | 单表元数据 | tableName / fieldList / primaryKey |
| **QueryWrapper / LambdaQueryWrapper** | 条件构造 | eq / ge / like / orderBy |
| **MetaObjectHandler** | 自动填充 | insertFill / updateFill |
| **IPage<T> / Page<T>** | 分页模型 | current / size / total / records |
| **IdWorker** | 雪花算法 | getId → Long |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：MyBatis-Plus 3.5.17  
**面试考点：18 题**
