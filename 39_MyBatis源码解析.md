# MyBatis 源码深度解析（MP 的底座）

> **源码基线**：MyBatis 3.5.19（最新稳定）  
> **核心仓库**：https://github.com/mybatis/mybatis-3  
> **面试权重**：⭐⭐⭐⭐⭐（ORM 框架高频，MP 是它的增强）

---

## 一、MyBatis 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：半自动 ORM 框架（手写 SQL，自动做结果映射）          │
│  设计哲学：SQL 和 Java 代码解耦 → XML/注解写 SQL，Java 写逻辑   │
│                                                               │
│  分层执行流程：                                                │
│                                                               │
│  User Code（写 Mapper 接口方法）                              │
│      ↓                                                        │
│  SqlSessionFactory（工厂，配置全局属性 + Mapper 注册）         │
│      ↓ openSession()                                          │
│  SqlSession（会话，CRUD API + 事务控制）                       │
│      ↓ getMapper() → JDK Proxy 动态代理                       │
│  MapperProxy → MapperMethod（方法路由）→ Configuration        │
│      ↓ selectList / update                                   │
│  Executor（★★★★★ 核心！执行链）                              │
│    → CachingExecutor（二级缓存，装饰）                         │
│    → BaseExecutor（一级缓存 + 事务）                          │
│      → SimpleExecutor / ReuseExecutor / BatchExecutor         │
│        ↓ StatementHandler                                    │
│  StatementHandler（JDBC Statement 封装）                       │
│    → RoutingStatementHandler                                  │
│    → PreparedStatementHandler → JDBC PreparedStatement        │
│      ↓ ParameterHandler                                      │
│  ParameterHandler（参数处理：Java → JDBC）                    │
│      ↓ ResultSetHandler                                      │
│  ResultSetHandler（★ 结果集映射：JDBC → Java 对象）            │
│      → DefaultResultSetHandler → 反射/构造器赋值               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 SqlSessionFactory + Configuration（配置核心）

```java
// ★ SqlSessionFactoryBuilder.build() 核心流程
//
//   1. InputStream / Reader 加载 mybatis-config.xml
//   2. XMLConfigBuilder.parse() → 解析配置
//   3. 构建 Configuration（全局唯一，所有属性 + Mapper 注册都在这里）
//   4. XMLMapperBuilder.parse() → 扫描所有 Mapper XML / 注解
//      → 每个 Mapper 接口生成 MappedStatement（SQL 模板）
//      → Configuration.addMappedStatement(ms) → 全局 Map 缓存
//   5. 返回 DefaultSqlSessionFactory（持有 Configuration）
//
// ★ Configuration 持有什么？
//   Map<String, MappedStatement> mappedStatements  ← 所有 SQL 模板
//   Map<String, MapperProxyFactory> mapperRegistry  ← 所有 Mapper 接口
//   Environment environment                        ← DataSource + TransactionFactory
//   TypeHandlerRegistry typeHandlerRegistry        ← 类型处理器
//   MapperBuilderAssistant mapperBuilderAssistant  ← Mapper 构建助手
//
// ★ MappedStatement（SQL 模板，面试高频）：
//   String id;               // 唯一标识（namespace + "." + method）
//   SqlSource sqlSource;     // SQL 源码（StaticSqlSource / DynamicSqlSource / RawSqlSource）
//   SqlCommandType sqlCommandType;  // SELECT / INSERT / UPDATE / DELETE
//   ResultMaps resultMaps;   // 结果映射
//   ParameterMap parameterMap; // 参数映射
//   Integer fetchSize;      // JDBC fetchSize
//   StatementType statementType;  // PREPARED / SIMPLE / CALLABLE
```

### 2.2 Executor 执行链（★★★★★ 面试必问）

```java
// ★ Executor 是 MyBatis 执行链核心（类似 Sentinel Slot 链 / Dubbo Filter 链）
//
// 四种实现：
//   ① CachingExecutor（装饰）→ 二级缓存（如果开启 cache-enabled）
//   ② SimpleExecutor（默认）→ 每次创建新 Statement
//   ③ ReuseExecutor → 复用 Statement（关闭时才关闭）
//   ④ BatchExecutor → 批量执行（addBatch + executeBatch）
//
// ★ BaseExecutor（简单/复用/批量的父类，核心实现）
//   protected PerpetualCache localCache;  ← ★ 一级缓存！
//   protected Transaction transaction;
//   protected Executor wrapper;
//
// ★ query() 流程（简化）：
//   public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler) {
//       // ① 生成 CacheKey（用 ms.id + parameter + rowBounds + sql + boundSql）
//       CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
//
//       // ② 查一级缓存
//       List<E> list = (List<E>) localCache.getObject(key);
//       if (list != null) {
//           // 命中！直接返回（不查 DB）
//           return list;
//       }
//
//       // ③ 查 DB
//       list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
//
//       // ④ 写入一级缓存
//       localCache.putObject(key, list);
//       return list;
//   }
//
// ★ 一级缓存 vs 二级缓存（面试必问！）：
//
//   ┌──────────┬────────────────────┬────────────────────┐
//   │          │    一级缓存           │    二级缓存           │
//   ├──────────┼────────────────────┼────────────────────┤
//   │ 归属      │ BaseExecutor        │ CachingExecutor      │
//   │ 作用域    │ 同一个 SqlSession    │ 同一个 Mapper 不同    │
//   │          │（SqlSession 关闭清空）│ SqlSession 共享      │
//   │ 默认开启？│ ✅ 是                 │ ❌ 否（要手动配）      │
//   │ 存储位置  │ JVM 堆（PerpetualCache）│ 可插拔内存 / Redis / │
//   │          │                    │ Ehcache             │
//   │ 失效时机  │ SqlSession.close() / │ commit() / rollback()│
//   │          │ update 操作           │ update 操作          │
//   │ 面试陷阱  │ 同一个 SqlSession 里 │ 跨 SqlSession 共享    │
//   │          │ 同一个 SQL 只查一次    │ 不同 SqlSession 还能 │
//   │          │                    │ 命中（但要二级缓存开启）│
//   └──────────┴────────────────────┴────────────────────┘
```

### 2.3 StatementHandler + ParameterHandler + ResultSetHandler

```java
// ★ StatementHandler 三种实现：
//   RoutingStatementHandler → 根据 MappedStatement.statementType 路由
//   SimpleStatementHandler → JDBC Statement
//   PreparedStatementHandler → ★ 默认！JDBC PreparedStatement（防止 SQL 注入）
//   CallableStatementHandler → JDBC CallableStatement（存储过程）
//
// PreparedStatementHandler.query() 核心：
//   ① preparedStatement = instantiateStatement(connection, sql)
//   ② parameterHandler.setParameters(preparedStatement) ← ★ 参数处理
//   ③ preparedStatement.execute()                        ← JDBC 执行
//   ④ resultSetHandler.handleResultSets(preparedStatement) ← ★ 结果处理
//
// ★ ParameterHandler（参数处理核心）：
//   DefaultParameterHandler.setParameters()
//     → 遍历 ParameterMapping
//     → typeHandler.setParameter(ps, i, value, jdbcType)
//     → TypeHandler 实现：IntegerTypeHandler / StringTypeHandler / DateTypeHandler
//       （把 Java 对象设到 PreparedStatement 上）
//
// ★ ResultSetHandler（结果集处理核心）：
//   DefaultResultSetHandler.handleResultSets()
//     → ResultSetMetaData（JDBC 元数据，拿到列名和类型）
//     → 循环 ResultSet.next()
//     → 对每行创建 ResultObjectFactory.create() → new Bean
//     → 反射/构造器 + TypeHandler.getResult() 赋值
//     → ★ 支持嵌套（ResultMap 嵌套 ResultMap → 多表 Join 结果自动组装）
//
// ★ 嵌套结果处理（面试高频）：
//   一对多 / 一对一 Join 查询 → ResultMap 嵌套 ResultMap
//   → DefaultResultSetHandler.createRowResultHandler
//   → 同一个 id 的多条结果 → 合并成一个对象（association / collection）
```

### 2.4 Dynamic SQL（MyBatis 动态标签）

```xml
<!-- ★ MyBatis 动态 SQL 标签（面试高频） -->

<select id="findUser" resultType="User">
  SELECT * FROM user
  <where>                        <!-- 自动加 WHERE + 去掉多余的 AND -->
    <if test="name != null">     <!-- if 判断 -->
      AND name = #{name}
    </if>
    <choose>                      <!-- choose/when/otherwise 互斥分支 -->
      <when test="age > 18">
        AND age > 18
      </when>
      <otherwise>
        AND age <= 18
      </otherwise>
    </choose>
  </where>
  <foreach collection="ids" item="id" open="id IN (" separator="," close=")">
    #{id}                         <!-- foreach 遍历集合 -->
  </foreach>
  <trim prefix="ORDER BY" suffixOverrides=",">
    <if test="orderBy != null">
      ${orderBy},                 <!-- trim 去掉尾部逗号 -->
    </if>
  </trim>
</select>

<!-- ★ ${} vs #{}（面试高频！） -->
#{} → 预编译参数（PreparedStatement）→ 防 SQL 注入 ✅
${} → 直接字符串替换（Statement）→ 可能 SQL 注入 ❌
  → 适合动态表名 / 列名 / ORDER BY 字段（这些不能用 ?）
  → 但必须自己做校验（白名单）！
```

---

## 三、面试考点（16 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | MyBatis 和 Hibernate 区别？| MyBatis：**半自动 ORM（手写 SQL，自动做结果映射）**；Hibernate：全自动 ORM（自动生成 SQL）；MyBatis SQL 可控性高 → 国内更流行 |
| 2 | Executor 四种实现？| **SimpleExecutor（默认，每次创建 Statement）/ ReuseExecutor（复用 Statement）/ BatchExecutor（批量）/ CachingExecutor（装饰二级缓存）**|
| 3 | 一级缓存 vs 二级缓存？| 一级缓存：**同一个 SqlSession 内，同一个 SQL 只查一次**（默认开启，SqlSession 关闭清空）；二级缓存：跨 SqlSession 共享（要手动开启 + mapper 配 cache）|
| 4 | MappedStatement 作用？| **SQL 模板**（存 SQL 源码 + 参数映射 + 结果映射 + SQL 类型）；Configuration 全局 Map 缓存，key = namespace.method |
| 5 | #{} vs ${}？| **#{} = PreparedStatement 参数（防注入）**；**${} = 字符串直接替换（可能注入，只能动态表名/列名）** |
| 6 | 动态 SQL 标签？| **if / where / choose / foreach / trim / set** → XMLLanguageDriver 解析成 Ognl 表达式判断 |
| 7 | ResultMap 嵌套？| 一对多 Join → ResultMap 嵌套 ResultMap → DefaultResultSetHandler 自动按 id 合并成对象（association / collection）|
| 8 | SqlSessionFactory vs SqlSession？| SqlSessionFactory：**全局唯一，线程安全**（持有 Configuration）；SqlSession：**每次请求新建，不是线程安全** |
| 9 | 结果集处理核心类？| **DefaultResultSetHandler** → ResultSetMetaData + TypeHandler + 反射/构造器赋值 → 支持嵌套 ResultMap |
| 10 | 插件机制？| **MyBatis Interceptor 接口** → 拦截 Executor / StatementHandler / ParameterHandler / ResultSetHandler → 类似 AOP |
| 11 | 和 MyBatis-Plus 关系？| MP 是 **MyBatis 增强工具**（只增强不改变）→ BaseMapper 通用 CRUD 是 MP 加的；MyBatis 本身没有通用 CRUD |
| 12 | MyBatis 懒加载？| 关联查询（association/collection）**不是立即查，等 getter 时才查** → 解决 N+1 问题但有事务要求 |
| 13 | 二级缓存为什么默认不开？| **设计问题**：跨 SqlSession 共享 → 更新时要失效所有缓存（复杂）+ MyBatis 作者认为应该在业务层做（Redis）|
| 14 | TypeHandler 作用？| 类型转换器：**Java ↔ JDBC** → 自定义 TypeHandler 可以支持 JSON/UUID 等特殊类型 |
| 15 | XML vs 注解？| 注解：@Select/@Insert → 简单 SQL；XML → 动态 SQL / 复杂 Join / 多 ResultMap → **推荐 XML（动态 SQL 灵活）**|
| 16 | MyBatis 如何和 Spring 集成？| MapperFactoryBean（FactoryBean）→ SqlSessionTemplate（代理 SqlSession，线程安全）→ MapperScannerConfigurer（自动扫描 Mapper 注册）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：MyBatis 3.5.19  
**面试考点：16 题**
