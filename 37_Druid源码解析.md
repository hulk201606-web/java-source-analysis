# Alibaba Druid 源码深度解析（连接池 + 监控 + SQL 防火墙）

> **源码基线**：Alibaba Druid 1.2.x（最新稳定）  
> **核心仓库**：https://github.com/alibaba/druid  
> **面试权重**：⭐⭐⭐⭐（国内连接池监控高频）

---

## 一、Druid 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：集连接池 + 监控 + SQL 防火墙于一身的全能组件          │
│  阿里开源，国内应用广泛（淘宝/天猫/支付宝都在用）                │
│                                                               │
│  三大核心能力：                                                │
│  ① DruidDataSource（高性能连接池，比肩 HikariCP）              │
│  ② StatFilter（★ SQL 监控统计，慢 SQL 日志）                  │
│  ③ WallFilter（★ SQL 防火墙，防注入）                          │
│                                                               │
│  为什么国内比 HikariCP 更流行？                                │
│  → HikariCP 只有连接池功能（监控要自己搭 Prometheus/Grafana）   │
│  → Druid 有内置监控面板（StatViewServlet）+ SQL 防火墙         │
│  → 开箱即用！适合中小企业快速上线                               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Filter 链（★ Druid 的灵魂！和 Sentinel Slot 链设计相同）      │
│                                                               │
│  Druid 核心是 Filter 链！每一步 JDBC 操作都经过 Filter：        │
│                                                               │
│  Connection.getConnection()                                   │
│    → FilterChain（责任链）                                    │
│      → StatFilter（监控）                                     │
│      → WallFilter（SQL 防火墙）                                │
│      → LogFilter（日志）                                      │
│      → ExceptionFilter（异常处理）                             │
│      → 真实 JDBC 操作                                         │
│                                                               │
│  内置 Filter（按功能）：                                      │
│  ┌──────────┬──────────────┬───────────────────────────┐    │
│  │   Filter   │     功能       │       面试高频点           │    │
│  ├──────────┼──────────────┼───────────────────────────┤    │
│  │ StatFilter │ SQL 监控统计  │ ★★★★★ 慢 SQL、执行次数、并发 │    │
│  │ WallFilter │ SQL 防火墙   │ ★★★★★ AST 解析防注入         │    │
│  │ Slf4jLogFilter │ SQL 日志 │ 每次 SQL 打日志（调试用）      │    │
│  │ ExceptionFilter │ 异常处理 │ 统一异常包装               │    │
│  │ ContextFilter │ 上下文传递  │ 多租户 / TraceId 透传       │    │
│  └──────────┴──────────────┴───────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 StatFilter（★ SQL 监控核心）

```java
// ★ StatFilter 是 Druid 监控的源头！
//   所有 SQL 执行统计、慢 SQL 检测、并发监控都基于它
//
// FilterChain.addStatFilter() 流程：
//   ① PreparedStatement.execute()
//      → StatFilter.onStatementPrepare()    // SQL 解析 + 生成 SQL Stat
//      → 执行真实 JDBC 操作
//      → StatFilter.onStatementExecuteAfter()  // 统计执行时间
//      → 如果执行时间 > slowSqlMillis（默认 2000ms）
//        → 触发 slowSqlLogger（日志输出）
//
// ★ StatFilter 统计内容：
//   - Execute Count（总执行次数）
//   - Total Time（总耗时）
//   - Max Timespan（单次最长耗时）
//   - In Transaction Count（事务内执行次数）
//   - Error Count（错误次数）
//   - Concurrent Max（最大并发）
//   - 慢 SQL 列表（log-slow-sql + slow-sql-millis）
//
// ★ 监控数据存哪？
//   StatDataManager → ConcurrentHashMap<String, StatSql>
//   → 每个 SQL 语句（按 Template 归一化）一个 StatSql 对象
//   → 定期 /actuator/druid 查询（Spring Boot Actuator 集成）
```

### 2.2 WallFilter（★ SQL 防火墙核心）

```java
// ★ WallFilter 通过 **SQL AST 解析** 做防火墙（不是字符串匹配！比 MyBatis 插件强）
//
// 拦截内容：
//   ① SQL 注入：检测 OR/1=1、UNION SELECT、注释 -- 、分号 ;
//   ② 危险操作：DROP TABLE、DELETE *、TRUNCATE、ALTER
//   ③ 多语句：一次执行多条 SQL
//   ④ SELECT * 防御
//   ⑤ 参数数量限制
//
// 原理（★ AST 解析，面试高频）：
//   // Druid 内置 SQL Parser（Antlr4 实现，支持 MySQL/PG/Oracle/SQLServer）
//   SQLParserEngine parser = new SQLParserEngine(sql, dbType);
//   SQLStatement stmt = parser.parseStatement();  // AST 解析
//
//   WallCheckResult result = WallProvider.check(stmt);
//   // 遍历 AST 节点
//   // → 检测是否有 UNION / OR / DROP / 危险函数
//   // → 触发 WallException（阻止执行）
//
// ★ WallFilter vs MyBatis 插件防注入：
//   MyBatis 插件：字符串匹配（简单，但容易被绕过）
//   WallFilter：AST 解析（语义级，绕过难）
//
// ★ WallFilter 配置：
//   filter: wall
//   wall:
//     config:
//       multi-statement-allow: false  # 禁止多语句
//       select-where-alway-check: true  # SELECT 必须有 WHERE
//       block-allow: false  # 禁止 BLOCK
//       comment-allow: false  # 禁止 SQL 注释
//
// ★ 面试高频陷阱：
//   字符串拼接 SQL（#{name} 会自动防注入）WallFilter 也拦不住吗？
//   → 不！WallFilter 拦截是在 Druid 层（JDBC 级别），所有 SQL 都拦
//   → 不管你 MyBatis 还是 Hibernate，只要用 Druid 连接池就能防
```

### 2.3 StatViewServlet（内置监控面板）

```java
// ★ Druid 内置监控面板（开箱即用）
//
// 配置（Spring Boot）：
//   spring.datasource.druid.stat-view-servlet:
//     enabled: true
//     url-pattern: /druid/*        # 访问路径
//     login-username: admin        # 用户名
//     login-password: admin123     # 密码
//     allow: 127.0.0.1             # 允许的 IP
//     deny:                        # 禁止的 IP
//
// 页面功能：
//   - DataSource：数据源状态（活跃连接数、线程池、最大等待）
//   - SQL Monitor：SQL 执行统计 + 慢 SQL 列表
//   - Web URL：URI 监控（Spring 场景集成）
//   - Web Session：Session 监控
//   - Spring URI：Spring MVC URL 监控
//   - API：JSON 格式返回监控数据（方便 Prometheus 抓取）
//
// ★ 和 HikariCP 对比：
//   HikariCP：只有 Metrics（需要 Prometheus 配置）
//   Druid：**内置 HTML 面板 + JSON API** → 开箱即用
```

### 2.4 DruidDataSource 核心

```java
// ★ DruidDataSource 是连接池核心
//
// 启动流程：
//   DruidDataSource.getConnection()
//     → if (!inited) init()
//       → createConnectionIdGenerator()  // 生成 Connection ID
//       → createDataSourceStat()  // 数据源统计对象
//       → initCheck()  // 参数校验（initialSize / maxActive）
//       → // 初始化 initialSize 个连接
//       for (int i = 0; i < initialSize; i++) {
//           connectionList.add(createConnection());
//       }
//       → // 后台线程 Keep-Alive + 定时任务
//       CreateConnectionThread / DestroyTask / LogThread
//
// 连接获取：
//   getConnection()
//     → 先从 connectionList（空闲队列）拿
//     → 没有？→ 如果 activeCount < maxActive → createConnection()
//     → 还没有？→ 等待（maxWait 毫秒）
//     → 还没有？→ 抛 SQLException（连接耗尽）
//
// ★ 和 HikariCP ConcurrentBag 的区别：
//   Druid：LinkedBlockingQueue（简单但全局竞争）
//   HikariCP：ConcurrentBag（ThreadLocal + steal，无锁）
//   → HikariCP 性能高，但 Druid 监控功能强
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Druid 三大核心能力？| **DruidDataSource（连接池）+ StatFilter（SQL 监控）+ WallFilter（SQL 防火墙）** |
| 2 | WallFilter 防注入原理？| **SQL AST 解析**（Antlr4，支持 MySQL/PG/Oracle）→ 语义级检测 UNION/OR/DROP/多语句/注释 → 比字符串匹配防注入更彻底 |
| 3 | StatFilter 监控什么？| SQL 执行次数、总耗时、最大耗时、并发、错误次数、慢 SQL 日志（log-slow-sql + slow-sql-millis）|
| 4 | Druid vs HikariCP？| Druid：**监控 + 防火墙 + 国内流行 + 内置面板**；HikariCP：**性能最高 + Spring Boot 默认 + 代码精简** |
| 5 | StatViewServlet 是什么？| **Druid 内置监控面板**（/druid/*）→ DataSource/SQL/Web API 全部可视化；开箱即用 |
| 6 | Druid Filter 链设计？| **责任链模式**（FilterChain）→ 每个 JDBC 操作都经过 Filter 链；StatFilter → WallFilter → LogFilter 等 |
| 7 | initialSize / maxActive / maxIdle？| initialSize：**启动时预创建连接数**；maxActive：**最大活跃连接数**；maxIdle：**最大空闲连接数**；Spring Boot 默认 maxActive=8 |
| 8 | Druid 为什么国内流行？| **阿里开源 + 内置监控面板 + SQL 防火墙**；HikariCP 只有连接池功能（监控要自己搭）→ 中小企业开箱即用 |
| 9 | 慢 SQL 怎么配置？| filter: stat + log-slow-sql: true + slow-sql-millis: 2000 → 执行超过 2s 自动打印日志 |
| 10 | WallFilter 能拦 PreparedStatement 吗？| 可以！**AST 解析看参数化后的 SQL 结构** → 不管字符串拼接还是 ? 占位符 → 只要 SQL 结构危险就拦 |
| 11 | Druid 支持哪些数据库？| **MySQL / PostgreSQL / Oracle / SQLServer / SQLite / H2**（Druid SQL AST Parser 都支持）|
| 12 | Druid 线程模型？| CreateConnectionThread（后台创建连接）+ DestroyTask（后台销毁 idle 连接）+ LogThread（日志输出） |
| 13 | Druid 监控怎么集成 Prometheus？| Druid 有 **/actuator/druid API** → Prometheus HTTP 抓取 → Grafana 配置 Druid Dashboard |
| 14 | 多数据源配置？| 每个 DataSource 独立 DruidDataSource → Spring Boot 配多个 DataSource Bean + @Primary 指定默认 |
| 15 | Druid 有什么劣势？| **性能略逊于 HikariCP**（LinkedBlockingQueue vs ConcurrentBag）+ 代码量大（~15K 行 vs Hikari ~1400 行）+ 社区活跃度下降（阿里内部用更多但开源版更新慢）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Alibaba Druid 1.2.x  
**面试考点：15 题**
