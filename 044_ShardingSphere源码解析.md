# Apache ShardingSphere 源码深度解析（分库分表）

> **源码基线**：Apache ShardingSphere 5.5.x（最新稳定）  
> **核心仓库**：https://github.com/apache/shardingsphere  
> **面试权重**：⭐⭐⭐⭐⭐（国内分库分表高频）

---

## 一、ShardingSphere 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  三大产品形态                                                  │
│                                                               │
│  ① ShardingSphere-JDBC（★ 最常用！嵌入应用）                  │
│     → 一个增强版的 DataSource（Java 接口）                    │
│     → 零额外部署；只改配置 + DataSource Bean                  │
│     → 适合 Spring Boot 应用                                    │
│                                                               │
│  ② ShardingSphere-Proxy（独立代理服务）                       │
│     → 伪装成 MySQL/PostgreSQL Server                          │
│     → 所有业务应用连 Proxy（不感知分库分表）                   │
│     → 额外部署成本，但业务代码零侵入                           │
│                                                               │
│  ③ ShardingSphere-Sidecar（Service Mesh）                      │
│     → K8s 环境 Sidecar 代理                                   │
│     → 5.x 版本仍在实验阶段                                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  核心功能五件套                                                │
│                                                               │
│  ① 数据分片（★ 核心！）                                       │
│     → 库分片：ds${0..1}.t_order（分到 2 个库）                 │
│     → 表分片：ds0.t_order${0..1023}（每个库 1024 张表）       │
│     → 分片算法：inline / standard / complex / hint            │
│                                                               │
│  ② 读写分离                                                   │
│     → Master（写）+ N Slave（读）                              │
│     → ReadWriteSplittingDataSource                           │
│
│  ③ 分布式事务（Seata 集成）                                   │
│     → ShardingSphere-TransactionManager + Seata AT 模式      │
│
│  ④ 影子库压测                                                 │
│     → ShadowDataSource → 影子库（不影响生产数据）              │
│
│  ⑤ 数据加密                                                   │
│     → EncryptDataSource → 自动对敏感列加解密                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  SQL 执行核心链路（★ 面试高频！）                              │
│                                                               │
│  Application → ShardingSphere-JDBC DataSource                 │
│     → ShardingSpherePreparedStatement（增强 Statement）        │
│     → SQLParser（Antlr4 解析 SQL AST）                        │
│     → SQLRouter（★ 根据分片列 + 分片算法路由）                  │
│     → SQLRewriter（改写 SQL 适配真实表名）                     │
│     → Executor（多数据源并发执行）                             │
│     → ResultMerger（合并多结果集返回给应用）                   │
│     → Application 拿到合并后的 ResultSet                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 ShardingSphere-JDBC DataSource 配置

```yaml
# ★ 典型配置（Spring Boot + ShardingSphere-JDBC）
spring:
  shardingsphere:
    datasource:
      names: ds0, ds1                  # 两个物理库
      ds0:
        type: com.zaxxer.hikari.HikariDataSource
        driver-class-name: com.mysql.cj.jdbc.Driver
        jdbc-url: jdbc:mysql://localhost:3306/ds0
        username: root
      ds1:
        jdbc-url: jdbc:mysql://localhost:3306/ds1
    rules:
      sharding:
        tables:
          t_order:
            actual-data-nodes: ds$->{0..1}.t_order$->{0..1023}  # ★ 分片表达式！
            table-strategy:
              standard:
                sharding-column: order_id                      # 分片键
                sharding-algorithm-name: order-inline          # 分片算法
        sharding-algorithms:
          order-inline:
            type: INLINE                                       # 内联分片算法
            props:
              algorithm-expression: t_order_$->{order_id % 2048}  # 2048/1024=2个表/库
```

### 2.2 SQLParser（Antlr4 解析 AST）

```java
// ★ ShardingSphere 用 Antlr4 解析 SQL
//   支持 MySQL / PostgreSQL / Oracle / SQLServer
//
// 流程：
//   SQLParserEngine.parse(sql, databaseType)
//     → Antlr4 Lexer → Tokens
//     → Antlr4 Parser → SQLStatement（AST 根节点）
//     → 提取 SQL 类型：SELECT / INSERT / UPDATE / DELETE
//     → 提取表名：TableSegment（原始 t_order）
//     → 提取条件：WhereSegment（WHERE order_id = ? AND ...）
//     → 提取分片列值：order_id=123
//
// ★ 为什么要解析 SQL？
//   ShardingSphere 要根据分片列值路由到正确的物理库/表
//   → 不解析怎么知道 SQL 操作的是哪个逻辑表 + 分片列值是多少？
//
// ★ 如果分片列没出现在 WHERE 里？
//   → 无法精确路由（不知道要查哪个分片）
//   → 变成广播查询（查所有物理表 → 合并结果）→ 性能极差！
//   → 生产严禁这种情况！分片列必须出现在 WHERE 条件中
```

### 2.3 SQLRouter（★★★★★ 核心路由）

```java
// ★ SQLRouter 是分库分表的核心！
//
// 流程：
//   SQLRouterEngine.route(routeContext)
//     ↓
//   1. 提取路由条件（TableShardingCondition + RangeShardingCondition）
//      → 从 SQL AST 的 WHERE 条件里提取分片列值
//      → = 123 → exact condition（精确路由）
//      → BETWEEN 100 AND 200 → range condition（范围路由）
//      → IN (1, 2, 3) → 多个 exact conditions
//
//   2. 分片算法计算（★ 根据 sharding-column 值路由）
//      INLINE 算法：order_id=123 → 123 % 2048 = 123 → t_order_123
//      HASH_MOD：hash(user_id) % 2 → ds0 或 ds1
//      RANGE：age BETWEEN 18 AND 25 → 路由到青少年库
//
//   3. 生成 RouteTableUnit
//      → 逻辑表 t_order → 物理表 t_order_123
//      → 逻辑库 ds → 物理库 ds0 或 ds1
//      → 路由单元（DataSource + 真实表名）
//
//   ★ 四种分片策略：
//   ① INLINE（内联表达式，最常用）：algorithmExpression: ds${user_id % 2}
//   ② STANDARD（标准策略，Java 类实现）：precise / range 两个算法
//   ③ COMPLEX（复合分片，多分片键）
//   ④ HINT（强制路由，代码指定分片值）→ HintManager.setDatabaseShardingValue()
//
//   ★ 经典 Hash 分片 vs 范围分片：
//   Hash：均匀分布但范围查询要查所有分片
//   Range：适合范围查询但数据分布不均
```

### 2.4 SQLRewriter（改写 SQL）

```java
// ★ SQLRewriter 把逻辑表名改写成物理表名
//
// 原始 SQL（逻辑表）：
//   SELECT * FROM t_order WHERE order_id = 123;
//
// 改写后（物理表）：
//   SELECT * FROM t_order_123 WHERE order_id = 123;
//
// 改写内容：
//   表名替换（t_order → t_order_123）
//   参数值调整（如果分片规则需要）
//   分页改写（LIMIT offset, size 要做偏移修正）
//
// ★ 分页改写坑点：
//   SELECT * FROM t_order LIMIT 1000, 10;
//   分 2 个物理表 → 每个表都要查 LIMIT 1000, 10？
//   不是！要查每个表 LIMIT 0, 1010（先拉前 1010 条到内存）
//   → 内存排序 → 取 1000~1010 区间
//   → 这就是为什么大数据量分页要谨慎！
```

### 2.5 Executor + ResultMerger

```java
// ★ Executor 多数据源并发执行
//   路由到 N 个物理表 → 用线程池并发执行 N 条 SQL
//
// ★ ResultMerger 合并多结果集
//   多表返回结果 → 合并 + 排序 + 分页修正
//   合并策略：
//     StreamMergedResult（有序流合并，类似归并排序）
//     GroupByMergedResult（GROUP BY 合并）
//     MemoryMergedResult（内存全量加载后合并，简单但耗内存）
//
// ★ 分布式分页经典问题：
//   SELECT * FROM t_order ORDER BY create_time DESC LIMIT 100, 10;
//   → 每个物理表都要查前 110 条（不是 10 条！）
//   → 合并排序 → 取第 100~110 条
//   → 分页越深越慢（LIMIT 10000, 10 → 每个表查 10010 条）
//   → 生产建议：**不用深分页**（改用游标/Key 分页）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | ShardingSphere 三种形态？| **ShardingSphere-JDBC（★ 最常用，嵌入应用，零额外部署）/ Proxy（独立代理服务）/ Sidecar（K8s Service Mesh）** |
| 2 | 核心功能五件套？| **数据分片 / 读写分离 / 分布式事务（Seata）/ 影子库压测 / 数据加密** |
| 3 | SQL 执行核心链路？| **SQLParser（Antlr4 解析 AST）→ SQLRouter（分片算法路由）→ SQLRewriter（改表名）→ Executor（并发执行）→ ResultMerger（合并结果）** |
| 4 | 四种分片算法？| **INLINE（内联表达式，最常用）/ STANDARD（Java 类实现）/ COMPLEX（多分片键）/ HINT（代码强制路由）** |
| 5 | 分片列必须在 WHERE 里吗？| **是的！** 否则无法精确路由 → 广播查询所有分片 → 性能灾难；生产严禁分片列缺失的查询 |
| 6 | 分布式分页坑？| SELECT * LIMIT 100, 10 → **每个物理表要查前 110 条** → 内存合并 → 分页越深越慢；**改用游标/Key 分页** |
| 7 | INLINE 分片表达式？| `actual-data-nodes: ds$->{0..1}.t_order$->{0..1023}` + `algorithm-expression: t_order_$->{order_id % 2048}` |
| 8 | Hint 分片？| **ThreadLocal 强制路由** → HintManager.setDatabaseShardingValue(123) → 适合分片列不存在 SQL 里的场景 |
| 9 | Hash vs Range 分片？| Hash（hash(user_id) % 2）→ 均匀但范围查询要查所有分片；Range（age BETWEEN）→ 范围查询快但数据分布不均 |
| 10 | 读写分离？| **Master（写）+ N Slave（读）** → ReadWriteSplittingDataSource → Master 写，Slave 读（延迟问题要注意）|
| 11 | 分布式事务？| 集成 Seata → ShardingSphere 管理事务上下文 + Seata AT 模式保证跨库事务 |
| 12 | 和 MyBatis-Plus 配合？| MP 的 SQL 最终是 JDBC 执行 → ShardingSphere-JDBC 是 DataSource 层 → **完全兼容，透明生效** |
| 13 | 影子库压测？| ShadowDataSource → 根据标记（HTTP Header 或 SQL Comment）路由到影子库 → 不影响生产数据 |
| 14 | 和 TDDL（阿里）对比？| ShardingSphere：Apache 开源 + Java 生态完善 + 社区活跃；TDDL：阿里内部 → 功能类似但不开源 |
| 15 | 性能影响？| SQLParser（Antlr4 约 1ms）+ SQLRouter（分片算法 O(1)）→ 单条 SQL 额外开销 < 5%；并发执行反而更快（多库并行）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache ShardingSphere 5.5.x  
**面试考点：15 题**
