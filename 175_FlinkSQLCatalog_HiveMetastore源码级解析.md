# Flink SQL Catalog + Hive Metastore 集成源码级深度解析

> **源码基线**：Apache Flink 1.19+/1.20+（Java/Scala）  
> **核心位置**：flink-table/flink-table-common + flink-connectors/flink-connector-hive  
> **面试权重**：⭐⭐⭐⭐⭐（Flink 数仓必备，Hive 元数据集成高频）

---

## 一、Flink SQL Catalog 体系全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink SQL Catalog = 元数据管理核心                        │
│                                                               │
│  Catalog 的作用：                                              │
│   统一管理 Table / Database / Function 的元数据             │
│   类似 Hive Metastore / Iceberg Catalog / JDBC Schema       │
│   Flink SQL 解析时通过 Catalog 查找表/函数/类型            │
│                                                               │
│  ★ Flink Catalog 层级体系（源码级）：                         │
│  ┌─────────────────────────────────────────────────┐         │
│  │ Catalog（顶层元数据抽象接口）                     │         │
│  │  flink-table-api-java/.../catalog/Catalog.java   │         │
│  │  ├── listDatabases()  / getDatabase()            │         │
│  │  ├── listTables()     / getTable()               │         │
│  │  ├── listFunctions()  / getFunction()            │         │
│  │  ├── createTable()    / dropTable() / alterTable()│        │
│  │  └── getFactory() → CatalogFactory SPI           │         │
│  └─────────────────────────────────────────────────┘         │
│              ↑ implements                                     │
│  ┌─────────────────────────────────────────────────┐         │
│  │ Catalog 实现类（按存储介质分）：                  │         │
│  │                                                  │         │
│  │  ① GenericInMemoryCatalog        内存（默认）    │         │
│  │  ② JdbcCatalog                   JDBC 元数据库  │         │
│  │  ③ ★ HiveCatalog                 Hive Metastore │         │
│  │  ④ IcebergCatalog / PaimonCatalog 湖仓元数据    │         │
│  │  ⑤ GlueCatalog                   AWS Glue       │         │
│  │  ⑥ FileSystemCatalog             FS 元数据      │         │
│  └─────────────────────────────────────────────────┘         │
│                                                               │
│  Catalog 与 Database 关系：                                    │
│   Flink SQL 的 qualifiedName = catalog.db.table             │
│   默认 catalog = default_catalog                              │
│   默认 database = default_database                            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Catalog SPI + Factory 源码级

```java
// ★ Catalog SPI 机制（源码级）
//
// flink-table/flink-table-common/src/main/java/org/apache/flink/table/catalog/Catalog.java
// flink-table/.../catalog/factory/CatalogFactory.java
// flink-table/.../catalog/factory/CatalogFactoryUtil.java
//
// // Catalog 接口核心方法：
// public interface Catalog {
//     // Database
//     void createDatabase(String name, Database database, boolean ignoreIfExists);
//     Database getDatabase(String name);
//     List<String> listDatabases();
//
//     // Table / View
//     void createTable(ObjectIdentifier tablePath, CatalogBaseTable table, boolean ignoreIfExists);
//     CatalogBaseTable getTable(ObjectIdentifier tablePath);
//     List<String> listTables(String databaseName);
//     void dropTable(ObjectIdentifier tablePath, boolean ignoreIfNotExists);
//     void alterTable(ObjectIdentifier tablePath, CatalogBaseTable newTable, ...);
//
//     // Function（UDF/UDTF/UDAF）
//     void createFunction(ObjectIdentifier functionPath, CatalogFunction function, ...);
//     CatalogFunction getFunction(ObjectIdentifier functionPath);
//     List<String> listFunctions(String databaseName);
//
//     // Partition（Hive 表分区管理）
//     CatalogPartition getPartition(ObjectIdentifier tablePath, CatalogPartitionSpec partitionSpec);
//     List<CatalogPartitionSpec> listPartitions(ObjectIdentifier tablePath);
//
//     // ★ Factory：Catalog 自身如何创建
//     Optional<CatalogFactory> getFactory();
// }
//
// // ★ CatalogFactory SPI（创建 Catalog 的工厂，面试核心）：
// // 每个 Catalog 实现类都配一个 Factory 类
// // 通过 Java SPI（META-INF/services/org.apache.flink.table.catalog.CatalogFactory）发现
// public interface CatalogFactory extends Factory {
//     Catalog createCatalog(Context context);
// }
//
// // CatalogFactoryUtil.createCatalog():
// // 1. 扫描所有 CatalogFactory SPI 实现
// // 2. 按 factoryIdentifier 匹配（例如 "hive"、"jdbc"、"memory"）
// // 3. 调用 factory.createCatalog(Context) 创建实例
// //
// // SQL: CREATE CATALOG hive WITH ('type' = 'hive', ...)
// //   → CatalogManager → CatalogFactoryUtil → HiveCatalogFactory → HiveCatalog
//
```

---

## 三、HiveCatalog 核心源码级

### 3.1 HiveCatalogFactory 配置项

```java
// ★ HiveCatalogFactory（源码级，面试高频）
//
// flink-connectors/flink-connector-hive/src/main/java/org/apache/flink/table/catalog/hive/factory/HiveCatalogFactory.java
//
// public class HiveCatalogFactory implements CatalogFactory {
//
//     @Override
//     public Catalog createCatalog(Context context) {
//         // ★ 读取所有 Hive Catalog 配置项
//         //    关键配置项清单（面试必背，含生产坑点）：
//         //
//         // ① hive-conf-dir: Hive 配置目录（hive-site.xml 所在）
//         //    Flink 会加载 hive-site.xml 拿 metastore URI
//         //
//         // ② hive.metastore.uris: 直接指定 HMS 地址（hive-site.xml 里也有）
//         //    格式: thrift://host:port,thrift://host2:port
//         //
//         // ③ hive-database: 要作为默认 DB 的 Hive DB（可选）
//         //
//         // ④ default-database: Flink Catalog 里的默认 DB（通常同 hive-database）
//         //
//         // ⑤ hive-version: ★ 显式指定 Hive 版本
//         //    Flink 内置了 Hive 1.2.1 / 2.2.0 / 2.3.6 / 3.1.x 等客户端
//         //    如果 Metastore 版本不同 → 需要加额外依赖
//         //
//         // ⑥ hive.stats.fetch.column.stats: ★ 生产坑点！默认 true
//         //    true 时 HiveCatalog 在 getTable() 时会拉列统计信息
//         //    Hive 4.x Metastore API 和 Flink 内置客户端不兼容 → NoSuchMethodError！
//         //    解决方案：SET 'hive.stats.fetch.column.stats' = 'false'
//         //    （这是生产最常见的 HiveCatalog NoSuchMethodError 兜底方案）
//         //
//         // ⑦ hive.table-factories: 自定义 TableFactory（少用）
//         //
//         // ⑧ hive.version.auto-discovery: 自动探测 Hive 版本
//         //    Flink 1.15+ 支持；自动从 HMS thrift 接口判断
//         //
//         // 构造 HiveCatalog：
//         String hiveConfDir = context.getOptions().get(HIVE_CONF_DIR);
//         String hiveVersion = context.getOptions().get(HIVE_VERSION);
//         // ...
//         return new HiveCatalog(
//             catalogName,
//             defaultDatabase,
//             hiveConfDir,
//             hiveVersion,
//             classLoader,  // ★ 类加载器隔离（关键）
//             hiveConf
//         );
//     }
// }
//
```

### 3.2 HiveCatalog 类加载器与依赖冲突

```java
// ★ HiveCatalog 类加载器隔离（源码级，面试核心）
//
// HiveCatalog 构造函数关键逻辑：
//
// public class HiveCatalog extends AbstractCatalog {
//
//     private final ClassLoader hiveClassLoader;
//
//     public HiveCatalog(String catalogName, String defaultDatabase,
//                         String hiveConfDir, String hiveVersion,
//                         ClassLoader parentClassLoader, HiveConf hiveConf) {
//         // ★ 创建独立的类加载器（Child First）
//         //    Hive 客户端（org.apache.hadoop.hive.*）版本必须隔离
//         //    避免和 Flink 自身的 Hive 类冲突
//         this.hiveClassLoader = HiveCatalogUtil.createHiveClassLoader(
//             parentClassLoader, hiveConfDir, hiveVersion);
//
//         // ★ 初始化 HiveConf + IMetaStoreClient
//         //    在 hiveClassLoader 下反射创建 HMS 客户端
//         //    Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStoreClient", true, hiveClassLoader)
//         //
//         // ★ 这就是为什么版本不匹配会报 NoSuchMethodError：
//         //    hiveClassLoader 加载的 HiveMetaStoreClient 和 Metastore 服务端版本不一致
//         //    方法签名不同 → 运行时找不到
//     }
// }
//
// ★ 生产最常见的 Hive 依赖冲突（Experience 提炼）：
// ┌──────────────────────────────────────────────────────────────┐
// │ 冲突 1：flink-connector-hive vs flink-sql-connector-hive    │
// │   同功能两种打包形态同时在 classpath                       │
// │   → 类加载混乱（NoSuchMethodError、ClassCastException）    │
// │   → 解决：只保留一种（推荐 sql-connector-hive 带依赖）     │
// │                                                              │
// │ 冲突 2：Flink 内置 Hive vs 用户自带 Hive jars               │
// │   Flink 内置 Hive 1.2.1 client，用户在集群 lib 加了 Hive 3.x│
// │   → HiveMetaStoreClient 版本冲突                           │
// │   → 解决：hive.version 显式指定 + 用 ClassLoader 隔离       │
// │                                                              │
// │ 冲突 3：Hadoop 版本与 Hive 版本不匹配                        │
// │   Flink 绑定的 Hadoop 版本 vs 集群 Hadoop 版本              │
// │   → HDFS API 不兼容                                        │
// │   → 解决：Flink 集群 Hadoop 版本与 Hive/HDFS 统一           │
// └──────────────────────────────────────────────────────────────┘
//
```

### 3.3 HiveCatalog 与 Metastore 交互

```java
// ★ HiveCatalog 与 Hive Metastore 交互源码级
//
// flink-connector-hive/.../HiveCatalog.java
// flink-connector-hive/.../HiveCatalogUtil.java
//
// // HiveCatalog.getTable() 全链路：
// public CatalogBaseTable getTable(ObjectIdentifier tablePath) {
//     try {
//         // ① 获取 IMetaStoreClient（反射调用，在 hiveClassLoader 下）
//         IMetaStoreClient client = getHiveMetaStoreClient();
//         //    client = HiveMetaStoreClient(hiveConf)
//         //    → thrift 连接 Metastore（地址从 hive-site.xml / hive.metastore.uris 取）
//
//         // ② 调 HMS Thrift API：getTable(dbName, tableName)
//         org.apache.hadoop.hive.metastore.api.Table hiveTable =
//             client.getTable(dbName, tableName);
//
//         // ③ 把 Hive Metastore Table 转成 Flink CatalogTable
//         //    HiveCatalogUtil.convertHiveTableToCatalogTable(hiveTable)
//         //    - Hive Table → CatalogTable
//         //    - Hive PartitionKeys → CatalogTable.partitionKeys
//         //    - SerDe/StorageHandler → CatalogTable.storage（connector='hive'）
//         //    - Comment → CatalogTable.comment
//         CatalogTable flinkTable = convertToCatalogTable(hiveTable);
//
//         // ④ ★ 生产坑点：hive.stats.fetch.column.stats=true
//         //    HiveCatalog 默认还会调 getTableColumnStatistics
//         //    Hive 4.x Metastore 的 getTableColumnStatistics API 签名变了
//         //    → NoSuchMethodError！
//         if (fetchColumnStats) {
//             ColumnStatistics stats = client.getTableColumnStatistics(...);
//             //    ← 这里如果 Flink 内置 Hive 客户端版本 < Metastore 版本
//             //       而 Metastore API 升级了 → NoSuchMethodError
//             flinkTable = flinkTable.copy(... stats ...);
//         }
//
//         return flinkTable;
//     } catch (TException e) {
//         throw new CatalogException("Failed to get table from Hive Metastore", e);
//     }
// }
//
// // ★ getTableColumnStatistics 兜底方案：
// // CREATE CATALOG hive WITH (
// //   'type' = 'hive',
// //   'hive-conf-dir' = '/etc/hive/conf',
// //   'hive.stats.fetch.column.stats' = 'false'
// // );
// // → 跳过统计拉取，避免 NoSuchMethodError
//
```

---

## 四、Flink 与 Hive 版本兼容矩阵

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink ↔ Hive 版本兼容矩阵（面试必背，生产血泪总结）       │
│                                                               │
│  ┌──────────┬──────────────────────────────────────────┐   │
│  │ Flink      │ 兼容的 Hive Metastore 版本               │   │
│  ├──────────┼──────────────────────────────────────────┤   │
│  │ 1.11       │ 1.2.1（内置）、2.2.0、2.3.6              │   │
│  │ 1.12-1.16  │ 1.2.1、2.2.0、2.3.6、★ 3.1.2 等          │   │
│  │ 1.17-1.18  │ 1.2.1、2.3.6、★ 3.1.2、3.1.3              │   │
│  │ 1.19-1.20  │ 1.2.1、2.3.6、★ 3.1.x、★ 4.0.0（部分）    │   │
│  └──────────┴──────────────────────────────────────────┘   │
│                                                               │
│  ★ 绝对避坑（Experience 提炼）：                              │
│   ❌ Flink 1.17 不支持 Hive 4.x Metastore（API 完全不兼容）  │
│   ❌ Flink 1.17 + Hive 4.1.0 一定 NoSuchMethodError          │
│   ✅ Flink 1.17 + Hive 3.1.2 没问题（hive.version='3.1.2'）   │
│   ✅ Flink 1.20 + Hive 4.x 有限支持（仍建议 fetch.column.stats=false）│
│                                                               │
│  ★ 自动探测 hive.version：                                    │
│   hive.version.auto-discovery = true（Flink 1.15+ 支持）     │
│   HiveCatalog 连 Metastore 时调 getVersion() 判断版本        │
│                                                               │
│  ★ 版本切换对应用的影响：                                     │
│   Flink SQL 本身不变（ANSI SQL 子集）                       │
│   但：Hive SerDe 类、Hive UDF 类、Hive PartitionSpec 可能变 │
│   版本升级要测 HiveCatalog.getTable + executeDDL 的兼容性   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、CatalogManager + 多 Catalog 架构

```java
// ★ CatalogManager（源码级，核心调度器）
//
// flink-table/.../catalog/CatalogManager.java
//
// // 全局唯一的 Catalog 注册表
// // 通过 StreamExecutionEnvironment.getCatalogManager() 拿到
//
// public class CatalogManager {
//     // 内置 Catalog
//     private final Catalog genericInMemoryCatalog;  // default_catalog（内存）
//
//     // 用户注册的 Catalog Map
//     private final Map<String, Catalog> catalogs = new HashMap<>();
//     private final Map<String, CatalogSpec> catalogSpecs = new HashMap<>();
//
//     // 当前选中的 Catalog/Database
//     private String currentCatalog = "default_catalog";
//     private String currentDatabase = "default_database";
//
//     // ★ 注册新 Catalog（SQL / Table API）
//     public void registerCatalog(String catalogName, Catalog catalog) {
//         catalogs.put(catalogName, catalog);
//         // 同时加载 catalog.spec（持久化配置）
//     }
//
//     // DDL: CREATE CATALOG hive WITH (...)
//     //   → CatalogManager.registerCatalog("hive", HiveCatalog实例)
//     //   → SQL: USE CATALOG hive → currentCatalog="hive"
//     //
//     // 多 Catalog 共存：
//     //   default_catalog（内存）+ hive（HMS）+ iceberg（湖仓）
//     //   SQL 跨 Catalog：SELECT * FROM hive.db1.table1 JOIN iceberg.db2.table2
//
//     // ★ 临时表 vs 永久表（面试高频）：
//     //   临时表：Session 级别，存在内存（genericInMemoryCatalog）
//     //     CREATE TEMPORARY TABLE t AS ...
//     //     CatalogManager.getTable() 先查临时表 Map
//     //
//     //   永久表：存在已注册的 Catalog 下（HiveCatalog/IcebergCatalog 等）
//     //     CREATE CATALOG hive ...; CREATE TABLE hive.db.t (...)
//     //     CatalogManager.getTable() 按 catalogName 分发到对应 Catalog
//
//     // getTable(ObjectIdentifier):
//     //   if catalog.isTemporaryTable(identifier) → 返回临时表
//     //   else → catalog.getTable(identifier)  // 分发到具体 Catalog 实现
// }
//
```

---

## 六、HiveCatalog vs IcebergCatalog vs JDBCCatalog 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比表：三大 Catalog 实现                        │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐  │
│  │ 维度      │ HiveCatalog    │ IcebergCatalog │ JDBCCatalog   │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 元存储     │ Hive Metastore │ HMS/Nessie/Glue│ JDBC Database │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 表格式     │ Hive File Format│ ★ Iceberg     │ 关系表         │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 分区管理   │ ✅ 原生        │ ★ 隐式分区    │ ❌            │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ Schema 演进│ 有限           │ ★ 完整（加/删/改列）│ 有限        │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 快照/时间旅行│ ❌           │ ★ 完整 Snapshot│ ❌            │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ ACID       │ Hive 2.0+（有限）│ ★ 完整 Snapshot│ 依赖 DB       │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 与 Hive 兼容│ ★ 原生        │ 需桥接（Iceberg Hive Catalog）│ ❌│  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 适用场景   │ Hive 数仓     │ 湖仓新架构    │ 关系库元数据   │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ 面试高频   │ ★★★★★        │ ★★★★        │ ★★          │  │
│  └──────────┴──────────────┴──────────────┴──────────────┘  │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   已有 Hive 数仓 → **HiveCatalog**（成本最低）                │
│   湖仓新架构（Spark+Flink 共用）→ **IcebergCatalog**         │
│   统一查询 MySQL/Postgres → **JDBCCatalog**（表透传查询）    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 七、面试考点（28 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Flink Catalog 是什么？| 统一管理 Table/Database/Function 元数据的抽象层；类 Hive Metastore |
| 2 | Catalog 接口核心方法？| listDatabases/getDatabase；listTables/getTable/createTable；listFunctions/getFunction |
| 3 | Catalog SPI 怎么工作的？| 每个 Catalog 配 CatalogFactory 实现 → META-INF/services SPI 发现 → FactoryUtil.createCatalog() |
| 4 | Flink 内置哪些 Catalog 实现？| **GenericInMemoryCatalog（内存默认）+ HiveCatalog + JDBCCatalog + IcebergCatalog + PaimonCatalog + GlueCatalog** |
| 5 | HiveCatalog 怎么创建 HMS 连接？| ClassLoader 隔离加载 HiveMetaStoreClient → thrift 连 HMS（地址从 hive-site.xml / hive.metastore.uris）|
| 6 | HiveCatalogFactory 关键配置？| **hive-conf-dir + hive.metastore.uris + hive-version + hive.stats.fetch.column.stats（生产坑）** |
| 7 | ★ hive.stats.fetch.column.stats=false 是什么？| 拉列统计信息时 Hive 4.x Metastore API 和 Flink 内置客户端不兼容 → NoSuchMethodError；**关掉就能绕开** |
| 8 | 生产最常见的 HiveCatalog NoSuchMethodError 原因？| **Flink 内置 Hive 客户端版本 ≠ Hive Metastore 版本**；或 classpath 同时有 connector-hive 和 sql-connector-hive |
| 9 | flink-connector-hive vs flink-sql-connector-hive 区别？| 同功能两种打包形态；sql-connector-hive 自带所有依赖（fat jar）；**不要同时放，会类加载冲突** |
| 10 | Flink 1.17 支持 Hive 4.x 吗？| **❌ 不支持**！Flink 1.17 最高 Hive 3.1.x；Flink 1.20 有限支持 Hive 4.x |
| 11 | HiveCatalog 怎么隔离 Hive 客户端版本？| **独立 Child-First ClassLoader**（hiveClassLoader）反射创建 HiveMetaStoreClient |
| 12 | CatalogManager 做什么？| 全局 Catalog 注册表；多 Catalog 调度；临时表 vs 永久表管理；当前 catalog/db 切换 |
| 13 | 临时表 vs 永久表？| 临时表 Session 级（内存 catalog）；永久表存在具体 Catalog（Hive/Iceberg）；getTable 先查临时表再分发 |
| 14 | 跨 Catalog 查询？| SELECT * FROM hive.db1.t1 JOIN iceberg.db2.t2 → CatalogManager 分发到各自 Catalog.getTable() |
| 15 | hive.version.auto-discovery？| Flink 1.15+ 支持；HiveCatalog 连接 Metastore 时自动探测版本 |
| 16 | HiveCatalog.getTable() 全链路？| IMetaStoreClient → getTable(db, name) Thrift 调用 → Hive Table → CatalogTable 转换 → 可选 getTableColumnStatistics |
| 17 | IcebergCatalog vs HiveCatalog？| Iceberg 有完整 Snapshot/Time Travel/Schema Evolutions；Hive 原生但功能有限 |
| 18 | JDBCCatalog 做什么？| 把 MySQL/Postgres 等 JDBC 表透传成 Flink 表元数据；支持 DDL（取决于底层 DB） |
| 19 | Catalog 持久化？| HiveCatalog 元数据存在 Hive Metastore（MySQL/DBS）；Flink 自身不持久化 |
| 20 | Catalog 函数管理？| Flink UDF/UDTF/UDAF 可注册到 Catalog；HiveCatalog 还能加载 Hive Function（ClassLoader 隔离） |
| 21 | 面试最高频考点排序？| **1. hive.stats.fetch.column.stats 坑 2. Flink↔Hive 版本兼容矩阵 3. ClassLoader 隔离 4. Catalog SPI 工厂模式 5. CatalogManager 多 Catalog** |
| 22 | 学习路径？| Catalog 接口 → CatalogFactory SPI → HiveCatalog → CatalogManager |
| 23 | 面试常见追问？| Flink SQL 默认用哪个 Catalog？→ default_catalog（GenericInMemoryCatalog），临时 session 用 |
| 24 | 面试常见追问？| HiveCatalog 和 Flink connector='hive' 什么关系？→ Catalog 负责元数据管理，connector='hive' 负责数据读写（HDFS 上的 Hive FileFormat）|
| 25 | 面试常见追问？| Flink 1.20 + Hive 4.1.0 能跑吗？→ 有限支持，必须 hive.stats.fetch.column.stats=false + 检查具体 API |
| 26 | 面试陷阱？| 说 Flink 1.17 支持 Hive 4.x → ❌ 一定 NoSuchMethodError；说 connector-hive 和 sql-connector-hive 可以共存 → ❌ 类加载冲突 |
| 27 | 依赖治理策略？| **集群提供 lib → 应用用 provided，作业 jar 不 shade 同类库；作业自带 → 集群 lib 移除同名连接器** |
| 28 | 选型建议？| 已有 Hive → HiveCatalog；湖仓架构 → IcebergCatalog；关系库透传 → JDBCCatalog |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：flink-table/catalog/Catalog.java + HiveCatalog.java + HiveCatalogFactory.java + CatalogManager.java  
**面试考点：28 题**  
**差异化点**：Catalog SPI 工厂模式、ClassLoader 隔离机制、Flink↔Hive 版本兼容矩阵、hive.stats.fetch.column.stats 生产坑点、HiveCatalog vs IcebergCatalog vs JDBCCatalog 三选一对比
