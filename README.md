# Java / JDK 源码分析知识库 · 阅读顺序与知识地图

> **239 篇**笔记（219 篇源码解析 + 20 篇专项面试题库）+ **JDK 1.8.0_451 完整源码副本**（`src/`，7,731 个 Java 文件）。
> 文件名编号即推荐阅读顺序；所有源码锚点使用仓库相对路径（如 `src/java/util/HashMap.java:L183`）。

---

## 一、仓库构成

| 路径 / 区间 | 内容 | 篇数 |
|---|---|---|
| [README.md](README.md) | 本知识地图（入口） | 1 |
| [001](001_JDK核心源码深度解析_总览索引.md)–[003](003_JDK核心源码注释总览.md) | JDK 总览索引、模块速查、注释总览 | 3 |
| 004–015 | JDK 核心：集合、并发、IO/NIO、JVM、反射、AQS、ForkJoin、跳表、Stream、类加载、Lambda、Unsafe | 12 |
| 016–020 | Spring Framework / Boot：IoC、AOP、事务、扩展点、MVC | 5 |
| 021–033 | Spring Cloud：Nacos、Seata、OpenFeign、Sentinel、Gateway、Ribbon、Bus、Sleuth、Config、Resilience4j、LoadBalancer | 13 |
| 034–084 | 中间件与基础框架：MyBatis、Dubbo、RocketMQ、Kafka、Redis、Netty、MySQL、TiDB、Flink、K8s、ES、数据湖等 | 51 |
| 085–107 | 分布式理论与工程：分布式事务、Nacos Raft、TiKV/PD、CAP、分布式 ID、Envoy、一致性算法 | 23 |
| 108–217 | 源码级深度专题：MySQL 18 篇、TiKV/TiDB 23 篇、Kafka 12 篇、Flink 11 篇等 | 110 |
| 218–219 | 两份阶段性总结报告（历史快照，统计口径以本文为准） | 2 |
| 220–239 | 专项面试题库：Netty、Dubbo、RocketMQ、Kafka、JVM、Spring、MySQL、Redis、分布式、一致性、Spring Cloud、ES、Flink、TiDB/TiKV、K8s、系统设计 | 20 |
| `src/` | JDK 1.8 源码副本（`java/ javax/ org/ com/sun/ launcher/`），未修改 | — |

---

## 二、快速入口

| 目标 | 入口文档 |
|---|---|
| 先看全局 | [001_JDK核心源码深度解析_总览索引.md](001_JDK核心源码深度解析_总览索引.md) |
| JDK 集合 | [004_集合框架源码解析.md](004_集合框架源码解析.md) |
| JDK 并发 | [005_并发基础源码解析.md](005_并发基础源码解析.md) → [009_AQS共享工具类源码解析.md](009_AQS共享工具类源码解析.md) |
| ForkJoin / 并行流 | [010_ForkJoinPool分治框架源码解析.md](010_ForkJoinPool分治框架源码解析.md) |
| Spring 启动与 IoC | [016_Spring核心源码解析.md](016_Spring核心源码解析.md) |
| Spring AOP / 事务 | [017_SpringAOP源码解析.md](017_SpringAOP源码解析.md) → [018_Spring事务管理源码解析.md](018_Spring事务管理源码解析.md) |
| 消息中间件 | [038_Kafka源码解析.md](038_Kafka源码解析.md) |
| 缓存 | [040_Redis源码解析.md](040_Redis源码解析.md) |
| 关系型数据库 | [045_MySQL源码解析.md](045_MySQL源码解析.md) |
| 分布式数据库 | [050_TiDB源码解析.md](050_TiDB源码解析.md) |
| 流计算 | [051_Flink源码解析.md](051_Flink源码解析.md) |
| OLAP | [058_ClickHouse源码解析.md](058_ClickHouse源码解析.md) |
| 分布式理论 | [085_分布式事务原理源码解析.md](085_分布式事务原理源码解析.md) → [092_分布式一致性算法全景对比.md](092_分布式一致性算法全景对比.md) |
| 专项题库（中间件） | [220_Netty面试题库.md](220_Netty面试题库.md) · [221_Dubbo面试题库.md](221_Dubbo面试题库.md) · [222_RocketMQ面试题库.md](222_RocketMQ面试题库.md) · [223_Kafka面试题库.md](223_Kafka面试题库.md) |
| 专项题库（JVM/语言） | [224_JVM底层C++面试题库.md](224_JVM底层C++面试题库.md) · [225_JavaSecurity与Crypto面试题库.md](225_JavaSecurity与Crypto面试题库.md) · [226_JIT编译器面试题库.md](226_JIT编译器面试题库.md) · [227_Java9+新特性面试题库.md](227_Java9+新特性面试题库.md) |
| 专项题库（核心/数据） | [228_Java核心与并发面试题库.md](228_Java核心与并发面试题库.md) · [229_Spring体系面试题库.md](229_Spring体系面试题库.md) · [230_MySQL面试题库.md](230_MySQL面试题库.md) · [231_Redis面试题库.md](231_Redis面试题库.md) |
| 专项题库（架构/一致性） | [232_分布式与微服务面试题库.md](232_分布式与微服务面试题库.md) · [233_高并发数据一致性面试题库.md](233_高并发数据一致性面试题库.md) |
| 专项题库（组件/数据） | [234_SpringCloud组件面试题库.md](234_SpringCloud组件面试题库.md) · [235_Elasticsearch面试题库.md](235_Elasticsearch面试题库.md) · [236_Flink面试题库.md](236_Flink面试题库.md) · [237_TiDB与TiKV面试题库.md](237_TiDB与TiKV面试题库.md) |
| 专项题库（云原生/设计） | [238_Kubernetes与云原生面试题库.md](238_Kubernetes与云原生面试题库.md) · [239_系统设计与场景面试题库.md](239_系统设计与场景面试题库.md) |

---

## 三、推荐阅读路线

### 路线 A · 面试速成（JDK + Spring，约 10 天）

```
001 总览索引
 → 004 集合框架 → 005 并发基础
 → 009 AQS 工具类 → 010 ForkJoinPool
 → 013 网络 IO 与类加载 → 014 invokedynamic → 015 Unsafe
 → 016 Spring 核心 → 017 AOP → 018 事务 → 019 扩展点 → 020 MVC
```

### 路线 B · JDK 系统精读（004–015）

```
集合(004) → 并发(005) → IO/NIO(006) → JVM/GC(007) → 反射与异步(008)
 → AQS 工具(009) → ForkJoin(010) → 跳表(011) → Stream(012)
 → 网络IO与类加载(013) → invokedynamic(014) → Unsafe/VarHandle/JPMS(015)
```

### 路线 C · Spring 全家桶（016–033）

```
Spring 核心(016) → AOP(017) → 事务(018) → 扩展点(019) → MVC(020)
 → Spring Cloud 大纲(021) → Nacos(022) → Seata(023) → OpenFeign(024)
 → Sentinel(025) → Gateway(026/027) → Ribbon(028) → Bus(029)
 → Sleuth(030) → Config(031) → Resilience4j(032) → LoadBalancer(033)
```

### 路线 D · 中间件与分布式（034–107）

```
ORM/RPC(034 MyBatisPlus / 046 MyBatis / 035 Dubbo)
 → 消息(036 RocketMQ / 038 Kafka / 055 Pulsar / 069 KafkaConnect / 080 KafkaStreams)
 → 缓存(040 Redis / 059 Cluster / 064 Streams / 072 Cluster / 083 Kvrocks)
 → 存储(045 MySQL / 050 TiDB / 049 MongoDB / 047 ZooKeeper / 048 ETCD)
 → 计算(051 Flink / 061 Spark / 056 Doris / 058 ClickHouse)
 → 分布式理论(085 事务原理 / 092 一致性对比 / 096 S3 / 097 分布式 ID)
```

### 路线 E · 数据系统深度专题（108–217）

按主题横向对比阅读，每个主题建议从「基础篇」进入，再读「源码级」系列：

```
MySQL: 045(基础) → 109 → 142 → 147 → 150 → 156 → 163 → 207(共 18 篇)
TiKV:  087(基础) → 100 → 134 → 152 → 155 → 164 → 208 → 214(共 11 篇)
TiDB:  050(基础) → 112 → 128 → 137 → 146 → 149 → 167 → 200(共 12 篇)
Kafka: 038(基础) → 118 → 141 → 145 → 148 → 154 → 179 → 209(共 12 篇)
Flink: 051(基础) → 108 → 123 → 138 → 157 → 194 → 206(共 11 篇)
Redis: 040(基础) → 110 → 120 → 188 → 197 → 216(共 6 篇，含 136 RedLock)
```

### 路线 F · 专项题库冲刺（220–239）

```
Netty(220) → Dubbo(221) → RocketMQ(222) → Kafka(223)
 → JVM C++(224) → Security/Crypto(225) → JIT(226) → Java 9+(227)
 → Java 核心并发(228) → Spring(229) → MySQL(230) → Redis(231)
 → 分布式微服务(232) → 高并发一致性(233)
 → Spring Cloud(234) → ES(235) → Flink(236) → TiDB/TiKV(237)
 → K8s/云原生(238) → 系统设计(239)
```

每篇按「结论 → 关键机制 → 追问」组织，适合面试前按方向快速过题。

---

## 四、章节地图（001–239）

| 区间 | 章节 | 篇数 | 阅读定位 |
|---|---|---|---|
| 001–003 | JDK 索引与速查 | 3 | 先建立全局框架，按需回查 |
| 004–015 | JDK 核心源码 | 12 | Java 面试与框架底层的地基 |
| 016–020 | Spring Framework / Boot | 5 | IoC/AOP/事务/MVC 主线 |
| 021–033 | Spring Cloud | 13 | 微服务组件与分布式协作 |
| 034–084 | 中间件与基础框架 | 51 | 按组件选读，不必顺序通读 |
| 085–107 | 分布式理论与工程 | 23 | 事务、共识、调度、限流、ID |
| 108–217 | 源码级深度专题 | 110 | 按主题横向对比，深入实现 |
| 218–219 | 总结报告（附录） | 2 | 历史快照，用于回顾产出 |
| 220–239 | 专项面试题库 | 20 | 按方向刷题，配合源码文档使用 |

### 4.1 中间件章节导航（034–084）

| 方向 | 编号 |
|---|---|
| ORM / RPC | 034 MyBatis-Plus、035 Dubbo、046 MyBatis |
| 消息队列 | 036 RocketMQ、038 Kafka、055 Pulsar、069 Kafka Connect、080 Kafka Streams |
| 缓存 | 040 Redis、059 Redis Cluster、064 Redis Streams、072 Redis Cluster、083 Kvrocks |
| 数据库 / 协调 | 045 MySQL、049 MongoDB、050 TiDB、047 ZooKeeper、048 ETCD、082 Raft |
| 搜索 / 查询 | 037 Elasticsearch、060 Elasticsearch、073 Elasticsearch、079 Calcite |
| 批流计算 / OLAP | 051 Flink、061 Spark、056 Doris、071 Doris、058 ClickHouse、070 Materialize、077 Pinot、075 VictoriaMetrics、076 Paimon、081 Hudi、074 Iceberg |
| 存储基础 | 078 RocksDB |
| 容器 / 运维 | 053 Kubernetes、062 Helm、065 ArgoCD、054 Prometheus、052 SkyWalking |
| 网关 / 服务网格 | 039 Netty、057 Istio、063 gRPC、067 Nginx/Envoy、068 gRPC Gateway |
| 连接池 / 调度 | 041 HikariCP、043 Druid、042 XXL-Job、044 ShardingSphere |
| 数据湖 / 元数据 | 066 Hive、084 Hive Metastore |

### 4.2 分布式理论导航（085–107）

| 方向 | 编号 |
|---|---|
| 分布式事务 | 085 原理（2PC/3PC/TCC/Saga）、104 Percolator vs Seata AT |
| Nacos 3.x | 086 Distro/Raft、089 gRPC、090 JRaft、094 JRaft Snapshot、101 DistroDataProcessor、103 Config 链路 |
| TiKV / PD | 087 Percolator+Raft、088 PD、091 HotRegion、093 Raft 工程优化、102 PD RegionScheduler、105 HotRegion 深度 |
| 一致性与对比 | 092 算法全景、106 算法全景最新版、107 Raft 2025 应用 |
| Percolator | 100 源码级解析 |
| 基础设施 | 095 CAP/BASE、096 S3/MinIO、097 分布式 ID |
| Envoy | 098 xDS、099 WASM Filter |

### 4.3 深度专题导航（108–217）

| 主题 | 编号 |
|---|---|
| MySQL（18） | 109、142、147、150、151、156、159、160、163、169、178、180、183、187、190、193、202、207 |
| TiKV（11） | 125、129、134、140、152、155、164、177、208、212、214 |
| TiDB（12） | 112、128、137、146、149、158、161、167、170、173、200、204 |
| Kafka（12） | 118、141、145、148、154、174、179、192、195、203、209、211 |
| Flink（11） | 108、123、138、144、157、171、175、194、198、201、206 |
| Redis（6） | 110、120、136、188、197、216 |
| Elasticsearch（4） | 180、189、199、213 |
| ClickHouse（4） | 168、176、186、210 |
| Pulsar（4） | 132、162、181、191 |
| RocketMQ（3） | 122、130、153 |
| Seata / 分布式事务（3） | 121、131、184 |
| 存储引擎 / 数据湖（4） | 135、182、183、185 |
| Kubernetes（2） | 116、117 |
| Spark（2） | 172、215 |
| 可观测性（2） | 143、217 |
| 其他专题 | 111 HDFS、113 分布式限流、114 YARN、115 缓存三兄弟、119 一致性哈希、124 分布式锁、126 RedLock、127 限流熔断、133 Sentinel/Dubbo、139 分布式 ID、166 MySQL MGR、196 ZooKeeper ZAB、205 Raft 协议 |

> 注：187 同时属于 MySQL 与 TiDB 的优化器专题，跨主题重叠属正常。

### 4.4 专项面试题库导航（220–239）

| 编号 | 方向 | 题目数 | 配套源码文档 |
|---|---|---|---|
| [220](220_Netty面试题库.md) | Netty 线程模型/Pipeline/ByteBuf | 45 | [039](039_Netty源码解析.md) |
| [221](221_Dubbo面试题库.md) | Dubbo SPI/暴露引用/容错/协议 | 45 | [035](035_Dubbo源码解析.md) |
| [222](222_RocketMQ面试题库.md) | RocketMQ 存储/事务消息/消费 | 40 | [036](036_RocketMQ源码解析.md) |
| [223](223_Kafka面试题库.md) | Kafka 副本/生产消费/KRaft | 40 | [038](038_Kafka源码解析.md) |
| [224](224_JVM底层C++面试题库.md) | HotSpot 对象/GC/类加载/锁 | 40 | [007](007_JVM与GC源码解析.md) |
| [225](225_JavaSecurity与Crypto面试题库.md) | JCA/JCE/TLS/密钥管理 | 40 | [013](013_网络IO与类加载源码解析.md) |
| [226](226_JIT编译器面试题库.md) | C1/C2/分层编译/逃逸分析 | 40 | [224](224_JVM底层C++面试题库.md) |
| [227](227_Java9+新特性面试题库.md) | JPMS/Records/虚拟线程/FFM | 42 | [015](015_UnsafeVarHandleSwitchPointLookupJPMS源码解析.md) |
| [228](228_Java核心与并发面试题库.md) | 集合/并发/AQS/线程池/JMM/GC | 45 | [004](004_集合框架源码解析.md) |
| [229](229_Spring体系面试题库.md) | IoC/循环依赖/AOP/事务/MVC/Boot | 45 | [016](016_Spring核心源码解析.md) |
| [230](230_MySQL面试题库.md) | 索引/MVCC/锁/日志/优化/高可用 | 45 | [045](045_MySQL源码解析.md) |
| [231](231_Redis面试题库.md) | 结构编码/持久化/集群/缓存/锁 | 45 | [040](040_Redis源码解析.md) |
| [232](232_分布式与微服务面试题库.md) | CAP/分布式事务/治理/Raft | 45 | [085](085_分布式事务原理源码解析.md) |
| [233](233_高并发数据一致性面试题库.md) | 缓存双写/幂等/库存/对账 | 45 | [231](231_Redis面试题库.md) |
| [234](234_SpringCloud组件面试题库.md) | Nacos/Feign/Sentinel/Gateway/Seata | 45 | [022](022_Nacos源码解析.md) |
| [235](235_Elasticsearch面试题库.md) | 倒排/Mapping/段合并/聚合/集群 | 45 | [037](037_Elasticsearch源码解析.md) |
| [236](236_Flink面试题库.md) | 架构/Watermark/状态/Checkpoint/SQL | 45 | [051](051_Flink源码解析.md) |
| [237](237_TiDB与TiKV面试题库.md) | Region/Raft/Percolator/MVCC/HTAP | 45 | [050](050_TiDB源码解析.md) |
| [238](238_Kubernetes与云原生面试题库.md) | Pod/Service/调度/存储/发布 | 45 | [053](053_Kubernetes源码解析.md) |
| [239](239_系统设计与场景面试题库.md) | 短链/秒杀/Feed/IM/支付/容量 | 45 | [085](085_分布式事务原理源码解析.md) |

### 4.5 题库去重与主题归属

同一主题只保留一处“权威详解”，其他文档保留结论、场景差异与交叉引用，避免重复与冲突。

| 主题 | 权威详解 | 其他出现位置（已收敛为结论/引用） |
|---|---|---|
| 分布式事务协议（2PC/3PC/TCC/Saga） | [232 Q5–Q12](232_分布式与微服务面试题库.md) | 233 Q23–Q28、234 Q28–Q34、239 Q27 |
| 幂等设计 | [233 Q4](233_高并发数据一致性面试题库.md) | 232 Q4、239 Q22 |
| 分布式锁 | [231 Q30–Q32](231_Redis面试题库.md)、[232 Q27–Q29](232_分布式与微服务面试题库.md) | 233 Q23、239 Q23 |
| 缓存与数据库一致性 | [233 Q9–Q18](233_高并发数据一致性面试题库.md) | 231 Q28–Q29、239 Q5/Q26 |
| 消息可靠/重复/顺序 | [232 Q14–Q16](232_分布式与微服务面试题库.md) | 222 Q22–Q27、233 Q33、239 Q27 |
| 消息队列选型 | [222 Q35/Q40](222_RocketMQ面试题库.md) | 223 Q35、239 Q7 |
| Spring Cloud 组件细节 | [234](234_SpringCloud组件面试题库.md) | 232 Q9/Q18/Q22–Q26 |
| JVM / GC / JIT | [228 Q33–Q40](228_Java核心与并发面试题库.md)、[224](224_JVM底层C++面试题库.md)、[226](226_JIT编译器面试题库.md) | 227 仅保留 JPMS/语言特性 |
| MySQL / TiDB MVCC | [230 Q11–Q22](230_MySQL面试题库.md)、[237 Q16–Q25](237_TiDB与TiKV面试题库.md) | 两篇互相引用，不重复实现细节 |
| 系统设计场景 | [239](239_系统设计与场景面试题库.md) | 机制细节指向 230/231/232/233 |

> 001–219 各篇末尾的“面试考点”保留为源码专题内速查，不迁移；跨专题系统复习以 220–239 题库为准。

---

## 五、知识依赖关系

```
Java 语言基础
   └─ 004 集合 ── 005 并发 ─┬─ 009 AQS 工具
                            ├─ 010 ForkJoin / 012 Stream
                            └─ 011 跳表
   └─ 006 IO/NIO ─ 013 网络 IO 与类加载
   └─ 007 JVM/GC ─ 015 Unsafe / 内存模型
   └─ 008 反射 / 动态代理 ─ 014 invokedynamic / Lambda
                                   │
Spring 体系 ────────────────────────┘
   016 IoC（三级缓存） → 017 AOP（代理链） → 018 事务（传播）
        → 019 扩展点 → 020 MVC（DispatcherServlet）
        → 021–033 Spring Cloud 组件
                                   │
中间件：034–084（MySQL / Redis / Kafka / Flink / TiDB …）
                                   │
分布式理论：085–107（事务 / Raft / 调度 / 限流 / ID）
                                   │
源码级深度专题：108–217（按组件横向展开）
                                   │
专项面试题库：220–239（Netty / RPC / MQ / JVM / Spring / DB / 分布式 / 云原生 / 系统设计）
```

---

## 六、源码基线说明

| 项目 | 路径写法 | 状态 |
|---|---|---|
| JDK 1.8.0_451 | `src/java/...`（仓库相对路径） | ✅ 已随仓库提供，锚点可直接定位 |
| Spring Framework / Boot | `d:\GC_WorkSpaces\spring-framework-5.3.35\...`、`spring-boot-2.7.18\...` | ⚠️ 外部源码，未随仓库提供；文档中保留原路径 |
| Nacos 3.2.4 | `d:\GC_WorkSpaces\nacos-3.2.4\...` | ⚠️ 外部源码，未随仓库提供 |
| Seata 2.0.0 | `d:\GC_WorkSpaces\incubator-seata-2.0.0\...` | ⚠️ 外部源码，未随仓库提供 |

- 本仓库已统一修正 **JDK 路径基线**：原文档中的旧绝对路径（`GC_WorkSpaces\jdk-source-analysis\src\`）已全部替换为 `src/`，跨机器可移植。
- 外部项目（Spring/Nacos/Seata）的绝对路径保持原样，仅作为“分析时的源码版本”记录；如需复核，请按对应版本自行检出。
- 源码锚点格式：`相对路径.java:L起始行` 或 `L起始行-结束行`，例如 [HashMap.java](src/java/util/HashMap.java) 的 `L183-203`。

---

## 七、维护约定

1. **编号即顺序**：新增文档从 **240** 起续编，不插入已有编号，避免引用失效。
2. **编号与标题**：编号以文件名为准；标题首行保持主题名即可（004–020 沿用 `# NNN · 主题` 样式）。
3. **链接格式**：文档间引用使用仓库相对链接（如 `004_集合框架源码解析.md`），不使用绝对路径。
4. **源码锚点**：JDK 内容统一写 `src/...`；外部源码需注明版本与相对模块路径。
5. **总结报告**：218–219 为阶段性快照，新增内容以专题文档为准，不回写到报告中。

---

## 八、附录

- [218_JDK核心源码项目完整总结报告.md](218_JDK核心源码项目完整总结报告.md) —— 第一轮 JDK 12 篇产出总结
- [219_JDK源码解析完整总结报告.md](219_JDK源码解析完整总结报告.md) —— 扩展至 Spring 后的 17 篇产出总结
- [001_JDK核心源码深度解析_总览索引.md](001_JDK核心源码深度解析_总览索引.md) —— 按模块/面试权重检索
- [002_JDK核心源码模块速查.md](002_JDK核心源码模块速查.md) —— 按类名速查
- [003_JDK核心源码注释总览.md](003_JDK核心源码注释总览.md) —— 按源码注释线索速查
