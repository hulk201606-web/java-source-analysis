# Apache Kafka Connect 源码深度解析（Kafka 数据集成）

> **源码基线**：Apache Kafka 3.8.x（最新稳定）  
> **核心仓库**：https://github.com/apache/kafka  
> **面试权重**：⭐⭐⭐⭐（Kafka 生态高频）

---

## 一、Kafka Connect 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：Kafka 和外部系统之间的数据搬运框架                │
│  "让数据流入流出 Kafka 变得像写配置一样简单"                 │
│                                                               │
│  两大 Connector 类型：                                        │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │                │ Source Connector │ Sink Connector │     │
│  ├──────────────┼──────────────┼──────────────┤             │
│  │ 数据流向       │ 外部 → Kafka    │ Kafka → 外部       │     │
│  │ 作用           │ 采集外部数据     │ 把数据写入外部系统  │     │
│  │ 示例           │ MySQL CDC → Kafka│ Kafka → ES/S3/PG   │     │
│  └──────────────┴──────────────┴──────────────┘             │
│                                                               │
│  为什么不用自己写 Consumer/Producer？                         │
│  ① 现成的 100+ Connector（MySQL/PG/Mongo/ES/S3/RabbitMQ...）│
│  ② 统一配置（JSON/YAML）                                     │
│  ③ 自动 Offset 管理（Consumer Group + Offset Topic）         │
│  ④ 分布式容错（Worker 挂了自动 Failover）                    │
│  ⑤ 和 Kafka 安全体系无缝集成（SASL/SSL/ACL）                │
│                                                               │
│ ★ Kafka Connect 是 Kafka 生态中 **数据搬运的标准方案**      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Kafka Connect 架构                                          │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Kafka Connect Cluster                                   │  │
│  │  ┌──────────────┬──────────────┬──────────────┐       │  │
│  │  │ Worker 1      │ Worker 2      │ Worker 3      │       │  │
│  │  │ Source Task   │ Source Task   │ Sink Task    │       │  │
│  │  │ + Sink Task   │ + Sink Task   │              │       │  │
│  │  └──────────────┴──────────────┴──────────────┘       │  │
│  │           ↓                 ↓                 ↓          │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │ Kafka Broker（Config Topic + Offset Topic + Status）│  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────┘  │
│                          ↓                                    │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │ MySQL CDC    │ PostgreSQL    │ Elasticsearch │ S3/Snowflake│
│  │ (Source)     │ (Source)      │ (Sink)       │ (Sink)     │
│  └──────────────┴──────────────┴──────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Worker + Task 模型（★ 核心）

```java
// ★ Kafka Connect 核心：Worker 集群 + Task 并行执行
//
// Worker（工作进程）：
//   每个 Worker 是一个 JVM 进程
//   可以跑 N 个 Source Task + N 个 Sink Task
//   Worker 之间通过 Kafka Config Topic 协调
//
// Task（任务）：
//   Connector 实例化后生成 Task
//   每个 Task 是一个线程
//   Source Task：poll() 拉外部数据 → send() 推 Kafka
//   Sink Task：poll() 拉 Kafka 数据 → put() 写外部系统
//
// ★ 典型配置：
//   Worker 配置：worker.properties
//     bootstrap.servers=localhost:9092
//     group.id=connect-cluster
//     key.converter=org.apache.kafka.connect.json.JsonConverter
//     value.converter=org.apache.kafka.connect.json.JsonConverter
//     offset.storage.topic=connect-offsets
//     config.storage.topic=connect-configs
//     status.storage.topic=connect-status
//
//   MySQL CDC Connector（Source）：
//   {
//     "name": "mysql-source",
//     "config": {
//       "connector.class": "io.debezium.connector.mysql.MySqlConnector",
//       "database.hostname": "mysql.example.com",
//       "database.port": "3306",
//       "database.user": "debezium",
//       "database.password": "xxx",
//       "database.server.id": "5401",
//       "database.server.name": "my-app",
//       "database.include.list": "inventory",
//       "table.include.list": "inventory.users",
//       "include.schema.changes": "false"
//     }
//   }
//
//   ES Connector（Sink）：
//   {
//     "name": "es-sink",
//     "config": {
//       "connector.class": "org.apache.kafka.connect.elasticsearch.ElasticsearchSinkConnector",
//       "topics": "my-app.inventory.users",
//       "connection.url": "https://elasticsearch.example.com:9200",
//       "connection.username": "elastic",
//       "connection.password": "xxx",
//       "key.ignore": "true",
//       "schema.ignore": "true"
//     }
//   }
```

### 2.2 Source Connector 核心链路

```java
// ★ Source Connector 核心接口（面试高频）
//
// ① Connector（配置 + Task 分配）：
public class MySourceConnector implements Connector {
    @Override
    public void start(Map<String, String> props) {
        // 初始化：校验配置、建连接等
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return MySourceTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // 把工作分配给 maxTasks 个 Task
        // 比如按表分：Task 1 负责 users 表，Task 2 负责 orders 表
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("partition", String.valueOf(i));
            configs.add(config);
        }
        return configs;
    }
    
    @Override
    public void stop() {
        // 关闭资源
    }
    
    @Override
    public ConfigDef config() {
        // 配置项定义
        return new ConfigDef();
    }
}

// ② Task（实际拉数据）：
public class MySourceTask implements SourceTask {
    @Override
    public void start(Map<String, String> props) {
        // Task 初始化
    }
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // ★ 核心方法：拉外部数据
        // 返回 SourceRecord（topic + partition + key + value）
        // Connect 框架自动把 SourceRecord 写 Kafka
        // → 不需要自己写 Producer！
        List<SourceRecord> records = new ArrayList<>();
        MyData data = externalSystem.fetchNewData();
        records.add(new SourceRecord(
            sourcePartition, sourceOffset,
            "my-topic", partition,
            keySchema, key,
            valueSchema, value
        ));
        return records;
    }
    
    @Override
    public void stop() {
        // 关闭资源
    }
    
    @Override
    public OffsetStorageReader offsetStorageReader() {
        // 读取上次 offset（断点续传）
        return null;
    }
}
```

### 2.3 Sink Connector 核心链路

```java
// ★ Sink Connector 核心接口（面试高频）
//
// Sink 比 Source 简单（poll 数据由 Connect 框架做）：
public class MySinkConnector implements Connector {
    // 和 Source Connector 接口相同...
}

public class MySinkTask implements SinkTask {
    @Override
    public void start(Map<String, String> props) {
        // 初始化外部连接
    }
    
    @Override
    public void put(Collection<SinkRecord> records) {
        // ★ 核心方法：Connect 框架拉 Kafka 数据 → 调用 put 写外部系统
        for (SinkRecord record : records) {
            externalSystem.write(record.key(), record.value());
        }
    }
    
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // flush 时要保证数据写入完成
        // Connect 根据 flush 结果提交 Consumer Offset
    }
    
    @Override
    public void stop() {
        // 关闭资源
    }
    
    @Override
    public String version() { return "1.0"; }
}
```

### 2.4 Offset 存储 + Failover

```java
// ★ Kafka Connect 三大内部 Topic（自动创建）
//
// ① config.storage.topic（connect-configs）
//   存所有 Connector 配置
//   → Connector 创建/更新/删除 → 写 Config Topic → Worker Watch → 自动重启
//
// ② offset.storage.topic（connect-offsets）
//   存 Source Task offset（断点续传）
//   存 Sink Task Consumer Group offset
//   → Worker 挂了 → 新 Worker 从 Offset Topic 恢复 → 不丢数据
//
// ③ status.storage.topic（connect-status）
//   存 Connector/Task 状态（RUNNING / FAILED / PAUSED）
//   → Connect REST API 查询状态从这里读
//
// ★ Failover 流程：
//   Worker 1（Source Task 挂了）
//   → Kafka 检测到 Worker 离开 Group（心跳超时）
//   → Worker 2/3 感知到配置变化（Config Topic 消息）
//   → Worker 2/3 重新分配 Task（Rebalance）
//   → 从 Offset Topic 读取上次 offset → 断点续传
//   → 数据不丢（Exactly-Once 配置）
//
// ★ Exactly-Once 保证（面试高频）：
//   ① Source Task：offset 存在 Topic partition → 幂等写入 Kafka 同 Partition
//   ② Sink Task：put + flush 原子写入 → flush 成功才提交 Consumer Offset
//   ③ 配置：offset.flush.interval.ms + exactly.once.enabled
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Kafka Connect 核心定位？| **Kafka 数据集成框架** → Source（外部→Kafka）+ Sink（Kafka→外部）→ 避免自己写 Consumer/Producer |
| 2 | Source vs Sink？| Source：**poll() 拉外部数据 → Connect 框架自动写 Kafka**；Sink：Connect 框架拉 Kafka → **put() 写外部** |
| 3 | Worker + Task 模型？| Worker（JVM 进程，跑多个 Task）；Task（线程，实际拉数据/写数据）；Connector 负责把工作分配给 N 个 Task |
| 4 | Kafka Connect 三大内部 Topic？| **config.storage.topic（Connector 配置）/ offset.storage.topic（Offset 断点续传）/ status.storage.topic（状态）** → Worker 集群协调靠这些 Topic |
| 5 | Failover 流程？| Worker 挂 → Kafka Rebalance → 其他 Worker 从 **Offset Topic 恢复上次位置** → 断点续传 |
| 6 | Offset 管理？| **Source：自己存 Offset StorageReader**（外部系统的进度）；**Sink：Connect 框架自动 Consumer Group Offset**；Flush 成功才 Commit |
| 7 | 现成 Connector？| **100+ 现成**：Debezium（MySQL/PG CDC）+ JDBC + Elasticsearch + S3 + Snowflake + RabbitMQ + Twitter... |
| 8 | REST API？| Connect 启动后暴露 REST API：GET /connectors / POST /connectors（创建）/ PUT（更新）/ DELETE（删除）|
| 9 | 分布式？| Worker 集群通过 Kafka Config Topic 协调 → 多 Worker 自动 Rebalance → 高可用 |
| 10 | Exactly-Once 保证？| Source：Offset + Idempotent Kafka Produce；**Sink：put+flush 原子 + flush 成功才 Commit Consumer Offset** |
| 11 | 自定义 Connector？| 实现 Connector + SourceTask/SinkTask 接口 → 打 JAR 放到 Worker 插件目录 → 自动加载 |
| 12 | Kafka Connect vs Flink CDC？| Connect：**轻量 + 配置驱动 + 简单场景**；Flink CDC：**实时流 + 复杂 ETL + 窗口/Join**；按场景选 |
| 13 | 常见 Source Connector？| **Debezium MySQL CDC**（Binlog→Kafka）+ Kafka Connect JDBC（全表扫描）+ FilePulse（文件监听）+ Oracle CDC |
| 14 | 常见 Sink Connector？| **Elasticsearch Sink** + S3 Sink + JDBC Sink（Kafka→DB）+ Snowflake Sink + BigQuery Sink |
| 15 | Kafka Connect 学习路径？| Docker 起 Connect + MySQL + ES → 写 Connect 配置 JSON → curl REST API 创建 Connector → 看 Config/Offset/Status Topic |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Apache Kafka Connect 3.8.x  
**面试考点：15 题**
