# MongoDB 源码深度解析（NoSQL 文档数据库）

> **源码基线**：MongoDB 8.0.x（最新稳定）  
> **核心仓库**：https://github.com/mongodb/mongo  
> **底层引擎**：WiredTiger（默认）  
> **面试权重**：⭐⭐⭐⭐（NoSQL 高频）

---

## 一、MongoDB 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式文档数据库（Document-Oriented NoSQL）         │
│  文档 = BSON（Binary JSON）→ 灵活的 Schema（无固定表结构）    │
│                                                               │
│  两大核心设计：                                                │
│  ① Replica Set（副本集）：Master + N Slave + 自动故障转移    │
│  ② Sharded Cluster（分片集群）：Shard + Mongos + Config Server│
│                                                               │
│  对比 MySQL：                                                  │
│  MySQL：表 → 行 → 列（固定 Schema）→ 强类型 → Join           │
│  Mongo：库 → Collection → Document（灵活 Schema）→ 无 Join    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Replica Set 副本集架构（★ 高可用核心）                       │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   Primary      │   Secondary   │   Secondary   │             │
│  │  （主节点）    │  （从节点）    │  （从节点）    │             │
│  │  处理写请求    │  复制 oplog    │  复制 oplog    │             │
│  │  → oplog 写   │  → 本地重放    │  → 本地重放    │             │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         │              │              │                       │
│         └──────────────┼──────────────┘                       │
│                   心跳 + 选主                                   │
│                                                               │
│  ★ Primary 挂了 → Secondary 选主（Bully 算法 + Priority）     │
│  → Priority 高的 Secondary 优先当选                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 WiredTiger 存储引擎（★ 默认引擎核心）

```cpp
// ★ WiredTiger 是 MongoDB 3.2+ 默认存储引擎（替代 MMAPv1，4.2 移除 MMAPv1）
//
// 核心特性：
// ① LSM-Tree（Log-Structured Merge Tree）→ 顺序写 + 后台合并
// ② MVCC（多版本并发控制）→ 类似 InnoDB
// ③ 文档级并发控制（Document-level Concurrency Control）
// ④ 可插拔压缩（Snappy / Zlib / ZSTD）
//
// WiredTiger vs InnoDB 核心区别：
//
// ┌──────────┬────────────────────┬────────────────────┐
// │          │    WiredTiger        │    InnoDB           │
// ├──────────┼────────────────────┼────────────────────┤
// │ 数据模型  │ 文档（BSON）         │ 行（关系型）         │
// │ 索引结构  │ B+Tree（每个 Collection）│ B+Tree（聚簇+二级）│
// │ 日志结构  │ WiredTiger Log（LSM 前日志）│ Redo Log + Undo Log│
// │ 刷盘      │ Checkpoint + Journal │ Doublewrite Buffer  │
// │ 锁粒度    │ 文档级锁            │ 行锁 + Gap Lock      │
// │ MVCC      │ Snapshot Isolation  │ Snapshot Isolation   │
// └──────────┴────────────────────┴────────────────────┘
//
// WiredTiger 写流程：
//   ① 写 Journal（WAL，类似 Redo Log）
//   ② 写内存 B+Tree（WT_CURSOR）
//   ③ Checkpoint 定期把内存 B+Tree 刷盘
//   ④ LSM-Tree 后台合并（类似 ES Merge）
```

### 2.2 Oplog（★ 主从复制核心）

```javascript
// ★ Oplog（Operation Log）是 Mongo 主从复制核心！
//   存于 local.oplog.rs（Capped Collection，固定大小循环覆盖）
//
// Oplog 条目结构：
//   {
//     ts: Timestamp,          // 时间戳（秒+自增，单调递增）
//     t: Number(2),          // 任期（term，选主变化时 +1）
//     h: NumberLong,         // 哈希（唯一标识这个操作）
//     v: NumberInt(2),       // Oplog 版本
//     op: "u",               // 操作类型：i(insert)/u(update)/d(delete)/c(命令)
//     ns: "test.user",       // 命名空间（db.collection）
//     o: { name: "张三" },   // 操作内容（insert: 完整文档；update: $set 操作）
//     o2: { _id: 1 }         // 操作条件（update/delete 时的查询条件）
//   }
//
// ★ Oplog 特性：
//   ① Capped Collection（固定大小，默认 5% 磁盘空间，最小 990MB）
//   ② 顺序追加写 → 磁盘 IO 快
//   ③ 每个 Secondary 从自己上次停的 ts 开始追
//   ④ Primary 挂了 → 新 Primary 继续写 oplog（ts 单调递增）
//
// ★ 和 MySQL Binlog 对比：
//   MySQL Binlog：逻辑日志（Row Format 存行变化）
//   Mongo Oplog：**操作日志**（存操作本身，可重放）→ 幂等（重复执行不影响结果）
//
// ★ Oplog 幂等性：
//   同一个 oplog entry 执行多次结果一样吗？
//   → insert：重复执行会有重复 _id → 但 _id 唯一约束会报错（不是幂等！）
//   → update：$set 操作重复执行 → 一样（幂等）
//   → delete：重复执行（删不存在的）→ 一样（幂等）
//   → 为了 insert 幂等：Mongo 在 oplog insert 时会先检查 _id 是否存在
```

### 2.3 分布式事务（Mongo 4.0+）

```javascript
// ★ MongoDB 4.0+ 支持多文档事务（类似 MySQL 事务）
//   但不支持跨 Shard 事务（分片集群里只能单 Shard 事务）
//
// 单文档原子性：Mongo 从出生就支持（一个 document 的写入是原子的）
// 多文档事务：Mongo 4.0+ 引入
//
// 事务流程：
//   const session = db.getMongo().startSession();
//   session.startTransaction();
//   try {
//     db.orders.insertOne({ _id: 1, status: "pending" });
//     db.inventory.updateOne({ _id: 1 }, { $inc: { qty: -1 } });
//     session.commitTransaction();
//   } catch (e) {
//     session.abortTransaction();
//   } finally {
//     session.endSession();
//   }
//
// ★ 和 MySQL 事务对比：
//   MySQL：ACID（InnoDB 完整支持）
//   Mongo：ACID（单机 Replica Set 内支持，但**跨 Shard 不支持**）
//   → 分片集群只能单 Shard 事务，跨 Shard 要用两阶段提交（应用层自己实现）
//
// ★ 为什么跨 Shard 不支持？
//   每个 Shard 有自己的 oplog → 跨 Shard 事务要协调多个 oplog
//   → 类似 MySQL 分布式事务（Seata AT 模式）→ Mongo 没实现
```

### 2.4 Sharded Cluster（分片集群）

```
// ★ Sharded Cluster 架构（水平扩展核心）
//
// ┌─────────────┬─────────────┬─────────────┐
// │   Mongos     │  Config Svr  │   Shard      │
// │  （路由）    │  （元数据）   │  （数据存储）  │
// └─────────────┴─────────────┴─────────────┘
//      ↓              ↓             ↓
//   Client      分片规则      1个 Shard =
//   发请求      存哪          1个 Replica Set
//
// ★ Mongos 路由流程：
//   Client → Mongos.find({ userId: 123 })
//     → Mongos 查 Config Server：userId=123 属于哪个 Shard？
//       → 分片规则：hash(userId) % 4 → Shard[2]
//     → Mongos 把请求转给 Shard[2] 的 Primary
//     → 返回结果
//
// ★ 分片策略三种：
//   ① Hash（默认）：hash(field) % shardCount → 均匀
//   ② Range：field BETWEEN 0-25 → Shard0; 26-50 → Shard1
//   ③ Hashed + 复合：hash + 范围组合
//
// ★ 和 MySQL 分库分表（ShardingSphere）对比：
//   Mongo 分片：**原生支持** + 自动路由 + 自动均衡 + oplog 分片
//   MySQL 分库分表：中间件（ShardingSphere）→ SQL 改写 + 应用层路由
```

### 2.5 Mongo 索引

```javascript
// ★ Mongo 索引类型（和 MySQL B+Tree 本质相同）
//
// ① Single Field Index（单字段索引）
//   db.user.createIndex({ name: 1 })  // 1=升序，-1=降序
//
// ② Compound Index（复合索引）
//   db.user.createIndex({ name: 1, age: -1 })
//   → 最左前缀原则（和 MySQL 一样）
//
// ③ Multikey Index（多键索引，数组字段）
//   db.product.createIndex({ tags: 1 })  // tags 是数组
//   → 数组每个元素都建索引
//
// ④ Text Index（文本索引，全文搜索）
//   db.article.createIndex({ content: "text" })
//   → 支持 $text 搜索
//
// ⑤ 2dSphere Index（地理位置索引）
//   db.store.createIndex({ location: "2dsphere" })
//   → 支持 $nearSphere 附近搜索
//
// ★ _id 索引自动创建：
//   每个 Collection 自动建 { _id: 1 } 唯一索引
//   → 类似 MySQL 主键
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Mongo 核心定位？| **分布式文档数据库**（BSON 格式，灵活 Schema）；Replica Set 高可用；Sharded Cluster 水平扩展 |
| 2 | WiredTiger 是什么？| **Mongo 默认存储引擎**（3.2+）→ LSM-Tree + MVCC + Journal + 文档级锁；替代 MMAPv1（4.2 移除）|
| 3 | Oplog 作用？| **主从复制核心**（Capped Collection，默认 5% 磁盘）→ Primary 每个写操作都写 oplog → Secondary 追 oplog 重放；和 MySQL Binlog 对比 |
| 4 | Oplog 幂等性？| update/delete 幂等（重复执行一样）；insert 非幂等（重复 _id 会报错）→ Mongo 在 insert oplog 重放时会先检查 _id 是否存在 |
| 5 | Capped Collection 是什么？| **固定大小循环覆盖的 Collection** → Oplog 存在这；顺序追加写 → 满了覆盖最旧的 |
| 6 | Replica Set 选主？| **Bully 算法 + Priority** → Priority 高的 Secondary 优先选 → 心跳超时触发选举 |
| 7 | Mongo 事务支持？| 4.0+ 支持**单 Replica Set 内多文档事务**（ACID）；**跨 Shard 不支持事务**（要用应用层 2PC）|
| 8 | Mongo vs MySQL？| Mongo：**文档模型 + 灵活 Schema + 无 Join + 水平扩展**；MySQL：关系型 + 强 Schema + Join + ACID；选哪个看业务模型 |
| 9 | Sharded Cluster 架构？| Mongos（路由）+ Config Server（元数据/分片规则）+ N Shard（每个 Shard = 一个 Replica Set）→ Client 只连 Mongos |
| 10 | Mongo 分片策略？| **Hash（默认，均匀）/ Range（范围，适合顺序场景）/ Hashed + 复合**；分片键一旦选了不能改！ |
| 11 | Mongo 索引类型？| Single Field / Compound（最左前缀）/ Multikey（数组）/ Text（全文）/ 2dSphere（地理位置）；**_id 自动建唯一索引**|
| 12 | Mongo 为什么快？| 文档级锁 + WiredTiger LSM-Tree 顺序写 + 内存缓存（working set 放内存）+ 水平扩展 |
| 13 | Mongo 慢查询？| db.currentOp() 看当前操作；db.system.profile 看慢查询日志（默认 100ms）；explain() 分析查询计划 |
| 14 | Mongo NoSQL 是什么意思？| Not Only SQL → **不只是 SQL** → 泛指非关系型数据库；Mongo 是文档型 NoSQL |
| 15 | WiredTiger vs InnoDB 核心？| 都是 MVCC + B+Tree；WiredTiger：文档级锁 + LSM-Tree；InnoDB：行锁 + Gap Lock + Doublewrite Buffer |
| 16 | Mongo 复制延迟？| Secondary 追 oplog 有延迟（ms~s 级）→ **读 Secondary 可能读到旧数据**（最终一致性）；强一致读要读 Primary |
| 17 | Mongo 容量上限？| 单 Collection 建议 **1-2TB**；单个文档最大 **16MB**（BSON 限制）；Sharded Cluster 可以无限扩展 |
| 18 | Mongo vs ES？| 都是 JSON 文档；Mongo：CRUD + 主从复制 + 事务；ES：**全文搜索 + 倒排索引 + BM25 算分**；生产常组合用（Mongo 存数据 + ES 做搜索）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：MongoDB 8.0.x + WiredTiger  
**面试考点：18 题**
