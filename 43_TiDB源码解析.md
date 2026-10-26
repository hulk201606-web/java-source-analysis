# TiDB 源码深度解析（国产分布式 NewSQL）

> **源码基线**：TiDB 7.5.x（最新稳定）  
> **核心仓库**：https://github.com/pingcap/tidb  
> **底层存储**：TiKV（Rust 实现 + Raft 协议）  
> **面试权重**：⭐⭐⭐⭐⭐（国产分布式数据库高频）

---

## 一、TiDB 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式 NewSQL（MySQL 兼容 + 水平扩展 + 强一致）     │
│  解决什么问题？MySQL 分库分表的痛点 → 原生分布式                │
│                                                               │
│  三大组件：                                                    │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐  │
│  │  Client    │   TiDB Server │     PD       │    TiKV      │  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤  │
│  │ MySQL 协议 │ SQL 解析/优化 │ 调度/元数据  │ 数据存储       │  │
│  │ 驱动       │ 执行计划       │ Raft 调度   │ Raft 副本     │  │
│  └──────────┴──────────────┴──────────────┴──────────────┘  │
│       ↕          ↕              ↕              ↕            │
│  MySQL Wire Protocol    gRPC + etcd        gRPC + Raft        │
│                                                               │
│ ★ TiDB = MySQL 协议 + Raft 共识 + 分布式事务 + 水平扩展       │
│ ★ 兼容性：支持 MySQL 8.0 语法 + 大部分存储过程 + 大部分函数    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  和 MySQL + ShardingSphere 对比                               │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │                │   TiDB         │ MySQL + ShardingSphere │             │
│  ├──────────────┼──────────────┼──────────────┤             │
│  │ 水平扩展       │ ★★★★★ 原生   │ ★★★ 中间件  │             │
│  │ 分布式事务     │ ★★★★★ 2PC    │ ★★ 应用层   │             │
│  │ 自动分片       │ ★★★★★ PD 调度 │ 手动配置     │             │
│  │ 故障转移       │ ★★★★★ Raft   │ ★★ 主从切换   │             │
│  │ 兼容性         │ ★★★★ MySQL  │ ★★★★★ 完全    │             │
│  │ 运维复杂度     │ ★★★ 中等      │ ★★★★ 较复杂  │             │
│  │ 社区           │ PingCAP      │ 阿里开源     │             │
│  └──────────────┴──────────────┴──────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 TiDB Server（Go 实现，SQL 层）

```go
// ★ TiDB Server 负责 SQL 解析/优化/执行（类似 MySQL Server 层）
//   用 Go 语言实现（不是 C++）
//
// SQL 执行流程：
//   Client（MySQL 协议）→ TiDB Server
//     ① Parser：yacc 语法解析 → AST（语法树）
//     ② Optimizer：基于成本的查询优化（CBO）→ PhysicalPlan
//     ③ Executor：执行 PhysicalPlan
//       → Coprocessor 下推到 TiKV ★（TiDB 性能核心！）
//     ④ 结果返回 Client
//
// ★ Coprocessor（协处理器，TiDB 最核心的设计）：
//   问题：如果 TiDB Server 把所有数据拉回来过滤 → 网络 IO 爆炸
//   方案：**把部分计算下推到 TiKV 执行**
//     → TiKV 有 Coprocessor 模块
//     → 接收 TiDB 下推的谓词（WHERE 条件）+ 聚合函数（SUM/COUNT）
//     → 在存储端过滤/聚合 → 只返回结果（不是原始数据）
//     → 类似 MySQL 把计算下推到存储引擎（但 TiDB 更彻底）
//
// ★ 下推能力（面试高频）：
//   ✅ 表达式（WHERE a > 10 AND b = 'x'）
//   ✅ 聚合（GROUP BY / SUM / COUNT / AVG / MIN / MAX）
//   ✅ JOIN（部分 JOIN 下推到 Coprocessor）
//   ✅ LIMIT / TOP N
//   ✅ DISTINCT
//   ❌ 复杂函数（CONCAT / IF / CASE WHEN 大部分不能下推）
//
// ★ 如果下推失败怎么办？
//   TiDB Server 把原始数据从 TiKV 拉回来 → 本地计算
//   → 性能下降但功能正确（兼容优先）
```

### 2.2 TiKV（Rust 实现，Raft + 存储）

```rust
// ★ TiKV 是 TiDB 的底层存储（Rust 实现！）
//   基于 Raft 协议的分布式 KV 存储
//
// TiKV 内部组件：
//   ┌────────────────────────────────────────────┐
//   │  TiKV                                       │
//   │  ├── RaftStore（Raft 副本管理）              │
//   │  │   └── raftpb（go.uber.org/raft 库）      │
//   │  ├── StorageEngine（存储引擎）               │
//   │  │   └── RocksDB（LSM-Tree，Facebook 出品）  │
//   │  ├── Coprocessor（SQL 下推计算）★            │
//   │  ├── MVCC（多版本并发控制，类似 InnoDB）      │
//   │  └── Scheduler（事务冲突检测）               │
//   └────────────────────────────────────────────┘
//
// ★ TiDB 行存储格式（TableCodec）：
//   TiDB 表行 → 编码成 KV 对存到 TiKV（RocksDB）
//
//   PK = 123 的 User 表：
//   Key:   t_user_r_123        → Value: {name: "张三", age: 20}
//          t_user_i_name_张三  → Value: 123   (二级索引)
//          t_user_i_name_李四  → Value: 456   (二级索引)
//
//   表名 + 列名 + PK → 编码成 Key
//   → TiDB 查索引 → 直接在 TiKV 里走 RocksDB 查索引 Key
//
// ★ 和 MySQL 存储引擎对比：
//   MySQL InnoDB：B+Tree + Redo/Undo Log
//   TiKV：RocksDB（LSM-Tree）+ Raft 日志
//   → TiKV 用 LSM-Tree 顺序写更高效 + Raft 保证多副本
```

### 2.3 PD（Placement Driver，调度核心）

```
// ★ PD 是 TiDB 的大脑（调度中心）
//   用 Go 实现 + etcd 存储元数据
//
// PD 三大职责：
//
// ① 元数据管理
//   所有表的 TableID / RegionID / 分片规则
//   存在 etcd（etcd 的 MVCC 保证一致性）
//
// ② Region 调度（★ 核心！自动均衡分片）
//   TiDB 表被分成 N 个 Region（类似 ShardingSphere 分片）
//   → PD 监控每个 TiKV 的 Region 数 + 数据量
//   → 自动均衡：Region 多的 TiKV 移到 Region 少的 TiKV
//   → Region 大小默认 96MB（超过自动分裂）
//   → Region 挂了 → PD 通知其他 TiKV 用 Raft 副本补
//
// ③ Raft 调度
//   Region 有 3 副本（默认）→ 1 Leader + 2 Follower
//   PD 监控 Leader 分布 → 均衡 Leader 数（避免热点 TiKV）
//
// ★ 对比 ShardingSphere 手动配置分片：
//   TiDB PD **自动分片 + 自动均衡 + 自动故障转移**
//   → 运维几乎不用管分片！
//
// ★ 对比 MySQL + Vitess：
//   Vitess：也有自动分片（类似 TiDB PD）
//   TiDB：Raft 共识更强 + MySQL 兼容更好
```

### 2.4 Percolator（分布式事务，★★★★★）

```
// ★ TiDB 分布式事务基于 Percolator（Google Spanner 论文）
//   核心：2PC + 时间戳（Tso）+ Prewrite
//
// 事务流程：
//
// ① Begin：从 PD 申请 start_ts（事务开始时间戳，单调递增）
//
// ② Prewrite：两阶段提交第一阶段
//    a. 写锁（Pessimistic Lock 或 Optimistic 检查）
//    b. 写值 + start_ts
//    c. 写一个唯一 primary（主记录，决定事务最终结果）
//    d. 等待所有 Region 的 Prewrite 完成
//       → 每个 Region 都写 Prewrite 值 + primary key + start_ts
//
// ③ Commit：两阶段提交第二阶段
//    a. 从 PD 申请 commit_ts（start_ts 和 commit_ts 必须在同一个 Tso 上）
//    b. 通知所有 Region 把 Prewrite → Commit（写 commit_ts）
//    c. 删除锁
//    d. 返回 Client OK
//
// ④ Rollback：
//    如果 Prewrite 失败 → 删除锁 → 返回 Client Fail
//
// ★ Percolator 核心机制（面试高频）：
//
//   ① Tso（Time Server Oracle）：PD 提供单调递增时间戳
//      → start_ts + commit_ts 定义事务的时间范围
//
//   ② Lock（锁）：每个 Region 用 Raft 写锁
//      → 乐观锁 + 悲观锁都支持
//
//   ③ Primary：唯一主记录
//      → 事务提交/回滚的唯一决定点
//      → 其他 Region 只需要知道 Primary 的状态
//
//   ④ GC：后台线程清理旧锁
//      → 如果事务 commit_ts 太久没写 → 认为失败 → 自动回滚
//
// ★ 和 Seata AT 模式对比：
//   Seata AT：Undo Log + 一阶段提交 + 二阶段异步
//   TiDB Percolator：**2PC + Tso 时间戳 + Raft 保证**
//   → TiDB 更强一致但性能略低（Raft 多副本同步）
```

### 2.5 DDL（在线 DDL，★ 面试高频）

```
// ★ TiDB 在线 DDL 和 MySQL 的区别（面试高频）
//
// MySQL DDL：
//   ALTER TABLE ADD COLUMN col2 INT
//   → 重建整张表（COPY）
//   → 锁表（大表锁几分钟/几小时）
//   → 业务请求被阻塞！
//
// TiDB DDL：
//   ALTER TABLE ADD COLUMN col2 INT
//   → **在线后台异步执行**
//   → 不锁表！业务请求正常进行
//   → 6 个阶段（版本 7.6 引入 DDL V2 架构，批量 DDL 快 10 倍）
//
// TiDB DDL 6 阶段：
//   ① pre-commit：存储元信息（Schema Version）
//   ② write-reorg：后台异步扫描旧数据 → 写新数据
//   ③ post-write-reorg：新旧数据对齐
//   ④ delete-only：旧列标记删除
//   ⑤ delete-reorg：后台异步删除旧数据
//   ⑥ done：DDL 完成，更新 Schema Version
//
// ★ 为什么在线 DDL 不锁表？
//   TiDB MVCC 机制：
//   → 每个事务都有 start_ts（时间戳）
//   → DD 开始 → 记录 old_schema_ts 和 new_schema_ts
//   → 在 old_schema_ts 和 new_schema_ts 之间的请求：
//     读旧数据用 old schema（旧列）
//     读新数据用 new schema（新列）
//     → 双写阶段同时写旧列和新列
//   → 自动过渡，业务无感！
```

---

## 三、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiDB 三大组件？| **TiDB Server（SQL 层，Go）/ PD（调度中心，Go + etcd）/ TiKV（存储层，Rust + Raft + RocksDB）**|
| 2 | TiDB 核心设计？| **Copocessor 下推计算到 TiKV**（过滤/聚合不下推到 Server）+ **Raft 多副本** + **Percolator 分布式事务** |
| 3 | Coprocessor 是什么？| **TiKV 内置 SQL 协处理器** → TiDB 把 WHERE/聚合/LIMIT 下推到 TiKV 执行 → **只返回结果（不是原始数据）** → 性能核心！ |
| 4 | TiKV 存储引擎？| **RocksDB（LSM-Tree，Facebook 出品）** + Raft 协议保证多副本；LSM-Tree 顺序写比 InnoDB B+Tree 更高效 |
| 5 | PD 三大职责？| ① **元数据管理**（TableID/RegionID）② **Region 自动调度均衡**（Region 多的 TiKV 移到少的）③ Raft Leader 均衡 |
| 6 | TiDB 分布式事务？| **Percolator 算法**（Google Spanner）→ 2PC + Tso 单调时间戳 + Primary 主记录；commit_ts 和 start_ts 必须在同一个 Tso |
| 7 | TiDB 在线 DDL？| **后台异步执行 6 阶段**（不锁表！业务正常）→ pre-commit → write-reorg（旧数据扫描写新列）→ delete-only → done；MVCC 机制保证新旧 schema 自动过渡 |
| 8 | TiDB vs MySQL 原生分库分表？| TiDB：**原生分布式 + 自动分片 + 自动均衡 + 在线 DDL**；MySQL + ShardingSphere：中间件 + 手动配置分片 + DDL 锁表 |
| 9 | TiDB Region 是什么？| **TiDB 表的分片单元**（默认 96MB 一个 Region）→ 超过自动分裂；每个 Region 3 副本（1 Leader + 2 Follower Raft）|
| 10 | TiDB 支持 JOIN 吗？| 支持！**部分 JOIN 下推到 Coprocessor** → Region 内 Join 性能好；跨 Region Join 拉数据到 TiDB Server 本地算 |
| 11 | TiDB 主键设计？| 推荐 **非递增全局唯一主键**（UUID / Snowflake）→ 递增主键会导致所有写请求集中在最后一个 Region（热点）→ PD 均衡困难 |
| 12 | TiDB GC 机制？| **MVCC 多版本 → 后台线程清理过期版本**（超过 1h 默认）→ 类似 InnoDB Purge Thread；防止 Undo Log 无限增长 |
| 13 | TiDB 兼容性？| **MySQL 8.0 兼容** → 支持大部分语法/存储过程/函数 → 不支持部分 MySQL 特有特性（如 AUTO_INCREMENT 精确分配、全文索引）|
| 14 | TiDB 和 Seata 关系？| TiDB 自己实现分布式事务（Percolator 2PC + Raft）→ 不需要 Seata；Seata 是给 MySQL + 分库分表用的 |
| 15 | TiDB 性能瓶颈？| ① PD 单点调度（7.x 开始 PD 高可用）② 跨 Region JOIN 拉数据 ③ 小表高频写入热点 Region |
| 16 | TiDB SQL 执行流程？| Client → TiDB Parser（yacc 解析 AST）→ Optimizer（CBO 生成 PhysicalPlan）→ Executor（Copocessor 下推到 TiKV）→ 返回结果 |
| 17 | TiKV 和 etcd 关系？| TiKV 不用 etcd！PD 用 etcd 存元数据；TiKV 内部用 **go.uber.org/raft** 协议做副本同步 |
| 18 | TiDB vs CockroachDB？| 都是 Spanner 架构；TiDB：MySQL 兼容 + RocksDB；CockroachDB：Postgres 兼容 + Pebble；国内 TiDB 更流行 |
| 19 | TiCDC 是什么？| TiDB 变更数据捕获（CDC）→ 类似 MySQL Binlog → 订阅 TiDB 表变更 → 同步到下游（ES/Kafka/其他 TiDB）|
| 20 | TiDB 学习路线？| ① 用 SQL 层（不用懂 TiKV/Raft）② 然后看 PD 调度 ③ 最后看 TiKV + Raft + Percolator 事务 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：TiDB 7.5.x + TiKV（Rust）+ PD（Go）  
**面试考点：20 题**
