# TiDB Lightning + Dumpling 数据导入导出源码级深度解析

> **源码基线**：TiDB v8.0.x（Go）  
> **核心位置**：pingcap/tidb → br/pkg/lightning/ + dumpling/  
> **面试权重**：⭐⭐⭐⭐（TiDB 数据迁移核心，大数据初始化高频）

---

## 一、工具定位总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB 数据迁移全家桶                                        │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │ 工具       │ 方向           │ 场景           │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ Dumpling   │ ★ 导出 TiDB/   │ 逻辑备份       │                │
│  │            │ MySQL → SQL/CSV│ 全量导出       │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ Lightning  │ ★ 高速导入     │ TB 级初始化    │                │
│  │            │ 文件 → TiDB    │ 全量迁移       │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ BR         │ TiDB ↔ S3/磁盘 │ 物理备份恢复   │                │
│  │            │ （备份恢复）    │ （列存/快照）   │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ TiCDC      │ TiDB → 下游    │ 增量实时同步   │                │
│  └──────────┴──────────────┴──────────────┘                │
│                                                               │
│  典型全量+增量迁移：                                           │
│   Dumpling/BR 全量 → Lightning/BR 导入 → TiCDC 追增量        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Dumpling 导出源码级

### 2.1 并发一致性导出

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Dumpling 核心：保证导出数据一致性快照                      │
│                                                               │
│  // dumpling/cli/.../restore.go（导出主流程）                 │
│                                                               │
│  ① 建立一致性快照：                                            │
│     - 对 TiDB：用 ★ TSO 快照（SET TRANSACTION READ CONSISTENCY│
│       TSO 或 START TRANSACTION WITH CONSISTENT SNAPSHOT）    │
│     - 对 MySQL：FLUSH TABLES WITH READ LOCK 或事务快照       │
│     → 记录 snapshot_ts（整个导出使用同一个时间点）           │
│                                                               │
│  ② 获取所有表/元数据：INFORMATION_SCHEMA                     │
│                                                               │
│  ③ 并发导出：                                                  │
│     - 表级并发（多张表并行）                                 │
│     - ★ 单表分片（chunks）：按主键/索引范围切成多段          │
│       WHERE id >= x AND id < y，多个 worker 并行 SELECT      │
│     - 每个 chunk 输出一个文件（SQL 或 CSV）                  │
│                                                               │
│  ④ 输出格式：                                                  │
│     - SQL：INSERT 语句                                        │
│     - CSV：供 Lightning Local Backend 高速导入               │
│     - 元数据文件：schema.sql（建表语句）+ metadata（snapshot）│
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Dumpling vs mysqldump

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Dumpling 相比 mysqldump 的优势（面试高频）                │
│   ① 单表可并行分片导出（mysqldump 基本单线程）              │
│   ② 原生支持 TiDB TSO 一致性快照                             │
│   ③ 输出 CSV 直接喂 Lightning（超高速导入链路）             │
│   ④ 支持云存储（S3/GCS）                                     │
│   ⑤ 更好的内存控制和断点续传                                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Lightning 导入源码级（重点！）

### 3.1 三种 Backend

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB Lightning 三种导入后端（面试必背）                   │
│                                                               │
│  ① Local Backend（本地后端，★最快，物理 KV 写入）            │
│     Lightning 直接生成 SST 文件 → Ingest 到 TiKV            │
│     绕过 SQL 层，直接写 RocksDB                              │
│     需要 TiKV 节点权限 + 集群空闲（建议导入期停业务）        │
│     速度：100GB+/小时                                        │
│                                                               │
│  ② TiDB Backend（旧 SQL Backend，逻辑写入）                  │
│     把 CSV/SQL 转成 INSERT 走 TiDB SQL 接口                  │
│     慢但在线可用、无需特殊权限                               │
│                                                               │
│  ③ Importer Backend（旧版，已废弃）                          │
│     依赖独立 tikv-importer 组件，新版并入 Local Backend     │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Local Backend 核心流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Lightning Local Backend 导入全流程（面试核心！）          │
│                                                               │
│  // br/pkg/lightning/lightning.go                            │
│                                                               │
│  阶段 ①：预处理（PreCheck）                                   │
│    - 检查集群空间、表是否冲突、区域分布                      │
│    - 切到导入模式：tidb_mode 优化（关闭自动压缩等）          │
│                                                               │
│  阶段 ②：读取 + 编码                                          │
│    - 读 CSV/SQL/Parquet 数据文件                             │
│    - 解析成行 → 按 TiDB TableInfo 编码成 ★ KV 对            │
│      （row key + value，和 TiKV 存储格式一致）               │
│    - 按 Region 的 Key Range 分组                            │
│                                                               │
│  阶段 ③：本地生成 SST                                         │
│    - KV 在 Lightning 本地用 RocksDB 排序、生成 SST 文件     │
│    - SST 的 Key Range 对齐 TiKV Region                      │
│                                                               │
│  阶段 ④：Upload + Ingest                                     │
│    - SST 上传到 TiKV 节点（或共享存储）                      │
│    - 通过 Raft 命令 IngestSST：                              │
│      ★ SST 作为 Raft 日志在副本间复制 → Apply 时直接         │
│        ingest 进 RocksDB（不是一条条写！）                    │
│    - Region Split：提前按 SST 边界分裂 Region，分散写入     │
│                                                               │
│  阶段 ⑤：校验 + 收尾（Checksum）                             │
│    - ADMIN CHECKSUM TABLE 比对源和目标                       │
│    - 恢复普通模式、更新统计信息（ANALYZE）                    │
│                                                               │
│  // 关键源码包：                                              │
│  // br/pkg/lightning/restore/   各 controller               │
│  // br/pkg/lightning/backend/local/  本地 SST + ingest      │
│  // br/pkg/kv/  KV 编码（row key 编码）                     │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 为什么 Local Backend 这么快

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Lightning 快的根本原因（面试必背）                        │
│                                                               │
│  对比普通 INSERT：                                             │
│   普通 INSERT：SQL 解析 → 优化 → 事务（Percolator 2PC）      │
│                 → Raft 逐条复制 → RocksDB 写                 │
│                                                               │
│  Lightning Local：                                            │
│   ① ★ 绕过 SQL 层和事务（直接编码 KV）                       │
│   ② ★ 批量生成有序 SST（顺序写，无 LSM compaction 放大）    │
│   ③ ★ Raft IngestSST 批量复制（整个 SST 文件，不是一条条）  │
│   ④ 提前 Region Split + 分散到多 TiKV 并行                  │
│   ⑤ 导入期切 TiKV 导入模式（关 compaction/调度干扰）         │
│   → 比 SQL 导入快 10 倍以上                                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、断点续传与容错

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Lightning checkpoint 机制（面试高频）                     │
│                                                               │
│  断点信息存在目标 TiDB 的 task_meta 表（或本地文件）：       │
│   - 每个 engine/表/chunk 的已处理状态                        │
│   - 已生成/已导入的 SST 列表                                 │
│                                                               │
│  失败重启：                                                    │
│   读 checkpoint → 跳过已完成部分 → 从未完成 chunk 继续       │
│                                                               │
│  冲突处理（task-info / 重复导入）：                           │
│   - 新集群初始化：none（直接写）                              │
│   - 表已存在：replace（DROP 重建）/ ignore / error           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、与 BR 的区别（面试易混）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Lightning vs BR vs Dumpling 对比（面试必背表）            │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │            │ Dumpling     │ Lightning    │ BR           │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 层级       │ 逻辑（SQL/CSV）│ 逻辑→物理KV │ 物理（SST）  │        │
│  │ 数据源     │ TiDB/MySQL   │ 文件         │ TiDB         │        │
│  │ 目标       │ 文件          │ TiDB         │ TiDB         │        │
│  │ 跨版本兼容 │ ✅ 好（逻辑） │ ✅ 好         │ ❌ 版本要匹配│        │
│  │ MySQL→TiDB│ ✅           │ ✅（读Dumpling）│ ❌          │        │
│  │ 速度       │ 中            │ ★ 极快（Local）│ 极快         │        │
│  │ 用途       │ 导出/逻辑备份 │ 全量初始化    │ 物理备份恢复│        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│  记忆：                                                       │
│   Dumpling = 逻辑导出；Lightning = 高速逻辑导入；BR = 物理备份│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiDB 迁移四件套？| **Dumpling（导出）+ Lightning（导入）+ BR（物理备份）+ TiCDC（增量）** |
| 2 | Dumpling 一致性？| 整个导出用同一个 TSO 快照（TiDB）/ FTWRL（MySQL）|
| 3 | Dumpling 怎么并行？| 表级并发 + **单表按主键范围切 chunk 多 worker** |
| 4 | Dumpling vs mysqldump？| 可并行分片 + TSO 快照 + CSV 喂 Lightning + 云存储 |
| 5 | Lightning 三 Backend？| **Local（物理 KV 最快）+ TiDB（SQL 在线）+ Importer（废弃）** |
| 6 | Local Backend 五阶段？| PreCheck → 读+编码 KV → 本地生成 SST → Upload+Ingest → Checksum |
| 7 | 为什么 Local 快？| **绕过 SQL/事务 + 批量 SST 顺序写 + Raft IngestSST + Region Split 并行** |
| 8 | 什么是 IngestSST？| SST 走 Raft 复制后在 Apply 时直接灌进 RocksDB，非逐条写 |
| 9 | 为什么提前 Region Split？| 按 SST 边界分裂，让导入分散到多 TiKV 多 Region 并行 |
| 10 | 导入模式？| 切 TiKV 导入模式临时关 compaction/调度干扰，导完恢复 |
| 11 | 断点续传？| checkpoint 存表/文件已处理状态，失败跳过已完成 chunk |
| 12 | Lightning vs BR？| Lightning 逻辑文件→KV（跨版本/MySQL 可迁移）；BR 物理 SST（快但版本要匹配）|
| 13 | Dumpling vs BR？| Dumpling 逻辑导出兼容性好；BR 物理备份恢复快但绑定版本 |
| 14 | 面试最高频考点排序？| **1. Local Backend 为何快 2. 三 Backend 3. IngestSST 4. 一致性快照 5. vs BR** |
| 15 | 学习路径？| lightning.go → restore controllers → backend/local → kv 编码 → IngestSST Raft |
| 16 | 面试常见追问？| Local Backend 能在线用吗？→ 建议导入期停业务/独占集群；在线用 TiDB Backend |
| 17 | CSV 怎么变 KV？| 按 TableInfo 用相同 row key 编码规则生成 TiKV KV 对 |
| 18 | 导入后做什么？| CHECKSUM 校验 + ANALYZE 更新统计信息 |
| 19 | 全量+增量方案？| Dumpling 全量+记录 TSO → Lightning 导入 → TiCDC 从该 TSO 追增量 |
| 20 | 面试陷阱？| 说 Lightning 走 INSERT 写 TiDB → ❌ Local Backend 直接生成 SST 经 Raft ingest，绕过 SQL/事务 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：br/pkg/lightning/ + br/pkg/lightning/backend/local/ + dumpling/  
**面试考点：20 题**  
**差异化点**：Dumpling TSO 快照分片导出 + Local Backend SST/Ingest 五阶段 + 为何快 10x + vs BR/Dumpling
