# TiDB BR 物理备份恢复 + EBS Snapshot 源码级深度解析

> **源码基线**：TiDB v8.0.x（Go）  
> **核心位置**：pingcap/tidb → br/cmd + br/pkg/backup + br/pkg/restore + br/pkg/stream  
> **面试权重**：⭐⭐⭐⭐（TiDB 生产容灾必备，PB 级备份核心）

---

## 一、BR 核心定位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ BR = Backup & Restore（TiDB 分布式物理备份恢复工具）      │
│                                                               │
│  对比其他工具（面试必背）：                                    │
│  ┌──────────┬──────────────┬──────────────┐                │
│  │            │ BR            │ Dumpling      │                │
│  ├──────────┼──────────────┼──────────────┤                │
│  │ 层级       │ ★ 物理（SST） │ 逻辑（SQL/CSV）│               │
│  │ 备份内容   │ Region SST   │ 行数据        │                │
│  │ 速度       │ ★ 极快（TB/h）│ 慢            │                │
│  │ 跨版本兼容 │ 要求版本匹配  │ 好            │                │
│  │ 用途       │ 全量物理容灾  │ 逻辑迁移/导出 │                │
│  │ 增量       │ log backup    │ ❌            │                │
│  └──────────┴──────────────┴──────────────┘                │
│                                                               │
│  BR 备份对象：                                                 │
│   ① TiKV Region 的 SST 文件（实际数据，物理快照）           │
│   ② 元数据（backupmeta：TS、表、Region 映射）               │
│   ③ 存储到 S3/GCS/Azure Blob/本地/分布式文件系统           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、全量备份（Backup）源码级

### 2.1 备份一致性快照

```
┌─────────────────────────────────────────────────────────────┐
│  ★ BR 如何保证全量备份一致性（面试核心！）                    │
│                                                               │
│  ① BR 向 PD 申请一个全局时间戳 backup_ts（TSO）              │
│     类似 MVCC 快照：备份这个时间点的一致性数据              │
│                                                               │
│  ② BR 从 PD 获取所有 Region 的路由分布                        │
│     把每个 Region 的备份任务调度给对应 Leader TiKV           │
│                                                               │
│  ③ TiKV 收到备份请求（backup::Endpoint，类似 Coprocessor）  │
│     - 读 RocksDB 的 SST 文件                                 │
│     - 用 backup_ts 过滤：只备份 commit_ts ≤ backup_ts 的 MVCC │
│       版本（旧版本不备份）                                  │
│     - 通过 RocksDB backup_engine / Ingest 方式导出 SST       │
│     - 数据直接流式上传到对象存储（不落本地盘）              │
│                                                               │
│  ④ 备份进行中的新写入：                                      │
│     commit_ts > backup_ts 的数据不会进入备份（快照隔离）    │
│     → 不需要锁表、不影响在线业务                            │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 备份执行链路

```go
// ★ BR 备份核心流程（源码级）
//
// br/pkg/backup/client.go
//
// func (bc *Client) BackupAll(ctx) {
//     // ① 申请备份时间戳
//     backupTS, _ := pdClient.GetTS(ctx)
//
//     // ② 获取全集群 Region 列表
//     regions := PD.ScanRegions(keyRanges)  // 按表/全库 key range
//
//     // ③ 构造备份任务，按 TiKV 节点分组（并发备份）
//     // br/pkg/backup/pushdown.go
//     bctxs := makeBackupContext(regions, backupTS)
//
//     // ④ 调用 TiKV Backup RPC（gRPC stream）
//     //    TiKV 端 backup endpoint 执行：
//     //      - backup_engine 读 RocksDB SST
//     //      - 过滤 MVCC 版本 ≤ backupTS
//     //      - 生成新的 SST 上传 external storage（S3）
//     err = batcher.Backup(ctx, bctxs)
//
//     // ⑤ 汇总各 TiKV 返回的 SST 元数据
//     //    写 backupmeta（备份清单：TS、表 schema、SST 列表）
//     meta := buildBackupMeta(backupTS, schemas, sstFiles)
//     externalStorage.Write("backupmeta", meta)
// }
//
// ★ 关键优化（面试高频）：
//   ① 多 TiKV 并发备份（每个节点并行，接近集群总 IO 上限）
//   ② SST 文件级备份（不是逐行，直接复制 RocksDB 文件）
//   ③ 直接流式传 S3（TiKV → S3，不经过 BR/PD）
//   ④ 只备份 backup_ts 时的可见版本（跳过旧 MVCC 垃圾）
//   ⑤ 支持按表/库过滤 + 限流（ratelimit，不影响线上）
//
```

---

## 三、全量恢复（Restore）源码级

### 3.1 恢复执行链路

```go
// ★ BR 恢复核心流程（源码级，面试核心）
//
// br/pkg/restore/client.go + client_iterate_sst.go
//
// func (rc *Client) RestoreAll(ctx) {
//     // ① 读 backupmeta：拿到 backupTS、schema、SST 列表
//     meta := externalStorage.Read("backupmeta")
//
//     // ② 在目标集群重建库表（先恢复 DDL/schema）
//     rc.restoreSchemas(meta.Databases, meta.Tables)
//     // 新表会分配新的 tableID（可能和源集群不同）
//
//     // ③ 下载 SST 并按新 Region 路由重写
//     //    ★ Rewrite：SST 里的 key 编码带旧 tableID
//     //    恢复时必须替换成新 tableID（key rewrite）
//     //    br/pkg/restore/split_client.go
//     files := rc.splitAndScatterSST(meta.Files)
//
//     // ④ 调度：把 SST 发送给目标 Region 的 Leader TiKV
//     //    TiKV 通过 IngestSST（Raft 命令）批量灌入
//     //    （和 Lightning Local Backend 同样的机制！）
//     rc.ingest(ctx, files)
// }
//
// ★ 恢复关键步骤：
//  ① Schema 先行：先建表，才能确定新 tableID 和 Region 分布
//  ② Key Rewrite：SST 的 key 前缀含 tableID，目标库表 ID 变了要重写
//  ③ Split & Scatter：恢复前先按 SST key range 预分裂 Region
//     并打散到不同 TiKV（避免所有 Region 在一个节点）
//  ④ IngestSST：SST 走 Raft 复制，Apply 时直接 ingest 进 RocksDB
//     （批量、绕过 SQL/Percolator，速度极快）
//  ⑤ 恢复后 ANALYZE 更新统计信息
//
```

### 3.2 为什么恢复也要预分裂 Region

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Split + Scatter（面试高频）                              │
│                                                               │
│  不预分裂：                                                    │
│   新建表只有 1 个 Region，所有 SST 都灌到一个 TiKV           │
│   → 单点写入瓶颈，随后 PD 慢慢 Split（慢、热点）            │
│                                                               │
│  BR 恢复前：                                                   │
│   ① 按 SST 的 key range 批量 Split Region（一次切好）       │
│   ② Scatter：PD 把新 Region 的 Peer 调度到不同 TiKV          │
│   ③ 等 Leader 分散后，并发 IngestSST                         │
│   → 全集群并行灌入，TB 级数据小时级恢复                      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、日志备份与 PITR（增量核心）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Log Backup + PITR（Point-in-Time Recovery，面试核心）    │
│                                                               │
│  全量备份是某个 TSO 的快照；之后的数据变化靠日志备份追：     │
│                                                               │
│  ① log backup start：                                        │
│     TiKV 持续把新产生的 Raft 日志/KV 变更                   │
│     （commit 后的 write）流式备份到 S3                      │
│     备份内容：MVCC write 变更 + TSO 水位（global checkpoint）│
│                                                               │
│  ② 备份水位 global checkpoint：                               │
│     所有 TiKV 都已备份到这个 TSO 之前的变更                 │
│     类似 resolved_ts，保证日志连续性                         │
│                                                               │
│  ③ PITR 恢复到任意时间点：                                    │
│     步骤 A：先恢复全量备份（backup_full_ts）                 │
│     步骤 B：回放 [full_ts, target_ts] 区间的 log backup      │
│     步骤 C：执行到 target_ts 的一致性状态（GC 掉之后版本）   │
│                                                               │
│  // br/pkg/stream/                                           │
│  //   backup_stream.go：日志备份                             │
│  //   stream_mgr.go：日志管理                                │
│  //   restore stream：日志回放                               │
│                                                               │
│  典型用法：                                                    │
│   每周全量 BR + 持续 log backup                              │
│   误删数据 → PITR 恢复到误删前的 TSO                         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、EBS Snapshot 快照备份（云原生）

### 5.1 传统 BR vs EBS 快照

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB EBS Snapshot Backup（云上块存储快照，面试高频）     │
│                                                               │
│  传统 BR：TiKV 读 SST → 网络上传 S3（消耗 TiKV CPU/网络）  │
│                                                               │
│  EBS 快照模式（AWS EBS / 云盘）：                             │
│   TiKV 数据目录就在云盘（EBS Volume）                        │
│   ① BR 协调一致性：PD 取 backupTS + 让各 TiKV 做 checkpoint │
│   ② 对每个 TiKV 节点的数据盘调用云厂商 EBS Snapshot API     │
│   ③ 快照由云存储层完成（增量块、秒级触发，几乎不耗 TiKV）   │
│                                                               │
│  优势：                                                        │
│   ① 备份速度极快（云盘原生快照，分钟级 TB 级）               │
│   ② 不占 TiKV 计算/网络资源（不影响在线业务）              │
│   ③ 增量快照（只存变化的 block，成本低）                    │
│                                                               │
│  限制：                                                        │
│   ① 只能恢复到同类云盘环境（绑定基础设施）                  │
│   ② 跨云/跨平台迁移仍需传统 BR                               │
│   ③ 需要保证一致性 checkpoint（PD TSO + TiKV flush）       │
│
└─────────────────────────────────────────────────────────────┘
```

### 5.2 EBS 备份一致性协调

```go
// ★ EBS Snapshot 一致性协调（源码级）
//
// br/pkg/backup/ebs/ （或 cloud/ 相关包）
//
// func EBSBackup(ctx) {
//     // ① PD 取 backup_ts
//     backupTS := pd.GetTS()
//
//     // ② 通知所有 TiKV：
//     //    - 强制把内存中的 SST/数据 flush 到云盘
//     //    - 建立到 backup_ts 的一致性点（类似 freeze）
//     tikv.FreezeAndFlush(backupTS)
//
//     // ③ 调用云 API 对每块数据盘打快照
//     //    AWS: ec2.CreateSnapshot(volumeId)
//     //    快照之间通过 backup_ts + meta 保证全局一致
//     for volume := range tikvVolumes {
//         cloud.CreateSnapshot(volume, backupTS)
//     }
//
//     // ④ 记录备份元信息（哪些快照对应哪些 TiKV、backupTS）
//     uploadEBSBackupMeta(backupTS, snapshotIDs)
//
//     // ⑤ 解冻 TiKV，恢复正常写入
//     tikv.Unfreeze()
// }
//
// ★ EBS 恢复：
//   ① 从快照创建新云盘（EBS volume from snapshot）
//   ② 挂载到新 TiKV 节点
//   ③ 用 backupTS 元数据启动新集群
//   ④ Region Raft 自动均衡、补齐副本
//
```

### 5.3 三种备份方式对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ BR Local / BR S3 / EBS Snapshot 对比（面试必背）         │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │            │ BR→S3        │ BR→本地/NFS  │ EBS Snapshot│        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 原理       │ TiKV 导 SST  │ 同左         │ 云盘原生快照 │        │
│  │ 资源消耗   │ TiKV CPU/网络│ 磁盘 IO     │ ★ 极低       │        │
│  │ 速度       │ 快           │ 快           │ ★ 极快(秒/分)│        │
│  │ 增量       │ log backup   │ log backup   │ 块级增量     │        │
│  │ 跨环境     │ ✅           │ ✅           │ ❌ 绑定云盘  │        │
│  │ 成本       │ 对象存储便宜 │ 自备存储    │ 云快照费用   │        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（24 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | BR 是什么？| Backup & Restore，TiDB **物理**备份（SST 文件级），TB 级快速容灾 |
| 2 | BR vs Dumpling？| BR 物理 SST 快但版本绑定；Dumpling 逻辑 SQL/CSV 慢但兼容/可迁移 |
| 3 | BR 备份到哪？| S3/GCS/Azure Blob/本地分布式存储；TiKV 流式直传 |
| 4 | 备份一致性怎么保证？| **PD 取 backupTS（TSO 快照），只备份 commit_ts ≤ backupTS 的版本** |
| 5 | 备份影响在线吗？| 不锁表；MVCC 快照读 + 限流，新写入（更大 ts）不进备份 |
| 6 | TiKV 备份端做什么？| backup endpoint 读 RocksDB SST，按 backupTS 过滤 MVCC，流式上传 |
| 7 | 恢复为什么要 Key Rewrite？| SST key 编码带旧 tableID；目标新表 ID 不同，必须重写前缀 |
| 8 | 为什么恢复要 Split Region？| 避免单 Region 单 TiKV 成瓶颈；先切分再 scatter 打散 |
| 9 | SST 怎么进 TiKV？| **IngestSST Raft 命令**（和 Lightning Local 一样），绕过 SQL/事务批量灌入 |
| 10 | 恢复后做什么？| ANALYZE TABLE 更新统计信息 |
| 11 | PITR 原理？| 全量 BR（到 full_ts）+ 回放 log backup 到 target_ts |
| 12 | log backup 备什么？| TiKV 持续把新 KV 变更（Raft apply 的 write）流式备份到 S3 |
| 13 | global checkpoint？| 所有 TiKV 都已备份到的 TSO 水位，保证日志连续（类 resolved_ts）|
| 14 | EBS 快照原理？| 备份前 PD 取 TS+TiKV flush/freeze → 调云 API 对数据盘打快照 |
| 15 | EBS 快照优势？| 秒级/分钟级 + 几乎不耗 TiKV 资源 + 块级增量 |
| 16 | EBS 快照限制？| 绑定云盘环境，不能跨云迁移 |
| 17 | BR 为什么快？| SST 文件级复制 + 全 TiKV 并行 + 流式直传 + IngestSST 批量恢复 |
| 18 | 面试最高频考点排序？| **1. backupTS 一致性 2. 恢复 Key Rewrite+Split 3. IngestSST 4. PITR 5. EBS** |
| 19 | 学习路径？| backup/client → TiKV backup endpoint → restore client_iterate_sst → stream log → ebs |
| 20 | 面试常见追问？| BR 备份时业务还能写吗？→ 能，commit_ts 大于 backupTS 的不进备份 |
| 21 | 备份期间 GC 怎么办？| backupTS 注册为 GC safepoint，GC 不会清理备份需要的旧版本 |
| 22 | 能备份部分表吗？| 可以，--filter 指定库表（备份对应 key range）|
| 23 | 容灾方案组合？| 周期全量 BR + 持续 log backup + PITR；云上可加 EBS 快照 |
| 24 | 面试陷阱？| 说 BR 是逻辑备份 → ❌ 是物理 SST 备份；说 EBS 快照能跨云恢复 → ❌ 绑定云盘 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：br/pkg/backup + br/pkg/restore + br/pkg/stream + br/pkg/backup/ebs  
**面试考点：24 题**  
**差异化点**：backupTS MVCC 快照 + 恢复 Key Rewrite/Split/IngestSST + log backup PITR + EBS 云盘快照一致性
