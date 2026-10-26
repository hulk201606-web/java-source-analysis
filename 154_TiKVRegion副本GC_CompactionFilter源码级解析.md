# TiDB Region 副本 GC + Compaction Filter 源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）  
> **核心位置**：tikv/components/raftstore + src/storage/src/gc/ + components/engine_rocks  
> **面试权重**：⭐⭐⭐⭐（MVCC 垃圾如何真正释放磁盘，深挖高频）

---

## 一、问题：GC 删了数据为什么磁盘没释放

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 关键认知（面试开场必问）                                 │
│                                                               │
│  TiKV 的 MVCC GC（之前文档）只是给旧版本/墓碑打删除标记     │
│  （写入删除标记、tombstone），数据还在 RocksDB 的 SST 文件里│
│   → ★ 磁盘空间不会立刻释放！                                │
│                                                               │
│  真正的空间回收靠 RocksDB 自身：                             │
│   ① Compaction（合并 SST 时丢弃 tombstone/旧数据）          │
│   ② ★ Compaction Filter（TiKV 自定义过滤器，GC 协作）       │
│   ③ DeleteFilesInRange（整段删 SST，DROP TABLE 快速回收）   │
│   ④ RocksDB TTL/CompactionFilterFactory                     │
│                                                               │
│  本文讲清楚：MVCC GC → Compaction Filter → 空间释放的闭环    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、RocksDB Compaction 基础

### 2.1 LSM-Tree 合并如何删数据

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RocksDB LSM Compaction 回收原理（面试必背）               │
│                                                               │
│  LSM 写路径：WAL + MemTable → Flush → L0 SST → Compaction  │
│                                                               │
│  Compaction 把多层 SST 合并、重写成新 SST：                   │
│   ① 读取旧 SST 的 KV                                         │
│   ② 相同 User Key 只保留最新版本（sequence number 最大）    │
│   ③ ★ 遇到 Delete 墓碑，且更旧层没有更新数据 → 直接丢弃     │
│   ④ 丢弃的数据不写入新 SST → 旧 SST 删除后空间释放          │
│                                                               │
│  TiKV MVCC 的特殊性：                                         │
│   User Key 里编码了 commit_ts（data_key + ts）              │
│   → 对 RocksDB 来说每个版本都是"不同的 key"                  │
│   → RocksDB 默认 compaction 不知道哪些 MVCC 版本能删！      │
│   → 必须用 ★ Compaction Filter 告诉它                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Compaction Filter 源码级（核心！）

### 3.1 TiKV 自定义 MVCC 过滤器

```rust
// ★ TiKV MVCC Compaction Filter（源码级，面试核心）
//
// components/engine_rocks/src/compact_listener.rs +
// components/cdc/src/... 以及 src/storage/src/gc/
//
// // 实现 RocksDB 的 CompactionFilter 接口
// pub struct MvccCompactionFilter {
//     // 当前 compaction 的 safe_point（GC 水位）
//     safe_point: TimeStamp,
//     // 各 CF 分别处理
// }
//
// impl CompactionFilter for MvccCompactionFilter {
//     // ★ 每合并一个 KV，RocksDB 回调这个方法决定去留
//     fn filter(&mut self, _level, key, value) -> FilterDecision {
//         // 解析 key：是 default/write/lock 哪个 CF
//         match cf {
//             // ① Write CF：判断 MVCC 版本是否可删
//             CF_WRITE => {
//                 let (raw_key, commit_ts) = decode_key(key);
//                 let write = WriteRef::parse(value)?;
//
//                 // ★ 关键判断：
//                 // - commit_ts <= safe_point（足够旧）
//                 // - 该版本已不被任何快照需要
//                 // - 结合 MVCC GC 规则决定：
//                 //   Put 旧版本：可删
//                 //   Rollback/废弃记录：可直接删
//                 if self.is_obsolete(commit_ts, &write) {
//                     return FilterDecision::Remove;  // 丢弃！
//                 }
//             }
//
//             // ② Default(Data) CF：删对应旧版本 value
//             CF_DEFAULT => {
//                 // data key 带 start_ts，超过 safe_point 保留
//                 // 更旧且无 write 指向 → Remove
//             }
//
//             // ③ Lock CF：残留锁清理（极旧的锁）
//             CF_LOCK => {
//                 // 超时残留锁由 GC ResolveLock 处理
//             }
//         }
//         FilterDecision::Keep
//     }
// }
//
// // FilterDecision：Keep / Remove / ChangeValue / ...
//
```

### 3.2 safe_point 如何传给 Compaction

```
┌─────────────────────────────────────────────────────────────┐
│  ★ GC 与 Compaction 的协作链路（面试必背）                   │
│                                                               │
│  ① TiDB GC worker 推进 safe_point（存 PD）                  │
│  ② TiKV 收到 safe_point，执行 MVCC GC：                      │
│     - ResolveLock + 扫描 Write CF 加删除标记/清理逻辑        │
│  ③ ★ RocksDB Compaction 触发时：                             │
│     - CompactionFilterFactory 读取当前 safe_point            │
│     - 创建 MvccCompactionFilter，按水位在合并时物理删除      │
│  ④ 旧 SST 重写后删除 → 磁盘空间真正释放                     │
│                                                               │
│  // CompactionFilterFactory 每次 compaction 创建新 filter    │
│  // 保证用的是最新 safe_point                                │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 两种 Compaction 触发方式

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiKV 空间回收的触发方式（面试高频）                       │
│                                                               │
│  ① RocksDB 自动 Compaction：                                 │
│     L0 文件数/大小超阈值、level 放大触发                     │
│     后台 compaction_worker 线程自动执行                      │
│     → 顺带通过 Filter 删旧 MVCC 版本                         │
│                                                               │
│  ② 手动 Compact（TiKV 主动）：                               │
│     tikv-ctl compact / compact-cluster                       │
│     - GC 大量删除后想立刻回收空间                            │
│     - 业务低峰手动强制全量 compaction                        │
│     - 注意 IO/CPU 开销大，必须低峰执行                       │
│                                                               │
│  ③ ★ DeleteFilesInRange（整段删除，最快）：                  │
│     DROP TABLE / TRUNCATE / 大量 Delete + GC 后              │
│     如果一个 SST 的整个 key range 都被删                     │
│     → GC 检测到整段可删 → 直接删除整个 SST 文件             │
│     → 不经过逐 key compaction，秒级回收 TB 空间             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Region 副本之间的 GC 一致性

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 多副本如何保证 GC 一致（面试高频追问）                    │
│                                                               │
│  GC 不是每个副本各自为政：                                    │
│   ① safe_point 通过 Raft 日志/PD 下发到 Region Leader       │
│   ② Leader 产生 GC 操作（作为 Raft 命令）                    │
│   ③ GC 写操作经 Raft 复制到所有 Follower，再各自 Apply      │
│   ④ 各副本本地 RocksDB 在 compaction 时用相同 safe_point    │
│      物理回收 → 副本间逻辑数据一致                           │
│                                                               │
│  resolved_ts 的作用（复习）：                                 │
│   Region Leader 推进 resolved_ts，保证低于水位没有未决事务   │
│   → GC 才敢安全清理该 Region 的旧版本                        │
│                                                               │
│  Learner/TiFlash 副本：                                       │
│   同样通过 Raft 收到 GC 产生的删除记录                       │
│   各自 compaction 回收（列存引擎有自己的 GC/DMFile 清理）   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、空间放大与监控

```
┌─────────────────────────────────────────────────────────────┐
│  ★ GC 后空间未释放的常见原因（面试/生产实战）               │
│                                                               │
│  ① Compaction 还没跑（删除标记刚写，等后台合并）            │
│  ② 大量版本分布在高层 SST，需要多层 compaction 才能清        │
│  ③ Compaction Filter 的 safe_point 太旧（GC 卡住）          │
│     - 长事务/未提交锁卡住 safe_point/resolved_ts            │
│  ④ Region 副本分布：drop 的数据分布广，要逐 Region 回收     │
│  ⑤ RocksDB 空间放大本身（LSM 临时写放大）                   │
│                                                               │
│  关键监控指标：                                                │
│   tikv_gc_* ：GC 进度、safe_point、resolved_ts lag           │
│   rocksdb_compaction_*：compaction 字节数/待 compaction 字节│
│   storage size / region size ratios                         │
│   disk 使用率对比 MVCC 删除量                                │
│                                                               │
│  处理手段：                                                    │
│   排查长事务（INFORMATION_SCHEMA.CLUSTER_TIDB_TRX）          │
│   手动 compact-cluster（低峰）                               │
│   调大 gc.life_time 是否合理 / 确认无停滞 changefeed         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | GC 删数据磁盘立刻释放吗？| **不会**；MVCC GC 只写删除标记，物理释放靠 RocksDB Compaction |
| 2 | Compaction 怎么删数据？| 合并 SST 时同 key 留最新版本；遇墓碑且无更新数据则丢弃 |
| 3 | 为什么需要 Compaction Filter？| MVCC key 编码了 commit_ts，RocksDB 视为不同 key，默认规则无法判断旧版本能否删 |
| 4 | Filter 核心逻辑？| 解析 CF/key，**commit_ts ≤ safe_point 且无快照需要 → Remove** |
| 5 | safe_point 怎么传给 Filter？| CompactionFilterFactory 每次 compaction 读最新 safe_point 创建 filter |
| 6 | 三个 CF 各自处理？| Write CF 判版本/墓碑；Data CF 删旧 value；Lock CF 清残留锁 |
| 7 | 空间回收三方式？| **自动 Compaction + 手动 compact-cluster + DeleteFilesInRange 整段删 SST** |
| 8 | DeleteFilesInRange 场景？| DROP/TRUNCATE/大范围删除，整个 SST key range 失效，直接删文件秒级回收 |
| 9 | GC 与 Compaction 协作？| GC 推进 safe_point + 写删除记录；Compaction 按水位物理丢弃 |
| 10 | 多副本 GC 一致性？| GC 作为 Raft 命令 Leader 发起复制，各副本 Apply 后用相同 safe_point 本地回收 |
| 11 | resolved_ts 与 GC？| 保证低于水位没有未决事务，GC 才安全清理 |
| 12 | 空间未释放原因？| compaction 未跑 / 高层 SST / safe_point 被长事务卡住 / LSM 写放大 |
| 13 | 面试最高频考点排序？| **1. GC≠空间释放 2. Compaction Filter 3. safe_point 传递 4. 整段删 SST 5. 多副本一致** |
| 14 | 学习路径？| MVCC GC worker → RocksDB compaction → MvccCompactionFilter → FilterFactory → delete_files_in_range |
| 15 | 面试常见追问？| 大量 DROP 后怎么快速回收？→ DeleteFilesInRange；GC 卡住查什么？→ 长事务/resolved_ts |
| 16 | 手动 compact 风险？| IO/CPU 开销大，可能影响线上，必须业务低峰 |
| 17 | TiFlash 也有这机制？| TiFlash Delta-Tree 有自己的 GC/DMFile 清理，不是 RocksDB Filter |
| 18 | RocksDB sequence number？| RocksDB 内部版本号；MVCC 把 ts 编进 user key，两套版本概念要区分 |
| 19 | GC life_time 影响回收？| life_time 越长旧版本保留越久，compaction 能删的越少，空间占用越大 |
| 20 | 面试陷阱？| 说 GC worker 直接删除磁盘文件 → ❌ GC 只写逻辑删除标记，物理删除发生在 RocksDB compaction |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：src/storage/src/gc/ + engine_rocks compaction listener + RocksDB CompactionFilter  
**面试考点：20 题**  
**差异化点**：GC 标记 vs Compaction 物理回收 + MvccCompactionFilter safe_point + Raft 副本一致 + 整段删 SST
