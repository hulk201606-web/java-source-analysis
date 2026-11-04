# TiKV GC + MVCC 旧版本回收源码级深度解析

> **源码基线**：TiKV v8.0.x（Rust）+ PD v8.0.x（Go）  
> **核心位置**：tikv/components/resolved_ts + src/storage/src/gc/ + pd/pkg/ttl  
> **面试权重**：⭐⭐⭐⭐（MVCC 生命周期闭环，TiDB 运维高频）

---

## 一、为什么需要 GC

```
┌─────────────────────────────────────────────────────────────┐
│  ★ MVCC 保留多版本 → 旧版本必须定期回收                     │
│                                                               │
│  回顾 TiKV MVCC 三 CF（之前文档）：                           │
│   Data CF：key+commit_ts → value（多个历史版本）            │
│   Write CF：key+start_ts → Write(commit_ts, type)           │
│   Lock CF：未提交锁                                          │
│                                                               │
│  每次 UPDATE 都产生新版本，旧版本不删：                      │
│   → 磁盘无限膨胀                                             │
│   → 扫描时要跳过的版本越来越多，读放大、性能下降            │
│                                                               │
│  ★ GC（Garbage Collection）任务：                            │
│   定期清理"足够旧、不再被任何事务需要"的历史版本和删除标记   │
│                                                               │
│  ★ 核心矛盾（面试高频）：                                     │
│   删太早：老事务（长事务/快照）需要读旧版本 → 报错           │
│   删太晚：垃圾堆积、性能下降                                  │
│   → 用 safe_point（安全点）界定：只删 ≤ safe_point 的旧版本  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、safe_point 与 GC 生命周期

### 2.1 safe_point 如何确定

```
┌─────────────────────────────────────────────────────────────┐
│  ★ safe_point = 当前可以安全回收数据的时间下界（面试核心）   │
│                                                               │
│  计算规则：                                                    │
│   safe_point = now - tikv_gc_life_time（默认 10 分钟）       │
│                                                               │
│  ★ 但不能早于任何活跃事务的 start_ts！                       │
│   PD 维护全局最小的活跃事务时间戳（minStartTS / gc_safepoint）│
│   来源：                                                       │
│    ① TiDB 上报正在运行的事务 start_ts                        │
│    ② ★ resolved_ts（Raft Region 推进的安全时间，后文详述）   │
│    ③ active_advancing / 锁服务                               │
│                                                               │
│  最终 safe_point = min(now - life_time, 最老活跃事务 ts, ...)│
│   → 保证没有还活着的快照需要看比 safe_point 更老的数据       │
│                                                               │
│  ★ GC 触发方式：                                               │
│   TiDB 内部自动 GC worker（默认开启）                        │
│   每个 tikv_gc_run_interval（默认 10min）推进一次 safe_point │
│   safe_point 存在 PD 中（tikv_gc_safe_point key）            │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 GC 三个阶段（全量+增量）

```rust
// ★ TiKV GC 执行流程（源码级，面试核心！）
//
// src/storage/src/gc/worker.rs
//
// TiKV 的 GC 由 GCWorker 驱动，分三阶段：
//
// 阶段 ① ResolveLock（解决残留锁）
//   扫描 Lock CF 中 start_ts ≤ safe_point 的锁
//   - 如果对应的 Write 已提交 → rollback/把锁转成 write 记录
//   - 如果未提交且超时 → 回滚（清掉锁）
//   → 保证后续 GC 扫描时不会遇到未决事务
//
// 阶段 ② RaftGC / DeleteRange（清理整段删除数据）
//   检查连续的 Delete 版本，如果某个 key 在 safe_point 前
//   已被删除且之后再无写入
//   → 直接 delete_all_files_in_range（删 SST，比逐 key 删快）
//   → 大范围 DROP TABLE/TRUNCATE 空间快速回收
//
// 阶段 ③ GC（逐 key 清理旧版本，核心）
//   扫描每个 key 的 Write CF 版本链：
//   for 每个 key 的所有 write（按 ts 从新到旧）：
//     - 只保留满足 MVCC 可见性所需的版本
//     - 删除 commit_ts ≤ safe_point 且不再需要的旧 Data/Write
//     - 遇到 Put 且比它旧的版本都不可见 → 删旧版本
//     - Delete 标记早于 safe_point 且其后无新版本 → 物理删 key
//
// // GCKeyMode：按 key 扫描 + GCWorker 后台执行
// // 为避免扫描影响在线读写，GC 按 Region 分批 + 限流（gc.max-write-bytes-per-sec）
//
```

### 2.3 旧版本保留规则

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 一个 key 的版本怎么删？（面试必背）                       │
│                                                               │
│  某 key 的 Write CF 版本链（新→旧）：                        │
│   commit_ts=200 Put(v3)                                      │
│   commit_ts=150 Put(v2)                                      │
│   commit_ts=100 Put(v1)                                      │
│                                                               │
│  safe_point = 120：                                           │
│   - commit_ts=200 > 120：保留（新事务可能看到）             │
│   - commit_ts=150 > 120：保留                                │
│   - commit_ts=100 ≤ 120：                                    │
│       v1 是否还需要？                                        │
│       最早活跃快照若 start_ts > 150 → 只能看到 v2/v3        │
│       → v1 对所有 ≥safe_point 的读都不可见 → 可删 v1        │
│       实际规则：保留"safe_point 能看到的那个版本"即可        │
│                                                               │
│  Delete 版本（墓碑）：                                        │
│   commit_ts=80 Delete                                        │
│   safe_point=120 > 80 且该 key 之后无写入                    │
│   → 没有任何 ≥safe_point 的读能看到该 key（已删）           │
│   → 整个 key（Data+Write+Delete 标记）物理删除              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、resolved_ts（关键工程机制）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ resolved_ts = Region 级别"所有读都能读到这个时间"的水位  │
│                                                               │
│  问题：Region 上如果有一个很老的未提交锁（start_ts 很小）    │
│   → safe_point 无法推进过这个锁 → 整个 Region GC 被卡住     │
│   → 垃圾堆积                                                  │
│                                                               │
│  resolved_ts 机制（components/resolved_ts）：                 │
│   每个 Region Leader 维护一个时间戳：                         │
│   "该 Region 所有低于此 ts 的 KV 读取结果都已确定"          │
│   = min(apply 进度, 所有未提交锁的 start_ts, 副本同步进度)  │
│                                                               │
│  推进：                                                        │
│   Region 内无锁或锁都很新 → resolved_ts 随时间推进           │
│   有老锁 → resolved_ts 卡在锁的 start_ts                     │
│   → resolved_ts 通过 Raft heartbeat 上报 PD                 │
│   → PD 用它辅助计算 gc_safe_point、做闪回保护               │
│                                                               │
│  价值：                                                        │
│   ① 让 GC safe_point 计算更精确（知道每个 Region 的安全水位）│
│   ② 支撑 CDC / Change Data Capture（只发 resolved 之后的变更）│
│   ③ 长事务/残留锁定位（resolved_ts 不推进说明有老锁）        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、GC 相关参数与问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ GC 关键参数（mysql.tidb / pd-ctl，面试高频）              │
│                                                               │
│  tikv_gc_enable = true              是否开启自动 GC           │
│  tikv_gc_run_interval = 10m0s       GC 运行间隔              │
│  tikv_gc_life_time = 10m0s          ★ 版本保留时长（决定 safe_point）│
│  tikv_gc_safe_point                  当前安全点（自动维护）   │
│  tikv_gc_mode = distribute          分布式 GC（老版本 central）│
│  tikv_gc_max_write_bytes_per_sec    GC 限速，避免影响线上     │
│                                                               │
│  ★ life_time 权衡：                                           │
│   短（如 10min）：磁盘省、读快；但长查询/事务可能报          │
│     "GC life time is shorter than transaction duration"     │
│   长（如 24h）：支持长快照读、Time Travel；但垃圾多、占空间  │
│                                                               │
│  ★ 手动 GC / 暂停 GC：                                       │
│   备份、大导入、BR 恢复时常临时调大 life_time 或暂停 GC      │
│
└─────────────────────────────────────────────────────────────┘
```

### 常见问题：长事务阻塞 GC

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 长事务/大查询对 GC 的影响（面试高频场景）                │
│                                                               │
│  一个事务 start_ts 很老、跑了很久没提交：                    │
│   → safe_point 不能推进过它的 start_ts                      │
│   → 全局 GC 被拖慢（所有 Region 旧版本保留）                │
│   → resolved_ts 卡在老锁                                     │
│                                                               │
│  TiDB 限制（防护）：                                          │
│   max-txn-ttl（默认 1h）：事务最长存活，超时回滚            │
│   tidb_gc_scan_lock_mode / 锁上报                          │
│                                                               │
│  TiDB 7.x+ 改进：分布式 GC 按 Region 精细化，缩小影响面     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 为什么需要 GC？| MVCC 多版本无限堆积占磁盘+读放大；定期清理旧版本/删除标记 |
| 2 | safe_point 是什么？| 可安全回收的时间下界；**只删 commit_ts ≤ safe_point 的版本** |
| 3 | safe_point 怎么算？| min(now - gc_life_time[默认10min], 最老活跃事务 ts, resolved_ts 限制) |
| 4 | GC 三阶段？| **ResolveLock（清残留锁）→ DeleteRange（整段删）→ GC（逐 key 清版本）** |
| 5 | 旧版本保留规则？| 只保留 safe_point 能读到的版本；更早的 Put 版本可删 |
| 6 | Delete 何时物理删？| Delete 标记 ts ≤ safe_point 且其后无新版本 → 整个 key 物理删 |
| 7 | resolved_ts？| Region 级"低于此 ts 读取结果确定"的水位；卡在最老未提交锁 |
| 8 | resolved_ts 作用？| 精确计算 safe_point + 支撑 CDC + 定位残留老锁 |
| 9 | GC 触发？| TiDB GC worker 每 10min（gc_run_interval）推进 safe_point 下发 TiKV |
| 10 | safe_point 存哪？| PD（tikv_gc_safe_point）|
| 11 | tikv_gc_life_time？| **版本保留时长（默认 10min）**，直接决定 safe_point；权衡磁盘 vs 长事务 |
| 12 | 长事务影响？| start_ts 很老会卡住 safe_point/resolved_ts，全局 GC 变慢、垃圾堆积 |
| 13 | 长事务防护？| max-txn-ttl（默认 1h）超时回滚 |
| 14 | GC 限速？| gc_max_write_bytes_per_sec 分批限流，避免影响在线读写 |
| 15 | DROP TABLE 空间怎么快回收？| GC DeleteRange 直接删整段 SST（delete_all_files_in_range），非逐 key |
| 16 | 面试最高频考点排序？| **1. safe_point 计算 2. GC 三阶段 3. resolved_ts 4. 长事务阻塞 GC 5. life_time** |
| 17 | 学习路径？| pd ttl safepoint → resolved_ts crate → storage/gc/worker.rs → Write CF 版本链清理 |
| 18 | 面试常见追问？| GC 会删掉正在读的旧版本吗？→ 不会，safe_point 不超过任何活跃事务 start_ts |
| 19 | 备份/导入时 GC？| 临时调大 gc_life_time 或暂停 GC，避免备份读旧版本被回收 |
| 20 | 面试陷阱？| 说 GC 只删数据不删 Write/Delete → ❌ 是沿 Write CF 版本链清理，Data/Write/墓碑一起处理 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码位置**：tikv/src/storage/src/gc/worker.rs + components/resolved_ts + pd ttl safepoint  
**面试考点：20 题**  
**差异化点**：safe_point 与活跃事务 + GC 三阶段 + Write 版本链删除规则 + resolved_ts 水位 + 长事务阻塞
